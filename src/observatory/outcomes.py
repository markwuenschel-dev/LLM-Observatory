"""Read-only engineering outcome collectors.

Outcome records express correlation and evidence, not causality.  They can be
created by CI wrappers, local command runners, Git integrations, or human
correction workflows without adding files or dependencies to the observed
repository.
"""

from __future__ import annotations

from dataclasses import dataclass, replace
from datetime import datetime, timezone
import json
import subprocess
import time
from typing import Any, Iterable, Mapping, Sequence

from .contracts import NormalizedEvent, ProjectIdentity, stable_event_id
from .project import resolve_project


PASS_STATUSES = frozenset({"pass", "passed", "success", "succeeded", "accepted", "complete", "completed"})
FAIL_STATUSES = frozenset({"fail", "failed", "failure", "rejected", "error", "aborted"})


def _now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def make_outcome_event(
    kind: str,
    status: str,
    *,
    correlation_id: str | None = None,
    correlation_basis: str | None = None,
    evidence_source: str = "unknown",
    task_id: str | None = None,
    task_class: str | None = None,
    session_id: str | None = None,
    project: ProjectIdentity | None = None,
    source_name: str = "outcome-collector",
    observed_at: datetime | str | None = None,
    attributes: Mapping[str, Any] | None = None,
    volatile_project_fields: Sequence[str] = (),
) -> NormalizedEvent:
    """Build an explicit outcome observation without asserting a cause."""

    if not kind.strip() or not status.strip():
        raise ValueError("outcome kind and status are required")
    project = project or ProjectIdentity()
    timestamp = observed_at or _now()
    value: dict[str, Any] = {
        "schema_version": "1.0",
        "event_type": f"outcome.{kind.strip()}",
        "observed_at": timestamp,
        "source": {"kind": "collector", "name": source_name},
        "project": project.__dict__,
        "execution": {"task_id": task_id, "task_class": task_class, "session_id": session_id},
        "reliability": {
            "status": "succeeded" if status.casefold() in PASS_STATUSES else "failed" if status.casefold() in FAIL_STATUSES or status.casefold() == "timeout" else "unknown",
            "timeout": status.casefold() == "timeout",
            "aborted": status.casefold() == "aborted",
        },
        "outcome": {
            "kind": kind.strip(),
            "status": status.strip(),
            "correlation_id": correlation_id,
            "correlation_basis": correlation_basis,
            "evidence_source": evidence_source or "unknown",
        },
        "provenance": {
            "fields": {"outcome.status": evidence_source or "unknown"},
            "adapter": source_name,
            "semantic_conventions": "llm-observatory.outcome/v1",
            "content_capture": "disabled",
        },
        "attributes": dict(attributes or {}),
    }
    # Some project fields describe the machine that did the collecting rather
    # than the thing being observed. `worktree` is a hash of the checkout path,
    # so the same landed commit collected from two worktrees of one repository
    # produced two ids, identical in every dimension a dashboard can group by.
    # Neutralize them for the identity only: the envelope keeps the real values
    # as evidence, and filters and dashboards keep working.
    identity = value
    if volatile_project_fields:
        identity = dict(value)
        identity["project"] = {
            key: (None if key in set(volatile_project_fields) else field)
            for key, field in value["project"].items()
        }
    value["event_id"] = stable_event_id(identity)
    return NormalizedEvent.from_mapping(value)


@dataclass(frozen=True)
class CommandOutcome:
    event: NormalizedEvent
    returncode: int
    duration_ms: float


def run_command_outcome(
    command: Sequence[str],
    *,
    project_path: str,
    kind: str = "command",
    correlation_id: str | None = None,
    correlation_basis: str | None = None,
    task_id: str | None = None,
    task_class: str | None = None,
    session_id: str | None = None,
    evidence_source: str = "local-command",
    timeout_seconds: float = 900.0,
) -> CommandOutcome:
    """Run an explicitly supplied command and retain only outcome metadata.

    The command is passed as an argument list (never a shell string).  Stdout,
    stderr, credentials, and file contents are intentionally not included in
    the event.  This collector is opt-in and does not run as part of intake.
    """

    if not command:
        raise ValueError("command must not be empty")
    started = time.perf_counter()
    try:
        completed = subprocess.run(
            list(command),
            cwd=project_path,
            check=False,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            timeout=timeout_seconds,
        )
        returncode = completed.returncode
    except subprocess.TimeoutExpired:
        returncode = -1
    duration_ms = (time.perf_counter() - started) * 1000
    event = make_outcome_event(
        kind,
        "passed" if returncode == 0 else "timeout" if returncode == -1 else "failed",
        correlation_id=correlation_id,
        correlation_basis=correlation_basis,
        task_id=task_id,
        task_class=task_class,
        session_id=session_id,
        evidence_source=evidence_source,
        project=resolve_project(project_path),
        attributes={
            "command": list(command),
            "command_name": str(command[0]).replace("\\", "/").rsplit("/", 1)[-1],
            "command_arg_count": max(len(command) - 1, 0),
            "exit_code": returncode,
            "duration_ms": round(duration_ms, 3),
        },
    )
    return CommandOutcome(event=event, returncode=returncode, duration_ms=duration_ms)


DEFAULT_COMMIT_WINDOW_SECONDS = 4 * 3600
MAX_COMMITS_PER_PROJECT = 500


def git_commit_outcomes(
    project_path: str,
    *,
    since: str | None = None,
    limit: int = MAX_COMMITS_PER_PROJECT,
    window_seconds: int = DEFAULT_COMMIT_WINDOW_SECONDS,
) -> list[NormalizedEvent]:
    """Observe commits that landed in a repository, as outcome events.

    A commit is engineering work reaching a durable state, and it can be seen
    from outside the repository with a read-only Git command -- no hook, no
    file, and nothing installed in the project. It carries no session or task of
    its own, so it is associated by bounded temporal proximity within the same
    project, which is the weakest basis this store accepts and is labelled as
    such on every resulting link.

    Only counts are retained. Commit messages, author identities, and file paths
    are never read into the event: a message or path can carry exactly the
    content the privacy boundary exists to exclude.
    """

    project = resolve_project(project_path)
    arguments = [
        "git",
        "--no-optional-locks",
        "-C",
        str(project_path),
        "log",
        f"--max-count={max(1, min(int(limit), MAX_COMMITS_PER_PROJECT))}",
        "--no-merges",
        "--numstat",
        "--pretty=format:%H%cI",
    ]
    if since:
        arguments.append(f"--since={since}")
    try:
        completed = subprocess.run(
            arguments,
            check=False,
            capture_output=True,
            text=True,
            timeout=15,
        )
    except (OSError, subprocess.TimeoutExpired):
        return []
    if completed.returncode != 0 or not completed.stdout.strip():
        return []

    events: list[NormalizedEvent] = []
    commit: dict[str, Any] | None = None

    def _flush() -> None:
        if commit is None:
            return
        events.append(
            make_outcome_event(
                "commit",
                "landed",
                correlation_id=project.project_id,
                correlation_basis="project_window",
                evidence_source="git",
                # `resolve_project` reports HEAD at collection time, not the
                # commit being described. `commit` is inside the stable event
                # identity, so stamping HEAD gave every historical commit a new
                # id the moment anything landed -- and the scheduled collector
                # re-inserted the whole trailing window every cycle.
                # `branch` is in the identity too and also comes from HEAD at
                # collection time, so a plain `git checkout -b` re-minted every
                # id and the recorder re-inserted the whole window. A landed
                # commit is not owned by whatever branch is checked out now.
                project=replace(project, commit=commit["sha"], branch=None),
                # `worktree` hashes the checkout path, so one landed commit
                # collected from two worktrees of the same repository produced
                # two events identical in project, repository and commit --
                # double-counted and indistinguishable in every dashboard.
                volatile_project_fields=("worktree",),
                observed_at=commit["committed_at"],
                source_name="git-outcome-collector",
                attributes={
                    "commit": commit["sha"][:12],
                    "files_changed": commit["files"],
                    "insertions": commit["insertions"],
                    "deletions": commit["deletions"],
                    "branch_at_collection": project.branch,
                    "correlation_window_seconds": int(window_seconds),
                    "association": "bounded temporal proximity within the same project",
                },
            )
        )

    for line in completed.stdout.splitlines():
        if line.startswith("") or (line and line[0] == ""):
            line = line[1:]
        if "" in line or (len(line) > 40 and line[40:41] == ""):
            _flush()
            sha, _, stamp = line.partition("")
            commit = {"sha": sha.strip(), "committed_at": stamp.strip(), "files": 0, "insertions": 0, "deletions": 0}
            continue
        if commit is None or not line.strip():
            continue
        parts = line.split("	")
        if len(parts) >= 3:
            commit["files"] += 1
            for key, raw in (("insertions", parts[0]), ("deletions", parts[1])):
                try:
                    commit[key] += int(raw)
                except ValueError:
                    # "-" marks a binary file; the change is real but uncounted.
                    pass
    _flush()
    return events


# GitHub Actions conclusions mapped onto the outcome vocabulary. Anything not
# listed -- notably a run still in progress, whose conclusion is null -- is not
# an outcome yet and is skipped rather than guessed at.
_CI_CONCLUSIONS = {
    "success": "passed",
    "failure": "failed",
    "startup_failure": "failed",
    "timed_out": "timeout",
    "cancelled": "aborted",
    "action_required": "failed",
}
MAX_CI_RUNS_PER_PROJECT = 100


def ci_run_outcomes(
    project_path: str,
    *,
    since: str | None = None,
    limit: int = MAX_CI_RUNS_PER_PROJECT,
    window_seconds: int = DEFAULT_COMMIT_WINDOW_SECONDS,
    timeout_seconds: float = 25.0,
) -> list[NormalizedEvent]:
    """Observe CI results for a repository through the GitHub CLI.

    CI is the one widely available source of a genuine pass/fail engineering
    outcome. Commits say work landed; they cannot say whether it worked, so a
    store fed only by commits can never produce a success rate however long it
    runs. This reads run metadata only -- conclusion, head SHA, workflow name,
    timestamp -- and never job logs, which can contain anything.

    Requires an authenticated `gh`; absence is not an error, it simply means
    this source is unavailable and the caller records nothing for it.
    """

    project = resolve_project(project_path)
    try:
        completed = subprocess.run(
            [
                "gh",
                "run",
                "list",
                "--limit",
                str(max(1, min(int(limit), MAX_CI_RUNS_PER_PROJECT))),
                "--json",
                "conclusion,headSha,headBranch,createdAt,name,databaseId",
            ],
            check=False,
            capture_output=True,
            text=True,
            timeout=timeout_seconds,
            cwd=project_path,
        )
    except (OSError, subprocess.TimeoutExpired):
        return []
    if completed.returncode != 0 or not completed.stdout.strip():
        return []
    try:
        runs = json.loads(completed.stdout)
    except json.JSONDecodeError:
        return []
    if not isinstance(runs, list):
        return []

    events: list[NormalizedEvent] = []
    for run in runs:
        if not isinstance(run, Mapping):
            continue
        conclusion = str(run.get("conclusion") or "").strip().lower()
        status = _CI_CONCLUSIONS.get(conclusion)
        if status is None:
            # Still running, skipped, or a conclusion we do not model.
            continue
        created = str(run.get("createdAt") or "").strip()
        if not created:
            continue
        # Without an age bound this re-collects runs that retention has already
        # expired, so every cycle re-inserts them and the next purge deletes
        # them again -- work that never converges and, because each purge
        # compacts the store, starves intake on a schedule.
        if since and created < since:
            continue
        head_sha = str(run.get("headSha") or "")
        # Unlike a commit -- which is not owned by whatever branch happens to be
        # checked out when it is collected -- a CI run genuinely has a branch,
        # it is immutable for that run, and GitHub reports it. Using it keeps
        # `?branch=` filtering meaningful for the one outcome family that can
        # answer it honestly, instead of leaving every CI panel to render 0.
        head_branch = str(run.get("headBranch") or "").strip() or None
        events.append(
            make_outcome_event(
                "ci",
                status,
                correlation_id=project.project_id,
                correlation_basis="project_window",
                evidence_source="github-actions",
                # An absent headSha must not silently borrow the collector's
                # HEAD: that asserts CI ran on a commit it never ran on.
                project=replace(project, commit=head_sha or None, branch=head_branch),
                volatile_project_fields=("worktree",),
                observed_at=created,
                source_name="ci-outcome-collector",
                attributes={
                    "workflow": str(run.get("name") or "unknown")[:120],
                    "run_id": run.get("databaseId"),
                    "commit": head_sha[:12],
                    "conclusion": conclusion,
                    "correlation_window_seconds": int(window_seconds),
                    "association": "bounded temporal proximity within the same project",
                },
            )
        )
    return events


def git_outcome_snapshot(
    project_path: str,
    *,
    correlation_id: str | None = None,
    correlation_basis: str | None = None,
    task_id: str | None = None,
    task_class: str | None = None,
    session_id: str | None = None,
) -> NormalizedEvent:
    """Collect a read-only Git snapshot as an outcome/correlation event."""

    project = resolve_project(project_path)
    changed_files: list[str] = []
    try:
        result = subprocess.run(
            ["git", "--no-optional-locks", "-C", project_path, "status", "--porcelain", "--untracked-files=all"],
            check=False,
            capture_output=True,
            text=True,
            timeout=3,
        )
        if result.returncode == 0:
            for line in result.stdout.splitlines():
                if len(line) >= 4:
                    changed_files.append(line[3:].strip())
    except (OSError, subprocess.TimeoutExpired):
        pass
    return make_outcome_event(
        "git.snapshot",
        "captured",
        correlation_id=correlation_id,
        correlation_basis=correlation_basis,
        task_id=task_id,
        task_class=task_class,
        session_id=session_id,
        evidence_source="git",
        project=project,
        attributes={"changed_files": changed_files, "changed_file_count": len(changed_files), "commit": project.commit, "branch": project.branch},
    )
