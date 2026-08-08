"""Read-only engineering outcome collectors.

Outcome records express correlation and evidence, not causality.  They can be
created by CI wrappers, local command runners, Git integrations, or human
correction workflows without adding files or dependencies to the observed
repository.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
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
    project: ProjectIdentity | None = None,
    source_name: str = "outcome-collector",
    observed_at: datetime | str | None = None,
    attributes: Mapping[str, Any] | None = None,
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
        "execution": {"task_id": task_id, "task_class": task_class},
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
    value["event_id"] = stable_event_id(value)
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


def git_outcome_snapshot(project_path: str, *, correlation_id: str | None = None) -> NormalizedEvent:
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
        evidence_source="git",
        project=project,
        attributes={"changed_files": changed_files, "changed_file_count": len(changed_files), "commit": project.commit, "branch": project.branch},
    )
