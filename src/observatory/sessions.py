"""Host-level discovery of session-to-project bindings.

Claude Code records each session under a per-project directory in the user's
home area: ``~/.claude/projects/<encoded-working-directory>/<session-id>.jsonl``.
That layout is a global, external source of exactly the fact native OTLP
telemetry lacks -- which working directory a session belonged to -- and reading
it requires no configuration in any client and leaves nothing inside any
observed repository.

Privacy boundary: only directory names and file *names* are read. Session
transcripts contain prompts and completions and are never opened. The discovery
result is a session identifier plus a resolved project identity, nothing else.
"""

from __future__ import annotations

import os
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Iterator

from .project import ProjectIdentity, resolve_project

# Session files are named by UUID; anything else in the directory is ignored.
_SESSION_NAME = re.compile(r"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$")

# Bound so a pathological home directory cannot make discovery unbounded.
MAX_PROJECT_DIRECTORIES = 512
MAX_SESSIONS_PER_PROJECT = 2000
# Hard ceiling on decode work, so one malformed name cannot hang discovery.
MAX_DECODE_ATTEMPTS = 4096


@dataclass(frozen=True)
class DiscoveredSession:
    session_id: str
    project_root: str
    project: ProjectIdentity
    evidence_source: str = "claude-session-directory"


def default_claude_projects_root() -> Path:
    home = os.environ.get("USERPROFILE") or os.environ.get("HOME")
    base = Path(home) if home else Path.home()
    return base / ".claude" / "projects"


def decode_project_directory(name: str) -> Path | None:
    """Recover the working directory a project folder was named after.

    The encoding replaces the drive colon and every separator with ``-``, and a
    repository name may itself contain ``-`` (``ibkr-auto-trader``), so the
    mapping is ambiguous. Resolve it against the filesystem with backtracking: a
    greedy longest-match commits to the first directory that happens to exist
    and then fails on the remainder, silently dropping every session under a
    project whose name collides with a shorter sibling. A name that does not
    resolve to a real directory yields None rather than a fabricated path.
    """

    if not name or name.startswith("."):
        return None
    segments = name.split("-")
    if not segments:
        return None
    if len(segments) >= 2 and len(segments[0]) == 1 and segments[1] == "":
        root = Path(f"{segments[0]}:\\")
        remaining = segments[2:]
    else:
        root = Path("/")
        remaining = [segment for segment in segments if segment]
    if not root.exists():
        return None

    # Backtracking over n segments is exponential without memoisation: a
    # pathological name with ~20 hyphens took 11 s and 75,000 stat calls, and a
    # 255-character component would hang discovery indefinitely.
    attempted: set[tuple[str, int]] = set()

    def _walk(current: Path, rest: list[str]) -> Path | None:
        if not rest:
            return current
        memo_key = (str(current), len(rest))
        if memo_key in attempted:
            return None
        attempted.add(memo_key)
        if len(attempted) > MAX_DECODE_ATTEMPTS:
            return None
        # Longest first, but fall back to shorter joins when the tail fails.
        for length in range(len(rest), 0, -1):
            segment = "-".join(rest[:length])
            # A segment naming a parent would let an encoded name escape the
            # tree it claims to describe, and the escaped path would then be
            # handed to `resolve_project`, which shells out to git there.
            if segment in ("..", "."):
                continue
            candidate = current / segment
            if not candidate.is_dir():
                continue
            found = _walk(candidate, rest[length:])
            if found is not None:
                return found
        return None

    resolved = _walk(root, remaining)
    if resolved is None:
        return None
    # Normalize so one directory cannot be processed twice under two spellings.
    return Path(os.path.normpath(str(resolved)))


def discover_sessions(
    root: str | Path | None = None,
    *,
    git_timeout_seconds: float = 0.5,
) -> Iterator[DiscoveredSession]:
    """Yield one binding per discovered session.

    Never opens a session file. A project directory that cannot be resolved to
    a real path is skipped rather than reported under a guessed identity.
    """

    base = Path(root) if root is not None else default_claude_projects_root()
    if not base.is_dir():
        return
    resolved_projects: dict[str, ProjectIdentity] = {}
    for index, entry in enumerate(sorted(base.iterdir())):
        if index >= MAX_PROJECT_DIRECTORIES:
            break
        if not entry.is_dir():
            continue
        project_root = decode_project_directory(entry.name)
        if project_root is None:
            continue
        key = str(project_root)
        if key not in resolved_projects:
            try:
                resolved_projects[key] = resolve_project(project_root, git_timeout_seconds=git_timeout_seconds)
            except (OSError, RuntimeError, ValueError):
                continue
        identity = resolved_projects[key]
        seen: set[str] = set()
        for count, child in enumerate(sorted(entry.iterdir())):
            if count >= MAX_SESSIONS_PER_PROJECT:
                break
            # Sessions appear both as `<uuid>.jsonl` and as a `<uuid>/` folder.
            stem = child.stem if child.suffix == ".jsonl" else child.name
            if not _SESSION_NAME.match(stem) or stem in seen:
                continue
            seen.add(stem)
            yield DiscoveredSession(session_id=stem, project_root=key, project=identity)
