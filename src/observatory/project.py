"""Read-only Git/project attribution with privacy-safe fallbacks."""

from __future__ import annotations

import hashlib
from pathlib import Path
import re
import subprocess
from typing import Iterable
from urllib.parse import urlsplit

from .contracts import ProjectIdentity


def _digest(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _git_value(path: Path, args: Iterable[str], timeout_seconds: float) -> str | None:
    try:
        result = subprocess.run(
            ["git", "--no-optional-locks", "-C", str(path), *args],
            check=False,
            capture_output=True,
            text=True,
            timeout=timeout_seconds,
        )
    except (OSError, subprocess.TimeoutExpired):
        return None
    if result.returncode != 0:
        return None
    value = result.stdout.strip()
    return value or None


def sanitize_remote(value: str | None) -> str | None:
    """Remove credentials, query strings, and fragments from a Git remote."""

    if not value:
        return None
    text = value.strip()
    scp_match = re.fullmatch(r"[^@\s]+@([^:/\s]+):(.+)", text)
    if scp_match and "://" not in text:
        host, path = scp_match.groups()
        return f"ssh://{host.lower()}/{path.lstrip('/')}"
    parts = urlsplit(text)
    if not parts.scheme or not parts.hostname:
        return None
    try:
        port = f":{parts.port}" if parts.port is not None else ""
    except ValueError:
        port = ""
    host = parts.hostname.lower()
    path = parts.path.rstrip("/")
    return f"{parts.scheme.lower()}://{host}{port}{path}"


def _repository_name(remote: str | None, root: Path) -> str | None:
    if remote:
        path = urlsplit(remote).path.rstrip("/")
        name = Path(path).name
        if name:
            return name.removesuffix(".git") or name
    return root.name or None


def resolve_project(path: str | Path, *, git_timeout_seconds: float = 2.0) -> ProjectIdentity:
    """Resolve Git identity without writing to the supplied path."""

    candidate = Path(path).expanduser().resolve(strict=False)
    root_value = _git_value(candidate, ("rev-parse", "--show-toplevel"), git_timeout_seconds)
    if root_value:
        root = Path(root_value).expanduser().resolve(strict=False)
        remote = sanitize_remote(_git_value(root, ("config", "--get", "remote.origin.url"), git_timeout_seconds))
        branch = _git_value(root, ("symbolic-ref", "--short", "-q", "HEAD"), git_timeout_seconds)
        commit = _git_value(root, ("rev-parse", "HEAD"), git_timeout_seconds)
        common_value = _git_value(root, ("rev-parse", "--git-common-dir"), git_timeout_seconds)
        common = Path(common_value).expanduser() if common_value else root / ".git"
        if not common.is_absolute():
            common = (root / common).resolve(strict=False)
        worktree = f"worktree_sha256:{_digest(f'{common}|{root}') }"
        identity_basis = f"remote:{remote}" if remote else f"root:{str(root).casefold()}"
        return ProjectIdentity(
            project_id=f"repo_sha256:{_digest(identity_basis)}",
            repository=_repository_name(remote, root),
            root=str(root),
            remote=remote,
            branch=branch,
            commit=commit,
            worktree=worktree,
        )

    fallback = str(candidate).casefold()
    return ProjectIdentity(
        project_id=f"local_sha256:{_digest(fallback)}",
        repository=None,
        root=str(candidate),
        remote=None,
        branch=None,
        commit=None,
        worktree=f"worktree_sha256:{_digest(fallback)}",
    )
