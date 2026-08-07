"""Metadata-first redaction applied before persistence or export."""

from __future__ import annotations

from dataclasses import dataclass, replace
import hashlib
import re
from typing import Any, Mapping

from .contracts import NormalizedEvent
from .project import sanitize_remote


CONTENT_KEYS = frozenset({
    "prompt", "completion", "content", "message", "messages", "arguments", "result",
    "prompt_text", "response_body", "raw_prompt", "raw_completion", "request_body",
    "input_text", "output_text", "tool_input", "tool_output", "tool_result",
    "tool_arguments", "body", "response", "raw_body", "input", "output", "request", "response_data",
})
ALWAYS_REDACT_KEYS = frozenset({
    "authorization", "auth", "api_key", "apikey", "access_token", "refresh_token",
    "client_secret", "secret", "password", "cookie", "bearer", "credential",
    "api_token", "session_token", "id_token", "private_key", "signing_key",
})
PATH_KEYS = frozenset({
    "path", "file", "filepath", "filename", "cwd", "root", "command", "env", "environment",
    "files", "changed_files", "file_paths", "worktree_path", "repository_root",
})

_SECRET_VALUE_PATTERNS = (
    re.compile(r"(?i)\b(?:bearer|basic)\s+[A-Za-z0-9._~+/=-]{4,}"),
    re.compile(r"(?i)\b(?:sk|xai|gsk)-[A-Za-z0-9_-]{8,}"),
    re.compile(r"(?i)\bAIza[A-Za-z0-9_-]{16,}"),
)


@dataclass(frozen=True)
class PrivacyPolicy:
    """Controls the values that may cross the persistence boundary."""

    content_capture: bool = False
    hash_sensitive_values: bool = False
    store_raw_paths: bool = False
    max_string_length: int = 512

    def __post_init__(self) -> None:
        if self.max_string_length < 32:
            raise ValueError("max_string_length must be at least 32")


def _key_kind(key: str) -> str:
    normalized = key.casefold().replace("-", "_")
    last = normalized.split(".")[-1]
    content_suffixes = ("_prompt", "_completion", "_body", "_messages", "_arguments", "_result", "_content")
    if last in CONTENT_KEYS or last.endswith(content_suffixes):
        return "content"
    secret_suffixes = (
        "_api_key", "_apikey", "_secret", "_password", "_authorization", "_credential",
        "_api_token", "_session_token", "_id_token", "_private_key", "_signing_key",
    )
    if (
        last in ALWAYS_REDACT_KEYS
        or normalized in ALWAYS_REDACT_KEYS
        or normalized.endswith(secret_suffixes)
        or any(marker in normalized.split("_") for marker in {"secret", "password", "cookie", "bearer", "credential"})
    ):
        return "secret"
    if last in PATH_KEYS or normalized in PATH_KEYS:
        return "path"
    return "ordinary"


def _redacted_value(value: Any, policy: PrivacyPolicy, *, kind: str) -> Any:
    if policy.hash_sensitive_values and isinstance(value, (str, int, float, bool)):
        digest = hashlib.sha256(str(value).encode("utf-8")).hexdigest()
        return f"redacted:sha256:{digest}"
    if kind == "content":
        return "[CONTENT_REDACTED]"
    if kind == "path":
        return "[PATH_REDACTED]"
    return "[REDACTED]"


def _looks_like_secret(value: str) -> bool:
    return any(pattern.search(value) for pattern in _SECRET_VALUE_PATTERNS)


def redact_mapping(value: Mapping[str, Any], policy: PrivacyPolicy) -> dict[str, Any]:
    """Recursively redact content, credentials, paths, and overlong strings."""

    def walk(item: Any, key: str | None = None) -> Any:
        kind = _key_kind(key) if key is not None else "ordinary"
        if kind == "content" and not policy.content_capture:
            return _redacted_value(item, policy, kind=kind)
        if kind == "secret":
            return _redacted_value(item, policy, kind=kind)
        if kind == "path" and not policy.store_raw_paths:
            return _redacted_value(item, policy, kind=kind)
        if kind == "ordinary" and isinstance(item, str) and _looks_like_secret(item):
            return _redacted_value(item, policy, kind="secret")
        if isinstance(item, Mapping):
            return {str(child_key): walk(child_value, str(child_key)) for child_key, child_value in item.items()}
        if isinstance(item, list):
            return [walk(child, key) for child in item]
        if isinstance(item, tuple):
            return [walk(child, key) for child in item]
        if isinstance(item, str) and len(item) > policy.max_string_length:
            return f"{item[:policy.max_string_length]}…"
        return item

    return walk(value)


def _looks_like_local_path(value: Any) -> bool:
    if not isinstance(value, str):
        return False
    text = value.strip().replace("\\", "/")
    return bool(re.match(r"^(?:[A-Za-z]:/|/|\\\\|~/)", text))


def _pseudonymize(value: str, prefix: str) -> str:
    normalized = value.strip().replace("\\", "/").casefold()
    return f"{prefix}_sha256:{hashlib.sha256(normalized.encode('utf-8')).hexdigest()}"


def redact_event(event: NormalizedEvent, policy: PrivacyPolicy | None = None) -> NormalizedEvent:
    """Return a redacted immutable event suitable for persistence/export."""

    policy = policy or PrivacyPolicy()
    mapping = redact_mapping(event.to_mapping(), policy)
    project = dict(mapping.get("project", {}))
    remote = project.get("remote")
    if isinstance(remote, str):
        project["remote"] = sanitize_remote(remote)
    if not policy.store_raw_paths:
        project["root"] = None
    for field, prefix in (("project_id", "project"), ("worktree", "worktree")):
        if _looks_like_local_path(project.get(field)):
            project[field] = _pseudonymize(str(project[field]), prefix)
    mapping["project"] = project
    provenance = dict(mapping.get("provenance", {}))
    provenance["content_capture"] = "enabled" if policy.content_capture else "disabled"
    mapping["provenance"] = provenance
    return NormalizedEvent.from_mapping(
        mapping,
        received_at=event.received_at,
        source_kind=event.source.kind,
        source_name=event.source.name,
    )
