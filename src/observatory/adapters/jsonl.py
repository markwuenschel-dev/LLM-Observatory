"""Generic metadata JSONL adapter for synthetic fixtures and external bridges."""

from __future__ import annotations

from datetime import datetime, timezone
import json
from pathlib import Path
from typing import Any, Iterator, Mapping

from .base import AdapterError, CapabilityRecord
from ..contracts import stable_event_id
from ..project import resolve_project


_MAX_ERRORS = 256


class JsonlAdapter:
    name = "jsonl"

    def __init__(
        self,
        path: str | Path,
        *,
        source_name: str = "jsonl",
        max_line_bytes: int = 1_048_576,
        project_path: str | Path | None = None,
    ) -> None:
        self.path = Path(path)
        self.source_name = source_name
        self.max_line_bytes = max_line_bytes
        self.project = resolve_project(project_path).to_mapping() if project_path is not None else None
        self.errors: list[str] = []

    def capabilities(self) -> CapabilityRecord:
        return CapabilityRecord(
            provider="unknown",
            client=self.source_name,
            confidence="VERIFIED_LOCALLY",
            capabilities={"jsonl_events": "SUPPORTED", "native_otel": "UNKNOWN", "authoritative_usage": "UNKNOWN"},
            auth_modes=("unknown",),
            evidence=("generic JSONL parser contract",),
            last_verified=datetime.now(timezone.utc).date().isoformat(),
        )

    def iter_events(self) -> Iterator[Mapping[str, Any]]:
        if not self.path.exists():
            raise AdapterError(f"source file not found: {self.path}")
        with self.path.open("rb") as handle:
            for line_number, raw_line in enumerate(handle, start=1):
                if len(raw_line) > self.max_line_bytes:
                    if len(self.errors) < _MAX_ERRORS:
                        self.errors.append(f"line {line_number}: exceeds max_line_bytes")
                    continue
                if not raw_line.strip():
                    continue
                try:
                    value = json.loads(raw_line.decode("utf-8"))
                except (UnicodeDecodeError, json.JSONDecodeError) as exc:
                    if len(self.errors) < _MAX_ERRORS:
                        self.errors.append(f"line {line_number}: invalid JSON: {exc}")
                    continue
                if not isinstance(value, dict):
                    if len(self.errors) < _MAX_ERRORS:
                        self.errors.append(f"line {line_number}: record must be an object")
                    continue
                normalized = dict(value)
                normalized.setdefault("schema_version", "1.0")
                normalized.setdefault("event_type", "unknown.event")
                source = dict(normalized.get("source", {})) if isinstance(normalized.get("source"), Mapping) else {}
                source.setdefault("kind", "adapter")
                source.setdefault("name", self.source_name)
                normalized["source"] = source
                if self.project is not None:
                    existing_project = normalized.get("project") if isinstance(normalized.get("project"), Mapping) else {}
                    project = dict(self.project)
                    for key, value in existing_project.items():
                        if value not in (None, ""):
                            project[key] = value
                    normalized["project"] = project
                # Project and source identity are part of the fallback ID
                # boundary.  Add them before hashing so the same raw event
                # replayed from two repositories cannot conflict globally.
                normalized["event_id"] = stable_event_id(normalized)
                yield normalized
