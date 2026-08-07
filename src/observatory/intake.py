"""Bounded, fail-open event intake shared by HTTP and offline adapters."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Iterable, Mapping

from .clock import utc_now
from .contracts import ContractError, NormalizedEvent
from .store import EventStore


@dataclass(frozen=True)
class IntakeResult:
    inserted: int = 0
    duplicate: int = 0
    conflict: int = 0
    rejected: int = 0
    errors: tuple[str, ...] = ()

    def add(self, status: str, error: str | None = None) -> "IntakeResult":
        values = {
            "inserted": self.inserted,
            "duplicate": self.duplicate,
            "conflict": self.conflict,
            "rejected": self.rejected,
            "errors": list(self.errors),
        }
        if status in values and status != "errors":
            values[status] += 1
        if error:
            values["errors"].append(error)
        return IntakeResult(**{**values, "errors": tuple(values["errors"])})

    def to_mapping(self) -> dict[str, Any]:
        return {
            "inserted": self.inserted,
            "duplicate": self.duplicate,
            "conflict": self.conflict,
            "rejected": self.rejected,
            "errors": list(self.errors),
        }


class Intake:
    """Normalize and append records without ever invoking a provider."""

    def __init__(self, store: EventStore, *, max_records: int = 256) -> None:
        if max_records < 1:
            raise ValueError("max_records must be positive")
        self.store = store
        self.max_records = max_records

    def ingest(self, records: Iterable[Mapping[str, Any]]) -> IntakeResult:
        result = IntakeResult()
        for index, record in enumerate(records):
            if index >= self.max_records:
                result = result.add("rejected", f"batch exceeds max_records={self.max_records}")
                break
            if not isinstance(record, Mapping):
                result = result.add("rejected", f"record {index} must be an object")
                continue
            try:
                event = NormalizedEvent.from_mapping(record, received_at=utc_now())
                result = result.add(self.store.append(event).status)
            except (ContractError, ValueError) as exc:
                result = result.add("rejected", f"record {index}: {exc}")
        return result

