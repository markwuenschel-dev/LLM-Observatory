"""Capability-declared adapter interfaces."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Iterable, Iterator, Mapping, Protocol


class AdapterError(ValueError):
    """A source record cannot be safely converted to an observation."""


@dataclass(frozen=True)
class CapabilityRecord:
    provider: str
    client: str
    confidence: str
    capabilities: Mapping[str, str] = field(default_factory=dict)
    auth_modes: tuple[str, ...] = ()
    evidence: tuple[str, ...] = ()
    last_verified: str | None = None

    def to_mapping(self) -> dict[str, Any]:
        return {
            "provider": self.provider,
            "client": self.client,
            "confidence": self.confidence,
            "capabilities": dict(self.capabilities),
            "auth_modes": list(self.auth_modes),
            "evidence": list(self.evidence),
            "last_verified": self.last_verified,
        }


class ObservationAdapter(Protocol):
    name: str

    def capabilities(self) -> CapabilityRecord:
        ...

    def iter_events(self) -> Iterator[Mapping[str, Any]]:
        ...


class AdapterRegistry:
    def __init__(self, adapters: Iterable[ObservationAdapter] = ()) -> None:
        self._adapters: dict[str, ObservationAdapter] = {}
        for adapter in adapters:
            self.register(adapter)

    def register(self, adapter: ObservationAdapter) -> None:
        name = getattr(adapter, "name", "").strip()
        if not name:
            raise AdapterError("adapter name is required")
        if name in self._adapters:
            raise AdapterError(f"adapter already registered: {name}")
        self._adapters[name] = adapter

    def get(self, name: str) -> ObservationAdapter:
        try:
            return self._adapters[name]
        except KeyError as exc:
            raise AdapterError(f"unknown adapter: {name}") from exc

    def names(self) -> tuple[str, ...]:
        return tuple(sorted(self._adapters))

