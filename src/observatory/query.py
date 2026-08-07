"""Small query helpers shared by the API and CLI."""

from __future__ import annotations

from typing import Any, Mapping

from .store import EventStore


def summary_for(store: EventStore, filters: Mapping[str, str] | None = None) -> dict[str, Any]:
    return store.summary(filters)

