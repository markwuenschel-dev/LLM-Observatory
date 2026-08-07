"""Explicit backup, restore, migration, and audited retention operations."""

from __future__ import annotations

from datetime import datetime, timezone
from contextlib import closing
import hashlib
import json
from pathlib import Path
import sqlite3
import uuid
from typing import Any, Iterable

from .store import EventStore


_EVIDENCE_TRIGGERS = (
    "prevent_ingest_ledger_update",
    "prevent_ingest_ledger_delete",
    "prevent_measurement_facts_update",
    "prevent_measurement_facts_delete",
    "prevent_outcome_events_update",
    "prevent_outcome_events_delete",
    "prevent_attribution_edges_update",
    "prevent_attribution_edges_delete",
)

_EVIDENCE_TRIGGER_SQL = """
CREATE TRIGGER IF NOT EXISTS prevent_ingest_ledger_update BEFORE UPDATE ON ingest_ledger BEGIN SELECT RAISE(ABORT, 'ingest_ledger is append-only'); END;
CREATE TRIGGER IF NOT EXISTS prevent_ingest_ledger_delete BEFORE DELETE ON ingest_ledger BEGIN SELECT RAISE(ABORT, 'ingest_ledger is append-only'); END;
CREATE TRIGGER IF NOT EXISTS prevent_measurement_facts_update BEFORE UPDATE ON measurement_facts BEGIN SELECT RAISE(ABORT, 'measurement_facts is append-only'); END;
CREATE TRIGGER IF NOT EXISTS prevent_measurement_facts_delete BEFORE DELETE ON measurement_facts BEGIN SELECT RAISE(ABORT, 'measurement_facts is append-only'); END;
CREATE TRIGGER IF NOT EXISTS prevent_outcome_events_update BEFORE UPDATE ON outcome_events BEGIN SELECT RAISE(ABORT, 'outcome_events is append-only'); END;
CREATE TRIGGER IF NOT EXISTS prevent_outcome_events_delete BEFORE DELETE ON outcome_events BEGIN SELECT RAISE(ABORT, 'outcome_events is append-only'); END;
CREATE TRIGGER IF NOT EXISTS prevent_attribution_edges_update BEFORE UPDATE ON attribution_edges BEGIN SELECT RAISE(ABORT, 'attribution_edges is append-only'); END;
CREATE TRIGGER IF NOT EXISTS prevent_attribution_edges_delete BEFORE DELETE ON attribution_edges BEGIN SELECT RAISE(ABORT, 'attribution_edges is append-only'); END;
"""


def _restore_evidence_triggers(connection: sqlite3.Connection) -> None:
    for statement in _EVIDENCE_TRIGGER_SQL.strip().splitlines():
        if statement.strip():
            connection.execute(statement)


def schema_versions(store: EventStore) -> list[str]:
    rows = store.connection.execute("SELECT version FROM schema_migrations ORDER BY version").fetchall()
    return [str(row["version"]) for row in rows]


def backup_database(source: str | Path, target: str | Path) -> dict[str, Any]:
    source_path = Path(source).expanduser().resolve(strict=True)
    target_path = Path(target).expanduser().resolve(strict=False)
    if source_path == target_path:
        raise ValueError("backup target must differ from source")
    target_path.parent.mkdir(parents=True, exist_ok=True)
    with closing(sqlite3.connect(source_path)) as source_connection:
        with closing(sqlite3.connect(target_path)) as target_connection:
            source_connection.backup(target_connection)
            target_connection.commit()
    return {
        "source": str(source_path),
        "target": str(target_path),
        "bytes": target_path.stat().st_size,
        "sha256": _sha256(target_path),
        "integrity": _integrity(target_path),
    }


def restore_database(source: str | Path, target: str | Path, *, overwrite: bool = False) -> dict[str, Any]:
    source_path = Path(source).expanduser().resolve(strict=True)
    target_path = Path(target).expanduser().resolve(strict=False)
    if source_path == target_path:
        raise ValueError("restore source and target must differ")
    if target_path.exists() and not overwrite:
        raise FileExistsError(f"restore target exists; pass overwrite explicitly: {target_path}")
    target_path.parent.mkdir(parents=True, exist_ok=True)
    with closing(sqlite3.connect(source_path)) as source_connection:
        with closing(sqlite3.connect(target_path)) as target_connection:
            source_connection.backup(target_connection)
            target_connection.commit()
    return {
        "source": str(source_path),
        "target": str(target_path),
        "bytes": target_path.stat().st_size,
        "sha256": _sha256(target_path),
        "integrity": _integrity(target_path),
    }


def _integrity(path: Path) -> str:
    with closing(sqlite3.connect(path)) as connection:
        value = connection.execute("PRAGMA integrity_check").fetchone()[0]
    return str(value)


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1_048_576), b""):
            digest.update(chunk)
    return digest.hexdigest()


def purge_events(
    store: EventStore,
    *,
    before: str | None = None,
    event_ids: Iterable[str] = (),
    confirm: bool = False,
) -> dict[str, Any]:
    """Physically delete selected telemetry with an append-only audit record.

    The application append path cannot invoke this function.  Callers must
    explicitly confirm, and the immutable evidence triggers are removed only
    inside the same SQLite transaction as the deletion and immediately
    recreated before commit.
    """

    if not confirm:
        raise ValueError("purge requires explicit confirm=True")
    ids = [value.strip() for value in event_ids if isinstance(value, str) and value.strip()]
    clauses: list[str] = []
    params: list[str] = []
    if before is not None:
        clauses.append("observed_at < ?")
        params.append(before)
    if ids:
        placeholders = ",".join("?" for _ in ids)
        clauses.append(f"event_id IN ({placeholders})")
        params.extend(ids)
    if not clauses:
        raise ValueError("purge requires before or at least one event id")
    rows = store.connection.execute(
        f"SELECT event_id FROM events WHERE {' OR '.join(clauses)} ORDER BY event_id",
        params,
    ).fetchall()
    selected = [str(row["event_id"]) for row in rows]
    action_id = f"maintenance:{uuid.uuid4()}"
    requested_at = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
    selector = json.dumps({"before": before, "event_ids": ids}, sort_keys=True)
    try:
        with store.connection:
            store.connection.execute(
                "INSERT INTO maintenance_actions(action_id, action_type, selector_json, requested_at, outcome, affected_events, detail) VALUES (?, ?, ?, ?, ?, ?, ?)",
                (action_id, "purge", selector, requested_at, "started", len(selected), None),
            )
            for trigger in _EVIDENCE_TRIGGERS:
                store.connection.execute(f"DROP TRIGGER IF EXISTS {trigger}")
            if selected:
                placeholders = ",".join("?" for _ in selected)
                store.connection.execute(
                    f"DELETE FROM attribution_edges WHERE child_event_id IN ({placeholders}) OR parent_event_id IN ({placeholders})",
                    (*selected, *selected),
                )
                for table, column in (
                    ("measurement_facts", "event_id"),
                    ("outcome_events", "event_id"),
                    ("ingest_ledger", "event_id"),
                    ("event_conflicts", "event_id"),
                    ("events", "event_id"),
                ):
                    store.connection.execute(f"DELETE FROM {table} WHERE {column} IN ({placeholders})", selected)
            _restore_evidence_triggers(store.connection)
            completed_at = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
            store.connection.execute(
                "INSERT INTO maintenance_actions(action_id, action_type, selector_json, requested_at, completed_at, outcome, affected_events, detail) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                (f"{action_id}:completed", "purge", selector, requested_at, completed_at, "completed", len(selected), "explicit operator retention/deletion"),
            )
    except Exception:
        # DDL is transactional on SQLite, but restore the guards defensively
        # before handing the error to the caller.
        _restore_evidence_triggers(store.connection)
        raise
    return {"action_id": action_id, "outcome": "completed", "affected_events": len(selected), "selector": json.loads(selector)}
