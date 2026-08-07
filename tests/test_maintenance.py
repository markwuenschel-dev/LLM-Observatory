from pathlib import Path
import sqlite3
import tempfile
import unittest

from observatory.maintenance import backup_database, purge_events, restore_database, schema_versions
from observatory.contracts import NormalizedEvent
from observatory.store import EventStore

from tests.test_store import make_event


class MaintenanceTests(unittest.TestCase):
    def test_migrations_backup_restore_and_explicit_purge(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            source = Path(temp) / "events.sqlite3"
            backup = Path(temp) / "backup.sqlite3"
            restored = Path(temp) / "restored.sqlite3"
            with EventStore(source) as store:
                store.append(make_event("maint-1"))
                self.assertIn("003_maintenance", schema_versions(store))
                self.assertEqual(purge_events(store, event_ids=["maint-1"], confirm=True)["affected_events"], 1)
                self.assertIsNone(store.get("maint-1"))
                self.assertEqual(store.connection.execute("SELECT COUNT(*) FROM maintenance_actions").fetchone()[0], 2)
                store.append(make_event("maint-2"))
                with self.assertRaises(sqlite3.IntegrityError):
                    store.connection.execute("DELETE FROM ingest_ledger")
            backup_result = backup_database(source, backup)
            self.assertEqual(backup_result["integrity"], "ok")
            self.assertEqual(len(backup_result["sha256"]), 64)
            self.assertEqual(restore_database(backup, restored)["integrity"], "ok")
            with EventStore(restored) as store:
                self.assertIsNotNone(store.get("maint-2"))

    def test_purge_requires_explicit_confirmation_and_selector(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            with EventStore(Path(temp) / "events.sqlite3") as store:
                with self.assertRaises(ValueError):
                    purge_events(store, event_ids=["missing"], confirm=False)
                with self.assertRaises(ValueError):
                    purge_events(store, confirm=True)

    def test_purge_removes_edges_to_deleted_parent_events(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            with EventStore(Path(temp) / "events.sqlite3") as store:
                parent = make_event("parent-1")
                child_value = make_event("child-1").to_mapping()
                child_value["execution"] = {"parent_event_id": "parent-1"}
                child = NormalizedEvent.from_mapping(child_value)
                store.append(parent)
                store.append(child)
                self.assertTrue(store.attribution_edges(event_id="child-1"))
                purge_events(store, event_ids=["parent-1"], confirm=True)
                self.assertFalse(any(item["relation"] == "parent_event" for item in store.attribution_edges(event_id="child-1")))


if __name__ == "__main__":
    unittest.main()
