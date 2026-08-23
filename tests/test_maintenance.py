from pathlib import Path
import io
import sqlite3
import tempfile
import json
import tarfile
from types import SimpleNamespace
import unittest
import zipfile

from contextlib import redirect_stderr, redirect_stdout
from datetime import datetime, timedelta, timezone

from observatory.cli import EXIT_DEGRADED, main as cli_main
from observatory.maintenance import BACKEND_VOLUME_KEYS, DEFAULT_RETENTION_TARGET_RATIO, backup_database, backup_state, enforce_normalized_retention, inspect_backend_volume_capacity, purge_events, resolve_backend_volumes, restore_database, restore_state, schema_versions
from observatory.contracts import NormalizedEvent
from observatory.store import EventStore

from tests.test_store import make_event


class MaintenanceTests(unittest.TestCase):
    def test_compose_backend_volume_resolution_uses_rendered_names(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            compose = Path(temp) / "compose.yaml"
            env = Path(temp) / "compose.env"
            compose.write_text("name: ignored\n", encoding="utf-8")
            env.write_text("OBSERVATORY_STATE_DIR=state\n", encoding="utf-8")
            rendered = {
                "name": "observatory-test",
                "volumes": {
                    logical: {"name": f"observatory-test_{logical}"}
                    for logical in BACKEND_VOLUME_KEYS
                },
            }

            def runner(command, **_kwargs):
                return SimpleNamespace(returncode=0, stdout=json.dumps(rendered), stderr="")

            result = resolve_backend_volumes(compose, env, runner=runner)
            self.assertEqual(result["tempo-data"], "observatory-test_tempo-data")

    def test_backend_volume_capacity_is_scoped_to_resolved_volumes_and_parses_docker_sizes(self) -> None:
        volume_map = {logical: f"observatory-test_{logical}" for logical in BACKEND_VOLUME_KEYS}
        report = {
            "Volumes": [
                {"Name": volume_map[logical], "Size": "1MB", "Links": "1"}
                for logical in BACKEND_VOLUME_KEYS
            ] + [{"Name": "unrelated_volume", "Size": "900GB", "Links": "0"}],
        }

        def runner(command, **_kwargs):
            self.assertEqual(command, ["docker", "system", "df", "-v", "--format", "{{json .}}"])
            return SimpleNamespace(returncode=0, stdout=json.dumps(report), stderr="")

        result = inspect_backend_volume_capacity(volume_map, max_bytes=10 * 1024 * 1024, runner=runner)
        self.assertEqual(result["status"], "pass")
        self.assertEqual(result["bytes"], 5_000_000)
        self.assertEqual(result["missing"], [])
        self.assertEqual(len(result["volumes"]), len(BACKEND_VOLUME_KEYS))

        over_budget = inspect_backend_volume_capacity(volume_map, max_bytes=4_000_000, runner=runner)
        self.assertEqual(over_budget["status"], "fail")

    def test_backend_volume_capacity_reports_missing_volumes_without_failing_inference(self) -> None:
        volume_map = {logical: f"observatory-test_{logical}" for logical in BACKEND_VOLUME_KEYS}
        report = {"Volumes": [{"Name": volume_map["tempo-data"], "Size": "2KiB", "Links": "1"}]}

        def runner(command, **_kwargs):
            return SimpleNamespace(returncode=0, stdout=json.dumps(report), stderr="")

        result = inspect_backend_volume_capacity(volume_map, max_bytes=1024 * 1024, runner=runner)
        self.assertEqual(result["status"], "warn")
        self.assertEqual(set(result["missing"]), set(BACKEND_VOLUME_KEYS) - {"tempo-data"})
        self.assertEqual(result["bytes"], 2 * 1024)

    def test_full_state_backend_volume_bundle_round_trip_is_manifest_verified(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp) / "state"
            with EventStore(root / "data" / "events.sqlite3") as store:
                store.append(make_event("backend-backup-1"))
            (root / "config.json").write_text('{"schema_version":"1.0"}\n', encoding="utf-8")
            (root / "secrets").mkdir(parents=True)
            (root / "secrets" / "grafana_admin_password").write_text("test-secret\n", encoding="utf-8")
            archive = Path(temp) / "full-state.zip"
            volume_map = {logical: f"observatory-test_{logical}" for logical in BACKEND_VOLUME_KEYS}
            calls: list[list[str]] = []

            def runner(command, **_kwargs):
                calls.append(command)
                if command[1:3] == ["volume", "inspect"]:
                    return SimpleNamespace(returncode=0, stdout="[]", stderr="")
                if command[1] == "run":
                    bind = next(item for item in command if item.startswith("type=bind,source="))
                    staging = Path(bind.split("source=", 1)[1].split(",target=", 1)[0])
                    logical = command[-1].split("/backup/", 1)[1].split(".tar.gz", 1)[0]
                    staging.mkdir(parents=True, exist_ok=True)
                    with tarfile.open(staging / f"{logical}.tar.gz", mode="w:gz") as volume_archive:
                        marker = tarfile.TarInfo("marker.txt")
                        payload = f"{logical}\n".encode("utf-8")
                        marker.size = len(payload)
                        volume_archive.addfile(marker, fileobj=io.BytesIO(payload))
                return SimpleNamespace(returncode=0, stdout="", stderr="")

            result = backup_state(root, archive, include_secret=True, backend_volumes=volume_map, docker_runner=runner)
            self.assertEqual(result["docker_named_volumes"], "included")
            self.assertEqual(result["backend_volumes"], sorted(BACKEND_VOLUME_KEYS))
            with zipfile.ZipFile(archive) as bundle:
                manifest = json.loads(bundle.read("manifest.json"))
                self.assertEqual(manifest["scope"], "host_state_and_backend_volumes")
                self.assertEqual(set(manifest["backend_volumes"]), set(BACKEND_VOLUME_KEYS))

            restored_root = Path(temp) / "restored"
            restored_root.mkdir()
            def restore_runner(command, **_kwargs):
                calls.append(command)
                if command[1:3] == ["volume", "inspect"]:
                    return SimpleNamespace(returncode=1, stdout="", stderr="No such volume")
                return SimpleNamespace(returncode=0, stdout="", stderr="")

            restored = restore_state(
                archive,
                restored_root,
                overwrite=True,
                restore_secret=True,
                backend_volumes=volume_map,
                docker_runner=restore_runner,
            )
            self.assertEqual(set(restored["restored_backend_volumes"]), set(BACKEND_VOLUME_KEYS))
            self.assertTrue(any(command[1:3] == ["volume", "create"] for command in calls))

            host_only_restore = Path(temp) / "host-only-restore"
            host_only_restore.mkdir()
            with self.assertRaises(ValueError):
                restore_state(archive, host_only_restore, overwrite=True)

    def test_full_state_restore_rolls_back_host_files_when_backend_restore_fails(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp) / "state"
            with EventStore(root / "data" / "events.sqlite3") as store:
                store.append(make_event("rollback-source-1"))
            (root / "config.json").write_text('{"before":true}\n', encoding="utf-8")
            (root / "secrets").mkdir(parents=True)
            (root / "secrets" / "grafana_admin_password").write_text("before-secret\n", encoding="utf-8")
            archive = Path(temp) / "rollback.zip"
            volume_map = {logical: f"observatory-test_{logical}" for logical in BACKEND_VOLUME_KEYS}

            def backup_runner(command, **_kwargs):
                if command[1:3] == ["volume", "inspect"]:
                    return SimpleNamespace(returncode=0, stdout="[]", stderr="")
                if command[1] == "run":
                    bind = next(item for item in command if item.startswith("type=bind,source="))
                    staging = Path(bind.split("source=", 1)[1].split(",target=", 1)[0])
                    logical = command[-1].split("/backup/", 1)[1].split(".tar.gz", 1)[0]
                    staging.mkdir(parents=True, exist_ok=True)
                    with tarfile.open(staging / f"{logical}.tar.gz", mode="w:gz") as volume_archive:
                        marker = tarfile.TarInfo("before.txt")
                        payload = f"before-{logical}\n".encode("utf-8")
                        marker.size = len(payload)
                        volume_archive.addfile(marker, fileobj=io.BytesIO(payload))
                return SimpleNamespace(returncode=0, stdout="", stderr="")

            backup_state(root, archive, include_secret=True, backend_volumes=volume_map, docker_runner=backup_runner)
            (root / "config.json").write_text('{"current":true}\n', encoding="utf-8")
            original_config = (root / "config.json").read_text(encoding="utf-8")

            failed_once = False

            def failing_restore_runner(command, **_kwargs):
                nonlocal failed_once
                if command[1:3] == ["volume", "inspect"]:
                    return SimpleNamespace(returncode=0, stdout="[]", stderr="")
                if command[1] == "run" and not failed_once and any("target=/target" in item for item in command) and "tempo-data" in command[-1]:
                    failed_once = True
                    return SimpleNamespace(returncode=1, stdout="", stderr="simulated restore failure")
                if command[1] == "run":
                    return backup_runner(command, **_kwargs)
                return SimpleNamespace(returncode=0, stdout="", stderr="")

            with self.assertRaises(RuntimeError) as context:
                restore_state(
                    archive,
                    root,
                    overwrite=True,
                    restore_secret=True,
                    backend_volumes=volume_map,
                    docker_runner=failing_restore_runner,
                )
            self.assertIn("rolled back", str(context.exception))
            self.assertEqual((root / "config.json").read_text(encoding="utf-8"), original_config)
            with EventStore(root / "data" / "events.sqlite3") as store:
                self.assertIsNotNone(store.get("rollback-source-1"))

    def test_migrations_backup_restore_and_explicit_purge(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            source = Path(temp) / "events.sqlite3"
            backup = Path(temp) / "backup.sqlite3"
            restored = Path(temp) / "restored.sqlite3"
            with EventStore(source) as store:
                store.append(make_event("maint-1"))
                self.assertIn("003_maintenance", schema_versions(store))
                purge_result = purge_events(store, event_ids=["maint-1"], confirm=True)
                self.assertEqual(purge_result["affected_events"], 1)
                # A one-event purge must not rewrite the whole database; the
                # threshold exists so a scheduled sweep cannot starve intake.
                self.assertEqual(purge_result["compaction"]["status"], "skipped")
                self.assertIsNone(store.get("maint-1"))
                self.assertEqual(store.connection.execute("SELECT COUNT(*) FROM maintenance_actions").fetchone()[0], 2)
                store.append(make_event("maint-2"))
                with self.assertRaises(sqlite3.IntegrityError):
                    store.connection.execute("DELETE FROM ingest_ledger")
            backup_result = backup_database(source, backup)
            self.assertEqual(backup_result["integrity"], "ok")
            with self.assertRaises(FileExistsError):
                backup_database(source, backup)
            backup_database(source, backup, overwrite=True)
            self.assertEqual(len(backup_result["sha256"]), 64)
            self.assertEqual(restore_database(backup, restored)["integrity"], "ok")
            with EventStore(restored) as store:
                self.assertIsNotNone(store.get("maint-2"))

    def test_host_state_backup_round_trip_is_checksum_verified_and_secret_opt_in(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp) / "state"
            root.mkdir()
            with EventStore(root / "data" / "events.sqlite3") as store:
                store.append(make_event("state-backup-1"))
            (root / "config.json").write_text('{"schema_version":"1.0"}\n', encoding="utf-8")
            (root / "compose.env").write_text("OBSERVATORY_STATE_DIR=state\n", encoding="utf-8")
            (root / "spool").mkdir()
            (root / "spool" / "events-1.jsonl").write_text('{"event_id":"spooled"}\n', encoding="utf-8")
            (root / "secrets").mkdir()
            (root / "secrets" / "grafana_admin_password").write_text("do-not-include-by-default\n", encoding="utf-8")
            archive = Path(temp) / "state-backup.zip"
            result = backup_state(root, archive)
            self.assertEqual(result["schema"], "observatory.state-backup/v1")
            self.assertFalse(result["secret_included"])
            restored_root = Path(temp) / "restored"
            restored_root.mkdir()
            restored = restore_state(archive, restored_root, overwrite=True)
            self.assertIn("data/events.sqlite3", restored["restored"])
            self.assertFalse(restored["secret_restored"])
            with EventStore(restored_root / "data" / "events.sqlite3") as store:
                self.assertIsNotNone(store.get("state-backup-1"))
            self.assertFalse((restored_root / "secrets" / "grafana_admin_password").exists())

    def test_host_state_backup_rejects_archive_inside_state_root(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp) / "state"
            with EventStore(root / "data" / "events.sqlite3"):
                pass
            (root / "config.json").write_text('{"schema_version":"1.0"}\n', encoding="utf-8")
            with self.assertRaises(ValueError):
                backup_state(root, root / "backup.zip")

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

    def test_purge_before_normalizes_offsets_and_rejects_malformed_time(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            with EventStore(Path(temp) / "events.sqlite3") as store:
                store.append(make_event("time-1"))
                result = purge_events(store, before="2026-08-07T10:00:02-04:00", confirm=True)
                self.assertEqual(result["affected_events"], 1)
                with self.assertRaises(ValueError):
                    purge_events(store, before="not-a-timestamp", confirm=True)



class PurgeCompactionTests(unittest.TestCase):
    """A purge must not transiently inflate the measured store size.

    VACUUM rebuilds the database through the WAL, and `storage_bytes()` counts
    the WAL, so without a checkpoint afterwards the measurement roughly doubles
    right after a purge. Near the budget that makes the capacity guard reject
    intake -- an outage caused by the operation meant to prevent one.
    """

    def test_storage_shrinks_after_a_purge_rather_than_ballooning(self):
        import tempfile
        from datetime import datetime, timedelta, timezone
        from pathlib import Path as _Path

        from observatory.contracts import NormalizedEvent
        from observatory.maintenance import purge_events
        from observatory.store import EventStore

        with tempfile.TemporaryDirectory() as temp:
            store = EventStore(_Path(temp) / "events.sqlite3")
            try:
                old = (datetime.now(timezone.utc) - timedelta(days=90)).isoformat()
                for index in range(200):
                    store.append(
                        NormalizedEvent.from_mapping(
                            {
                                "schema_version": "1.0",
                                "event_id": f"old-{index}",
                                "event_type": "model.operation",
                                "observed_at": old,
                                "received_at": old,
                                "project": {"project_id": "repo_sha256:abc"},
                                "llm": {"provider": "anthropic", "model": "claude-opus-5", "client": "claude-code"},
                                "attributes": {"filler": "x" * 2000},
                            }
                        )
                    )
                before = store.storage_bytes()
                cutoff = (datetime.now(timezone.utc) - timedelta(days=30)).isoformat()
                result = purge_events(store, before=cutoff, confirm=True, compact=True)
                after = store.storage_bytes()

                self.assertEqual(result["affected_events"], 200)
                self.assertEqual(result["compaction"]["status"], "completed")
                # The whole point: measured size must not grow after a purge.
                self.assertLessEqual(after, before)
                wal = _Path(f"{store.path}-wal")
                if wal.exists():
                    self.assertLess(wal.stat().st_size, before)
            finally:
                store.close()


class RetentionMeasurementTests(unittest.TestCase):
    """Retention must decide from a truthful size, and report honestly.

    ``storage_bytes()`` counts the ``-wal`` sidecar, and the read plane keeps
    passive checkpoints from resetting it, so the inflated reading is the
    normal one. Sizing a purge from it deletes telemetry that never needed
    deleting -- and this runs unattended every 15 minutes.
    """

    @staticmethod
    def _seed(store, count: int, *, filler: int = 2000) -> None:
        base = datetime(2026, 8, 23, 12, 0, tzinfo=timezone.utc)
        for index in range(count):
            stamp = (base - timedelta(minutes=count - index)).isoformat()
            store.append(
                NormalizedEvent.from_mapping(
                    {
                        "schema_version": "1.0",
                        "event_id": f"retain-{index}",
                        "event_type": "model.operation",
                        "observed_at": stamp,
                        "received_at": stamp,
                        "project": {"project_id": "repo_sha256:abc"},
                        "llm": {"provider": "anthropic", "model": "claude-opus-5", "client": "claude-code"},
                        "attributes": {"filler": "x" * filler},
                    }
                )
            )

    @staticmethod
    def _durable_bytes(store) -> int:
        """The page image the store actually holds, WAL sidecar excluded."""

        page_count = int(store.connection.execute("PRAGMA page_count").fetchone()[0])
        page_size = int(store.connection.execute("PRAGMA page_size").fetchone()[0])
        return page_count * page_size

    @staticmethod
    def _budget_for(target_bytes: int) -> int:
        return int(target_bytes / DEFAULT_RETENTION_TARGET_RATIO) + 1

    def test_wal_inflation_alone_never_deletes_telemetry(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            with EventStore(Path(temp) / "events.sqlite3") as store:
                self._seed(store, 300)
                inflated = store.storage_bytes()
                durable = self._durable_bytes(store)
                # Precondition: the uncheckpointed reading is the misleading one.
                self.assertGreater(inflated, durable * 2)

                # A budget the store already satisfies once the WAL is folded in,
                # but that the inflated reading blows straight through.
                max_bytes = self._budget_for(durable + 128 * 1024)
                self.assertLess(int(max_bytes * DEFAULT_RETENTION_TARGET_RATIO), inflated)

                report = enforce_normalized_retention(store, max_bytes=max_bytes)
                remaining = int(store.connection.execute("SELECT COUNT(*) FROM events").fetchone()[0])

                self.assertEqual(remaining, 300)
                self.assertEqual(report["deleted_events"], 0)
                self.assertEqual(report["outcome"], "satisfied")
                self.assertEqual(report["reasons"], [])

    def test_a_checkpoint_blocked_by_a_reader_still_does_not_over_delete(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            path = Path(temp) / "events.sqlite3"
            with EventStore(path, busy_timeout_ms=50) as store:
                self._seed(store, 300)
                durable = self._durable_bytes(store)
                reader = sqlite3.connect(path, timeout=0.1)
                try:
                    reader.execute("BEGIN")
                    reader.execute("SELECT COUNT(*) FROM events").fetchone()
                    # Precondition: the dashboard read plane blocks the truncate.
                    busy = store.connection.execute("PRAGMA wal_checkpoint(TRUNCATE)").fetchone()
                    self.assertNotEqual(int(busy[0]), 0)
                    self.assertGreater(store.storage_bytes(), durable * 2)

                    max_bytes = self._budget_for(durable + 128 * 1024)
                    report = enforce_normalized_retention(store, max_bytes=max_bytes)
                    remaining = int(store.connection.execute("SELECT COUNT(*) FROM events").fetchone()[0])
                finally:
                    reader.rollback()
                    reader.close()

                self.assertEqual(remaining, 300)
                self.assertEqual(report["deleted_events"], 0)
                self.assertFalse(report["measurement"]["checkpointed"])

    def test_still_over_budget_after_the_purge_is_not_reported_as_enforced(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            with EventStore(Path(temp) / "events.sqlite3") as store:
                self._seed(store, 400)
                report = enforce_normalized_retention(store, max_bytes=200_000)

                self.assertGreater(report["deleted_events"], 0)
                self.assertGreater(report["bytes_after"], report["target_bytes"])
                self.assertEqual(report["outcome"], "ineffective")
                self.assertIn("still", report["blocker"])

    def test_both_return_paths_share_one_shape(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            with EventStore(Path(temp) / "events.sqlite3") as store:
                self._seed(store, 40)
                satisfied = enforce_normalized_retention(store, max_bytes=8 * 1024 * 1024 * 1024)
                acted = enforce_normalized_retention(store, max_bytes=200_000)

                self.assertEqual(satisfied["outcome"], "satisfied")
                self.assertNotEqual(acted["outcome"], "satisfied")
                self.assertEqual(sorted(satisfied), sorted(acted))
                for key in ("blocker", "action_id", "compaction", "measurement"):
                    self.assertIn(key, satisfied)


class RetentionCliHonestyTests(unittest.TestCase):
    """``observatory retention --enforce`` printed success and exited 0 while
    the store stayed far over budget: only a ``--json`` reader could see it."""

    def _run(self, arguments: list[str]) -> tuple[int, dict]:
        output = io.StringIO()
        errors = io.StringIO()
        with redirect_stdout(output), redirect_stderr(errors):
            code = cli_main(["--json", *arguments])
        return code, json.loads(output.getvalue())

    def test_ineffective_enforcement_is_visible_at_the_process_level(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            self._run(["--state-dir", temp, "install"])
            database = Path(temp) / "data" / "events.sqlite3"
            with EventStore(database, max_bytes=None) as store:
                RetentionMeasurementTests._seed(store, 400)

            code, result = self._run([
                "--state-dir", temp, "retention", "--enforce", "--max-database-bytes", "200000",
            ])

            self.assertEqual(result["data"]["enforcement"]["outcome"], "ineffective")
            self.assertEqual(result["outcome"], "degraded")
            self.assertEqual(code, EXIT_DEGRADED)
            self.assertTrue(any("still" in warning for warning in result["warnings"]))

if __name__ == "__main__":
    unittest.main()
