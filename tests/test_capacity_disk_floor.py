"""A byte budget is a policy, not a promise the disk has the space.

Found live: the deployment's configured `max_database_bytes` was 17.18 GB while
the volume had 17.0 GB free. Honouring that budget literally would have filled
the system disk. `doctor` does compare free space against the module default
(`cli.py:915-918`), but that check is `"blocking": False` and only runs when an
operator invokes it -- nothing on the append path consulted the disk at all, so
the store would have grown until the machine had no space left.

Telemetry storage is never worth taking the host down. Clamping in `capacity()`
makes the intake gate, the observation verdict and `/healthz` honest at once:
the store degrades and says so instead of consuming the last free byte.
"""

import tempfile
import unittest
from pathlib import Path

import observatory.store as store_module
from observatory.store import MIN_FREE_DISK_BYTES, EventStore

GB = 1024 ** 3


class _FakeUsage:
    def __init__(self, free):
        self.free = free
        self.total = free
        self.used = 0


class CapacityDiskFloorTests(unittest.TestCase):
    def setUp(self):
        self._dir = tempfile.TemporaryDirectory()
        self.addCleanup(self._dir.cleanup)
        self.path = Path(self._dir.name) / "events.sqlite3"
        self._real_usage = store_module.shutil.disk_usage
        self.addCleanup(setattr, store_module.shutil, "disk_usage", self._real_usage)

    def _store(self, *, max_bytes):
        store = EventStore(self.path, max_bytes=max_bytes)
        self.addCleanup(store.close)
        return store

    def _with_free(self, free_bytes):
        store_module.shutil.disk_usage = lambda path: _FakeUsage(free_bytes)

    def test_a_budget_larger_than_the_disk_is_clamped_to_the_disk(self):
        # The live shape: budget 17.18 GB, volume with far less free.
        self._with_free(4 * GB)
        store = self._store(max_bytes=17 * GB)
        capacity = store.capacity()
        self.assertTrue(capacity["disk_limited"])
        self.assertLess(capacity["max_bytes"], 17 * GB)
        self.assertEqual(capacity["configured_max_bytes"], 17 * GB)
        # Growth is allowed only down to the floor.
        self.assertEqual(capacity["max_bytes"], capacity["bytes"] + 4 * GB - MIN_FREE_DISK_BYTES)

    def test_a_budget_the_disk_can_honour_is_left_alone(self):
        self._with_free(500 * GB)
        capacity = self._store(max_bytes=2 * GB).capacity()
        self.assertFalse(capacity["disk_limited"])
        self.assertEqual(capacity["max_bytes"], 2 * GB)

    def test_the_floor_is_never_crossed_even_with_no_configured_budget(self):
        # An unbounded budget must still not be allowed to fill the volume.
        self._with_free(3 * GB)
        capacity = self._store(max_bytes=None).capacity()
        self.assertTrue(capacity["disk_limited"])
        self.assertIsNotNone(capacity["max_bytes"])
        self.assertEqual(capacity["max_bytes"], capacity["bytes"] + 3 * GB - MIN_FREE_DISK_BYTES)

    def test_a_volume_already_below_the_floor_reports_exhausted(self):
        # Nothing more may be written; the honest answer is "full", not a
        # negative ceiling or a crash.
        self._with_free(MIN_FREE_DISK_BYTES // 2)
        capacity = self._store(max_bytes=100 * GB).capacity()
        self.assertTrue(capacity["exhausted"])
        self.assertGreaterEqual(capacity["max_bytes"], 0)
        self.assertIsNotNone(capacity["ratio"])

    def test_an_unreadable_volume_falls_back_to_the_configured_budget(self):
        # A failure to stat the disk must not make the store unusable.
        def _boom(path):
            raise OSError("volume unavailable")

        store_module.shutil.disk_usage = _boom
        capacity = self._store(max_bytes=2 * GB).capacity()
        self.assertFalse(capacity["disk_limited"])
        self.assertEqual(capacity["max_bytes"], 2 * GB)
        self.assertIsNone(capacity["disk_free_bytes"])

    def test_the_existing_capacity_contract_is_preserved(self):
        # Callers read these keys; clamping must not change their meaning.
        self._with_free(500 * GB)
        capacity = self._store(max_bytes=2 * GB).capacity()
        for key in ("bytes", "max_bytes", "ratio", "exhausted"):
            self.assertIn(key, capacity)
        self.assertIsInstance(capacity["exhausted"], bool)
        self.assertAlmostEqual(capacity["ratio"], capacity["bytes"] / capacity["max_bytes"])


if __name__ == "__main__":
    unittest.main()
