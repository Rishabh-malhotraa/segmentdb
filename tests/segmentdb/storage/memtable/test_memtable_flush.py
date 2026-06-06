"""Integration tests: memtable flush → SSTable + manifest."""

import time
from pathlib import Path

import pytest
from sortedcontainers import SortedDict

from segmentdb.storage.manifest import Manifest, SSTableMeta
from segmentdb.storage.memtable import FlushTask, Memtable, MemtableEntry
from segmentdb.storage.sstable.SSTableReader import SSTableReader
from segmentdb.storage.sstable.SSTableWriter import SSTableWriter


# ------------------------------------------------------------------ #
# Helpers
# ------------------------------------------------------------------ #


def _build_flush_task(n: int = 100, seq_start: int = 1) -> FlushTask:
    """Create a FlushTask with *n* sequential key-value pairs."""
    store = SortedDict()
    for i in range(n):
        key = f"key-{i:06d}".encode()
        val = f"val-{i:06d}".encode()
        store[key] = MemtableEntry(seq_no=seq_start + i, value=val)
    return FlushTask(store=store, checkpoint_seq_no=seq_start + n - 1)


# ------------------------------------------------------------------ #
# SSTableWriter + Manifest wiring (unit-level, no Memtable thread)
# ------------------------------------------------------------------ #


class TestFlushProducesValidSSTable:
    """Verify that flushing a store produces an SSTable readable by SSTableReader."""

    def test_sstable_created_on_disk(self, tmp_path: Path):
        task = _build_flush_task(50)
        path = tmp_path / "sst-000001.sst"

        SSTableWriter(1, task.store, level=0).write(path)

        assert path.exists()
        assert path.stat().st_size > 0

    def test_all_keys_readable(self, tmp_path: Path):
        task = _build_flush_task(200)
        path = tmp_path / "sst-000001.sst"

        SSTableWriter(1, task.store, level=0).write(path)

        reader = SSTableReader(str(path))
        try:
            for key, entry in task.store.items():
                assert reader.get(key) == entry.value
        finally:
            reader.close()

    def test_tombstones_readable(self, tmp_path: Path):
        store = SortedDict()
        store[b"alive"] = MemtableEntry(seq_no=1, value=b"v")
        store[b"dead"] = MemtableEntry(seq_no=2, value=None)  # tombstone
        task = FlushTask(store=store, checkpoint_seq_no=2)

        path = tmp_path / "sst-000001.sst"
        SSTableWriter(1, task.store, level=0).write(path)

        reader = SSTableReader(str(path))
        try:
            assert reader.get(b"alive") == b"v"
            # Tombstone: SSTableReader.get returns None (same as not-found)
            assert reader.get(b"dead") is None
        finally:
            reader.close()

    def test_missing_key_returns_none(self, tmp_path: Path):
        task = _build_flush_task(10)
        path = tmp_path / "sst-000001.sst"
        SSTableWriter(1, task.store, level=0).write(path)

        reader = SSTableReader(str(path))
        try:
            assert reader.get(b"nonexistent") is None
        finally:
            reader.close()


class TestFlushUpdatesManifest:
    """Verify that the manifest is correctly updated after a flush."""

    def _do_flush(self, data_dir: Path, task: FlushTask) -> SSTableMeta:
        """Simulate the flush callback logic from Database._flush_to_sstable."""
        sst_dir = data_dir / "sstables"
        sst_dir.mkdir(parents=True, exist_ok=True)

        manifest = Manifest.load(data_dir)
        sst_id = manifest.allocate_id()
        path = sst_dir / f"sst-{sst_id:06d}.sst"

        meta = SSTableWriter(sst_id, task.store, level=0).write(path)

        manifest.add_sstable(meta)
        manifest.save(data_dir)
        return meta

    def test_manifest_has_one_entry(self, tmp_path: Path):
        task = _build_flush_task(50)
        self._do_flush(tmp_path, task)

        manifest = Manifest.load(tmp_path)
        assert len(manifest.sstables) == 1

    def test_manifest_metadata_correct(self, tmp_path: Path):
        task = _build_flush_task(50, seq_start=10)
        meta = self._do_flush(tmp_path, task)

        assert meta.level == 0
        assert meta.min_key == b"key-000000"
        assert meta.max_key == b"key-000049"
        assert meta.min_seq_no == 10
        assert meta.max_seq_no == 59
        assert meta.entry_count == 50
        assert meta.file_size > 0

    def test_manifest_persists_to_disk(self, tmp_path: Path):
        task = _build_flush_task(20)
        self._do_flush(tmp_path, task)

        # Reload from disk — should survive restart
        reloaded = Manifest.load(tmp_path)
        assert len(reloaded.sstables) == 1
        assert reloaded.sstables[0].entry_count == 20

    def test_multiple_flushes_unique_ids(self, tmp_path: Path):
        for i in range(3):
            task = _build_flush_task(10, seq_start=i * 10 + 1)
            self._do_flush(tmp_path, task)

        manifest = Manifest.load(tmp_path)
        ids = [m.id for m in manifest.sstables]
        assert len(ids) == 3
        assert len(set(ids)) == 3  # all unique


class TestDatabaseFlushIntegration:
    """End-to-end: Database wires memtable → SSTable → manifest."""

    def test_flush_triggered_by_size(self, tmp_path: Path):
        """Fill memtable past threshold, verify SSTable + manifest on disk."""
        from segmentdb.db import Database

        db = Database(str(tmp_path))
        # Lower threshold so the test doesn't need 100MB of data
        db._memtable.MAX_SIZE_BYTES = 4 * 1024  # 4KB

        try:
            # Write enough to trigger at least one flush
            for i in range(500):
                key = f"k-{i:06d}".encode()
                entry = MemtableEntry(seq_no=i + 1, value=b"x" * 100)
                db._memtable.put(key, entry)
        finally:
            db.close()

        # Verify SSTables written
        sst_dir = tmp_path / "sstables"
        sst_files = list(sst_dir.glob("*.sst"))
        assert len(sst_files) >= 1

        # Verify manifest updated
        manifest = Manifest.load(tmp_path)
        assert len(manifest.sstables) >= 1
        assert all(m.level == 0 for m in manifest.sstables)

    def test_data_readable_after_flush(self, tmp_path: Path):
        """Keys written before flush are readable from the SSTable."""
        from segmentdb.db import Database

        db = Database(str(tmp_path))
        db._memtable.MAX_SIZE_BYTES = 4 * 1024

        written_keys: dict[bytes, bytes] = {}
        try:
            for i in range(200):
                key = f"k-{i:06d}".encode()
                val = f"v-{i:06d}".encode()
                entry = MemtableEntry(seq_no=i + 1, value=val)
                db._memtable.put(key, entry)
                written_keys[key] = val
        finally:
            db.close()

        # Read all SSTables and verify keys
        sst_dir = tmp_path / "sstables"
        found: dict[bytes, bytes] = {}
        for sst_file in sst_dir.glob("*.sst"):
            reader = SSTableReader(str(sst_file))
            try:
                for key, val in written_keys.items():
                    result = reader.get(key)
                    if result is not None:
                        found[key] = result
            finally:
                reader.close()

        # Some keys may still be in the active memtable (not flushed),
        # but flushed keys must be correct.
        for key, val in found.items():
            assert val == written_keys[key]
