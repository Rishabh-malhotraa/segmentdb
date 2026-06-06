"""Main Database interface for SegmentDB."""

from pathlib import Path
from types import TracebackType
from typing import Optional, Type

from segmentdb.storage.manifest import Manifest
from segmentdb.storage.memtable import FlushTask, Memtable, MemtableEntry
from segmentdb.storage.sstable.SSTableWriter import SSTableWriter


class Database:
    """
    SegmentDB - A segment-based key-value store with Write-Ahead Logging.

    Usage:
        with Database("/path/to/data") as db:
            db.put(b"key", b"value")
            value = db.get(b"key")
            db.delete(b"key")
        # Automatically closed and flushed
    """

    SSTABLE_DIR_NAME = "sstables"

    def __init__(self, data_dir: str):
        self.data_dir = Path(data_dir)
        self._sstable_dir = self.data_dir / self.SSTABLE_DIR_NAME
        self._sstable_dir.mkdir(parents=True, exist_ok=True)

        self._manifest = Manifest.load(self.data_dir)
        self._memtable = Memtable(on_flush=self._flush_to_sstable)
        self._closed = False

    def __enter__(self) -> "Database":
        return self

    def __exit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_val: Optional[BaseException],
        exc_tb: Optional[TracebackType],
    ) -> None:
        self.close()

    def put(self, key: bytes, value: bytes) -> None:
        """Store a key-value pair."""
        pass

    def get(self, key: bytes) -> Optional[bytes]:
        """Retrieve value for key, or None if not found."""
        pass

    def delete(self, key: bytes) -> None:
        """Delete a key."""
        pass

    def close(self) -> None:
        """Close the database and flush pending writes."""
        if self._closed:
            return
        self._memtable.close()
        self._closed = True

    def _flush_to_sstable(self, task: FlushTask) -> None:
        """Flush an immutable memtable store to a new L0 SSTable."""
        sst_id = self._manifest.allocate_id()
        path = self._sstable_dir / f"sst-{sst_id:06d}.sst"

        meta = SSTableWriter(sst_id, task.store, level=0).write(path)

        self._manifest.add_sstable(meta)
        self._manifest.save(self.data_dir)

        # TODO: Checkpoint WAL at task.checkpoint_seq_no
