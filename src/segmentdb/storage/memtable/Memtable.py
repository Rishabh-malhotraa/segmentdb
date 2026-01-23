from collections import deque
from dataclasses import dataclass
from threading import Condition, Lock, Thread

from sortedcontainers import SortedDict


@dataclass(slots=True, frozen=True)
class FlushTask:
    """A store to flush along with its checkpoint sequence number."""

    store: SortedDict
    checkpoint_seq_no: int


@dataclass(slots=True, frozen=True)
class MemtableEntry:
    seq_no: int
    value: bytes | None = None

    @property
    def size_bytes(self) -> int:
        # seq_no (8 bytes) + value length (or 0 for tombstone)
        return 8 + (len(self.value) if self.value else 0)


class Memtable:
    """
    In-memory sorted key-value store with background flushing.

    The memtable accumulates writes in a sorted data structure (SortedDict) until
    it reaches MAX_SIZE_BYTES, at which point it becomes immutable and is queued
    for flushing to disk as an SSTable by a background thread.

    Thread Safety:
        - All public methods are thread-safe
        - Uses a single Condition (_flush_signal) to protect shared state
        - Lock is held briefly for pointer operations; actual I/O happens unlocked
        - Background flushing is coordinated via _flush_signal.wait_for()

    Example:
        memtable = Memtable()
        memtable.put(b"key", MemtableEntry(seq_no=1, value=b"value"))
        entry = memtable.get(b"key")
        memtable.close()  # Graceful shutdown

    Attributes:
        MAX_SIZE_BYTES: Threshold (4MB) at which memtable is rotated to immutable.
    """

    MAX_SIZE_BYTES = 4 * 1024 * 1024  # 4MB

    def __init__(self):
        self._store = SortedDict()
        self._size_bytes = 0
        self._immutable_stores: deque[FlushTask | None] = deque()

        self._flush_signal = Condition()

        self._thread = Thread(target=self._flush_worker, daemon=True)
        self._thread.start()

    @property
    def should_flush(self) -> bool:
        return self._size_bytes >= self.MAX_SIZE_BYTES

    def put(self, key: bytes, entry: MemtableEntry) -> None:
        self._set(key, entry)

    def delete(self, key: bytes, tombstone: MemtableEntry) -> None:
        self._set(key, tombstone)

    def get(self, key: bytes) -> MemtableEntry | None:
        with self._flush_signal:
            if key in self._store:
                return self._store[key]

            for task in reversed(self._immutable_stores):
                if task is None:
                    continue
                if key in task.store:
                    return task.store[key]

            return None

    def close(self):
        with self._flush_signal:
            self._immutable_stores.append(None)
            self._flush_signal.notify()
        self._thread.join()

    def _set(self, key: bytes, entry: MemtableEntry) -> None:
        with self._flush_signal:
            if key in self._store:
                old_entry = self._store[key]
                self._size_bytes -= len(key) + old_entry.size_bytes

            self._store[key] = entry
            self._size_bytes += len(key) + entry.size_bytes

            if self.should_flush:
                # Use the entry's seq_no as the checkpoint
                # (highest seq_no in this store since it's the last write)
                task = FlushTask(store=self._store, checkpoint_seq_no=entry.seq_no)
                self._immutable_stores.append(task)
                self._store = SortedDict()
                self._size_bytes = 0
                self._flush_signal.notify()

    def _flush_worker(self):
        """Background thread that flushes immutable stores to disk."""
        while True:
            with self._flush_signal:
                self._flush_signal.wait_for(lambda: self._immutable_stores)
                task = self._immutable_stores.popleft()

            if task is None:
                return

            # Iterate through all entries in sorted order
            for key, entry in task.store.items():
                # TODO: Write to SSTable
                # TODO: Add key to bloom filter
                # TODO: Update sparse index
                pass

            # TODO: After successful flush, checkpoint the WAL at task.checkpoint_seq_no
            # This allows truncating WAL entries up to this sequence number
