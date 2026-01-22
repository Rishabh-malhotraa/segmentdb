from collections import deque
from dataclasses import dataclass
from threading import Condition, Lock, Thread, Event

from sortedcontainers import SortedDict


@dataclass(slots=True, frozen=True)
class MemtableEntry:
    seq_no: int
    value: bytes | None = None

    @property
    def is_tombstone(self) -> bool:
        return self.value is None

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
        - Writes are protected by _lock
        - Background flushing is coordinated via _flush_signal

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
        self._immutable_stores: deque[SortedDict] = deque()

        self._lock = Lock()
        self._flush_signal = Condition()
        self._shutdown_event = Event()

        self._thread = Thread(target=self._flush_worker, daemon=True)
        self._thread.start()

    @property
    def should_flush(self) -> bool:
        return self._size_bytes >= self.MAX_SIZE_BYTES

    def put(self, key: bytes, entry: MemtableEntry) -> None:
        self._set(key, entry)

    def get(self, key: bytes) -> MemtableEntry | None:
        with self._lock:
            if key in self._store:
                return self._store[key]

            for store in reversed(self._immutable_stores):
                if key in store:
                    return store[key]

            return None

    def delete(self, key: bytes, tombstone: MemtableEntry) -> None:
        self._set(key, tombstone)

    def close(self):
        with self._flush_signal:
            self._shutdown_event.set()
            self._flush_signal.notify()
        self._thread.join()

    def _set(self, key: bytes, entry: MemtableEntry) -> None:
        with self._lock:
            if key in self._store:
                old_entry = self._store[key]
                self._size_bytes -= len(key) + old_entry.size_bytes

            self._store[key] = entry
            self._size_bytes += len(key) + entry.size_bytes

            with self._flush_signal:
                if self.should_flush:
                    self._immutable_stores.append(self._store)
                    self._store = SortedDict()
                    self._size_bytes = 0
                    self._flush_signal.notify()

    def _flush_worker(self):
        """Background thread that flushes immutable stores to disk."""
        while True:
            with self._flush_signal:
                while not self._immutable_stores and not self._shutdown_event.is_set():
                    self._flush_signal.wait()

                stores_to_flush = self._immutable_stores.copy()

            for store in stores_to_flush:
                # TODO: Implement Memtable Writer to flush the immutable store
                # Create a bloom filter
                # Create a sparse index
                pass

            with self._flush_signal:
                for _ in stores_to_flush:
                    self._immutable_stores.popleft()

            if self._shutdown_event.is_set():
                return
