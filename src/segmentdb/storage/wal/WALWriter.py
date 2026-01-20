from itertools import count
from queue import Queue
from typing import BinaryIO

from segmentdb.storage.wal import OperationType, WALEntry


class WALWriter:
    """Writes entries to a Write-Ahead Log with buffered queue."""

    def __init__(self, fd: BinaryIO, start_seq: int = 1) -> None:
        self._fd = fd
        self._queue: Queue[bytes] = Queue()
        self._seq_counter = count(start_seq)

    def put(self, key: str, value: str) -> None:
        """Queue a PUT operation for the given key-value pair."""
        entry = WALEntry(
            seq_no=next(self._seq_counter),
            op_type=OperationType.PUT,
            key=key.encode("utf-8"),
            value=value.encode("utf-8"),
        )
        self._queue.put(entry.to_bytes())

    def delete(self, key: str) -> None:
        """Queue a DELETE operation for the given key."""
        entry = WALEntry(
            seq_no=next(self._seq_counter),
            op_type=OperationType.DELETE,
            key=key.encode("utf-8"),
            value=None,
        )
        self._queue.put(entry.to_bytes())

    def flush(self) -> None:
        """Flush all pending writes to WAL file."""
        while not self._queue.empty():
            self._fd.write(self._queue.get_nowait())
        self._fd.flush()
