from queue import Queue, Empty
from threading import Thread
from typing import BinaryIO
import os

from segmentdb.storage.wal import WALEntry


class WALWriter:
    """Writes entries to a Write-Ahead Log with buffered queue."""

    def __init__(self, fd: BinaryIO) -> None:
        self._fd = fd
        self._queue: Queue[bytes | None] = Queue()
        self._thread = Thread(target=self._background_writer, daemon=True)
        self._thread.start()

    def append(self, entry: WALEntry) -> None:
        """Queue a WAL entry to be written."""
        self._queue.put(entry.to_bytes())

    def close(self) -> None:
        """Signal shutdown and wait for background writer to finish."""
        self._queue.put(None)
        self._thread.join()

    def _background_writer(self):
        while True:
            item = self._queue.get()
            if item is None:
                return

            batch: list[bytes] = [item]
            while len(batch) < 256:
                try:
                    item = self._queue.get_nowait()
                    if item is None:
                        self._write_batch(batch)
                        return
                    batch.append(item)
                except Empty:
                    break

            self._write_batch(batch)

    def _write_batch(self, batch: list[bytes]) -> None:
        """Write a batch of entries to disk and sync."""
        if batch:
            self._fd.write(b"".join(batch))
            self._fd.flush()
            os.fsync(self._fd.fileno())
