from typing import BinaryIO

from .models import SSTableFooter, SparseIndex
from .BloomFilter import BloomFilter
import os


class SSTableReader:
    """
    Reads an SSTable file, keeping bloom filter and index in RAM.

    The file descriptor stays open for the lifetime of the reader
    to avoid repeated open/close on each lookup.

    Usage:
        with SSTableReader(path) as reader:
            if reader.bloom.might_contain(key):
                value = reader.get(key)
    """

    def __init__(self, filename: str):
        self.filename = filename
        self.fd: BinaryIO = open(file=filename, mode="rb")

        try:
            self._load_metadata()
        except Exception:
            self.fd.close()
            raise

    def get(self, key: bytes) -> bytes | None:
        if key in self.bloom:
            offset = self.index.find_block_offset(key)
        pass

    def _load_metadata(self):
        """Load footer, bloom filter, and sparse index into RAM."""
        # Read 1: Footer (32 bytes, one seek to end)
        self.fd.seek(-SSTableFooter.SIZE, os.SEEK_END)
        self.footer = SSTableFooter.from_bytes(
            self._read_exact(self.fd, SSTableFooter.SIZE, "footer")
        )

        # Read 2: Bloom filter (few KB, one seek)
        self.fd.seek(self.footer.bloom_offset)
        self.bloom = BloomFilter.from_bytes(
            self._read_exact(self.fd, self.footer.bloom_size, "bloom filter")
        )

        # Read 3: Sparse index (few KB, one seek)
        self.fd.seek(self.footer.index_offset)
        self.index = SparseIndex.from_bytes(
            self._read_exact(self.fd, self.footer.index_size, "sparse index")
        )

    def close(self):
        """Close the underlying file descriptor."""
        if self.fd:
            self.fd.close()

    def _read_exact(self, f: BinaryIO, n: int, context: str = "") -> bytes:
        """
        Read exactly n bytes from file, raising if not enough data.

        Args:
            f: File object to read from
            n: Number of bytes to read
            context: Description for error message (e.g., "block header")

        Raises:
            ValueError: If fewer than n bytes are available
        """
        data = f.read(n)
        if len(data) < n:
            ctx = f" reading {context}" if context else ""
            raise ValueError(
                f"Unexpected end of file{ctx}: expected {n} bytes, got {len(data)}"
            )
        return data
