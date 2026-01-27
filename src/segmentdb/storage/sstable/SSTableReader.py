from typing import BinaryIO
import struct
import xxhash

from .models import SSTableFooter, SparseIndex, Block
from .BloomFilter import BloomFilter
import os


class SSTableReader:
    """
    Reads an SSTable file, keeping bloom filter and index in RAM.

    The file descriptor stays open for the lifetime of the reader
    to avoid repeated open/close on each lookup. Designed to be pooled
    and kept warm for efficient repeated access.

    Usage:
        reader = SSTableReader(path)
        value = reader.get(key)  # Bloom filter checked internally
        # ... keep reader open for more lookups ...
        reader.close()  # When shutting down
    """

    def __init__(self, filename: str):
        self.filename = filename
        self.fd: BinaryIO = open(file=filename, mode="rb")

        try:
            self._load_metadata()
        except Exception:
            self.fd.close()
            raise

    def __contains__(self, key: bytes) -> bool:
        """Check if key exists in this SSTable."""
        return self.get(key) is not None

    def get(self, key: bytes) -> bytes | None:
        if key not in self.bloom:
            return None

        offset = self.index.find_block_offset(key)
        if offset is None:
            return None

        self.fd.seek(offset)

        header = self._read_exact(self.fd, 8, "block header")
        compressed_size, uncompressed_size = struct.unpack(">II", header)

        compressed_data = self._read_exact(self.fd, compressed_size, "compressed data")
        stored_checksum = struct.unpack(">I", self._read_exact(self.fd, 4, "checksum"))[
            0
        ]

        computed_checksum = xxhash.xxh32(header + compressed_data).intdigest()
        if stored_checksum != computed_checksum:
            raise ValueError(
                f"Block checksum mismatch: stored={stored_checksum:#x}, "
                f"computed={computed_checksum:#x}"
            )

        # Create block and iterate entries
        block = Block(data=compressed_data, uncompressed_size=uncompressed_size)

        for entry in block:
            if entry.key == key:
                return entry.value
            if entry.key > key:
                break

        return None

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

    @staticmethod
    def _read_exact(f: BinaryIO, n: int, context: str = "") -> bytes:
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
