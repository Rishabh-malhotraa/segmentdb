from dataclasses import dataclass
import struct
import xxhash
import lz4.block
from bisect import bisect_right
from typing import ClassVar, BinaryIO


def read_exact(f: BinaryIO, n: int, context: str = "") -> bytes:
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


@dataclass(slots=True)
class SSTableHeader:
    MAGIC_NUMBER: ClassVar[bytes] = b"SEGMTSST"
    SIZE: ClassVar[int] = 16

    version: int
    entry_count: int

    def to_bytes(self) -> bytes:
        return struct.pack("<8sII", self.MAGIC_NUMBER, self.version, self.entry_count)

    @classmethod
    def from_bytes(cls, data: bytes) -> "SSTableHeader":
        magic, version, entry_count = struct.unpack("<8sII", data)
        if magic != cls.MAGIC_NUMBER:
            raise ValueError(f"Invalid SSTable: bad magic number {magic}")
        return cls(version=version, entry_count=entry_count)


@dataclass(slots=True)
class SSTableEntry:
    key: bytes
    value: bytes
    seq_no: int

    @property
    def bytes_size(self) -> int:
        return 8 + 2 + 4 + len(self.key) + len(self.value)

    def to_bytes(self) -> bytes:
        """Serialize entry to bytes: length(4) + payload."""
        key_len = len(self.key)
        val_len = len(self.value) if self.value else 0

        payload = struct.pack(
            f">QHI{key_len}s{val_len}s",
            self.seq_no,
            key_len,
            val_len,
            self.key,
            self.value or b"",
        )

        entry_length = len(payload)

        return struct.pack(">I", entry_length) + payload


@dataclass(slots=True)
class Block:
    """
    A self-describing compressed block of SSTable entries.

    Block format on disk:
    ┌────────────────────┬────────────────────┬─────────────────────────────┬──────────┐
    │ compressed_size    │ uncompressed_size  │ compressed_data             │ xxh32    │
    │ (4 bytes, BE)      │ (4 bytes, BE)      │ (variable)                  │ (4 bytes)│
    └────────────────────┴────────────────────┴─────────────────────────────┴──────────┘

    Benefits:
    - Self-describing: can read blocks without relying on index
    - Recoverable: can scan and rebuild index if corrupted
    - Integrity: xxh32 checksum for fast corruption detection
    """

    # LZ4 high-compression level (1-12)
    # Level 4 balances compression speed and ratio for SSTable workloads
    COMPRESSION_LEVEL: ClassVar[int] = 4

    # Block header: compressed_size (4) + uncompressed_size (4)
    # Block footer: xxh32 checksum (4)
    HEADER_SIZE: ClassVar[int] = 8
    FOOTER_SIZE: ClassVar[int] = 4
    OVERHEAD: ClassVar[int] = 12

    first_key: bytes
    data: bytes  # Compressed data (without header/footer)
    uncompressed_size: int
    entry_count: int
    offset: int = 0

    @property
    def compressed_size(self) -> int:
        """Size of compressed data in bytes (without header/footer)."""
        return len(self.data)

    @property
    def size(self) -> int:
        """Total size on disk including header and footer."""
        return self.HEADER_SIZE + len(self.data) + self.FOOTER_SIZE

    @property
    def compression_ratio(self) -> float:
        """Compression ratio (uncompressed / compressed)."""
        return (
            self.uncompressed_size / self.compressed_size
            if self.compressed_size > 0
            else 0.0
        )

    @classmethod
    def from_entries(cls, entries: list[SSTableEntry], offset: int = 0) -> "Block":
        """
        Create a compressed block from a list of entries.

        Args:
            entries: List of SSTableEntry objects to compress
            offset: Byte offset where this block will be written

        Returns:
            A new Block with compressed data
        """
        if not entries:
            raise ValueError("Cannot create block from empty entries list")

        raw_data = b"".join(entry.to_bytes() for entry in entries)
        compressed = lz4.block.compress(
            raw_data,
            mode="high_compression",
            compression=cls.COMPRESSION_LEVEL,
            store_size=False,  # Size stored in block header
        )

        return cls(
            first_key=entries[0].key,
            data=compressed,
            uncompressed_size=len(raw_data),
            entry_count=len(entries),
            offset=offset,
        )

    @classmethod
    def from_file(
        cls, f: BinaryIO, first_key: bytes = b"", entry_count: int = 0
    ) -> "Block":
        """
        Read a block from file at current position.

        Args:
            f: File object positioned at block start
            first_key: The first key in this block (from index, for metadata)
            entry_count: Number of entries (from index, for metadata)

        Returns:
            A Block with compressed data

        Raises:
            ValueError: If checksum fails (data corruption)
        """
        offset = f.tell()

        header = read_exact(f, cls.HEADER_SIZE, "block header")
        compressed_size, uncompressed_size = struct.unpack(">II", header)

        compressed_data = read_exact(f, compressed_size, "block data")

        checksum_bytes = read_exact(f, cls.FOOTER_SIZE, "block checksum")
        stored_checksum = struct.unpack(">I", checksum_bytes)[0]
        computed_checksum = xxhash.xxh32(header + compressed_data).intdigest()

        if stored_checksum != computed_checksum:
            raise ValueError(
                f"Block checksum mismatch at offset {offset}: "
                f"stored={stored_checksum:#x}, computed={computed_checksum:#x}"
            )

        return cls(
            first_key=first_key,
            data=compressed_data,
            uncompressed_size=uncompressed_size,
            entry_count=entry_count,
            offset=offset,
        )

    def decompress(self) -> bytes:
        """Decompress and return the raw block data."""
        return lz4.block.decompress(self.data, uncompressed_size=self.uncompressed_size)

    def to_bytes(self) -> bytes:
        """
        Serialize block to bytes for writing to disk.

        Returns:
            Complete block bytes: header + compressed_data + xxh32
        """
        header = struct.pack(">II", self.compressed_size, self.uncompressed_size)
        checksum = xxhash.xxh32(header + self.data).intdigest()
        footer = struct.pack(">I", checksum)
        return header + self.data + footer


@dataclass(slots=True)
class SparseIndexEntry:
    """
    Single entry in the sparse index: maps a key to its block offset.

    Format on disk (fixed fields first for easier unpacking):
    ┌──────────┬──────────┬─────────────┐
    │ offset   │ key_len  │ key         │
    │ (8 bytes)│ (2 bytes)│ (variable)  │
    └──────────┴──────────┴─────────────┘
    """

    key: bytes
    offset: int  # Byte offset where the block starts

    def to_bytes(self) -> bytes:
        """Serialize: offset(8) + key_len(2) + key."""
        key_len = len(self.key)
        return struct.pack(f">QH{key_len}s", self.offset, key_len, self.key)

    @classmethod
    def from_file(cls, f: BinaryIO) -> "SparseIndexEntry":
        """Read a single index entry from file."""
        header = read_exact(f, 10, "index entry header")
        offset, key_len = struct.unpack(">QH", header)

        key = read_exact(f, key_len, "index key")
        return cls(key=key, offset=offset)


@dataclass(slots=True)
class SparseIndex:
    """
    Sparse index for fast key lookups.

    Stores the first key of each block with its offset.
    Enables binary search to find the right block for any key.
    """

    entries: list[SparseIndexEntry]

    @classmethod
    def from_blocks(cls, blocks: list[Block]) -> "SparseIndex":
        """Build sparse index from a list of blocks."""
        entries = [
            SparseIndexEntry(key=block.first_key, offset=block.offset)
            for block in blocks
        ]
        return cls(entries=entries)

    def to_bytes(self) -> bytes:
        """Serialize: entry_count(4) + entries."""
        parts = [struct.pack(">I", len(self.entries))]
        for entry in self.entries:
            parts.append(entry.to_bytes())
        return b"".join(parts)

    def find_block_offset(self, key: bytes) -> int | None:
        """
        Find the block offset for a key using binary search.

        Returns the offset of the block that may contain the key,
        or None if the key is smaller than all indexed keys.
        """
        if not self.entries:
            return None

        keys = [e.key for e in self.entries]
        idx = bisect_right(keys, key) - 1

        if idx < 0:
            return None

        return self.entries[idx].offset


@dataclass(slots=True)
class SSTableFooter:
    """
    Footer at the end of SSTable file.

    Contains offsets to index and bloom filter for quick loading.

    Format (32 bytes):
    ┌─────────────┬────────────┬─────────────┬────────────┬─────────────┐
    │index_offset │ index_size │bloom_offset │ bloom_size │ magic       │
    │ (8 bytes)   │ (4 bytes)  │ (8 bytes)   │ (4 bytes)  │ (8 bytes)   │
    └─────────────┴────────────┴─────────────┴────────────┴─────────────┘
    """

    MAGIC_NUMBER: ClassVar[bytes] = b"SEGMTSST"
    SIZE: ClassVar[int] = 32

    index_offset: int
    index_size: int
    bloom_offset: int
    bloom_size: int

    def to_bytes(self) -> bytes:
        return struct.pack(
            ">QIQI8s",
            self.index_offset,
            self.index_size,
            self.bloom_offset,
            self.bloom_size,
            self.MAGIC_NUMBER,
        )

    @classmethod
    def from_bytes(cls, data: bytes) -> "SSTableFooter":
        if len(data) != cls.SIZE:
            raise ValueError(f"Invalid footer size: {len(data)} != {cls.SIZE}")

        index_offset, index_size, bloom_offset, bloom_size, magic = struct.unpack(
            ">QIQI8s", data
        )

        if magic != cls.MAGIC_NUMBER:
            raise ValueError(f"Invalid SSTable footer: bad magic number {magic}")

        return cls(
            index_offset=index_offset,
            index_size=index_size,
            bloom_offset=bloom_offset,
            bloom_size=bloom_size,
        )
