from dataclasses import dataclass
import struct
import xxhash
import lz4.block
from bisect import bisect_right
from typing import ClassVar

"""
SSTable data models and serialization formats.

This module defines the on-disk binary formats for SSTable components:
- SSTableHeader: File header with magic number and metadata
- SSTableEntry: Individual key-value record
- Block: Compressed container of entries with integrity checking
- SparseIndex/SparseIndexEntry: Block-level index for binary search
- SSTableFooter: File trailer with section offsets

All multi-byte integers use big-endian (network) byte order.
"""


@dataclass(slots=True)
class SSTableHeader:
    """
    SSTable file header containing metadata for validation and recovery.

    Format (17 bytes, big-endian):
    ┌──────────────┬─────────┬───────┬─────────────┐
    │ magic        │ version │ level │ entry_count │
    │ (8 bytes)    │ (4 B)   │ (1 B) │ (4 bytes)   │
    └──────────────┴─────────┴───────┴─────────────┘

    The level field enables manifest recovery by scanning SSTable files.
    """

    MAGIC_NUMBER: ClassVar[bytes] = b"SEGMTSST"
    SIZE: ClassVar[int] = 17

    version: int
    level: int
    entry_count: int

    def to_bytes(self) -> bytes:
        """Serialize header to 17 bytes."""
        return struct.pack(
            ">8sIBI", self.MAGIC_NUMBER, self.version, self.level, self.entry_count
        )

    @classmethod
    def from_bytes(cls, data: bytes) -> "SSTableHeader":
        """Deserialize header from 17 bytes."""
        magic, version, level, entry_count = struct.unpack(">8sIBI", data)
        if magic != cls.MAGIC_NUMBER:
            raise ValueError(f"Invalid SSTable: bad magic number {magic}")
        return cls(version=version, level=level, entry_count=entry_count)


@dataclass(slots=True)
class SSTableEntry:
    """
    A single key-value entry in the SSTable.

    Entry format on disk (big-endian):
    ┌────────────┬──────────┬─────────┬─────────┬───────────┬───────┬───────┐
    │ length     │ seq_no   │ key_len │ val_len │ tombstone │ key   │ value │
    │ (4 bytes)  │ (8 bytes)│ (2 B)   │ (4 B)   │ (1 byte)  │ (var) │ (var) │
    └────────────┴──────────┴─────────┴─────────┴───────────┴───────┴───────┘

    Tombstone flag: 0x00 = normal value, 0x01 = deleted (value is None)
    """

    LENGTH_PREFIX_SIZE: ClassVar[int] = 4
    HEADER_SIZE: ClassVar[int] = (
        15  # seq_no(8) + key_len(2) + val_len(4) + tombstone(1)
    )

    key: bytes
    value: bytes | None
    seq_no: int

    @property
    def is_tombstone(self) -> bool:
        """True if this entry represents a deletion."""
        return self.value is None

    @property
    def bytes_size(self) -> int:
        """Total serialized size in bytes."""
        val_len = len(self.value) if self.value is not None else 0
        return self.LENGTH_PREFIX_SIZE + self.HEADER_SIZE + len(self.key) + val_len

    def to_bytes(self) -> bytes:
        """Serialize entry to bytes."""
        key_len = len(self.key)
        val_len = len(self.value) if self.value is not None else 0
        tombstone_flag = 1 if self.value is None else 0

        payload = struct.pack(
            f">QHIB{key_len}s{val_len}s",
            self.seq_no,
            key_len,
            val_len,
            tombstone_flag,
            self.key,
            self.value if self.value is not None else b"",
        )

        entry_length = len(payload)

        return struct.pack(">I", entry_length) + payload

    @classmethod
    def from_bytes(cls, data: bytes) -> "SSTableEntry":
        """Deserialize entry from bytes (includes 4-byte length prefix)."""
        entry_length = struct.unpack(">I", data[: cls.LENGTH_PREFIX_SIZE])[0]
        payload = data[cls.LENGTH_PREFIX_SIZE : cls.LENGTH_PREFIX_SIZE + entry_length]

        seq_no, key_len, val_len, tombstone_flag = struct.unpack(
            ">QHIB", payload[: cls.HEADER_SIZE]
        )
        key_start = cls.HEADER_SIZE
        val_start = key_start + key_len
        key = payload[key_start : key_start + key_len]

        if tombstone_flag:
            value = None
        else:
            value = payload[val_start : val_start + val_len]

        return cls(key=key, value=value, seq_no=seq_no)


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

    COMPRESSION_LEVEL: ClassVar[int] = 4
    HEADER_SIZE: ClassVar[int] = 8
    FOOTER_SIZE: ClassVar[int] = 4
    OVERHEAD: ClassVar[int] = 12

    data: bytes
    uncompressed_size: int

    @property
    def size(self) -> int:
        """Total size on disk including header and footer."""
        return self.HEADER_SIZE + len(self.data) + self.FOOTER_SIZE

    @classmethod
    def from_entries(cls, entries: list[SSTableEntry]) -> "Block":
        """
        Create a compressed block from a list of entries.

        Args:
            entries: List of SSTableEntry objects to compress

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
            store_size=False,
        )

        return cls(data=compressed, uncompressed_size=len(raw_data))

    def decompress(self) -> bytes:
        """
        Decompress and return the raw block data.

        Returns:
            Concatenated serialized SSTableEntry bytes that can be
            parsed sequentially to reconstruct individual entries.
        """
        return lz4.block.decompress(self.data, uncompressed_size=self.uncompressed_size)

    def iter_entries(self):
        """
        Decompress block and yield SSTableEntry objects.

        Yields:
            SSTableEntry objects in sorted key order.
        """
        data = self.decompress()
        pos = 0
        while pos < len(data):
            # Read entry length prefix to know how much to consume
            entry_length = struct.unpack(
                ">I", data[pos : pos + SSTableEntry.LENGTH_PREFIX_SIZE]
            )[0]
            entry_size = SSTableEntry.LENGTH_PREFIX_SIZE + entry_length
            entry = SSTableEntry.from_bytes(data[pos : pos + entry_size])
            yield entry
            pos += entry_size

    def to_bytes(self) -> bytes:
        """
        Serialize block to bytes for writing to disk.

        Returns:
            Complete block bytes: header + compressed_data + xxh32
        """
        header = struct.pack(">II", len(self.data), self.uncompressed_size)
        checksum = xxhash.xxh32(header + self.data).intdigest()
        footer = struct.pack(">I", checksum)
        return header + self.data + footer

    @classmethod
    def from_bytes(cls, data: bytes) -> "Block":
        """
        Deserialize block from bytes.

        Args:
            data: Complete block bytes (header + compressed_data + checksum)

        Returns:
            A Block with compressed data

        Raises:
            ValueError: If checksum fails (data corruption)
        """
        header = data[: cls.HEADER_SIZE]
        compressed_size, uncompressed_size = struct.unpack(">II", header)

        compressed_data = data[cls.HEADER_SIZE : cls.HEADER_SIZE + compressed_size]

        checksum_bytes = data[
            cls.HEADER_SIZE
            + compressed_size : cls.HEADER_SIZE
            + compressed_size
            + cls.FOOTER_SIZE
        ]
        stored_checksum = struct.unpack(">I", checksum_bytes)[0]
        computed_checksum = xxhash.xxh32(header + compressed_data).intdigest()

        if stored_checksum != computed_checksum:
            raise ValueError(
                f"Block checksum mismatch: "
                f"stored={stored_checksum:#x}, computed={computed_checksum:#x}"
            )

        return cls(data=compressed_data, uncompressed_size=uncompressed_size)


@dataclass(slots=True)
class SparseIndexEntry:
    """
    Single entry in the sparse index mapping a block's first key to its offset.

    Format on disk (big-endian):
    ┌──────────┬──────────┬─────────────┐
    │ offset   │ key_len  │ key         │
    │ (8 bytes)│ (2 bytes)│ (variable)  │
    └──────────┴──────────┴─────────────┘
    """

    HEADER_SIZE: ClassVar[int] = 10  # offset(8) + key_len(2)

    key: bytes
    offset: int

    def to_bytes(self) -> bytes:
        """Serialize: offset(8) + key_len(2) + key."""
        key_len = len(self.key)
        return struct.pack(f">QH{key_len}s", self.offset, key_len, self.key)

    @classmethod
    def from_bytes(cls, data: bytes) -> "SparseIndexEntry":
        """Deserialize index entry from bytes."""
        offset, key_len = struct.unpack(">QH", data[: cls.HEADER_SIZE])
        key = data[cls.HEADER_SIZE : cls.HEADER_SIZE + key_len]
        return cls(key=key, offset=offset)


@dataclass(slots=True)
class SparseIndex:
    """
    Sparse (block-level) index for fast key lookups.

    Unlike a dense index that maps every key, a sparse index only stores
    the first key of each block. This dramatically reduces index size
    while still enabling O(log n) block lookup via binary search.

    Serialization format (big-endian):
    ┌─────────────┬───────────────────────────────────┐
    │ entry_count │ SparseIndexEntries                  │
    │ (4 bytes)   │ (variable)                        │
    └─────────────┴───────────────────────────────────┘
    """

    entries: list[SparseIndexEntry]

    @classmethod
    def from_bytes(cls, data: bytes) -> "SparseIndex":
        """Deserialize sparse index from bytes."""
        (entry_count,) = struct.unpack(">I", data[:4])

        entries: list[SparseIndexEntry] = []
        pos = 4
        for _ in range(entry_count):
            # Read entry header to get key length
            offset, key_len = struct.unpack(">QH", data[pos : pos + 10])
            key = data[pos + 10 : pos + 10 + key_len]
            entries.append(SparseIndexEntry(key=key, offset=offset))
            pos += 10 + key_len

        return cls(entries=entries)

    def to_bytes(self) -> bytes:
        """Serialize: entry_count(4) + entries."""
        parts = [struct.pack(">I", len(self.entries))]
        for entry in self.entries:
            parts.append(entry.to_bytes())
        return b"".join(parts)

    def find_block_offset(self, key: bytes) -> int | None:
        """
        Find the block offset that may contain the given key.

        Uses binary search to find the largest indexed key <= target key,
        then returns that block's offset. The caller must still scan the
        block to confirm the key exists.

        Args:
            key: The key to search for.

        Returns:
            Byte offset of the candidate block, or None if the key is
            smaller than all indexed keys (and thus not in the SSTable).

        Time complexity: O(log n) where n is the number of blocks.
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
    Fixed-size footer at the end of an SSTable file.

    The footer is read first (via seek to EOF - SIZE) to locate the
    sparse index and bloom filter without scanning the entire file.
    The trailing magic number validates file integrity and format.

    Format (32 bytes, big-endian):
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
        """Serialize footer to 32 bytes for writing at end of SSTable."""
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
        """
        Deserialize footer from 32 bytes read from end of SSTable.

        Args:
            data: Exactly 32 bytes from the last 32 bytes of the file.

        Returns:
            Parsed SSTableFooter with index and bloom filter locations.

        Raises:
            ValueError: If data length is wrong or magic number is invalid.
        """
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
