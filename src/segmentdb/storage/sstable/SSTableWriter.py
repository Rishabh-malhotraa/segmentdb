import os
import time
from pathlib import Path
from sortedcontainers import SortedDict

from .models import (
    Block,
    SSTableEntry,
    SSTableHeader,
    SparseIndex,
    SparseIndexEntry,
    SSTableFooter,
)
from .BloomFilter import BloomFilter
from segmentdb.storage.manifest import SSTableMeta


class SSTableWriter:
    BLOCK_SIZE = 4 * 1024  # 4KB
    VERSION = 1

    def __init__(self, sst_id: int, store: SortedDict, level: int = 0):
        self.sst_id = sst_id
        self.store = store
        self.level = level

    def write(self, path: Path) -> SSTableMeta:
        """
        Write the entire SSTable to disk atomically.

        File layout:

            ┌──────────────────────────────────────────┐
            │ Header (17 bytes)                        │
            ├──────────────────────────────────────────┤
            │ Data Blocks (compressed, variable)       │
            ├──────────────────────────────────────────┤
            │ Sparse Index (variable)                  │
            ├──────────────────────────────────────────┤
            │ Bloom Filter (variable)                  │
            ├──────────────────────────────────────────┤
            │ Footer (32 bytes)                        │
            └──────────────────────────────────────────┘

        Args:
            path: Destination path for the SSTable file
        """
        blocks, index_entries, entry_count = self._build_blocks_and_index()

        # Build header
        header = SSTableHeader(
            version=self.VERSION, level=self.level, entry_count=entry_count
        )
        header_bytes = header.to_bytes()

        # Build block data
        block_bytes = b"".join(block.to_bytes() for block in blocks)

        # Build sparse index
        sparse_index = SparseIndex(entries=index_entries)
        index_bytes = sparse_index.to_bytes()
        index_offset = len(header_bytes) + len(block_bytes)

        # Build bloom filter from all keys
        bloom = BloomFilter.from_keys(self.store.keys())
        bloom_bytes = bloom.to_bytes()
        bloom_offset = index_offset + len(index_bytes)

        # Build footer
        footer = SSTableFooter(
            index_offset=index_offset,
            index_size=len(index_bytes),
            bloom_offset=bloom_offset,
            bloom_size=len(bloom_bytes),
        )
        footer_bytes = footer.to_bytes()

        # Write atomically
        temp_path = path.with_suffix(".tmp")
        with open(temp_path, "wb") as f:
            f.write(
                header_bytes + block_bytes + index_bytes + bloom_bytes + footer_bytes
            )
            f.flush()
            os.fsync(f.fileno())

        temp_path.rename(path)

        # Build metadata from what we just wrote
        keys = self.store.keys()
        seq_nos = [entry.seq_no for entry in self.store.values()]
        return SSTableMeta(
            id=self.sst_id,
            filename=path.name,
            level=self.level,
            min_key=keys[0],
            max_key=keys[-1],
            min_seq_no=min(seq_nos),
            max_seq_no=max(seq_nos),
            entry_count=entry_count,
            file_size=path.stat().st_size,
            created_at=int(time.time()),
        )

    def _build_blocks_and_index(
        self,
    ) -> tuple[list[Block], list[SparseIndexEntry], int]:
        """
        Partition store entries into compressed blocks and build sparse index.

        Returns:
            Tuple of (blocks, index_entries, total_entry_count)
        """
        entries: list[SSTableEntry] = []
        blocks: list[Block] = []
        index_entries: list[SparseIndexEntry] = []
        curr_size = 0
        offset = SSTableHeader.SIZE
        total_entry_count = 0

        for k, v in self.store.items():
            entry = SSTableEntry(k, v.value, v.seq_no)
            entries.append(entry)
            curr_size += entry.bytes_size

            if curr_size >= self.BLOCK_SIZE:
                first_key = entries[0].key
                block = Block.from_entries(entries)

                blocks.append(block)
                index_entries.append(SparseIndexEntry(key=first_key, offset=offset))

                offset += block.size
                total_entry_count += len(entries)
                entries = []
                curr_size = 0

        if entries:
            first_key = entries[0].key
            block = Block.from_entries(entries)

            blocks.append(block)
            index_entries.append(SparseIndexEntry(key=first_key, offset=offset))
            total_entry_count += len(entries)

        return blocks, index_entries, total_entry_count
