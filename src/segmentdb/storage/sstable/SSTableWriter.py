import os
from pathlib import Path
from sortedcontainers import SortedDict

from .models import Block, SSTableEntry, SSTableHeader, SparseIndex, SSTableFooter
from .BloomFilter import BloomFilter


class SSTableWriter:
    BLOCK_SIZE = 4 * 1024  # 4KB
    VERSION = 1

    def __init__(self, store: SortedDict):
        self.store = store

    def write(self, path: Path) -> None:
        """
        Write the entire SSTable to disk atomically.

        File layout:
        ┌──────────────────────────────────────────┐
        │ Header (16 bytes)                        │
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
        blocks = self._create_blocks()
        entry_count = sum(b.entry_count for b in blocks)

        # Build header
        header = SSTableHeader(version=self.VERSION, entry_count=entry_count)
        header_bytes = header.to_bytes()

        # Build block data
        block_bytes = b"".join(block.to_bytes() for block in blocks)

        # Build sparse index (one entry per block)
        sparse_index = SparseIndex.from_blocks(blocks)
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
            f.write(header_bytes + block_bytes + index_bytes + bloom_bytes + footer_bytes)
            f.flush()
            os.fsync(f.fileno())

        temp_path.rename(path)

    def _create_blocks(self) -> list[Block]:
        """
        Create compressed blocks from the store.

        Returns:
            List of Block objects ready to be written to disk
        """
        entries: list[SSTableEntry] = []
        blocks: list[Block] = []
        curr_size = 0
        offset = SSTableHeader.SIZE

        for k, v in self.store.items():
            entry = SSTableEntry(k, v.value, v.seq_no)
            entries.append(entry)
            curr_size += entry.bytes_size

            if curr_size >= self.BLOCK_SIZE:
                block = Block.from_entries(entries, offset)
                blocks.append(block)
                offset += block.size

                entries = []
                curr_size = 0

        # Handle remaining entries in the last block
        if entries:
            block = Block.from_entries(entries, offset)
            blocks.append(block)

        return blocks
