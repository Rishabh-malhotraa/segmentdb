# SSTable File Format

*Last Updated: January 26, 2026*

This document describes the on-disk format for SSTable (Sorted String Table) files in SegmentDB.

## Overview

An SSTable is an immutable, sorted key-value file written when a memtable is flushed to disk. Each SSTable contains:

1. **Header** — Magic number, version, metadata
2. **Data Blocks** — Sorted key-value entries
3. **Sparse Index** — Sampled keys with offsets for fast seeking
4. **Bloom Filter** — Probabilistic filter to avoid unnecessary disk reads
5. **Footer** — Offsets to index and bloom filter sections

## File Layout

```
┌─────────────────────────────────────────────────────────────┐
│                     Header (17 bytes)                       │
│  ┌────────────────────────────────────────────────────────┐ │
│  │ magic_number     (8 bytes)  "SEGMTSST"                 │ │
│  │ version          (4 bytes)  uint32, big-endian         │ │
│  │ level            (1 byte)   uint8, compaction level    │ │
│  │ entry_count      (4 bytes)  uint32, big-endian         │ │
│  └────────────────────────────────────────────────────────┘ │
├─────────────────────────────────────────────────────────────┤
│                     Data Blocks (variable)                  │
│  ┌────────────────────────────────────────────────────────┐ │
│  │ Block 1: compressed_size | uncompressed_size | data |  │ │
│  │          xxh32 checksum                                │ │
│  │ Block 2: ...                                           │ │
│  │ ...                                                    │ │
│  │ Block N: ...                                           │ │
│  └────────────────────────────────────────────────────────┘ │
├─────────────────────────────────────────────────────────────┤
│                     Sparse Index (variable)                 │
│  ┌────────────────────────────────────────────────────────┐ │
│  │ index_entry_count (4 bytes)                            │ │
│  │ Entry 1: offset | key_len | key                        │ │
│  │ Entry 2: offset | key_len | key                        │ │
│  │ ...                                                    │ │
│  └────────────────────────────────────────────────────────┘ │
├─────────────────────────────────────────────────────────────┤
│                     Bloom Filter (variable)                 │
│  ┌────────────────────────────────────────────────────────┐ │
│  │ rbloom serialized bytes (using xxh3_64 hash)           │ │
│  └────────────────────────────────────────────────────────┘ │
├─────────────────────────────────────────────────────────────┤
│                     Footer (32 bytes)                       │
│  ┌────────────────────────────────────────────────────────┐ │
│  │ index_offset     (8 bytes)  uint64, big-endian         │ │
│  │ index_size       (4 bytes)  uint32, big-endian         │ │
│  │ bloom_offset     (8 bytes)  uint64, big-endian         │ │
│  │ bloom_size       (4 bytes)  uint32, big-endian         │ │
│  │ magic_number     (8 bytes)  "SEGMTSST"                 │ │
│  └────────────────────────────────────────────────────────┘ │
└─────────────────────────────────────────────────────────────┘
```

## Section Details

### Header (17 bytes, fixed)

All integers are big-endian.

| Field | Size | Type | Description |
|-------|------|------|-------------|
| magic_number | 8 bytes | bytes | `"SEGMTSST"` — identifies file as SSTable |
| version | 4 bytes | uint32 | Format version (currently 1) |
| level | 1 byte | uint8 | Compaction level (0-255, enables manifest recovery) |
| entry_count | 4 bytes | uint32 | Total number of key-value entries |

### Block Format

Entries are grouped into LZ4-compressed blocks with integrity checking:

| Field | Size | Type | Description |
|-------|------|------|-------------|
| compressed_size | 4 bytes | uint32 | Size of compressed data |
| uncompressed_size | 4 bytes | uint32 | Original size before compression |
| compressed_data | variable | bytes | LZ4 compressed entry data |
| checksum | 4 bytes | uint32 | xxh32 of header + compressed_data |

**Compression:** LZ4 high-compression mode, level 4.

### Entry Format (within decompressed block)

Each entry in a decompressed block:

| Field | Size | Type | Description |
|-------|------|------|-------------|
| length | 4 bytes | uint32 | Total length of entry payload |
| seq_no | 8 bytes | uint64 | Sequence number for MVCC |
| key_len | 2 bytes | uint16 | Length of key in bytes |
| val_len | 4 bytes | uint32 | Length of value in bytes |
| tombstone | 1 byte | uint8 | 0x00 = value, 0x01 = deleted |
| key | variable | bytes | The key |
| value | variable | bytes | The value (empty if tombstone) |

**Tombstone encoding:** `tombstone = 0x01` indicates a delete marker (value is empty).

### Sparse Index

The sparse index stores the first key of each block to enable fast seeking:

| Field | Size | Type | Description |
|-------|------|------|-------------|
| entry_count | 4 bytes | uint32 | Number of index entries |

Each index entry (repeated `entry_count` times):

| Field | Size | Type | Description |
|-------|------|------|-------------|
| offset | 8 bytes | uint64 | Byte offset of block in data section |
| key_len | 2 bytes | uint16 | Length of key |
| key | variable | bytes | First key in the block |

**Lookup strategy:**
1. Binary search the sparse index to find the largest key ≤ target
2. Seek to that block's offset in the data section
3. Decompress block and scan entries until target key is found (or passed)

### Bloom Filter

Probabilistic filter to quickly reject keys that don't exist:

The bloom filter is serialized using the `rbloom` library format with a custom hash function (`xxh3_64_intdigest`) for deterministic, portable hashing.

**Configuration:**
- False positive rate: ~1%
- Hash function: xxh3_64 (deterministic across runs)
- Serialization: `rbloom.Bloom.dumps()` / `rbloom.Bloom.loads()`

### Footer (32 bytes, fixed)

All integers are big-endian.

| Field | Size | Type | Description |
|-------|------|------|-------------|
| index_offset | 8 bytes | uint64 | Byte offset where sparse index starts |
| index_size | 4 bytes | uint32 | Size of sparse index in bytes |
| bloom_offset | 8 bytes | uint64 | Byte offset where bloom filter starts |
| bloom_size | 4 bytes | uint32 | Size of bloom filter in bytes |
| magic_number | 8 bytes | bytes | `"SEGMTSST"` — validates footer integrity |

## Read Path

1. **Open file**, seek to `file_size - 32`, read footer
2. **Validate** footer magic number
3. **Load bloom filter** into memory using `bloom_offset` and `bloom_size`
4. **Load sparse index** into memory using `index_offset` and `index_size`
5. **Keep file descriptor open** for the lifetime of the reader
6. **On lookup:**
   - Check bloom filter → if negative, key definitely doesn't exist
   - If positive, binary search sparse index to find candidate block
   - Seek to block offset, read and decompress block
   - Scan entries in block until key found (or passed)

## Write Path

1. **Partition entries into blocks** — Group sorted entries into ~4KB blocks
2. **Compress each block** — LZ4 high-compression, add xxh32 checksum
3. **Build sparse index** — Record first key and offset of each block
4. **Build bloom filter** — Add all keys with ~1% false positive rate
5. **Assemble file** — Header + blocks + index + bloom + footer
6. **Write atomically** — temp file + fsync + rename

## File Naming

SSTable files are named with monotonically increasing IDs:

```
data/sstables/
├── sst-000001.sst
├── sst-000002.sst
└── sst-000003.sst
```

## Atomic Writes & Immutability

SSTables are **immutable** — once written, they are never modified. This simplifies concurrency and crash recovery.

### Write Strategy: Temp File + fsync + Rename

```python
# 1. Write to temporary file
temp_path = path.with_suffix(".tmp")
with open(temp_path, "wb") as f:
    f.write(header + blocks + index + bloom + footer)
    f.flush()
    os.fsync(f.fileno())  # Force to disk

# 2. Atomic rename
temp_path.rename(path)  # All-or-nothing visibility
```

### Why This Works

| Step | Crash Behavior |
|------|----------------|
| During write to `.tmp` | Incomplete temp file, ignored on recovery |
| After `fsync()`, before `rename()` | Complete temp file, can be cleaned up or recovered |
| After `rename()` | SSTable fully visible and durable |

### Key Properties

1. **No partial files** — Readers never see incomplete SSTables
2. **No locks needed** — Old SSTable readable while new one is created
3. **Crash-safe** — Either the full file exists, or it doesn't

### fsync Semantics

```python
f.flush()              # Python buffer → OS buffer
os.fsync(f.fileno())   # OS buffer → disk platters
```

**Without fsync:** OS may report write complete while data is still in RAM. Power loss = data loss.

**With fsync:** Blocks until data is physically on disk (or battery-backed cache).

### Rename Atomicity

On POSIX systems, `rename()` is atomic:
- Either the old name points to old file, or new name points to new file
- Never a state where the file is "half-renamed"
- Works across directories on same filesystem

```
Before: sst-000001.tmp exists, sst-000001.sst does not
After:  sst-000001.sst exists (temp file gone)
```

### Immutability Benefits

| Benefit | Explanation |
|---------|-------------|
| **No corruption** | Can't corrupt what you don't modify |
| **Lock-free reads** | Readers access immutable data, no synchronization |
| **Simple recovery** | Just delete incomplete `.tmp` files on startup |
| **Cache-friendly** | File contents never change, can cache aggressively |

### How Updates/Deletes Work

Since SSTables are immutable, updates and deletes create new entries:

```
SSTable_001: {key: "a", value: "old", seq: 1}
SSTable_002: {key: "a", value: "new", seq: 5}  ← Higher seq wins

# Delete = tombstone (value=None)
SSTable_003: {key: "a", value: None, seq: 10}  ← Key is "deleted"
```

Old versions are cleaned up during **compaction**, which merges SSTables and drops shadowed entries.

---

## Design Decisions

### Why footer at end?

- Can write file sequentially in one pass
- Only need to seek once (to read footer) when opening
- Index and bloom sizes unknown until after data is written

### Why sparse index instead of full index?

- Full index would be nearly as large as data
- Sparse index trades O(1) lookup for O(log n + k) where k is the sampling interval
- Significantly smaller memory footprint

### Why embed bloom filter in file?

- Single file is simpler to manage (atomic, easier cleanup)
- Bloom filter is loaded once on open, checked on every read
- Alternative: separate `.bloom` files add complexity

## Constants

```python
MAGIC_NUMBER = b"SEGMTSST"
HEADER_SIZE = 17  # magic(8) + version(4) + level(1) + entry_count(4)
FOOTER_SIZE = 32
BLOCK_HEADER_SIZE = 8   # compressed_size(4) + uncompressed_size(4)
BLOCK_FOOTER_SIZE = 4   # xxh32 checksum
ENTRY_HEADER_SIZE = 15  # seq_no(8) + key_len(2) + val_len(4) + tombstone(1)
SPARSE_INDEX_ENTRY_HEADER_SIZE = 10  # offset(8) + key_len(2)
BLOOM_FALSE_POSITIVE_RATE = 0.01  # 1%
COMPRESSION_LEVEL = 4   # LZ4 high-compression
```
