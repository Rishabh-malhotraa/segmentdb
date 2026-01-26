# SSTable File Format

*Last Updated: January 23, 2026*

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
│                     Header (16 bytes)                       │
│  ┌────────────────────────────────────────────────────────┐ │
│  │ magic_number     (8 bytes)  "SEGMTSST"                 │ │
│  │ version          (4 bytes)  uint32, little-endian      │ │
│  │ entry_count      (4 bytes)  uint32, little-endian      │ │
│  └────────────────────────────────────────────────────────┘ │
├─────────────────────────────────────────────────────────────┤
│                     Data Blocks (variable)                  │
│  ┌────────────────────────────────────────────────────────┐ │
│  │ Entry 1: key_len | key | val_len | value | seq_no      │ │
│  │ Entry 2: key_len | key | val_len | value | seq_no      │ │
│  │ ...                                                    │ │
│  │ Entry N: key_len | key | val_len | value | seq_no      │ │
│  └────────────────────────────────────────────────────────┘ │
├─────────────────────────────────────────────────────────────┤
│                     Sparse Index (variable)                 │
│  ┌────────────────────────────────────────────────────────┐ │
│  │ index_entry_count (4 bytes)                            │ │
│  │ Entry 1: key_len | key | offset                        │ │
│  │ Entry 2: key_len | key | offset                        │ │
│  │ ...                                                    │ │
│  └────────────────────────────────────────────────────────┘ │
├─────────────────────────────────────────────────────────────┤
│                     Bloom Filter (variable)                 │
│  ┌────────────────────────────────────────────────────────┐ │
│  │ num_hash_funcs   (1 byte)                              │ │
│  │ bit_array_size   (4 bytes)                             │ │
│  │ bit_array        (variable)                            │ │
│  └────────────────────────────────────────────────────────┘ │
├─────────────────────────────────────────────────────────────┤
│                     Footer (32 bytes)                       │
│  ┌────────────────────────────────────────────────────────┐ │
│  │ index_offset     (8 bytes)  uint64, little-endian      │ │
│  │ index_size       (4 bytes)  uint32, little-endian      │ │
│  │ bloom_offset     (8 bytes)  uint64, little-endian      │ │
│  │ bloom_size       (4 bytes)  uint32, little-endian      │ │
│  │ magic_number     (8 bytes)  "SEGMTSST"                 │ │
│  └────────────────────────────────────────────────────────┘ │
└─────────────────────────────────────────────────────────────┘
```

## Section Details

### Header (16 bytes, fixed)

| Field | Size | Type | Description |
|-------|------|------|-------------|
| magic_number | 8 bytes | bytes | `"SEGMTSST"` — identifies file as SSTable |
| version | 4 bytes | uint32 | Format version (currently 1) |
| entry_count | 4 bytes | uint32 | Total number of key-value entries |

### Data Entry Format

Each entry in the data block:

| Field | Size | Type | Description |
|-------|------|------|-------------|
| key_len | 2 bytes | uint16 | Length of key in bytes |
| key | variable | bytes | The key |
| val_len | 4 bytes | uint32 | Length of value (0 = tombstone) |
| value | variable | bytes | The value (empty if tombstone) |
| seq_no | 8 bytes | uint64 | Sequence number for MVCC |

**Tombstone encoding:** `val_len = 0` indicates a delete marker.

### Sparse Index

The sparse index stores every Nth key (e.g., every 16th) to enable fast seeking:

| Field | Size | Type | Description |
|-------|------|------|-------------|
| index_entry_count | 4 bytes | uint32 | Number of index entries |
| key_len | 2 bytes | uint16 | Length of key |
| key | variable | bytes | The sampled key |
| offset | 8 bytes | uint64 | Byte offset in data section |

**Lookup strategy:**
1. Binary search the sparse index to find the nearest key ≤ target
2. Seek to that offset in the data section
3. Linear scan until target key is found (or passed)

### Bloom Filter

Probabilistic filter to quickly reject keys that don't exist:

| Field | Size | Type | Description |
|-------|------|------|-------------|
| num_hash_funcs | 1 byte | uint8 | Number of hash functions (k) |
| bit_array_size | 4 bytes | uint32 | Size of bit array in bytes |
| bit_array | variable | bytes | The bloom filter bits |

**False positive rate:** Configured at ~1% with optimal k for expected entry count.

### Footer (32 bytes, fixed)

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
5. **On lookup:**
   - Check bloom filter → if negative, key definitely doesn't exist
   - If positive, binary search sparse index for nearest offset
   - Seek to offset, linear scan data entries until found or passed

## Write Path

1. **Write header** with entry count
2. **Write data entries** in sorted order, tracking offsets for index
3. **Build sparse index** (sample every Nth key with its offset)
4. **Build bloom filter** (add all keys)
5. **Write sparse index**, record offset
6. **Write bloom filter**, record offset
7. **Write footer** with offsets and sizes
8. **fsync** to ensure durability

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
HEADER_SIZE = 16
FOOTER_SIZE = 32
SPARSE_INDEX_INTERVAL = 16  # Sample every 16th key
BLOOM_FALSE_POSITIVE_RATE = 0.01  # 1%
```
