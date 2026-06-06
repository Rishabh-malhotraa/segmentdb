# Day 1: Wire Memtable Flush

*Completed: June 6, 2026*

This document captures the design decisions and implementation for connecting `Memtable._flush_worker()` to `SSTableWriter` + `Manifest`, completing the data path from in-memory writes to durable on-disk storage.

## Problem

Before this change, the memtable flush worker iterated every entry in the immutable store and **did nothing**:

```python
for key, entry in task.store.items():
    # TODO: Write to SSTable
    # TODO: Add key to bloom filter
    # TODO: Update sparse index
    pass

# TODO: After successful flush, checkpoint the WAL
```

The data silently vanished after every flush. The database was a write-only black hole.

## Solution

Wire the flush worker to a callback that executes the full durability pipeline:

```
allocate_id → write SSTable → add_sstable → save manifest → popleft → (checkpoint WAL)
```

---

## Files Changed

### 1. `src/segmentdb/storage/memtable/Memtable.py`

**Change:** Accept a required `on_flush` callback; call it from `_flush_worker`.

**Before:**
```python
def __init__(self):
    ...

def _flush_worker(self):
    ...
    for key, entry in task.store.items():
        pass  # TODOs
```

**After:**
```python
def __init__(self, on_flush: Callable[[FlushTask], None]):
    self._on_flush = on_flush
    ...

def _flush_worker(self):
    ...
    self._on_flush(task)
```

**Design decision — required, not optional:**

The callback was initially `on_flush: Callable | None = None` with a `lambda _: None` fallback. We removed the default because a silent no-op is exactly the bug we're fixing — data disappearing with no error. Making it required forces callers to be explicit:

- Database passes `self._flush_to_sstable`
- Tests pass `lambda _: None` (intentional, visible)

Fail at construction, not silently at flush time.

### 2. `src/segmentdb/storage/sstable/SSTableWriter.py`

**Changes:**
- Constructor now takes `sst_id: int` as first argument (needed for `SSTableMeta`)
- `write()` returns `SSTableMeta` instead of `None`

**Before:**
```python
def __init__(self, store: SortedDict, level: int = 0):
    ...

def write(self, path: Path) -> None:
    # ... write file ...
```

**After:**
```python
def __init__(self, sst_id: int, store: SortedDict, level: int = 0):
    self.sst_id = sst_id
    ...

def write(self, path: Path) -> SSTableMeta:
    # ... write file ...
    keys = self.store.keys()
    seq_nos = [entry.seq_no for entry in self.store.values()]
    return SSTableMeta(
        id=self.sst_id, filename=path.name, level=self.level,
        min_key=keys[0], max_key=keys[-1],
        min_seq_no=min(seq_nos), max_seq_no=max(seq_nos),
        entry_count=entry_count, file_size=path.stat().st_size,
        created_at=int(time.time()),
    )
```

**Rationale:** The writer already has all the information to build metadata (keys, seq_nos, file size, entry count). Returning it avoids redundant iteration in the caller and keeps metadata construction co-located with the write.

### 3. `src/segmentdb/db.py`

**Change:** Wire `Manifest`, `Memtable`, and the flush callback together.

```python
class Database:
    SSTABLE_DIR_NAME = "sstables"

    def __init__(self, data_dir: str):
        self.data_dir = Path(data_dir)
        self._sstable_dir = self.data_dir / self.SSTABLE_DIR_NAME
        self._sstable_dir.mkdir(parents=True, exist_ok=True)

        self._manifest = Manifest.load(self.data_dir)
        self._memtable = Memtable(on_flush=self._flush_to_sstable)
        self._closed = False

    def _flush_to_sstable(self, task: FlushTask) -> None:
        sst_id = self._manifest.allocate_id()
        path = self._sstable_dir / f"sst-{sst_id:06d}.sst"

        meta = SSTableWriter(sst_id, task.store, level=0).write(path)

        self._manifest.add_sstable(meta)
        self._manifest.save(self.data_dir)

        # TODO: Checkpoint WAL at task.checkpoint_seq_no
```

The method is a clean 3-step pipeline:
1. **Allocate** — unique SSTable ID from manifest
2. **Write** — atomic SSTable file (tmp → fsync → rename), get metadata back
3. **Register** — add to manifest, persist manifest atomically

### 4. `src/segmentdb/storage/memtable/__init__.py`

**Change:** Export `FlushTask` so `db.py` and tests can import it.

```python
from .Memtable import FlushTask, Memtable, MemtableEntry
```

---

## Data Flow After This Change

```
User: db.put(b"key", b"value")
       │
       ▼
  ┌──────────┐
  │ Memtable  │  SortedDict, accumulates writes
  └──────────┘
       │  size >= threshold
       ▼
  ┌──────────────┐
  │ Rotate store  │  active → immutable (FlushTask), notify worker
  └──────────────┘
       │  background thread wakes
       ▼
  ┌──────────────┐
  │ on_flush()    │  = Database._flush_to_sstable
  └──────────────┘
       │
       ├─ 1. manifest.allocate_id()  → sst_id=1
       │
       ├─ 2. SSTableWriter(1, store, level=0).write("sst-000001.sst")
       │      └─ builds blocks, sparse index, bloom filter
       │      └─ writes tmp → fsync → rename (atomic)
       │      └─ returns SSTableMeta
       │
       ├─ 3. manifest.add_sstable(meta)
       │      manifest.save()  → MANIFEST.tmp → fsync → rename
       │
       └─ 4. popleft()  ← safe: data is durable on disk
```

## Tests Added

File: `tests/segmentdb/storage/memtable/test_memtable_flush.py`

### Unit-level (no threads)

| Test | Verifies |
|------|----------|
| `test_sstable_created_on_disk` | Flush produces a non-empty `.sst` file |
| `test_all_keys_readable` | All 200 keys readable via SSTableReader |
| `test_tombstones_readable` | Tombstone entries survive roundtrip |
| `test_missing_key_returns_none` | Non-existent key → `None` |
| `test_manifest_has_one_entry` | Manifest contains exactly 1 SSTable after flush |
| `test_manifest_metadata_correct` | Level, key range, seq range, entry count are correct |
| `test_manifest_persists_to_disk` | Manifest survives reload from disk |
| `test_multiple_flushes_unique_ids` | 3 flushes → 3 unique SSTable IDs |

### Integration (with threads + Database)

| Test | Verifies |
|------|----------|
| `test_flush_triggered_by_size` | Writes past threshold → SSTable files + manifest entries appear |
| `test_data_readable_after_flush` | Flushed keys are correctly readable from SSTable files |

---

## Still TODO

- **Checkpoint WAL** at `task.checkpoint_seq_no` after successful flush (marked with `# TODO` in `db.py`)
- **Database.put/get/delete** — full API wiring (WAL → memtable → SSTable lookup chain)
- **SSTableReader tombstone distinction** — `get()` currently returns `None` for both "not found" and "deleted" (need three-way return for the DB read path)
