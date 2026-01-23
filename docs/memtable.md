# Memtable Design Document

*Last Updated: January 23, 2026*

This document explains the design choices behind `Memtable`, `MemtableEntry`, and `FlushTask`, capturing the iterations and decisions made during development.

## Final Design

### MemtableEntry

```python
@dataclass(slots=True, frozen=True)
class MemtableEntry:
    seq_no: int
    value: bytes | None = None  # None = tombstone (delete marker)

    @property
    def size_bytes(self) -> int:
        return 8 + (len(self.value) if self.value else 0)
```

### FlushTask

```python
@dataclass(slots=True, frozen=True)
class FlushTask:
    store: SortedDict
    checkpoint_seq_no: int
```

### Memtable

```python
class Memtable:
    MAX_SIZE_BYTES = 4 * 1024 * 1024  # 4MB
    
    _store: SortedDict
    _size_bytes: int
    _immutable_stores: deque[FlushTask | None]
    _flush_signal: Condition
    _thread: Thread
```

---

## Design Iterations

### Iteration 1: Condition Wait Loop — `while` vs `wait_for`

**Original (hard to read):**
```python
while (self._immutable_stores or self._shutdown_event.is_set()) == False:
    self._flush_signal.wait()
```

**Improved (using `not`):**
```python
while not self._immutable_stores and not self._shutdown_event.is_set():
    self._flush_signal.wait()
```

**Final (using `wait_for`):**
```python
self._flush_signal.wait_for(lambda: self._immutable_stores)
```

**Rationale:**
- `wait_for(predicate)` is equivalent to `while not predicate(): wait()`
- The predicate returns `True` when we should **stop** waiting
- Cleaner, less error-prone, built-in spurious wakeup handling

---

### Iteration 2: Shutdown Mechanism — Event vs Sentinel

**Original (using Event):**
```python
self._shutdown_event = Event()

# In close():
self._shutdown_event.set()
self._flush_signal.notify()

# In worker:
while not self._immutable_stores and not self._shutdown_event.is_set():
    self._flush_signal.wait()
# ... complex logic to handle remaining stores on shutdown
```

**Problem:** Race condition — if shutdown is set while worker is flushing, newly queued stores could be lost.

**Final (using sentinel):**
```python
# In close():
self._immutable_stores.append(None)  # Sentinel
self._flush_signal.notify()

# In worker:
self._flush_signal.wait_for(lambda: self._immutable_stores)
task = self._immutable_stores.popleft()

if task is None:
    return  # Shutdown
```

**Rationale:**
- Single synchronization mechanism — just the queue + condition
- No race conditions — sentinel goes into the queue, so all stores queued before it get flushed first
- Simpler logic — pop one item, if it's the sentinel exit, otherwise flush it
- Guarantees ordering: everything added before `close()` is processed before shutdown

---


### Iteration 3: Checkpointing via Sequence Number

**Addition:** `FlushTask` now includes `checkpoint_seq_no`

**Rationale:**
- WAL stores every write for durability, but can't grow infinitely
- After memtable is flushed to SSTable, those WAL entries are redundant (data is durable on disk)
- `checkpoint_seq_no` is the highest seq_no in the flushed batch
- After successful flush, WAL can be truncated up to this sequence number
- On crash recovery: replay WAL only from the checkpoint, not from the beginning

```
WAL:     [1] [2] [3] [4] [5] [6] [7] [8]
                          ↑
                   checkpoint_seq_no = 5
                   
After checkpoint:
WAL:                      [6] [7] [8]   ← entries 1-5 truncated
```

---

## Thread Safety Analysis

### Shared State
- `_store` — active memtable (main thread writes, both threads read)
- `_size_bytes` — size tracking (main thread only)
- `_immutable_stores` — queue of stores to flush (both threads access)

### Why `_immutable_stores` Needs Synchronization

```
Main thread (get):                      Flush thread:
─────────────────                       ─────────────
for task in reversed(_immutable_stores):
    │                                   
    │  ← iterating                      _immutable_stores.popleft()
    │                                   ← modifies deque
    ↓ iterator corrupted → crash or undefined behavior
```

### Lock Scope — Keep It Brief

```python
def _flush_worker(self):
    with self._flush_signal:
        task = self._immutable_stores.popleft()  # Lock held: microseconds
    
    # Lock released! Reads/writes can proceed
    for key, entry in task.store.items():
        write_to_sstable(key, entry)  # Slow I/O without lock
```

This matches RocksDB/LevelDB design — immutable stores are read-only after rotation, so the flush thread can read them without holding the lock during I/O.

---

## Memory Layout

```
MemtableEntry instance (with slots):
┌──────────────┬─────────────────────────┐
│ seq_no       │ 8 bytes (int reference) │
│ value        │ 8 bytes (reference)     │ → bytes object (variable size) or None
└──────────────┴─────────────────────────┘

bytes object (pointed to by value):
┌──────────────┬─────────────────────────┐
│ PyObject hdr │ ~16 bytes               │
│ length       │ 8 bytes                 │
│ data         │ N bytes (actual content)│
└──────────────┴─────────────────────────┘

Total per entry: ~16 bytes (MemtableEntry) + ~24+N bytes (bytes object)
If tombstone (value=None): ~16 bytes only

Without slots: +56 bytes for __dict__
```

---

## MemtableEntry Design Decisions

### Using `value: bytes | None` Instead of Separate `tombstone` Field

**Rejected:**
```python
class MemtableEntry:
    value: bytes
    tombstone: bool = False  # Redundant field
```

**Chosen:**
```python
class MemtableEntry:
    value: bytes | None = None  # None indicates tombstone
```

**Rationale:**
- Saves 1 byte per entry (bool with slots = 1 byte)
- Eliminates redundancy — `tombstone=True` with non-empty `value` would be inconsistent
- Tombstone check is simple: `if entry.value is None`

### Using `slots=True` and `frozen=True`

- `slots=True`: Prevents `__dict__` creation, reduces memory ~40-50%
- `frozen=True`: Entries are immutable after creation, prevents bugs, makes entries hashable

### Using `bytes | None` vs `Optional[bytes]`

- `bytes | None` is semantically clearer: `None` has meaning (tombstone)
- Modern Python 3.10+ syntax, no import needed

---

## Usage

```python
# Create memtable
memtable = Memtable()

# PUT operation
memtable.put(b"key", MemtableEntry(seq_no=1, value=b"hello"))

# DELETE operation (tombstone)
memtable.delete(b"key", MemtableEntry(seq_no=2, value=None))

# GET operation
entry = memtable.get(b"key")

# Graceful shutdown (flushes remaining stores)
memtable.close()
```
