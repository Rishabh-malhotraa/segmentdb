# MemtableEntry Design

This document explains the design choices behind `MemtableEntry`, which represents a single key value entry stored in the memtable.
## Final Design

```python
@dataclass(slots=True, frozen=True)
class MemtableEntry:
    seq_no: int
    value: bytes | None = None  # None = tombstone (delete marker)

    @property
    def is_tombstone(self) -> bool:
        return self.value is None
```

## Design Decisions

### 1. Using `value: bytes | None` Instead of Separate `tombstone` Field

**Rejected approach:**
```python
class MemtableEntry:
    value: bytes
    seq_no: int
    tombstone: bool = False  # Redundant field
```

**Chosen approach:**
```python
class MemtableEntry:
    seq_no: int
    value: bytes | None = None  # None indicates tombstone
```

**Rationale:**
- Saves 1 byte per entry (bool with slots = 1 byte)
- With millions of entries: `1M × 1 byte = ~1 MB` saved
- Eliminates redundancy — `tombstone=True` with non-empty `value` would be inconsistent
- Tombstone check is simple: `if entry.value is None`

### 2. Using `slots=True`

```python
@dataclass(slots=True)
```

**Rationale:**
- Prevents `__dict__` creation per instance
- Reduces memory footprint significantly (~40-50% for small objects)
- Faster attribute access
- Trade-off: Cannot add attributes dynamically (acceptable for this use case)

### 3. Using `frozen=True`

```python
@dataclass(slots=True, frozen=True)
```

**Rationale:**
- Entries are immutable after creation — matches the semantic model (a write is a new entry, not a mutation)
- Prevents accidental modification bugs
- Makes entries hashable (useful for debugging, testing, and potential future use cases)
- No significant memory overhead

### 4. Using `@property` for `is_tombstone`

```python
@property
def is_tombstone(self) -> bool:
    return self.value is None
```

**Rationale:**
- Properties are stored on the **class**, not per-instance — zero memory overhead
- Provides a readable, self-documenting API: `if entry.is_tombstone`
- Encapsulates the tombstone representation — if the convention changes, only one place to update
- More explicit than checking `value is None` throughout the codebase

### 5. Using `bytes | None` vs `Optional[bytes]`

**Chosen:** `bytes | None`

**Rationale:**
- Semantically clearer: `None` has meaning (tombstone), not just "unset"
- `Optional[X]` connotes "parameter can be omitted"
- `X | None` is neutral, just states the union type
- Modern Python 3.10+ syntax, no import needed

## Memory Layout

```
MemtableEntry instance (with slots):
┌──────────────┬─────────────────────────┐
│ seq_no       │ 8 bytes (int)           │
│ value        │ 8 bytes (pointer/None)  │
└──────────────┴─────────────────────────┘
Total: ~16 bytes + value data (if not None)

Without slots: +56 bytes for __dict__
With tombstone bool: +1 byte per entry
Property: 0 bytes (stored on class, not instance)
```

## Usage

```python
# PUT operation
entry = MemtableEntry(seq_no=1, value=b"hello")

# DELETE operation (tombstone)
entry = MemtableEntry(seq_no=2, value=None)

# Check if tombstone
if entry.is_tombstone:
    # Handle delete marker
```
