# Manifest Design Document

*Last Updated: January 26, 2026*

The manifest tracks which SSTables are active and their metadata, enabling the database to know which files to read on startup and during lookups.

## Problem

Without a manifest, the database has no way to know:
- Which SSTable files are valid (vs partial/orphaned)
- What level each SSTable belongs to
- Key ranges for efficient level 1+ lookups

## Design Decision: Atomic JSON vs WAL

| Approach | Pros | Cons |
|----------|------|------|
| **WAL-style** (append log) | Fast incremental updates | Complex replay, compaction needed |
| **Atomic JSON** (rewrite) | Simple, single read on startup | Full rewrite on every change |

**Choice:** Atomic JSON rewrite

**Rationale:**
- Manifest is small (tens of SSTables, not millions)
- Changes are infrequent (only on flush/compaction)
- Simpler implementation, easier debugging
- Can always add WAL-style later if needed

## File Format

```json
{
  "version": 1,
  "next_sstable_id": 42,
  "sstables": [
    {
      "id": 1,
      "filename": "sst-000001.sst",
      "level": 0,
      "min_key": "YWFh",
      "max_key": "enp6",
      "entry_count": 1000,
      "file_size": 65536,
      "created_at": 1706284800
    }
  ]
}
```

**Notes:**
- Keys are base64-encoded for JSON safety (binary keys)
- `next_sstable_id` ensures unique filenames across restarts

## Write Strategy

Same atomic pattern as SSTables:

```python
def save(self, path: Path) -> None:
    temp_path = path.with_suffix(".tmp")
    with open(temp_path, "w") as f:
        json.dump(self.to_dict(), f)
        f.flush()
        os.fsync(f.fileno())
    temp_path.rename(path)
```

## Recovery Strategy

If manifest is corrupted or missing, we can recover by scanning SSTable files:

1. List all `*.sst` files in the data directory
2. Read each file's header to get `level` field
3. Optionally read first/last block to determine key ranges
4. Rebuild manifest from discovered files

This is why we added `level` to `SSTableHeader` — self-describing files enable recovery.

## Lookup Strategy

The manifest enables efficient lookups:

```python
def find_sstables_for_key(self, key: bytes) -> list[SSTableMeta]:
    results = []
    
    # Level 0: Check ALL files (overlapping key ranges)
    for sst in self.level_0_sstables:
        results.append(sst)  # Must check all, newest first
    
    # Level 1+: Binary search to find ONE file per level
    for level in range(1, self.max_level + 1):
        sst = self._find_sstable_in_level(level, key)
        if sst and sst.min_key <= key <= sst.max_key:
            results.append(sst)
    
    return results

def _find_sstable_in_level(self, level: int, key: bytes) -> SSTableMeta | None:
    """Binary search for SSTable containing key in a non-overlapping level."""
    sstables = self.sstables_by_level[level]  # Sorted by min_key
    # Binary search...
```

## Data Model

```python
@dataclass
class SSTableMeta:
    id: int
    filename: str
    level: int
    min_key: bytes
    max_key: bytes
    entry_count: int
    file_size: int
    created_at: int  # Unix timestamp

@dataclass
class Manifest:
    version: int
    next_sstable_id: int
    sstables: list[SSTableMeta]
    
    def add_sstable(self, meta: SSTableMeta) -> None: ...
    def remove_sstable(self, id: int) -> None: ...
    def save(self, path: Path) -> None: ...
    
    @classmethod
    def load(cls, path: Path) -> "Manifest": ...
    
    @classmethod
    def recover_from_files(cls, data_dir: Path) -> "Manifest": ...
```

## Integration Points

1. **On flush:** Add new SSTable to manifest, save
2. **On compaction:** Remove input SSTables, add output SSTables, save
3. **On startup:** Load manifest (or recover), open all SSTable readers
4. **On lookup:** Query manifest for candidate SSTables, then check each

## Key Ranges

Key ranges (`min_key`, `max_key`) are obtained from the writer:
- `min_key` = first entry's key (entries are sorted)
- `max_key` = last entry's key

The writer returns this metadata after writing:

```python
def write(self, path: Path) -> SSTableMeta:
    # ... write file ...
    return SSTableMeta(
        filename=path.name,
        level=self.level,
        min_key=first_key,
        max_key=last_key,
        entry_count=total_entries,
        file_size=path.stat().st_size,
    )
```

## Why Not Store max_key in SSTable Header?

We considered adding `max_key` to the SSTable header for self-describing recovery. Decided against it because:

1. Keys are variable-length — header becomes variable-length
2. Complicates the fixed-size header design
3. Recovery can scan first/last blocks if needed
4. Premature optimization for an edge case

The `level` field (1 byte) in the header is sufficient for basic recovery.
