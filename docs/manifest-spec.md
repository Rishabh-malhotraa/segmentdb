# Manifest Specification

*Last Updated: June 3, 2026*

The manifest is the database's source of truth for **which SSTable files are
live right now** — at what level, over what key range, and up to what sequence
number. This document explains, from first principles, why that record has to
exist, then specifies the public API and the functionality it must provide.

---

## Part 1 — Why a manifest is needed

### The underlying problem

SSTables are immutable, but the *set* of them is not. The database is, at any
instant, nothing more than **the current collection of live SSTables**. That
collection changes constantly:

- every memtable **flush** creates a new SSTable,
- every **compaction** creates new SSTables and retires old ones.

So the central question the storage engine must always be able to answer is:
**"which files make up the database at this moment?"** Everything else — reads,
recovery, compaction — depends on having a trustworthy answer.

### Why we can't just list the directory

The obvious idea is to skip the bookkeeping and just look at which `.sst` files
are on disk. That fails for three fundamental reasons:

1. **Crashes leave junk behind.** A flush might write a file and die before it
   was meant to "count." A compaction might write three new files, delete two
   old ones, and crash halfway. The directory then holds a mix of live, dead,
   and half-written files — and a listing **cannot tell them apart**. Reading
   such a set would surface duplicated or partial data.

2. **The filesystem doesn't store what reads need.** To look up a key at level
   1+, the engine must find the one file whose key range covers it. That needs
   each file's **min_key / max_key, level, and seq range** — none of which the
   directory provides. You'd have to open and partially read every file on
   startup just to reconstruct it.

3. **No ordering or stable identity.** Newest-wins resolution needs to know the
   relative age and level of files, and we need a counter that never reuses a
   filename across restarts. A directory gives none of that reliably.

### The real reason: atomic state transitions

Here is the deepest justification. A flush or compaction changes **several files
at once** (add N, remove M), but the filesystem offers only **one** atomic
operation: `rename`. There is no way to atomically "add three files and delete
two."

The manifest solves this by funneling the entire multi-file change into a
**single atomic pointer swap**: write the new manifest to a temp file, fsync it,
then `rename` it over the old one. Because rename is atomic on POSIX, at every
instant a reader sees **either** the complete old state **or** the complete new
state — never a half-applied mix.

> The manifest is the database's **commit point**. New SSTables physically exist
> on disk beforehand, but they only become *part of the database* the instant
> the manifest rename succeeds. It converts "many risky file operations" into
> "one atomic commit."

### What depends on it

| Consumer | Needs the manifest for |
|----------|------------------------|
| **Flush** | A place to publish the new SSTable so it "counts." |
| **Reads** | Per-file key ranges to prune which files to probe. |
| **Compaction** | An atomic swap of input files for output files. |
| **Recovery** | The live file set + the seq_no boundary for WAL replay. |

Because all four are blocked on it, the manifest is the keystone of the storage
engine.

---

## Part 2 — How it works (the model)

The manifest is a small **in-memory object** that is queried constantly and
**committed to disk atomically** only when the live set changes.

- **Reads/queries hit RAM only.** The live set is small enough to live entirely
  in memory; lookups never touch the file.
- **Mutations change RAM only.** Adding or removing an SSTable edits an in-memory
  dictionary; nothing is persisted yet.
- **`save()` is the only thing that writes**, and it does so atomically.

The canonical in-memory store is a `dict[id -> SSTableMeta]`, keyed by each
SSTable's **immutable id**. Level and key-range groupings (needed by reads) are
**derived on demand** from that dict, because a file's level changes over its
lifetime while its id never does. Storage is organized around identity; views
are derived from state.

### The commit rhythm

Every mutating event — flush or compaction — follows the same three beats:

```
1. BEFORE : make the referenced .sst files durable on disk (write + fsync).
2. COMMIT : mutate the manifest in RAM, then save() (temp -> fsync -> rename).
3. AFTER  : clean up — drop the flushed memtable / delete retired files /
            checkpoint the WAL.
```

Data is made durable **before** the commit; cleanup happens **after** it. The
`save()` rename is the single instant at which the new state becomes truth.

---

## Part 3 — Data model

```python
@dataclass(slots=True, frozen=True)
class SSTableMeta:
    id: int            # unique, monotonic identity (never reused)
    filename: str      # e.g. "sst-000042.sst"
    level: int         # LSM level (0 = freshest flushes)
    min_key: bytes     # first key (sorted) — base64 in JSON
    max_key: bytes     # last key  (sorted) — base64 in JSON
    min_seq_no: int    # seq range of entries in this file
    max_seq_no: int
    entry_count: int
    file_size: int
    created_at: int    # unix seconds (informational)

    def covers(self, key: bytes) -> bool:
        return self.min_key <= key <= self.max_key
```

One `SSTableMeta` describes one live file. It is **metadata, not data** — it
summarizes the file enough for read pruning and recovery, but the keys/values
live in the `.sst` file itself.

---

## Part 4 — Public API

### Lifecycle

```python
@classmethod
def load(cls, data_dir: Path) -> "Manifest"
```
Build the in-memory store from the `MANIFEST` file. Returns an **empty** manifest
if no file exists (fresh database). Raises if the file exists but is malformed
(the caller may then fall back to recovery by scanning SSTable files).

```python
def save(self, data_dir: Path) -> None
```
Atomically persist the current state: write `MANIFEST.tmp`, `flush` + `fsync`,
`rename` over `MANIFEST`, then fsync the directory so the rename itself survives
a crash. This is the **commit point**.

### Mutation (in-memory; persisted by a following `save()`)

```python
def allocate_id(self) -> int
```
Return a fresh, never-reused id and advance the counter. Used to name a new
SSTable before it is written.

```python
def add_sstable(self, meta: SSTableMeta) -> None
```
Register a newly written SSTable as live, and advance `last_seq_no` to cover it.
Raises if the id is already present.

```python
def remove_sstable(self, sstable_id: int) -> None
```
Drop an SSTable from the live set (it ceases to "count"). Does **not** delete the
file from disk — that is the caller's job, *after* the next `save()`.

```python
def replace(self, remove_ids: list[int], add: list[SSTableMeta]) -> None
```
Remove a set of inputs and add a set of outputs as a **single in-memory state
change** — the atomic swap that compaction commits with one `save()`.

### Read-path support (query only; never mutate or touch disk)

```python
def candidates_for_key(self, key: bytes) -> list[SSTableMeta]
```
Return the SSTables that may contain `key`, in **newest → oldest** priority so
the DB `get` can stop at the first real value or tombstone:

1. **Level 0** — overlapping ranges, so return *every* covering file, newest
   first (highest id).
2. **Level 1+** — non-overlapping ranges, so binary-search `min_key` to return
   **at most one** covering file per level, in ascending level order.

```python
def sstables_at(self, level: int) -> list[SSTableMeta]   # sorted by min_key
@property
def sstables(self) -> list[SSTableMeta]                  # all, by id (oldest first)
@property
def max_level(self) -> int
```
Derived views over the in-memory store, used by reads, compaction selection, and
serialization.

---

## Part 5 — Functionality requirements

The manifest MUST provide:

1. **Authoritative liveness.** A file is part of the database **iff** it appears
   in the committed manifest. Files on disk but absent from it are orphans, to be
   ignored (and optionally garbage-collected later).
2. **Atomic commits.** Every state change becomes visible all-or-nothing via
   temp-write + fsync + rename. Readers never see a partial update.
3. **Durability ordering.** Every SSTable the committed manifest references must
   already be fully written and fsynced *before* that manifest is committed.
   Retired files are deleted only *after* the commit.
4. **Monotonic identity.** `next_sstable_id` strictly increases and is never
   reused across restarts, so filenames are globally unique forever.
5. **Level invariant.** Level 0 may have overlapping key ranges; every level ≥ 1
   must be non-overlapping and orderable by `min_key` (what makes the L1+ binary
   search valid).
6. **Recovery boundary.** Track `last_seq_no` — the highest seq_no durably
   reflected in any live SSTable — so recovery knows which WAL entries to replay.
7. **Read pruning.** Given a key, return the minimal, correctly ordered set of
   candidate files to probe.
8. **Crash consistency at every step.** A crash before commit leaves the prior
   manifest valid plus some orphan files; a crash after commit but before
   cleanup leaves stale files as harmless orphans. No state is ever half-applied.

---

## Part 6 — On-disk format

A single UTF-8 JSON file named `MANIFEST` (no extension) in the data directory.

```json
{
  "format_version": 1,
  "next_sstable_id": 42,
  "last_seq_no": 13371,
  "sstables": [
    {
      "id": 1,
      "filename": "sst-000001.sst",
      "level": 0,
      "min_key": "YWFh",
      "max_key": "enp6",
      "min_seq_no": 1,
      "max_seq_no": 1000,
      "entry_count": 1000,
      "file_size": 65536,
      "created_at": 1706284800
    }
  ]
}
```

The top-level fields (`format_version`, `next_sstable_id`, `last_seq_no`) are
document-level bookkeeping owned by the `Manifest`; each element of `sstables`
is one `SSTableMeta`. Keys are arbitrary bytes, so `min_key` / `max_key` are
**base64-encoded** on write and decoded on load; all other values are plain JSON.

**Why atomic-JSON-rewrite rather than an append log?** The manifest is small
(tens to hundreds of files) and changes infrequently (only on flush/compaction),
so a full rewrite per change is cheap and far simpler to implement and debug than
a replayable edit log. An append-style manifest can be added later if the file
count ever grows large enough to matter.

---

## Part 7 — Integration points

| Event | Manifest sequence |
|-------|-------------------|
| **Startup** | `load()` (or recover by scanning files if corrupt); open an `SSTableReader` per entry. |
| **Flush** | `id = allocate_id()` → write + fsync SSTable → `add_sstable(meta)` → `save()` → drop the flushed memtable → checkpoint WAL at `max_seq_no`. |
| **Compaction** | write + fsync output files → `replace(remove_ids, add=outputs)` → `save()` → delete retired input files. |
| **Read** | `candidates_for_key(key)` → probe each reader in newest→oldest order, stopping at the first value or tombstone. |

### Tie-in with the memtable flush

The memtable's `_flush_worker` keeps a `FlushTask` in its deque until the data is
durable, then pops it. That ordering extends through the manifest:

```
write SSTable (fsync) -> add_sstable + save (commit)
   -> THEN pop the FlushTask from the deque
   -> THEN checkpoint the WAL at max_seq_no
```

This preserves the invariant **"a written key is always findable somewhere"** —
it lives in the deque until the manifest commit makes it findable via the
SSTable, with only brief, harmless double-visibility in between.

---

## Part 8 — Recovery

```
1. Try Manifest.load(data_dir).
     - missing -> start empty (fresh DB).
     - corrupt -> rebuild by scanning *.sst headers (level) and first/last
                  blocks (key + seq ranges).
2. Open an SSTableReader for every listed SSTable.
3. Replay WAL entries with seq_no > last_seq_no into a fresh memtable.
4. Discard WAL segments fully covered by last_seq_no.
```

`last_seq_no` is the bridge between durable SSTables and the WAL: everything at
or below it is safely in an SSTable; everything above it must be replayed.

---

## Part 9 — Concurrency & out of scope

- **Concurrency.** All mutations and `save()` calls MUST be serialized by the
  caller (the DB holds a single manifest lock); the class does no internal
  locking. Reads operate over the in-memory snapshot.
- **Out of scope (future work):** append-style incremental manifest; storing
  `max_key` in the SSTable header (rejected — it would make the fixed header
  variable-length); multi-version manifest history / point-in-time snapshots.
