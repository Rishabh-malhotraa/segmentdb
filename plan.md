Can you plan this project for me, like what do i need to implement and it what order also let me know if i am missing anything

Workspace: Collecting workspace informationHere’s a concise implementation plan, in order:

## Progress Status (Updated: June 6, 2026)

| Component | Status | Notes |
|-----------|--------|-------|
| WAL layer | ✅ Done | WALEntry, WALHeader, WALReader, WALWriter with xxh32 checksums |
| Memtable | ✅ Done | SortedDict-based, flush trigger, tombstone support, peek-then-pop visibility fix |
| Bloom Filter | ✅ Done | rbloom + xxh3_64 for deterministic hashing |
| SSTable Writer | ✅ Done | LZ4 compression, block checksums, atomic writes, returns SSTableMeta |
| SSTable Models | ✅ Done | Header, Entry (with tombstone), Block (iterable), SparseIndex, Footer |
| SSTable Reader | ✅ Done | Opens file, loads bloom/index, get() with lazy block iteration |
| Manifest | ✅ Done | SSTableMeta + Manifest with atomic JSON save, candidates_for_key (L0 scan + L1+ binary search), tests |
| Wire Memtable Flush | ✅ Done | Callback wiring: Memtable → SSTableWriter → Manifest. See docs/memtable-flush-wiring.md |
| Compaction | ⬜ Not Started | Folder structure created |
| Full DB API | ⬜ Next Up | WAL → memtable → SSTable lookup chain, tombstone-aware get |
| Recovery | ⬜ Not Started | |

**Estimated Progress: ~70%**

---

## Implementation Plan

1. **Stabilize WAL layer** ✅
   - Finish reader/writer API atop `segmentdb.storage.wal.WALEntry` and `segmentdb.storage.wal.WALHeader`.
   - Add WAL iteration, fsync semantics, and recovery replayer (scan, CRC verify, idempotent apply).
   - Tests: crash/recovery scenarios, partial/corrupt segments.

2. **Memtable (mutable in-memory index)** ✅
   - Choose data structure: skip list / ordered linked list. Expose put/delete/get, sequence numbers, size accounting.
   - Support flush trigger (size/ops/time).
   - Tests: ordering, overwrites, tombstones, memory thresholds.

3. **SSTable write path (flush memtable to disk)** ✅
   - Define SSTable layout: data blocks, block index, footer with offsets, checksum per block, magic/version.
   - Implement writer: sequential write, block building with restart points.
   - Generate Bloom filter per table for keys.
   - Write minimal manifest/metadata entry for new table.
   - Tests: roundtrip single/multiple entries, tombstones, checksums.

4. **Read path (single SSTable)** ✅
   - Implement SSTable reader: footer parse, block index lookup, Bloom pre-check, block read + binary search.
   - Support tombstones and latest-seq resolution within block.
   - Block implements `__iter__` for lazy entry parsing (reusable like `range()`)
   - Tests: get hits/misses, Bloom positives/negatives, checksum failures.

5. **Segmented storage & manifest**
   - Track active SSTables and their levels in a manifest file (recoverable).
   - Maintain live set ordering by generation/level.
   - Tests: manifest load/save, recovery to consistent set.

6. **Compaction**
   - Implement level-0 to level-N compaction: merge-sort multiple SSTables, drop shadowed keys/tombstones past grace period.
   - Handle overlapping ranges, size-based triggers, and output new tables + manifest update.
   - **Leveled compaction**: ~10x size ratio per level, ~10-30x write amplification, low read amplification
   - **Consider size-tiered alternative**: ~4x write amplification but higher read amp (for write-heavy workloads)
   - Tests: compaction correctness, tombstone purge, crash during compaction (atomic replace).

7. **Get/Read API (full stack)**
   - Lookup order: memtable → immutable (flushing) memtables → L0…Ln SSTables.
   - Use Bloom filter + block index to prune IO.
   - Return value/tombstone with highest sequence.
   - Tests: mixed PUT/DELETE across structures, most-recent-wins.

8. **Write API**
   - Append to WAL, apply to memtable; on flush, rotate WAL segment.
   - Backpressure when WAL/memtable thresholds reached.
   - Tests: durability (fsync), WAL rotation, flush trigger behavior.

9. **Recovery workflow**
   - On startup: load manifest, rebuild table set, replay WAL segments newer than last flush, discard fully flushed WALs.
   - Tests: restart after crashes at various points (before/after flush, during compaction).

10. **Operational extras (optional)**
    - Metrics/logging, config (block size, Bloom fp-rate, compaction thresholds).
    - CLI/HTTP for inspection.
    - Benchmarks for read/write/compaction.

Missing pieces to add:
- ~~WAL writer/reader implementation in `segmentdb.storage.wal`.~~ ✅
- ~~Memtable + flush logic.~~ ✅
- ~~SSTable format (data/index/footer), Bloom filter, manifest.~~ ✅
- ~~SSTableReader with lazy block iteration.~~ ✅
- ~~Manifest (SSTableMeta + atomic JSON + candidates_for_key).~~ ✅
- Wire memtable flush to SSTableWriter + Manifest
- SSTableReader tombstone vs not-found distinction (three-way return for DB get)
- Compaction engine (folder created: `src/segmentdb/compaction/`)
- End-to-end recovery and lookup integration.

## Design Decisions

### Block Size: 4KB
- Aligns with OS page size and SSD pages
- Optimized for point lookups (less data to decompress)
- Tradeoff: Larger sparse index (but index lives in RAM, so acceptable)

### SSTableReader Design
- Pooled and kept warm (no context manager)
- Bloom filter + sparse index loaded into RAM on init
- `__contains__` for membership check
- `_read_exact` as `@staticmethod` to enforce explicit fd positioning

### Compaction Strategy Options
| Strategy | Write Amp | Read Amp | Space Amp | Use Case |
|----------|-----------|----------|-----------|----------|
| Leveled | 10-30x | Low | Low | Read-heavy, point lookups |
| Size-Tiered | 2-4x | High | 2x | Write-heavy |
| Universal | Medium | Medium | Medium | Balanced |

## Next Steps

1. ~~**Implement Manifest**~~ ✅ — SSTableMeta + Manifest with atomic JSON save, L0 scan + L1+ binary search, tests
2. ~~**Wire Memtable flush**~~ ✅ — Callback wiring: Memtable → SSTableWriter (returns SSTableMeta) → Manifest. Required on_flush callback, no silent no-ops. See docs/memtable-flush-wiring.md
3. **Full DB class** — Unified API: WAL → memtable → SSTable lookup chain, tombstone-aware get
4. **Recovery** — Load manifest, replay WAL entries > last_seq_no, rebuild state
5. **Implement Compaction** — Start with leveled, k-way merge, atomic manifest swap



---

https://chatgpt.com/s/t_696eea69f6088191ad38c1ba60cd3fd4