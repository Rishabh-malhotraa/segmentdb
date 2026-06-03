# SSTable Compression & Checksums Design Document

*Last Updated: June 3, 2026*

Why SSTable data blocks are compressed and checksummed — and when each is
actually necessary. This documents the *decision* to compress and checksum,
as distinct from [sstable-block-compression.md](sstable-block-compression.md),
which compares compression *algorithms*.

## Final Design

Each SSTable block is independently LZ4-compressed and carries an xxh32
checksum over its size header plus compressed payload.

```python
# Block.to_bytes() — write path
header = struct.pack(">II", len(self.data), self.uncompressed_size)
checksum = xxhash.xxh32(header + self.data).intdigest()
footer = struct.pack(">I", checksum)
return header + self.data + footer

# Block.from_bytes() — read path verifies BEFORE decompressing
computed_checksum = xxhash.xxh32(header + compressed_data).intdigest()
if stored_checksum != computed_checksum:
    raise ValueError("Block checksum mismatch: ...")
```

**Key points:**

- **Compression is an optimization** (cost: space/speed). It is optional and
  could be made pluggable per-table.
- **Checksums are a correctness mechanism** (trust: integrity). They are
  effectively mandatory and should never be disabled.
- Both are applied **per block (~4KB)**, which is the granularity that makes
  compression worthwhile and keeps checksum verification localized.
- The checksum is verified **before** decompression, because a single flipped
  bit in an LZ4 stream can corrupt the entire decompressed output.

---

## Design Iterations

### Iteration 1: Do we need block compression at all?

**Problem:** Compression adds CPU cost and complexity. Is an uncompressed
SSTable good enough?

**Decision:** Keep compression as the default, but understand it is optional.

**Rationale:**

| Buys | Costs |
|------|-------|
| Less disk space (2–4x on typical KV data) | CPU on every read/write |
| Fewer bytes read from disk (often the bottleneck) | Whole block must decompress to read one entry |
| LZ4 decompresses at multiple GB/s — near free | Added complexity |

- Disk I/O usually dominates over CPU, so reading a smaller compressed block
  and decompressing in RAM is frequently **faster** than reading a larger raw
  block.
- Compression only pays off because it is done **per block**. Per-entry would
  find too little redundancy; per-file would force decompressing the whole file
  for a single key.
- Industry default: RocksDB, LevelDB, Cassandra all compress blocks by default.

**When to turn it off:** high-entropy or already-compressed data (UUIDs,
hashes, JPEGs). Then compression buys ~nothing and is pure CPU cost.

---

### Iteration 2: Do we need block checksums?

**Problem:** Checksums add 4 bytes/block and a hash computation. Are they worth
it?

**Decision:** Keep checksums always. They are not negotiable.

**Rationale:**

- **Disks suffer silent corruption** — bit rot, bad sectors, firmware bugs,
  torn writes on power loss. The disk returns wrong bytes *without any error*.
- Without a checksum, the DB deserializes garbage and either returns a wrong
  value or crashes far away with a confusing error. The checksum turns silent
  corruption into a **loud, immediate, localized failure**.
- **Compression raises the stakes:** one flipped bit in a compressed stream can
  produce completely wrong output. So integrity must be verified *before*
  decompression.
- **Nearly free:** xxh32 runs at many GB/s — far faster than the disk — for
  4 bytes/block of overhead.

---

## Decision Summary

| Feature | Needed? | Category | Why |
|---------|---------|----------|-----|
| Block compression | Optional (good default) | Cost (space/speed) | Saves disk + I/O; near-free with LZ4; worthwhile only because applied per block. Skip for high-entropy data. |
| Block checksums | Keep — effectively mandatory | Trust (correctness) | Detects silent disk corruption; turns garbage into a loud error; critical *because* we compress; basically free with xxh32. |

**Mental model:** Compression is about *cost*; checksums are about *trust*.
You can negotiate on cost. You should not negotiate on trust.

---

## Possible Future Work

- Make compression pluggable via a `compression: none | lz4` field recorded in
  `SSTableHeader`, so already-compressed data can skip the CPU cost per table.
