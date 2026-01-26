# SSTable Block Compression

*Last Updated: January 26, 2026*

Comparison of compression algorithms for SSTable data blocks.

## Final Choice: LZ4 (Level 4)

```python
lz4.block.compress(data, mode="high_compression", compression=4)
```

---

## Algorithm Comparison

| Algorithm | Decompress Speed | Compress Speed | Ratio | Used By |
|-----------|------------------|----------------|-------|---------|
| **LZ4** | **~4 GB/s** | ~500 MB/s | 2-3x | RocksDB, Cassandra, ClickHouse |
| Zstandard | ~1.5 GB/s | ~300 MB/s | 3.5-4.5x | MySQL 8.0, newer RocksDB |
| Snappy | ~1.5 GB/s | ~500 MB/s | 2-2.5x | LevelDB, Bigtable |
| zlib | ~400 MB/s | ~50 MB/s | 3-4x | General purpose |

### Why LZ4?

1. **Fastest decompression** (~10x faster than zlib)
2. **Reads >> Writes** - SSTable reads are frequent, writes are rare
3. **Industry standard** - proven in production databases
4. **Simple API** - no tuning complexity

### Why Not Zstandard?

Zstd has better compression ratio, but:
- LZ4 decompression is ~2.5x faster
- For 4KB blocks, ratio difference is minimal (~5-10%)
- Extra dependency complexity not worth it

---

## LZ4 Compression Levels

| Level | Compress Speed | Ratio | Use Case |
|-------|----------------|-------|----------|
| 1-3 | Fastest | Lower | Real-time streaming |
| **4** | Fast | Good | **SSTables (RocksDB default)** |
| 6 | Medium | Better | General purpose |
| 9-12 | Slow | Best | Archival, cold storage |

### Why Level 4?

- **Decompression speed is identical** at all levels
- ~30% faster compression than level 9
- Only ~5% larger output than level 9
- Memtable flushes should be fast

---

## LZ4 Modes

```python
# Fast mode - prioritizes compression speed
lz4.block.compress(data, mode="default")

# HC mode - prioritizes compression ratio
lz4.block.compress(data, mode="high_compression", compression=4)
```

| Mode | Algorithm | Compress Speed | Ratio |
|------|-----------|----------------|-------|
| `default` | LZ4 fast | ~500 MB/s | Lower |
| `high_compression` | LZ4-HC | ~50-100 MB/s | Better |

**We use `high_compression`** because:
- SSTable writes are infrequent (memtable flush)
- Smaller files = less disk I/O on reads
- Decompression speed is **identical** for both modes

---

## Compressed Block Format

Self-describing format with CRC integrity check:

```
┌────────────────────┬────────────────────┬─────────────────────────────┬──────────┐
│ compressed_size    │ uncompressed_size  │ compressed_data             │ CRC32    │
│ (4 bytes, BE)      │ (4 bytes, BE)      │ (variable)                  │ (4 bytes)│
└────────────────────┴────────────────────┴─────────────────────────────┴──────────┘
         ↑                    ↑                                               ↑
   Lets you read         For LZ4 to                               Validates header
   exact bytes           pre-allocate                              + compressed_data
```

### Why Self-Describing?

| Approach | Recovery | Overhead |
|----------|----------|----------|
| Index-only | ❌ If index corrupts, data lost | 0 bytes |
| **Self-describing + CRC** | ✅ Can scan & rebuild index | **12 bytes/block** |

- **compressed_size first** - know how many bytes to read
- **CRC32 covers header + data** - catches any corruption
- **12 bytes overhead** per 4KB block = 0.3%
- Used by RocksDB, LevelDB for exactly this reason

### Read Flow

```python
# Even without index, can sequentially scan:
offset = HEADER_SIZE
while offset < footer_offset:
    f.seek(offset)
    block = Block.from_file(f)  # Self-describing!
    offset += block.total_size
```

---

## Typical Compression Ratios (4KB blocks)

| Data Type | LZ4 | zlib |
|-----------|-----|------|
| JSON/Text | 2.5-3.5x | 4-5x |
| Binary KV | 2-3x | 3-4x |
| Random bytes | ~1x | ~1x |

For a 4KB uncompressed block:
- **LZ4**: ~1500-1800 bytes
- **zlib**: ~1100-1400 bytes

The ~300 byte difference per block is negligible compared to 10x faster reads.
