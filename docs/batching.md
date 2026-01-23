# WAL Batching Design Document

*Last Updated: January 23, 2026*

This document explains the batching approach used in `WALWriter` and the rationale for choosing entry-count batching over alternatives.

## Final Design

```python
def _background_writer(self):
    while True:
        item = self._queue.get()  # Block until first item
        if item is None:
            return

        batch: list[bytes] = [item]
        while len(batch) < 256:
            try:
                item = self._queue.get_nowait()
                if item is None:
                    self._write_batch(batch)
                    return
                batch.append(item)
            except Empty:
                break

        self._write_batch(batch)
```

**Key parameters:**
- `BATCH_SIZE = 256` entries per flush
- Blocks on first item, then drains queue non-blocking
- Sentinel (`None`) triggers graceful shutdown

---

## Design Iterations

### Iteration 1: Batching Strategy — Time vs Size vs Count

**Problem:** Each `fsync()` call is expensive (~5-10ms on HDD, ~0.5-2ms on SSD). Writing entries one-by-one would severely limit throughput.

**Options considered:**

| Strategy | Pros | Cons |
|----------|------|------|
| Time-based (flush every 2ms) | Bounded latency, good for bursts | Clock-dependent, may flush tiny batches |
| Size-based (flush at 64KB) | Optimal disk alignment | Unpredictable entry count, size tracking overhead |
| Entry-count (flush at N entries) | Simple, predictable memory, no clock deps | Not optimal for variable-size entries |

**Final:** Entry-count batching (256 entries)

**Rationale:**
- Simplest to reason about and implement
- Predictable memory usage — bounded allocation per flush
- No clock dependencies — deterministic behavior
- Clear upper bound on work per flush cycle

---

### Iteration 2: Shutdown Handling — Drain Before Exit

**Problem:** What happens to queued entries when `close()` is called?

**Original concern:** Entries in the queue could be lost on shutdown.

**Final:** Sentinel-based shutdown with drain

```python
# In close():
self._queue.put(None)  # Sentinel

# In worker:
if item is None:
    self._write_batch(batch)  # Flush remaining before exit
    return
```

**Rationale:**
- Sentinel goes into queue, so all entries before it get processed
- Final batch is flushed before worker exits
- No data loss on graceful shutdown

---

## Why 256 Entries?

The batch limit of 256 provides a balance between:

| Concern | Tradeoff |
|---------|----------|
| **Throughput** | Enough entries to amortize `fsync()` cost |
| **Latency** | Not waiting too long to accumulate a full batch |
| **Memory** | Bounded allocation per flush cycle |

At typical entry sizes (~100-500 bytes), this results in ~25-125KB per batch — a reasonable I/O size.

---

## Usage

```python
writer = WALWriter("/path/to/wal")

# Writes are batched automatically
writer.append(entry1)
writer.append(entry2)
writer.append(entry3)

# Graceful shutdown - flushes remaining entries
writer.close()
```
