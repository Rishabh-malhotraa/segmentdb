# WAL Batching Strategy

This document explains the batching approach used in `WALWriter` and the rationale for choosing entry-count batching over alternatives.

## Problem

Each `fsync()` call is expensive (~5-10ms on HDD, ~0.5-2ms on SSD). Writing entries one-by-one would severely limit throughput. Batching amortizes the `fsync()` cost across multiple entries.

## Options Considered

### 1. Time-Based Batching

Flush all records that arrive within a fixed time window (e.g., 2ms).

```python
deadline = time.perf_counter() + 0.002  # 2ms window
while time.perf_counter() < deadline:
    try:
        batch.append(q.get_nowait())
    except queue.Empty:
        break
```

| Pros | Cons |
|------|------|
| Bounded latency | Clock-dependent behavior |
| Good for bursty workloads | Adds timing complexity |
| | May flush tiny batches under low load |

### 2. Size-Based Batching

Flush when total payload reaches a limit (e.g., 64KB).

```python
MAX_BATCH_BYTES = 64 * 1024
total_bytes = 0
while total_bytes < MAX_BATCH_BYTES:
    item = q.get_nowait()
    total_bytes += len(item)
    batch.append(item)
```

| Pros | Cons |
|------|------|
| Optimal disk I/O alignment | Unpredictable entry count per batch |
| Good for large values | Requires tracking cumulative size |
| | Large entries can trigger frequent flushes |

### 3. Entry-Count Batching (Chosen)

Flush when batch reaches N entries (e.g., 256) or queue is empty.

```python
while len(batch) < 256:
    try:
        batch.append(q.get_nowait())
    except queue.Empty:
        break
```

| Pros | Cons |
|------|------|
| Simple to implement | Not optimal for variable-size entries |
| Predictable memory usage | |
| No clock dependencies | |
| Clear upper bound on work per flush | |

## Decision

I chose entry-count batching because it is the easiest to reason about and provides a clear upper bound on work per flush. The behavior is predictable and does not depend on clocks or timing. The flush thread processes at most N records per batch, which avoids large variance in batch size and keeps both latency and memory usage bounded.

## Implementation

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

The batch limit of 256 entries provides a balance between:
- **Throughput** — Enough entries to amortize `fsync()` cost
- **Latency** — Not waiting too long to accumulate a full batch
- **Memory** — Bounded allocation per flush cycle