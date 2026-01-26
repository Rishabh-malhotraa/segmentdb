# BloomFilter Design Document

*Last Updated: January 23, 2026*

Wrapper around `rbloom` (Rust-backed bloom filter) for probabilistic key existence checks in SSTables.

## Final Design

```python
from rbloom import Bloom
from xxhash import xxh3_64_intdigest


def _hash_func(key: bytes) -> int:
    """Deterministic hash function for bloom filter persistence."""
    return xxh3_64_intdigest(key)


class BloomFilter:
    __slots__ = ("_bloom",)

    def __init__(self, bloom: Bloom) -> None:
        self._bloom = bloom

    @classmethod
    def from_keys(cls, keys: Iterable[bytes], false_positive_rate: float = 0.01) -> "BloomFilter":
        keys_list = list(keys)
        num_keys = max(1, len(keys_list))
        bloom = Bloom(num_keys, false_positive_rate, hash_func=_hash_func)
        for key in keys_list:
            bloom.add(key)
        return cls(bloom)

    def __contains__(self, key: bytes) -> bool:
        return key in self._bloom

    def to_bytes(self) -> bytes:
        return self._bloom.save_bytes()

    @classmethod
    def from_bytes(cls, data: bytes) -> "BloomFilter":
        bloom = Bloom.load_bytes(data, hash_func=_hash_func)
        return cls(bloom)
```

**Key points:**
- Uses `rbloom` for fast Rust-backed bit operations
- Custom hash function (`xxh3`) for deterministic persistence
- Thin wrapper providing Pythonic API (`if key in bloom`)

---

## Design Iterations

### Iteration 1: Implementation Choice — Custom vs Library

**Problem:** Need a bloom filter for SSTable lookups to avoid unnecessary disk reads.

**Options considered:**

| Approach | Pros | Cons |
|----------|------|------|
| Custom (mmh3 + bytearray) | Full control, no deps | More code, potential bugs |
| `rbloom` | Rust-backed, fast, simple | External dependency |
| `pybloomfiltermmap3` | Memory-mapped | Complex API |

**Final:** `rbloom` — fast Rust bit operations, simple API

**Rationale:**
- Bloom filter is well-understood, no need to reinvent
- Rust backing gives us speed without C extension hassle
- Simple `key in bloom` API

---

### Iteration 2: Persistence Problem — Hash Function Determinism

**Problem:** `rbloom` uses Python's built-in `hash()` by default, which is **non-deterministic** across Python sessions.

```python
# Session 1
hash(b"key")  # → 12345

# Session 2 (different PYTHONHASHSEED)
hash(b"key")  # → 67890  ← DIFFERENT!
```

This breaks persistence: saved bloom filter becomes useless after restart.

**Original:**
```python
bloom = Bloom(num_keys, false_positive_rate)  # Uses built-in hash
bloom.save_bytes()  # Saves bit array
Bloom.load_bytes(data)  # ❌ Wrong hash positions!
```

**Final:**
```python
bloom = Bloom(num_keys, false_positive_rate, hash_func=_hash_func)
bloom.save_bytes()
Bloom.load_bytes(data, hash_func=_hash_func)  # ✅ Same hash function
```

**Rationale:**
- Custom hash function must be passed to both create and load
- Function must be deterministic across Python sessions

---

### Iteration 3: Hash Function Choice — Speed vs Simplicity

**Problem:** Need a fast, deterministic hash function.

**Iterations:**

| Version | Hash | Issue |
|---------|------|-------|
| v1 | `hashlib.sha256` | Cryptographic = overkill, slow |
| v2 | `xxhash.xxh64().intdigest()` | Fast, but creates intermediate object |
| v3 | `xxhash.xxh3_64_intdigest()` | Fastest, single call |

**Final:**
```python
from xxhash import xxh3_64_intdigest

def _hash_func(key: bytes) -> int:
    return xxh3_64_intdigest(key)
```

**Rationale:**
- `xxh3` uses SIMD on modern CPUs — fastest non-crypto hash
- `_intdigest()` variant avoids object creation
- Still deterministic across sessions/machines

---

## How rbloom Works Internally

Understanding this helped us make the right choices:

```
Key: b"hello"
        │
        ▼
   hash_func(b"hello")  ← ONE Python call (xxh3)
        │
        ▼
   hash = 0x7B3F2A1C...
        │
        ▼
   LCG generates k indexes:  [42, 1337, 8192, ...]  ← Pure Rust
        │
        ▼
   Set/check bits in BitLine  ← Pure Rust
```

**Key insight:** Even with a Python hash function, only ONE Python→Rust call happens per key. The LCG and bit operations are fast Rust.

---

## False Positive Rate Tradeoffs

| FPR | Bits/key | Memory for 100K keys |
|-----|----------|---------------------|
| 10% | ~4.8 | ~60 KB |
| 1% | ~9.6 | ~120 KB |
| 0.1% | ~14.4 | ~180 KB |

We use 1% (default) — good balance of memory vs accuracy.

---

## Usage

```python
from segmentdb.storage.sstable.BloomFilter import BloomFilter

# Create from memtable keys
bloom = BloomFilter.from_keys(sorted_dict.keys())

# Check membership (before disk read)
if b"mykey" in bloom:
    # Might exist, read from SSTable
    pass
else:
    # Definitely doesn't exist, skip disk read
    pass

# Serialize for SSTable footer
data = bloom.to_bytes()

# Deserialize when opening SSTable
bloom = BloomFilter.from_bytes(data)
```
