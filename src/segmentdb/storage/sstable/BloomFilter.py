from rbloom import Bloom
from typing import Iterable
from xxhash import xxh3_64_intdigest


class BloomFilter:
    """
    Probabilistic data structure for membership testing.

    False positives are possible, false negatives are not.
    Used to avoid unnecessary disk reads when a key definitely doesn't exist.

    Example:
        bloom = BloomFilter.from_keys(sorted_dict.keys())
        if key in bloom:
            # Might exist, check SSTable
        else:
            # Definitely doesn't exist, skip SSTable
    """

    @staticmethod
    def _hash_func(key: bytes) -> int:
        """Deterministic hash function for bloom filter persistence."""
        return xxh3_64_intdigest(key)

    def __init__(self, bloom: Bloom) -> None:
        self._bloom = bloom

    @property
    def size_bytes(self) -> int:
        """Size of serialized bloom filter."""
        return len(self._bloom.save_bytes())

    @classmethod
    def from_keys(
        cls, keys: Iterable[bytes], false_positive_rate: float = 0.01
    ) -> "BloomFilter":
        """
        Create a bloom filter from an iterable of keys.

        Args:
            keys: Iterable of byte keys (e.g., sorted_dict.keys())
            false_positive_rate: Desired false positive rate (default 1%)
        """
        keys_list = list(keys)
        num_keys = max(1, len(keys_list))  # Avoid zero-size filter

        bloom = Bloom(num_keys, false_positive_rate, hash_func=cls._hash_func)
        for key in keys_list:
            bloom.add(key)

        return cls(bloom)

    @classmethod
    def from_bytes(cls, data: bytes) -> "BloomFilter":
        bloom = Bloom.load_bytes(data, hash_func=cls._hash_func)
        return cls(bloom)

    def __contains__(self, key: bytes) -> bool:
        return key in self._bloom

    def to_bytes(self) -> bytes:
        return self._bloom.save_bytes()
