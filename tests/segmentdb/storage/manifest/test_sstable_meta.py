"""Tests for SSTableMeta serialization."""

from segmentdb.storage.manifest import SSTableMeta


def make_meta(**overrides) -> SSTableMeta:
    defaults = dict(
        id=1,
        filename="sst-000001.sst",
        level=0,
        min_key=b"aaa",
        max_key=b"zzz",
        min_seq_no=1,
        max_seq_no=1000,
        entry_count=1000,
        file_size=65536,
        created_at=1706284800,
    )
    defaults.update(overrides)
    return SSTableMeta(**defaults)


class TestSSTableMetaRoundtrip:
    def test_dict_roundtrip(self):
        original = make_meta()
        restored = SSTableMeta.from_dict(original.to_dict())
        assert restored == original

    def test_binary_keys_survive_base64(self):
        original = make_meta(min_key=b"\x00\xff\x01", max_key=b"\xfe\xfd")
        restored = SSTableMeta.from_dict(original.to_dict())
        assert restored.min_key == b"\x00\xff\x01"
        assert restored.max_key == b"\xfe\xfd"

    def test_to_dict_is_json_safe(self):
        import json

        meta = make_meta(min_key=b"\x00\x01", max_key=b"\xff")
        # Should not raise; binary keys are base64 strings.
        json.dumps(meta.to_dict())


class TestSSTableMetaCovers:
    def test_covers_within_range(self):
        meta = make_meta(min_key=b"d", max_key=b"h")
        assert meta.covers(b"e")
        assert meta.covers(b"d")  # inclusive lower
        assert meta.covers(b"h")  # inclusive upper

    def test_covers_outside_range(self):
        meta = make_meta(min_key=b"d", max_key=b"h")
        assert not meta.covers(b"a")
        assert not meta.covers(b"z")
