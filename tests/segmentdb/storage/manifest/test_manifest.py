"""Tests for the Manifest: persistence, mutation, and lookup."""

import json

import pytest

from segmentdb.storage.manifest import Manifest, SSTableMeta


def make_meta(id: int, level: int, min_key: bytes, max_key: bytes, **overrides):
    defaults = dict(
        id=id,
        filename=f"sst-{id:06d}.sst",
        level=level,
        min_key=min_key,
        max_key=max_key,
        min_seq_no=id * 100,
        max_seq_no=id * 100 + 99,
        entry_count=100,
        file_size=4096,
        created_at=1706284800,
    )
    defaults.update(overrides)
    return SSTableMeta(**defaults)


class TestPersistence:
    def test_load_missing_returns_empty(self, tmp_path):
        manifest = Manifest.load(tmp_path)
        assert manifest.sstables == []
        assert manifest.next_sstable_id == 1
        assert manifest.last_seq_no == 0

    def test_save_load_roundtrip(self, tmp_path):
        manifest = Manifest()
        meta = make_meta(manifest.allocate_id(), 0, b"aaa", b"zzz")
        manifest.add_sstable(meta)
        manifest.save(tmp_path)

        restored = Manifest.load(tmp_path)
        assert restored.sstables == [meta]
        assert restored.next_sstable_id == manifest.next_sstable_id
        assert restored.last_seq_no == meta.max_seq_no

    def test_save_is_atomic_no_temp_left(self, tmp_path):
        manifest = Manifest()
        manifest.add_sstable(make_meta(manifest.allocate_id(), 0, b"a", b"b"))
        manifest.save(tmp_path)

        assert (tmp_path / Manifest.FILENAME).exists()
        assert not (tmp_path / Manifest.TEMP_FILENAME).exists()

    def test_rejects_unknown_format_version(self, tmp_path):
        path = tmp_path / Manifest.FILENAME
        path.write_text(json.dumps({"format_version": 999, "sstables": []}))
        with pytest.raises(ValueError, match="format_version"):
            Manifest.load(tmp_path)


class TestMutation:
    def test_allocate_id_is_monotonic(self):
        manifest = Manifest()
        assert manifest.allocate_id() == 1
        assert manifest.allocate_id() == 2
        assert manifest.next_sstable_id == 3

    def test_add_updates_last_seq_no(self):
        manifest = Manifest()
        manifest.add_sstable(make_meta(1, 0, b"a", b"b", max_seq_no=500))
        assert manifest.last_seq_no == 500
        # Lower max_seq_no must not lower last_seq_no.
        manifest.add_sstable(make_meta(2, 0, b"c", b"d", max_seq_no=200))
        assert manifest.last_seq_no == 500

    def test_add_duplicate_id_raises(self):
        manifest = Manifest()
        manifest.add_sstable(make_meta(1, 0, b"a", b"b"))
        with pytest.raises(ValueError, match="already in manifest"):
            manifest.add_sstable(make_meta(1, 0, b"c", b"d"))

    def test_remove_unknown_id_raises(self):
        manifest = Manifest()
        with pytest.raises(KeyError):
            manifest.remove_sstable(42)

    def test_replace_swaps_inputs_for_outputs(self):
        manifest = Manifest()
        a = make_meta(1, 0, b"a", b"f")
        b = make_meta(2, 0, b"c", b"k")
        manifest.add_sstable(a)
        manifest.add_sstable(b)

        out = make_meta(3, 1, b"a", b"k")
        manifest.replace(remove_ids=[1, 2], add=[out])

        assert manifest.sstables == [out]


class TestCandidatesForKey:
    def test_level0_overlapping_returns_all_newest_first(self):
        manifest = Manifest()
        # Two overlapping L0 files both covering b"e".
        old = make_meta(1, 0, b"a", b"h")
        new = make_meta(2, 0, b"c", b"k")
        manifest.add_sstable(old)
        manifest.add_sstable(new)

        candidates = manifest.candidates_for_key(b"e")
        assert candidates == [new, old]  # newest (higher id) first

    def test_level0_excludes_non_covering(self):
        manifest = Manifest()
        manifest.add_sstable(make_meta(1, 0, b"a", b"c"))
        covering = make_meta(2, 0, b"d", b"h")
        manifest.add_sstable(covering)

        assert manifest.candidates_for_key(b"e") == [covering]

    def test_level1_binary_search_single_file(self):
        manifest = Manifest()
        # Non-overlapping L1 files.
        f1 = make_meta(1, 1, b"a", b"f")
        f2 = make_meta(2, 1, b"g", b"m")
        f3 = make_meta(3, 1, b"n", b"z")
        for f in (f1, f2, f3):
            manifest.add_sstable(f)

        assert manifest.candidates_for_key(b"h") == [f2]
        assert manifest.candidates_for_key(b"a") == [f1]
        assert manifest.candidates_for_key(b"z") == [f3]

    def test_level1_gap_returns_nothing(self):
        manifest = Manifest()
        manifest.add_sstable(make_meta(1, 1, b"a", b"f"))
        manifest.add_sstable(make_meta(2, 1, b"n", b"z"))
        # Key in the gap between files.
        assert manifest.candidates_for_key(b"h") == []

    def test_mixed_levels_l0_before_l1(self):
        manifest = Manifest()
        l0 = make_meta(5, 0, b"a", b"z")
        l1 = make_meta(1, 1, b"d", b"h")
        manifest.add_sstable(l0)
        manifest.add_sstable(l1)

        # L0 must come before L1 (newer data wins).
        assert manifest.candidates_for_key(b"e") == [l0, l1]
