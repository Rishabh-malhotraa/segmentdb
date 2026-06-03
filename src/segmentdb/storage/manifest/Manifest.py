"""
The Manifest: authoritative, crash-consistent record of live SSTables.

See docs/manifest-spec.md for the full specification. The manifest is the
single source of truth for which SSTable files are part of the database, at
what level, and over what key/seq ranges. It is updated via an atomic
temp-write + fsync + rename so that every state change commits all-or-nothing.

API Overview
────────────
Lifecycle:
    load(data_dir)            → Manifest   Load from disk (or empty if fresh DB)
    save(data_dir)                         Atomic persist: tmp → fsync → rename

Serialization:
    to_dict()                 → dict       JSON-safe snapshot
    from_dict(data)           → Manifest   Reconstruct from JSON dict

Mutation (in-memory only — call save() to commit):
    allocate_id()             → int        Fresh, never-reused SSTable id
    add_sstable(meta)                      Register a new live SSTable
    remove_sstable(id)                     Drop an SSTable from the live set
    replace(remove_ids, add)               Atomic swap for compaction

Read-path:
    sstables                  → list       All live SSTables, oldest first
    sstables_at(level)        → list       SSTables at a level, sorted by min_key
    max_level                 → int        Highest populated level
    candidates_for_key(key)   → list       SSTables that may contain key (newest first)
"""

import json
import os
from bisect import bisect_right
from pathlib import Path
from typing import Any

from .SSTableMeta import SSTableMeta


class Manifest:
    """
    In-memory view of the live SSTable set, persisted atomically to ``MANIFEST``.

    Mutations (:meth:`add_sstable`, :meth:`remove_sstable`, :meth:`replace`,
    :meth:`allocate_id`) update in-memory state only; call :meth:`save` to
    commit. All mutations must be serialized by the caller (the DB holds a
    single manifest lock) — this class does no internal locking.
    """

    FORMAT_VERSION = 1
    FILENAME = "MANIFEST"
    TEMP_FILENAME = "MANIFEST.tmp"

    def __init__(
        self,
        next_sstable_id: int = 1,
        last_seq_no: int = 0,
        sstables: list[SSTableMeta] | None = None,
    ) -> None:
        self.next_sstable_id = next_sstable_id
        self.last_seq_no = last_seq_no
        self._sstables: dict[int, SSTableMeta] = {}
        for meta in sstables or []:
            self._sstables[meta.id] = meta

    # ------------------------------------------------------------------ #
    # Lifecycle
    # ------------------------------------------------------------------ #

    @classmethod
    def load(cls, data_dir: Path) -> "Manifest":
        """
        Load the manifest from ``data_dir``.

        Returns an empty manifest if no ``MANIFEST`` file exists (fresh DB).
        Raises if the file exists but is malformed (caller may then fall back
        to recovery by scanning SSTable files).
        """
        path = data_dir / cls.FILENAME
        if not path.exists():
            return cls()

        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)

        return cls.from_dict(data)

    def save(self, data_dir: Path) -> None:
        """
        Atomically persist the manifest (temp-write -> fsync -> rename).

        Per the spec, all SSTables referenced here must already be durably
        written before calling this, and input files removed by this change
        should be deleted only after this returns.
        """
        data_dir.mkdir(parents=True, exist_ok=True)
        temp_path = data_dir / self.TEMP_FILENAME
        final_path = data_dir / self.FILENAME

        with open(temp_path, "w", encoding="utf-8") as f:
            json.dump(self.to_dict(), f)
            f.flush()
            os.fsync(f.fileno())

        temp_path.rename(final_path)

        # Persist the rename itself by fsyncing the directory.
        dir_fd = os.open(data_dir, os.O_RDONLY)
        try:
            os.fsync(dir_fd)
        finally:
            os.close(dir_fd)

    # ------------------------------------------------------------------ #
    # Serialization
    # ------------------------------------------------------------------ #

    def to_dict(self) -> dict[str, Any]:
        """Serialize the manifest to a JSON-safe dict."""
        return {
            "format_version": self.FORMAT_VERSION,
            "next_sstable_id": self.next_sstable_id,
            "last_seq_no": self.last_seq_no,
            "sstables": [m.to_dict() for m in self.sstables],
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "Manifest":
        """Deserialize a manifest from a JSON dict produced by :meth:`to_dict`."""
        version = data.get("format_version")
        if version != cls.FORMAT_VERSION:
            raise ValueError(f"Unsupported manifest format_version: {version} " f"(expected {cls.FORMAT_VERSION})")

        sstables = [SSTableMeta.from_dict(d) for d in data["sstables"]]
        return cls(
            next_sstable_id=data["next_sstable_id"],
            last_seq_no=data["last_seq_no"],
            sstables=sstables,
        )

    # ------------------------------------------------------------------ #
    # Mutation
    # ------------------------------------------------------------------ #

    def allocate_id(self) -> int:
        """Return a fresh, never-reused SSTable id and advance the counter."""
        sstable_id = self.next_sstable_id
        self.next_sstable_id += 1
        return sstable_id

    def add_sstable(self, meta: SSTableMeta) -> None:
        """Register a new live SSTable and advance ``last_seq_no``."""
        if meta.id in self._sstables:
            raise ValueError(f"SSTable id {meta.id} already in manifest")
        self._sstables[meta.id] = meta
        self.last_seq_no = max(self.last_seq_no, meta.max_seq_no)

    def remove_sstable(self, sstable_id: int) -> None:
        """Remove an SSTable from the live set."""
        if sstable_id not in self._sstables:
            raise KeyError(f"SSTable id {sstable_id} not in manifest")
        del self._sstables[sstable_id]

    def replace(self, remove_ids: list[int], add: list[SSTableMeta]) -> None:
        """
        Atomically swap a set of inputs for a set of outputs (compaction).

        All ``remove_ids`` are removed and all ``add`` metas are inserted as a
        single in-memory state change; persist with :meth:`save`.
        """
        for sstable_id in remove_ids:
            self.remove_sstable(sstable_id)
        for meta in add:
            self.add_sstable(meta)

    # ------------------------------------------------------------------ #
    # Read-path support
    # ------------------------------------------------------------------ #

    @property
    def sstables(self) -> list[SSTableMeta]:
        """All live SSTables, ordered by id (ascending = oldest first)."""
        return [self._sstables[i] for i in sorted(self._sstables)]

    def sstables_at(self, level: int) -> list[SSTableMeta]:
        """Live SSTables at ``level``, sorted by ``min_key``."""
        metas = [m for m in self._sstables.values() if m.level == level]
        metas.sort(key=lambda m: m.min_key)
        return metas

    @property
    def max_level(self) -> int:
        """Highest level currently populated (0 if empty)."""
        if not self._sstables:
            return 0
        return max(m.level for m in self._sstables.values())

    def candidates_for_key(self, key: bytes) -> list[SSTableMeta]:
        """
        Return SSTables that may contain ``key``, in newest -> oldest priority.

        - Level 0: all files whose range covers the key, newest first (highest
          id), because L0 files have overlapping ranges.
        - Level 1+: at most one covering file per level (binary search over
          ``min_key``), since those levels are non-overlapping.
        """
        results: list[SSTableMeta] = []

        # Level 0: overlapping -> check every covering file, newest first.
        level_0 = [m for m in self._sstables.values() if m.level == 0]
        level_0.sort(key=lambda m: m.id, reverse=True)
        results.extend(m for m in level_0 if m.covers(key))

        # Level 1+: non-overlapping -> binary search a single covering file.
        for level in range(1, self.max_level + 1):
            metas = self.sstables_at(level)
            if not metas:
                continue
            min_keys = [m.min_key for m in metas]
            idx = bisect_right(min_keys, key) - 1
            if idx >= 0 and metas[idx].covers(key):
                results.append(metas[idx])

        return results
