"""Metadata describing a single live SSTable, as recorded in the manifest."""

from base64 import b64decode, b64encode
from dataclasses import dataclass
from typing import Any


@dataclass(slots=True, frozen=True)
class SSTableMeta:
    """
    Immutable description of one live SSTable.

    Keys are arbitrary bytes, so ``min_key``/``max_key`` are base64-encoded
    when serialized to the manifest's JSON form and decoded back on load.
    """

    id: int
    filename: str
    level: int
    min_key: bytes
    max_key: bytes
    min_seq_no: int
    max_seq_no: int
    entry_count: int
    file_size: int
    created_at: int

    def covers(self, key: bytes) -> bool:
        """True if ``key`` falls within this SSTable's key range (inclusive)."""
        return self.min_key <= key <= self.max_key

    def to_dict(self) -> dict[str, Any]:
        """Serialize to a JSON-safe dict (binary keys base64-encoded)."""
        return {
            "id": self.id,
            "filename": self.filename,
            "level": self.level,
            "min_key": b64encode(self.min_key).decode("ascii"),
            "max_key": b64encode(self.max_key).decode("ascii"),
            "min_seq_no": self.min_seq_no,
            "max_seq_no": self.max_seq_no,
            "entry_count": self.entry_count,
            "file_size": self.file_size,
            "created_at": self.created_at,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "SSTableMeta":
        """Deserialize from a JSON dict produced by :meth:`to_dict`."""
        return cls(
            id=data["id"],
            filename=data["filename"],
            level=data["level"],
            min_key=b64decode(data["min_key"]),
            max_key=b64decode(data["max_key"]),
            min_seq_no=data["min_seq_no"],
            max_seq_no=data["max_seq_no"],
            entry_count=data["entry_count"],
            file_size=data["file_size"],
            created_at=data["created_at"],
        )
