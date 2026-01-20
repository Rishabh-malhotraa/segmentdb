from dataclasses import dataclass
from typing import ClassVar
import struct


@dataclass(slots=True)
class WALHeader:
    """
    WAL file header (32 bytes total).

    Binary format:
    ┌─────────────┬──────┬───────────┬──────────┐
    │ Magic       │ Ver  │ Timestamp │ Reserved │
    │ 4 bytes     │ 4B   │ 8 bytes   │ 16 bytes │
    │ 'WALX'      │ u32  │ u64       │ zeros    │
    └─────────────┴──────┴───────────┴──────────┘
    Byte order: All integers use big-endian encoding (most significant byte first).
    """

    HEADER_SIZE: ClassVar[int] = 32

    magic: bytes = b"WALX"
    version: int = 1
    timestamp: int = 0

    def to_bytes(self) -> bytes:
        return struct.pack(
            ">4sIQ16s", self.magic, self.version, self.timestamp, b"\x00" * 16
        )

    @classmethod
    def from_bytes(cls, data: bytes) -> "WALHeader":
        magic, version, timestamp, _ = struct.unpack(">4sIQ16s", data)

        if magic != b"WALX":
            raise ValueError(
                f"Invalid WAL file: expected 'WALX', got '{magic.decode('ascii', errors='replace')}'"
            )
        if version == 0:
            raise ValueError(f"Invalid WAL version: {version}")

        return cls(magic, version, timestamp)

    @classmethod
    def validate(cls, data: bytes) -> None:
        """Validate header bytes without returning an object."""
        cls.from_bytes(data)
