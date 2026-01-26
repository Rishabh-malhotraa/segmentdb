from dataclasses import dataclass
from enum import Enum
from typing import Optional
import struct
import xxhash


class OperationType(Enum):
    PUT = 1
    DELETE = 2


@dataclass(slots=True)
class WALEntry:
    """
    WAL entry representing a single operation (PUT or DELETE).

    Binary format (on disk):
    ┌────────────┬─────────────────────────────────────────────────────────────┐
    │ Length     │ Payload + Checksum                                          │
    │ 4 bytes    │ (passed to from_bytes)                                      │
    └────────────┴─────────────────────────────────────────────────────────────┘

    Payload + checksum format:
    ┌──────┬──────────┬──────────┬─────────┬──────────┬─────────┬──────────┐
    │ Seq# │ Op Type  │ Key Len  │ Val Len │ Key      │ Value   │ Checksum │
    │ 8B   │ 1 byte   │ 2 bytes  │ 4 bytes │ variable │ var     │ 4 bytes  │
    │ u64  │ u8       │ u16      │ u32     │ bytes    │ bytes   │ u32      │
    └──────┴──────────┴──────────┴─────────┴──────────┴─────────┴──────────┘
    """

    FIXED_HEADER_SIZE = 15  # seq_no(8) + op_type(1) + key_len(2) + val_len(4)
    CHECKSUM_SIZE = 4
    LENGTH_SIZE = 4

    seq_no: int
    op_type: OperationType
    key: bytes
    value: Optional[bytes] = None

    def to_bytes(self) -> bytes:
        """Serialize to: length(4) + payload + checksum(4)."""
        key_len = len(self.key)
        val_len = len(self.value) if self.value else 0

        payload = struct.pack(
            f">QBHI{key_len}s{val_len}s",
            self.seq_no,
            self.op_type.value,
            key_len,
            val_len,
            self.key,
            self.value or b"",
        )

        checksum = xxhash.xxh32(payload).digest()
        entry_length = len(payload) + self.CHECKSUM_SIZE

        return struct.pack(">I", entry_length) + payload + checksum

    @classmethod
    def from_bytes(cls, data: bytes) -> "WALEntry":
        """
        Deserialize from payload + checksum (WITHOUT length prefix).

        Args:
            data: payload + checksum bytes (length already read by caller)

        Returns:
            WALEntry object

        Raises:
            ValueError: If corrupted or checksum mismatch
        """
        if len(data) < cls.FIXED_HEADER_SIZE + cls.CHECKSUM_SIZE:
            raise ValueError(f"Data too short: {len(data)} bytes")

        payload = data[: -cls.CHECKSUM_SIZE]
        stored_checksum = data[-cls.CHECKSUM_SIZE :]

        # Verify integrity
        computed_checksum = xxhash.xxh32(payload).digest()
        if computed_checksum != stored_checksum:
            raise ValueError(
                f"Checksum mismatch: stored={stored_checksum.hex()}, computed={computed_checksum.hex()}"
            )

        # Unpack header
        seq_no, op_type_val, key_len, val_len = struct.unpack(
            ">QBHI", payload[: cls.FIXED_HEADER_SIZE]
        )

        # Extract key and value
        key_start = cls.FIXED_HEADER_SIZE
        key = payload[key_start : key_start + key_len]

        value_start = key_start + key_len
        value = payload[value_start : value_start + val_len] if val_len > 0 else None

        return cls(
            seq_no=seq_no,
            op_type=OperationType(op_type_val),
            key=key,
            value=value,
        )
