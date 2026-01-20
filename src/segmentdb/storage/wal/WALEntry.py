from dataclasses import dataclass
from enum import Enum
from typing import Optional
import struct
import zlib


class OperationType(Enum):
    PUT = 1
    DELETE = 2


@dataclass
class WALEntry:
    """
    WAL entry representing a single operation (PUT or DELETE).

    Binary format:
    ┌────────────┬──────┬──────────┬──────────┬─────────┬──────────┬─────────┬──────────┐
    │ Length     │ Seq# │ Op Type  │ Key Len  │ Val Len │ Key      │ Value   │ CRC32    │
    │ 4 bytes    │ 8B   │ 1 byte   │ 2 bytes  │ 4 bytes │ variable │ var     │ 4 bytes  │
    │ u32        │ u64  │ u8       │ u16      │ u32     │ bytes    │ bytes   │ u32      │
    └────────────┴──────┴──────────┴──────────┴─────────┴──────────┴─────────┴──────────┘

    Byte order: All integers use big-endian encoding (most significant byte first).
    CRC32: Cyclic Redundancy Check (32-bit) computed over the entry data (excluding itself).
    """

    seq_no: int
    op_type: OperationType
    key: bytes
    value: Optional[bytes] = None  # None for DELETE

    def to_bytes(self):
        key_len = len(self.key)
        val_len = len(self.value) if self.value else 0

        # Pack payload without length and crc32
        payload = struct.pack(
            f">QBHI{key_len}s{val_len}s",
            self.seq_no,
            self.op_type.value,
            key_len,
            val_len,
            self.key,
            self.value or b"",
        )

        # Calculate CRC32 (Cyclic Redundancy Check 32-bit) for integrity
        # zlib.crc32() returns a signed integer, so we mask with 0xffffffff
        # to convert it to an unsigned 32-bit value for proper serialization
        crc32_value = zlib.crc32(payload) & 0xFFFFFFFF

        # Total length: payload + crc32(4 bytes)
        entry_length = len(payload) + 4

        # Return: length(4) + payload + crc32(4)
        return (
            struct.pack(">I", entry_length) + payload + struct.pack(">I", crc32_value)
        )

    @classmethod
    def from_bytes(cls, data: bytes) -> "WALEntry":
        """
        Deserialize a WALEntry from binary format.

        Args:
            data: Binary data containing length + entry + crc32

        Returns:
            WALEntry object

        Raises:
            ValueError: If data is malformed or CRC32 verification fails
        """
        FIXED_HEADER_SIZE = 15  # seq_no(8) + op_type(1) + key_len(2) + val_len(4)
        LENGTH_FIELD_SIZE = 4
        CRC32_FIELD_SIZE = 4

        # Parse length field and validate we have enough data
        entry_length = struct.unpack(">I", data[:LENGTH_FIELD_SIZE])[0]
        total_required = LENGTH_FIELD_SIZE + entry_length

        if len(data) < total_required:
            raise ValueError(
                f"Insufficient data: expected {total_required} bytes, got {len(data)}"
            )

        # Extract entry chunk (payload + crc32)
        entry_chunk = data[LENGTH_FIELD_SIZE:total_required]
        payload = entry_chunk[:-CRC32_FIELD_SIZE]
        stored_crc32 = struct.unpack(">I", entry_chunk[-CRC32_FIELD_SIZE:])[0]

        # Verify integrity with CRC32
        calculated_crc32 = zlib.crc32(payload) & 0xFFFFFFFF
        if calculated_crc32 != stored_crc32:
            raise ValueError(
                f"CRC32 mismatch: expected {stored_crc32}, got {calculated_crc32}"
            )

        # Unpack fixed header fields
        seq_no, op_type_val, key_len, val_len = struct.unpack(
            ">QBHI", payload[:FIXED_HEADER_SIZE]
        )

        # Extract variable-length key and value
        key_start = FIXED_HEADER_SIZE
        key_end = key_start + key_len
        key = payload[key_start:key_end]

        value_start = key_end
        value_end = value_start + val_len
        value = payload[value_start:value_end] if val_len > 0 else None

        op_type = OperationType(op_type_val)

        return cls(seq_no=seq_no, op_type=op_type, key=key, value=value)
