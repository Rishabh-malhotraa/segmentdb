"""Tests for WALEntry serialization and deserialization."""

import pytest
from segmentdb.storage.wal import WALEntry, OperationType


class TestWALEntryValid:
    """Tests for valid WALEntry cases."""

    def test_put_roundtrip(self):
        """Test PUT operation serialization and deserialization."""
        original = WALEntry(
            seq_no=42,
            op_type=OperationType.PUT,
            key=b"user:1",
            value=b"john_doe",
        )

        data = original.to_bytes()
        restored = WALEntry.from_bytes(data)

        assert restored.seq_no == original.seq_no
        assert restored.op_type == original.op_type
        assert restored.key == original.key
        assert restored.value == original.value

    def test_delete_roundtrip(self):
        """Test DELETE operation serialization and deserialization."""
        original = WALEntry(
            seq_no=99,
            op_type=OperationType.DELETE,
            key=b"expired_token",
            value=None,
        )

        data = original.to_bytes()
        restored = WALEntry.from_bytes(data)

        assert restored.seq_no == original.seq_no
        assert restored.op_type == OperationType.DELETE
        assert restored.key == original.key
        assert restored.value is None


class TestWALEntryInvalid:
    """Tests for invalid WALEntry cases."""

    def test_crc32_mismatch_detection(self):
        """Test that corrupted data is detected via CRC32."""
        entry = WALEntry(
            seq_no=1,
            op_type=OperationType.PUT,
            key=b"test",
            value=b"data",
        )

        data = entry.to_bytes()
        corrupted_data = data[:-1] + bytes([data[-1] ^ 0xFF])

        with pytest.raises(ValueError, match="CRC32 mismatch"):
            WALEntry.from_bytes(corrupted_data)
