"""Tests for WALHeader serialization and deserialization."""

import pytest
from segmentdb.storage.wal import WALHeader


class TestWALHeaderValid:
    """Tests for valid WALHeader cases."""

    def test_default_roundtrip(self):
        """Test default header serialization and deserialization."""
        original = WALHeader()
        data = original.to_bytes()
        restored = WALHeader.from_bytes(data)

        assert restored.magic == b"WALX"
        assert restored.version == 1
        assert restored.timestamp == 0

    def test_custom_version_and_timestamp_roundtrip(self):
        """Test roundtrip with custom version and timestamp."""
        timestamp = 1234567890
        original = WALHeader(version=2, timestamp=timestamp)
        data = original.to_bytes()
        restored = WALHeader.from_bytes(data)

        assert restored.magic == b"WALX"
        assert restored.version == 2
        assert restored.timestamp == timestamp


class TestWALHeaderInvalid:
    """Tests for invalid WALHeader cases."""

    def test_invalid_magic_number(self):
        """Test that invalid magic number is rejected."""
        data = b"XXXX" + b"\x00" * 28

        with pytest.raises(ValueError, match="Invalid WAL file"):
            WALHeader.from_bytes(data)

    def test_invalid_version_zero(self):
        """Test that version 0 is rejected."""
        header = WALHeader(version=0)
        data = header.to_bytes()

        with pytest.raises(ValueError, match="Invalid WAL version"):
            WALHeader.from_bytes(data)

    def test_insufficient_data(self):
        """Test error handling for insufficient data."""
        with pytest.raises(Exception):
            WALHeader.from_bytes(b"WAL")
