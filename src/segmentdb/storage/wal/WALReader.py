from typing import BinaryIO, Iterator, Optional
import struct

from segmentdb.storage.wal import WALHeader, WALEntry


class WALReader:
    """
    Reads entries from a Write-Ahead Log file.

    Usage:
        with open("data.wal", "rb") as f:
            for entry in WALReader(f):
                print(entry.key, entry.op_type)
    """

    LENGTH_FIELD_SIZE: int = 4

    def __init__(self, fd: BinaryIO) -> None:
        self._fd = fd
        self._validate_header()

    def _validate_header(self) -> None:
        """Read and validate the WAL header."""
        data = self._fd.read(WALHeader.HEADER_SIZE)

        if len(data) < WALHeader.HEADER_SIZE:
            raise ValueError(
                f"Truncated WAL file: expected {WALHeader.HEADER_SIZE} bytes, got {len(data)}"
            )

        try:
            WALHeader.validate(data)
        except ValueError as e:
            raise ValueError(f"Invalid WAL header: {e}") from e

    def _read_entry(self) -> Optional[WALEntry]:
        """Read the next WAL entry."""
        length_data = self._fd.read(4)

        if len(length_data) == 0:
            return None
        if len(length_data) < 4:
            raise ValueError("Truncated entry: incomplete length field")

        entry_length = struct.unpack(">I", length_data)[0]
        payload_with_checksum = self._fd.read(entry_length)

        if len(payload_with_checksum) < entry_length:
            raise ValueError(
                f"Truncated entry: expected {entry_length} bytes, got {len(payload_with_checksum)}"
            )

        return WALEntry.from_bytes(payload_with_checksum)

    def __iter__(self) -> Iterator[WALEntry]:
        """Return self as iterator."""
        return self

    def __next__(self) -> WALEntry:
        """Return next entry or raise StopIteration."""
        entry = self._read_entry()
        if entry is None:
            raise StopIteration
        return entry
