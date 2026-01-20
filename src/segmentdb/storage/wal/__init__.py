"""Write-Ahead Log (WAL) module for SegmentDB."""

from .WALEntry import WALEntry
from .WALHeader import WALHeader

__all__ = ["WALHeader", "WALEntry"]
