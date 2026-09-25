"""Persistent state for a Raft node.

Raft requires three pieces of state to survive a crash: ``current_term``,
``voted_for`` and the log. Everything else can be rebuilt. The node calls
``save()`` after every mutation of persistent state, so an implementation
only needs to make a single ``save()`` call durable.
"""

import json
import os
from abc import ABC, abstractmethod
from typing import Any, List, Optional, Tuple

from .log import LogEntry


class Storage(ABC):
    @abstractmethod
    def load(self) -> Tuple[int, Optional[str], List[LogEntry]]:
        """Return ``(current_term, voted_for, log_entries)``."""

    @abstractmethod
    def save(
        self, current_term: int, voted_for: Optional[str], entries: List[LogEntry]
    ) -> None:
        """Durably record the node's persistent state."""


class MemoryStorage(Storage):
    """Volatile storage. Handy for tests and embedding."""

    def __init__(self) -> None:
        self._term = 0
        self._voted_for: Optional[str] = None
        self._entries: List[LogEntry] = []

    def load(self) -> Tuple[int, Optional[str], List[LogEntry]]:
        return self._term, self._voted_for, list(self._entries)

    def save(
        self, current_term: int, voted_for: Optional[str], entries: List[LogEntry]
    ) -> None:
        self._term = current_term
        self._voted_for = voted_for
        self._entries = list(entries)


class FileStorage(Storage):
    """JSON file storage with atomic writes (temp file + rename)."""

    def __init__(self, path: str) -> None:
        self.path = path

    def load(self) -> Tuple[int, Optional[str], List[LogEntry]]:
        if not os.path.exists(self.path):
            return 0, None, []
        with open(self.path, "r", encoding="utf-8") as f:
            data = json.load(f)
        entries = [LogEntry.from_dict(d) for d in data.get("log", [])]
        return data.get("current_term", 0), data.get("voted_for"), entries

    def save(
        self, current_term: int, voted_for: Optional[str], entries: List[LogEntry]
    ) -> None:
        data = {
            "current_term": current_term,
            "voted_for": voted_for,
            "log": [e.to_dict() for e in entries],
        }
        tmp = self.path + ".tmp"
        directory = os.path.dirname(os.path.abspath(self.path))
        os.makedirs(directory, exist_ok=True)
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(data, f)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, self.path)
