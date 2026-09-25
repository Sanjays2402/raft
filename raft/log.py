"""The replicated log.

Raft log entries are 1-indexed. Index 0 is a sentinel meaning "no entry" and
always carries term 0. Commands stored in the log must be JSON-serializable
so entries can cross the TCP transport and the file-backed storage.
"""

from dataclasses import dataclass
from typing import Any, Dict, List, Optional


@dataclass
class LogEntry:
    term: int
    command: Any

    def to_dict(self) -> Dict[str, Any]:
        return {"term": self.term, "command": self.command}

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "LogEntry":
        return cls(term=data["term"], command=data["command"])


class Log:
    """In-memory ordered list of LogEntry values, 1-indexed."""

    def __init__(self, entries: Optional[List[LogEntry]] = None) -> None:
        self._entries: List[LogEntry] = list(entries) if entries else []

    def __len__(self) -> int:
        return len(self._entries)

    def append(self, entry: LogEntry) -> None:
        self._entries.append(entry)

    def get(self, index: int) -> Optional[LogEntry]:
        """Return the entry at 1-based ``index``; ``None`` for index 0 or gaps."""
        if index <= 0 or index > len(self._entries):
            return None
        return self._entries[index - 1]

    def term_at(self, index: int) -> int:
        entry = self.get(index)
        return entry.term if entry is not None else 0

    def truncate_from(self, index: int) -> None:
        """Delete every entry at 1-based ``index`` and beyond."""
        del self._entries[index - 1 :]

    def entries_from(self, index: int) -> List[LogEntry]:
        """Return entries from 1-based ``index`` to the end of the log."""
        return list(self._entries[index - 1 :])

    def last_index(self) -> int:
        return len(self._entries)

    def last_term(self) -> int:
        return self._entries[-1].term if self._entries else 0

    def to_dicts(self) -> List[Dict[str, Any]]:
        return [e.to_dict() for e in self._entries]

    @classmethod
    def from_dicts(cls, dicts: List[Dict[str, Any]]) -> "Log":
        return cls([LogEntry.from_dict(d) for d in dicts])
