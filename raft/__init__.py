"""raft: a small, dependency-free Raft consensus implementation in Python.

Implements the Raft core from Ongaro and Ousterhout's "In Search of an
Understandable Consensus Algorithm": leader election, log replication and
safety. Standard library only.
"""

from .log import Log, LogEntry
from .node import (
    CANDIDATE,
    FOLLOWER,
    LEADER,
    PRE_CANDIDATE,
    NotLeaderError,
    RaftNode,
    TransferInProgressError,
)
from .storage import FileStorage, MemoryStorage, Storage
from .transport import LocalTransport, TcpTransport, Transport

__all__ = [
    "RaftNode",
    "NotLeaderError",
    "TransferInProgressError",
    "FOLLOWER",
    "PRE_CANDIDATE",
    "CANDIDATE",
    "LEADER",
    "Log",
    "LogEntry",
    "Storage",
    "MemoryStorage",
    "FileStorage",
    "Transport",
    "LocalTransport",
    "TcpTransport",
]

__version__ = "0.1.0"
