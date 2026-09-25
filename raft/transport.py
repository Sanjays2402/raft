"""Message transports for Raft RPCs.

A transport only needs one method, ``send(target_id, message)``, where
``message`` is a JSON-serializable dict. Inbound messages are handed to a
handler callable registered by the host.
"""

import json
import socket
import struct
import threading
from abc import ABC, abstractmethod
from collections import deque
from typing import Any, Callable, Dict, Optional, Set, Tuple


class Transport(ABC):
    @abstractmethod
    def send(self, target_id: str, message: Dict[str, Any]) -> None:
        """Deliver ``message`` to ``target_id`` (best effort)."""


Handler = Callable[[Dict[str, Any]], Optional[Dict[str, Any]]]


class LocalTransport(Transport):
    """In-process transport for tests and embedding.

    Sends are queued and delivered by :meth:`drain`, so there is no
    re-entrant dispatch: a full drain processes messages breadth-first until
    the queue is empty. Partitions can be simulated with :meth:`partition`
    and undone with :meth:`heal`.
    """

    def __init__(self) -> None:
        self._handlers: Dict[str, Handler] = {}
        self._queue: deque = deque()
        self._isolated: Set[str] = set()

    def register(self, node_id: str, handler: Handler) -> None:
        self._handlers[node_id] = handler

    def partition(self, node_id: str) -> None:
        """Drop every message to or from ``node_id``."""
        self._isolated.add(node_id)

    def heal(self, node_id: str) -> None:
        self._isolated.discard(node_id)

    def _dropped(self, sender: Optional[str], target: str) -> bool:
        return target in self._isolated or (
            sender is not None and sender in self._isolated
        )

    def send(self, target_id: str, message: Dict[str, Any]) -> None:
        if self._dropped(message.get("from"), target_id):
            return
        self._queue.append((target_id, message))

    def drain(self) -> None:
        while self._queue:
            target_id, message = self._queue.popleft()
            if self._dropped(message.get("from"), target_id):
                continue
            handler = self._handlers.get(target_id)
            if handler is None:
                continue
            response = handler(message)
            if response is not None:
                sender = message.get("from")
                if sender is not None and not self._dropped(target_id, sender):
                    self._queue.append((sender, response))

    @property
    def pending(self) -> int:
        return len(self._queue)


def _frame(payload: bytes) -> bytes:
    return struct.pack(">I", len(payload)) + payload


def _read_frame(conn: socket.socket) -> Optional[bytes]:
    header = b""
    while len(header) < 4:
        chunk = conn.recv(4 - len(header))
        if not chunk:
            return None
        header += chunk
    (length,) = struct.unpack(">I", header)
    payload = b""
    while len(payload) < length:
        chunk = conn.recv(length - len(payload))
        if not chunk:
            return None
        payload += chunk
    return payload


class _Conn:
    """One socket plus a lock serializing frame writes from any thread."""

    def __init__(self, sock: socket.socket) -> None:
        self.sock = sock
        self._send_lock = threading.Lock()

    def send_frame(self, payload: bytes) -> None:
        with self._send_lock:
            self.sock.sendall(_frame(payload))

    def close(self) -> None:
        try:
            self.sock.close()
        except OSError:
            pass


class TcpTransport(Transport):
    """TCP transport: length-prefixed JSON frames over persistent connections.

    Connections are bidirectional: every socket, whether accepted inbound or
    dialed outbound, gets a reader thread that dispatches inbound frames to
    the registered handler and writes the handler's response back on the same
    socket. A node dials each peer lazily on first send; the peer's listener
    accepts it, so under normal operation each pair ends up with two
    connections and delivery works even if only one side has dialed.
    Duplicate deliveries are harmless: Raft RPCs are idempotent.
    """

    def __init__(
        self,
        node_id: str,
        host: str,
        port: int,
        peers: Dict[str, Tuple[str, int]],
    ) -> None:
        self.node_id = node_id
        self.host = host
        self.port = port
        self.peers = dict(peers)
        self._handler: Optional[Handler] = None
        self._outbound: Dict[str, _Conn] = {}
        self._lock = threading.Lock()
        self._started = False

    def start(self, handler: Handler) -> None:
        self._handler = handler
        if self._started:
            return
        self._started = True
        thread = threading.Thread(target=self._serve, daemon=True, name="raft-tcp")
        thread.start()

    # -- inbound ------------------------------------------------------

    def _serve(self) -> None:
        server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server.bind((self.host, self.port))
        server.listen(16)
        while True:
            sock, _ = server.accept()
            self._spawn_reader(_Conn(sock))

    def _spawn_reader(self, conn: _Conn) -> None:
        thread = threading.Thread(
            target=self._read_loop, args=(conn,), daemon=True
        )
        thread.start()

    def _read_loop(self, conn: _Conn) -> None:
        try:
            while True:
                payload = _read_frame(conn.sock)
                if payload is None:
                    return
                message = json.loads(payload.decode("utf-8"))
                response = self._handler(message) if self._handler else None
                if response is not None:
                    conn.send_frame(json.dumps(response).encode("utf-8"))
        except OSError:
            return
        finally:
            conn.close()

    # -- outbound -----------------------------------------------------

    def _get_conn(self, target_id: str) -> _Conn:
        with self._lock:
            conn = self._outbound.get(target_id)
        if conn is not None:
            return conn
        host, port = self.peers[target_id]
        sock = socket.create_connection((host, port), timeout=2.0)
        conn = _Conn(sock)
        with self._lock:
            existing = self._outbound.get(target_id)
            if existing is not None:
                conn.close()  # another thread dialed first; drop the spare
                return existing
            self._outbound[target_id] = conn
        self._spawn_reader(conn)
        return conn

    def _drop_conn(self, target_id: str) -> None:
        with self._lock:
            conn = self._outbound.pop(target_id, None)
        if conn is not None:
            conn.close()

    def send(self, target_id: str, message: Dict[str, Any]) -> None:
        if target_id not in self.peers or target_id == self.node_id:
            return
        try:
            conn = self._get_conn(target_id)
            conn.send_frame(json.dumps(message).encode("utf-8"))
        except OSError:
            self._drop_conn(target_id)
