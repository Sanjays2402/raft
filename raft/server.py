"""A tiny distributed key-value store on top of Raft.

Run three nodes in separate terminals (or one terminal each)::

    python -m raft.server --id n1 --port 5001 \\
        --peers n2=127.0.0.1:5002,n3=127.0.0.1:5003
    python -m raft.server --id n2 --port 5002 \\
        --peers n1=127.0.0.1:5001,n3=127.0.0.1:5003
    python -m raft.server --id n3 --port 5003 \\
        --peers n1=127.0.0.1:5001,n2=127.0.0.1:5002

Commands are submitted through the node's ``submit`` method, for example
from a Python REPL connected to the same process. Persistent state lives in
``<data-dir>/<node-id>.json``.
"""

import argparse
import os
import threading
import time
from typing import Any, Dict, Tuple

from .node import LEADER, RaftNode
from .storage import FileStorage
from .transport import TcpTransport


def parse_peers(spec: str) -> Dict[str, Tuple[str, int]]:
    peers: Dict[str, Tuple[str, int]] = {}
    for item in spec.split(","):
        item = item.strip()
        if not item:
            continue
        node_id, address = item.split("=", 1)
        host, port = address.rsplit(":", 1)
        peers[node_id.strip()] = (host.strip(), int(port))
    return peers


def main() -> None:
    parser = argparse.ArgumentParser(description="Run a Raft node.")
    parser.add_argument("--id", required=True, help="This node's id, e.g. n1")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, required=True)
    parser.add_argument(
        "--peers",
        default="",
        help="Comma-separated id=host:port entries for the other nodes",
    )
    parser.add_argument("--data-dir", default="./data")
    parser.add_argument("--election-timeout", type=float, nargs=2, default=(0.150, 0.300))
    parser.add_argument("--heartbeat", type=float, default=0.050)
    parser.add_argument(
        "--no-pre-vote",
        action="store_true",
        help="Disable the PreVote phase and elect with plain RequestVote",
    )
    args = parser.parse_args()

    peers = parse_peers(args.peers)
    state: Dict[str, Any] = {}

    def apply(index: int, command: Any) -> None:
        if isinstance(command, dict) and command.get("op") == "set":
            state[command["key"]] = command["value"]
            print(f"[{args.id}] applied #{index}: {command['key']}={command['value']!r}")

    storage = FileStorage(os.path.join(args.data_dir, f"{args.id}.json"))
    transport = TcpTransport(args.id, args.host, args.port, peers)
    node = RaftNode(
        args.id,
        list(peers),
        storage,
        transport,
        apply_fn=apply,
        election_timeout=tuple(args.election_timeout),
        heartbeat_interval=args.heartbeat,
        pre_vote=not args.no_pre_vote,
    )
    transport.start(node.handle_message)

    stop = threading.Event()

    def tick_loop() -> None:
        while not stop.is_set():
            node.tick()
            time.sleep(0.005)

    thread = threading.Thread(target=tick_loop, daemon=True, name="raft-tick")
    thread.start()
    print(f"[{args.id}] listening on {args.host}:{args.port}, peers={sorted(peers)}")
    try:
        while True:
            time.sleep(1.0)
            role = node.state.upper()
            extra = " <- LEADER" if role == LEADER else ""
            print(
                f"[{args.id}] term={node.current_term} state={role} "
                f"commit={node.commit_index} log={node.log.last_index()}{extra}"
            )
    except KeyboardInterrupt:
        stop.set()
        print(f"[{args.id}] shutting down")


if __name__ == "__main__":
    main()
