"""Deterministic test harness: virtual clock + in-process transport."""

import random
from typing import Any, Callable, Dict, List, Optional

from raft.node import FOLLOWER, LEADER, RaftNode
from raft.storage import MemoryStorage
from raft.transport import LocalTransport


class VirtualClock:
    def __init__(self) -> None:
        self.now = 0.0

    def __call__(self) -> float:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += seconds


class Cluster:
    """A fully deterministic Raft cluster driven by a virtual clock."""

    def __init__(self, node_ids: List[str], seed: int = 42, **node_kwargs: Any) -> None:
        self.clock = VirtualClock()
        self.transport = LocalTransport()
        self.nodes: Dict[str, RaftNode] = {}
        self.applied: Dict[str, List[Any]] = {}
        for i, node_id in enumerate(node_ids):
            peers = [n for n in node_ids if n != node_id]
            applied: List[Any] = []
            self.applied[node_id] = applied

            def apply_fn(index: int, command: Any, _a=applied) -> None:
                _a.append(command)

            node = RaftNode(
                node_id,
                peers,
                MemoryStorage(),
                self.transport,
                apply_fn=apply_fn,
                clock=self.clock,
                rng=random.Random(seed + i * 1000),
                **node_kwargs,
            )
            self.transport.register(node_id, node.handle_message)
            self.nodes[node_id] = node

    def step(self, dt: float = 0.005) -> None:
        self.clock.advance(dt)
        for node in self.nodes.values():
            node.tick()
        self.transport.drain()

    def run_until(
        self, condition: Callable[[], bool], timeout: float = 10.0, dt: float = 0.005
    ) -> bool:
        steps = int(timeout / dt)
        for _ in range(steps):
            self.step(dt)
            if condition():
                return True
        return False

    def leaders(self) -> List[RaftNode]:
        return [n for n in self.nodes.values() if n.state == LEADER]

    def leader(self) -> Optional[RaftNode]:
        leaders = self.leaders()
        return leaders[0] if leaders else None

    def terms(self) -> Dict[str, int]:
        return {nid: n.current_term for nid, n in self.nodes.items()}
