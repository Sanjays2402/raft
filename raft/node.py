"""Raft consensus node: leader election and log replication.

The node is deliberately decoupled from I/O. It never blocks and never starts
threads: the host drives it by calling :meth:`tick` on a schedule and routing
inbound RPC dicts to :meth:`handle_message`. Outbound RPCs go through the
injected ``Transport``. Time comes from the injected ``clock`` and randomness
from the injected ``rng``, which makes the whole state machine deterministic
under test.

Committed log entries are handed to ``apply_fn(index, command)`` in log
order. The node knows nothing about what commands mean.
"""

import random
import time
from typing import Any, Callable, Dict, List, Optional

from .log import Log, LogEntry
from .rpc import (
    append_entries,
    append_entries_response,
    request_vote,
    request_vote_response,
    timeout_now,
)
from .storage import Storage

FOLLOWER = "follower"
PRE_CANDIDATE = "pre-candidate"
CANDIDATE = "candidate"
LEADER = "leader"


class NotLeaderError(Exception):
    """Raised by :meth:`RaftNode.submit` when called on a non-leader."""

    def __init__(self, leader_id: Optional[str] = None) -> None:
        self.leader_id = leader_id
        hint = f" (leader is {leader_id})" if leader_id else ""
        super().__init__(f"not the leader{hint}")


class TransferInProgressError(Exception):
    """Raised by :meth:`RaftNode.submit` while a leadership transfer is pending."""

    def __init__(self, target: str) -> None:
        self.target = target
        super().__init__(f"leadership transfer to {target} in progress")


class RaftNode:
    def __init__(
        self,
        node_id: str,
        peer_ids: List[str],
        storage: Storage,
        transport: Any,
        *,
        apply_fn: Optional[Callable[[int, Any], None]] = None,
        election_timeout: tuple = (0.150, 0.300),
        heartbeat_interval: float = 0.050,
        clock: Optional[Callable[[], float]] = None,
        rng: Optional[random.Random] = None,
        pre_vote: bool = True,
    ) -> None:
        self.id = node_id
        self.peers = list(peer_ids)
        self.storage = storage
        self.transport = transport
        self.apply_fn = apply_fn or (lambda index, command: None)
        self.election_timeout = election_timeout
        self.heartbeat_interval = heartbeat_interval
        self.clock = clock or time.monotonic
        self.rng = rng or random.Random()
        self.pre_vote = pre_vote

        # Persistent state (survives crashes via storage).
        term, voted_for, entries = storage.load()
        self.current_term: int = term
        self.voted_for: Optional[str] = voted_for
        self.log = Log(entries)

        # Volatile state.
        self.state: str = FOLLOWER
        self.leader_id: Optional[str] = None
        self.commit_index: int = 0
        self.last_applied: int = 0
        self._votes_received: set = set()
        self._next_index: Dict[str, int] = {}
        self._match_index: Dict[str, int] = {}
        self._last_leader_contact: float = float("-inf")
        self._transfer_target: Optional[str] = None
        self._transfer_deadline: float = 0.0

        now = self.clock()
        self._election_deadline = now + self._random_timeout()
        self._heartbeat_deadline = 0.0

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def tick(self) -> None:
        """Advance timers. Call frequently (e.g. every 5-10 ms)."""
        now = self.clock()
        if self.state == LEADER:
            if self._transfer_target is not None and now >= self._transfer_deadline:
                self._transfer_target = None  # target never caught up; give up
            if now >= self._heartbeat_deadline:
                for peer in self.peers:
                    self._replicate_to(peer)
                self._heartbeat_deadline = now + self.heartbeat_interval
        elif now >= self._election_deadline:
            self._start_election()

    def handle_message(self, msg: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Process one inbound RPC. Returns a response dict, if any."""
        msg_type = msg.get("type")
        if msg_type == "request_vote":
            return self._on_request_vote(msg)
        if msg_type == "append_entries":
            return self._on_append_entries(msg)
        if msg_type == "request_vote_response":
            self._on_request_vote_response(msg)
        elif msg_type == "append_entries_response":
            self._on_append_entries_response(msg)
        elif msg_type == "timeout_now":
            self._on_timeout_now(msg)
        return None

    def status(self) -> Dict[str, Any]:
        """A JSON-serializable snapshot of this node, for logs and monitoring.

        ``next_index`` and ``match_index`` are only populated on the leader.
        """
        return {
            "id": self.id,
            "state": self.state,
            "term": self.current_term,
            "leader_id": self.leader_id,
            "peers": list(self.peers),
            "commit_index": self.commit_index,
            "last_applied": self.last_applied,
            "last_log_index": self.log.last_index(),
            "last_log_term": self.log.last_term(),
            "pre_vote": self.pre_vote,
            "transfer_target": self._transfer_target,
            "next_index": dict(self._next_index) if self.state == LEADER else {},
            "match_index": dict(self._match_index) if self.state == LEADER else {},
        }

    def transfer_leadership(self, target: str) -> None:
        """Hand leadership to ``target``, a peer that is caught up with the log.

        While the transfer is pending, :meth:`submit` raises
        :class:`TransferInProgressError`. If the target has not caught up
        within one election timeout, the transfer is abandoned and the leader
        resumes normal operation.
        """
        if self.state != LEADER:
            raise NotLeaderError(self.leader_id)
        if target not in self.peers:
            raise ValueError(f"unknown peer: {target!r}")
        self._transfer_target = target
        self._transfer_deadline = self.clock() + self.election_timeout[1]
        self._maybe_send_timeout_now()

    def submit(self, command: Any) -> int:
        """Append a command to the log. Only valid on the leader.

        Returns the 1-based log index of the new entry. Raises
        :class:`NotLeaderError` on followers and candidates.
        """
        if self.state != LEADER:
            raise NotLeaderError(self.leader_id)
        if self._transfer_target is not None:
            raise TransferInProgressError(self._transfer_target)
        entry = LogEntry(term=self.current_term, command=command)
        self.log.append(entry)
        self._persist()
        for peer in self.peers:
            self._replicate_to(peer)
        return self.log.last_index()

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------

    def _majority(self) -> int:
        return (len(self.peers) + 1) // 2 + 1

    def _persist(self) -> None:
        self.storage.save(self.current_term, self.voted_for, self.log.entries_from(1))

    def _random_timeout(self) -> float:
        low, high = self.election_timeout
        return self.rng.uniform(low, high)

    def _reset_election_deadline(self) -> None:
        self._election_deadline = self.clock() + self._random_timeout()

    def _log_up_to_date(self, candidate_term: int, candidate_index: int) -> bool:
        """Raft's election restriction: is the candidate's log at least as current?"""
        our_term = self.log.last_term()
        return candidate_term > our_term or (
            candidate_term == our_term and candidate_index >= self.log.last_index()
        )

    # -- state transitions -------------------------------------------

    def _become_follower(self, term: int) -> None:
        if term > self.current_term:
            self.current_term = term
            self.voted_for = None
            self._persist()
        self.state = FOLLOWER
        self.leader_id = None
        self._votes_received.clear()
        self._transfer_target = None
        self._reset_election_deadline()

    def _start_election(self) -> None:
        """Begin an election, probing with a PreVote first when enabled."""
        if self.pre_vote:
            self._start_pre_vote()
        else:
            self._start_real_election()

    def _start_pre_vote(self) -> None:
        # PreVote leaves term and voted_for alone, so a node cut off from the
        # cluster cannot inflate its term and depose a healthy leader later.
        self.state = PRE_CANDIDATE
        self.leader_id = None
        self._votes_received = {self.id}
        self._reset_election_deadline()
        if len(self._votes_received) >= self._majority():
            self._start_real_election()
            return
        msg = request_vote(
            sender=self.id,
            term=self.current_term + 1,
            candidate_id=self.id,
            last_log_index=self.log.last_index(),
            last_log_term=self.log.last_term(),
            pre_vote=True,
        )
        for peer in self.peers:
            self.transport.send(peer, msg)

    def _start_real_election(self) -> None:
        self.current_term += 1
        self.voted_for = self.id
        self._persist()
        self.state = CANDIDATE
        self.leader_id = None
        self._votes_received = {self.id}
        self._reset_election_deadline()
        if len(self._votes_received) >= self._majority():
            # Single-node cluster: already have a quorum.
            self._become_leader()
            return
        msg = request_vote(
            sender=self.id,
            term=self.current_term,
            candidate_id=self.id,
            last_log_index=self.log.last_index(),
            last_log_term=self.log.last_term(),
        )
        for peer in self.peers:
            self.transport.send(peer, msg)

    def _become_leader(self) -> None:
        self.state = LEADER
        self.leader_id = self.id
        self._votes_received.clear()
        last = self.log.last_index()
        for peer in self.peers:
            self._next_index[peer] = last + 1
            self._match_index[peer] = 0
        self._heartbeat_deadline = 0.0  # announce immediately on next tick
        for peer in self.peers:
            self._replicate_to(peer)

    # -- leader replication ------------------------------------------

    def _replicate_to(self, peer: str) -> None:
        next_idx = self._next_index.get(peer, 1)
        prev_index = next_idx - 1
        msg = append_entries(
            sender=self.id,
            term=self.current_term,
            leader_id=self.id,
            prev_log_index=prev_index,
            prev_log_term=self.log.term_at(prev_index),
            entries=[e.to_dict() for e in self.log.entries_from(next_idx)],
            leader_commit=self.commit_index,
        )
        self.transport.send(peer, msg)

    def _advance_commit_index(self) -> None:
        # Only entries from the current term can be committed by counting
        # replicas (Figure 8 in the Raft paper).
        for index in range(self.commit_index + 1, self.log.last_index() + 1):
            if self.log.term_at(index) != self.current_term:
                continue
            replicated = 1  # the leader itself
            for peer in self.peers:
                if self._match_index.get(peer, 0) >= index:
                    replicated += 1
            if replicated >= self._majority():
                self.commit_index = index
        self._apply_committed()

    def _apply_committed(self) -> None:
        while self.last_applied < self.commit_index:
            self.last_applied += 1
            entry = self.log.get(self.last_applied)
            if entry is not None:
                self.apply_fn(self.last_applied, entry.command)

    # -- RPC handlers -------------------------------------------------

    def _on_request_vote(self, msg: Dict[str, Any]) -> Dict[str, Any]:
        if msg.get("pre_vote"):
            return self._on_pre_vote(msg)
        term = msg["term"]
        if term > self.current_term:
            self._become_follower(term)
        granted = False
        if term == self.current_term:
            candidate = msg["candidate_id"]
            log_ok = self._log_up_to_date(
                msg["last_log_term"], msg["last_log_index"]
            )
            if (self.voted_for is None or self.voted_for == candidate) and log_ok:
                granted = True
                self.voted_for = candidate
                self._persist()
                self._reset_election_deadline()
        return request_vote_response(
            sender=self.id, term=self.current_term, vote_granted=granted
        )

    def _on_pre_vote(self, msg: Dict[str, Any]) -> Dict[str, Any]:
        # Grant only if we have not heard from a leader within the minimum
        # election timeout, so a live leader is never displaced by a PreVote.
        # Nothing here mutates term or voted_for.
        heard_recently = (
            self.clock() - self._last_leader_contact < self.election_timeout[0]
        )
        granted = (
            self.state != LEADER
            and not heard_recently
            and msg["term"] > self.current_term
            and self._log_up_to_date(msg["last_log_term"], msg["last_log_index"])
        )
        return request_vote_response(
            sender=self.id,
            term=self.current_term,
            vote_granted=granted,
            pre_vote=True,
        )

    def _on_request_vote_response(self, msg: Dict[str, Any]) -> None:
        if msg["term"] > self.current_term:
            self._become_follower(msg["term"])
            return
        if msg.get("pre_vote"):
            if self.state == PRE_CANDIDATE and msg["vote_granted"]:
                self._votes_received.add(msg["from"])
                if len(self._votes_received) >= self._majority():
                    self._start_real_election()
            return
        if self.state != CANDIDATE or msg["term"] != self.current_term:
            return
        if msg["vote_granted"]:
            self._votes_received.add(msg["from"])
            if len(self._votes_received) >= self._majority():
                self._become_leader()

    def _on_append_entries(self, msg: Dict[str, Any]) -> Dict[str, Any]:
        term = msg["term"]
        if term > self.current_term:
            self._become_follower(term)
        if term < self.current_term:
            return append_entries_response(
                sender=self.id,
                term=self.current_term,
                success=False,
                last_log_index=self.log.last_index(),
            )
        if self.state == LEADER:
            # Two leaders cannot legitimately share a term; keep ours.
            return append_entries_response(
                sender=self.id,
                term=self.current_term,
                success=False,
                last_log_index=self.log.last_index(),
            )
        # Valid contact from the current leader: stay follower.
        self.state = FOLLOWER
        self.leader_id = msg["leader_id"]
        self._last_leader_contact = self.clock()
        self._reset_election_deadline()

        prev_index = msg["prev_log_index"]
        if prev_index > 0 and (
            prev_index > self.log.last_index()
            or self.log.term_at(prev_index) != msg["prev_log_term"]
        ):
            return append_entries_response(
                sender=self.id,
                term=self.current_term,
                success=False,
                last_log_index=self.log.last_index(),
            )

        # Merge entries: delete conflicts, append anything new.
        index = prev_index + 1
        for raw in msg["entries"]:
            entry = LogEntry.from_dict(raw)
            existing = self.log.get(index)
            if existing is not None and existing.term != entry.term:
                self.log.truncate_from(index)
                existing = None
            if existing is None:
                self.log.append(entry)
            index += 1
        self._persist()

        if msg["leader_commit"] > self.commit_index:
            self.commit_index = min(msg["leader_commit"], self.log.last_index())
            self._apply_committed()
        return append_entries_response(
            sender=self.id,
            term=self.current_term,
            success=True,
            last_log_index=self.log.last_index(),
        )

    def _maybe_send_timeout_now(self) -> None:
        target = self._transfer_target
        if target is None:
            return
        if self._match_index.get(target, 0) >= self.log.last_index():
            self.transport.send(target, timeout_now(sender=self.id, term=self.current_term))
            self._transfer_target = None
        else:
            self._replicate_to(target)

    def _on_timeout_now(self, msg: Dict[str, Any]) -> None:
        # Sent by the leader we just caught up with, so our term already matches.
        # Skip PreVote: the transfer is deliberate, and the other voters must
        # not refuse it because they recently heard from the old leader.
        if msg["term"] != self.current_term or self.state == LEADER:
            return
        self._start_real_election()

    def _on_append_entries_response(self, msg: Dict[str, Any]) -> None:
        if msg["term"] > self.current_term:
            self._become_follower(msg["term"])
            return
        if self.state != LEADER or msg["term"] != self.current_term:
            return
        peer = msg["from"]
        if msg["success"]:
            self._match_index[peer] = msg["last_log_index"]
            self._next_index[peer] = msg["last_log_index"] + 1
            self._advance_commit_index()
            self._maybe_send_timeout_now()
        else:
            # Back off one entry and retry; converges because the follower
            # eventually accepts an empty heartbeat prefix.
            self._next_index[peer] = max(1, self._next_index.get(peer, 1) - 1)
            self._replicate_to(peer)
