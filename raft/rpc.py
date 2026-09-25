"""Raft RPC message constructors.

Messages are plain JSON-serializable dicts so they can travel over either the
in-process transport (tests, embedding) or the TCP transport unchanged.
Every message carries a ``from`` field with the sender's node id.
"""

from typing import Any, Dict, List


def request_vote(
    sender: str, term: int, candidate_id: str, last_log_index: int, last_log_term: int
) -> Dict[str, Any]:
    return {
        "type": "request_vote",
        "from": sender,
        "term": term,
        "candidate_id": candidate_id,
        "last_log_index": last_log_index,
        "last_log_term": last_log_term,
    }


def request_vote_response(
    sender: str, term: int, vote_granted: bool
) -> Dict[str, Any]:
    return {
        "type": "request_vote_response",
        "from": sender,
        "term": term,
        "vote_granted": vote_granted,
    }


def append_entries(
    sender: str,
    term: int,
    leader_id: str,
    prev_log_index: int,
    prev_log_term: int,
    entries: List[Dict[str, Any]],
    leader_commit: int,
) -> Dict[str, Any]:
    return {
        "type": "append_entries",
        "from": sender,
        "term": term,
        "leader_id": leader_id,
        "prev_log_index": prev_log_index,
        "prev_log_term": prev_log_term,
        "entries": entries,
        "leader_commit": leader_commit,
    }


def append_entries_response(
    sender: str, term: int, success: bool, last_log_index: int
) -> Dict[str, Any]:
    return {
        "type": "append_entries_response",
        "from": sender,
        "term": term,
        "success": success,
        "last_log_index": last_log_index,
    }
