# raft

A small, dependency-free implementation of the [Raft consensus algorithm](https://raft.github.io/) in Python. Standard library only, no third-party packages.

Implements the Raft core: **leader election** with randomized timeouts, **log replication** with consistency checks, and the **safety** rules that keep committed entries durable across leader changes.

## Architecture

```
                  +-------------------+
                  |     RaftNode      |   pure state machine: no threads,
                  |  tick() / handle_  |   no sockets, driven by the host
                  |  message() /       |
                  |  submit()          |
                  +--------+----------+
                           |  send(target, msg)
              +------------+------------+
              |                         |
     LocalTransport              TcpTransport
     (in-process, tests)         (JSON over TCP)
              |                         |
         +----+-------------------------+----+
         |        Storage (term, vote, log)   |
         |   MemoryStorage | FileStorage     |
         +-----------------------------------+
```

- `raft/node.py` — the consensus state machine: follower/candidate/leader roles, election timers, `RequestVote` / `AppendEntries` handling, commit advancement, ordered application of committed entries via an `apply_fn` callback.
- `raft/log.py` — 1-indexed in-memory log of `(term, command)` entries.
- `raft/storage.py` — persistent-state abstraction (`current_term`, `voted_for`, log). `MemoryStorage` for tests, `FileStorage` with atomic JSON writes for real nodes.
- `raft/transport.py` — `LocalTransport` (queued in-process delivery with partition simulation) and `TcpTransport` (length-prefixed JSON frames over persistent per-peer connections).
- `raft/server.py` — runnable node: `python -m raft.server --id n1 --port 5001 --peers n2=127.0.0.1:5002,...`. Persists state to disk and applies committed `{"op": "set", ...}` commands to an in-memory map.

Time and randomness are injected (`clock`, `rng`), so the whole cluster runs deterministically on a virtual clock in tests.

## Quickstart

Three nodes on localhost (one terminal each):

```bash
python -m raft.server --id n1 --port 5001 --peers n2=127.0.0.1:5002,n3=127.0.0.1:5003
python -m raft.server --id n2 --port 5002 --peers n1=127.0.0.1:5001,n3=127.0.0.1:5003
python -m raft.server --id n3 --port 5003 --peers n1=127.0.0.1:5001,n2=127.0.0.1:5002
```

Within a few hundred milliseconds one node prints `LEADER`; the others follow it. Kill the leader's process and watch the remaining two elect a replacement.

Embedding the node programmatically:

```python
from raft import RaftNode, MemoryStorage, LocalTransport

transport = LocalTransport()
nodes = {}
for i in ("n1", "n2", "n3"):
    peers = [p for p in ("n1", "n2", "n3") if p != i]
    node = RaftNode(i, peers, MemoryStorage(), transport,
                    apply_fn=lambda idx, cmd: print("applied", idx, cmd))
    transport.register(i, node.handle_message)
    nodes[i] = node

import time
deadline = time.monotonic() + 5
while time.monotonic() < deadline:
    for n in nodes.values():
        n.tick()
    transport.drain()
    time.sleep(0.005)

leader = next(n for n in nodes.values() if n.state == "leader")
leader.submit({"op": "set", "key": "hello", "value": "world"})
```

Commands must be JSON-serializable; they cross the TCP transport and the file storage as JSON.

## Testing

```bash
python -m unittest discover -s tests -v
```

The suite drives 3-node clusters on a virtual clock through `LocalTransport`:

- `test_election.py` — exactly one leader is elected, leadership is stable, all nodes converge on the leader's term.
- `test_replication.py` — entries submitted to the leader commit on every node with identical logs; `submit` on a follower raises `NotLeaderError`; a follower with a conflicting uncommitted suffix converges to the leader's log.
- `test_stepdown.py` — a partitioned leader is replaced at a higher term and steps down to follower when the partition heals; stale-term `RequestVote`s are denied.
- `test_storage.py` — persistent-state round-trips for both storage backends.

## Design notes

- **Election safety** follows the paper: a candidate wins only with a majority, votes are granted at most once per term, and a candidate's log must be at least as up-to-date as the voter's.
- **Commit rule**: the leader advances `commit_index` only for entries from its *current* term replicated on a majority, then applies them in order. Followers learn the commit point via `leader_commit` on heartbeats.
- **Log consistency**: a follower rejects `AppendEntries` whose `prev_log_index`/`prev_log_term` don't match, and the leader backs off `next_index` until the logs align, deleting conflicting suffixes.
- **Persistence**: `current_term`, `voted_for`, and the log are saved on every mutation, so a restarted node never double-votes or forgets committed entries.

## License

MIT — see [LICENSE](LICENSE).
