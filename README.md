# raft

**Raft consensus in pure Python, no dependencies.** A readable, battle-tested core you can actually learn from: leader election, log replication, and the safety rules that make distributed agreement work.

[![License: MIT](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)
[![Python 3.9+](https://img.shields.io/badge/python-3.9%2B-blue.svg)](raft/)
[![Tests](https://img.shields.io/badge/tests-22%20passing-brightgreen.svg)](tests/)

## Why Raft, and why this one

Consensus is the hard problem behind etcd, Kubernetes, and every database that claims high availability. [Raft](https://raft.github.io/) is the algorithm designed to be understood, and this implementation takes that seriously: the whole state machine lives in one module (`raft/node.py`), reads top to bottom like the paper, and runs with nothing but the standard library.

Most Raft code out there is either a toy that skips the safety rules or a framework buried in dependencies. This one keeps the actual guarantees, election safety, log consistency, leader completeness, so what you read here maps one-to-one onto real systems. Use it to learn consensus, to prototype a replicated service, or as a reference implementation when debugging your own.

## Features

- **Leader election** with randomized election timeouts; exactly one leader per term
- **Log replication** with consistency checks, `next_index` backoff, and ordered commit
- **PreVote** (on by default): a partitioned node can't inflate its term and depose a healthy leader when it rejoins
- **Leadership transfer**: hand leadership to a caught-up follower without waiting for a timeout
- **Persistent state**: `current_term`, `voted_for`, and the log survive restarts via atomic file writes
- **Deterministic testing**: time and randomness are injected, so whole clusters run on a virtual clock
- **Pluggable transports**: in-process delivery for tests, JSON-over-TCP for real nodes

## Quickstart: a cluster in 60 seconds

Clone the repo, then start three nodes (one terminal each):

```bash
git clone https://github.com/Sanjays2402/raft.git && cd raft
python -m raft.server --id n1 --port 5001 --peers n2=127.0.0.1:5002,n3=127.0.0.1:5003
python -m raft.server --id n2 --port 5002 --peers n1=127.0.0.1:5001,n3=127.0.0.1:5003
python -m raft.server --id n3 --port 5003 --peers n1=127.0.0.1:5001,n2=127.0.0.1:5002
```

Each node prints a per-second status line. Within a few hundred milliseconds one of them will show `state=LEADER <- LEADER`; the others follow it. Each node also runs a tiny key-value store on top of Raft: every committed `{"op": "set", "key": ..., "value": ...}` command is applied to an in-memory map.

Kill the leader's process and watch the remaining two elect a replacement within an election timeout, with no lost committed writes. Persistent state lives in `./data/<node-id>.json`, so a restarted node rejoins with its term and log intact.

## Embed it in your own code

The state machine is pure: no threads, no sockets. You drive it with `tick()`, `handle_message()`, and `submit()`:

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
index = leader.submit({"op": "set", "key": "hello", "value": "world"})
print("committed at index", index)
```

`submit()` on a follower raises `NotLeaderError` (the exception carries the known leader's id). Commands must be JSON-serializable; they cross the TCP transport and the file storage as JSON.

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

- `raft/node.py` — the consensus state machine: follower/candidate/leader roles, election timers, `RequestVote` / `AppendEntries` handling, commit advancement, ordered application of committed entries via an `apply_fn` callback
- `raft/log.py` — 1-indexed in-memory log of `(term, command)` entries
- `raft/storage.py` — persistent-state abstraction (`current_term`, `voted_for`, log). `MemoryStorage` for tests, `FileStorage` with atomic JSON writes for real nodes
- `raft/transport.py` — `LocalTransport` (queued in-process delivery with partition simulation) and `TcpTransport` (length-prefixed JSON frames over persistent per-peer connections)
- `raft/rpc.py` — message constructors for `RequestVote` (including the PreVote variant), `AppendEntries`, and leadership transfer (`TimeoutNow`)
- `raft/server.py` — runnable node with a persistent key-value store on top

## Testing

```bash
python -m unittest discover -s tests -v
```

The suite drives 3-node clusters on a virtual clock through `LocalTransport`:

- `test_election.py` — exactly one leader is elected, leadership is stable, all nodes converge on the leader's term
- `test_replication.py` — entries submitted to the leader commit on every node with identical logs; `submit` on a follower raises `NotLeaderError`; a follower with a conflicting uncommitted suffix converges to the leader's log
- `test_stepdown.py` — a partitioned leader is replaced at a higher term and steps down to follower when the partition heals; stale-term `RequestVote`s are denied
- `test_storage.py` — persistent-state round-trips for both storage backends
- `test_pre_vote.py` — a partitioned node keeps its term; a live leader denies pre-votes; elections still work with PreVote disabled
- `test_transfer.py` — leadership moves to the target and committed entries survive; submits are blocked during a pending transfer; stalled transfers time out; invalid targets are rejected
- `test_status.py` — `status()` is serializable and reports leader replication progress

## Design notes

- **Election safety** follows the paper: a candidate wins only with a majority, votes are granted at most once per term, and a candidate's log must be at least as up-to-date as the voter's.
- **Commit rule**: the leader advances `commit_index` only for entries from its *current* term replicated on a majority, then applies them in order. Followers learn the commit point via `leader_commit` on heartbeats.
- **Log consistency**: a follower rejects `AppendEntries` whose `prev_log_index`/`prev_log_term` don't match, and the leader backs off `next_index` until the logs align, deleting conflicting suffixes.
- **Persistence**: `current_term`, `voted_for`, and the log are saved on every mutation, so a restarted node never double-votes or forgets committed entries.

## License

MIT — see [LICENSE](LICENSE).

## Terms & Disclaimer

By using this project you agree to the following.

- **No warranty.** This software is provided "as is", without warranty of any kind, express or implied, including merchantability, fitness for a particular purpose, and non-infringement.
- **No liability.** To the maximum extent permitted by law, the author and contributors are not liable for any claim, loss, or damage (direct, indirect, incidental, or consequential, including lost data or profits) arising from its use.
- **Your responsibility.** You use it at your own risk and are responsible for complying with the laws that apply to you.
- **Third parties.** Names, logos, APIs, and services of others belong to their owners and are used for identification only; no affiliation or endorsement is implied.
- **Your statutory rights.** Nothing here limits rights that cannot be waived by agreement, including under consumer-protection laws in the USA, Canada (including Quebec), and India (Consumer Protection Act, 2019).
- **License.** Use, copying, and distribution are governed by the [LICENSE](LICENSE) file; if anything here conflicts with it, the license controls.

## Content, Assets & Fair Use

- **Original work.** Unless noted otherwise (including below), the code, text, design, and images in this repository were created by the author.
- **Libraries, fonts, and icons.** Third-party libraries, fonts (for example Google Fonts), and icon sets belong to their creators and are used under their own licenses; see the dependency manifests and each source for terms.
- **Logos, brands, and screenshots.** Third-party names, logos, and screenshots appear only to identify or describe their products (nominative use). They remain the property of their owners and do not imply affiliation or endorsement.
- **Ideas and inspiration.** Where this project is inspired by another product, game, or idea, it is an independent implementation. It borrows only general ideas, mechanics, or styles, which are not anyone's exclusive property, and is meant to contain no third-party code or artwork used without permission.
- **Fair use and fair dealing.** Any third-party material is used in limited ways for purposes such as education, research, review, commentary, or identification, relying on fair use in the USA (17 U.S.C. § 107), fair dealing in Canada (Copyright Act, ss. 29–29.2), and fair dealing in India (Copyright Act, 1957, s. 52), where they apply.
- **Concerns and takedown requests.** If you own rights in something here and believe it is used improperly, [open an issue](https://github.com/Sanjays2402/raft/issues) or contact the author through their GitHub profile with a link to the material and your claim. It will be reviewed promptly and credited, changed, or removed as appropriate. You may also use GitHub's DMCA process.
- **Contributions.** By contributing, you confirm you have the right to submit your contribution and agree it is licensed under this repository's license.
