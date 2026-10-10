"""Leadership transfer: a caught-up follower takes over on request."""

import unittest

from raft.node import FOLLOWER, LEADER, NotLeaderError, TransferInProgressError
from tests.cluster import Cluster


def _elect(cluster):
    assert cluster.run_until(lambda: len(cluster.leaders()) == 1)
    return cluster.leader()


class TestLeadershipTransfer(unittest.TestCase):
    def test_transfer_to_caught_up_follower(self):
        cluster = Cluster(["n1", "n2", "n3"])
        old = _elect(cluster)
        target = next(n for n in cluster.nodes.values() if n.id != old.id)

        old.transfer_leadership(target.id)
        self.assertTrue(cluster.run_until(lambda: target.state == LEADER))
        self.assertEqual(old.state, FOLLOWER)
        self.assertEqual(len(cluster.leaders()), 1)
        self.assertEqual(cluster.leader().id, target.id)

    def test_transfer_preserves_committed_entries(self):
        cluster = Cluster(["n1", "n2", "n3"])
        old = _elect(cluster)
        old.submit({"op": "set", "key": "a", "value": 1})
        self.assertTrue(cluster.run_until(
            lambda: all(n.commit_index >= 1 for n in cluster.nodes.values())
        ))
        target = next(n for n in cluster.nodes.values() if n.id != old.id)

        old.transfer_leadership(target.id)
        self.assertTrue(cluster.run_until(lambda: target.state == LEADER))
        target.submit({"op": "set", "key": "b", "value": 2})
        self.assertTrue(cluster.run_until(
            lambda: all(n.commit_index >= 2 for n in cluster.nodes.values())
        ))
        for node_id in cluster.nodes:
            self.assertEqual(
                cluster.applied[node_id],
                [{"op": "set", "key": "a", "value": 1}, {"op": "set", "key": "b", "value": 2}],
            )

    def test_submit_blocked_while_transfer_pending(self):
        cluster = Cluster(["n1", "n2", "n3"])
        old = _elect(cluster)
        old.submit({"op": "set", "key": "a", "value": 1})
        cluster.run_until(lambda: old.commit_index >= 1)
        target = next(n for n in cluster.nodes.values() if n.id != old.id)

        # Isolate the target so it cannot catch up, leaving the transfer pending.
        cluster.transport.partition(target.id)
        old.submit({"op": "set", "key": "b", "value": 2})
        old.transfer_leadership(target.id)
        with self.assertRaises(TransferInProgressError):
            old.submit({"op": "set", "key": "c", "value": 3})

    def test_transfer_abandoned_after_timeout(self):
        cluster = Cluster(["n1", "n2", "n3"])
        old = _elect(cluster)
        old.submit({"op": "set", "key": "a", "value": 1})
        cluster.run_until(lambda: old.commit_index >= 1)
        target = next(n for n in cluster.nodes.values() if n.id != old.id)

        cluster.transport.partition(target.id)
        old.submit({"op": "set", "key": "b", "value": 2})
        old.transfer_leadership(target.id)
        for _ in range(int(old.election_timeout[1] / 0.005) + 50):
            cluster.step()
        self.assertEqual(old.state, LEADER)
        old.submit({"op": "set", "key": "c", "value": 3})  # no longer blocked

    def test_transfer_requires_leader_and_known_peer(self):
        cluster = Cluster(["n1", "n2", "n3"])
        leader = _elect(cluster)
        follower = next(n for n in cluster.nodes.values() if n.id != leader.id)
        with self.assertRaises(NotLeaderError):
            follower.transfer_leadership(leader.id)
        with self.assertRaises(ValueError):
            leader.transfer_leadership("nobody")


if __name__ == "__main__":
    unittest.main()
