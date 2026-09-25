"""Log replication: entries submitted to the leader reach every node."""

import unittest

from raft.node import NotLeaderError
from tests.cluster import Cluster


class TestReplication(unittest.TestCase):
    def test_entries_replicate_to_all_followers(self):
        cluster = Cluster(["n1", "n2", "n3"])
        cluster.run_until(lambda: len(cluster.leaders()) == 1)
        leader = cluster.leader()
        assert leader is not None

        commands = [{"op": "set", "key": f"k{i}", "value": i} for i in range(5)]
        for cmd in commands:
            leader.submit(cmd)

        replicated = cluster.run_until(
            lambda: all(n.commit_index == 5 for n in cluster.nodes.values())
        )
        self.assertTrue(replicated, "entries were not committed on every node")

        reference = cluster.nodes["n1"].log.to_dicts()
        for node_id, node in cluster.nodes.items():
            self.assertEqual(
                node.log.to_dicts(), reference, f"log diverged on {node_id}"
            )
            self.assertEqual(cluster.applied[node_id], commands)

    def test_submit_on_follower_raises(self):
        cluster = Cluster(["n1", "n2", "n3"])
        cluster.run_until(lambda: len(cluster.leaders()) == 1)
        follower = next(
            n for n in cluster.nodes.values() if n.id != cluster.leader().id
        )
        with self.assertRaises(NotLeaderError) as ctx:
            follower.submit({"op": "set", "key": "x", "value": 1})
        self.assertEqual(ctx.exception.leader_id, cluster.leader().id)

    def test_conflicting_entries_are_overwritten(self):
        # A follower with a stale uncommitted suffix must converge to the
        # leader's log.
        cluster = Cluster(["n1", "n2", "n3"])
        cluster.run_until(lambda: len(cluster.leaders()) == 1)
        leader = cluster.leader()
        assert leader is not None

        victim_id = next(n for n in cluster.nodes if n != leader.id)
        victim = cluster.nodes[victim_id]
        # Inject a bogus uncommitted entry from an older term directly into
        # the follower's log (as if a crashed past leader had replicated it
        # to this follower only).
        from raft.log import LogEntry

        bogus_term = max(0, leader.current_term - 1)
        victim.log.append(LogEntry(term=bogus_term, command="bogus"))
        victim._persist()

        leader.submit({"op": "set", "key": "k", "value": 1})
        converged = cluster.run_until(
            lambda: all(
                n.log.to_dicts() == leader.log.to_dicts()
                for n in cluster.nodes.values()
            )
            and all(n.commit_index == 1 for n in cluster.nodes.values())
        )
        self.assertTrue(converged, "follower log did not converge to the leader's")
        self.assertEqual(cluster.applied[victim_id], [{"op": "set", "key": "k", "value": 1}])


if __name__ == "__main__":
    unittest.main()
