"""Leader election: exactly one leader emerges and stays stable."""

import unittest

from raft.node import FOLLOWER, LEADER
from tests.cluster import Cluster


class TestElection(unittest.TestCase):
    def test_single_leader_elected(self):
        cluster = Cluster(["n1", "n2", "n3"])
        elected = cluster.run_until(lambda: len(cluster.leaders()) == 1)
        self.assertTrue(elected, "no leader elected within the timeout")
        self.assertEqual(len(cluster.leaders()), 1)

    def test_leader_is_stable(self):
        cluster = Cluster(["n1", "n2", "n3"])
        cluster.run_until(lambda: len(cluster.leaders()) == 1)
        first = cluster.leader()
        assert first is not None
        first_id, first_term = first.id, first.current_term
        # Two more virtual seconds: no flip-flopping, no new terms.
        cluster.run_until(lambda: False, timeout=2.0)
        self.assertEqual(len(cluster.leaders()), 1)
        self.assertEqual(cluster.leader().id, first_id)
        self.assertEqual(cluster.leader().current_term, first_term)

    def test_all_nodes_converge_on_term(self):
        cluster = Cluster(["n1", "n2", "n3"])
        cluster.run_until(lambda: len(cluster.leaders()) == 1)
        term = cluster.leader().current_term
        converged = cluster.run_until(
            lambda: all(t == term for t in cluster.terms().values())
        )
        self.assertTrue(converged, f"terms did not converge: {cluster.terms()}")
        for node in cluster.nodes.values():
            if node.state == FOLLOWER:
                self.assertEqual(node.leader_id, cluster.leader().id)


if __name__ == "__main__":
    unittest.main()
