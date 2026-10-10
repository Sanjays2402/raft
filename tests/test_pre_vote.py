"""PreVote: a node cut off from the cluster must not inflate the term."""

import unittest

from raft import rpc
from raft.node import FOLLOWER
from tests.cluster import Cluster


class TestPreVote(unittest.TestCase):
    def test_partitioned_follower_does_not_bump_term(self):
        cluster = Cluster(["n1", "n2", "n3"])
        cluster.run_until(lambda: len(cluster.leaders()) == 1)
        leader = cluster.leader()
        assert leader is not None
        term = leader.current_term
        isolated = next(n for n in cluster.nodes.values() if n.id != leader.id)

        cluster.transport.partition(isolated.id)
        # Several election timeouts pass; without PreVote this node would
        # have started real elections and incremented its term every time.
        for _ in range(1000):
            cluster.step()
        self.assertEqual(isolated.current_term, term)

        cluster.transport.heal(isolated.id)
        self.assertTrue(cluster.run_until(lambda: isolated.state == FOLLOWER))
        self.assertEqual(cluster.leader().id, leader.id)
        self.assertEqual(cluster.leader().current_term, term)

    def test_pre_vote_denied_while_leader_is_live(self):
        cluster = Cluster(["n1", "n2", "n3"])
        cluster.run_until(lambda: len(cluster.leaders()) == 1)
        leader = cluster.leader()
        assert leader is not None
        follower = next(n for n in cluster.nodes.values() if n.id != leader.id)

        response = follower.handle_message(
            rpc.request_vote(
                sender="ghost",
                term=follower.current_term + 1,
                candidate_id="ghost",
                last_log_index=follower.log.last_index(),
                last_log_term=follower.log.last_term(),
                pre_vote=True,
            )
        )
        self.assertFalse(response["vote_granted"])
        self.assertEqual(response["term"], follower.current_term)
        self.assertEqual(follower.voted_for, leader.id)

    def test_pre_vote_grant_leaves_state_untouched(self):
        cluster = Cluster(["n1", "n2", "n3"])
        node = cluster.nodes["n1"]
        # No leader contact yet, so a fresh node will grant a PreVote.
        before = (node.current_term, node.voted_for)
        response = node.handle_message(
            rpc.request_vote(
                sender="n2",
                term=node.current_term + 1,
                candidate_id="n2",
                last_log_index=0,
                last_log_term=0,
                pre_vote=True,
            )
        )
        self.assertTrue(response["vote_granted"])
        self.assertTrue(response["pre_vote"])
        self.assertEqual((node.current_term, node.voted_for), before)

    def test_election_still_works_without_pre_vote(self):
        cluster = Cluster(["n1", "n2", "n3"], pre_vote=False)
        self.assertTrue(cluster.run_until(lambda: len(cluster.leaders()) == 1))


if __name__ == "__main__":
    unittest.main()
