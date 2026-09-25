"""Leader step-down: a deposed leader yields to the higher term."""

import unittest

from raft.node import FOLLOWER, LEADER
from tests.cluster import Cluster


class TestStepDown(unittest.TestCase):
    def test_leader_steps_down_on_higher_term(self):
        cluster = Cluster(["n1", "n2", "n3"])
        cluster.run_until(lambda: len(cluster.leaders()) == 1)
        old_leader = cluster.leader()
        assert old_leader is not None
        old_term = old_leader.current_term

        # Partition the leader away; the remaining two form a majority and
        # elect a new leader at a higher term.
        cluster.transport.partition(old_leader.id)
        reelected = cluster.run_until(
            lambda: any(
                n.state == LEADER and n.id != old_leader.id and n.current_term > old_term
                for n in cluster.nodes.values()
            )
        )
        self.assertTrue(reelected, "no new leader elected behind the partition")
        new_leader = next(
            n
            for n in cluster.nodes.values()
            if n.state == LEADER and n.id != old_leader.id
        )

        # Heal the partition. The old leader's next contact with the higher
        # term must demote it to follower.
        cluster.transport.heal(old_leader.id)
        stepped_down = cluster.run_until(
            lambda: old_leader.state == FOLLOWER
            and old_leader.current_term == new_leader.current_term
        )
        self.assertTrue(stepped_down, "old leader did not step down")
        self.assertEqual(old_leader.leader_id, new_leader.id)
        self.assertEqual(len(cluster.leaders()), 1)
        self.assertEqual(cluster.leader().id, new_leader.id)

    def test_stale_candidate_cannot_disrupt_leader(self):
        cluster = Cluster(["n1", "n2", "n3"])
        cluster.run_until(lambda: len(cluster.leaders()) == 1)
        leader = cluster.leader()
        assert leader is not None

        # A RequestVote from an older term is denied and changes nothing.
        from raft import rpc

        follower_id = next(n for n in cluster.nodes if n != leader.id)
        follower = cluster.nodes[follower_id]
        response = follower.handle_message(
            rpc.request_vote(
                sender="ghost",
                term=leader.current_term - 1 if leader.current_term > 0 else 0,
                candidate_id="ghost",
                last_log_index=0,
                last_log_term=0,
            )
        )
        self.assertFalse(response["vote_granted"])
        self.assertEqual(cluster.leader().id, leader.id)


if __name__ == "__main__":
    unittest.main()
