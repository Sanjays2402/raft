"""status() gives a JSON-serializable snapshot of a node."""

import json
import unittest

from raft.node import LEADER
from tests.cluster import Cluster


class TestStatus(unittest.TestCase):
    def test_leader_status_reports_replication_progress(self):
        cluster = Cluster(["n1", "n2", "n3"])
        cluster.run_until(lambda: len(cluster.leaders()) == 1)
        leader = cluster.leader()
        leader.submit({"op": "set", "key": "a", "value": 1})
        cluster.run_until(lambda: leader.commit_index >= 1)

        status = leader.status()
        json.dumps(status)  # must be serializable
        self.assertEqual(status["state"], LEADER)
        self.assertEqual(status["leader_id"], leader.id)
        self.assertEqual(status["term"], leader.current_term)
        self.assertEqual(status["last_log_index"], 1)
        self.assertEqual(status["commit_index"], 1)
        self.assertEqual(set(status["match_index"]), set(leader.peers))

    def test_follower_status_has_no_replication_fields(self):
        cluster = Cluster(["n1", "n2", "n3"])
        cluster.run_until(lambda: len(cluster.leaders()) == 1)
        follower = next(n for n in cluster.nodes.values() if n.state != LEADER)
        status = follower.status()
        self.assertEqual(status["next_index"], {})
        self.assertEqual(status["match_index"], {})
        self.assertIsNone(status["transfer_target"])


if __name__ == "__main__":
    unittest.main()
