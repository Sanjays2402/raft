"""Persistent state round-trips through both storage backends."""

import os
import tempfile
import unittest

from raft.log import LogEntry
from raft.storage import FileStorage, MemoryStorage


class TestStorage(unittest.TestCase):
    def _roundtrip(self, storage):
        entries = [LogEntry(term=1, command={"op": "set", "k": "v"}), LogEntry(term=2, command="x")]
        storage.save(3, "n2", entries)
        term, voted_for, loaded = storage.load()
        self.assertEqual(term, 3)
        self.assertEqual(voted_for, "n2")
        self.assertEqual([e.to_dict() for e in loaded], [e.to_dict() for e in entries])

    def test_memory_storage(self):
        self._roundtrip(MemoryStorage())

    def test_file_storage(self):
        with tempfile.TemporaryDirectory() as d:
            self._roundtrip(FileStorage(os.path.join(d, "n1.json")))

    def test_file_storage_empty_on_first_load(self):
        with tempfile.TemporaryDirectory() as d:
            term, voted_for, entries = FileStorage(os.path.join(d, "new.json")).load()
            self.assertEqual(term, 0)
            self.assertIsNone(voted_for)
            self.assertEqual(entries, [])


if __name__ == "__main__":
    unittest.main()
