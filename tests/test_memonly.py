import shutil
import unittest
from pathlib import Path

from fuzzytester import FuzzyTester
from kevo import MemOnly, PathRemote, MinioRemote


class TestMemOnly(unittest.TestCase, FuzzyTester):
    dir = Path('./data_test')

    def setUp(self):
        self.remote = PathRemote('/tmp/remote')
        # self.remote = MinioRemote('testbucket')
        self.dir.mkdir()

    def tearDown(self):
        shutil.rmtree(self.dir.name)
        self.remote.destroy()

    def test_basic(self):
        db = MemOnly(self.dir)

        db.set(b'b', b'2')
        db.set(b'asdf', b'12345')
        db.set(b'cc', b'cici345')
        db.set(b'b', b'3')

        self.assertEqual(db.get(b'b'), b'3')
        self.assertEqual(db.get(b'asdf'), b'12345')
        self.assertEqual(db.get(b'cc'), b'cici345')

        db.close()

    def test_fuzzy_basic(self):
        self.fuzzy_test(MemOnly, args={'data_dir': self.dir.name, 'remote': self.remote}, key_len_range=(1, 10),
                        val_len_range=(0, 10), n_items=1_000, n_iter=10_000, seeds=[1], test_recovery=True,
                        test_remote=True)

    def test_fuzzy_snapshot_continuous(self):
        self.fuzzy_test_snapshot_continuous(MemOnly, args={'data_dir': self.dir.name, 'remote': self.remote},
                                            key_len_range=(1, 10), val_len_range=(0, 13), n_items=10_000, n_iter=10_000,
                                            seed=1)

    def test_versioning(self):
        db = MemOnly(self.dir, remote=self.remote)

        db.set(b'1', b'1')
        db.set(b'2', b'2')
        db.snapshot(0)
        db.set(b'1', b'3')
        db.snapshot(1)
        db.close()

        shutil.rmtree(self.dir)

        db = MemOnly(self.dir, remote=self.remote)

        self.assertEqual(db.get(b'1'), b'3')
        self.assertEqual(db.get(b'2'), b'2')

        db.close()


if __name__ == "__main__":
    unittest.main()
