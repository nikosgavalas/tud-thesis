import shutil
import unittest
from pathlib import Path

from fuzzytester import FuzzyTester
from kevo import LSMTree, PathReplica, MinioReplica


class TestLSMTree(unittest.TestCase, FuzzyTester):
    dir = Path('./data_test')

    def setUp(self):
        self.replica = PathReplica(self.dir.name, '/tmp/remote')
        # self.replica = MinioReplica(self.dir.name, 'testbucket')
        self.dir.mkdir()

    def tearDown(self):
        shutil.rmtree(self.dir.name)
        self.replica.destroy()

    def test_basic(self):
        l = LSMTree(self.dir.name, max_runs_per_level=3, density_factor=3, memtable_bytes_limit=10)

        l.set(b'b', b'2')
        l.set(b'asdf', b'12345')
        l.set(b'cc', b'cici345')
        l.set(b'b', b'3')

        self.assertEqual(l.get(b'b'), b'3')
        self.assertEqual(l.get(b'asdf'), b'12345')
        self.assertEqual(l.get(b'cc'), b'cici345')

        l.close()

    def test_fuzzy_granular(self):
        self.fuzzy_test(LSMTree, args={'data_dir': self.dir.name, 'max_runs_per_level': 2, 'density_factor': 3,
                                       'memtable_bytes_limit': 10}, key_len_range=(1, 10), val_len_range=(0, 13),
                        n_items=10, n_iter=10_000, seeds=[1], test_recovery=False, test_replica=False)

    def test_fuzzy_realistic(self):
        self.fuzzy_test(LSMTree, args={'data_dir': self.dir.name, 'replica': None}, key_len_range=(1, 10),
                        val_len_range=(0, 13), n_items=100, n_iter=1_000_000, seeds=[1], test_recovery=True,
                        test_replica=False)

    def test_fuzzy_large_kvs(self):
        self.fuzzy_test(LSMTree, args={'data_dir': self.dir.name, 'max_key_len': 100_000, 'max_value_len': 100_000,
                                       'max_runs_per_level': 2, 'density_factor': 3, 'memtable_bytes_limit': 10},
                        key_len_range=(1, 100_000), val_len_range=(0, 100_000), n_items=10, n_iter=100, seeds=[1],
                        test_recovery=False, test_replica=False)

    def test_fuzzy_recovery(self):
        self.fuzzy_test(LSMTree,
                        args={'data_dir': self.dir.name, 'memtable_bytes_limit': 100},
                        key_len_range=(1, 10), val_len_range=(0, 13), n_items=10_000, n_iter=10_000, seeds=[1],
                        test_recovery=True, test_replica=False)

    # def test_fuzzy_replication(self):
    #     self.fuzzy_test(LSMTree,
    #                     args={'data_dir': self.dir.name, 'memtable_bytes_limit': 100_000, 'replica': self.replica},
    #                     key_len_range=(1, 10), val_len_range=(0, 13), n_items=100, n_iter=1_000_000, seeds=[1],
    #                     test_recovery=True, test_replica=True)

    def test_wal(self):
        l1 = LSMTree(self.dir.name)

        l1.set(b'a', b'1')
        l1.set(b'b', b'2')
        l1.set(b'c', b'3')

        l1.close()

        l2 = LSMTree(self.dir.name)

        self.assertEqual(l2.get(b'a'), b'1')
        self.assertEqual(l2.get(b'b'), b'2')
        self.assertEqual(l2.get(b'c'), b'3')

        l2.close()

    # def test_replica(self):
    #     db = LSMTree(self.dir.name, replica=self.replica)
    #     db.set(b'a', b'1')
    #     db.set(b'b', b'2')
    #     db.snapshot()
    #     db.set(b'a', b'3')
    #     db.set(b'b', b'4')
    #     db.close()
    #
    #     shutil.rmtree(self.dir.name)
    #
    #     db = LSMTree(self.dir.name, replica=self.replica)
    #     self.assertEqual(db.get(b'a'), b'3')
    #     self.assertEqual(db.get(b'b'), b'4')
    #     db.restore(version=1)
    #     self.assertEqual(db.get(b'a'), b'1')
    #     self.assertEqual(db.get(b'b'), b'2')
    #     db.close()


if __name__ == "__main__":
    unittest.main()
