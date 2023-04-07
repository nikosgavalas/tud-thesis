import shutil
import unittest
from pathlib import Path

from fuzzytester import FuzzyTester
from kevo import LSMTree, PathReplica


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

    def test_fuzzy_1(self):
        # highly granular, fishing for edgecases
        self.fuzzy_test(LSMTree, args={'data_dir': self.dir.name, 'max_runs_per_level': 2, 'density_factor': 3,
                                       'memtable_bytes_limit': 10}, key_len_range=(1, 10), val_len_range=(0, 13),
                        n_items=10, n_iter=10_000, seeds=[1], test_recovery=False, test_replica=False)

    def test_fuzzy_2(self):
        # more realistic, with replica
        self.fuzzy_test(LSMTree, args={'data_dir': self.dir.name, 'replica': self.replica}, key_len_range=(1, 10),
                        val_len_range=(0, 13), n_items=100, n_iter=1_000_000, seeds=[1], test_recovery=True,
                        test_replica=True)

    def test_fuzzy_3(self):
        # test for large keys/values
        self.fuzzy_test(LSMTree, args={'data_dir': self.dir.name, 'max_key_len': 100_000, 'max_value_len': 100_000,
                                       'max_runs_per_level': 2, 'density_factor': 3, 'memtable_bytes_limit': 10},
                        key_len_range=(1, 100_000), val_len_range=(0, 100_000), n_items=10, n_iter=100, seeds=[1],
                        test_recovery=False, test_replica=False)

    def test_merge(self):
        l = LSMTree(self.dir.name, max_runs_per_level=2, density_factor=3, memtable_bytes_limit=10)

        l.set(b'a1', b'a1')
        l.set(b'a1', b'a11')
        l.set(b'a2', b'a2')

        l.set(b'a2', b'a22')
        l.set(b'a3', b'a3')
        l.set(b'a4', b'a4')

        l.set(b'a3', b'a31')
        l.set(b'a5', b'a5')
        l.set(b'a6', b'a6')

        with (self.dir / 'L1.0.run').open('br') as f:
            content = f.read()

        self.assertEqual(content, b'\x02a1\x03a11\x02a2\x03a22\x02a3\x03a31\x02a4\x02a4\x02a5\x02a5\x02a6\x02a6')

        l.close()

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

    def test_replica(self):
        l = LSMTree(self.dir.name, replica=self.replica)
        l.set(b'a', b'1')
        l.set(b'b', b'2')
        l.close()
        shutil.rmtree(self.dir.name)

        l = LSMTree(self.dir.name, replica=self.replica)
        self.assertEqual(l.get(b'a'), b'1')
        self.assertEqual(l.get(b'b'), b'2')
        l.close()


if __name__ == "__main__":
    unittest.main()
