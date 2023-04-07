import shutil
import unittest
from pathlib import Path

from kevo import AppendLog, PathReplica
from fuzzytester import FuzzyTester


class TestAppendLog(unittest.TestCase, FuzzyTester):
    dir = Path('./data_test')

    def setUp(self):
        self.replica = PathReplica(self.dir.name, '/tmp/remote')
        # self.replica = MinioReplica(self.dir.name, 'testbucket')
        self.dir.mkdir()

    def tearDown(self):
        shutil.rmtree(self.dir.name)
        self.replica.destroy()

    def test_basic(self):
        l = AppendLog(self.dir.name)

        l.set(b'a', b'a')
        l.set(b'asdf', b'\x00\x01\x00\x00')
        l.set(b'to be deleted', b'delete me')
        l.set(b'b', b'\x00\x00\x02\x00')
        l.set(b'd', b'3\x002\x00')
        l.set(b'a', b'a1')
        l.set(b'e', b'55')
        l.set(b'to be deleted', b'')

        self.assertEqual(l.get(b'a'), b'a1')
        self.assertEqual(l.get(b'asdf'), b'\x00\x01\x00\x00')
        self.assertEqual(l.get(b'b'), b'\x00\x00\x02\x00')
        self.assertEqual(l.get(b'c'), b'')
        self.assertEqual(l.get(b'd'), b'3\x002\x00')
        self.assertEqual(l.get(b'e'), b'55')
        self.assertEqual(l.get(b'to be deleted'), b'')

        l.close()

    def test_fuzzy(self):
        self.fuzzy_test(AppendLog, args={'data_dir': self.dir.name, 'threshold': 1_000, 'replica': self.replica},
                        key_len_range=(1, 10), val_len_range=(0, 10), n_items=100, n_iter=100_000, seeds=[1],
                        test_recovery=True, test_replica=True)

    def test_fuzzy2(self):
        # large keys/values
        self.fuzzy_test(AppendLog, args={'data_dir': self.dir.name, 'max_key_len': 100_000, 'max_value_len': 100_000,
                                         'threshold': 1_000, 'replica': None},
                        key_len_range=(1, 100_000), val_len_range=(0, 100_000), n_items=10, n_iter=1000, seeds=[1],
                        test_recovery=True, test_replica=False)

    def test_rebuild(self):
        l1 = AppendLog(self.dir.name, threshold=10)

        l1.set(b'a', b'1')
        l1.set(b'b', b'2')
        l1.set(b'c', b'3')

        l1.close()

        l2 = AppendLog(self.dir.name)

        self.assertEqual(l2.get(b'a'), b'1')
        self.assertEqual(l2.get(b'b'), b'2')
        self.assertEqual(l2.get(b'c'), b'3')

        l2.close()


if __name__ == "__main__":
    unittest.main()
