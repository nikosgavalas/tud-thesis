import shutil
import unittest
from pathlib import Path

from fuzzytester import FuzzyTester
from kevo import AppendLog, PathRemote, MinioRemote


class TestAppendLog(unittest.TestCase, FuzzyTester):
    dir = Path('./data_test')

    def setUp(self):
        self.remote = PathRemote('/tmp/remote')
        # self.remote = MinioRemote('testbucket')
        self.dir.mkdir()

    def tearDown(self):
        shutil.rmtree(self.dir.name)
        self.remote.destroy()

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

    def test_remote(self):
        db = AppendLog(self.dir.name, remote=self.remote)
        db.set(b'a', b'1')
        db.set(b'b', b'2')
        db.snapshot(0)
        db.set(b'a', b'3')
        db.set(b'b', b'4')
        db.snapshot(1)
        db.close()

        shutil.rmtree(self.dir.name)

        db = AppendLog(self.dir.name, remote=self.remote)
        self.assertEqual(db.get(b'a'), b'3')
        self.assertEqual(db.get(b'b'), b'4')
        db.restore(version=0)
        self.assertEqual(db.get(b'a'), b'1')
        self.assertEqual(db.get(b'b'), b'2')
        db.close()

    def test_fuzzy_generic(self):
        self.fuzzy_test(AppendLog, args={'data_dir': self.dir.name, 'compaction': True, 'remote': None},
                        key_len_range=(1, 10), val_len_range=(0, 10), n_items=1000, n_iter=1_000_000, seeds=[1],
                        test_recovery=False, test_remote=False)

    def test_fuzzy_granular(self):
        self.fuzzy_test(AppendLog, args={'data_dir': self.dir.name, 'threshold': 100, 'remote': None},
                        key_len_range=(1, 10), val_len_range=(0, 10), n_items=100, n_iter=10_000, seeds=[1],
                        test_recovery=True, test_remote=False)

    def test_fuzzy_compaction(self):
        self.fuzzy_test(AppendLog,
                        args={'data_dir': self.dir.name, 'threshold': 100, 'compaction': True, 'remote': None},
                        key_len_range=(1, 10), val_len_range=(0, 10), n_items=100, n_iter=10_000, seeds=[1],
                        test_recovery=True, test_remote=False)

    def test_fuzzy_recovery(self):
        self.fuzzy_test(AppendLog, args={'data_dir': self.dir.name, 'threshold': 1_000, 'remote': self.remote},
                        key_len_range=(1, 10), val_len_range=(0, 10), n_items=100, n_iter=10_000, seeds=[1],
                        test_recovery=True, test_remote=True)

    def test_fuzzy_large_kvs(self):
        self.fuzzy_test(AppendLog, args={'data_dir': self.dir.name, 'max_key_len': 100_000, 'max_value_len': 100_000,
                                         'threshold': 1_000, 'remote': None},
                        key_len_range=(1, 100_000), val_len_range=(0, 100_000), n_items=10, n_iter=1000, seeds=[1],
                        test_recovery=True, test_remote=False)

    def test_fuzzy_snapshot(self):
        self.fuzzy_test_snapshot(AppendLog, args={'data_dir': self.dir.name, 'threshold': 100, 'remote': self.remote},
                                 key_len_range=(1, 10), val_len_range=(0, 13), n_items=10_000, n_iter=10_000, seed=1)

    def test_fuzzy_snapshot_continuous(self):
        self.fuzzy_test_snapshot_continuous(AppendLog, args={'data_dir': self.dir.name, 'threshold': 100,
                                                             'remote': self.remote}, key_len_range=(1, 10),
                                            val_len_range=(0, 13), n_items=10_000, n_iter=10_000, seed=1)

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
