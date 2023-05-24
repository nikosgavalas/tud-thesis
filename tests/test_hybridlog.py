import shutil
import unittest
from pathlib import Path

from fuzzytester import FuzzyTester
from kevo import HybridLog, PathRemote, MinioRemote


class TestHybridLog(unittest.TestCase, FuzzyTester):
    dir = Path('./data_test')

    def setUp(self):
        self.remote = PathRemote('/tmp/remote')
        # self.remote = MinioRemote('testbucket')
        self.dir.mkdir()

    def tearDown(self):
        shutil.rmtree(self.dir.name)
        self.remote.destroy()

    def test_basic(self):
        l = HybridLog(self.dir.name, ro_lag_interval=1, flush_interval=1)

        l.set(b'asdf', b'\x00\x01\x00\x00')
        l.set(b'b', b'\x00\x00\x02\x00')
        l.set(b'd', b'3\x002\x00')
        l.set(b'e', b'55')

        self.assertEqual(l.get(b'asdf'), b'\x00\x01\x00\x00')
        self.assertEqual(l.get(b'b'), b'\x00\x00\x02\x00')
        self.assertEqual(l.get(b'c'), b'')
        self.assertEqual(l.get(b'd'), b'3\x002\x00')
        self.assertEqual(l.get(b'e'), b'55')

        l.close()

    def test_remote(self):
        db = HybridLog(self.dir.name, remote=self.remote)
        db.set(b'a', b'1')
        db.set(b'b', b'2')
        db.snapshot(0)
        db.set(b'a', b'3')
        db.set(b'b', b'4')
        db.snapshot(1)
        db.close()

        shutil.rmtree(self.dir.name)

        db = HybridLog(self.dir.name, remote=self.remote)
        self.assertEqual(db.get(b'a'), b'3')
        self.assertEqual(db.get(b'b'), b'4')
        db.restore(version=0)
        self.assertEqual(db.get(b'a'), b'1')
        self.assertEqual(db.get(b'b'), b'2')
        db.close()

    def test_fuzzy_realistic(self):
        self.fuzzy_test(HybridLog, args={'data_dir': self.dir.name}, key_len_range=(1, 10), val_len_range=(0, 10),
                        n_items=1000, n_iter=1_000_000, seeds=[1], test_recovery=False, test_remote=False)

    def test_fuzzy_merge(self):
        self.fuzzy_test(HybridLog, args={'data_dir': self.dir.name, 'ro_lag_interval': 10,
                                         'flush_interval': 10}, key_len_range=(1, 4), val_len_range=(0, 4),
                        n_items=1_000, n_iter=10_000, seeds=[1], test_recovery=False, test_remote=False)

    def test_fuzzy_index_rebuild(self):
        self.fuzzy_test(HybridLog, args={'data_dir': self.dir.name, 'ro_lag_interval': 10,
                                         'flush_interval': 10}, key_len_range=(1, 4), val_len_range=(0, 4),
                        n_items=1_000, n_iter=10_000, seeds=[1], test_recovery=True, test_remote=False)

    def test_fuzzy_remote(self):
        self.fuzzy_test(HybridLog, args={'data_dir': self.dir.name, 'ro_lag_interval': 10,
                                         'flush_interval': 10, 'remote': self.remote}, key_len_range=(1, 4),
                        val_len_range=(0, 4), n_items=1_000, n_iter=10_000, seeds=[1], test_recovery=True,
                        test_remote=True)

    def test_fuzzy_large_kvs(self):
        self.fuzzy_test(HybridLog, args={'data_dir': self.dir.name, 'max_key_len': 100_000, 'max_value_len': 100_000,
                                         'ro_lag_interval': 100, 'flush_interval': 100,
                                         'remote': None}, key_len_range=(1, 100_000), val_len_range=(0, 100_000),
                        n_items=10, n_iter=100, seeds=[1], test_recovery=False, test_remote=False)

    def test_fuzzy_altogether(self):
        self.fuzzy_test(HybridLog,
                        args={'data_dir': self.dir.name, 'ro_lag_interval': 150_000,
                              'flush_interval': 150_000, 'remote': self.remote},
                        key_len_range=(1, 10), val_len_range=(0, 10), n_items=10_000, n_iter=1_000_000, seeds=[1],
                        test_recovery=True, test_remote=True)

    def test_fuzzy_snapshot(self):
        self.fuzzy_test_snapshot(HybridLog,
                                 args={'data_dir': self.dir.name, 'ro_lag_interval': 400,
                                       'flush_interval': 400, 'remote': self.remote},
                                 key_len_range=(1, 10), val_len_range=(0, 13), n_items=10_000, n_iter=10_000, seed=1)

    def test_fuzzy_snapshot_continuous(self):
        self.fuzzy_test_snapshot_continuous(HybridLog, args={'data_dir': self.dir.name,
                                                             'ro_lag_interval': 400, 'flush_interval': 400,
                                                             'remote': self.remote}, key_len_range=(1, 10),
                                            val_len_range=(0, 13), n_items=10_000, n_iter=10_000, seed=1)


if __name__ == "__main__":
    unittest.main()
