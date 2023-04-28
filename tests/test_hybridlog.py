import shutil
import unittest
from pathlib import Path

from fuzzytester import FuzzyTester
from kevo import HybridLog, PathReplica, MinioReplica


class TestHybridLog(unittest.TestCase, FuzzyTester):
    dir = Path('./data_test')

    def setUp(self):
        self.replica = PathReplica(self.dir.name, '/tmp/remote')
        # self.replica = MinioReplica(self.dir.name, 'testbucket')
        self.dir.mkdir()

    def tearDown(self):
        shutil.rmtree(self.dir.name)
        self.replica.destroy()

    def test_basic(self):
        l = HybridLog(self.dir.name, mem_segment_len=3, ro_lag_interval=1, flush_interval=1)

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

    def test_fuzzy_realistic(self):
        self.fuzzy_test(HybridLog, args={'data_dir': self.dir.name}, key_len_range=(1, 10), val_len_range=(0, 10),
                        n_items=1000, n_iter=1_000_000, seeds=[1], test_recovery=False, test_replica=False)

    def test_fuzzy_merge(self):
        self.fuzzy_test(HybridLog, args={'data_dir': self.dir.name, 'mem_segment_len': 30, 'ro_lag_interval': 10,
                                         'flush_interval': 10}, key_len_range=(1, 4), val_len_range=(0, 4),
                        n_items=1_000, n_iter=10_000, seeds=[1], test_recovery=False, test_replica=False)

    def test_fuzzy_index_rebuild(self):
        self.fuzzy_test(HybridLog, args={'data_dir': self.dir.name, 'mem_segment_len': 30, 'ro_lag_interval': 10,
                                         'flush_interval': 10}, key_len_range=(1, 4), val_len_range=(0, 4),
                        n_items=1_000, n_iter=10_000, seeds=[1], test_recovery=True, test_replica=False)

    def test_fuzzy_replica(self):
        self.fuzzy_test(HybridLog, args={'data_dir': self.dir.name, 'mem_segment_len': 30, 'ro_lag_interval': 10,
                                         'flush_interval': 10, 'replica': self.replica}, key_len_range=(1, 4),
                        val_len_range=(0, 4), n_items=1_000, n_iter=10_000, seeds=[1], test_recovery=True,
                        test_replica=True)

    def test_fuzzy_large_kvs(self):
        self.fuzzy_test(HybridLog, args={'data_dir': self.dir.name, 'max_key_len': 100_000, 'max_value_len': 100_000,
                                         'mem_segment_len': 300, 'ro_lag_interval': 100, 'flush_interval': 100,
                                         'replica': None}, key_len_range=(1, 100_000), val_len_range=(0, 100_000),
                        n_items=10, n_iter=100, seeds=[1], test_recovery=False, test_replica=False)

    def test_fuzzy_compaction(self):
        self.fuzzy_test(HybridLog, args={'data_dir': self.dir.name, 'mem_segment_len': 30, 'ro_lag_interval': 10,
                                         'flush_interval': 10, 'compaction_enabled': True, 'replica': None},
                        key_len_range=(1, 4), val_len_range=(0, 4), n_items=1_000, n_iter=10_000, seeds=[1],
                        test_recovery=False, test_replica=False)

    def test_fuzzy_altogether(self):
        self.fuzzy_test(HybridLog,
                        args={'data_dir': self.dir.name, 'mem_segment_len': 300_000, 'ro_lag_interval': 100_000,
                              'flush_interval': 100_000, 'compaction_enabled': True, 'replica': self.replica},
                        key_len_range=(1, 10), val_len_range=(0, 10), n_items=10_000, n_iter=1_000_000, seeds=[1],
                        test_recovery=True, test_replica=True)


if __name__ == "__main__":
    unittest.main()
