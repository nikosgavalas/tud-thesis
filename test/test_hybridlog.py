import sys
import unittest
import shutil
from random import Random
from pathlib import Path

# make it runnable from the root level
sys.path.append('.')

from src.hybridlog import HybridLog
from fuzzy import FuzzyTester


class TestHybridLog(unittest.TestCase, FuzzyTester):
    dir = Path('./data_test')

    def setUp(self):
        self.dir.mkdir()

    def tearDown(self):
        shutil.rmtree(self.dir.name)
    
    def test_basic(self):
        l = HybridLog(self.dir.name, mem_segment_len=3, ro_lag_interval=1, flush_interval=1)

        l.set(b'asdf', b'\x00\x01\x00\x00')
        l.set(b'b', b'\x00\x00\x02\x00')
        l.set(b'd', b'3\x002\x00')
        l.set(b'e', b'55')

        self.assertEqual(l.get(b'asdf'), b'\x00\x01\x00\x00')
        self.assertEqual(l.get(b'b'),  b'\x00\x00\x02\x00')
        self.assertEqual(l.get(b'c'), b'')
        self.assertEqual(l.get(b'd'), b'3\x002\x00')
        self.assertEqual(l.get(b'e'), b'55')

        l.close()

    def test_fuzzy_1(self):
        self.fuzzy_test(HybridLog, args={'data_dir': self.dir.name, 'mem_segment_len': 2000, 'ro_lag_interval': 1000, 'flush_interval': 1000,
            'compaction_interval': 0}, key_len_range=(1, 4), val_len_range=(0, 4), n_items=100, n_iter=1_000_000, seeds=[1],
            test_recovery=True, test_replica=False)

    def test_fuzzy_2(self):
        # index rebuild focused
        self.fuzzy_test(HybridLog, args={'data_dir': self.dir.name, 'mem_segment_len': 30, 'ro_lag_interval': 10, 'flush_interval': 10,
            'compaction_interval': 0}, key_len_range=(1, 4), val_len_range=(0, 4), n_items=10, n_iter=100, seeds=[1],
            test_recovery=True, test_replica=False)

    def test_fuzzy_3(self):
        # compaction focused
        self.fuzzy_test(HybridLog, args={'data_dir': self.dir.name, 'mem_segment_len': 300, 'ro_lag_interval': 100, 'flush_interval': 100,
            'compaction_interval': 8}, key_len_range=(1, 4), val_len_range=(0, 4), n_items=1_000, n_iter=10_000, seeds=[1],
            test_recovery=False, test_replica=False)

    def test_offset_translation(self):
        rng = Random(1)
        l = HybridLog(self.dir.name)

        self.assertEqual(l.LA_to_file_offset(1), 0)
        self.assertEqual(l.file_offset_to_LA(0), 1)

        for _ in range(100):
            i = rng.randint(0, 100)
            self.assertEqual(l.file_offset_to_LA(l.LA_to_file_offset(i)), i)
        
        l.close()


if __name__ == "__main__":
    unittest.main()
