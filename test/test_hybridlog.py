import sys
import unittest
import shutil
from random import Random
from pathlib import Path
# import time

# make it runnable from the root level
sys.path.append('.')

from src.hybridlog import HybridLog


class TestHybridLog(unittest.TestCase):
    dir = Path('./data_test')

    def setUp(self):
        self.dir.mkdir()
        # self.start_time = time.time()

    def tearDown(self):
        # print(f'{self.id()}: {time.time() - self.start_time:.3f}s')
        shutil.rmtree(self.dir.name)
    
    def test_e2e_1(self):
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

    def test_e2e_2(self):
        # high-granularity test
        rng = Random(1)
        l = HybridLog(self.dir.name, mem_segment_len=10, ro_lag_interval=2, flush_interval=4)
        n_items = 100
        n_iter = 1_000_000

        dict = {}
        keys = [rng.randbytes(rng.randint(1, 4)) for _ in range(n_items)]
        values = [rng.randbytes(rng.randint(0, 4)) for _ in range(n_items)]

        for _ in range(n_iter):
            rand_idx = rng.randint(0, n_items - 1)
            rand_key = keys[rand_idx]
            rand_value = values[rand_idx]

            if not rand_value:
                if rand_key in dict:
                    del dict[rand_key]
            else:
                dict[rand_key] = rand_value

            l.set(rand_key, rand_value)

        for k, v in dict.items():
            self.assertEqual(v, l.get(k))


if __name__ == "__main__":
    unittest.main()
