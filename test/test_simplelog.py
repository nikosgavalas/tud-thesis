import sys
import unittest
import shutil
from random import Random
from pathlib import Path
# import time

# make it runnable from the root level
sys.path.append('.')

from src.simplelog import SimpleLog


class TestSimpleLog(unittest.TestCase):
    dir = Path('./data_test')

    def setUp(self):
        self.dir.mkdir()
        # self.start_time = time.time()

    def tearDown(self):
        # print(f'{self.id()}: {time.time() - self.start_time:.3f}s')
        shutil.rmtree(self.dir.name)
    
    def test_e2e_1(self):
        l = SimpleLog(self.dir.name)

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
        self.assertEqual(l.get(b'b'),  b'\x00\x00\x02\x00')
        self.assertEqual(l.get(b'c'), b'')
        self.assertEqual(l.get(b'd'), b'3\x002\x00')
        self.assertEqual(l.get(b'e'), b'55')
        self.assertEqual(l.get(b'to be deleted'), b'')

        l.close()

    def test_e2e_2(self):
        rng = Random(1)
        l = SimpleLog(self.dir.name)
        n_items = 100
        n_iter = 1_000

        dict = {}
        keys = [rng.randbytes(rng.randint(1, 10)) for _ in range(n_items)]
        values = [rng.randbytes(rng.randint(0, 10)) for _ in range(n_items)]

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

        # also test index rebuilding here
        l.close()
        l = SimpleLog(self.dir.name)

        for k, v in dict.items():
            self.assertEqual(v, l.get(k))

    def test_rebuild(self):
        l1 = SimpleLog(self.dir.name, threshold=10)

        l1.set(b'a', b'1')
        l1.set(b'b', b'2')
        l1.set(b'c', b'3')

        l1.close()

        l2 = SimpleLog(self.dir.name)

        self.assertEqual(l2.get(b'a'), b'1')
        self.assertEqual(l2.get(b'b'), b'2')
        self.assertEqual(l2.get(b'c'), b'3')


if __name__ == "__main__":
    unittest.main()
