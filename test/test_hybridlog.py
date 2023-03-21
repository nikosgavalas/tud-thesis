import sys
import unittest
import shutil
from random import Random
from pathlib import Path
# import time

# make it runnable from the root level
sys.path.append('.')

from src.hybridlog import HybridLog


class TestHybridLog(unittest.IsolatedAsyncioTestCase):
    dir = Path('./data_test')

    def setUp(self):
        self.dir.mkdir()
        # self.start_time = time.time()

    def tearDown(self):
        # print(f'{self.id()}: {time.time() - self.start_time:.3f}s')
        shutil.rmtree(self.dir.name)
    
    async def test_e2e_1(self):
        l = await HybridLog(self.dir.name, mem_segment_len=3, ro_lag_interval=1, flush_interval=1)

        await l.set(b'asdf', b'\x00\x01\x00\x00')
        await l.set(b'b', b'\x00\x00\x02\x00')
        await l.set(b'd', b'3\x002\x00')
        await l.set(b'e', b'55')

        self.assertEqual(await l.get(b'asdf'), b'\x00\x01\x00\x00')
        self.assertEqual(await l.get(b'b'),  b'\x00\x00\x02\x00')
        self.assertEqual(await l.get(b'c'), b'')
        self.assertEqual(await l.get(b'd'), b'3\x002\x00')
        self.assertEqual(await l.get(b'e'), b'55')

        await l.close()

    async def test_e2e_2(self):
        rng = Random(1)
        l = await HybridLog(self.dir.name, mem_segment_len=2000, ro_lag_interval=1000, flush_interval=1000, compaction_interval=0)
        n_items = 100
        n_iter = 1_000

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

            await l.set(rand_key, rand_value)
        
        for k, v in dict.items():
            self.assertEqual(v, await l.get(k))

    async def test_offset_translation(self):
        rng = Random(1)
        l = await HybridLog(self.dir.name)
        self.assertEqual(l.LA_to_file_offset(1), 0)
        self.assertEqual(l.file_offset_to_LA(0), 1)
        for _ in range(100):
            i = rng.randint(0, 100)
            self.assertEqual(l.file_offset_to_LA(l.LA_to_file_offset(i)), i)

    async def test_index_rebuild(self):
        rng = Random(1)
        l = await HybridLog(self.dir.name, mem_segment_len=30, ro_lag_interval=10, flush_interval=10)
        n_items = 10
        n_iter = 100

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

            await l.set(rand_key, rand_value)

        await l.close()
        l = await HybridLog(self.dir.name)

        for k, v in dict.items():
            self.assertEqual(v, await l.get(k))

    async def test_compaction(self):
        rng = Random(1)
        l = await HybridLog(self.dir.name, mem_segment_len=300, ro_lag_interval=100, flush_interval=100, compaction_interval=8)
        n_items = 100
        n_iter = 10_000

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

            await l.set(rand_key, rand_value)

        for k, v in dict.items():
            self.assertEqual(v, await l.get(k))


if __name__ == "__main__":
    unittest.main()
