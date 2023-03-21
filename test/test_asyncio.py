import sys
import unittest
import shutil
from random import Random
from pathlib import Path
# import time

# make it runnable from the root level
sys.path.append('.')

from src.hybridlog import HybridLog
from src.appendlog import AppendLog
from src.lsmtree import LSMTree


class TestHybridLog(unittest.IsolatedAsyncioTestCase):
    dir = Path('./data_test')

    def setUp(self):
        self.dir.mkdir()
        # self.start_time = time.time()

    def tearDown(self):
        # print(f'{self.id()}: {time.time() - self.start_time:.3f}s')
        shutil.rmtree(self.dir.name)
    
    async def test_hybridlog_generic(self):
        l = await HybridLog(self.dir.name, mem_segment_len=3, ro_lag_interval=1, flush_interval=1, compaction_interval=1)

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

    async def test_appendlog_generic(self):
        l = await AppendLog(self.dir.name, threshold=2, max_runs_per_level=1)
        await l.set(b'a', b'1')
        await l.set(b'b', b'2')
        await l.set(b'c', b'3')
        await l.set(b'd', b'4')
        self.assertEqual(await l.get(b'a'), b'1')
        self.assertEqual(await l.get(b'b'), b'2')
        self.assertEqual(await l.get(b'c'), b'3')
        self.assertEqual(await l.get(b'd'), b'4')
        await l.close()
    
    async def test_lsmtree_generic(self):
        l = await LSMTree(self.dir.name, max_runs_per_level=1, memtable_bytes_limit=2)
        await l.set(b'a', b'1')
        await l.set(b'b', b'2')
        await l.set(b'c', b'3')
        await l.set(b'd', b'4')
        self.assertEqual(await l.get(b'a'), b'1')
        self.assertEqual(await l.get(b'b'), b'2')
        self.assertEqual(await l[b'c'], b'3')
        self.assertEqual(await l.get(b'd'), b'4')
        await l.close()


if __name__ == "__main__":
    unittest.main()
