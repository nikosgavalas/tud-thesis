import sys
import unittest
import shutil
from random import Random
from pathlib import Path
# import time

# make it runnable from the root level
sys.path.append('.')

from src.appendlog import AppendLog


class TestAppendLog(unittest.IsolatedAsyncioTestCase):
    dir = Path('./data_test')

    def setUp(self):
        self.dir.mkdir()
        # self.start_time = time.time()

    def tearDown(self):
        # print(f'{self.id()}: {time.time() - self.start_time:.3f}s')
        shutil.rmtree(self.dir.name)
    
    async def test_e2e_1(self):
        l = await AppendLog(self.dir.name)

        await l.set(b'a', b'a')
        await l.set(b'asdf', b'\x00\x01\x00\x00')
        await l.set(b'to be deleted', b'delete me')
        await l.set(b'b', b'\x00\x00\x02\x00')
        await l.set(b'd', b'3\x002\x00')
        await l.set(b'a', b'a1')
        await l.set(b'e', b'55')
        await l.set(b'to be deleted', b'')

        self.assertEqual(await l.get(b'a'), b'a1')
        self.assertEqual(await l.get(b'asdf'), b'\x00\x01\x00\x00')
        self.assertEqual(await l.get(b'b'),  b'\x00\x00\x02\x00')
        self.assertEqual(await l.get(b'c'), b'')
        self.assertEqual(await l.get(b'd'), b'3\x002\x00')
        self.assertEqual(await l.get(b'e'), b'55')
        self.assertEqual(await l.get(b'to be deleted'), b'')

        await l.close()

    async def test_e2e_2(self):
        rng = Random(1)
        l = await AppendLog(self.dir.name)
        n_items = 10
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

            await l.set(rand_key, rand_value)

        # also test index rebuilding here
        await l.close()
        l = await AppendLog(self.dir.name)

        for k, v in dict.items():
            self.assertEqual(v, await l.get(k))

    async def test_rebuild(self):
        l1 = await AppendLog(self.dir.name, threshold=10)

        await l1.set(b'a', b'1')
        await l1.set(b'b', b'2')
        await l1.set(b'c', b'3')

        await l1.close()

        l2 = await AppendLog(self.dir.name)

        self.assertEqual(await l2.get(b'a'), b'1')
        self.assertEqual(await l2.get(b'b'), b'2')
        self.assertEqual(await l2.get(b'c'), b'3')


if __name__ == "__main__":
    unittest.main()
