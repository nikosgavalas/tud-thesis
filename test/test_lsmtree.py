import sys
import unittest
import shutil
from random import Random
from pathlib import Path

# make it runnable from the root level
sys.path.append('.')

from src.lsmtree import LSMTree
from src.replication import PathReplica, MinioReplica


class TestLSMTree(unittest.TestCase):
    dir = Path('./data_test')

    def setUp(self):
        self.replica = PathReplica(self.dir.name, '/tmp/remote')
        # self.replica = MinioReplica(self.dir.name, 'testbucket')
        self.dir.mkdir()

    def tearDown(self):
        shutil.rmtree(self.dir.name)
        self.replica.destroy()

    def test_e2e_1(self):
        l = LSMTree(self.dir.name, max_runs_per_level=3, density_factor=3, memtable_bytes_limit=10)

        l.set(b'b', b'2')
        l.set(b'asdf', b'12345')
        l.set(b'cc', b'cici345')
        l.set(b'b', b'3')

        self.assertEqual(l.get(b'b'), b'3')
        self.assertEqual(l.get(b'asdf'), b'12345')
        self.assertEqual(l.get(b'cc'), b'cici345')

        l.close()
    
    def test_e2e_2(self):
        # highly granular, fishing for edgecases
        for a in range(1):  # change this range to as much as you want to wait
            rng = Random(a)
            l = LSMTree(self.dir.name, max_runs_per_level=2, density_factor=3, memtable_bytes_limit=10)
            n_items = 10
            n_iter = 10_000

            dict = {}  # testing against the python dict
            keys = [rng.randbytes(rng.randint(1, 10)) for _ in range(n_items)]
            values = [rng.randbytes(rng.randint(0, 13)) for _ in range(n_items)]

            for _ in range(n_iter):
                rand_idx = rng.randint(0, n_items - 1)
                rand_key = keys[rand_idx]
                rand_value = values[rand_idx]

                if not rand_value:
                    if rand_key in dict:
                        del dict[rand_key]  # emulating the kvstore's behaviour ("value 0" == delete)
                else:
                    dict[rand_key] = rand_value

                l.set(rand_key, rand_value)
            
            for k, v in dict.items():
                self.assertEqual(v, l.get(k))

            l.close()

    def test_e2e_3(self):
        # more realistic, with replica
        rng = Random(1)
        l = LSMTree(self.dir.name, replica=self.replica)
        n_items = 100
        n_iter = 1_000_000

        dict = {}
        keys = [rng.randbytes(rng.randint(1, 10)) for _ in range(n_items)]
        values = [rng.randbytes(rng.randint(0, 13)) for _ in range(n_items)]

        for _ in range(n_iter):
            rand_idx = rng.randint(0, n_items - 1)
            rand_key = keys[rand_idx]
            rand_value = values[rand_idx]

            if not rand_value: # TODO fix the rest ones as this one!
                if rand_key in dict:
                    del dict[rand_key]
            else:
                dict[rand_key] = rand_value

            l.set(rand_key, rand_value)

        # also test with a full reset here
        l.close()
        shutil.rmtree(self.dir.name)
        l = LSMTree(self.dir.name, replica=self.replica)

        for k, v in dict.items():
            self.assertEqual(v, l.get(k))

        l.close()

    def test_merge_1(self):
        l = LSMTree(self.dir.name, max_runs_per_level=2, density_factor=3, memtable_bytes_limit=10)

        l.set(b'a1', b'a1')
        l.set(b'a1', b'a11')
        l.set(b'a2', b'a2')

        l.set(b'a2', b'a22')
        l.set(b'a3', b'a3')
        l.set(b'a4', b'a4')

        l.set(b'a3', b'a31')
        l.set(b'a5', b'a5')
        l.set(b'a6', b'a6')

        with (self.dir / 'L1.0.run').open('br') as f:
            content = f.read()

        self.assertEqual(content, b'\x02a1\x03a11\x02a2\x03a22\x02a3\x03a31\x02a4\x02a4\x02a5\x02a5\x02a6\x02a6')

        l.close()
    
    def test_wal(self):
        l1 = LSMTree(self.dir.name)

        l1.set(b'a', b'1')
        l1.set(b'b', b'2')
        l1.set(b'c', b'3')

        l1.close()

        l2 = LSMTree(self.dir.name)

        self.assertEqual(l2.get(b'a'), b'1')
        self.assertEqual(l2.get(b'b'), b'2')
        self.assertEqual(l2.get(b'c'), b'3')

        l2.close()

    def test_remote(self):
        l = LSMTree(self.dir.name, replica=self.replica)
        l.set(b'a', b'1')
        l.set(b'b', b'2')
        l.close()
        shutil.rmtree(self.dir.name)

        l = LSMTree(self.dir.name, replica=self.replica)
        self.assertEqual(l.get(b'a'), b'1')
        self.assertEqual(l.get(b'b'), b'2')
        l.close()


if __name__ == "__main__":
    unittest.main()
