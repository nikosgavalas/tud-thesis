import sys
import unittest
import shutil
from pathlib import Path

# make it runnable from the root level
sys.path.append('.')

from src.lsmtree import LSMTree


class TestLSMTree(unittest.TestCase):
    dir = Path('./data_test')

    def setUp(self):
        self.dir.mkdir()

    def tearDown(self):
        shutil.rmtree(self.dir.name)
    
    def test_e2e_1(self):
        l = LSMTree(self.dir.name, max_runs_per_level=3, fence_pointer_skips=3, memtable_bytes_limit=10)

        l.set(b'b', b'2')
        l.set(b'asdf', b'12345')
        l.set(b'cc', b'cici345')
        l.set(b'b', b'3')

        self.assertEqual(l.get(b'b'), b'3')
        self.assertEqual(l.get(b'asdf'), b'12345')
        self.assertEqual(l.get(b'cc'), b'cici345')

    def test_merge(self):
        l = LSMTree(self.dir.name, max_runs_per_level=3, fence_pointer_skips=3, memtable_bytes_limit=10)

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


if __name__ == "__main__":
    unittest.main()
