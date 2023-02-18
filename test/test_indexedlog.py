import sys
import unittest
import shutil
import struct
from pathlib import Path

# make it runnable from the root level
sys.path.append('.')

from src.indexedlog import IndexedLog


class TestIndexedLog(unittest.TestCase):
    dir = Path('./data_test')

    def setUp(self):
        self.dir.mkdir()

    def tearDown(self):
        shutil.rmtree(self.dir.name)
    
    def test_e2e_1(self):
        l = IndexedLog(self.dir.name)

        l.set(b'asdf', struct.pack('i', 1))
        l.set(b'b', struct.pack('i', 2))

        self.assertEqual(l.get(b'asdf'), b'\x01')
        self.assertEqual(l.get(b'b'),  b'\x02')
        self.assertEqual(l.get(b'c'), b'')

        l.close()


if __name__ == "__main__":
    unittest.main()
