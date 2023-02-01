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
        l = LSMTree(self.dir.name)

        # l.set(b'b', b'2')
        # l.set(b'asdf', b'12345')
        # l.set(b'cc', b'cici345')
        # l.set(b'b', b'3')

        # l.flush_memtable()

        # print(l.get(b'b'))
        # print(l.get(b'asdf'))
        # print(l.get(b'cc'))

        # l.merge()

    # def test_e2e_2(self):
    #     l = LSMTree()

    #     l.set(b'a1', b'a1')
    #     l.set(b'a1', b'a11')
    #     l.set(b'a2', b'a2')

    #     l.set(b'a2', b'a22')
    #     l.set(b'a3', b'a3')
    #     l.set(b'a4', b'a4')

    #     l.set(b'a3', b'a31')
    #     l.set(b'a5', b'a5')
    #     l.set(b'a6', b'a6')

    #     l.merge()
    #     self.assertTrue(True)


if __name__ == "__main__":
    unittest.main()
