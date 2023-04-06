import unittest

from random import Random
from kevo.common import FencePointers


class TestFencePointers(unittest.TestCase):
    def test_ser_der(self):
        rng = Random(1)
        fp1 = FencePointers()
        for i in range(100):
            randbytes = rng.randbytes(rng.randint(50, 100))
            randint = rng.randint(0, 1000)
            fp1.add(randbytes, randint)
        fp2 = FencePointers(from_str=fp1.serialize())
        self.assertEqual(fp1.pointers, fp2.pointers)


if __name__ == "__main__":
    unittest.main()
