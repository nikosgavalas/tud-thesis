import unittest
from random import Random

from kevo.common import BloomFilter


class TestBloomFilter(unittest.TestCase):
    def test_false_positive_probability(self):
        for accuracy in [3, 4, 5]:
            skip = 50
            n = 1000000
            fp_prob = 10 ** (-accuracy)
            b = BloomFilter(int(n / skip), fp_prob)

            for i in range(n):
                s = (str(i)).encode()
                if i % skip == 0:
                    b.add(s)

            fp_count = 0
            for i in range(n):
                s = (str(i)).encode()
                self.assertFalse(i % skip == 0 and s not in b)
                if i % skip != 0 and s in b:
                    fp_count += 1

            self.assertAlmostEqual(fp_count / n, fp_prob, 4)
        
    def test_ser_der(self):
        rng = Random(1)
        b1 = BloomFilter()
        for i in range(20):
            b1.add(rng.randbytes(10))
        b2 = BloomFilter(from_str=b1.serialize())
        self.assertEqual(b1.bitarray, b2.bitarray)


if __name__ == "__main__":
    unittest.main()
