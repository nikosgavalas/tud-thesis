import unittest
from random import Random

from kevo.common import HashIndex


class TestHashIndex(unittest.TestCase):
    def test_basic(self):
        h = HashIndex()
        h[b'asdf'] = 12
        self.assertEqual(h[b'asdf'], 12)
        del h[b'asdf']
        self.assertEqual(h[b'asdf'], None)

    def test_e2e(self):
        rng = Random(1)
        h = HashIndex()
        d = {}
        rand_keys = [rng.randint(1, 100).to_bytes(4) for _ in range(1000)]
        rand_values = [rng.randint(0, 100) for _ in range(1000)]
        for k, v in zip(rand_keys, rand_values):
            h[k] = v
            d[k] = v
            if k in d and v == 0:  # value 0 is equivalent to deletion
                del d[k]
        for k, v in zip(rand_keys, rand_values):
            if k in d:
                self.assertEqual(h[k], d[k])


if __name__ == "__main__":
    unittest.main()
