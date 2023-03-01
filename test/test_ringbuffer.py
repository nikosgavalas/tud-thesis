import sys
import unittest

sys.path.append('.')

from src.ringbuffer import RingBuffer


class TestRingBuffer(unittest.TestCase):
    def test_e2e(self):
        r = RingBuffer(5)
        self.assertTrue(r.is_empty())
        self.assertEqual(len(r), 0)
        r.add(1)
        r.add(2)
        r.add(3)
        self.assertEqual(len(r), 3)
        self.assertEqual(r.pop(), 1)
        r.add(4)
        self.assertEqual(r.pop(), 2)
        r.add(5)
        r.add(1)
        r.add(2)
        with self.assertRaises(BufferError) as ctx:
            r.add(3)
        r.pop()
        r.pop()
        r.pop()
        r.pop()
        r.pop()
        with self.assertRaises(BufferError) as ctx:
            r.pop()

        with self.assertRaises(AssertionError) as ctx:
            r = RingBuffer(0)

        r = RingBuffer(1)
        r.add(1)
        self.assertEqual(len(r), 1)
        self.assertTrue(r.is_full())
        self.assertFalse(r.is_empty())
        self.assertEqual(r.pop(), 1)
        self.assertEqual(len(r), 0)
        self.assertTrue(r.is_empty())
        self.assertFalse(r.is_full())

if __name__ == "__main__":
    unittest.main()
