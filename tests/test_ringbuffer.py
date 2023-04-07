import unittest

from kevo.common import RingBuffer


class TestRingBuffer(unittest.TestCase):
    def test_basic(self):
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

        with self.assertRaises(ValueError) as ctx:
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

    def test_indexing(self):
        r = RingBuffer(3)
        o1 = r.add(0)
        o2 = r.add(1)
        self.assertEqual(r[o1], 0)
        self.assertEqual(r[o2], 1)
        r.pop()
        r.pop()
        with self.assertRaises(IndexError) as ctx:
            _ = r[o1]
            _ = r[o2]

        o1 = r.add(3)
        r.add(4)
        o2 = r.add(5)
        while o1 < o2:
            r.pop()
            o1 += 1
        self.assertEqual(r.pop(), 5)
        self.assertTrue(r.is_empty())

        r.add(5)
        o = r.add(6)
        r[o] = 0
        self.assertEqual(r[o], 0)
        with self.assertRaises(IndexError) as ctx:
            _ = r[o + 1]
        self.assertEqual(r[o - 1], 5)
        with self.assertRaises(IndexError) as ctx:
            _ = r[o - 2]
        r.add(7)
        self.assertTrue(r.is_full())
    
        r = RingBuffer(1)
        o = r.add(0)
        self.assertEqual(r[o], r.pop())


if __name__ == "__main__":
    unittest.main()
