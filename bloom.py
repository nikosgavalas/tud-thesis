# I wrote this because pybloomfiltermmap3 does not work with python 3.11 yet
# https://en.wikipedia.org/wiki/Bloom_filter#Probability_of_false_positives.

from math import log, ceil, floor

from mmh3 import hash
from bitarray import bitarray


class BloomFilter:

    def __init__(self, num_items, false_positive_prob=0.01):
        # https://stackoverflow.com/questions/658439/how-many-hash-functions-does-my-bloom-filter-need
        self.bitarray_size = ceil(-(num_items * log(false_positive_prob)) / (log(2) ** 2))
        self.num_hash_funcs = floor((self.bitarray_size / num_items) * log(2))  # floor to keep the hash funcs as few as possible for performance

        self.bitarray = bitarray(self.bitarray_size)
        self.bitarray.setall(False)

    def add(self, item):
        for i in range(self.num_hash_funcs):
            self.bitarray[hash(item, i) % self.bitarray_size] = True

    def __contains__(self, item):
        for i in range(self.num_hash_funcs):
            if not self.bitarray[hash(item, i) % self.bitarray_size]:
                return False
        return True
