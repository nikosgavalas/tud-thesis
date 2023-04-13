# I wrote this because pybloomfiltermmap3 does not work with python 3.11 yet
# https://en.wikipedia.org/wiki/Bloom_filter#Probability_of_false_positives.

from sys import getsizeof
import json
from base64 import b64encode, b64decode
from math import log, ceil, floor

from mmh3 import hash
from bitarray import bitarray


class BloomFilter:
    def __init__(self, est_num_items=1000, false_positive_prob=0.01, from_str: str | None = None):
        if type(from_str) is str:
            data = json.loads(from_str)

            arr = bitarray(endian=data['endian'])
            arr.frombytes(b64decode(data['bytes']))
            self.bitarray_size = data['bitarray_size']
            self.bitarray = arr[:self.bitarray_size]

            self.num_hash_funcs = data['num_hash_funcs']

            self.est_num_items = data['est_num_items']
            self.false_positive_prob = data['false_positive_prob']

        else:
            self.est_num_items = est_num_items
            self.false_positive_prob = false_positive_prob

            # https://stackoverflow.com/questions/658439/how-many-hash-functions-does-my-bloom-filter-need
            self.bitarray_size = ceil(-(est_num_items * log(false_positive_prob)) / (log(2) ** 2))
            self.num_hash_funcs = floor((self.bitarray_size / est_num_items) * log(2))  # floor to keep the hash funcs as few as possible for performance

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

    def serialize(self):
        return json.dumps({
            'bytes': b64encode(self.bitarray.tobytes()).decode(),
            'bitarray_size': len(self.bitarray),
            'endian': self.bitarray.endian(),
            'num_hash_funcs': self.num_hash_funcs,
            # the last two are not necessary but I put them for completeness
            'est_num_items': self.est_num_items,
            'false_positive_prob': self.false_positive_prob
        })

    def __str__(self) -> str:
        return self.serialize()

    def __sizeof__(self):
        return getsizeof(self.bitarray)
