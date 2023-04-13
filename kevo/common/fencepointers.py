'''
This now is implemented as a sorted dictionary (because I need the bisect_left/right) with base64/json-based ser/der.
A better implementation would be: two arrays (one for keys one for values) so that I can binary-search on the keys, and binary encoding for ser/der.
TODO rebuilding from string could be done linearly if the serialization is sorted, right now the sorteddict is being rebuilt from scratch so that should be fixed
'''

from sys import getsizeof
import json
from base64 import b64encode, b64decode

from sortedcontainers import SortedDict


class FencePointers:
    def __init__(self, density_factor=20, from_str: str | None = None):
        self.pointers = SortedDict()

        self.density_factor = density_factor
        self.counter = 0

        if type(from_str) is str:
            data = json.loads(from_str) 
            for k, v in data['pointers'].items():
                self.pointers[b64decode(k)] = v
            self.density_factor = data['density_factor']
            self.counter = data['counter']

    def add(self, key: bytes, offset: int):
        if self.counter % self.density_factor == 0:
            self.pointers[key] = offset

        self.counter += 1
    
    def bisect(self, key: bytes):
        return self.pointers.bisect(key)
    
    def peekitem(self, idx):
        return self.pointers.peekitem(idx)

    def serialize(self):
        pointers = {}
        for k, v in self.pointers.items():
            pointers[b64encode(k).decode()] = v
        return json.dumps({
            'pointers': pointers,
            'density_factor': self.density_factor,
            'counter': self.counter
        })

    def __len__(self):
        return self.counter

    def __str__(self) -> str:
        return self.serialize()

    def __sizeof__(self):
        return sum((getsizeof(k) + getsizeof(v) for k, v in self.pointers.items()))
