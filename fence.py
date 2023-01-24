'''
This now is implemented as a sorted dictionary (because I need the bisect_left/right) with base64/json-based ser/der.
A better implementation would be: two arrays (one for keys one for values) so that I can binary-search on the keys, and binary encoding.
'''

import json
from base64 import b64encode, b64decode

from sortedcontainers import SortedDict


class FencePointers:
    
    def __init__(self):
        self.pointers = SortedDict()

    def add(self, key: bytes, offset: int):
        self.pointers[key] = offset
    
    def bisect(self, key: bytes):
        return self.pointers.bisect(key)
    
    def peekitem(self, idx):
        return self.pointers.peekitem(idx)

    def serialize(self):
        data = {}
        for k, v in self.pointers.items():
            data[b64encode(k).decode()] = v
        return json.dumps(data)

    def deserialize(self, data):
        data = json.loads(data)
        self.pointers = SortedDict()
        for k, v in data.items():
            self.pointers[b64decode(k)] = v
