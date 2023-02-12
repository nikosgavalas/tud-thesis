'''
This now is implemented as a sorted dictionary (because I need the bisect_left/right) with base64/json-based ser/der.
A better implementation would be: two arrays (one for keys one for values) so that I can binary-search on the keys, and binary encoding for ser/der.
'''

import json
from base64 import b64encode, b64decode

from sortedcontainers import SortedDict


class FencePointers:
    def __init__(self, from_str: str | None = None):
        self.pointers = SortedDict()

        if type(from_str) is str:
            data = json.loads(from_str) 
            for k, v in data.items():
                self.pointers[b64decode(k)] = v

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

    def __str__(self) -> str:
        return self.serialize()
