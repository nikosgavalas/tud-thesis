'''
Base class for IndexedLog and LSMTree classes.
'''

import json
from pathlib import Path


EMPTY = b''
# TODO maybe bump these up to 65536? the drawback is that I'll need two bytes for len(key) and len(value) in the binary encoding
MAX_KEY_LENGTH = 256  # maximum values encodable with one byte
MAX_VALUE_LENGTH = 256

class KVStore():
    def __init__(self, data_dir='./data'):
        self.data_dir = Path(data_dir)
        self.data_dir.mkdir(exist_ok=True)

        self.metadata = {}
        self.metadata_path = self.data_dir / 'metadata'
        self.load_metadata()
        if 'type' in self.metadata:
            assert self.metadata['type'] == self.type, 'incorrect directory structure'
        else:
            self.metadata['type'] = self.type


    def load_metadata(self):
        if self.metadata_path.is_file():
            with self.metadata_path.open('r') as metadata_file:
                self.metadata = json.loads(metadata_file.read())

    def save_metadata(self):
        with self.metadata_path.open('w') as metadata_file:
            metadata_file.write(json.dumps(self.metadata))
    
    # abstract methods
    def get(self, key: bytes):
        raise NotImplementedError("")

    def set(self, key: bytes, value: bytes):
        raise NotImplementedError("")
