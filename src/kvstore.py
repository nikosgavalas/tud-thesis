'''
Base class for IndexedLog and LSMTree classes.
'''

import json
import struct
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

    def _read_kv_pair(self, fd):
        first_byte = fd.read(1)
        if not first_byte:
            return EMPTY, EMPTY
        key_len = struct.unpack('<B', first_byte)[0]
        key = fd.read(key_len)
        val_len = struct.unpack('<B', fd.read(1))[0]
        value = fd.read(val_len)
        return key, value

    def _write_kv_pair(self, fd, key, value):
        fd.write(struct.pack('<B', len(key)))
        fd.write(key)
        fd.write(struct.pack('<B', len(value)))
        fd.write(value)

    # abstract methods
    def __getitem__(self, key):
        raise NotImplementedError('')

    def __setitem__(self, key, value):
        raise NotImplementedError('')

    def get(self, key: bytes):
        raise NotImplementedError('')

    def set(self, key: bytes, value: bytes):
        raise NotImplementedError('')

    def close(self):
        raise NotImplementedError('')
