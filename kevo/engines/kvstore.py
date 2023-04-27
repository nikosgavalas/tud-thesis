"""
Base class for IndexedLog and LSMTree classes.
"""

import json
from pathlib import Path
from collections import namedtuple


Record = namedtuple('Record', ['level', 'run', 'offset'])


def bytes_needed_to_encode_len(length):
    i = 0
    while 2 ** (i * 8) <= length:
        i += 1
    return i


class KVStore:
    EMPTY = b''

    def __init__(self,
                 data_dir='./data',
                 max_key_len=255,
                 max_value_len=255,
                 replica=None):

        self.data_dir = Path(data_dir)
        self.data_dir.mkdir(exist_ok=True)

        self.max_key_len = max_key_len
        self.max_value_len = max_value_len

        self.key_enc_len = bytes_needed_to_encode_len(self.max_key_len)
        self.val_enc_len = bytes_needed_to_encode_len(self.max_value_len)

        self.replica = replica

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
        first_bytes = fd.read(self.key_enc_len)
        if not first_bytes:
            return KVStore.EMPTY, KVStore.EMPTY
        key_len = int.from_bytes(first_bytes, byteorder='little')
        key = fd.read(key_len)
        val_len = int.from_bytes(fd.read(self.val_enc_len), byteorder='little')
        value = fd.read(val_len)
        return key, value

    def _write_kv_pair(self, fd, key, value, flush=False):
        fd.write(len(key).to_bytes(self.key_enc_len, byteorder='little'))
        fd.write(key)
        fd.write(len(value).to_bytes(self.val_enc_len, byteorder='little'))
        fd.write(value)
        if flush:
            fd.flush()

    # abstract methods
    def __getitem__(self, key):
        raise NotImplementedError('')

    def __setitem__(self, key, value):
        raise NotImplementedError('')

    def get(self, key: bytes):
        raise NotImplementedError('')

    def set(self, key: bytes, value: bytes):
        raise NotImplementedError('')

    def __sizeof__(self):
        raise NotImplementedError('')

    def close(self):
        raise NotImplementedError('')
