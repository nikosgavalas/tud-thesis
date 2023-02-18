'''
Key-value store based on Microsoft's FASTER (https://microsoft.github.io/FASTER/docs/td-research-papers/)
'''

from src.kvstore import KVStore, EMPTY


MAX_KEY_LENGTH = 4
MAX_VALUE_LENGTH = 4

class IndexedLog(KVStore):
    def __init__(self, data_dir='./data'):
        super().__init__(data_dir)

        # metadata not used for something yet.
        if 'type' in self.metadata:
            assert self.metadata['type'] == 'indexedlog', 'incorrect directory structure'
        else:
            self.metadata['type'] = 'indexedlog'
        
        self.index = {}
        self.log_file = (self.data_dir / 'log').open('wb+')
        self.log_idx = 0

    def close(self):
        self.log_file.close()

    def get(self, key: bytes):
        assert type(key) is bytes and 0 < len(key) <= MAX_KEY_LENGTH

        if key not in self.index:
            return EMPTY

        offset = self.index[key] * (MAX_KEY_LENGTH + MAX_VALUE_LENGTH)
        self.log_file.seek(offset + MAX_KEY_LENGTH)
        value = self.log_file.read(MAX_VALUE_LENGTH).strip(b'\x00')  # strip the padding

        return value

    def set(self, key: bytes, value: bytes = EMPTY):
        assert type(key) is bytes and type(value) is bytes and 0 < len(key) <= MAX_KEY_LENGTH and len(value) <= MAX_VALUE_LENGTH

        self.index[key] = self.log_idx
        offset = self.log_idx * (MAX_KEY_LENGTH + MAX_VALUE_LENGTH)

        self.log_file.write(key)
        self.log_file.seek(offset + MAX_KEY_LENGTH)
        self.log_file.write(value)
        self.log_file.seek(offset + MAX_KEY_LENGTH + MAX_VALUE_LENGTH)

        self.log_idx += 1
