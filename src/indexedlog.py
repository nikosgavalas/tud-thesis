'''
Key-value store based on Microsoft's FASTER (https://microsoft.github.io/FASTER/docs/td-research-papers/)
'''

from src.kvstore import KVStore, EMPTY


class IndexedLog(KVStore):
    def __init__(self, data_dir='./data', max_key_len=4, max_value_len=4):
        super().__init__(data_dir)

        # metadata not used for something yet.
        if 'type' in self.metadata:
            assert self.metadata['type'] == 'indexedlog', 'incorrect directory structure'
        else:
            self.metadata['type'] = 'indexedlog'

        self.max_key_len = max_key_len
        self.max_value_len = max_value_len

        self.index = {}
        self.log_file = (self.data_dir / 'log').open('wb+')
        self.log_idx = 0

    def close(self):
        self.log_file.close()

    def get(self, key: bytes):
        assert type(key) is bytes and 0 < len(key) <= self.max_key_len

        if key not in self.index:
            return EMPTY

        offset = self.index[key] * (self.max_key_len + self.max_value_len)
        self.log_file.seek(offset + self.max_key_len)
        value = self.log_file.read(self.max_value_len).strip(b'\x00')  # strip the padding

        return value

    def set(self, key: bytes, value: bytes = EMPTY):
        assert type(key) is bytes and type(value) is bytes and 0 < len(key) <= self.max_key_len and len(value) <= self.max_value_len

        self.index[key] = self.log_idx
        offset = self.log_idx * (self.max_key_len + self.max_value_len)

        self.log_file.write(key)
        self.log_file.seek(offset + self.max_key_len)
        self.log_file.write(value)
        self.log_file.seek(offset + self.max_key_len + self.max_value_len)

        self.log_idx += 1
