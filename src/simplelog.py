from src.kvstore import KVStore, EMPTY, MAX_KEY_LENGTH, MAX_VALUE_LENGTH


class SimpleLog(KVStore):
    def __init__(self, data_dir='./data', compaction_interval=1_024):
        self.type = 'simplelog'
        super().__init__(data_dir)

        self.hash_index = {}
        self.log_path = self.data_dir / 'log'
        if self.log_path.is_file():
            with self.log_path.open('rb') as log_file:
                k, _, o = self._read_kv_pair(log_file, return_offset=True)
                while k:
                    self.hash_index[k] = o
                    k, _, o = self._read_kv_pair(log_file, return_offset=True)

    def __del__(self):
        self.close()

    def close(self):
        pass

    def get(self, key: bytes):
        assert type(key) is bytes and 0 < len(key) <= MAX_KEY_LENGTH

        if key not in self.hash_index:
            return EMPTY

        with self.log_path.open('rb') as log_file:
            offset = self.hash_index[key]
            log_file.seek(offset)
            k, v = self._read_kv_pair(log_file)
            assert k == key
            return v

    def set(self, key: bytes, value: bytes = EMPTY):
        assert type(key) is bytes and type(value) is bytes and 0 < len(key) <= MAX_KEY_LENGTH and len(value) <= MAX_VALUE_LENGTH

        with self.log_path.open('ab') as log_file:
            self.hash_index[key] = log_file.tell()
            self._write_kv_pair(log_file, key, value)
