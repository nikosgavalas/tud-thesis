import os

from src.kvstore import KVStore, EMPTY, MAX_KEY_LENGTH, MAX_VALUE_LENGTH


class SimpleLog(KVStore):
    def __init__(self, data_dir='./data', compaction_threshold=4_000_000):
        self.type = 'simplelog'
        super().__init__(data_dir)

        self.compaction_threshold = compaction_threshold
        self.compaction_counter = 0

        self.hash_index = {}

        self.log_path = self.data_dir / 'log'
        if self.log_path.is_file():
            with self.log_path.open('rb') as log_file:
                offset = log_file.tell()
                k, v = self._read_kv_pair(log_file)
                self.compaction_counter += len(k) + len(v)
                while k:
                    self.hash_index[k] = offset
                    offset = log_file.tell()
                    k, v = self._read_kv_pair(log_file)
                    self.compaction_counter += len(k) + len(v)

    def __del__(self):
        self.close()

    def close(self):
        self.save_metadata()

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

        if self.compaction_counter >= self.compaction_threshold:
            self.compact()

        with self.log_path.open('ab') as log_file:
            offset = log_file.tell()
            self._write_kv_pair(log_file, key, value)
            self.hash_index[key] = offset
            self.compaction_counter += len(key) + len(value)

    def compact(self):
        compacted_log_path = self.log_path.with_suffix('.tmp')
        new_hash_index = {}

        with self.log_path.open('rb') as log_file, compacted_log_path.open('ab') as compacted_log_file:
            read_offset = log_file.tell()
            k, v = self._read_kv_pair(log_file)
            while k:
                if k in self.hash_index and read_offset == self.hash_index[k]:
                    write_offset = compacted_log_file.tell()
                    self._write_kv_pair(compacted_log_file, k, v)
                    new_hash_index[k] = write_offset
                read_offset = log_file.tell()
                k, v = self._read_kv_pair(log_file)

        # rename the file back
        compacted_log_path.rename(compacted_log_path.with_suffix(''))
        # swap the index with the new one
        self.hash_index = new_hash_index
        # reset the compaction counter
        self.compaction_counter = 0
