from sys import getsizeof
from typing import Optional

from kevo.engines.kvstore import KVStore
from kevo.replication import Replica


class MemOnly(KVStore):
    name = 'MemOnly'

    def __init__(self,
                 data_dir='./data',
                 max_key_len=255,
                 max_value_len=255,
                 replica: Optional[Replica] = None):
        self.type = 'memonly'
        super().__init__(data_dir, max_key_len=max_key_len, max_value_len=max_value_len, replica=replica)

        self.hash_index: dict[bytes, bytes] = {}

        # it needs to be named like that so that it can work with the replica when needed
        self.log_path = (self.data_dir / 'L0.0.run')

        if self.replica:
            self.restore()
        else:
            self.rebuild_indices()

    def rebuild_indices(self):
        self.hash_index.clear()
        if self.log_path.is_file():
            with self.log_path.open('rb') as log_file:
                key, value = self._read_kv_pair(log_file)
                while key:
                    self.hash_index[key] = value
                    key, value = self._read_kv_pair(log_file)

    def close(self):
        self.save_metadata()
        self.snapshot()

    def __getitem__(self, key):
        return self.get(key)

    def __setitem__(self, key, value):
        return self.set(key, value)

    def get(self, key: bytes):
        assert type(key) is bytes
        assert 0 < len(key) <= self.max_key_len

        ret = KVStore.EMPTY
        try:
            ret = self.hash_index[key]
        except KeyError:
            pass
        return ret

    def set(self, key: bytes, value: bytes = KVStore.EMPTY):
        assert type(key) is bytes and type(value) is bytes
        assert 0 < len(key) <= self.max_key_len and len(value) <= self.max_value_len

        self.hash_index[key] = value

    def flush(self):
        if len(self.hash_index) == 0:
            return
        tmp_log_path = self.log_path.with_suffix('.tmp')

        with tmp_log_path.open('wb') as log_file:
            for key, value in self.hash_index.items():
                self._write_kv_pair(log_file, key, value)

        tmp_log_path.rename(tmp_log_path.with_suffix('.run'))

        if self.replica:
            self.replica.put(self.log_path.name)

    def snapshot(self):
        self.flush()

    def restore(self, version=None):
        self.flush()
        if self.replica:
            self.replica.restore(max_per_level=1, version=version)
            self.rebuild_indices()

    def __sizeof__(self):
        return getsizeof(self.hash_index)
