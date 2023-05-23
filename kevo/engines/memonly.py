from sys import getsizeof
from typing import Optional

from kevo.engines.kvstore import KVStore, discover_run_files
from kevo.remote import Remote


class MemOnly(KVStore):
    name = 'MemOnly'

    def __init__(self,
                 data_dir='./data',
                 max_key_len=255,
                 max_value_len=255,
                 remote: Optional[Remote] = None):
        self.type = 'memonly'
        super().__init__(data_dir, max_key_len=max_key_len, max_value_len=max_value_len, remote=remote)

        self.hash_index: dict[bytes, bytes] = {}

        self.global_version = 0
        self.snapshot_version = 0

        if self.remote:
            self.restore()
        else:
            self.rebuild_indices()

    def rebuild_indices(self):
        self.hash_index.clear()
        if (self.data_dir / f'L0.0.{self.global_version}.run').is_file():
            with (self.data_dir / f'L0.0.{self.global_version}.run').open('rb') as log_file:
                key, value = self._read_kv_pair(log_file)
                while key:
                    self.hash_index[key] = value
                    key, value = self._read_kv_pair(log_file)

    def close(self):
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

        with (self.data_dir / f'L0.0.{self.global_version}.run').open('wb') as log_file:
            for key, value in self.hash_index.items():
                self._write_kv_pair(log_file, key, value)

        if (self.data_dir / f'L0.0.{self.global_version - 1}.run').is_file():
            (self.data_dir / f'L0.0.{self.global_version - 1}.run').unlink()

        self.global_version += 1

    def snapshot(self):
        self.flush()
        if self.remote:
            runs = discover_run_files(self.data_dir)
            self.remote.push_deltas(runs, self.snapshot_version)
            self.snapshot_version += 1

    def restore(self, version=None):
        self.flush()
        if self.remote:
            self.global_version = self.remote.restore(version=version)
            if self.global_version is None:
                self.global_version = 0
            self.rebuild_indices()

    def __sizeof__(self):
        return getsizeof(self.hash_index)
