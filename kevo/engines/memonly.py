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

        if self.remote:
            self.restore()
        else:
            self.rebuild_indices()

    def rebuild_indices(self):
        self.hash_index.clear()

        runs_discovered = self.discover_runs()
        if not runs_discovered:
            return

        level_idx, run_idx, version = runs_discovered[0]
        with (self.data_dir / f'L{level_idx}.{run_idx}.{version}.run').open('rb') as log_file:
            key, value = self._read_kv_pair(log_file)
            while key:
                self.hash_index[key] = value
                key, value = self._read_kv_pair(log_file)

    def close(self):
        pass

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

        for file in self.data_dir.glob(f'L0.0.*.run'):
            if int(file.name.split('.')[2]) < self.global_version:
                file.unlink()

        self.global_version += 1

    def snapshot(self, id: int):
        self.flush()
        if self.remote:
            runs = discover_run_files(self.data_dir)
            self.remote.push_deltas(runs, id)

    def restore(self, version=None):
        self.flush()
        if self.remote:
            self.global_version = self.remote.restore(version=version)
            self.rebuild_indices()

    def __sizeof__(self):
        return getsizeof(self.hash_index)
