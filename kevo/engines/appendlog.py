from sys import getsizeof
from typing import Optional
from io import FileIO

from kevo.engines.kvstore import KVStore, Record, discover_run_files
from kevo.remote import Remote


class AppendLog(KVStore):
    name = 'AppendLog'

    def __init__(self,
                 data_dir='./data',
                 max_key_len=255,
                 max_value_len=255,
                 max_runs_per_level=3,
                 threshold=4_000_000,
                 compaction=False,
                 remote: Optional[Remote] = None):
        self.type = 'appendlog'
        super().__init__(data_dir, max_key_len=max_key_len, max_value_len=max_value_len, remote=remote)

        assert max_runs_per_level > 1
        assert threshold > 0

        self.max_runs_per_level = max_runs_per_level
        self.threshold = threshold
        self.counter = 0

        self.compaction_enabled = compaction

        self.hash_index: dict[bytes, Record] = {}
        self.levels: list[int] = []
        # read file-descriptors
        self.rfds: list[list[FileIO]] = []
        # write file-descriptor
        self.wfd: Optional[FileIO] = None

        self.global_version = 0

        if self.remote:
            self.restore()
        else:
            self.rebuild_indices()

    def rebuild_indices(self):
        self.levels.clear()
        self.rfds.clear()

        self.hash_index.clear()

        # do file discovery
        runs_discovered = self.discover_runs()
        if not runs_discovered:
            self.wfd = (self.data_dir / f'L{0}.{0}.{self.global_version}.run').open('wb')
            self.rfds.append([(self.data_dir / f'L{0}.{0}.{self.global_version}.run').open('rb')])
            self.levels.append(0)
            return

        # rebuild the index
        for level_idx, run_idx, version in sorted(runs_discovered):
            while level_idx >= len(self.levels):
                self.levels.append(0)
            self.levels[level_idx] += 1

            while level_idx >= len(self.rfds):
                self.rfds.append([])
            self.rfds[level_idx].append((self.data_dir / f'L{level_idx}.{run_idx}.{version}.run').open('rb'))

        # sort by first field asc and by second field desc
        for level_idx, run_idx, version in sorted(runs_discovered, key=lambda x: (-x[0], x[1])):
            log_file = self.rfds[level_idx][run_idx]
            offset = log_file.tell()
            k, _ = self._read_kv_pair(log_file)
            while k:
                self.hash_index[k] = Record(level_idx, run_idx, offset)
                offset = log_file.tell()
                k, _ = self._read_kv_pair(log_file)

        self.wfd = (self.data_dir / f'L{0}.{self.levels[0]}.{self.global_version}.run').open('wb')
        self.rfds[0].append((self.data_dir / f'L{0}.{self.levels[0]}.{self.global_version}.run').open('rb'))

    def close(self):
        self.close_run()
        self.wfd.close()
        for rfds in self.rfds:
            for rfd in rfds:
                rfd.close()

    def __getitem__(self, key):
        return self.get(key)

    def __setitem__(self, key, value):
        return self.set(key, value)

    def get(self, key: bytes):
        assert type(key) is bytes
        assert 0 < len(key) <= self.max_key_len

        if key not in self.hash_index:
            return KVStore.EMPTY

        record = self.hash_index[key]

        log_file = self.rfds[record.level][record.run]
        log_file.seek(record.offset)
        k, v = self._read_kv_pair(log_file)
        assert k == key
        return v

    def set(self, key: bytes, value: bytes = KVStore.EMPTY):
        assert type(key) is bytes and type(value) is bytes
        assert 0 < len(key) <= self.max_key_len and len(value) <= self.max_value_len

        if not value and key in self.hash_index:
            del self.hash_index[key]
            return

        # always write the latest log of the first level
        offset = self.wfd.tell()
        self._write_kv_pair(self.wfd, key, value, flush=True)
        self.hash_index[key] = Record(0, self.levels[0], offset)
        self.counter += len(key) + len(value)

        if self.counter >= self.threshold:
            self.close_run()
            self.open_new_files()

    def close_run(self):
        if self.counter == 0:
            return

        flush_level = 0
        if flush_level >= len(self.levels):
            self.levels.append(0)
        if flush_level >= len(self.rfds):
            self.rfds.append([])

        self.counter = 0
        self.wfd.close()

        if self.compaction_enabled:
            self.compaction(self.levels[flush_level])

        self.levels[flush_level] += 1
        if self.levels[flush_level] >= self.max_runs_per_level:
            self.merge(flush_level)

    def open_new_files(self):
        flush_level = 0
        self.wfd.close()
        self.wfd = (self.data_dir / f'L{flush_level}.{self.levels[flush_level]}.{self.global_version}.run').open('ab')
        self.rfds[flush_level].append((self.data_dir / f'L{flush_level}.{self.levels[flush_level]}.{self.global_version}.run').open('rb'))

    def compaction(self, run):
        log_path = (self.data_dir / f'L0.{run}.{self.global_version}.run')
        compacted_log_path = log_path.with_suffix('.tmp')
        # NOTE i can copy the index here and keep the old one for as long as the compaction is running to enable reads
        # concurrently

        with compacted_log_path.open('ab') as compacted_log_file:
            read_offset = 0
            self.rfds[0][run].seek(read_offset)
            k, v = self._read_kv_pair(self.rfds[0][run])
            while k:
                if k in self.hash_index and self.hash_index[k] == Record(0, run, read_offset):
                    write_offset = compacted_log_file.tell()
                    self._write_kv_pair(compacted_log_file, k, v)
                    self.hash_index[k] = Record(0, run, write_offset)
                read_offset = self.rfds[0][run].tell()
                k, v = self._read_kv_pair(self.rfds[0][run])

        self.rfds[0][run].close()
        # rename the file back
        compacted_log_path.rename(compacted_log_path.with_suffix('.run'))
        # get a new read fd
        self.rfds[0][run] = log_path.open('rb')

    def merge(self, level: int):
        next_level = level + 1
        if next_level >= len(self.levels):
            self.levels.append(0)
            self.rfds.append([])
        next_run = self.levels[next_level]

        dst_file = (self.data_dir / f'L{next_level}.{next_run}.{self.global_version}.run').open('ab')
        for run_idx in range(self.levels[level]):
            src_file = self.rfds[level][run_idx]
            src_offset = 0
            src_file.seek(src_offset)
            k, v = self._read_kv_pair(src_file)
            while k:
                if k in self.hash_index and self.hash_index[k] == Record(level, run_idx, src_offset):
                    dst_offset = dst_file.tell()
                    self._write_kv_pair(dst_file, k, v)
                    self.hash_index[k] = Record(next_level, next_run, dst_offset)
                src_offset = src_file.tell()
                k, v = self._read_kv_pair(src_file)
        dst_file.close()

        self.rfds[next_level].append((self.data_dir / f'L{next_level}.{next_run}.{self.global_version}.run').open('rb'))

        # delete merged files
        for rfd in self.rfds[level]:
            rfd.close()
        self.rfds[level].clear()

        # remove the files after successfully merging.
        for file in self.data_dir.glob(f'L{level}.*.*.run'):
            file.unlink()

        # update the runs counter
        self.levels[level] = 0
        self.levels[next_level] += 1

        # bump the global version
        self.global_version += 1

        # merge recursively
        if self.levels[next_level] >= self.max_runs_per_level:
            self.merge(next_level)

    def snapshot(self, id: int):
        self.close_run()
        if self.remote:
            runs = discover_run_files(self.data_dir)
            self.remote.push_deltas(runs, id)
        self.open_new_files()

    def restore(self, version=None):
        self.close_run()
        if self.remote:
            self.global_version = self.remote.restore(version=version)
            if self.wfd is not None:
                self.wfd.close()
            for rfds in self.rfds:
                for rfd in rfds:
                    rfd.close()
            self.rebuild_indices()

    def __sizeof__(self):
        return getsizeof(self.hash_index)
