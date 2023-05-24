from io import FileIO
from sys import getsizeof
from typing import Optional

# from kevo.common.hashindex import HashIndex
from kevo.common.ringbuffer import RingBuffer
from kevo.engines.kvstore import KVStore, Record, discover_run_files
from kevo.remote import Remote


class HybridLog(KVStore):
    name = 'HybridLog'

    # TODO make the mem_segment_len equal to the sum of
    # ro_lag_int + flush_int
    def __init__(self,
                 data_dir='./data',
                 max_key_len=255,
                 max_value_len=255,
                 max_runs_per_level=3,
                 ro_lag_interval=2 ** 10,
                 flush_interval=(4 * 2 ** 10),
                 hash_index='dict',
                 remote: Optional[Remote] = None):
        self.type = 'hybridlog'
        super().__init__(data_dir, max_key_len=max_key_len, max_value_len=max_value_len, remote=remote)

        assert max_runs_per_level > 1
        assert flush_interval > 0
        assert ro_lag_interval > 0
        assert hash_index in ['dict', 'native'], 'hash_index parameter must be either "dict" or "native"'

        self.max_runs_per_level = max_runs_per_level
        self.ro_lag_interval = ro_lag_interval
        self.flush_interval = flush_interval
        # The mem segment length is not a parameter because there is no reason using more memory than the required
        # amount, there is no trade-off. Therefore we keep it to the lowest possible value which is this sum.
        self.mem_segment_len = flush_interval + ro_lag_interval + 1

        if hash_index == 'native':
            # TODO
            # self.hash_index = HashIndex(n_buckets_power=4, key_len_bits=self.max_key_len*8, value_len_bits=self.max_value_len*8)
            raise NotImplementedError("do not use the native hash index, it currently has a bug, use 'dict' instead")
        else:
            self.hash_index: dict[bytes, int] = {}

        self.la_to_file_offset: dict[int, Record] = {}

        self.head_offset: int = 0  # LA > head_offset is in mem
        self.ro_offset: int = 0  # in LA > ro_offset we have the mutable region
        self.tail_offset: int = 0  # points to the tail of the log, the last record inserted

        self.levels: list[int] = []
        # read file-descriptors
        self.rfds: list[list[FileIO]] = []
        # write file-descriptor
        self.wfd: Optional[FileIO] = None

        self.global_version = 0

        self.memory: Optional[RingBuffer] = None

        if self.remote:
            self.restore()
        else:
            self.rebuild_indices()

    def rebuild_indices(self):
        self.levels.clear()
        self.rfds.clear()

        self.hash_index.clear()
        self.la_to_file_offset.clear()

        self.head_offset = 0
        self.ro_offset = 0
        self.tail_offset = 0

        self.memory = RingBuffer(self.mem_segment_len)

        # do file discovery
        runs_discovered = self.discover_runs()
        if not runs_discovered:
            self.wfd = (self.data_dir / f'L{0}.{0}.{self.global_version}.run').open('wb')
            self.rfds.append([(self.data_dir / f'L{0}.{0}.{self.global_version}.run').open('rb')])
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
                self.head_offset += 1
                self.hash_index[k] = self.head_offset
                self.la_to_file_offset[self.head_offset] = Record(level_idx, run_idx, offset)
                offset = log_file.tell()
                k, _ = self._read_kv_pair(log_file)

        self.wfd = (self.data_dir / f'L{0}.{self.levels[0]}.{self.global_version}.run').open('wb')
        self.rfds[0].append((self.data_dir / f'L{0}.{self.levels[0]}.{self.global_version}.run').open('rb'))

        self.ro_offset = self.head_offset
        self.tail_offset = self.ro_offset
        self.memory.set_tail_offset(self.tail_offset)

    def close(self):
        self.flush(self.tail_offset)  # flush everything
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

        offset = self.hash_index[key]
        if offset > self.head_offset:
            _, v = self.memory[offset]
            return v

        record = self.la_to_file_offset[offset]
        log_file = self.rfds[record.level][record.run]
        log_file.seek(record.offset)
        _, v = self._read_kv_pair(log_file)
        return v

    def set(self, key: bytes, value: bytes = KVStore.EMPTY):
        assert type(key) is bytes and type(value) is bytes
        assert 0 < len(key) <= self.max_key_len and len(value) <= self.max_value_len

        if self.memory.is_full():
            self.flush(self.ro_offset)

        if key in self.hash_index:
            offset = self.hash_index[key]
            if offset > self.ro_offset:
                # update in-place
                self.memory[offset] = (key, value)
                return

        self.tail_offset = self.memory.add((key, value))
        self.hash_index[key] = self.tail_offset
        # no need to increment the tail offset as the ring buffer returns the new (incremented) address

        if self.tail_offset - self.ro_offset > self.ro_lag_interval:
            self.ro_offset += 1

        if self.ro_offset - self.head_offset > self.flush_interval:
            self.flush(self.ro_offset)
            self.open_new_files()

    def flush(self, offset: int):
        if self.memory.is_empty():
            return

        flush_level = 0

        if flush_level >= len(self.levels):
            self.levels.append(0)
        if flush_level >= len(self.rfds):
            self.rfds.append([])

        while self.head_offset < offset:
            key, value = self.memory.pop()
            write_offset = self.wfd.tell()
            self.head_offset += 1
            # if is not the most recent record, drop it, no need to keep it.
            if self.hash_index[key] == self.head_offset:
                self._write_kv_pair(self.wfd, key, value)
                self.la_to_file_offset[self.head_offset] = Record(flush_level, self.levels[flush_level], write_offset)

        self.wfd.close()

        self.levels[flush_level] += 1
        if self.levels[flush_level] >= self.max_runs_per_level:
            self.merge(flush_level)

        # open a new file after merging
        # self.wfd = (self.data_dir / f'L{flush_level}.{self.levels[flush_level]}.{self.global_version}.run').open('ab')
        # self.rfds[flush_level].append((self.data_dir / f'L{flush_level}.{self.levels[flush_level]}.{self.global_version}.run').open('rb'))

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
                if k in self.hash_index:
                    la = self.hash_index[k]
                    if la in self.la_to_file_offset and self.la_to_file_offset[la] == Record(level, run_idx, src_offset):
                        dst_offset = dst_file.tell()
                        self._write_kv_pair(dst_file, k, v)
                        self.la_to_file_offset[la] = Record(next_level, next_run, dst_offset)
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

    def open_new_files(self):
        flush_level = 0
        self.wfd.close()
        self.wfd = (self.data_dir / f'L{flush_level}.{self.levels[flush_level]}.{self.global_version}.run').open('ab')
        self.rfds[flush_level].append((self.data_dir / f'L{flush_level}.{self.levels[flush_level]}.{self.global_version}.run').open('rb'))

    def snapshot(self, id: int):
        self.flush(self.tail_offset)
        self.ro_offset = self.tail_offset
        if self.remote:
            runs = discover_run_files(self.data_dir)
            self.remote.push_deltas(runs, id)
        self.open_new_files()

    def restore(self, version=None):
        if self.remote:
            self.global_version = self.remote.restore(version=version)
            # close open file descriptors before rebuilding
            if self.wfd is not None:
                self.wfd.close()
            for rfds in self.rfds:
                for rfd in rfds:
                    rfd.close()
            self.rebuild_indices()

    def __sizeof__(self):
        return getsizeof(self.hash_index) + getsizeof(self.la_to_file_offset) + getsizeof(self.memory)
