"""
Key-value store based on Microsoft's FASTER (https://microsoft.github.io/FASTER/docs/td-research-papers/).
The good thing about this one is that the memory part in front of the file works like a cache, so for some distributions
of data (like zipfian) it may work really well. However, it sucks for range queries (not that I support those in the LSM
implementation right now but I will if I have to, it's trivial) and the compaction cannot be done efficiently
(needs O(n) memory because of the need to use a hashset while log-structuring compaction is O(1)).  Also, wtf am I
supposed to do for fault-tolerance of the hash-index? In the paper they snapshot it but I am not sure how this is
completely watertight. And then a WAL would be stupid I think, I mean, we use the in-memory buffer to avoid I/O, and
we're going to use I/O anyway? Eh. I don't like this one also because the logical index compels for limited length
of the keys and values. If I only had an append log, I could store in my index the file offsets directly and with a
proper binary encoding I could have variable lengths for keys and values. I would also use a WAL for the hash index
and boom done. Another limitation of this type of log is that keys must all fit to memory at all times (due to the
index being entirely in-memory).

Logical addresses:
----------------------------------------------
|        |            |            :
|  disk  |   ro mem   |   rw mem   :
|        |            |            :
----------------------------------------------
^        ^            ^            ^
0        head offset  ro offset    tail offset

Another concern of mine: I am not sure why the read-only part (with copy-on-update) is useful. Maybe it has to do with
concurrency, with the epoch system in the paper, but I don't have multiple threads here... TODO re-read about that.
Wait a sec... even the copy-on-update... why? just insert the new value directly, why copy? this is only useful when
you want to do a blind update, like "increment by 1" so you need to do a read first. but even then, you don't have to
give it a specific name like copy-on-update, it's just the way you'd do it naturally...
TODO: compaction of the log on disk
Note: for the compaction you don't actually need extra memory, you can just use the index that you already have in-mem.
I just realized compaction (and also merging) is not possible here trivially. One needs to keep track of an array
of the previous file sizes/maxoffset to be able to maintain a continuous logical address space, which adds too much
overhead in the address translations, plus requires extra bookkeeping (persistence). I am not doing it.

Something I haven't done (iirc in the appendlog implementation as well) is clear the entry from the hash index when it is
removed from the db. well, TODO.

Something radical I just did: I completely removed the file offset to LA linear translation, which required me to
have very small keyval lengths (cause i needed padding and large keyvals would make the disk explode), and just used
extra memory for the translation by using a dict...
"""

from io import FileIO
from sys import getsizeof
from typing import Optional

# from kevo.common.hashindex import HashIndex
from kevo.common.ringbuffer import RingBuffer
from kevo.engines.kvstore import KVStore, Record
from kevo.replication import Replica


class HybridLog(KVStore):
    name = 'HybridLog'

    # TODO make the mem_segment_len equal to the sum of
    # ro_lag_int + flush_int
    def __init__(self,
                 data_dir='./data',
                 max_key_len=255,
                 max_value_len=255,
                 max_runs_per_level=3,
                 mem_segment_len=2 ** 20,
                 ro_lag_interval=2 ** 10,
                 flush_interval=(4 * 2 ** 10),
                 hash_index='dict',
                 compaction_enabled=False,
                 auto_push=True,
                 replica: Optional[Replica] = None):
        self.type = 'hybridlog'
        super().__init__(data_dir, max_key_len=max_key_len, max_value_len=max_value_len,
                         auto_push=auto_push, replica=replica)

        assert max_runs_per_level > 1
        assert flush_interval > 0
        assert mem_segment_len >= ro_lag_interval + flush_interval
        assert hash_index in ['dict', 'native'], 'hash_index parameter must be either "dict" or "native"'

        self.max_runs_per_level = max_runs_per_level
        self.ro_lag_interval = ro_lag_interval
        self.flush_interval = flush_interval
        self.mem_segment_len = mem_segment_len
        self.compaction_enabled = compaction_enabled

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
        self.rfds: list[list[FileIO]] = [[]]
        # write file-descriptor
        self.wfd: Optional[FileIO] = None

        self.memory: Optional[RingBuffer] = None

        if self.replica:
            self.restore()
        else:
            self.rebuild_indices()

    def rebuild_indices(self):
        self.hash_index = {}

        self.filenames_to_push.clear()

        self.la_to_file_offset = {}

        self.head_offset = 0  # LA > head_offset is in mem
        self.ro_offset = 0  # in LA > ro_offset we have the mutable region
        self.tail_offset = 0  # points to the tail of the log, the next free available slot in memory

        # do file discovery
        data_files_levels = [int(f.name.split('.')[0][1:]) for f in self.data_dir.glob('L*') if f.is_file()]
        self.levels = [0] * (max(data_files_levels) + 1) if data_files_levels else [0]
        for i in data_files_levels:
            self.levels[i] += 1

        self.rfds = [[(self.data_dir / f'L{level_idx}.{run_idx}.run').open('rb') for run_idx in range(n_runs)] for
                     level_idx, n_runs in enumerate(self.levels)]
        self.wfd = (self.data_dir / f'L{0}.{self.levels[0]}.run').open('wb')
        self.rfds[0].append((self.data_dir / f'L{0}.{self.levels[0]}.run').open('rb'))

        # rebuild the index
        for level_idx, n_runs in reversed(list(enumerate(self.levels))):
            for run_idx in range(n_runs):
                log_file = self.rfds[level_idx][run_idx]
                offset = log_file.tell()
                k, _ = self._read_kv_pair(log_file)
                while k:
                    self.head_offset += 1
                    self.hash_index[k] = self.head_offset
                    self.la_to_file_offset[self.head_offset] = Record(level_idx, run_idx, offset)
                    offset = log_file.tell()
                    k, _ = self._read_kv_pair(log_file)
        self.ro_offset = self.head_offset
        self.tail_offset = self.ro_offset

        self.memory = RingBuffer(self.mem_segment_len)

    def close(self):
        self.flush(self.tail_offset)  # flush everything
        self.save_metadata()
        if self.replica:
            self.snapshot()
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

    def flush(self, offset: int):
        while self.head_offset < offset:
            key, value = self.memory.pop()
            write_offset = self.wfd.tell()
            self.head_offset += 1
            # if is not the most recent record, drop it, like we do with the file compaction
            if self.hash_index[key] == self.head_offset:
                self._write_kv_pair(self.wfd, key, value)
                self.la_to_file_offset[self.head_offset] = Record(0, self.levels[0], write_offset)

        self.wfd.close()

        if self.compaction_enabled:
            # TODO remove compaction entirely. It is done in-mem when flushing,
            # there's no need for it at all.
            self.compaction(self.levels[0])

        if self.replica:
            self.filenames_to_push.append(self.wfd.name)
            if self.auto_push:
                self.push_files()

        self.levels[0] += 1
        if self.levels[0] >= self.max_runs_per_level:
            self.merge(0)

        # open a new file after merging
        self.wfd = (self.data_dir / f'L{0}.{self.levels[0]}.run').open('ab')
        self.rfds[0].append((self.data_dir / f'L{0}.{self.levels[0]}.run').open('rb'))

    def merge(self, level: int):
        next_level = level + 1
        if level + 1 >= len(self.levels):
            self.levels.append(0)
            self.rfds.append([])
        next_run = self.levels[level + 1]

        dst_file = (self.data_dir / f'L{next_level}.{next_run}.run').open('ab')
        for run_idx in range(self.levels[level]):
            src_file = self.rfds[level][run_idx]
            src_offset = src_file.tell()
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

        if self.replica:
            self.filenames_to_push.append(dst_file.name)
            if self.auto_push:
                self.push_files()

        self.rfds[next_level].append((self.data_dir / f'L{next_level}.{next_run}.run').open('rb'))

        # delete merged files
        for rfd in self.rfds[level]:
            rfd.close()
        self.rfds[level].clear()
        for run_idx in range(self.levels[level]):
            path_to_remove = (self.data_dir / f'L{level}.{run_idx}.run')
            path_to_remove.unlink()
        # update the runs counter
        self.levels[level] = 0
        self.levels[next_level] += 1
        # merge recursively
        if self.levels[next_level] >= self.max_runs_per_level:
            self.merge(next_level)

    def compaction(self, run):
        log_path = (self.data_dir / f'L0.{run}.run')
        compacted_log_path = log_path.with_suffix('.tmp')
        # NOTE i can copy the index here and keep the old one for as long as the compaction is running to enable reads
        # concurrently

        with compacted_log_path.open('ab') as compacted_log_file:
            read_offset = 0
            self.rfds[0][run].seek(read_offset)
            k, v = self._read_kv_pair(self.rfds[0][run])
            while k:
                # not checking if k in index as it is for sure (i never remove keys from the index so far)
                la = self.hash_index[k]
                # if the record lies on disk and is the most recent one:
                if la <= self.head_offset and self.la_to_file_offset[la] == Record(0, run, read_offset):
                    write_offset = compacted_log_file.tell()
                    self._write_kv_pair(compacted_log_file, k, v)
                    self.la_to_file_offset[la] = Record(0, run, write_offset)
                read_offset = self.rfds[0][run].tell()
                k, v = self._read_kv_pair(self.rfds[0][run])

        self.rfds[0][run].close()
        # rename the file back
        compacted_log_path.rename(compacted_log_path.with_suffix('.run'))
        # get a new read fd
        self.rfds[0][run] = log_path.open('rb')

    def snapshot(self):
        self.flush(self.tail_offset)
        self.ro_offset = self.tail_offset
        if not self.auto_push:
            self.push_files()

    def restore(self, version=None):
        if self.replica:
            self.replica.restore(max_per_level=self.max_runs_per_level, version=version)
            # close open file descriptors before rebuilding
            if self.wfd is not None:
                self.wfd.close()
            for rfds in self.rfds:
                for rfd in rfds:
                    rfd.close()
            self.rebuild_indices()

    def __sizeof__(self):
        return getsizeof(self.hash_index) + getsizeof(self.la_to_file_offset) + getsizeof(self.memory)
