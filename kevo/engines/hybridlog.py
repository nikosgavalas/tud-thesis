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

from sys import getsizeof
from typing import Optional

# from kevo.common.hashindex import HashIndex
from kevo.common.ringbuffer import RingBuffer
from kevo.engines.kvstore import KVStore
from kevo.replication import Replica


class HybridLog(KVStore):
    name = 'HybridLog'

    def __init__(self,
                 data_dir='./data',
                 max_key_len=255,
                 max_value_len=255,
                 mem_segment_len=2 ** 20,
                 ro_lag_interval=2 ** 10,
                 flush_interval=(4 * 2 ** 10),
                 hash_index='dict',
                 compaction_interval=0,
                 replica: Optional[Replica] = None):
        self.type = 'hybridlog'
        super().__init__(data_dir, max_key_len=max_key_len, max_value_len=max_value_len, replica=replica)

        assert flush_interval > 0
        assert compaction_interval >= 0  # if compaction interval is 0, compaction is disabled
        assert mem_segment_len >= ro_lag_interval + flush_interval
        assert hash_index in ['dict', 'native'], 'hash_index parameter must be either "dict" or "native"'

        self.ro_lag_interval = ro_lag_interval
        self.flush_interval = flush_interval
        self.compaction_interval = compaction_interval  # in number of flushes
        self.compaction_enabled = compaction_interval > 0

        if hash_index == 'native':
            # TODO
            # self.hash_index = HashIndex(n_buckets_power=4, key_len_bits=self.max_key_len*8, value_len_bits=self.max_value_len*8)
            raise NotImplementedError("do not use the native hash index, it currently has a bug, use 'dict' instead")
        else:
            self.hash_index = {}

        self.la_to_file_offset = {}

        self.head_offset = 0  # LA > head_offset is in mem
        self.ro_offset = 0  # in LA > ro_offset we have the mutable region
        self.tail_offset = 0  # points to the tail of the log, the next free available slot in memory

        self.compaction_counter = 0

        self.log_path = self.data_dir / 'log'
        self.log_path.touch()
        with self.log_path.open('rb') as log_file:
            offset = log_file.tell()
            k, _ = self._read_kv_pair(log_file)
            while k:
                self.head_offset += 1
                self.hash_index[k] = self.head_offset
                self.la_to_file_offset[self.head_offset] = offset
                offset = log_file.tell()
                k, _ = self._read_kv_pair(log_file)
        self.ro_offset = self.head_offset
        self.tail_offset = self.ro_offset

        self.rfd = self.log_path.open('rb')

        self.memory = RingBuffer(mem_segment_len)

    def close(self):
        self.flush(self.tail_offset)  # flush everything
        self.rfd.close()
        self.save_metadata()
        if self.replica:
            self.replica.put(self.log_path.name)
            self.replica.put(self.metadata_path.name)

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

        file_offset = self.la_to_file_offset[offset]
        self.rfd.seek(file_offset)
        _, v = self._read_kv_pair(self.rfd)
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
        with self.log_path.open('ab') as log_file:
            while self.head_offset < offset:
                key, value = self.memory.pop()
                write_offset = log_file.tell()
                self.head_offset += 1
                # if is not the most recent record, drop it, like we do with the file compaction
                if self.hash_index[key] == self.head_offset:
                    self._write_kv_pair(log_file, key, value)
                    self.la_to_file_offset[self.head_offset] = write_offset

        if self.replica and not self.compaction_enabled:
            self.replica.put(self.log_path.name)

        if self.compaction_enabled:
            self.compaction_counter += 1
            if self.compaction_counter >= self.compaction_interval:
                self.compaction()
                # if compaction is enabled, wait to sync to replica store *after* the compaction
                if self.replica and self.compaction_enabled:
                    self.replica.put(self.log_path.name)

    def compaction(self):
        compacted_log_path = self.log_path.with_suffix('.tmp')
        # NOTE i can copy the index here and keep the old one for as long as the compaction is running to enable reads
        # concurrently

        with compacted_log_path.open('ab') as compacted_log_file:
            read_offset = self.rfd.tell()
            k, v = self._read_kv_pair(self.rfd)
            while k:
                # not checking if k in index as it is for sure (i never remove keys from the index so far)
                la = self.hash_index[k]
                # if the record lies on disk and is the most recent one:
                if la <= self.head_offset and self.la_to_file_offset[la] == read_offset:
                    write_offset = compacted_log_file.tell()
                    self._write_kv_pair(compacted_log_file, k, v)
                    self.la_to_file_offset[la] = write_offset
                read_offset = self.rfd.tell()
                k, v = self._read_kv_pair(self.rfd)

        self.rfd.close()
        # rename the file back
        compacted_log_path.rename(compacted_log_path.with_suffix(''))
        # get a new read fd
        self.rfd = self.log_path.open('rb')
        # reset the compaction counter
        self.compaction_counter = 0

    def __sizeof__(self):
        return getsizeof(self.hash_index) + getsizeof(self.la_to_file_offset) + getsizeof(self.memory)
