'''
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
'''

from typing import Optional
import struct

from kevo.engines.kvstore import KVStore, EMPTY, MAX_KEY_LENGTH, MAX_VALUE_LENGTH
from kevo.common.hashindex import HashIndex
from kevo.common.ringbuffer import RingBuffer
from kevo.replication import Replica


class HybridLog(KVStore):
    name = 'HybridLog'
    def __init__(self, data_dir='./data', max_key_len=4, max_value_len=4, mem_segment_len=2**20, ro_lag_interval=2**10,
        flush_interval=(4 * 2**10), hash_index='dict', compaction_interval=0, replica: Optional[Replica] = None):
        self.type = 'hybridlog'
        super().__init__(data_dir, replica=replica)

        assert 0 < max_key_len <= MAX_KEY_LENGTH
        assert 0 < max_value_len <= MAX_VALUE_LENGTH
        assert ro_lag_interval > 0
        assert flush_interval > 0
        assert compaction_interval >= 0 # if compaction interval is 0, compaction is disabled
        assert mem_segment_len >= ro_lag_interval + flush_interval
        assert hash_index in ['dict', 'native'], 'hash_index parameter must be either "dict" or "native"'

        self.max_key_len = max_key_len
        self.max_value_len = max_value_len
        self.ro_lag_interval = ro_lag_interval
        self.flush_interval = flush_interval
        self.compaction_interval = compaction_interval  # in number of flushes
        self.compaction_enabled = compaction_interval > 0

        if hash_index == 'native':
            self.hash_index = HashIndex(n_buckets_power=4, key_len_bits=self.max_key_len*8, value_len_bits=self.max_value_len*8)
        else:
            self.hash_index = {}

        self.head_offset = 0  # LA > head_offset is in mem
        self.ro_offset = 0    # in LA > ro_offset we have the mutable region
        self.tail_offset = 0  # points to the tail of the log, the next free available slot in memory

        self.compaction_counter = 0

        self.log_path = self.data_dir / 'log'
        if self.log_path.is_file():
            with self.log_path.open('rb') as log_file:
                offset = log_file.tell()
                k, _ = self._read_kv_pair(log_file)
                self.head_offset += 1
                while k:
                    self.hash_index[k] = self.file_offset_to_LA(offset)
                    offset = log_file.tell()
                    k, _ = self._read_kv_pair(log_file)
                    self.head_offset += 1
            self.ro_offset = self.head_offset
            self.tail_offset = self.ro_offset

        self.memory = RingBuffer(mem_segment_len)

    def close(self):
        self.flush(self.tail_offset)  # flush everything
        self.save_metadata()
        if self.replica:
            self.replica.put(self.log_path.name)
            self.replica.put(self.metadata_path.name)

    def _read_kv_pair(self, fd, file_offset=None):
            if not file_offset:
                file_offset = fd.tell()
            fd.seek(file_offset)
            first_byte = fd.read(1)
            if not first_byte:
                return EMPTY, EMPTY
            k_len = struct.unpack('<B', first_byte)[0]
            k = fd.read(k_len)
            fd.seek(file_offset + self.max_key_len + 1)  # +1 for the encoding
            v_len = struct.unpack('<B', fd.read(1))[0]
            v = fd.read(v_len)
            fd.seek(file_offset + self.max_key_len + self.max_value_len + 2) # +2 for encoding
            return k, v

    def _write_kv_pair(self, fd, key, value):
        fd.write(struct.pack('<B', len(key)))
        fd.write(key + b'\x00' * (self.max_key_len - len(key)))  # key padded with \x00's
        fd.write(struct.pack('<B', len(value)))
        fd.write(value + b'\x00' * (self.max_value_len - len(value)))  # same for value

    def __getitem__(self, key):
        return self.get(key)

    def __setitem__(self, key, value):
        return self.set(key, value)

    def get(self, key: bytes):
        assert type(key) is bytes and 0 < len(key) <= self.max_key_len

        if key not in self.hash_index:
            return EMPTY

        offset = self.hash_index[key]
        if offset > self.head_offset:
            _, v = self.memory[offset]
            return v

        file_offset = self.LA_to_file_offset(offset)
        with self.log_path.open('rb') as log_file:
            _, v = self._read_kv_pair(log_file, file_offset)
            return v

    def set(self, key: bytes, value: bytes = EMPTY):
        assert type(key) is bytes and type(value) is bytes and 0 < len(key) <= self.max_key_len and len(value) <= self.max_value_len

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
                if self.compaction_enabled:
                    if self.hash_index[key] != self.head_offset:  # if is not the most recent record, drop it, like we do with the file compaction
                        continue
                    self.hash_index[key] = self.file_offset_to_LA(write_offset)
                self._write_kv_pair(log_file, key, value)

        if self.replica and not self.compaction_enabled:
            self.replica.put(self.log_path.name)

        if self.compaction_enabled:
            self.compaction_counter += 1
            if self.compaction_counter == self.compaction_interval:
                self.compaction()
                # if compaction is enabled, wait to sync to replica store *after* the compaction
                if self.replica and self.compaction_enabled:
                    self.replica.put(self.log_path.name)

    def compaction(self):
        compacted_log_path = self.log_path.with_suffix('.tmp')
        #new_hash_index = {} # TODO i can copy the index here and keep the old one for as long as the compaction is running to enable reads concurrently

        with self.log_path.open('rb') as log_file, compacted_log_path.open('ab') as compacted_log_file:
            read_offset = log_file.tell()
            k, v = self._read_kv_pair(log_file)
            while k:
                if self.file_offset_to_LA(read_offset) == self.hash_index[k]: # not checking if k in index as it is for sure (i never remove keys from the index so far)
                    write_offset = compacted_log_file.tell()
                    self._write_kv_pair(compacted_log_file, k, v)
                    self.hash_index[k] = self.file_offset_to_LA(write_offset) #new_hash_index[k] = write_offset # see TODO above
                read_offset = log_file.tell()
                k, v = self._read_kv_pair(log_file)

        # rename the file back
        compacted_log_path.rename(compacted_log_path.with_suffix(''))
        # swap the index with the new one
        #self.hash_index = new_hash_index # see TODO above
        # reset the compaction counter
        self.compaction_counter = 0

    def file_offset_to_LA(self, file_offset):
        return (file_offset // (self.max_key_len + self.max_value_len + 2)) + 1

    def LA_to_file_offset(self, la):
        return (la - 1) * (self.max_key_len + self.max_value_len + 2)  # +2 for the encoding of the lengths of keys and values
