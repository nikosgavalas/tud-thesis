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
'''

import struct

from src.kvstore import KVStore, EMPTY, MAX_KEY_LENGTH, MAX_VALUE_LENGTH


class IndexedLog(KVStore):
    def __init__(self, data_dir='./data', max_key_len=4, max_value_len=4, mem_segment_len=1_048_576, ro_lag_interval=1_024, flush_interval=4_096):  # 1Mi, 1Ki, 4Ki
        self.type = 'indexedlog'
        super().__init__(data_dir)

        # TODO if there are files in the directory, rebuild the index from them (write compaction first)

        assert 0 < max_key_len <= MAX_KEY_LENGTH
        assert 0 < max_value_len <= MAX_VALUE_LENGTH
        assert ro_lag_interval > 0
        assert flush_interval > 0
        assert mem_segment_len >= ro_lag_interval + flush_interval

        self.max_key_len = max_key_len
        self.max_value_len = max_value_len
        self.mem_segment_len = mem_segment_len
        self.ro_lag_interval = ro_lag_interval
        self.flush_interval = flush_interval

        self.hash_index = {}  # TODO write this index the same way it's described in the paper (now using just a simple dict)
        self.head_offset = 0
        self.ro_offset = 0
        self.tail_offset = 0

        self.log_path = self.data_dir / 'log'
        self.log_file_idx = 0

        self.mem_segment = [None] * self.mem_segment_len  # preallocate the circular buffer
        self.mem_start_idx = 0
        self.mem_end_idx = 0

    def __del__(self):
        self.close()

    def close(self):
        self.flush(self.tail_offset)  # flush everything

    def get(self, key: bytes):
        assert type(key) is bytes and 0 < len(key) <= self.max_key_len

        if key not in self.hash_index:
            return EMPTY

        offset = self.hash_index[key]
        if offset >= self.head_offset:
            _, v = self.mem_segment[offset % self.mem_segment_len]
            return v

        file_offset = offset * (self.max_key_len + self.max_value_len + 2)  # +2 for the encoding of the lengths of keys and values
        with self.log_path.open('rb') as log_file:
            log_file.seek(file_offset)
            k_len = struct.unpack('<B', log_file.read(1))[0]
            k = log_file.read(k_len)
            assert k == key
            log_file.seek(file_offset + self.max_key_len + 1)
            v_len = struct.unpack('<B', log_file.read(1))[0]
            v = log_file.read(v_len)
            return v

    def set(self, key: bytes, value: bytes = EMPTY):
        assert type(key) is bytes and type(value) is bytes and 0 < len(key) <= self.max_key_len and len(value) <= self.max_value_len

        # here I am supposed to check the offsets, and apply copy-on-update if the key already exists before the ro_offset,
        # but I won't do that as I'll be writing the key-value pair directly. There's no point in copying something that
        # will be immediately updated to a new value.

        self.mem_start_idx = self.tail_offset % self.mem_segment_len
        if self.mem_start_idx == self.mem_end_idx:  # if the buffer is full, flush
            self.flush(self.ro_offset)  # TODO not sure about this part

        self.hash_index[key] = self.tail_offset

        self.mem_segment[self.mem_start_idx] = (key, value)

        self.tail_offset += 1

        if self.tail_offset - self.ro_offset > self.ro_lag_interval:
            self.ro_offset += 1

        if self.ro_offset - self.head_offset > self.flush_interval:
            self.flush(self.ro_offset)

    def flush(self, offset: int):
        with self.log_path.open('ab') as log_file:
            while self.head_offset < offset:
                key, value = self.mem_segment[self.head_offset % self.mem_segment_len]

                log_file.write(struct.pack('<B', len(key)))
                log_file.write(key + b'\x00' * (self.max_key_len - len(key)))  # key padded with \x00's
                log_file.write(struct.pack('<B', len(value)))
                log_file.write(value + b'\x00' * (self.max_value_len - len(value)))  # same for value

                self.head_offset += 1

            self.mem_end_idx = self.head_offset
