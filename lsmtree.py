'''
LSM Tree with size-tiered compaction (write-optimized)
'''

import struct
import os.path

from sortedcontainers import SortedDict

from bloom import BloomFilter


EMPTY_VALUE = b''
MAX_KEY_LENGTH = 256
MAX_VALUE_LENGTH = 256
DATA_DIR = './data/'
MAX_RUNS_PER_LEVEL = 2
FENCE_POINTERS_SKIPS = 3


# class Level:

#     def __init__(self, level_num):
#         self.level_num = level_num
#         self.runs = []
#         self.latest_run_ptr = 0
#         # TODO self.pointers = None
#         # TODO self.bloom = None


class LSMTree:

    def __init__(self):
        # TODO directory checks
        # TODO load data from previous runs (WAL and data dir)
        self.memtable = SortedDict()
        self.memtable_bytes_limit = 20
        self.memtable_bytes_count = 0
        self.num_runs = 0

    def get(self, key: bytes):
        assert(type(key) is bytes and len(key) < MAX_KEY_LENGTH)

        return self.memtable[key] if key in self.memtable else EMPTY_VALUE
    
    def set(self, key: bytes, value: bytes = EMPTY_VALUE):
        assert(type(key) is bytes and type(value) is bytes and len(key) < MAX_KEY_LENGTH and len(value) < MAX_VALUE_LENGTH)

        self.memtable[key] = value
        self.memtable_bytes_count += len(value)

        if self.memtable_bytes_count > self.memtable_bytes_limit:
            # normally I would allocate a new memtable here so that writes can continue there
            # and then give the flushing of the old memtable to a background thread
            self.flush_memtable()
            # TODO reset WAL
    
    def get_filename(self, level, run):
        return os.path.join(DATA_DIR, f'L{level}.{run}')

    def flush_memtable(self):

        with open(self.get_filename(0, self.num_runs), 'wb') as f:

            fence_pointers = SortedDict()  # TODO instead of allocating a new SortedDict, mutate the same one used for the memtable
            bloom_filter = BloomFilter()
            kv_cnt = 0

            while self.memtable:
                k, v = self.memtable.popitem(0)
                f.write(struct.pack('<BB', len(k), len(v)))  # https://docs.python.org/3/library/struct.html#format-characters
                f.write(k)
                f.write(v)

                if kv_cnt % FENCE_POINTERS_SKIPS == 0:
                    fence_pointers[k] = f.tell()
                kv_cnt += 1

            fence_pointers_offset = f.tell()
            for k, v in fence_pointers.items():
                f.write(struct.pack('<B', len(k)))
                f.write(k)
                f.write(struct.pack('<I', fence_pointers[k]))

            bloom_offset = f.tell()
            # TODO serialize the bitarray of the bloomfilter f.write(json.dumps())

            f.write(struct.pack('<I', fence_pointers_offset))
            f.write(struct.pack('<I', bloom_offset))

        self.num_runs += 1
