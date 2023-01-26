'''
LSM Tree with size-tiered compaction (write-optimized)
TODO: consider using mmap for the files
'''

import struct
import json
from pathlib import Path

from sortedcontainers import SortedDict

from bloom import BloomFilter
from fence import FencePointers


# do not change these
EMPTY = b''
MAX_KEY_LENGTH = 256
MAX_VALUE_LENGTH = 256

# changable confs
DATA_DIR = './data/'
MAX_RUNS_PER_LEVEL = 3
FENCE_POINTERS_SKIPS = 3  # NOTE this can be used to organize data into compressable blocks


class Run:
    
    def __init__(self, pointers, filter):
        self.filter = filter
        self.pointers = pointers


class LSMTree:

    def __init__(self):
        # TODO directory checks
        # TODO load data from WAL
        self.memtable = SortedDict()
        self.memtable_bytes_limit = 10
        self.memtable_bytes_count = 0

        self.runs = []
        
        self.load_metadata()

        # load filters and pointers for levels and runs
        for i in range(self.metadata['num_runs']):
            with open(f'data/L0.{i}.pointers', 'r') as pointers_file:
                data = pointers_file.read()
            pointers = FencePointers()
            pointers.deserialize(data)

            with open(f'data/L0.{i}.filter', 'r') as filter_file:
                data = filter_file.read()
            filter = BloomFilter()
            filter.deserialize(data)

            self.runs.append(Run(pointers, filter))

    def load_metadata(self):
        metadata_path = Path('./data/metadata')
        if metadata_path.is_file():
            with metadata_path.open('r') as metadata_file:
                self.metadata = json.loads(metadata_file.read())
        else:
            self.metadata = {
                'num_runs': 0
            }

    def get(self, key: bytes):
        assert(type(key) is bytes and len(key) < MAX_KEY_LENGTH)

        if key in self.memtable:
            return self.memtable[key]
        
        for i, run in reversed(list(enumerate(self.runs))):  # TODO for level in levels, ... for run in runs:... TODO remove enumerate, add the file in the Runs class?
            if key in run.filter:
                idx = run.pointers.bisect(key) - 1  # -1 because I want the index of the item on the left
                if idx < 0:
                    return EMPTY
                _, offset = run.pointers.peekitem(idx)
                with open(f'data/L0.{i}.run', 'rb') as run_file:
                    run_file.seek(offset)
                    for i in range(FENCE_POINTERS_SKIPS):
                        read_key, read_value = self._read_kv_pair(run_file)
                        if read_key == key:
                            return read_value

        return EMPTY
    
    def _read_kv_pair(self, fd):
        first_byte = fd.read(1)
        if not first_byte:
            return EMPTY, EMPTY
        key_len = struct.unpack('<B', first_byte)[0]
        key = fd.read(key_len)
        val_len = struct.unpack('<B', fd.read(1))[0]
        value = fd.read(val_len)
        return key, value
    
    def _write_kv_pair(self, fd, key, value):
        fd.write(struct.pack('<B', len(key)))
        fd.write(key)
        fd.write(struct.pack('<B', len(value)))
        fd.write(value)
    
    def set(self, key: bytes, value: bytes = EMPTY):
        assert(type(key) is bytes and type(value) is bytes and len(key) < MAX_KEY_LENGTH and len(value) < MAX_VALUE_LENGTH)

        self.memtable[key] = value
        self.memtable_bytes_count += len(key) + len(value)

        if self.memtable_bytes_count > self.memtable_bytes_limit:
            # normally I would allocate a new memtable here so that writes can continue there
            # and then give the flushing of the old memtable to a background thread

            self.flush_memtable()  # TODO make sure to flush at the end of each program somehow? If I had a server or REPL this would be easy

            if len(self.runs) == MAX_RUNS_PER_LEVEL:
                self.merge()
                
            # TODO reset WAL
    
    def merge(self):
        fds = []
        keys = []
        values = []
        for i in range(MAX_RUNS_PER_LEVEL):
            fd = open(f'data/L0.{i}.run', 'rb')
            fds.append(fd)
            k, v = self._read_kv_pair(fd)
            keys.append(k)
            values.append(v)
        
        # TODO does this assertion make sense?
        assert(b'' not in keys)

        is_empty = [False for _ in range(MAX_RUNS_PER_LEVEL)]  # assuming no empty runs till here (see previous assertion)
        with open('data/L1.0.run', 'wb') as run_file:
            while not all(is_empty):
                argmin_key = MAX_RUNS_PER_LEVEL - 1
                # correctly initialize the argmin_key
                for i in reversed(range(MAX_RUNS_PER_LEVEL)):
                    if not is_empty[i]:
                        argmin_key = i
                        break
                for i in reversed(range(MAX_RUNS_PER_LEVEL)):
                    if not is_empty[i] and keys[i] < keys[argmin_key]:
                        argmin_key = i

                if values[argmin_key]:  # assumption: empty value == deleted item, so if empty I am writing nothing
                    self._write_kv_pair(run_file, keys[argmin_key], values[argmin_key])
                
                written_key = keys[argmin_key]
                
                # read next kv pair 
                keys[argmin_key], values[argmin_key] = self._read_kv_pair(fds[argmin_key])
                if not keys[argmin_key]:
                    is_empty[argmin_key] = True

                # skip duplicates
                for i in reversed(range(argmin_key + 1)):
                    while not is_empty[i] and written_key == keys[i]:  # if it's the same key, read one more pair to skip it
                        keys[i], values[i] = self._read_kv_pair(fds[i])
                        if not keys[i]:
                            is_empty[i] = True

    def flush_memtable(self):

        with open(f'data/L0.{self.metadata["num_runs"]}.run', 'wb') as run_file:
            kv_count = 0
            fence_pointers = FencePointers()
            filter = BloomFilter(len(self.memtable))
            
            while self.memtable:
                k, v = self.memtable.popitem(0)

                if kv_count % FENCE_POINTERS_SKIPS == 0:
                    fence_pointers.add(k, run_file.tell())
                kv_count += 1

                self._write_kv_pair(run_file, k, v)

                filter.add(k)
            
            self.runs.append(Run(fence_pointers, filter))
        
        with open(f'data/L0.{self.metadata["num_runs"]}.pointers', 'w') as pointers_file:
            pointers_file.write(fence_pointers.serialize())
        
        with open(f'data/L0.{self.metadata["num_runs"]}.filter', 'w') as filter_file:
            filter_file.write(filter.serialize())
        
        self.metadata['num_runs'] += 1
        self.save_metadata()
 
    def save_metadata(self):
        with open('data/metadata', 'w') as metadata_file:
            metadata_file.write(json.dumps(self.metadata))
