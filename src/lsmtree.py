'''
LSM Tree with size-tiered compaction (write-optimized)
TODO: consider using mmap for the files
'''

import struct
import json
from pathlib import Path

from sortedcontainers import SortedDict

from src.bloom import BloomFilter
from src.fence import FencePointers


# do not change these
EMPTY = b''
MAX_KEY_LENGTH = 256
MAX_VALUE_LENGTH = 256


class Run:
    def __init__(self, filter, pointers):
        self.filter = filter
        self.pointers = pointers


class LSMTree:
    # NOTE the fence pointers can be used to organize data into compressable blocks
    def __init__(self, data_dir='./data', max_runs_per_level=3, density_factor=20, memtable_bytes_limit=1000000):
        # TODO load data from WAL
        self.data_dir = Path(data_dir)
        self.data_dir.mkdir(exist_ok=True)

        self.max_runs_per_level = max_runs_per_level
        self.density_factor = density_factor

        self.memtable = SortedDict()
        self.memtable_bytes_limit = memtable_bytes_limit
        self.memtable_bytes_count = 0

        self.runs = []

        self.metadata = {
            'num_runs': 0
        }
        self.metadata_path = self.data_dir / 'meta'
        self.load_metadata()

        # load filters and pointers for levels and runs
        # TODO loading only first level for now
        for i in range(self.metadata['num_runs']):
            with (self.data_dir / f'L0.{i}.pointers').open('r') as pointers_file:
                data = pointers_file.read()
            pointers = FencePointers(from_str=data)

            with (self.data_dir / f'L0.{i}.filter').open('r') as filter_file:
                data = filter_file.read()
            filter = BloomFilter(from_str=data)

            self.runs.append(Run(filter, pointers))

    def load_metadata(self):
        if self.metadata_path.is_file():
            with self.metadata_path.open('r') as metadata_file:
                self.metadata = json.loads(metadata_file.read())

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
                with (self.data_dir / f'L0.{i}.run').open('rb') as run_file:
                    run_file.seek(offset)
                    for i in range(run.pointers.density_factor):
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
        assert(type(key) is bytes and type(value) is bytes and len(key) < MAX_KEY_LENGTH and len(value) < MAX_VALUE_LENGTH) # TODO assert that key and value are non empty?

        self.memtable[key] = value  # NOTE maybe i should write after the flush? cause this way the limit is not a hard limit, it may be passed by up to 255 bytes
        new_bytes_count = self.memtable_bytes_count + len(key) + len(value)

        if new_bytes_count > self.memtable_bytes_limit:
            # normally I would allocate a new memtable here so that writes can continue there
            # and then give the flushing of the old memtable to a background thread

            self.flush_memtable()  # TODO make sure to flush at the end of each program somehow? If I had a server or REPL this would be easy - no need now that i think about it cause of the WAL

            if len(self.runs) == self.max_runs_per_level:
                self.merge()

            # TODO reset WAL
        else:
            # TODO write to WAL
            self.memtable_bytes_count = new_bytes_count
    
    def merge(self):
        # TODO create bloom filter and fence pointers for this!! see self.flush_memtable()
        fds = []
        keys = []
        values = []
        for i in range(self.max_runs_per_level):
            fd = (self.data_dir / f'L0.{i}.run').open('rb')
            fds.append(fd)
            k, v = self._read_kv_pair(fd)
            keys.append(k)
            values.append(v)
        
        # TODO does this assertion make sense?
        assert(b'' not in keys)

        is_empty = [False for _ in range(self.max_runs_per_level)]  # assuming no empty runs till here (see previous assertion)
        with (self.data_dir / 'L1.0.run' ).open('wb') as run_file:
            while not all(is_empty):
                argmin_key = self.max_runs_per_level - 1
                # correctly initialize the argmin_key (cause empty key b'' would make it instantly the argmin_key in the next for loop which we don't want)
                for i in reversed(range(self.max_runs_per_level)):
                    if not is_empty[i]:
                        argmin_key = i
                        break
                for i in reversed(range(self.max_runs_per_level)):
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
                for i in reversed(range(argmin_key + 1)): # + 1 cause inclusive range
                    while not is_empty[i] and written_key == keys[i]:  # if it's the same key, read one more pair to skip it
                        keys[i], values[i] = self._read_kv_pair(fds[i])
                        if not keys[i]:
                            is_empty[i] = True

        for fd in fds:
            fd.close()
            # TODO empty the runs array
            # TODO remove the files too after successfully merging.

    def flush_memtable(self):
        with (self.data_dir / f'L0.{self.metadata["num_runs"]}.run').open('wb') as run_file:
            fence_pointers = FencePointers(self.density_factor)
            filter = BloomFilter(len(self.memtable))

            while self.memtable:
                k, v = self.memtable.popitem(0)
                fence_pointers.add(k, run_file.tell())
                self._write_kv_pair(run_file, k, v)
                filter.add(k)
 
            self.memtable_bytes_count = 0

            self.runs.append(Run(filter, fence_pointers))

        with (self.data_dir / f'L0.{self.metadata["num_runs"]}.pointers').open('w') as pointers_file:
            pointers_file.write(fence_pointers.serialize())

        with (self.data_dir / f'L0.{self.metadata["num_runs"]}.filter').open('w') as filter_file:
            filter_file.write(filter.serialize())

        self.metadata['num_runs'] += 1
        self.save_metadata()

    def save_metadata(self):
        with self.metadata_path.open('w') as metadata_file:
            metadata_file.write(json.dumps(self.metadata))
