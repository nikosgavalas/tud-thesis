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
    # NOTE the fence pointers can be used to organize data into compressible blocks
    def __init__(self, data_dir='./data', max_runs_per_level=3, density_factor=20, memtable_bytes_limit=1_000_000):
        self.data_dir = Path(data_dir)
        self.data_dir.mkdir(exist_ok=True)

        assert(max_runs_per_level >= 1)
        self.max_runs_per_level = max_runs_per_level
        self.density_factor = density_factor

        self.memtable = SortedDict()
        self.memtable_bytes_limit = memtable_bytes_limit
        self.memtable_bytes_count = 0

        self.wal_path = self.data_dir / 'wal'
        if self.wal_path.is_file():
            with self.wal_path.open('rb') as wal_file:
                k, v = self._read_kv_pair(wal_file)
                while k:
                    self.memtable[k] = v  # write the value to the memtable directly, no checks for amount of bytes etc.
                    k, v = self._read_kv_pair(wal_file)
        self.wal_file = self.wal_path.open('ab')

        self.levels = []

        self.metadata = {
            'runs_per_level': []
        }
        self.metadata_path = self.data_dir / 'metadata'
        self.load_metadata()

        # load filters and pointers for levels and runs
        for _ in self.metadata['runs_per_level']:
            self.levels.append([])
        for level_idx, n_runs in enumerate(self.metadata['runs_per_level']):
            for r in range(n_runs):
                with (self.data_dir / f'L{level_idx}.{r}.pointers').open('r') as pointers_file:
                    data = pointers_file.read()
                pointers = FencePointers(from_str=data)

                with (self.data_dir / f'L{level_idx}.{r}.filter').open('r') as filter_file:
                    data = filter_file.read()
                filter = BloomFilter(from_str=data)

                self.levels[level_idx].append(Run(filter, pointers))
    
    def __del__(self):
        self.wal_file.close()

    def load_metadata(self):
        if self.metadata_path.is_file():
            with self.metadata_path.open('r') as metadata_file:
                self.metadata = json.loads(metadata_file.read())

    def get(self, key: bytes):
        assert(type(key) is bytes and len(key) < MAX_KEY_LENGTH)

        if key in self.memtable:
            return self.memtable[key]

        for level_idx, level in enumerate(self.levels):
            for i, run in reversed(list(enumerate(level))):
                if key in run.filter:
                    idx = run.pointers.bisect(key) - 1  # -1 because I want the index of the item on the left
                    if idx < 0:
                        idx = 0
                    _, offset = run.pointers.peekitem(idx)
                    with (self.data_dir / f'L{level_idx}.{i}.run').open('rb') as run_file:
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
        assert(type(key) is bytes and type(value) is bytes and 0 < len(key) < MAX_KEY_LENGTH and len(value) < MAX_VALUE_LENGTH)

        self.memtable[key] = value  # NOTE maybe i should write after the flush? cause this way the limit is not a hard limit, it may be passed by up to 255 bytes
        new_bytes_count = self.memtable_bytes_count + len(key) + len(value)

        if new_bytes_count > self.memtable_bytes_limit:
            # normally I would allocate a new memtable here so that writes can continue there
            # and then give the flushing of the old memtable to a background thread
            self.flush_memtable()

            if len(self.levels[0]) > self.max_runs_per_level:  # here I don't risk index out of bounds cause flush runs before, and is guaranteed to create at least the first level
                self.merge(0)
        else:
            # write to wal
            self._write_kv_pair(self.wal_file, key, value)
            self.memtable_bytes_count = new_bytes_count

    def merge(self, level_idx: int):
        level = self.levels[level_idx]
        if level_idx + 1 >= len(self.levels):
            self.levels.append([])
        next_level = self.levels[level_idx + 1]

        fence_pointers = FencePointers(self.density_factor)
        filter = BloomFilter(sum([run.filter.est_num_items for run in level]))  # I can replace with an actual accurate count but I don't think it's worth it, it's an estimate anyway

        fds, keys, values, is_empty = [], [], [], []
        for i, _ in enumerate(level):
            fd = (self.data_dir / f'L{level_idx}.{i}.run').open('rb')
            fds.append(fd)
            k, v = self._read_kv_pair(fd)
            keys.append(k)
            values.append(v)
            is_empty.append(True if not k else False)
        
        with (self.data_dir / f'L{level_idx + 1}.{len(next_level)}.run' ).open('wb') as run_file:
            while not all(is_empty):
                argmin_key = len(level) - 1
                # correctly initialize the argmin_key (cause empty key b'' would make it instantly the argmin_key in the next for loop which we don't want)
                for i in reversed(range(len(level))):
                    if not is_empty[i]:
                        argmin_key = i
                        break
                for i in reversed(range(len(level))):
                    if not is_empty[i] and keys[i] < keys[argmin_key]:
                        argmin_key = i

                if values[argmin_key]:  # assumption: empty value == deleted item, so if empty I am writing nothing
                    fence_pointers.add(keys[argmin_key], run_file.tell())
                    self._write_kv_pair(run_file, keys[argmin_key], values[argmin_key])
                    filter.add(keys[argmin_key])

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

        with (self.data_dir / f'L{level_idx + 1}.{len(next_level)}.pointers').open('w') as pointers_file:
            pointers_file.write(fence_pointers.serialize())

        with (self.data_dir / f'L{level_idx + 1}.{len(next_level)}.filter').open('w') as filter_file:
            filter_file.write(filter.serialize())

        # remove the files after successfully merging.
        for i, _ in enumerate(level):
            (self.data_dir / f'L{level_idx}.{i}.run').unlink()
            (self.data_dir / f'L{level_idx}.{i}.pointers').unlink()
            (self.data_dir / f'L{level_idx}.{i}.filter').unlink()

        # empty the runs array
        level.clear()

        # append new run
        next_level.append(Run(filter, fence_pointers))

        # update metadata
        self.metadata['runs_per_level'] = [len(l) for l in self.levels]
        self.save_metadata()

        # cascade the merging recursively
        if len(next_level) > self.max_runs_per_level:
            self.merge(level_idx + 1)

    def flush_memtable(self):
        fence_pointers = FencePointers(self.density_factor)
        filter = BloomFilter(len(self.memtable))

        if not self.levels:
            self.levels.append([])

        flush_level = 0  # always flush at first level
        n_runs = len(self.levels[0])

        with (self.data_dir / f'L{flush_level}.{n_runs}.run').open('wb') as run_file:
            while self.memtable:
                k, v = self.memtable.popitem(0)
                fence_pointers.add(k, run_file.tell())
                self._write_kv_pair(run_file, k, v)
                filter.add(k)
 
        self.memtable_bytes_count = 0

        self.levels[flush_level].append(Run(filter, fence_pointers))

        with (self.data_dir / f'L{flush_level}.{n_runs}.pointers').open('w') as pointers_file:
            pointers_file.write(fence_pointers.serialize())

        with (self.data_dir / f'L{flush_level}.{n_runs}.filter').open('w') as filter_file:
            filter_file.write(filter.serialize())

        self.metadata['runs_per_level'] = [len(l) for l in self.levels]
        self.save_metadata()

        # reset WAL
        self.wal_file.close()
        self.wal_file = self.wal_path.open('wb')

    def save_metadata(self):
        with self.metadata_path.open('w') as metadata_file:
            metadata_file.write(json.dumps(self.metadata))
