from collections import namedtuple
from sys import getsizeof
from typing import Optional
from io import FileIO
import struct

from sortedcontainers import SortedDict

from kevo.common import BloomFilter, FencePointers
from kevo.engines.kvstore import KVStore, discover_run_files
from kevo.remote import Remote

Run = namedtuple('Run', ['filter', 'pointers', 'nr_records'])


def append_indices(file_descriptor, fence_pointers, bloom_filter, nr_records):
    pointers_offset = file_descriptor.tell()
    file_descriptor.write(fence_pointers.serialize().encode())
    bloom_offset = file_descriptor.tell()
    file_descriptor.write(bloom_filter.serialize().encode())
    # pack two 8 byte unsigned ints for the offsets of the pointers and the bloom filter
    file_descriptor.write(struct.pack('<QQQ', pointers_offset, bloom_offset, nr_records))


class LSMTree(KVStore):
    name = 'LSMTree'

    def __init__(self,
                 data_dir='./data',
                 max_key_len=255,
                 max_value_len=255,
                 max_runs_per_level=3,
                 density_factor=20,
                 memtable_bytes_limit=1_000_000,
                 remote: Optional[Remote] = None):
        self.type = 'lsmtree'
        super().__init__(data_dir=data_dir, max_key_len=max_key_len, max_value_len=max_value_len, remote=remote)

        assert max_runs_per_level > 1
        assert density_factor > 0
        assert memtable_bytes_limit > 0

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
                    # write the value to the memtable directly, no checks for amount of bytes etc.
                    self.memtable[k] = v
                    k, v = self._read_kv_pair(wal_file)
        self.wal_file = self.wal_path.open('ab')

        self.levels: list[list[Run]] = []
        self.rfds: list[list[FileIO]] = []

        # global version is used to attach a version number to every file flushed and merged.
        # this is used in snapshotting in the delta maps
        self.global_version = 0

        if self.remote:
            # restore calls rebuild_indices, so this way we avoid rebuilding twice
            self.restore()
        else:
            self.rebuild_indices()

    def rebuild_indices(self):
        # TODO set the global version to the max of the discovered ones
        self.levels.clear()
        self.rfds.clear()

        # do file discovery
        runs_discovered = self.discover_runs()
        if not runs_discovered:
            return

        # load filters and pointers for levels and runs
        for level_idx, run_idx, version in sorted(runs_discovered):
            with (self.data_dir / f'L{level_idx}.{run_idx}.{version}.run').open('rb') as run_file:
                # fetch the last 24 bytes (=3*8)
                run_file.seek(-24, 2)
                bloom_end_offset = run_file.tell()
                offsets = run_file.read()
                pointers_offset, bloom_offset, nr_records = struct.unpack('<QQQ', offsets)
                run_file.seek(pointers_offset)
                pointers = FencePointers(from_str=run_file.read(bloom_offset - pointers_offset).decode())
                run_file.seek(bloom_offset)
                bloom_filter = BloomFilter(from_str=run_file.read(bloom_end_offset - bloom_offset).decode())

            while level_idx >= len(self.levels):
                self.levels.append([])
            self.levels[level_idx].append(Run(bloom_filter, pointers, nr_records))

            while level_idx >= len(self.rfds):
                self.rfds.append([])
            self.rfds[level_idx].append((self.data_dir / f'L{level_idx}.{run_idx}.{version}.run').open('rb'))

    def close(self):
        self.wal_file.close()
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

        if key in self.memtable:
            return self.memtable[key]

        for level_idx, level in enumerate(self.levels):
            for i, run in reversed(list(enumerate(level))):
                if key in run.filter:
                    # bisect -1 because I want the index of the item on the left
                    idx = run.pointers.bisect(key) - 1
                    if idx < 0:
                        idx = 0
                    _, offset = run.pointers.peekitem(idx)
                    run_file = self.rfds[level_idx][i]
                    run_file.seek(offset)
                    for _ in range(run.pointers.density_factor):
                        read_key, read_value = self._read_kv_pair(run_file)
                        if read_key == key:
                            return read_value

        return KVStore.EMPTY

    def set(self, key: bytes, value: bytes = KVStore.EMPTY):
        assert type(key) is bytes and type(value) is bytes
        assert 0 < len(key) <= self.max_key_len and len(value) <= self.max_value_len

        if key not in self.memtable:
            self.memtable_bytes_count += len(key) + len(value)

        # NOTE maybe i should write after the flush?
        # cause this way the limit is not a hard limit, it may be passed by up to 255 bytes
        self.memtable[key] = value

        if self.memtable_bytes_count > self.memtable_bytes_limit:
            # normally I would allocate a new memtable here so that writes can continue there
            # and then give the flushing of the old memtable to a background thread
            self.flush()
        else:
            # write to wal
            self._write_kv_pair(self.wal_file, key, value)

    def merge(self, level_idx: int):
        level = self.levels[level_idx]
        if level_idx + 1 >= len(self.levels):
            self.levels.append([])
        next_level = self.levels[level_idx + 1]

        fence_pointers = FencePointers(self.density_factor)
        # I can replace with an actual accurate count but I don't think it's worth it, it's an estimate anyway
        bloom_filter = BloomFilter(sum([run.filter.est_num_items for run in level]))

        # use counters and the nr_records in the file to know when the key-values part has been parsed
        # the alternative would be to use tell() all the time which is slow
        fds, keys, values, is_empty, counters, nr_records_in_run = [], [], [], [], [], []
        for i, _ in enumerate(level):
            fd = self.rfds[level_idx][i]
            fd.seek(0)
            fds.append(fd)
            k, v = self._read_kv_pair(fd)
            keys.append(k)
            values.append(v)
            is_empty.append(True if not k else False)
            counters.append(0 if not k else 1)
            nr_records_in_run.append(self.levels[level_idx][i].nr_records)

        nr_records = 0
        with (self.data_dir / f'L{level_idx + 1}.{len(next_level)}.{self.global_version}.run').open('wb') as run_file:
            while not all(is_empty):
                argmin_key = len(level) - 1
                # correctly initialize the argmin_key (cause empty key b'' would make it instantly the argmin_key in
                # the next for loop which we don't want)
                for i in reversed(range(len(level))):
                    if not is_empty[i]:
                        argmin_key = i
                        break
                for i in reversed(range(len(level))):
                    if not is_empty[i] and keys[i] < keys[argmin_key]:
                        argmin_key = i

                # assumption: empty value == deleted item, so if empty I am writing nothing
                if values[argmin_key]:
                    fence_pointers.add(keys[argmin_key], run_file.tell())
                    self._write_kv_pair(run_file, keys[argmin_key], values[argmin_key])
                    bloom_filter.add(keys[argmin_key])
                    nr_records += 1

                written_key = keys[argmin_key]

                # read next kv pair
                if not is_empty[argmin_key]:
                    if counters[argmin_key] >= nr_records_in_run[argmin_key]:
                        is_empty[argmin_key] = True
                    else:
                        keys[argmin_key], values[argmin_key] = self._read_kv_pair(fds[argmin_key])
                        counters[argmin_key] += 1

                # skip duplicates
                # + 1 cause inclusive range
                for i in reversed(range(argmin_key + 1)):
                    # if it's the same key, read one more pair to skip it
                    while not is_empty[i] and written_key == keys[i]:
                        if counters[i] >= nr_records_in_run[i]:
                            is_empty[i] = True
                        else:
                            keys[i], values[i] = self._read_kv_pair(fds[i])
                            counters[i] += 1

            append_indices(run_file, fence_pointers, bloom_filter, nr_records)

        if level_idx + 1 >= len(self.rfds):
            self.rfds.append([])
        self.rfds[level_idx + 1].append((self.data_dir / f'L{level_idx + 1}.{len(next_level)}.{self.global_version}.run').open('rb'))
        for fd in fds:
            fd.close()
        self.rfds[level_idx].clear()

        self.global_version += 1

        # remove the files after successfully merging.
        for file in self.data_dir.glob(f'L{level_idx}.*.*.run'):
            file.unlink()

        # empty the runs array
        level.clear()

        # append new run
        next_level.append(Run(bloom_filter, fence_pointers, nr_records))

        # cascade the merging recursively
        if len(next_level) >= self.max_runs_per_level:
            self.merge(level_idx + 1)

    def flush(self):
        if len(self.memtable) == 0:
            return
        fence_pointers = FencePointers(self.density_factor)
        bloom_filter = BloomFilter(len(self.memtable))

        flush_level = 0  # always flush at first level

        if flush_level >= len(self.levels):
            self.levels.append([])
        if flush_level >= len(self.rfds):
            self.rfds.append([])

        n_runs = len(self.levels[0])

        nr_records = 0
        with (self.data_dir / f'L{flush_level}.{n_runs}.{self.global_version}.run').open('wb') as run_file:
            while self.memtable:
                k, v = self.memtable.popitem(0)
                fence_pointers.add(k, run_file.tell())
                self._write_kv_pair(run_file, k, v)
                bloom_filter.add(k)
                nr_records += 1
            append_indices(run_file, fence_pointers, bloom_filter, nr_records)

        self.memtable_bytes_count = 0

        self.levels[flush_level].append(Run(bloom_filter, fence_pointers, nr_records))
        self.rfds[flush_level].append((self.data_dir / f'L{flush_level}.{n_runs}.{self.global_version}.run').open('rb'))

        # reset WAL
        self.wal_file.close()
        self.wal_file = self.wal_path.open('wb')

        # trigger merge if exceeding the runs per level
        if len(self.levels[flush_level]) >= self.max_runs_per_level:
            self.merge(flush_level)

    def snapshot(self, id: int):
        self.flush()
        if self.remote:
            runs = discover_run_files(self.data_dir)
            self.remote.push_deltas(runs, id)

    def restore(self, version=None):
        # flush first to empty the memtable
        self.flush()
        if self.remote:
            self.global_version = self.remote.restore(version=version)
            # close open file descriptors first
            for rfds in self.rfds:
                for rfd in rfds:
                    rfd.close()
            self.rebuild_indices()

    def __sizeof__(self):
        memtable_size = sum((getsizeof(k) + getsizeof(v) for k, v in self.memtable.items()))
        bloom_filters_size = sum((getsizeof(run.filter) for level in self.levels for run in level))
        fence_pointers_size = sum((getsizeof(run.pointers) for level in self.levels for run in level))
        return memtable_size + bloom_filters_size + fence_pointers_size
