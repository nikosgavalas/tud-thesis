"""
LSM Tree with size-tiered compaction (write-optimized)
"""

from collections import namedtuple
from sys import getsizeof
from typing import Optional
from io import FileIO

from sortedcontainers import SortedDict

from kevo.common import BloomFilter, FencePointers
from kevo.engines.kvstore import KVStore
from kevo.replication import Replica

Run = namedtuple('Run', ['filter', 'pointers'])


class LSMTree(KVStore):
    name = 'LSMTree'

    def __init__(self,
                 data_dir='./data',
                 max_key_len=255,
                 max_value_len=255,
                 max_runs_per_level=3,
                 density_factor=20,
                 memtable_bytes_limit=1_000_000,
                 replica: Optional[Replica] = None):
        self.type = 'lsmtree'
        super().__init__(data_dir=data_dir, max_key_len=max_key_len, max_value_len=max_value_len, replica=replica)

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
        self.rfds: list[list[FileIO]] = [[]]

        if self.replica:
            # restore calls rebuild_indices, so this way we avoid rebuilding twice
            self.restore()
        else:
            self.rebuild_indices()

    def rebuild_indices(self):
        self.levels = []
        self.rfds: list[list[FileIO]] = [[]]

        # do file discovery
        data_files_levels = [int(f.name.split('.')[0][1:]) for f in self.data_dir.glob('L*') if
                             f.is_file() and f.name.endswith('.run')]
        levels_lengths: list[int] = [0] * (max(data_files_levels) + 1) if data_files_levels else [0]
        for i in data_files_levels:
            levels_lengths[i] += 1

        self.rfds = [[(self.data_dir / f'L{level_idx}.{run_idx}.run').open('rb') for run_idx in range(n_runs)] for
                     level_idx, n_runs in enumerate(levels_lengths)]

        # load filters and pointers for levels and runs
        for _ in levels_lengths:
            self.levels.append([])
        for level_idx, n_runs in enumerate(levels_lengths):
            for r in range(n_runs):
                with (self.data_dir / f'L{level_idx}.{r}.pointers').open('r') as pointers_file:
                    data = pointers_file.read()
                pointers = FencePointers(from_str=data)

                with (self.data_dir / f'L{level_idx}.{r}.filter').open('r') as filter_file:
                    data = filter_file.read()
                filter = BloomFilter(from_str=data)

                self.levels[level_idx].append(Run(filter, pointers))

    def close(self):
        self.save_metadata()
        if self.replica:
            self.snapshot()
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

        # NOTE maybe i should write after the flush?
        # cause this way the limit is not a hard limit, it may be passed by up to 255 bytes
        self.memtable[key] = value
        new_bytes_count = self.memtable_bytes_count + len(key) + len(value)

        if new_bytes_count > self.memtable_bytes_limit:
            # normally I would allocate a new memtable here so that writes can continue there
            # and then give the flushing of the old memtable to a background thread
            self.flush()
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
        # I can replace with an actual accurate count but I don't think it's worth it, it's an estimate anyway
        filter = BloomFilter(sum([run.filter.est_num_items for run in level]))

        fds, keys, values, is_empty = [], [], [], []
        for i, _ in enumerate(level):
            fd = self.rfds[level_idx][i]
            fds.append(fd)
            k, v = self._read_kv_pair(fd)
            keys.append(k)
            values.append(v)
            is_empty.append(True if not k else False)

        with (self.data_dir / f'L{level_idx + 1}.{len(next_level)}.run').open('wb') as run_file:
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
                    filter.add(keys[argmin_key])

                written_key = keys[argmin_key]

                # read next kv pair 
                keys[argmin_key], values[argmin_key] = self._read_kv_pair(fds[argmin_key])
                if not keys[argmin_key]:
                    is_empty[argmin_key] = True

                # skip duplicates
                # + 1 cause inclusive range
                for i in reversed(range(argmin_key + 1)):
                    # if it's the same key, read one more pair to skip it
                    while not is_empty[i] and written_key == keys[i]:
                        keys[i], values[i] = self._read_kv_pair(fds[i])
                        if not keys[i]:
                            is_empty[i] = True

        if level_idx + 1 >= len(self.rfds):
            self.rfds.append([])
        self.rfds[level_idx + 1].append((self.data_dir / f'L{level_idx + 1}.{len(next_level)}.run').open('rb'))
        for fd in fds:
            fd.close()
        self.rfds[level_idx].clear()

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

        # sync with remote
        if self.replica:
            # -1 cause next_level was appended to previously
            self.replica.put(f'L{level_idx + 1}.{len(next_level) - 1}.run')
            self.replica.put(f'L{level_idx + 1}.{len(next_level) - 1}.pointers')
            self.replica.put(f'L{level_idx + 1}.{len(next_level) - 1}.filter')

        # cascade the merging recursively
        if len(next_level) >= self.max_runs_per_level:
            self.merge(level_idx + 1)

    def flush(self):
        if len(self.memtable) == 0:
            return
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
        self.rfds[0].append((self.data_dir / f'L{flush_level}.{n_runs}.run').open('rb'))

        with (self.data_dir / f'L{flush_level}.{n_runs}.pointers').open('w') as pointers_file:
            pointers_file.write(fence_pointers.serialize())

        with (self.data_dir / f'L{flush_level}.{n_runs}.filter').open('w') as filter_file:
            filter_file.write(filter.serialize())

        if self.replica:
            self.replica.put(f'L{flush_level}.{n_runs}.run')
            self.replica.put(f'L{flush_level}.{n_runs}.pointers')
            self.replica.put(f'L{flush_level}.{n_runs}.filter')

        # reset WAL
        self.wal_file.close()
        self.wal_file = self.wal_path.open('wb')

        # trigger merge if exceeding the runs per level
        # here I don't risk index out of bounds cause flush runs before, and is guaranteed to create at least the
        # first level
        if len(self.levels[0]) >= self.max_runs_per_level:
            self.merge(0)

    def snapshot(self):
        self.flush()

    def restore(self, version=None):
        # flush first to empty the memtable
        self.flush()
        if self.replica:
            self.replica.restore(max_per_level=self.max_runs_per_level, version=version)
            self.rebuild_indices()

    def __sizeof__(self):
        memtable_size = sum((getsizeof(k) + getsizeof(v) for k, v in self.memtable.items()))
        bloom_filters_size = sum((getsizeof(run.filter) for level in self.levels for run in level))
        fence_pointers_size = sum((getsizeof(run.pointers) for level in self.levels for run in level))
        return memtable_size + bloom_filters_size + fence_pointers_size
