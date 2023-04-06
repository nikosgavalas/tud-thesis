from typing import Optional
from collections import namedtuple

from kevo.engines.kvstore import KVStore, EMPTY, MAX_KEY_LENGTH, MAX_VALUE_LENGTH
from kevo.replication import Replica


Record = namedtuple('Record', ['level', 'run', 'offset'])

class AppendLog(KVStore):
    name = 'AppendLog'
    def __init__(self, data_dir='./data', max_runs_per_level=3, threshold=4_000_000, replica: Optional[Replica] = None):
        self.type = 'appendlog'
        super().__init__(data_dir, replica=replica)

        # about state:
        # the state here, runs_per_level, contrary to the LSMTree implementation, keeps track
        # of all the files not just those that have been compacted/merged.
        # this means that i have to update it **BEFORE** any new files are written. this is because the index
        # always points to the most recent record, which may be to a new log.
        # this means that i have to check for unexistent files
        # in the LSMTree implementation, i have idempotency (i can rebuild stuff based on previous files)
        # one more thing about the state i just realized: only runs in the first level need compaction.
        # the merged files in greater levels are already compacted in a sense

        # TODO i may loose unflushed records, I should check how I can turn off buffering and flush always
        # also, I can detect if a record is malformed by checking if the len(key) and len(value) are equal
        # to their encoding byte when reading

        # actually I completely removed compaction. It adds a lot of complexity for no benefit, since
        # all the potential benefit is actually reaped anyway in the merging phase.

        # NOTE: handled deletes nicely, optimized by keeping the files open for reading, since 50% of the time was being
        # wasted in fopens as profiling showed.

        assert max_runs_per_level >= 1
        assert threshold > 0

        self.max_runs_per_level = max_runs_per_level
        self.threshold = threshold
        self.counter = 0

        self.hash_index: dict[bytes, Record] = {}

        # do file discovery
        data_files_levels = [int(f.name.split('.')[0][1:]) for f in self.data_dir.glob('L*') if f.is_file()]
        self.levels: list[int] = [0] * (max(data_files_levels) + 1) if data_files_levels else [0]
        for i in data_files_levels:
            self.levels[i] += 1

        # read file-descriptors
        self.rfds = [[(self.data_dir / f'L{level_idx}.{run_idx}.run').open('rb') for run_idx in range(n_runs)] for level_idx, n_runs in enumerate(self.levels)]
        # write file-descriptor
        self.wfd = (self.data_dir / f'L{0}.{self.levels[0]}.run').open('wb')
        self.rfds[0].append((self.data_dir / f'L{0}.{self.levels[0]}.run').open('rb'))

        # rebuild the index 
        for level_idx, n_runs in reversed(list(enumerate(self.levels))):
            for run_idx in range(n_runs):
                log_file = self.rfds[level_idx][run_idx]
                offset = log_file.tell()
                k, _ = self._read_kv_pair(log_file)
                while k:
                    self.hash_index[k] = Record(level_idx, run_idx, offset)
                    offset = log_file.tell()
                    k, _ = self._read_kv_pair(log_file)

    def close(self):
        self.wfd.close()
        for rfds in self.rfds:
            for rfd in rfds:
                rfd.close()
        self.save_metadata()
        if self.replica:
            self.replica.put(self.metadata_path.name)
            self.replica.put(self.wfd.name)

    def __getitem__(self, key):
        return self.get(key)

    def __setitem__(self, key, value):
        return self.set(key, value)

    def get(self, key: bytes):
        assert type(key) is bytes and 0 < len(key) <= MAX_KEY_LENGTH

        if key not in self.hash_index:
            return EMPTY

        record = self.hash_index[key]

        log_file = self.rfds[record.level][record.run]
        log_file.seek(record.offset)
        k, v = self._read_kv_pair(log_file)
        assert k == key
        return v

    def set(self, key: bytes, value: bytes = EMPTY):
        assert type(key) is bytes and type(value) is bytes and 0 < len(key) <= MAX_KEY_LENGTH and len(value) <= MAX_VALUE_LENGTH

        if not value and key in self.hash_index:
            del self.hash_index[key]
            return

        # always write the latest log of the first level
        offset = self.wfd.tell()
        self._write_kv_pair(self.wfd, key, value, flush=True)
        self.hash_index[key] = Record(0, self.levels[0], offset)
        self.counter += len(key) + len(value)

        if self.counter >= self.threshold:
            self.counter = 0
            self.wfd.close()

            if self.replica:
                self.replica.put(self.wfd.name)

            self.levels[0] += 1
            if self.levels[0] > self.max_runs_per_level:
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
                if k in self.hash_index and self.hash_index[k] == Record(level, run_idx, src_offset):
                    dst_offset = dst_file.tell()
                    self._write_kv_pair(dst_file, k, v)
                    self.hash_index[k] = Record(next_level, next_run, dst_offset)
                src_offset = src_file.tell()
                k, v = self._read_kv_pair(src_file)
        dst_file.close()

        if self.replica:
            self.replica.put(dst_file.name)

        self.rfds[next_level].append((self.data_dir / f'L{next_level}.{next_run}.run').open('rb'))

        # delete merged files
        for rfd in self.rfds[level]:
            rfd.close()
        self.rfds[level].clear()
        for run_idx in range(self.levels[level]):
            path_to_remove = (self.data_dir / f'L{level}.{run_idx}.run')
            path_to_remove.unlink()
            if self.replica:
                self.replica.rm(path_to_remove.name)
        # update the runs counter
        self.levels[level] = 0
        self.levels[next_level] += 1
        # merge recursively
        if self.levels[next_level] > self.max_runs_per_level:
            self.merge(next_level)
