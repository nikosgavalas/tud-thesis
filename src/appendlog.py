from src.kvstore import KVStore, EMPTY, MAX_KEY_LENGTH, MAX_VALUE_LENGTH


class AppendLog(KVStore):
    def __init__(self, data_dir='./data', max_runs_per_level=3, threshold=4_000_000):
        self.type = 'appendlog'
        super().__init__(data_dir)

        # about state:
        # the state here, runs_per_level, contrary to the LSMTree implementation, keeps track
        # of all the files not just those that have been compacted/merged.
        # this means that i have to update it **BEFORE** any new files are written. this is because the index
        # always points to the most recent record, which may be to a new log.
        # this means that i have to check for unexisted files
        # in the LSMTree implementation, i have idempotency (i can rebuild stuff based on previous files)
        # one more thing about the state i just realized: only runs in the first level need compaction.
        # the merged files in greater levels are already compacted in a sense

        # TODO i may loose unflushed records, I should check how I can turn off buffering and flush always
        # also, I can detect if a record is malformed by checking if the len(key) and len(value) are equal
        # to their encoding byte when reading

        # TODO what do i do with the deletes?

        # actually I completely removed compaction. It adds a lot of complexity for no benefit, since
        # all the benefit is actually done in the merging phase.
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

        # rebuild the index 
        for level_idx, n_runs in enumerate(self.levels):
            for run_idx in range(n_runs):
                log_path = self.data_dir / f'L{level_idx}.{run_idx}.run'
                with log_path.open('rb') as log_file:
                    offset = log_file.tell()
                    k, _ = self._read_kv_pair(log_file)
                    while k:
                        self.hash_index[k] = Record(level_idx, run_idx, offset)
                        offset = log_file.tell()
                        k, _ = self._read_kv_pair(log_file)

    def __del__(self):
        self.close()

    def close(self):
        self.save_metadata()

    def get(self, key: bytes):
        assert type(key) is bytes and 0 < len(key) <= MAX_KEY_LENGTH

        if key not in self.hash_index:
            return EMPTY

        record = self.hash_index[key]

        with (self.data_dir / str(record)).open('rb') as log_file:
            log_file.seek(record.offset)
            k, v = self._read_kv_pair(log_file)
            assert k == key
            return v

    def set(self, key: bytes, value: bytes = EMPTY):
        assert type(key) is bytes and type(value) is bytes and 0 < len(key) <= MAX_KEY_LENGTH and len(value) <= MAX_VALUE_LENGTH

        # always write the latest log of the first level
        log_path = self.data_dir / f'L{0}.{self.levels[0]}.run'
        with log_path.open('ab') as log_file:  # TODO consider keeping this file open all the time? will this make things considerably faster?
            offset = log_file.tell()
            self._write_kv_pair(log_file, key, value)
            self.hash_index[key] = Record(0, self.levels[0], offset)
            self.counter += len(key) + len(value)

        if self.counter >= self.threshold:
            self.counter = 0
            self.levels[0] += 1

        if self.levels[0] > self.max_runs_per_level:
            self.merge(0)

    def merge(self, level: int):
        next_level = level + 1
        if level + 1 >= len(self.levels):
            self.levels.append(0)
        next_run = self.levels[level + 1]

        for run_idx in range(self.levels[level]):
            with (self.data_dir / f'L{level}.{run_idx}.run').open('rb') as src_file, (self.data_dir / f'L{next_level}.{next_run}.run').open('ab') as dst_file:
                src_offset = src_file.tell()
                k, v = self._read_kv_pair(src_file)
                while k:
                    if k in self.hash_index:
                        if self.hash_index[k] == Record(level, run_idx, src_offset):
                            dst_offset = dst_file.tell()
                            self._write_kv_pair(dst_file, k, v)
                            self.hash_index[k] = Record(next_level, next_run, dst_offset)
                    src_offset = src_file.tell()
                    k, v = self._read_kv_pair(src_file)

        # bump the runs counter
        self.levels[next_level] += 1
        # merge recursively
        if self.levels[next_level] > self.max_runs_per_level:
            self.merge(next_level)


class Record():
    def __init__(self, level: int, run: int, offset: int):
        self.level = level
        self.run = run
        self.offset = offset

    def __eq__(self, other):
        # I can implement lt, le, gt, ge in the same way if needed
        return ((self.level, self.run, self.offset)
            == (other.level, other.run, other.offset))

    def __ne__(self, other):
        return ((self.level, self.run, self.offset)
            != (other.level, other.run, other.offset))

    def __str__(self):
        return f'L{self.level}.{self.run}.run'

    def __repr__(self):
        return f'({self.level},{self.run},{self.offset})'
