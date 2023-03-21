import aiofiles

from src.kvstore import KVStore, EMPTY, MAX_KEY_LENGTH, MAX_VALUE_LENGTH


class AppendLog(KVStore):
    async def __init__(self, data_dir='./data', max_runs_per_level=3, threshold=4_000_000):
        self.type = 'appendlog'
        await super().__init__(data_dir)

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

        # TODO what do i do with the deletes?

        # actually I completely removed compaction. It adds a lot of complexity for no benefit, since
        # all the potential benefit is actually reaped anyway in the merging phase.
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
                async with aiofiles.open(log_path, 'rb') as log_file:
                    offset = await log_file.tell()
                    k, _ = await self._read_kv_pair(log_file)
                    while k:
                        self.hash_index[k] = Record(level_idx, run_idx, offset)
                        offset = await log_file.tell()
                        k, _ = await self._read_kv_pair(log_file)

    async def close(self):
        await self.save_metadata()

    async def __getitem__(self, key):
        return await self.get(key)

    async def __setitem__(self, key, value):
        return await self.set(key, value)

    async def get(self, key: bytes):
        assert type(key) is bytes and 0 < len(key) <= MAX_KEY_LENGTH

        if key not in self.hash_index:
            return EMPTY

        record = self.hash_index[key]

        async with aiofiles.open(self.data_dir / str(record), 'rb') as log_file:
            await log_file.seek(record.offset)
            k, v = await self._read_kv_pair(log_file)
            assert k == key
            return v

    async def set(self, key: bytes, value: bytes = EMPTY):
        assert type(key) is bytes and type(value) is bytes and 0 < len(key) <= MAX_KEY_LENGTH and len(value) <= MAX_VALUE_LENGTH

        # always write the latest log of the first level
        log_path = self.data_dir / f'L{0}.{self.levels[0]}.run'
        async with aiofiles.open(log_path, 'ab') as log_file:  # TODO consider keeping this file open all the time? will this make things considerably faster?
            offset = await log_file.tell()
            await self._write_kv_pair(log_file, key, value)
            self.hash_index[key] = Record(0, self.levels[0], offset)
            self.counter += len(key) + len(value)

        if self.counter >= self.threshold:
            self.counter = 0
            self.levels[0] += 1

        if self.levels[0] > self.max_runs_per_level:
            await self.merge(0)

    async def merge(self, level: int):
        next_level = level + 1
        if level + 1 >= len(self.levels):
            self.levels.append(0)
        next_run = self.levels[level + 1]

        for run_idx in range(self.levels[level]):
            async with aiofiles.open(self.data_dir / f'L{level}.{run_idx}.run', 'rb') as src_file, aiofiles.open(self.data_dir / f'L{next_level}.{next_run}.run', 'ab') as dst_file:
                src_offset = await src_file.tell()
                k, v = await self._read_kv_pair(src_file)
                while k:
                    if k in self.hash_index:
                        if self.hash_index[k] == Record(level, run_idx, src_offset):
                            dst_offset = await dst_file.tell()
                            await self._write_kv_pair(dst_file, k, v)
                            self.hash_index[k] = Record(next_level, next_run, dst_offset)
                    src_offset = await src_file.tell()
                    k, v = await self._read_kv_pair(src_file)

        # bump the runs counter
        self.levels[next_level] += 1
        # merge recursively
        if self.levels[next_level] > self.max_runs_per_level:
            await self.merge(next_level)


# TODO change to namedtuple? maybe it's faster
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
