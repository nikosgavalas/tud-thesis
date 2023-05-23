from pathlib import Path
from collections import namedtuple


Record = namedtuple('Record', ['level', 'run', 'offset'])


def bytes_needed_to_encode_len(length):
    i = 0
    while 2 ** (i * 8) <= length:
        i += 1
    return i


def discover_run_files(dir_path: Path) -> list[Path]:
    return [f for f in dir_path.glob('L*.run') if f.is_file()]


def discover_version_files(dir_path: Path) -> list[Path]:
    return [f for f in dir_path.glob('version.*.txt') if f.is_file()]


class KVStore:
    EMPTY = b''

    def __init__(self,
                 data_dir='./data',
                 max_key_len=255,
                 max_value_len=255,
                 remote=None):

        self.data_dir = Path(data_dir)
        self.data_dir.mkdir(exist_ok=True)

        self.max_key_len = max_key_len
        self.max_value_len = max_value_len

        self.key_enc_len = bytes_needed_to_encode_len(self.max_key_len)
        self.val_enc_len = bytes_needed_to_encode_len(self.max_value_len)

        self.remote = remote
        if self.remote:
            self.remote.init(self.data_dir)

    def discover_runs(self) -> list[tuple[int, int, int]]:
        runs_discovered = []
        for f in discover_run_files(self.data_dir):
            level, run, version, _ = f.name.split('.')
            level = int(level[1:])
            run = int(run)
            version = int(version)
            runs_discovered.append((level, run, version))
        # has to be sorted for rebuilding to be done in the right order
        return sorted(runs_discovered)

    def _read_kv_pair(self, fd):
        first_bytes = fd.read(self.key_enc_len)
        if not first_bytes:
            return KVStore.EMPTY, KVStore.EMPTY
        key_len = int.from_bytes(first_bytes, byteorder='little')
        key = fd.read(key_len)
        val_len = int.from_bytes(fd.read(self.val_enc_len), byteorder='little')
        value = fd.read(val_len)
        return key, value

    def _write_kv_pair(self, fd, key, value, flush=False):
        fd.write(len(key).to_bytes(self.key_enc_len, byteorder='little'))
        fd.write(key)
        fd.write(len(value).to_bytes(self.val_enc_len, byteorder='little'))
        fd.write(value)
        if flush:
            fd.flush()

    # abstract methods
    def __getitem__(self, key):
        raise NotImplementedError('')

    def __setitem__(self, key, value):
        raise NotImplementedError('')

    def get(self, key: bytes):
        raise NotImplementedError('')

    def set(self, key: bytes, value: bytes):
        raise NotImplementedError('')

    def __sizeof__(self):
        raise NotImplementedError('')

    def close(self):
        raise NotImplementedError('')
