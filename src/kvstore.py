'''
Base class for HybridLog, AppendLog and LSMTree classes.
'''

import json
import struct
from pathlib import Path

import aiofiles


EMPTY = b''
# TODO maybe bump these up to 65536? the drawback is that I'll need two bytes for len(key) and len(value) in the binary encoding
MAX_KEY_LENGTH = 256  # maximum values encodable with one byte
MAX_VALUE_LENGTH = 256


class aobject(object):
    """Inherit this class to define async __init__'s."""
    async def __new__(cls, *a, **kw):
        instance = super().__new__(cls)
        await instance.__init__(*a, **kw)
        return instance

    async def __init__(self):
        pass


class KVStore(aobject):
    async def __init__(self, data_dir='./data'):
        self.data_dir = Path(data_dir)
        self.data_dir.mkdir(exist_ok=True)

        self.metadata = {}
        self.metadata_path = self.data_dir / 'metadata'
        await self.load_metadata()
        if 'type' in self.metadata:
            assert self.metadata['type'] == self.type, 'incorrect directory structure'
        else:
            self.metadata['type'] = self.type

    async def load_metadata(self):
        if self.metadata_path.is_file():
            async with aiofiles.open(self.metadata_path, 'r') as metadata_file:
                self.metadata = json.loads(await metadata_file.read())

    async def save_metadata(self):
        async with aiofiles.open(self.metadata_path, 'w') as metadata_file:
            await metadata_file.write(json.dumps(self.metadata))

    async def _read_kv_pair(self, fd):
        first_byte = await fd.read(1)
        if not first_byte:
            return EMPTY, EMPTY
        key_len = struct.unpack('<B', first_byte)[0]
        key = await fd.read(key_len)
        val_len = struct.unpack('<B', await fd.read(1))[0]
        value = await fd.read(val_len)
        return key, value

    async def _write_kv_pair(self, fd, key, value):
        await fd.write(struct.pack('<B', len(key)))
        await fd.write(key)
        await fd.write(struct.pack('<B', len(value)))
        await fd.write(value)

    # abstract methods
    def __getitem__(self, key):
        raise NotImplementedError('')

    def __setitem__(self, key, value):
        raise NotImplementedError('')

    def get(self, key: bytes):
        raise NotImplementedError('')

    def set(self, key: bytes, value: bytes):
        raise NotImplementedError('')

    def close(self):
        raise NotImplementedError('')
