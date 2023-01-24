import asyncio
import aiofiles


async def read(path):
    async with aiofiles.open(path, 'r') as f:
        contents = await f.read()
    print(contents)
    return contents

asyncio.run(read('async_test.py'))
