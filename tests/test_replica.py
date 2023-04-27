import os.path
import shutil
import unittest
from pathlib import Path

from kevo import PathReplica
from kevo.replication import expand_version


def write_file(file, content):
    with file.open('w') as f:
        f.write(content)


def read_file(file):
    with file.open('r') as f:
        contents = f.read()
    return contents


class TestReplica(unittest.TestCase):
    dir = Path('./data_test')
    remote = Path('/tmp/remote')

    def setUp(self):
        self.dir.mkdir()
        self.remote.mkdir()

    def tearDown(self):
        shutil.rmtree(self.dir.name)
        shutil.rmtree(self.remote.resolve())

    def test_preexisting_files(self):
        fname = 'L0.0.run'
        write_file(self.remote / f'{fname}-0', '0')
        write_file(self.remote / f'{fname}-1', '1')
        write_file(self.dir / fname, '2')

        self.replica = PathReplica(self.dir.name, '/tmp/remote')
        self.replica.put(fname)

        (self.dir / fname).unlink()

        self.replica.get(fname, version=1)
        self.assertEqual(read_file(self.dir / fname), '1')

        self.replica.get(fname)
        self.assertEqual(read_file(self.dir / fname), '2')

    def test_version_expansion(self):
        self.assertEqual(expand_version(0, 3), [])
        self.assertEqual(expand_version(8, 3), [(0, 1), (0, 0), (1, 1), (1, 0)])
        self.assertEqual(expand_version(9, 3), [(2, 0)])
        self.assertEqual(expand_version(10, 3), [(0, 0), (2, 0)])
        self.assertEqual(expand_version(64, 3), [(0, 0), (2, 0), (3, 1), (3, 0)])

    def test_basic(self):
        write_file(self.remote / 'L0.0.run-0', '1-run')
        write_file(self.remote / 'L0.0.filter-0', '1-filter')

        write_file(self.remote / 'L0.1.run-0', '2-run')
        write_file(self.remote / 'L0.1.filter-0', '2-filter')

        write_file(self.dir / 'L0.2.run', '3-run')
        write_file(self.dir / 'L0.2.filter', '3-filter')

        write_file(self.dir / 'L1.0.run', '4-run')
        write_file(self.dir / 'L1.0.filter', '4-filter')

        self.replica = PathReplica(self.dir.name, '/tmp/remote')
        self.replica.put('L0.2.run')
        self.replica.put('L0.2.filter')
        self.replica.put('L1.0.run')
        self.replica.put('L1.0.filter')

        self.replica.restore(3, 1)
        self.assertEqual(read_file(self.dir / 'L0.0.run'), '1-run')
        self.assertEqual(read_file(self.dir / 'L0.0.filter'), '1-filter')

        self.replica.restore(3, 2)
        self.assertEqual(read_file(self.dir / 'L0.1.run'), '2-run')
        self.assertEqual(read_file(self.dir / 'L0.1.filter'), '2-filter')

        self.replica.restore(3)
        # assert that file does not exist
        self.assertFalse(os.path.isfile((self.dir / 'L0.1.run').resolve()))

        self.assertEqual(read_file(self.dir / 'L1.0.run'), '4-run')
        self.assertEqual(read_file(self.dir / 'L1.0.filter'), '4-filter')


if __name__ == "__main__":
    unittest.main()
