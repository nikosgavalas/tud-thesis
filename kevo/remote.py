import os
import shutil
from time import sleep
from pathlib import Path
from typing import Optional

from minio import Minio, S3Error

from kevo.engines.kvstore import discover_run_files, discover_version_files


def read_key(filename):
    with open(filename, 'r') as f:
        return f.read()


# abstract class
class Remote:
    def __init__(self):
        self.src_dir_path = None

    def init(self, src_dir_path):
        self.src_dir_path = src_dir_path

    def push_deltas(self, delta_map: list[Path]):
        raise NotImplementedError

    def put(self, filename: str):
        raise NotImplementedError

    def get(self, filename: str):
        raise NotImplementedError

    def gc(self):
        raise NotImplementedError

    def restore(self, version=None):
        raise NotImplementedError

    def destroy(self):
        raise NotImplementedError


class PathRemote(Remote):
    def __init__(self, remote_dir_path: str, latency_per_byte: int = 0):
        # latency_per_byte simulates network delay, useful for benchmarking and tests
        super().__init__()

        self.remote_dir_path = Path(remote_dir_path)
        self.latency_per_byte = latency_per_byte

        os.makedirs(self.remote_dir_path, exist_ok=True)

    def init(self, src_dir_path: Path):
        super().init(src_dir_path)

        os.makedirs(self.remote_dir_path, exist_ok=True)

    def push_deltas(self, delta_map: list[Path], snapshot_version: int = 0):
        delta_map = {f.name for f in delta_map}

        discovered_files = discover_run_files(self.remote_dir_path)
        remote_delta_map = {f.name for f in discovered_files}

        files_to_push = delta_map - remote_delta_map
        for file in files_to_push:
            self.put(file)

        version_file_name = f'version.{snapshot_version}.txt'
        with open(os.path.join(self.src_dir_path.resolve(), version_file_name), 'w') as f:
            f.write('-'.join(delta_map))
        self.put(version_file_name)

    def _simulate_net_delay(self, filepath: str):
        filesize = os.path.getsize(filepath)
        if self.latency_per_byte > 0:
            sleep(self.latency_per_byte * filesize)

    def put(self, filename: str):
        filename = os.path.basename(filename)
        filepath = os.path.join(self.src_dir_path.resolve(), filename)
        self._simulate_net_delay(filepath)
        shutil.copy(filepath, self.remote_dir_path)

    def get(self, filename: str):
        filename = os.path.basename(filename)
        shutil.copy(os.path.join(self.remote_dir_path, filename), self.src_dir_path.resolve())

    def gc(self):
        raise NotImplementedError

    def restore(self, version=None):
        if version is None:
            # find the latest
            discovered_files = discover_version_files(self.remote_dir_path)
            if not discovered_files:
                return 0
            version = max(map(lambda p: int(p.name.split('.')[1]), discovered_files))

        # clean up the local tree first
        # TODO do this after a successful recovery, fetch all the files first and only afterwards delete the old ones.
        shutil.rmtree(self.src_dir_path)
        os.mkdir(self.src_dir_path)

        version_file_name = f'version.{version}.txt'
        self.get(version_file_name)
        with open(os.path.join(self.src_dir_path, version_file_name), 'r') as f:
            filenames = f.read().split('-')

        for f in filenames:
            self.get(f)

        global_version = 0
        global_versions = [int(f.name.split('.')[2]) for f in self.remote_dir_path.glob('L*.run') if f.is_file()]
        if global_versions:
            global_version = max(global_versions) + 1
        return global_version

    def destroy(self):
        shutil.rmtree(self.remote_dir_path)


class MinioRemote(Remote):
    def __init__(self,
                 bucket: str,
                 address='localhost:9000',
                 access_key_fname='access.key',
                 secret_key_fname='secret.key',
                 minio_client=None):
        super().__init__()

        self.bucket = bucket
        self.client = minio_client if minio_client else Minio(address, read_key(access_key_fname),
                                                              read_key(secret_key_fname), secure=False)

        self.delta_map: Optional[set[str]] = None

        if not self.client.bucket_exists(self.bucket):
            self.client.make_bucket(self.bucket)

        self.global_version = 0

    def init(self, src_dir_path: Path):
        super().init(src_dir_path)

        if not self.client.bucket_exists(self.bucket):
            self.client.make_bucket(self.bucket)

        self.delta_map = {obj.object_name for obj in self.client.list_objects(self.bucket) if
                          obj.object_name.startswith('L') and obj.object_name.endswith('run')}

    # TODO move this to Remote class
    def push_deltas(self, delta_map: list[Path], snapshot_version: int = 0):
        delta_map = {f.name for f in delta_map}

        files_to_push = delta_map - self.delta_map
        for file in files_to_push:
            self.put(file)

        version_file_name = f'version.{snapshot_version}.txt'
        with open(os.path.join(self.src_dir_path.resolve(), version_file_name), 'w') as f:
            f.write('-'.join(delta_map))
        self.put(version_file_name)

    def put(self, filename: str):
        filename = os.path.basename(filename)
        filepath = os.path.join(self.src_dir_path.resolve(), filename)
        self.client.fput_object(self.bucket, filename, filepath)

    def get(self, filename: str):
        filename = os.path.basename(filename)
        self.client.fget_object(self.bucket, filename, os.path.join(self.src_dir_path.resolve(), filename))

    def gc(self):
        raise NotImplementedError

    def restore(self, version=None):
        if version is None:
            # find the latest
            discovered_files = {obj.object_name for obj in self.client.list_objects(self.bucket) if
                                obj.object_name.startswith('version') and obj.object_name.endswith('txt')}
            if not discovered_files:
                return 0
            version = max(map(lambda p: int(p.split('.')[1]), discovered_files))

        # clean up the local tree first
        # TODO do this after a successful recovery, fetch all the files first and only afterwards delete the old ones.
        shutil.rmtree(self.src_dir_path)
        os.mkdir(self.src_dir_path)

        version_file_name = f'version.{version}.txt'
        self.get(version_file_name)
        with open(os.path.join(self.src_dir_path, version_file_name), 'r') as f:
            filenames = f.read().split('-')

        for f in filenames:
            self.get(f)

        global_version = 0
        global_versions = [int(obj.object_name.split('.')[2]) for obj in self.client.list_objects(self.bucket) if
                                obj.object_name.startswith('L') and obj.object_name.endswith('run')]
        if global_versions:
            global_version = max(global_versions) + 1
        return global_version

    def destroy(self):
        for o in self.client.list_objects(self.bucket):
            self.client.remove_object(self.bucket, o.object_name)
        self.client.remove_bucket(self.bucket)
