import os
import shutil
from collections import defaultdict

from minio import Minio


# abstract class
class Replica:
    def __init__(self, src_dir_path):
        self.src_dir_path = src_dir_path

    def put(self, filename):
        raise NotImplementedError

    def get(self, filename, version=None):
        raise NotImplementedError

    def gc(self):
        raise NotImplementedError

    def restore(self, max_per_level, version=None):
        raise NotImplementedError

    def destroy(self):
        raise NotImplementedError


def parse_file_name(raw_file_name):
    s = raw_file_name.split('-')
    if len(s) == 1:
        file_name = raw_file_name
        version = 0
    else:
        file_name, version_str = raw_file_name.split('-')
        version = int(version_str)
    # drop the L from the front
    file_name = file_name[1:]
    level_str, run_str, suffix = file_name.split('.')
    return int(level_str), int(run_str), suffix, version


def to_file_name(level, run, suffix, version=None):
    s = f'L{level}.{run}.{suffix}'
    if version is not None:
        s += f'-{version}'
    return s


def expand_version(version, max_per_level):
    if max_per_level == 1:
        return [(0, 0)]

    acc = []
    while version != 0:
        acc.append(version % max_per_level)
        version //= max_per_level

    levels_runs = []
    for i, e in enumerate(acc):
        j = e - 1
        while j >= 0:
            levels_runs.append((i, j))
            j -= 1

    return levels_runs


class PathReplica(Replica):
    def __init__(self, src_dir_path, remote_dir_path):
        # NOTE: works with files of the format L0.0.run-1
        super().__init__(src_dir_path)

        self.remote_dir_path = remote_dir_path

        if not os.path.isdir(self.remote_dir_path):
            os.mkdir(self.remote_dir_path)

        # global_version keeps track of the latest version
        self.global_version = 0

        self.level_and_run_to_latest_version: dict[tuple[int, int], int] = defaultdict(int)
        for raw_file_name in os.listdir(self.remote_dir_path):
            file_path = os.path.join(self.remote_dir_path, raw_file_name)
            if os.path.isfile(file_path):
                level, run, suffix, version = parse_file_name(raw_file_name)
                # count only the run files
                if suffix == 'run':
                    self.global_version += 1
                self.level_and_run_to_latest_version[(level, run)] = max(
                    self.level_and_run_to_latest_version[(level, run)], version)

    def put(self, filename):
        # using os.path.basename to be sure
        filename = os.path.basename(filename)
        level, run, suffix, version = parse_file_name(filename)
        if suffix == 'run':
            # if is runfile of level 0, increment global_version
            if level == 0:
                self.global_version += 1
            # if it's a runfile of whatever level, increment file version
            self.level_and_run_to_latest_version[(level, run)] += 1
        # attach the version to filename
        remote_filename = to_file_name(level, run, suffix, self.level_and_run_to_latest_version[(level, run)])

        # copy it over
        shutil.copy(
            os.path.join(self.src_dir_path, filename),
            os.path.join(self.remote_dir_path, remote_filename)
        )

    def get(self, filename, version=None):
        if version is None:
            # latest
            level, run, _, _ = parse_file_name(filename)
            version = self.level_and_run_to_latest_version[(level, run)]

        filename = os.path.basename(filename)
        filename_with_version = filename + f'-{version}'

        if not os.path.isfile(os.path.join(self.remote_dir_path, filename_with_version)):
            return

        shutil.copy(
            os.path.join(self.remote_dir_path, filename_with_version),
            os.path.join(self.src_dir_path, filename)
        )

    def gc(self):
        # remove all files with versions < latest and keep only the latest ones
        for raw_file_name in os.listdir(self.remote_dir_path):
            file_path = os.path.join(self.remote_dir_path, raw_file_name)
            if os.path.isfile(file_path):
                level, run, _, version = parse_file_name(raw_file_name)
                # if not latest, remove
                if self.level_and_run_to_latest_version[(level, run)] != version:
                    os.remove(file_path)

    def restore(self, max_per_level, version=None):
        if version is None:
            # fetch the latest version
            version = self.global_version
        # check that files exist
        for level, run in expand_version(version, max_per_level):
            if not (level, run) in self.level_and_run_to_latest_version:
                return False

        # clean up the local tree first
        shutil.rmtree(self.src_dir_path)
        os.mkdir(self.src_dir_path)

        for level, run in expand_version(version, max_per_level):
            self.get(to_file_name(level, run, 'run'))
            self.get(to_file_name(level, run, 'filter'))
            self.get(to_file_name(level, run, 'pointers'))
        return True

    def destroy(self):
        shutil.rmtree(self.remote_dir_path)


class MinioReplica(Replica):
    def __init__(self, src_dir_path, bucket, address='localhost:9000', access_key_fname='access.key', secret_key_fname='secret.key', minio_client=None):
        super().__init__(src_dir_path)

        self.bucket = bucket
        self.client = minio_client if minio_client else Minio(address, self.read_key(access_key_fname), self.read_key(secret_key_fname), secure=False)

        if not self.client.bucket_exists(self.bucket):
            self.client.make_bucket(self.bucket)

    def put(self, filename):
        filename = os.path.basename(filename)
        self.client.fput_object(self.bucket, filename, os.path.join(self.src_dir_path, filename))

    def get(self, filename):
        filename = os.path.basename(filename)
        self.client.fget_object(self.bucket, filename, os.path.join(self.src_dir_path, filename))

    def rm(self, filename):
        self.client.remove_object(self.bucket, os.path.basename(filename))

    def restore(self):
        for o in self.client.list_objects(self.bucket):
            self.get(o.object_name)

    def destroy(self):
        for o in self.client.list_objects(self.bucket):
            self.rm(o.object_name)
        self.client.remove_bucket(self.bucket)

    def read_key(self, filename):
        with open(filename, 'r') as f:
            return f.read()
