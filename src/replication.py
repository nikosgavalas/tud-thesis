# docker run --rm --name minio -p 9000:9000 -p 9001:9001 -e "MINIO_ROOT_USER=minio99" -e "MINIO_ROOT_PASSWORD=minio123" quay.io/minio/minio server /data --console-address ":9001"

import os
import shutil

from minio import Minio
# from minio.error import S3Error


# abstract class
class Replica():
    def __init__(self, src_dir_path):
        self.src_dir_path = src_dir_path

    def put(self, filename):
        raise NotImplementedError

    def get(self, filename):
        raise NotImplementedError

    def rm(self, filename):
        raise NotImplementedError

    def restore(self):
        raise NotImplementedError

    def destroy(self):
        raise NotImplementedError


class PathReplica(Replica):
    def __init__(self, src_dir_path, remote_dir_path):
        super().__init__(src_dir_path)

        self.remote_dir_path = remote_dir_path

        if not os.path.isdir(self.remote_dir_path):
            os.mkdir(self.remote_dir_path)

    def put(self, filename):
        shutil.copy(os.path.join(self.src_dir_path, filename), self.remote_dir_path)

    def get(self, filename):
        shutil.copy(os.path.join(self.remote_dir_path, filename), self.src_dir_path)

    def rm(self, filename):
        os.remove(os.path.join(self.src_dir_path, filename))

    def restore(self):
        for file_name in os.listdir(self.remote_dir_path):
            file_path = os.path.join(self.remote_dir_path, file_name)
            if os.path.isfile(file_path):
                shutil.copy(file_path, self.src_dir_path)

    def destroy(self):
        shutil.rmtree(self.remote_dir_path)


class MinioReplica(Replica):
    def __init__(self, src_dir_path, bucket, address='localhost:9000', access_key_fname='access.key', secret_key_fname='secret.key'):
        super().__init__(src_dir_path)

        self.bucket = bucket
        self.client = Minio(address, self.read_key(access_key_fname), self.read_key(secret_key_fname), secure=False)

        if not self.client.bucket_exists(self.bucket):
            self.client.make_bucket(self.bucket)

    def put(self, filename):
        self.client.fput_object(self.bucket, filename, os.path.join(self.src_dir_path, filename))

    def get(self, filename):
        self.client.fget_object(self.bucket, filename, os.path.join(self.src_dir_path, filename))

    def rm(self, filename):
        self.client.remove_object(self.bucket, filename)

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
