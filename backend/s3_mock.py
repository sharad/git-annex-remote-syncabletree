# backends/s3_mock.py - Mock S3 backend for git-annex custom remote
import os
import shutil

class S3BackendMock:
    def __init__(self, bucket_name):
        self.base_path = os.path.join('/tmp/mock_s3', bucket_name)
        os.makedirs(self.base_path, exist_ok=True)

    def _key_path(self, key):
        return os.path.join(self.base_path, key)

    def upload_file(self, file_path, key):
        dst_path = self._key_path(key)
        os.makedirs(os.path.dirname(dst_path), exist_ok=True)
        shutil.copyfile(file_path, dst_path)

    def download_file(self, key, dest_path):
        src_path = self._key_path(key)
        if os.path.exists(src_path):
            shutil.copyfile(src_path, dest_path)
        else:
            raise FileNotFoundError(f"Key {key} not found in mock S3")

    def has_key(self, key):
        return os.path.exists(self._key_path(key))

