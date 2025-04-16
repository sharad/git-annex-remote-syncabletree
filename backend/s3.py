# backends/s3.py - Real S3 backend for git-annex custom remote
import boto3
import os

class S3Backend:
    def __init__(self, bucket_name, aws_access_key_id=None, aws_secret_access_key=None, region_name=None):
        self.bucket_name = bucket_name
        self.s3 = boto3.resource(
            's3',
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            region_name=region_name
        )
        self.bucket = self.s3.Bucket(bucket_name)

    def upload_file(self, file_path, key):
        self.bucket.upload_file(file_path, key)

    def download_file(self, key, dest_path):
        try:
            self.bucket.download_file(key, dest_path)
        except self.s3.meta.client.exceptions.NoSuchKey:
            raise FileNotFoundError(f"Key {key} not found in S3")

    def has_key(self, key):
        objs = list(self.bucket.objects.filter(Prefix=key))
        return any(obj.key == key for obj in objs)

