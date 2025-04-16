# backends/s3_backend.py - Real S3 backend for git-annex custom remote
import boto3
import os

class S3Backend:
    def __init__(self, bucket_name, prefix='', aws_profile=None):
        if aws_profile:
            session = boto3.Session(profile_name=aws_profile)
        else:
            session = boto3.Session()

        self.s3 = session.client(
            's3',
            endpoint_url=os.getenv('S3_ENDPOINT_URL', None),
            aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID', None),
            aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY', None),
            region_name=os.getenv('AWS_REGION', 'us-east-1')
        )

        self.bucket = bucket_name
        self.prefix = prefix.rstrip('/')

    def _key_path(self, key):
        return f"{self.prefix}/{key}" if self.prefix else key

    def upload_file(self, file_path, key):
        s3_key = self._key_path(key)
        self.s3.upload_file(file_path, self.bucket, s3_key)

    def download_file(self, key, dest_path):
        s3_key = self._key_path(key)
        try:
            self.s3.download_file(self.bucket, s3_key, dest_path)
        except self.s3.exceptions.NoSuchKey:
            raise FileNotFoundError(f"Key {key} not found in S3")

    def has_key(self, key):
        s3_key = self._key_path(key)
        try:
            self.s3.head_object(Bucket=self.bucket, Key=s3_key)
            return True
        except self.s3.exceptions.ClientError as e:
            if e.response['Error']['Code'] == '404':
                return False
            raise
