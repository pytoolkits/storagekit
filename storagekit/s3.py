# -*- coding: utf-8 -*-
#
import boto3
import os

from .base import ObjectStorage


class S3Storage(ObjectStorage):
    def __init__(self, config):
        self.bucket = config.get("BUCKET", "jumpserver")
        self.region = config.get("REGION", None)
        self.access_key = config.get("ACCESS_KEY", None)
        self.secret_key = config.get("SECRET_KEY", None)
        self.endpoint = config.get("ENDPOINT", None)

        try:
            self.client = boto3.client(
                's3', region_name=self.region,
                aws_access_key_id=self.access_key,
                aws_secret_access_key=self.secret_key,
                endpoint_url=self.endpoint
            )
        except ValueError:
            pass

    def list_objects(self, **kwargs):
        data = []
        if 'max_keys' in kwargs: kwargs['MaxKeys'] = kwargs.pop('max_keys')
        if 'marker' in kwargs: kwargs['Marker'] = kwargs.pop('marker')
        if 'prefix' in kwargs: kwargs['Prefix'] = kwargs.pop('prefix')
        if 'delimiter' in kwargs: kwargs['Delimiter'] = kwargs.pop('delimiter')
        try:
            rets = self.client.list_objects(Bucket=self.bucket, **kwargs)
            if 'CommonPrefixes' in rets:
                data = [{'key': row['Prefix']} for row in rets['CommonPrefixes']]
            if 'Contents' not in rets: return data
            for row in rets['Contents']:
                d = {}
                d['key'] = row['Key']
                d['last_modified'] = row['LastModified']
                d['etag'] = row['ETag']
                d['size'] = row['Size']
                d['tyep'] = ''
                d['storage_class'] = row['StorageClass']
                data.append(d)
            return data
        except Exception as e:
            return False

    def exists_object(self, key):
        try:
            self.client.head_object(Bucket=self.bucket, Key=key)
            return True, None
        except Exception as e:
            return False, e

    def put_object(self, key, data):
        return self.client.put_object(Bucket=self.bucket, Key=key, Body=data)

    def get_object(self, key, **kwargs):
        return self.client.get_object(Bucket=self.bucket, Key=key, **kwargs)

    def delete_object(self, key):
        try:
            self.client.delete_object(Bucket=self.bucket, Key=key)
            return True, None
        except Exception as e:
            return False, e

    def create_folder(self, key):
        return self.client.put_object(Bucket=self.bucket, Key=key, Body='')

    def upload_file(self, src, target):
        try:
            self.client.upload_file(Filename=src, Bucket=self.bucket, Key=target)
            return True, None
        except Exception as e:
            return False, e

    def download_file(self, src, target):
        try:
            os.makedirs(os.path.dirname(target), 0o755, exist_ok=True)
            self.client.download_file(self.bucket, src, target)
            return True, None
        except Exception as e:
            return False, e

    def generate_presigned_url(self, key, expire=3600):
        try:
            return self.client.generate_presigned_url(
                ClientMethod='get_object',
                Params={'Bucket': self.bucket, 'Key': key},
                ExpiresIn=expire,
                HttpMethod='GET'), None
        except Exception as e:
            return False, e

    def list_buckets(self, **kwargs):
        response = self.client.list_buckets()
        buckets = response.get('Buckets', [])
        result = [{'name': b['Name'], 'create_time': b['CreationDate']} for b in buckets if b.get('Name')]
        return result

    def create_bucket(self, bucket=None, **kwargs):
        if not bucket: bucket = self.bucket
        return self.client.create_bucket(Bucket=bucket, **kwargs)

    def delete_bucket(self, bucket=None):
        if not bucket: bucket = self.bucket
        return self.client.delete_bucket(Bucket=bucket)

    def get_bucket(self, bucket=None):
        if not bucket: bucket = self.bucket
        return self.client.head_bucket(Bucket=bucket)

    @property
    def type(self):
        return 's3'
