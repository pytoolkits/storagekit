# -*- coding: utf-8 -*-
#
import os
import datetime
import oss2

from .base import ObjectStorage


class OSSStorage(ObjectStorage):
    def __init__(self, config):
        self.endpoint = config.get("ENDPOINT", None)
        self.bucket = config.get("BUCKET", None)
        self.access_key = config.get("ACCESS_KEY", None)
        self.secret_key = config.get("SECRET_KEY", None)
        if self.access_key and self.secret_key:
            self.auth = oss2.Auth(self.access_key, self.secret_key)
        else:
            self.auth = None
        if self.auth and self.endpoint and self.bucket:
            self.client = oss2.Bucket(self.auth, self.endpoint, self.bucket)
        else:
            self.client = None

    def list_objects(self, **kwargs):
        rets = self.client.list_objects(**kwargs)
        data = []
        if rets.prefix_list:
            data = [{'key': row} for row in rets.prefix_list]
        for row in rets.object_list:
            d = row.__dict__
            d['last_modified'] = datetime.datetime.fromtimestamp(d['last_modified'])
            data.append(d)
        return data

    def exists_object(self, key):
        return self.client.object_exists(key)

    def get_object(self, key, **kwargs):
        return self.client.get_object(key, **kwargs)

    def put_object(self, key, data):
        return self.client.put_object(key, data)

    def delete_object(self, key):
        try:
            self.client.delete_object(key)
            return True, None
        except Exception as e:
            return False, e

    def delete_objects(self, key_list):
        try:
            self.client.batch_delete_objects(key_list)
            return True, None
        except Exception as e:
            return False, e

    def create_folder(self, key):
        if not key.endswith('/'): key += '/'
        return self.client.put_object(key, None)

    def delete_folder(self, key):
        if not key.endswith('/'): key += '/'
        objects = oss2.ObjectIterator(self.client, prefix=key)
        key_list = [row.key for row in objects]
        return self.delete_objects(key_list)

    def upload_file(self, src, target):
        try:
            self.client.put_object_from_file(target, src)
            return True, None
        except Exception as e:
            return False, e

    def download_file(self, src, target):
        try:
            os.makedirs(os.path.dirname(target), 0o755, exist_ok=True)
            self.client.get_object_to_file(src, target)
            return True, None
        except Exception as e:
            return False, e

    def list_buckets(self, **kwargs):
        service = oss2.Service(self.auth,self.endpoint)
        return ([{'name': b.name, 'create_time': datetime.datetime.fromtimestamp(b.creation_date), 'location': b.location} for b in oss2.BucketIterator(service)])

    def create_bucket(self, bucket=None, **kwargs):
        if not bucket: bucket = self.bucket
        return oss2.Bucket(self.auth, self.endpoint, bucket).create_bucket(**kwargs)

    def delete_bucket(self, bucket=None):
        if not bucket: bucket = self.bucket
        return oss2.Bucket(self.auth, self.endpoint, bucket).delete_bucket()

    def get_bucket(self, bucket=None):
        if not bucket: bucket = self.bucket
        return oss2.Bucket(self.auth, self.endpoint, bucket).get_bucket_info()

    @property
    def type(self):
        return 'oss'
