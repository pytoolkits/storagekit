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

    def upload(self, src, target):
        try:
            self.client.put_object_from_file(target, src)
            return True, None
        except Exception as e:
            return False, e

    def list(self, **kwargs):
        rets = self.client.list_objects(**kwargs)
        data = []
        for row in rets.object_list:
            d = row.__dict__
            d['last_modified'] = datetime.datetime.fromtimestamp(d['last_modified'])
            data.append(d)
        return data

    def exists(self, path):
        return self.client.object_exists(path)

    def delete(self, path):
        try:
            self.client.delete_object(path)
            return True, None
        except Exception as e:
            return False, e

    def download(self, src, target):
        try:
            os.makedirs(os.path.dirname(target), 0o755, exist_ok=True)
            self.client.get_object_to_file(src, target)
            return True, None
        except Exception as e:
            return False, e

    def put(self, key, data):
        return self.client.put_object(key, data)

    def create_folder(self, key):
        if not key.endswith('/'): key += '/'
        return self.client.put_object(key, None)

    def list_buckets(self):
        service = oss2.Service(self.auth,self.endpoint)
        return ([{'name': b.name, 'create_time': datetime.datetime.fromtimestamp(b.creation_date), 'location': b.location} for b in oss2.BucketIterator(service)])

    def create_bucket(self, **kwargs):
        return self.client.create_bucket()

    def delete_bucket(self):
        return self.client.delete_bucket()

    def bucket_info(self):
        return self.client.get_bucket_info()

    # def create_bucket(self, name, **kwargs):
    #     bucket = oss2.Bucket(self.auth,self.endpoint, name)
    #     return bucket.create_bucket()

    @property
    def type(self):
        return 'oss'
