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
        resp = {'status': 'success', 'errmsg': ''}
        try:
            rets = self.client.list_objects(**kwargs)
            data = []
            if rets.prefix_list:
                data = [{'key': row} for row in rets.prefix_list]
            for row in rets.object_list:
                d = row.__dict__
                d['last_modified'] = datetime.datetime.fromtimestamp(d['last_modified'])
                data.append(d)
            resp['data'] = data
        except Exception as e:
            resp = {'status': 'failure', 'errmsg': str(e)}
        return resp

    def exists_object(self, key):
        resp = {'status': 'success', 'errmsg': ''}
        try:
            self.client.object_exists(key)
        except Exception as e:
            resp = {'status': 'failure', 'errmsg': str(e)}
        return resp
        return self.client.object_exists(key)

    def get_object(self, key, **kwargs):
        resp = {'status': 'success', 'errmsg': ''}
        try:
            ret = self.client.get_object(key, **kwargs)
            resp['data'] = {
                'key': key,
                'last_modified': ret.last_modified,
                'size': ret.content_length,
                'etag': ret.etag,
            }
        except Exception as e:
            resp = {'status': 'failure', 'errmsg': str(e)}
        return resp


    def put_object(self, key, data):
        resp = {'status': 'success', 'errmsg': ''}
        try:
            self.client.put_object(key, data)
        except Exception as e:
            resp = {'status': 'failure', 'errmsg': str(e)}
        return resp

    def delete_object(self, key):
        resp = {'status': 'success', 'errmsg': ''}
        try:
            self.client.delete_object(key)
        except Exception as e:
            resp = {'status': 'failure', 'errmsg': str(e)}
        return resp

    def delete_objects(self, key_list):
        resp = {'status': 'success', 'errmsg': ''}
        try:
            self.client.batch_delete_objects(key_list)
        except Exception as e:
            resp = {'status': 'failure', 'errmsg': str(e)}
        return resp

    def create_folder(self, key):
        resp = {'status': 'success', 'errmsg': ''}
        if not key.endswith('/'): key += '/'
        try:
            self.client.put_object(key, '')
        except Exception as e:
            resp = {'status': 'failure', 'errmsg': str(e)}
        return resp

    def delete_folder(self, key):
        resp = {'status': 'success', 'errmsg': ''}
        if not key.endswith('/'): key += '/'
        try:
            objects = oss2.ObjectIterator(self.client, prefix=key)
            key_list = [row.key for row in objects]
            self.delete_objects(key_list)
        except Exception as e:
            resp = {'status': 'failure', 'errmsg': str(e)}
        return resp

    def upload_file(self, src, target):
        resp = {'status': 'success', 'errmsg': ''}
        try:
            self.client.put_object_from_file(target, src)
        except Exception as e:
            resp = {'status': 'failure', 'errmsg': str(e)}
        return resp

    def download_file(self, src, target):
        resp = {'status': 'success', 'errmsg': ''}
        try:
            os.makedirs(os.path.dirname(target), 0o755, exist_ok=True)
            self.client.get_object_to_file(src, target)
        except Exception as e:
            resp = {'status': 'failure', 'errmsg': str(e)}
        return resp

    def list_buckets(self, **kwargs):
        resp = {'status': 'success', 'errmsg': ''}
        try:
            service = oss2.Service(self.auth,self.endpoint)
            resp['data'] = ([{'name': b.name, 'create_time': datetime.datetime.fromtimestamp(b.creation_date), 'location': b.location} for b in oss2.BucketIterator(service)])
        except Exception as e:
            resp = {'status': 'failure', 'errmsg': str(e)}
        return resp

    def create_bucket(self, bucket=None, **kwargs):
        resp = {'status': 'success', 'errmsg': ''}
        if not bucket: bucket = self.bucket
        try:
            oss2.Bucket(self.auth, self.endpoint, bucket).create_bucket(**kwargs)
        except Exception as e:
            resp = {'status': 'failure', 'errmsg': str(e)}
        return resp

    def delete_bucket(self, bucket=None):
        resp = {'status': 'success', 'errmsg': ''}
        if not bucket: bucket = self.bucket
        try:
            oss2.Bucket(self.auth, self.endpoint, bucket).delete_bucket()
        except Exception as e:
            resp = {'status': 'failure', 'errmsg': str(e)}
        return resp

    def get_bucket(self, bucket=None):
        resp = {'status': 'success', 'errmsg': ''}
        if not bucket: bucket = self.bucket
        try:
            resp['data'] = oss2.Bucket(self.auth, self.endpoint, bucket).get_bucket_info()
        except Exception as e:
            resp = {'status': 'failure', 'errmsg': str(e)}
        return resp

    @property
    def type(self):
        return 'oss'
