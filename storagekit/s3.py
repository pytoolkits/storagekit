# -*- coding: utf-8 -*-
#
import boto3
import os

from .base import ObjectStorage


class S3Storage(ObjectStorage):
    def __init__(self, config):
        self.bucket = config.get("BUCKET", None)
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
        resp = {'status': 'success', 'errmsg': '', 'data': []}
        data = []
        if 'max_keys' in kwargs: kwargs['MaxKeys'] = kwargs.pop('max_keys')
        if 'marker' in kwargs: kwargs['Marker'] = kwargs.pop('marker')
        if 'prefix' in kwargs: kwargs['Prefix'] = kwargs.pop('prefix')
        if 'delimiter' in kwargs: kwargs['Delimiter'] = kwargs.pop('delimiter')
        try:
            rets = self.client.list_objects(Bucket=self.bucket, **kwargs)
            if 'CommonPrefixes' in rets:
                data = [{'key': row['Prefix']} for row in rets['CommonPrefixes']]
            if 'Contents' in rets:
                for row in rets['Contents']:
                    d = {}
                    d['key'] = row['Key']
                    d['last_modified'] = row['LastModified']
                    d['etag'] = row['ETag']
                    d['size'] = row['Size']
                    d['tyep'] = ''
                    d['storage_class'] = row['StorageClass']
                    data.append(d)
            resp['data'] = data
        except Exception as e:
            resp = {'status': 'failure', 'errmsg': str(e)}
        return resp

    def exists_object(self, key):
        resp = {'status': 'success', 'errmsg': ''}
        try:
            self.client.head_object(Bucket=self.bucket, Key=key)
        except Exception as e:
            resp = {'status': 'failure', 'errmsg': str(e)}
        return resp

    def put_object(self, key, data):
        resp = {'status': 'success', 'errmsg': ''}
        try:
            self.client.put_object(Bucket=self.bucket, Key=key, Body=data)
        except Exception as e:
            resp = {'status': 'failure', 'errmsg': str(e)}
        return resp

    def get_object(self, key, **kwargs):
        resp = {'status': 'success', 'errmsg': ''}
        try:
            data = self.client.get_object(Bucket=self.bucket, Key=key, **kwargs)
            resp['data'] = data
            resp['data']['body'] = data['Body'].read()
            resp['data']['content_type'] = data['ResponseMetadata']['HTTPHeaders']['content-type']
        except Exception as e:
            resp = {'status': 'failure', 'errmsg': str(e)}
        return resp

    def delete_object(self, key):
        resp = {'status': 'success', 'errmsg': ''}
        try:
            self.client.delete_object(Bucket=self.bucket, Key=key)
        except Exception as e:
            resp = {'status': 'failure', 'errmsg': str(e)}
        return resp

    def delete_objects(self, key_list):
        resp = {'status': 'success', 'errmsg': ''}
        objects = [{'Key': row} for row in key_list]
        try:
            self.client.delete_objects(Bucket=self.bucket, Delete={'Objects':objects})
        except Exception as e:
            resp = {'status': 'failure', 'errmsg': str(e)}
        return resp

    def create_folder(self, key):
        resp = {'status': 'success', 'errmsg': ''}
        if not key.endswith('/'): key += '/'
        try:
            self.client.put_object(Bucket=self.bucket, Key=key, Body='')
        except Exception as e:
            resp = {'status': 'failure', 'errmsg': str(e)}
        return resp

    def delete_folder(self, key):
        resp = {'status': 'success', 'errmsg': ''}
        if not key.endswith('/'): key += '/'
        try:
            s3 = boto3.resource('s3',
                            aws_access_key_id=self.access_key,
                            aws_secret_access_key=self.secret_key,
                            endpoint_url=self.endpoint)
            bucket = s3.Bucket(self.bucket)
            bucket.objects.filter(Prefix=key).delete()
        except Exception as e:
            resp = {'status': 'failure', 'errmsg': str(e)}
        return resp

    def upload_file(self, src, target):
        resp = {'status': 'success', 'errmsg': ''}
        try:
            self.client.upload_file(Filename=src, Bucket=self.bucket, Key=target)
        except Exception as e:
            resp = {'status': 'failure', 'errmsg': str(e)}
        return resp

    def download_file(self, src, target):
        resp = {'status': 'success', 'errmsg': ''}
        try:
            os.makedirs(os.path.dirname(target), 0o755, exist_ok=True)
            self.client.download_file(self.bucket, src, target)
        except Exception as e:
            resp = {'status': 'failure', 'errmsg': str(e)}
        return resp

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
        resp = {'status': 'success', 'errmsg': '', 'data': []}
        try:
            response = self.client.list_buckets()
            buckets = response.get('Buckets', [])
            resp['data'] = [{'name': b['Name'], 'create_time': b['CreationDate']} for b in buckets if b.get('Name')]
        except Exception as e:
            resp = {'status': 'failure', 'errmsg': str(e)}
        return resp

    def create_bucket(self, bucket=None, **kwargs):
        resp = {'status': 'success', 'errmsg': ''}
        if not bucket: bucket = self.bucket
        try:
            self.client.create_bucket(Bucket=bucket, **kwargs)
        except Exception as e:
            resp = {'status': 'failure', 'errmsg': str(e)}
        return resp

    def delete_bucket(self, bucket=None):
        resp = {'status': 'success', 'errmsg': ''}
        if not bucket: bucket = self.bucket
        try:
            self.client.delete_bucket(Bucket=bucket)
        except Exception as e:
            resp = {'status': 'failure', 'errmsg': str(e)}
        return resp

    def get_bucket(self, bucket=None):
        resp = {'status': 'success', 'errmsg': ''}
        if not bucket: bucket = self.bucket
        try:
            resp['data'] = self.client.head_bucket(Bucket=bucket)
        except Exception as e:
            resp = {'status': 'failure', 'errmsg': str(e)}
        return resp

    @property
    def type(self):
        return 's3'
