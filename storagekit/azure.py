# -*- coding: utf-8 -*-
#

import os

from azure.storage.blob import BlockBlobService

from .base import ObjectStorage


class AzureStorage(ObjectStorage):

    def __init__(self, config):
        self.account_name = config.get("ACCOUNT_NAME", None)
        self.account_key = config.get("ACCOUNT_KEY", None)
        self.container_name = config.get("CONTAINER_NAME", None)
        self.endpoint_suffix = config.get("ENDPOINT_SUFFIX", 'core.chinacloudapi.cn')

        if self.account_name and self.account_key:
            self.client = BlockBlobService(
                account_name=self.account_name, account_key=self.account_key,
                endpoint_suffix=self.endpoint_suffix
            )
        else:
            self.client = None

    def list_objects(self, **kwargs):
        return self.client.get_block_list(self.container_name, **kwargs)

    def exists_object(self, key):
        return self.client.exists(self.container_name, key)

    def delete_object(self, key):
        try:
            self.client.delete_blob(self.container_name, key)
            return True, False
        except Exception as e:
            return False, e

    def put_object(self, key, data):
        pass

    def get_object(self, key, **kwargs):
        pass

    def create_folder(self, key):
        if not key.endswith('/'): key += '/'
        pass

    def upload_file(self, src, target):
        try:
            self.client.create_blob_from_path(self.container_name, target, src)
            return True, None
        except Exception as e:
            return False, e

    def download_file(self, src, target):
        try:
            os.makedirs(os.path.dirname(target), 0o755, exist_ok=True)
            self.client.get_blob_to_path(self.container_name, src, target)
            return True, None
        except Exception as e:
            return False, e

    def list_buckets(self, **kwargs):
        response = self.client.list_containers()
        return ([c.name for c in response.items])

    def create_bucket(self, bucket=None, **kwargs):
        if not bucket: bucket = self.bucket
        pass

    def delete_bucket(self, bucket=None):
        if not bucket: bucket = self.bucket
        pass

    def get_bucket(self, bucket=None):
        if not bucket: bucket = self.bucket
        pass

    @property
    def type(self):
        return 'azure'
