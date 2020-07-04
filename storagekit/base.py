# -*- coding: utf-8 -*-
#

import abc


class ObjectStorage(metaclass=abc.ABCMeta):

    @abc.abstractmethod
    def list_objects(self, **kwargs):
        pass

    @abc.abstractmethod
    def delete_object(self, key):
        pass

    @abc.abstractmethod
    def exists_object(self, key):
        pass

    @abc.abstractmethod
    def put_object(self, key, data):
        pass

    @abc.abstractmethod
    def get_object(self, key, **kwargs):
        pass

    @abc.abstractmethod
    def create_folder(self, key):
        pass

    @abc.abstractmethod
    def upload_file(self, src, target):
        return None, None

    @abc.abstractmethod
    def download_file(self, src, target):
        pass

    @abc.abstractmethod
    def list_buckets(self):
        pass

    @abc.abstractmethod
    def create_bucket(self, bucket, **kwargs):
        pass

    @abc.abstractmethod
    def delete_bucket(self, bucket):
        pass

    @abc.abstractmethod
    def get_bucket(self, bucket):
        pass

    def is_valid(self, src, target):
        ok, msg = self.upload(src=src, target=target)
        if not ok:
            return False
        self.delete(path=target)
        return True


class LogStorage(metaclass=abc.ABCMeta):
    @abc.abstractmethod
    def save(self, command):
        pass

    @abc.abstractmethod
    def bulk_save(self, command_set, raise_on_error=True):
        pass

    @abc.abstractmethod
    def filter(self, date_from=None, date_to=None,
               user=None, asset=None, system_user=None,
               input=None, session=None):
        pass

    @abc.abstractmethod
    def count(self, date_from=None, date_to=None,
              user=None, asset=None, system_user=None,
              input=None, session=None):
        pass
