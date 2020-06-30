#!/usr/bin/env python
# coding: utf-8
# Copyright (c) 2018
#

__version__ = '0.0.3'

from .oss import OSSStorage
from .s3 import S3Storage
from .azure import AzureStorage
from .ceph import CEPHStorage
from .es import ESStorage
from .multi import MultiObjectStorage


def get_object_storage(config):
    if config.get("TYPE") in ["s3", "ceph", "swift"]:
        return S3Storage(config)
    elif config.get("TYPE") == "oss":
        return OSSStorage(config)
    elif config.get("TYPE") == "azure":
        return AzureStorage(config)
    elif config.get("TYPE") == "ceph":
        return CEPHStorage(config)
    else:
        raise Exception("Not found proper storage")


def get_log_storage(config):
    if config.get("TYPE") in ("es", "elasticsearch"):
        return ESStorage(config)
    else:
        raise Exception("Not found proper storage")


def get_multi_object_storage(configs):
    return MultiObjectStorage(configs)
