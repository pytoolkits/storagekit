#!/bin/bash
# coding: utf-8

set -ex
[ -d dist ] && rm -f dist/* 
python setup.py sdist && \
twine upload dist/*.tar.gz
