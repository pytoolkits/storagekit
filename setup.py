#!/usr/bin/env python
# coding: utf-8
import re

try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup

with open('storagekit/__init__.py', 'r') as fd:
    version = re.search(r'^__version__\s*=\s*[\'"]([^\'"]*)[\'"]',
                        fd.read(), re.MULTILINE).group(1)

if not version:
    raise RuntimeError('Cannot find version information')

with open('README.md', 'rb') as f:
    readme = f.read().decode('utf-8')

with open('requirements.txt', 'r') as f:
    requirements = [x.strip() for x in f.readlines()]

setup(
    name='storagekit',
    version=version,
    keywords=['storage', 'ceph', 'oss', 's3', 'elasticsearch'],
    description='object storage python sdk tools',
    long_description=readme,
    license='MIT Licence',
    url='https://github.com/pytoolkits/storagekit',
    author='hhr66',
    author_email='hhr66@qq.com',
    packages=['storagekit'],
    data_files=[('requirements', ['requirements.txt'])],
    include_package_data=True,
    install_requires=requirements,
    platforms='any'
)
