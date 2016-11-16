#!/usr/bin/env python

import os
from setuptools import find_packages, setup
from io import open
import re

with open('README.rst', encoding='utf-8') as f:
    readme = f.read()

# Version extraction inspired from 'requests'
with open('azure/datalake/store/__init__.py', 'r') as fd:
    version = re.search(r'^__version__\s*=\s*[\'"]([^\'"]*)[\'"]',
                        fd.read(), re.MULTILINE).group(1)

if not version:
    raise RuntimeError('Cannot find version information')

setup(name='azure-datalake-store',
      version=version,
      description='Convenient Filesystem interface to Azure Data-lake Store',
      url='https://github.com/Azure/azure-data-lake-store-python',
      author='Microsoft Corporation',
      author_email='',
      license='MIT License',
      keywords='azure',
      classifiers=[
          'Development Status :: 4 - Beta',
          'Programming Language :: Python',
          'Programming Language :: Python :: 2',
          'Programming Language :: Python :: 2.7',
          'Programming Language :: Python :: 3',
          'Programming Language :: Python :: 3.3',
          'Programming Language :: Python :: 3.4',
          'Programming Language :: Python :: 3.5',
          'License :: OSI Approved :: MIT License',
      ],
      packages=find_packages(exclude=['tests']),
      install_requires=[
          'cffi',
          'adal>=0.4.2',
          'azure-nspkg'
      ],
      extras_require={
          ":python_version<'3.4'": ['pathlib2'],
      },
      long_description=readme,
      zip_safe=False
)
