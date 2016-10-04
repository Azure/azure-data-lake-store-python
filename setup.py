#!/usr/bin/env python

import os
from setuptools import setup

setup(name='azure-datalake-store',
      version='0.0.1',
      description='Convenient Filesystem interface to Azure Data-lake Store',
      url='https://github.com/Azure/azure-data-lake-store-python',
      maintainer='',
      maintainer_email='',
      license='',
      keywords='azure',
      packages=[
          'azure',
          'azure.datalake',
          'azure.datalake.store'
      ],
      install_requires=[open('requirements.txt').read().strip().split('\n')],
      long_description=(open('README.rst').read() if os.path.exists('README.rst')
                        else ''),
      zip_safe=False)
