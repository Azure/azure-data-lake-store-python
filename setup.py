#!/usr/bin/env python

import os
from setuptools import find_packages, setup
from io import open
import re
try:
    from azure_bdist_wheel import cmdclass
except ImportError:
    from distutils import log as logger
    logger.warn("Wheel is not available, disabling bdist_wheel hook")
    cmdclass = {}

with open('README.rst', encoding='utf-8') as f:
    readme = f.read()
with open('HISTORY.rst', encoding='utf-8') as f:
    history = f.read()

# Version extraction inspired from 'requests'
with open('azure/datalake/store/__init__.py', 'r') as fd:
    version = re.search(r'^__version__\s*=\s*[\'"]([^\'"]*)[\'"]',
                        fd.read(), re.MULTILINE).group(1)

if not version:
    raise RuntimeError('Cannot find version information')

setup(name='azure-datalake-store',
      version=version,
      description='Azure Data Lake Store Filesystem Client Library for Python',
      url='https://github.com/Azure/azure-data-lake-store-python',
      author='Microsoft Corporation',
      author_email='ptvshelp@microsoft.com',
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
          'Programming Language :: Python :: 3.6',
          'License :: OSI Approved :: MIT License',
      ],
      packages=find_packages(exclude=['tests']),
      install_requires=[
          'cffi',
          'adal>=0.4.2',
      ],
      extras_require={
          ":python_version<'3.4'": ['pathlib2'],
          ":python_version<='2.7'": ['futures'],
      },
      long_description=readme + '\n\n' + history,
      zip_safe=False,
      cmdclass=cmdclass
)
