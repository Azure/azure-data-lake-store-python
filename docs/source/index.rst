adlfs
====

A pure-python interface to the Azure Data-lake Storage system, providing
pythonic file-system and file objects, high-performance up- and down-loader
and CLI commands.

This software is under active development and not yet recommended for general
use.

Installation
------------
Using ``pip``::

    pip install adlfs

Manually (bleeding edge):

* Download the repo from https://github.com/Azure/azure-data-lake-store-python
* checkout the ``dev`` branch
* install the requirememnts (``pip install -r requirements.txt``)
* install in develop mode (``python setup.py develop``)
* optionally: build the documentation (including this page) by running ``make html`` in the docs directory.


Auth
----

Although users can generate and supply their own tokens to the base file-system
class, and there is a password-based function in the ``lib`` module for
generating tokens, the most convenient way to supply credentials is via
environment parameters. This latter method is the one used by default in both
library and CLI usage. The following variables are required:

* azure_tenant_id
* azure_username
* azure_password
* azure_store_name
* azure_url_suffix (optional)

Pythonic Filesystem
-------------------

The ``AzureDLFileSystem`` object is the main API for library usage of this
package. It provides typical file-system operations on the remote azure
store

.. code-block:: python

    adl = AzureDLFileSystem()  # uses environment variables for auth
    adl.ls()  # list files in the root directory
    adl.ls(detail=True) # same, but with file details
    adl.walk('')  # list all files at any directory depth
    adl.du('', deep=True, total=True)  # total bytes usage
    adl.mkdir('newdir')  # create directory
    adl.touch('newdir/newfile') # create empty file
    adl.put('remotefile', 'localfile') # upload a local file

In addition, the file-system generates file objects that are compatible with
the python file interface, ensuring compatibility with libraries that work on
python files. The recommended way to use this is with a context manager
(otherwise, be sure to call ``close()`` on the file object).

.. code-block:: python

    with adl.open('newfile', 'wb') as f:
        f.write(b'index,a,b\n')
        f.tell()   # now at position 9
        f.flush()  # forces data upstream
        f.write(b'0,1,True')

    with adl.open('newfile', 'rb') as f:
        print(f.readlines())

    with adl.open('newfile', 'rb') as f:
        df = pd.read_csv(f) # read into pandas.

Performant up-/down-loading
---------------------------

Classes ``ADLUploader`` and ``ADLDownloader`` will chunk large files and send
many files to/from azure using multiple threads. A whole directory tree can
be transferred, files matching a specific glob-pattern or any particular file.

.. code-block:: python

    # download the whole directory structure using 5 threads, 16MB chunks
    ADLDownloader(adl, '', 'my_temp_dir', 5, 2**24)

Command Line Usage
------------------

The package provides the above functionality also from the command line
(bash, powershell, etc.). Two principle modes are supported: execution of one
particular file-system operation; and interactive mode in which multiple
operations can be executed in series.

.. code-block:: bash

    python cli.py ls -l

Execute the program without arguments to access documentation.


Contents
========

.. toctree::
   api
   :maxdepth: 2

Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
