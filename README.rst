Microsoft Azure Data Lake Store Filesystem Library for Python
=============================================================

.. image:: https://travis-ci.org/Azure/azure-data-lake-store-python.svg?branch=dev
    :target: https://travis-ci.org/Azure/azure-data-lake-store-python
.. image:: https://coveralls.io/repos/github/Azure/azure-data-lake-store-python/badge.svg?branch=master
    :target: https://coveralls.io/github/Azure/azure-data-lake-store-python?branch=master

This project is the Python filesystem library for Azure Data Lake Store.

INSTALLATION
============

To install from source instead of pip (for local testing and development):

.. code-block:: bash

    > pip install -r dev_requirements.txt
    > python setup.py develop

Usage: Sample Code
==================

To play with the code, here is a starting point:

.. code-block:: python

    from azure.datalake.store import core, lib, multithread
    token = lib.auth(tenant_id, username, password)
    adl = core.AzureDLFileSystem(token, store_name=store_name)

    # typical operations
    adl.ls('')
    adl.ls('tmp/', detail=True)
    adl.ls('tmp/', detail=True, invalidate_cache=True)
    adl.cat('littlefile')
    adl.head('gdelt20150827.csv')

    # file-like object
    with adl.open('gdelt20150827.csv', blocksize=2**20) as f:
        print(f.readline())
        print(f.readline())
        print(f.readline())
        # could have passed f to any function requiring a file object:
        # pandas.read_csv(f)

    with adl.open('anewfile', 'wb') as f:
        # data is written on flush/close, or when buffer is bigger than
        # blocksize
        f.write(b'important data')

    adl.du('anewfile')

    # recursively download the whole directory tree with 5 threads and
    # 16MB chunks
    multithread.ADLDownloader(adl, "", 'my_temp_dir', 5, 2**24)

Progress can be tracked using a callback function in the form `track(current, total)`
When passed, this will keep track of transferred bytes and be called on each complete chunk.

Here's an example using the Azure CLI progress controller as the `progress_callback`:

.. code-block:: python

    from cli.core.application import APPLICATION

    def _update_progress(current, total):
        hook = APPLICATION.get_progress_controller(det=True)
        hook.add(message='Alive', value=current, total_val=total)
        if total == current:
            hook.end()

    ...
    ADLUploader(client, destination_path, source_path, thread_count, overwrite=overwrite,
            chunksize=chunk_size,
            buffersize=buffer_size,
            blocksize=block_size,
            progress_callback=_update_progress)

This will output a progress bar to the stdout:

.. code-block:: bash

    Alive[#########################                                       ]  40.0881%
    
    Finished[#############################################################]  100.0000%

Usage: Command Line Sample
==========================

To interact with the API at a higher-level, you can use the provided
command-line interface in "samples/cli.py". You will need to set
the appropriate environment variables 

* :code:`azure_username`

* :code:`azure_password`

* :code:`azure_data_lake_store_name`

* :code:`azure_subscription_id`

* :code:`azure_resource_group_name`

* :code:`azure_service_principal`

* :code:`azure_service_principal_secret`

to connect to the Azure Data Lake Store. Optionally, you may need to define :code:`azure_tenant_id` or :code:`azure_data_lake_store_url_suffix`.

Below is a simple sample, with more details beyond.


.. code-block:: bash

    python samples\cli.py ls -l

Execute the program without arguments to access documentation.

To start the CLI in interactive mode, run "python samples/cli.py"
and then type "help" to see all available commands (similiar to Unix utilities):

.. code-block:: bash

    > python samples/cli.py
    azure> help

    Documented commands (type help <topic>):
    ========================================
    cat    chmod  close  du      get   help  ls     mv   quit  rmdir  touch
    chgrp  chown  df     exists  head  info  mkdir  put  rm    tail

    azure>


While still in interactive mode, you can run "ls -l" to list the entries in the
home directory ("help ls" will show the command's usage details). If you're not
familiar with the Unix/Linux "ls" command, the columns represent 1) permissions,
2) file owner, 3) file group, 4) file size, 5-7) file's modification time, and
8) file name.

.. code-block:: bash

    > python samples/cli.py
    azure> ls -l
    drwxrwx--- 0123abcd 0123abcd         0 Aug 02 12:44 azure1
    -rwxrwx--- 0123abcd 0123abcd   1048576 Jul 25 18:33 abc.csv
    -r-xr-xr-x 0123abcd 0123abcd        36 Jul 22 18:32 xyz.csv
    drwxrwx--- 0123abcd 0123abcd         0 Aug 03 13:46 tmp
    azure> ls -l --human-readable
    drwxrwx--- 0123abcd 0123abcd   0B Aug 02 12:44 azure1
    -rwxrwx--- 0123abcd 0123abcd   1M Jul 25 18:33 abc.csv
    -r-xr-xr-x 0123abcd 0123abcd  36B Jul 22 18:32 xyz.csv
    drwxrwx--- 0123abcd 0123abcd   0B Aug 03 13:46 tmp
    azure>


To download a remote file, run "get remote-file [local-file]". The second
argument, "local-file", is optional. If not provided, the local file will be
named after the remote file minus the directory path.

.. code-block:: bash

    > python samples/cli.py
    azure> ls -l
    drwxrwx--- 0123abcd 0123abcd         0 Aug 02 12:44 azure1
    -rwxrwx--- 0123abcd 0123abcd   1048576 Jul 25 18:33 abc.csv
    -r-xr-xr-x 0123abcd 0123abcd        36 Jul 22 18:32 xyz.csv
    drwxrwx--- 0123abcd 0123abcd         0 Aug 03 13:46 tmp
    azure> get xyz.csv
    2016-08-04 18:57:48,603 - ADLFS - DEBUG - Creating empty file xyz.csv
    2016-08-04 18:57:48,604 - ADLFS - DEBUG - Fetch: xyz.csv, 0-36
    2016-08-04 18:57:49,726 - ADLFS - DEBUG - Downloaded to xyz.csv, byte offset 0
    2016-08-04 18:57:49,734 - ADLFS - DEBUG - File downloaded (xyz.csv -> xyz.csv)
    azure>


It is also possible to run in command-line mode, allowing any available command
to be executed separately without remaining in the interpreter.

For example, listing the entries in the home directory:

.. code-block:: bash

    > python samples/cli.py ls -l
    drwxrwx--- 0123abcd 0123abcd         0 Aug 02 12:44 azure1
    -rwxrwx--- 0123abcd 0123abcd   1048576 Jul 25 18:33 abc.csv
    -r-xr-xr-x 0123abcd 0123abcd        36 Jul 22 18:32 xyz.csv
    drwxrwx--- 0123abcd 0123abcd         0 Aug 03 13:46 tmp
    >


Also, downloading a remote file:

.. code-block:: bash

    > python samples/cli.py get xyz.csv
    2016-08-04 18:57:48,603 - ADLFS - DEBUG - Creating empty file xyz.csv
    2016-08-04 18:57:48,604 - ADLFS - DEBUG - Fetch: xyz.csv, 0-36
    2016-08-04 18:57:49,726 - ADLFS - DEBUG - Downloaded to xyz.csv, byte offset 0
    2016-08-04 18:57:49,734 - ADLFS - DEBUG - File downloaded (xyz.csv -> xyz.csv)
    >

Tests
=====

For detailed documentation about our test framework, please visit the 
`tests folder <https://github.com/Azure/azure-data-lake-store-python/tree/master/tests>`__.

Need Help?
==========

Be sure to check out the Microsoft Azure `Developer Forums on Stack Overflow <http://go.microsoft.com/fwlink/?LinkId=234489>`__
if you have trouble with the provided code. Most questions are tagged `azure and python <https://stackoverflow.com/questions/tagged/azure+python>`__.


Contribute Code or Provide Feedback
===================================

If you would like to become an active contributor to this project please
follow the instructions provided in `Microsoft Azure Projects Contribution Guidelines <http://azure.github.io/guidelines/>`__. 
Furthermore, check out `GUIDANCE.md <https://github.com/Azure/azure-data-lake-store-python/blob/master/GUIDANCE.md>`__ 
for specific information related to this project.

If you encounter any bugs with the library please file an issue in the
`Issues <https://github.com/Azure/azure-data-lake-store-python/issues>`__
section of the project.


Code of Conduct
===============
This project has adopted the `Microsoft Open Source Code of Conduct <https://opensource.microsoft.com/codeofconduct/>`__. 
For more information see the `Code of Conduct FAQ <https://opensource.microsoft.com/codeofconduct/faq/>`__ or contact 
`opencode@microsoft.com <mailto:opencode@microsoft.com>`__ with any additional questions or comments.
