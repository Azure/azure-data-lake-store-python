adlfs
====

|Build Status| |Doc Status|

adlfs is a file-system management system in python for the
Azure Data-Lake Store.

To install:

.. code-block:: bash

    > pip install -r requirements.txt
    > python setup.py install


To run tests, you are required to set the following environment variables:
azure_tenant_id, azure_username, azure_password, azure_store_name

To play with the code, here is a starting point:

.. code-block:: python

    from adlfs import core, lib, multithread
    token = lib.auth(tenant_id, username, password)
    adl = core.AzureDLFileSystem(store_name, token)

    # typical operations
    adl.ls('')
    adl.ls('tmp/', detail=True)
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
