# -*- coding: utf-8 -*-
# coding=utf-8
# --------------------------------------------------------------------------
# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See License.txt in the project root for
# license information.
# --------------------------------------------------------------------------

import os
import pytest
import shutil
import signal
import tempfile
import threading

from adlfs.multithread import ADLDownloader, ADLUploader

from tests import my_vcr

test_dir = 'azure_test_dir/'


@pytest.yield_fixture()
def tempdir():
    tmpdir = tempfile.mkdtemp()
    try:
        yield tmpdir
    finally:
        shutil.rmtree(tmpdir, True)


def linecount(infile):
    lines = 0
    with open(infile) as f:
        for line in f:
            lines += 1
    return lines

# TODO : when the uploader is ready, should place file in temp location
# rather than rely on file already in place.


@my_vcr.use_cassette
def test_download_single_file(azure, tempdir):

    fname = os.path.join(tempdir, 'agelt.csv')
    size = 81840585
    lines = 217017

    # single chunk
    down = ADLDownloader(azure, 'gdelt20150827.csv', fname, 1, size + 10)
    assert os.stat(fname).st_size == size
    assert linecount(fname) == lines
    os.remove(fname)

    # multiple chunks, one thread
    down = ADLDownloader(azure, 'gdelt20150827.csv', fname, 1, 2**24)
    assert os.stat(fname).st_size == size
    assert linecount(fname) == lines
    os.remove(fname)

    # one chunk, multiple threads
    down = ADLDownloader(azure, 'gdelt20150827.csv', fname, 4, size + 10)
    assert os.stat(fname).st_size == size
    assert linecount(fname) == lines
    os.remove(fname)

    # multiple chunks, multiple threads, all simultaneous
    down = ADLDownloader(azure, 'gdelt20150827.csv', fname, 5, 2**24)
    assert os.stat(fname).st_size == size
    assert linecount(fname) == lines
    os.remove(fname)

    # multiple chunks, multiple threads, oversubscribed
    down = ADLDownloader(azure, 'gdelt20150827.csv', fname, 2, 2**24)
    assert os.stat(fname).st_size == size
    assert linecount(fname) == lines
    os.remove(fname)


@my_vcr.use_cassette
def test_download_single_to_dir(azure, tempdir):
    fname = os.path.join(tempdir, 'gdelt20150827.csv')
    size = 81840585
    lines = 217017
    down = ADLDownloader(azure, 'gdelt20150827.csv', tempdir, 5, 2**24)
    assert os.stat(fname).st_size == size
    assert linecount(fname) == lines
    os.remove(fname)


@my_vcr.use_cassette
def test_download_many(azure, tempdir):
    down = ADLDownloader(azure, '', tempdir, 5, 2**24)
    nfiles = 0
    for dirpath, dirnames, filenames in os.walk(tempdir):
        nfiles += len(filenames)
    assert nfiles > 1


@my_vcr.use_cassette
def test_save_down(azure, tempdir):
    down = ADLDownloader(azure, '', tempdir, 5, 2**24, run=False)
    down.save()

    alldownloads = ADLDownloader.load()
    assert down.hash in alldownloads

    down.save(keep=False)
    alldownloads = ADLDownloader.load()
    assert down.hash not in alldownloads


@my_vcr.use_cassette
def test_interrupt_down(azure, tempdir):
    down = ADLDownloader(azure, '', tempdir, 5, 2**24, run=False)

    def interrupt():
        os.kill(os.getpid(), signal.SIGINT)

    threading.Timer(1, interrupt).start()

    down.run()
    assert down.nchunks > 0

    down.run()
    assert down.nchunks == 0


@my_vcr.use_cassette
@pytest.yield_fixture()
def local_files(tempdir):
    filenames = [os.path.join(tempdir, f) for f in ['bigfile', 'littlefile']]
    with open(filenames[0], 'wb') as f:
        for char in b"0 1 2 3 4 5 6 7 8 9".split():
            f.write(char * 1000000)
    with open(filenames[1], 'wb') as f:
        f.write(b'0123456789')
    nestpath = os.sep.join([tempdir, 'nested1', 'nested2'])
    os.makedirs(nestpath)
    for filename in ['a', 'b', 'c']:
        filenames.append(os.path.join(nestpath, filename))
        with open(os.path.join(nestpath, filename), 'wb') as f:
            f.write(b'0123456789')
    yield filenames


@my_vcr.use_cassette
def test_upload_one(azure, local_files):
    bigfile, littlefile, a, b, c = local_files

    # single chunk
    up = ADLUploader(azure, test_dir+'littlefile', littlefile)
    assert azure.info(test_dir+'littlefile')['length'] == 10

    # multiple chunks, one thread
    size = 10000000
    up = ADLUploader(azure, test_dir+'bigfile', bigfile, nthreads=1,
                     chunksize=size//5)
    assert azure.info(test_dir+'bigfile')['length'] == size

    azure.rm(test_dir+'bigfile')

    # multiple chunks, multiple threads
    up = ADLUploader(azure, test_dir+'bigfile', bigfile, nthreads=5,
                     chunksize=size//5)
    assert azure.info(test_dir+'bigfile')['length'] == size


@my_vcr.use_cassette
def test_upload_many(azure, local_files):
    bigfile, littlefile, a, b, c = local_files
    root = os.path.dirname(bigfile)

    # single thread
    up = ADLUploader(azure, test_dir, root)
    assert azure.info(test_dir+'littlefile')['length'] == 10
    assert azure.cat(test_dir+'/nested1/nested2/a') == b'0123456789'
    assert len(azure.du(test_dir, deep=True)) == 5
    assert azure.du(test_dir, deep=True, total=True) == 10000000 + 40


@my_vcr.use_cassette
def test_save_up(azure, local_files):
    bigfile, littlefile, a, b, c = local_files
    root = os.path.dirname(bigfile)

    up = ADLUploader(azure, '', root, 5, 1000000, run=False)
    up.save()

    alluploads = ADLUploader.load()
    assert up.hash in alluploads

    up.save(keep=False)
    alluploads = ADLUploader.load()
    assert up.hash not in alluploads


@my_vcr.use_cassette
def test_interrupt_up(azure, local_files):
    bigfile, littlefile, a, b, c = local_files
    root = os.path.dirname(bigfile)

    up = ADLUploader(azure, test_dir, root, 5, 1000000, run=False)

    def interrupt():
        os.kill(os.getpid(), signal.SIGINT)

    threading.Timer(1, interrupt).start()

    up.run()
    assert up.nchunks > 0

    up.run()
    assert up.nchunks == 0
