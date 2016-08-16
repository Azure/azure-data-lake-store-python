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

from tests.testing import md5sum, my_vcr, open_azure

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
def test_download_single_file(tempdir):
    name = 'gdelt20150827.csv'
    checksum = 'ce58e380a5783120e31b3934bab1b04b'
    fname = os.path.join(tempdir, 'agelt.csv')
    size = 81840585
    lines = 217017

    with open_azure() as azure:
        # single chunk
        down = ADLDownloader(azure, name, fname, 1, size + 10)
        assert md5sum(fname) == checksum
        assert os.stat(fname).st_size == size
        assert linecount(fname) == lines
        os.remove(fname)

        # multiple chunks, one thread
        down = ADLDownloader(azure, name, fname, 1, 2**24)
        assert md5sum(fname) == checksum
        assert os.stat(fname).st_size == size
        assert linecount(fname) == lines
        os.remove(fname)


@my_vcr.use_cassette
def test_download_single_to_dir(tempdir):
    name = 'gdelt20150827.csv'
    checksum = 'ce58e380a5783120e31b3934bab1b04b'
    fname = os.path.join(tempdir, name)
    size = 81840585
    lines = 217017

    with open_azure() as azure:
        down = ADLDownloader(azure, name, tempdir, 1, 2**24)
        assert md5sum(fname) == checksum
        assert os.stat(fname).st_size == size
        assert linecount(fname) == lines
        os.remove(fname)


@my_vcr.use_cassette
def test_download_many(tempdir):
    with open_azure() as azure:
        down = ADLDownloader(azure, '', tempdir, 1, 2**24)
        nfiles = 0
        for dirpath, dirnames, filenames in os.walk(tempdir):
            nfiles += len(filenames)
        assert nfiles > 1


@my_vcr.use_cassette
def test_download_glob(tempdir):
    with open_azure() as azure:
        for directory in ['data/a/', 'data/b/']:
            azure.mkdir(test_dir + directory)
            for f in ['x.csv', 'y.csv', 'z.txt']:
                azure.touch(test_dir + directory + f)

        down = ADLDownloader(azure, test_dir + 'data/a/*.csv', tempdir, run=False)
        assert len(down.rfiles) == 2

        lfiles = [os.path.relpath(f, tempdir) for f in down.lfiles]
        assert lfiles == ['x.csv', 'y.csv']

        down = ADLDownloader(azure, test_dir + 'data/*/*.csv', tempdir, run=False)
        assert len(down.rfiles) == 4

        lfiles = [os.path.relpath(f, tempdir) for f in down.lfiles]
        assert lfiles == ['a/x.csv', 'a/y.csv', 'b/x.csv', 'b/y.csv']

        down = ADLDownloader(azure, test_dir + 'data/*/z.txt', tempdir, run=False)
        assert len(down.rfiles) == 2

        lfiles = [os.path.relpath(f, tempdir) for f in down.lfiles]
        assert lfiles == ['a/z.txt', 'b/z.txt']


@my_vcr.use_cassette
def test_save_down(tempdir):
    with open_azure(directory=None) as azure:
        down = ADLDownloader(azure, '', tempdir, 1, 2**24, run=False)
        down.save()

        alldownloads = ADLDownloader.load()
        assert down.hash in alldownloads

        down.save(keep=False)
        alldownloads = ADLDownloader.load()
        assert down.hash not in alldownloads


@pytest.mark.skip(reason="first assert fails during VCR playback")
@my_vcr.use_cassette
def test_interrupt_down(tempdir):
    with open_azure() as azure:
        down = ADLDownloader(azure, '', tempdir, 1, 2**24, run=False)

        def interrupt():
            os.kill(os.getpid(), signal.SIGINT)

        threading.Timer(1, interrupt).start()

        down.run()
        assert down.nchunks > 0

        down.run()
        assert down.nchunks == 0


@pytest.yield_fixture()
def local_files(tempdir):
    filenames = [os.path.join(tempdir, f) for f in ['bigfile', 'littlefile']]
    with open(filenames[0], 'wb') as f:
        for char in b"0 1 2 3 4 5 6 7 8 9".split():
            f.write(char * 1000000)
    with open(filenames[1], 'wb') as f:
        f.write(b'0123456789')
    nestpath = os.path.join(tempdir, 'nested1', 'nested2')
    os.makedirs(nestpath)
    for filename in ['a', 'b', 'c']:
        filenames.append(os.path.join(nestpath, filename))
        with open(os.path.join(nestpath, filename), 'wb') as f:
            f.write(b'0123456789')
    yield filenames


@my_vcr.use_cassette
def test_upload_one(local_files):
    bigfile, littlefile, a, b, c = local_files

    with open_azure() as azure:
        # single chunk
        up = ADLUploader(azure, test_dir+'littlefile', littlefile, nthreads=1)
        assert azure.info(test_dir+'littlefile')['length'] == 10

        # multiple chunks, one thread
        size = 10000000
        up = ADLUploader(azure, test_dir+'bigfile', bigfile, nthreads=1,
                         chunksize=size//5)
        assert azure.info(test_dir+'bigfile')['length'] == size

        azure.rm(test_dir+'bigfile')


@my_vcr.use_cassette
def test_upload_many(local_files):
    bigfile, littlefile, a, b, c = local_files
    root = os.path.dirname(bigfile)

    with open_azure() as azure:
        # single thread
        up = ADLUploader(azure, test_dir, root, nthreads=1)
        assert azure.info(test_dir+'littlefile')['length'] == 10
        assert azure.cat(test_dir+'/nested1/nested2/a') == b'0123456789'
        assert len(azure.du(test_dir, deep=True)) == 5
        assert azure.du(test_dir, deep=True, total=True) == 10000000 + 40


@my_vcr.use_cassette
def test_upload_glob(tempdir):
    with open_azure(directory=None) as azure:
        for directory in ['data/a/', 'data/b/']:
            d = os.path.join(tempdir, directory)
            os.makedirs(d)
            for data in ['x.csv', 'y.csv', 'z.txt']:
                with open(d + '/' + data, 'wb') as f:
                    f.write(b'0123456789')

        up = ADLUploader(azure, test_dir, tempdir + '/data/a/*.csv', run=False)
        assert len(up.lfiles) == 2

        rfiles = [os.path.relpath(f, test_dir) for f in up.rfiles]
        assert rfiles == ['x.csv', 'y.csv']

        up = ADLUploader(azure, test_dir, tempdir + '/data/*/*.csv', run=False)
        assert len(up.lfiles) == 4

        rfiles = [os.path.relpath(f, test_dir) for f in up.rfiles]
        assert rfiles == ['a/x.csv', 'a/y.csv', 'b/x.csv', 'b/y.csv']

        up = ADLUploader(azure, test_dir, tempdir + '/data/*/z.txt', run=False)
        assert len(up.lfiles) == 2

        rfiles = [os.path.relpath(f, test_dir) for f in up.rfiles]
        assert rfiles == ['a/z.txt', 'b/z.txt']


def test_save_up(local_files):
    bigfile, littlefile, a, b, c = local_files
    root = os.path.dirname(bigfile)

    with open_azure(directory=None) as azure:
        up = ADLUploader(azure, '', root, 1, 1000000, run=False)
        up.save()

        alluploads = ADLUploader.load()
        assert up.hash in alluploads

        up.save(keep=False)
        alluploads = ADLUploader.load()
        assert up.hash not in alluploads


@pytest.mark.skip(reason="first assert fails during VCR playback")
@my_vcr.use_cassette
def test_interrupt_up(local_files):
    bigfile, littlefile, a, b, c = local_files
    root = os.path.dirname(bigfile)

    with open_azure() as azure:
        up = ADLUploader(azure, test_dir, root, 1, 1000000, run=False)

        def interrupt():
            os.kill(os.getpid(), signal.SIGINT)

        threading.Timer(1, interrupt).start()

        up.run()
        assert up.nchunks > 0

        up.run()
        assert up.nchunks == 0
