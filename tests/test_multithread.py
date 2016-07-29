import os
import pytest
import shutil
import signal
import tempfile
import threading

from adlfs.multithread import ADLDownloader, ADLUploader
from adlfs.utils import azure

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


def test_download_single_to_dir(azure, tempdir):
    fname = os.path.join(tempdir, 'gdelt20150827.csv')
    size = 81840585
    lines = 217017
    down = ADLDownloader(azure, 'gdelt20150827.csv', tempdir, 5, 2**24)
    assert os.stat(fname).st_size == size
    assert linecount(fname) == lines
    os.remove(fname)


def test_download_many(azure, tempdir):
    down = ADLDownloader(azure, '', tempdir, 5, 2**24)
    nfiles = 0
    for dirpath, dirnames, filenames in os.walk(tempdir):
        nfiles += len(filenames)
    assert nfiles > 1


def test_save(azure, tempdir):
    down = ADLDownloader(azure, '', tempdir, 5, 2**24, run=False)
    down.save()

    alldownloads = ADLDownloader.load()
    assert down.hash in alldownloads

    down.save(keep=False)
    alldownloads = ADLDownloader.load()
    assert down.hash not in alldownloads


def test_interrupt(azure, tempdir):
    down = ADLDownloader(azure, '', tempdir, 5, 2**24, run=False)

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
    nestpath = os.sep.join([tempdir, 'nested1', 'nested2'])
    os.makedirs(nestpath)
    for filename in ['a', 'b', 'c']:
        filenames.append(filename)
        with open(os.path.join(nestpath, filename), 'wb') as f:
            f.write(b'0123456789')
    yield filenames


def test_upload_simple(azure, local_files):
    bigfile, littlefile, a, b, c = local_files
    up = ADLUploader(azure, test_dir+'littlefile', littlefile)
    assert azure.info(test_dir+'littlefile')['length'] == 10
