import os
import pytest
import shutil
import tempfile

from adlfs.multithread import ADLDownloader
from adlfs.utils import azure


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
