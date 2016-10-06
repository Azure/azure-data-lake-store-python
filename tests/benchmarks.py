import functools
import hashlib
import logging
import os
import shutil
import sys
import time

from azure.datalake.store import core, multithread
from azure.datalake.store.transfer import ADLTransferClient
from azure.datalake.store.utils import WIN
from tests.testing import md5sum


def benchmark(f):
    @functools.wraps(f)
    def wrapped(*args, **kwargs):
        print('[%s] starting...' % (f.__name__))
        start = time.time()
        result = f(*args, **kwargs)
        stop = time.time()
        print('[%s] finished in %2.4fs' % (f.__name__, stop - start))
        return result

    return wrapped


def mock_client(adl, nthreads):
    def transfer(adlfs, src, dst, offset, size, buffersize, blocksize, shutdown_event=None):
        pass

    def merge(adlfs, outfile, files, shutdown_event=None):
        pass

    return ADLTransferClient(
        adl,
        'foo',
        transfer=transfer,
        merge=merge,
        nthreads=nthreads)


def checksum(path):
    """ Generate checksum for file/directory content """
    if not os.path.exists(path):
        return None
    if os.path.isfile(path):
        return md5sum(path)
    partial_sums = []
    for root, dirs, files in os.walk(path):
        for f in files:
            filename = os.path.join(root, f)
            if os.path.exists(filename):
                partial_sums.append(str.encode(md5sum(filename)))
    return hashlib.md5(b''.join(sorted(partial_sums))).hexdigest()


def du(path):
    """ Find total size of content used by path """
    if os.path.isfile(path):
        return os.path.getsize(path)
    size = 0
    for root, dirs, files in os.walk(path):
        for f in files:
            size += os.path.getsize(os.path.join(root, f))
    return size


def verify(adl, progress, lfile, rfile):
    """ Confirm whether target file matches source file """
    print("local file      :", lfile)
    if os.path.exists(lfile):
        print("local file size :", du(lfile))
    else:
        print("local file size :", None)

    print("remote file     :", rfile)
    if adl.exists(rfile):
        print("remote file size:", adl.du(rfile, total=True, deep=True))
    else:
        print("remote file size:", None)

    for f in progress:
        chunks_finished = 0
        for chunk in f.chunks:
            if chunk.state == 'finished':
                chunks_finished += 1
            elif chunk.exception:
                print("[{}] file {} -> {}, chunk {} {}: {}".format(
                    chunk.state, f.src, f.dst, chunk.name, chunk.offset,
                    chunk.exception))
            else:
                print("[{}] file {} -> {}, chunk {} {}".format(
                    chunk.state, f.src, f.dst, chunk.name, chunk.offset))
        if f.exception:
            print("[{:4d}/{:4d} chunks] {} -> {}: {}".format(
                chunks_finished, len(f.chunks), f.src, f.dst, f.exception))
        else:
            print("[{:4d}/{:4d} chunks] {} -> {}".format(
                chunks_finished, len(f.chunks), f.src, f.dst))


@benchmark
def bench_upload_1_50gb(adl, lpath, rpath, config):
    up = multithread.ADLUploader(
        adl,
        lpath=lpath,
        rpath=rpath,
        **config[bench_upload_1_50gb.__name__])

    verify(adl, up.client.progress, lpath, rpath)


@benchmark
def bench_upload_50_1gb(adl, lpath, rpath, config):
    up = multithread.ADLUploader(
        adl,
        lpath=lpath,
        rpath=rpath,
        **config[bench_upload_50_1gb.__name__])

    verify(adl, up.client.progress, lpath, rpath)


@benchmark
def bench_download_1_50gb(adl, lpath, rpath, config):
    down = multithread.ADLDownloader(
        adl,
        lpath=lpath,
        rpath=rpath,
        **config[bench_download_1_50gb.__name__])

    verify(adl, down.client.progress, lpath, rpath)


@benchmark
def bench_download_50_1gb(adl, lpath, rpath, config):
    down = multithread.ADLDownloader(
        adl,
        lpath=lpath,
        rpath=rpath,
        **config[bench_download_50_1gb.__name__])

    verify(adl, down.client.progress, lpath, rpath)


if __name__ == '__main__':
    if len(sys.argv) <= 2:
        print("Usage: benchmarks.py local_path remote_path")
        sys.exit(1)

    localdir = sys.argv[1]
    remoteFolderName = sys.argv[2]

    adl = core.AzureDLFileSystem()

    # Log only Azure messages, ignoring 3rd-party libraries
    logging.basicConfig(
        format='%(asctime)s %(name)-17s %(levelname)-8s %(message)s')
    logger = logging.getLogger('azure.datalake.store')
    logger.setLevel(logging.INFO)

    # Required setup until outstanding issues are resolved
    adl.mkdir(remoteFolderName)

    # OS-specific settings

    if WIN:
        config = {
            'bench_upload_1_50gb': {
                'nthreads': 64,
                'buffersize': 32 * 2**20,
                'blocksize': 4 * 2**20
            },
            'bench_upload_50_1gb': {
                'nthreads': 64,
                'buffersize': 32 * 2**20,
                'blocksize': 32 * 2**20
            },
            'bench_download_1_50gb': {
                'nthreads': 64,
                'buffersize': 32 * 2**20,
                'blocksize': 4 * 2**20
            },
            'bench_download_50_1gb': {
                'nthreads': 64,
                'buffersize': 32 * 2**20,
                'blocksize': 4 * 2**20
            }
        }
    else:
        config = {
            'bench_upload_1_50gb': {
                'nthreads': 64,
                'buffersize': 4 * 2**20,
                'blocksize': 4 * 2**20
            },
            'bench_upload_50_1gb': {
                'nthreads': 64,
                'buffersize': 4 * 2**20,
                'blocksize': 4 * 2**20
            },
            'bench_download_1_50gb': {
                'nthreads': 16,
                'buffersize': 4 * 2**20,
                'blocksize': 4 * 2**20
            },
            'bench_download_50_1gb': {
                'nthreads': 16,
                'buffersize': 4 * 2**20,
                'blocksize': 4 * 2**20
            }
        }

    # Upload/download 1 50GB files

    lpath_up = os.path.join(localdir, '50gbfile.txt')
    lpath_down = os.path.join(localdir, '50gbfile.txt.out')
    rpath = remoteFolderName + '/50gbfile.txt'

    if adl.exists(rpath):
        adl.rm(rpath)
    if os.path.exists(lpath_down):
        os.remove(lpath_down)

    bench_upload_1_50gb(adl, lpath_up, rpath, config)
    bench_download_1_50gb(adl, lpath_down, rpath, config)
    print(checksum(lpath_up), lpath_up)
    print(checksum(lpath_down), lpath_down)

    # Upload/download 50 1GB files

    lpath_up = os.path.join(localdir, '50_1GB_Files')
    lpath_down = os.path.join(localdir, '50_1GB_Files.out')
    rpath = remoteFolderName + '/50_1GB_Files'

    if adl.exists(rpath):
        adl.rm(rpath, recursive=True)
    if os.path.exists(lpath_down):
        shutil.rmtree(lpath_down)

    bench_upload_50_1gb(adl, lpath_up, rpath, config)
    bench_download_50_1gb(adl, lpath_down, rpath, config)
    print(checksum(lpath_up), lpath_up)
    print(checksum(lpath_down), lpath_down)
