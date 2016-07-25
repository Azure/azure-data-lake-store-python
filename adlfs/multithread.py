"""
High performance multi-threaded module to up/download

Calls method in `core` with thread pool executor to ensure the network
is used to its maximum throughput.

Only implements upload and download of (massive) files and directory trees.
"""
from concurrent.futures import ThreadPoolExecutor, wait
import os


def get_chunk(adlfs, rfile, lfile, offset, size):
    with adlfs.open(rfile, 'rb', blocksize=0) as fin:
        with open(lfile, 'rb+') as fout:
            fout.seek(offset)
            fin.seek(offset)
            fout.write(fin.read(size))
            # TODO : log writing bytes


def threaded_file_downloader(adlfs, threadpool, rfile, lfile, chunksize):
    fsize = adlfs.info(rfile)['length']
    root = os.path.split(lfile)[0]
    if not os.path.exists(root) and root:
        # don't attempt to create current directory
        os.makedirs(root)
    offsets = range(0, fsize, chunksize)
    with open(lfile, 'wb'):
        pass
        # TODO : log creating file
    futures = [threadpool.submit(get_chunk, adlfs, rfile, lfile, o, chunksize)
               for o in offsets]
    # TODO : add 'done' callbacks and log
    return futures


def download(adlfs, rpath, lpath, nthreads=None, chunksize=2**26):
    rfiles = adlfs.walk(rpath)
    # TODO : wrap in a class, so can save and re-start failed/cancelled futures
    pool = ThreadPoolExecutor(max_workers=nthreads)
    if (len(rfiles) > 1) or (os.path.exists(lpath) and os.path.isdir(lpath)):
        lfiles = [os.path.join(lpath, os.path.split(f)[1]) for f in rfiles]
    else:
        lfiles = [lpath]
    # TODO : save in dict by parameters (or hash of)
    futures = sum([threaded_file_downloader(adlfs, pool, rfile, lfile,
                   chunksize) for (rfile, lfile) in zip(rfiles, lfiles)], [])
    return wait(futures)
