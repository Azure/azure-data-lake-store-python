# -*- coding: utf-8 -*-
# coding=utf-8
# --------------------------------------------------------------------------
# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See License.txt in the project root for
# license information.
# --------------------------------------------------------------------------

"""
High performance multi-threaded module to up/download

Calls method in `core` with thread pool executor to ensure the network
is used to its maximum throughput.

Only implements upload and download of (massive) files and directory trees.
"""
from concurrent.futures import ThreadPoolExecutor, wait
import glob
import multiprocessing
import os
import pickle
import time
import uuid

from .utils import tokenize, logger, datadir

MAXRETRIES = 5


class ADLDownloader:
    """ Download remote file(s) using chunks and threads

    Launches multiple threads for efficient downloading, with `chunksize`
    assigned to each. The remote path can be a single file, a directory
    of files or a glob pattern.

    Parameters
    ----------
    adlfs: ADL filesystem instance
    rpath: str
        remote path/globstring to use to find remote files
    lpath: str
        local path. If downloading a single file, will write to this specific
        file, unless it is an existing directory, in which case a file is
        created within it. If downloading multiple files, this is the root
        directory to write within. Will create directories as required.
    nthreads: int [None]
        Number of threads to use. If None, uses the number of cores.
    chunksize: int [2**26]
        Number of bytes in each chunk for splitting big files. Files smaller
        than this number will always be downloaded in a single thread.
    run: bool (True)
        Whether to begin executing immediately.

    Returns
    -------
    downloader object
    """
    def __init__(self, adlfs, rpath, lpath, nthreads=None, chunksize=2**26,
                 run=True):
        self.adl = adlfs
        self.rpath = rpath
        self.lpath = lpath
        self.nthreads = nthreads
        self.chunksize = chunksize
        self.hash = tokenize(adlfs, rpath, lpath, chunksize)
        self._setup()
        if run:
            self.run()

    def _setup(self):
        """ Create set of parameters to loop over
        """
        if "*" not in self.rpath:
            rfiles = self.adl.walk(self.rpath)
        else:
            rfiles = self.adl.glob(self.rpath)
        if len(rfiles) > 1:
            lfiles = [os.path.join(self.lpath, os.path.relpath(f, self.rpath))
                      for f in rfiles]
        else:
            if os.path.exists(self.lpath) and os.path.isdir(self.lpath):
                lfiles = [os.path.join(self.lpath,
                                       os.path.basename(self.rpath))]
            else:
                lfiles = [self.lpath]
        self.rfiles = rfiles
        self.lfiles = lfiles
        self.progress = {}
        num = 0
        for lfile, rfile in zip(lfiles, rfiles):
            fsize = self.adl.info(rfile)['length']
            offsets = list(range(0, fsize, self.chunksize))
            self.progress[(rfile, lfile)] = {'waiting': offsets}
            num += len(offsets)
        self.nchunks = num
        self.nchunks_orig = num
        self.nfiles = len(rfiles)

    def run(self, nthreads=None, monitor=True):
        """ Create threadpool and execute downloads

        Parameters
        ----------
        nthreads: int (None)
            Override default nthreads, if given
        monitor: bool (True)
            To watch and wait (block) until completion. If False, `_check()`
            should be called manually, otherwise process runs as "fire and
            forget".
        """
        threads = nthreads or self.nthreads or multiprocessing.cpu_count()
        self.pool = ThreadPoolExecutor(threads)
        for rfile, lfile in self.progress:
            root = os.path.dirname(lfile)
            if not os.path.exists(root) and root:
                # don't attempt to create current directory
                logger.debug('Creating directory %s', root)
                os.makedirs(root)
            with open(lfile, 'wb'):
                dic = self.progress[(rfile, lfile)]
                logger.debug('Creating empty file %s', lfile)
                futures = [self.pool.submit(get_chunk, self.adl, rfile, lfile,
                                            o, self.chunksize)
                           for o in dic['waiting']]
                dic['futures'] = futures
        if monitor:
            self._monitor()

    def _check(self):
        for key in list(self.progress):
            dic = self.progress[key]
            for offset, future in zip(list(dic['waiting']),
                                      list(dic['futures'])):
                if future.done() and not future.cancelled():
                    dic['waiting'].remove(offset)
                    dic['futures'].remove(future)
                    self.nchunks -= 1
            if not dic['waiting']:
                logger.debug('File downloaded (%s -> %s)' % key)
                self.progress.pop(key)
                self.nfiles -= 1

    def _monitor(self):
        """ Wait for download to happen
        """
        try:
            while True:
                time.sleep(0.1)
                self._check()
                if self.nchunks == 0:
                    break
        except KeyboardInterrupt:
            logger.warning("%s suspended and persisted", self)
            for dic in self.progress.values():
                [f.cancel() for f in dic['futures']]
            self.pool.shutdown(wait=True)
            self._check()
        for dic in self.progress.values():
            dic['futures'] = []
        self.pool = None
        self.save()

    def __str__(self):
        return "<ADL Download: %s -> %s (%s of %s chunks remain)>" % (
            self.rpath, self.lpath, self.nchunks, self.nchunks_orig)

    __repr__ = __str__

    def save(self, keep=True):
        """ Persist this download, if it is incomplete, otherwise discard.

        Parameters
        ----------
        keep: bool (True)
            if False, remove from persisted downloads even if incomplete.
        """
        all_downloads = self.load()
        if self.nchunks and keep:
            all_downloads[self.hash] = self
        else:
            all_downloads.pop(self.hash, None)
        with open(os.path.join(datadir, 'downloads'), 'wb') as f:
            pickle.dump(all_downloads, f)

    @staticmethod
    def load():
        try:
            return pickle.load(open(os.path.join(datadir, 'downloads'), 'rb'))
        except:
            return {}


def get_chunk(adlfs, rfile, lfile, offset, size, retries=MAXRETRIES):
    """ Download a piece of a remote file and write locally

    Internal function used by `download`.
    """
    with adlfs.open(rfile, 'rb', blocksize=0) as fin:
        with open(lfile, 'rb+') as fout:
            tries = 0
            try:
                fout.seek(offset)
                fin.seek(offset)
                fout.write(fin.read(size))
            except Exception as e:
                # TODO : only some exceptions should be retriable
                logger.debug('Download failed %s, byte offset %s; %s, %s', lfile,
                             offset, e, e.args)
                tries += 1
                if tries >= retries:
                    logger.debug('Aborting %s, byte offset %s', lfile, offset)
                    raise
    logger.debug('Downloaded to %s, byte offset %s', lfile, offset)


class ADLUploader:
    """ Upload local file(s) using chunks and threads

    Launches multiple threads for efficient uploading, with `chunksize`
    assigned to each. The path can be a single file, a directory
    of files or a glob pattern.

    Parameters
    ----------
    adlfs: ADL filesystem instance
    rpath: str
        remote path to upload to; if multiple files, this is the dircetory
        root to write within
    lpath: str
        local path. Can be single file, directory (in which case, upload
        recursively) or glob pattern.
    nthreads: int [None]
        Number of threads to use. If None, uses the number of cores.
    chunksize: int [2**26]
        Number of bytes in each chunk for splitting big files. Files smaller
        than this number will always be sent in a single thread.
    run: bool (True)
        Whether to begin executing immediately.

    Returns
    -------
    uploader object
    """
    temp_upload_path = '/tmp/'

    def __init__(self, adlfs, rpath, lpath, nthreads=None, chunksize=2**26,
                 run=True):
        self.adl = adlfs
        self.rpath = rpath
        self.lpath = lpath
        self.nthreads = nthreads
        self.chunksize = chunksize
        self.hash = tokenize(adlfs, rpath, lpath, chunksize)
        self._setup()
        if run:
            self.run()

    def _setup(self):
        """ Create set of parameters to loop over
        """
        if "*" not in self.lpath:
            out = os.walk(self.lpath)
            lfiles = sum(([os.path.join(dir, f) for f in fnames] for
                         (dir, dirs, fnames) in out), [])
            if (not lfiles and os.path.exists(self.lpath) and
                    not os.path.isdir(self.lpath)):
                lfiles = [self.lpath]
        else:
            lfiles = glob.glob(self.lpath)
        if len(lfiles) > 1:
            rfiles = [os.path.join(self.rpath, os.path.relpath(f, self.lpath))
                      for f in lfiles]
        else:
            if (self.adl.exists(self.rpath) and
                        self.adl.info(self.rpath)['type'] == "DIRECTORY"):
                rfiles = [os.path.join(self.rpath,
                                       os.path.basename(self.lpath))]
            else:
                rfiles = [self.rpath]
        self.rfiles = rfiles
        self.lfiles = lfiles
        self.progress = {}
        num = 0
        for lfile, rfile in zip(lfiles, rfiles):
            fsize = os.stat(lfile).st_size
            offsets = list(range(0, fsize, self.chunksize))
            unique = uuid.uuid1().hex[:10]
            parts = [self.temp_upload_path+unique+"_%i" % i for i in offsets]
            self.progress[(rfile, lfile)] = {'waiting': offsets, 'uuid': unique,
                                             'files': parts, 'final': None,
                                             'futures': []}
            num += len(offsets)
        self.nchunks = num
        self.nchunks_orig = num
        self.nfiles = len(rfiles)

    def run(self, nthreads=None, monitor=True):
        threads = nthreads or self.nthreads or multiprocessing.cpu_count()
        self.pool = ThreadPoolExecutor(threads)

        for (rfile, lfile), dic in self.progress.items():
            unique = dic['uuid']
            if len(dic['waiting']) > 1:
                parts = [self.temp_upload_path+unique+"_%i" % i for i
                         in dic['waiting']]
                futures = [self.pool.submit(put_chunk, self.adl, part, lfile, o,
                                            self.chunksize)
                           for part, o in zip(parts, dic['waiting'])]
                dic['futures'] = futures
            else:
                dic['final'] = self.pool.submit(self.adl.put, lfile, rfile)
        if monitor:
            self._monitor()

    def _finalize(self, rfile, lfile):
        dic = self.progress[(rfile, lfile)]
        parts = dic['files']
        dic['final'] = self.pool.submit(self.adl.concat, rfile, parts)

    def _check(self):
        for key in list(self.progress):
            dic = self.progress[key]
            for offset, future in zip(list(dic['waiting']),
                                      list(dic['futures'])):
                if future.done() and not future.cancelled():
                    dic['waiting'].remove(offset)
                    dic['futures'].remove(future)
                    self.nchunks -= 1
            if not dic['waiting'] or dic['final']:
                if dic['final'] is None:
                    logger.debug('Finalizing (%s -> %s)' % (key[1], key[0]))
                    self._finalize(*key)
                elif dic['final'].done():
                    self.adl.invalidate_cache(key[0])
                    logger.debug('File uploaded (%s -> %s)' % (key[1], key[0]))
                    self.progress.pop(key)
                    self.nfiles -= 1
                    self.nchunks -= len(dic['waiting'])

    def _monitor(self):
        """ Wait for upload to happen
        """
        try:
            while True:
                time.sleep(0.1)
                self._check()
                if self.nchunks == 0 and self.nfiles == 0:
                    break
        except KeyboardInterrupt:
            logger.warning("%s suspended and persisted", self)
            for dic in self.progress.values():
                [f.cancel() for f in dic['futures']]
            self.pool.shutdown(wait=True)
            self._check()
        for dic in self.progress.values():
            dic['futures'] = []
            dic['final'] = None
        self.pool = None
        self.save()

    def __str__(self):
        return "<ADL Upload: %s -> %s (%s of %s chunks remain)>" % (self.lpath,
                    self.rpath, self.nchunks, self.nchunks_orig)

    __repr__ = __str__

    def save(self, keep=True):
        """ Persist this upload, if it is incomplete, otherwise discard.

        Parameters
        ----------
        keep: bool (True)
            if False, remove from persisted downloads even if incomplete.
        """
        all_uploads = self.load()
        if self.nchunks and self.nfiles and keep:
            all_uploads[self.hash] = self
        else:
            all_uploads.pop(self.hash, None)
        with open(os.path.join(datadir, 'uploads'), 'wb') as f:
            pickle.dump(all_uploads, f)

    @staticmethod
    def load():
        try:
            return pickle.load(open(os.path.join(datadir, 'uploads'), 'rb'))
        except:
            return {}


def put_chunk(adlfs, rfile, lfile, offset, size, retries=MAXRETRIES):
    """ Upload a piece of a local file

    Internal function used by `upload`.
    """
    with adlfs.open(rfile, 'wb') as fout:
        with open(lfile, 'rb') as fin:
            tries = 0
            try:
                fin.seek(offset)
                fout.write(fin.read(size))
            except Exception as e:
                # TODO : only some exceptions should be retriable
                logger.debug('Upload failed %s, byte offset %s; %s, %s', lfile,
                             offset, e, e.args)
                tries += 1
                if tries >= retries:
                    logger.debug('Aborting %s, byte offset %s', lfile, offset)
                    raise
    logger.debug('Uploaded from %s, byte offset %s', lfile, offset)


