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
import glob
import logging
import os

from .core import AzureDLPath
from .transfer import ADLTransferClient
from .utils import commonprefix, datadir, read_block, tokenize

MAXRETRIES = 5

logger = logging.getLogger(__name__)


class ADLDownloader(object):
    """ Download remote file(s) using chunks and threads

    Launches multiple threads for efficient downloading, with `chunksize`
    assigned to each. The remote path can be a single file, a directory
    of files or a glob pattern.

    Parameters
    ----------
    adlfs: ADL filesystem instance
    rpath: str
        remote path/globstring to use to find remote files. Recursive glob
        patterns using `**` are not supported.
    lpath: str
        local path. If downloading a single file, will write to this specific
        file, unless it is an existing directory, in which case a file is
        created within it. If downloading multiple files, this is the root
        directory to write within. Will create directories as required.
    nthreads: int [None]
        Number of threads to use. If None, uses the number of cores.
    chunksize: int [2**22]
        Number of bytes in each chunk for splitting big files. Files smaller
        than this number will always be downloaded in a single thread.
    client: ADLTransferClient [None]
        Set an instance of ADLTransferClient when finer-grained control over
        transfer parameters is needed. Ignores `nthreads` and `chunksize` set
        by constructor.
    run: bool [True]
        Whether to begin executing immediately.

    See Also
    --------
    adlfs.transfer.ADLTransferClient
    """
    def __init__(self, adlfs, rpath, lpath, nthreads=None, chunksize=2**22,
                 client=None, run=True):
        if client:
            self.client = client
        else:
            self.client = ADLTransferClient(
                adlfs,
                name=tokenize(adlfs, rpath, lpath, chunksize),
                transfer=get_chunk,
                nthreads=nthreads,
                chunksize=chunksize,
                tmp_path=None,
                persist_path=os.path.join(datadir, 'downloads'))
        self.rpath = rpath
        self.lpath = lpath
        self._setup()
        if run:
            self.run()

    @property
    def hash(self):
        return self.client._name

    def _setup(self):
        """ Create set of parameters to loop over
        """
        if "*" not in self.rpath:
            rfiles = self.client._adlfs.walk(self.rpath)
        else:
            rfiles = self.client._adlfs.glob(self.rpath)
        if len(rfiles) > 1:
            prefix = commonprefix(rfiles)
            lfiles = [os.path.join(self.lpath, os.path.relpath(f, prefix))
                      for f in rfiles]
        elif rfiles:
            if os.path.exists(self.lpath) and os.path.isdir(self.lpath):
                lfiles = [os.path.join(self.lpath, os.path.basename(rfiles[0]))]
            else:
                lfiles = [self.lpath]
        else:
            raise ValueError('No files to download')
        self.rfiles = rfiles
        self.lfiles = lfiles

        for lfile, rfile in zip(lfiles, rfiles):
            fsize = self.client._adlfs.info(rfile)['length']
            self.client.submit(rfile, lfile, fsize)

    def run(self, nthreads=None, monitor=True):
        """ Populate transfer queue and execute downloads

        Parameters
        ----------
        nthreads: int [None]
            Override default nthreads, if given
        monitor: bool [True]
            To watch and wait (block) until completion. If False, `update()`
            should be called manually, otherwise process runs as "fire and
            forget".
        """
        def touch(self, src, dst):
            root = os.path.dirname(dst)
            if not os.path.exists(root) and root:
                # don't attempt to create current directory
                logger.debug('Creating directory %s', root)
                os.makedirs(root)
            logger.debug('Creating empty file %s', dst)
            with open(dst, 'wb'):
                pass

        self.client.run(nthreads, monitor, before_scatter=touch)

    @staticmethod
    def load():
        return ADLTransferClient.load(os.path.join(datadir, 'downloads'))

    def save(self, keep=True):
        self.client.save(keep)

    def __str__(self):
        progress = self.client.progress
        nchunks_orig = sum([1 for f in progress for chunk in f.chunks])
        nchunks = sum([1 for f in progress for chunk in f.chunks if chunk.state != 'finished'])
        return "<ADL Download: %s -> %s (%s of %s chunks remain)>" % (
            self.rpath, self.lpath, nchunks, nchunks_orig)

    __repr__ = __str__


def get_chunk(adlfs, src, dst, offset, size, retries=MAXRETRIES,
              shutdown_event=None):
    """ Download a piece of a remote file and write locally

    Internal function used by `download`.
    """
    with adlfs.open(src, 'rb', blocksize=0) as fin:
        with open(dst, 'rb+') as fout:
            if shutdown_event and shutdown_event.is_set():
                return
            tries = 0
            try:
                fout.seek(offset)
                fin.seek(offset)
                fout.write(fin.read(size))
            except Exception as e:
                # TODO : only some exceptions should be retriable
                logger.debug('Download failed %s, byte offset %s; %s, %s', dst,
                             offset, e, e.args)
                tries += 1
                if tries >= retries:
                    logger.debug('Aborting %s, byte offset %s', dst, offset)
                    raise
    logger.debug('Downloaded to %s, byte offset %s', dst, offset)


class ADLUploader(object):
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
        recursively) or glob pattern. Recursive glob patterns using `**` are
        not supported.
    nthreads: int [None]
        Number of threads to use. If None, uses the number of cores.
    chunksize: int [2**26]
        Number of bytes in each chunk for splitting big files. Files smaller
        than this number will always be sent in a single thread.
    client: ADLTransferClient [None]
        Set an instance of ADLTransferClient when finer-grained control over
        transfer parameters is needed. Ignores `nthreads`, `chunksize`, and
        `delimiter` set by constructor.
    run: bool [True]
        Whether to begin executing immediately.
    delimiter: byte(s) or None
        If set, will write blocks using delimiters in the backend, as well as
        split files for uploading on that delimiter.

    See Also
    --------
    adlfs.transfer.ADLTransferClient
    """
    def __init__(self, adlfs, rpath, lpath, nthreads=None, chunksize=256*2**20,
                 client=None, run=True, delimiter=None):
        if client:
            self.client = client
        else:
            self.client = ADLTransferClient(
                adlfs,
                name=tokenize(adlfs, rpath, lpath, chunksize),
                transfer=put_chunk,
                merge=merge_chunks,
                nthreads=nthreads,
                chunksize=chunksize,
                persist_path=os.path.join(datadir, 'uploads'),
                delimiter=delimiter)
        self.rpath = AzureDLPath(rpath)
        self.lpath = lpath
        self._setup()
        if run:
            self.run()

    @property
    def hash(self):
        return self.client._name

    def _setup(self):
        """ Create set of parameters to loop over
        """
        if "*" not in self.lpath:
            out = os.walk(self.lpath)
            lfiles = sum(([os.path.join(dir, f) for f in fnames] for
                         (dir, _, fnames) in out), [])
            if (not lfiles and os.path.exists(self.lpath) and
                    not os.path.isdir(self.lpath)):
                lfiles = [self.lpath]
        else:
            lfiles = glob.glob(self.lpath)
        if len(lfiles) > 1:
            prefix = commonprefix(lfiles)
            rfiles = [self.rpath / AzureDLPath(f).relative_to(prefix)
                      for f in lfiles]
        elif lfiles:
            if (self.client._adlfs.exists(self.rpath) and
                        self.client._adlfs.info(self.rpath)['type'] == "DIRECTORY"):
                rfiles = [self.rpath / AzureDLPath(lfiles[0]).name]
            else:
                rfiles = [self.rpath]
        else:
            raise ValueError('No files to upload')
        self.rfiles = rfiles
        self.lfiles = lfiles

        for lfile, rfile in zip(lfiles, rfiles):
            fsize = os.stat(lfile).st_size
            self.client.submit(lfile, rfile, fsize)

    def run(self, nthreads=None, monitor=True):
        self.client.run(nthreads, monitor)

    @staticmethod
    def load():
        return ADLTransferClient.load(os.path.join(datadir, 'uploads'))

    def save(self, keep=True):
        self.client.save(keep)

    def __str__(self):
        progress = self.client.progress
        nchunks_orig = sum([1 for f in progress for chunk in f.chunks])
        nchunks = sum([1 for f in progress for chunk in f.chunks if chunk.state != 'finished'])
        return "<ADL Upload: %s -> %s (%s of %s chunks remain)>" % (
            self.lpath, self.rpath, nchunks, nchunks_orig)

    __repr__ = __str__


def put_chunk(adlfs, src, dst, offset, size, retries=MAXRETRIES,
              delimiter=None, shutdown_event=None):
    """ Upload a piece of a local file

    Internal function used by `upload`.
    """
    with adlfs.open(dst, 'wb', delimiter=delimiter) as fout:
        end = offset + size
        miniblock = min(size, 4*2**20)
        with open(src, 'rb') as fin:
            for o in range(offset, end, miniblock):
                if shutdown_event and shutdown_event.is_set():
                    return False
                tries = 0
                while True:
                    try:
                        fout.write(read_block(fin, o, miniblock, delimiter))
                        break
                    except Exception as e:
                        # TODO : only some exceptions should be retriable
                        logger.debug('Upload failed %s, byte offset %s; %s, %s', src,
                                     o, e, e.args)
                        tries += 1
                        if tries >= retries:
                            logger.debug('Aborting %s, byte offset %s', src, offset)
                            raise
    logger.debug('Uploaded from %s, byte offset %s', src, offset)
    return True


def merge_chunks(adlfs, outfile, files, shutdown_event=None):
    return adlfs.concat(outfile, files)
