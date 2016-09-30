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
from .exceptions import FileExistsError
from .transfer import ADLTransferClient
from .utils import commonprefix, datadir, read_block, tokenize

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
    chunksize: int [2**28]
        Number of bytes for a chunk. Large files are split into chunks. Files
        smaller than this number will always be transferred in a single thread.
    blocksize: int [2**22]
        Number of bytes for a block. Within each chunk, we write a smaller
        block for each API call. This block cannot be bigger than a chunk.
    client: ADLTransferClient [None]
        Set an instance of ADLTransferClient when finer-grained control over
        transfer parameters is needed. Ignores `nthreads` and `chunksize` set
        by constructor.
    run: bool [True]
        Whether to begin executing immediately.
    overwrite: bool [False]
        Whether to forcibly overwrite existing files/directories. If False and
        local path is a directory, will quit regardless if any files would be
        overwritten or not. If True, only matching filenames are actually
        overwritten.

    See Also
    --------
    adlfs.transfer.ADLTransferClient
    """
    def __init__(self, adlfs, rpath, lpath, nthreads=None, chunksize=2**28,
                 blocksize=2**22, client=None, run=True, overwrite=False,
                 verbose=True):
        if not overwrite and adlfs.exists(rpath):
            raise FileExistsError(lpath)
        if client:
            self.client = client
        else:
            self.client = ADLTransferClient(
                adlfs,
                name=tokenize(adlfs, rpath, lpath, chunksize, blocksize),
                transfer=get_chunk,
                nthreads=nthreads,
                chunksize=chunksize,
                blocksize=blocksize,
                chunked=False,
                persist_path=os.path.join(datadir, 'downloads'),
                verbose=verbose)
        self.rpath = rpath
        self.lpath = lpath
        self._overwrite = overwrite
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

        self.client.run(nthreads, monitor, before_start=touch)

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


def get_chunk(adlfs, src, dst, offset, size, blocksize, shutdown_event=None):
    """ Download a piece of a remote file and write locally

    Internal function used by `download`.
    """
    nbytes = 0
    try:
        with adlfs.open(src, 'rb') as fin:
            end = offset + size
            miniblock = min(size, blocksize)
            with open(dst, 'rb+') as fout:
                fout.seek(offset)
                fin.seek(offset)
                for o in range(offset, end, miniblock):
                    if shutdown_event and shutdown_event.is_set():
                        return nbytes, None
                    data = fin.read(miniblock)
                    nwritten = fout.write(data)
                    if nwritten:
                        nbytes += nwritten
    except Exception as e:
        exception = repr(e)
        logger.debug('Download failed %s; %s', dst, exception)
        return nbytes, exception
    logger.debug('Downloaded to %s, byte offset %s', dst, offset)
    return nbytes, None


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
    chunksize: int [2**28]
        Number of bytes for a chunk. Large files are split into chunks. Files
        smaller than this number will always be transferred in a single thread.
    blocksize: int [2**25]
        Number of bytes for a block. Within each chunk, we write a smaller
        block for each API call. This block cannot be bigger than a chunk.
    client: ADLTransferClient [None]
        Set an instance of ADLTransferClient when finer-grained control over
        transfer parameters is needed. Ignores `nthreads`, `chunksize`, and
        `delimiter` set by constructor.
    run: bool [True]
        Whether to begin executing immediately.
    delimiter: byte(s) or None
        If set, will write blocks using delimiters in the backend, as well as
        split files for uploading on that delimiter.
    overwrite: bool [False]
        Whether to forcibly overwrite existing files/directories. If False and
        remote path is a directory, will quit regardless if any files would be
        overwritten or not. If True, only matching filenames are actually
        overwritten.

    See Also
    --------
    adlfs.transfer.ADLTransferClient
    """
    def __init__(self, adlfs, rpath, lpath, nthreads=None, chunksize=2**28,
                 blocksize=2**25, client=None, run=True, delimiter=None,
                 overwrite=False, verbose=True):
        if not overwrite and os.path.exists(lpath):
            raise FileExistsError(rpath)
        if client:
            self.client = client
        else:
            self.client = ADLTransferClient(
                adlfs,
                name=tokenize(adlfs, rpath, lpath, chunksize, blocksize),
                transfer=put_chunk,
                merge=merge_chunks,
                nthreads=nthreads,
                chunksize=chunksize,
                blocksize=blocksize,
                persist_path=os.path.join(datadir, 'uploads'),
                delimiter=delimiter,
                verbose=verbose)
        self.rpath = AzureDLPath(rpath)
        self.lpath = lpath
        self._overwrite = overwrite
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


def put_chunk(adlfs, src, dst, offset, size, blocksize, delimiter=None,
              shutdown_event=None):
    """ Upload a piece of a local file

    Internal function used by `upload`.
    """
    nbytes = 0
    try:
        with adlfs.open(dst, 'wb', delimiter=delimiter) as fout:
            end = offset + size
            miniblock = min(size, blocksize)
            with open(src, 'rb') as fin:
                for o in range(offset, end, miniblock):
                    if shutdown_event and shutdown_event.is_set():
                        return nbytes, None
                    data = read_block(fin, o, miniblock, delimiter)
                    nwritten = fout.write(data)
                    if nwritten:
                        nbytes += nwritten
    except Exception as e:
        exception = repr(e)
        logger.debug('Upload failed %s; %s', src, exception)
        return nbytes, exception
    logger.debug('Uploaded from %s, byte offset %s', src, offset)
    return nbytes, None


def merge_chunks(adlfs, outfile, files, shutdown_event=None):
    try:
        adlfs.concat(outfile, files)
    except Exception as e:
        exception = repr(e)
        logger.debug('Merged failed %s; %s', outfile, exception)
        return exception
    logger.debug('Merged %s', outfile)
    return None
