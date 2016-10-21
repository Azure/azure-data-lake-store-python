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
import pickle

from .core import AzureDLPath, _fetch_range
from .exceptions import FileExistsError
from .transfer import ADLTransferClient
from .utils import commonprefix, datadir, read_block, tokenize

logger = logging.getLogger(__name__)


def save(instance, filename, keep=True):
    if os.path.exists(filename):
        all_downloads = load(filename)
    else:
        all_downloads = {}
    if not instance.client._fstates.contains_all('finished') and keep:
        all_downloads[instance._name] = instance
    else:
        all_downloads.pop(instance._name, None)
    try:
        # persist failure should not halt things
        with open(filename, 'wb') as f:
            pickle.dump(all_downloads, f)
    except IOError:
        logger.debug("Persist failed: %s" % filename)


def load(filename):
    try:
        return pickle.load(open(filename, 'rb'))
    except:
        return {}


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
    buffersize: int [2**22]
        Number of bytes for internal buffer. This block cannot be bigger than
        a chunk and cannot be smaller than a block.
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
    azure.datalake.store.transfer.ADLTransferClient
    """
    def __init__(self, adlfs, rpath, lpath, nthreads=None, chunksize=2**28,
                 buffersize=2**22, blocksize=2**22, client=None, run=True,
                 overwrite=False, verbose=True):
        if not overwrite and os.path.exists(lpath):
            raise FileExistsError(lpath)
        if client:
            self.client = client
        else:
            self.client = ADLTransferClient(
                adlfs,
                transfer=get_chunk,
                nthreads=nthreads,
                chunksize=chunksize,
                buffersize=buffersize,
                blocksize=blocksize,
                chunked=False,
                verbose=verbose,
                parent=self)
        self._name = tokenize(adlfs, rpath, lpath, chunksize, blocksize)
        self.rpath = rpath
        self.lpath = lpath
        self._overwrite = overwrite
        self._setup()
        if run:
            self.run()

    def save(self, keep=True):
        """ Persist this download

        Saves a copy of this transfer process in its current state to disk.
        This is done automatically for a running transfer, so that as a chunk
        is completed, this is reflected. Thus, if a transfer is interrupted,
        e.g., by user action, the transfer can be restarted at another time.
        All chunks that were not already completed will be restarted at that
        time.

        See methods ``load`` to retrieved saved transfers and ``run`` to
        resume a stopped transfer.

        Parameters
        ----------
        keep: bool (True)
            If True, transfer will be saved if some chunks remain to be
            completed; the transfer will be sure to be removed otherwise.
        """
        save(self, os.path.join(datadir, 'downloads'), keep)

    @staticmethod
    def load():
        """ Load list of persisted transfers from disk, for possible resumption.

        Returns
        -------
            A dictionary of download instances. The hashes are auto-
            generated unique. The state of the chunks completed, errored, etc.,
            can be seen in the status attribute. Instances can be resumed with
            ``run()``.
        """
        return load(os.path.join(datadir, 'downloads'))

    @staticmethod
    def clear_saved():
        """ Remove references to all persisted downloads.
        """
        if os.path.exists(os.path.join(datadir, 'downloads')):
            os.remove(os.path.join(datadir, 'downloads'))

    @property
    def hash(self):
        return self._name

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
            To watch and wait (block) until completion.
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

    def __str__(self):
        return "<ADL Download: %s -> %s (%s)>" % (self.rpath, self.lpath,
                                                  self.client.status)

    __repr__ = __str__


def get_chunk(adlfs, src, dst, offset, size, buffersize, blocksize, shutdown_event=None):
    """ Download a piece of a remote file and write locally

    Internal function used by `download`.
    """
    nbytes = 0
    try:
        response = _fetch_range(adlfs.azure, src, start=offset, end=offset+size, stream=True)
        with open(dst, 'rb+') as fout:
            fout.seek(offset)
            for chunk in response.iter_content(chunk_size=blocksize):
                if shutdown_event and shutdown_event.is_set():
                    return nbytes, None
                if chunk:
                    nwritten = fout.write(chunk)
                    if nwritten:
                        nbytes += nwritten
    except Exception as e:
        exception = repr(e)
        logger.error('Download failed %s; %s', dst, exception)
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
    buffersize: int [2**22]
        Number of bytes for internal buffer. This block cannot be bigger than
        a chunk and cannot be smaller than a block.
    blocksize: int [2**22]
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
    azure.datalake.store.transfer.ADLTransferClient
    """
    def __init__(self, adlfs, rpath, lpath, nthreads=None, chunksize=2**28,
                 buffersize=2**22, blocksize=2**22, client=None, run=True,
                 delimiter=None, overwrite=False, verbose=True):
        if not overwrite and adlfs.exists(rpath):
            raise FileExistsError(rpath)
        if client:
            self.client = client
        else:
            self.client = ADLTransferClient(
                adlfs,
                transfer=put_chunk,
                merge=merge_chunks,
                nthreads=nthreads,
                chunksize=chunksize,
                buffersize=buffersize,
                blocksize=blocksize,
                delimiter=delimiter,
                parent=self,
                verbose=verbose,
                unique_temporary=True)
        self._name = tokenize(adlfs, rpath, lpath, chunksize, blocksize)
        self.rpath = AzureDLPath(rpath)
        self.lpath = lpath
        self._overwrite = overwrite
        self._setup()
        if run:
            self.run()

    def save(self, keep=True):
        """ Persist this upload

        Saves a copy of this transfer process in its current state to disk.
        This is done automatically for a running transfer, so that as a chunk
        is completed, this is reflected. Thus, if a transfer is interrupted,
        e.g., by user action, the transfer can be restarted at another time.
        All chunks that were not already completed will be restarted at that
        time.

        See methods ``load`` to retrieved saved transfers and ``run`` to
        resume a stopped transfer.

        Parameters
        ----------
        keep: bool (True)
            If True, transfer will be saved if some chunks remain to be
            completed; the transfer will be sure to be removed otherwise.
        """
        save(self, os.path.join(datadir, 'uploads'), keep)

    @staticmethod
    def load():
        """ Load list of persisted transfers from disk, for possible resumption.

        Returns
        -------
            A dictionary of upload instances. The hashes are auto-
            generated unique. The state of the chunks completed, errored, etc.,
            can be seen in the status attribute. Instances can be resumed with
            ``run()``.
        """
        return load(os.path.join(datadir, 'uploads'))

    @staticmethod
    def clear_saved():
        """ Remove references to all persisted uploads.
        """
        if os.path.exists(os.path.join(datadir, 'uploads')):
            os.remove(os.path.join(datadir, 'uploads'))

    @property
    def hash(self):
        return self._name

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
        """ Populate transfer queue and execute downloads

        Parameters
        ----------
        nthreads: int [None]
            Override default nthreads, if given
        monitor: bool [True]
            To watch and wait (block) until completion.
        """
        self.client.run(nthreads, monitor)

    def __str__(self):
        return "<ADL Upload: %s -> %s (%s)>" % (self.lpath, self.rpath,
                                                self.client.status)

    __repr__ = __str__


def put_chunk(adlfs, src, dst, offset, size, buffersize, blocksize, delimiter=None,
              shutdown_event=None):
    """ Upload a piece of a local file

    Internal function used by `upload`.
    """
    nbytes = 0
    try:
        with adlfs.open(dst, 'wb', blocksize=buffersize, delimiter=delimiter) as fout:
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
        logger.error('Upload failed %s; %s', src, exception)
        return nbytes, exception
    logger.debug('Uploaded from %s, byte offset %s', src, offset)
    return nbytes, None


def merge_chunks(adlfs, outfile, files, shutdown_event=None):
    try:
        # note that it is assumed that only temp files from this run are in the segment folder created.
        # so this call is optimized to instantly delete the temp folder on concat.
        adlfs.concat(outfile, files, delete_source=True)
    except Exception as e:
        exception = repr(e)
        logger.error('Merged failed %s; %s', outfile, exception)
        return exception
    logger.debug('Merged %s', outfile)
    adlfs.invalidate_cache(outfile)
    return None
