"""
High performance multi-threaded module to up/download

Calls method in `core` with thread pool executor to ensure the network
is used to its maximum throughput.

Only implements upload and download of (massive) files and directory trees.
"""
from concurrent.futures import ThreadPoolExecutor, wait
import os
import pickle
import time

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
        local path. If downloading multiple files must be existing director,
        or a place a directory can be created. If downloading a single file,
        maybe either directory or file path.
    nthreads: int [None]
        Number of threads to use. If None, uses the number of cores * 5.
    chunksize: int [2**26]
        Number of bytes in each chunk for splitting big files. Files smaller
        than this number will always be downloaded in a single thread.
    run: bool (True)
        Whether to begin executing immediately.

    Returns
    -------
    List of complete and incomplete futures, which may include exception
    information.
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
            lfiles = [os.path.join(self.lpath, f.replace(self.rpath, ''))
                      for f in rfiles]
        else:
            if os.path.exists(self.lpath) and os.path.isdir(self.lpath):
                lfiles = [os.path.join(self.lpath,
                                       os.path.split(self.rpath)[1])]
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
        """
        threads = nthreads or self.nthreads
        self.pool = ThreadPoolExecutor(threads)
        self.futures = []
        for rfile, lfile in self.progress:
            root = os.path.split(lfile)[0]
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
                    return
        except KeyboardInterrupt:
            print('CANCEL!')
            for dic in self.progress.values():
                [f.cancel() for f in dic['futures']]
            self.pool.shutdown(wait=True)
            self._check()
        for dic in self.progress.values():
            dic['futures'] = []
        self.pool = None
        self.save()

    def __str__(self):
        return "ADL Download: %s -> %s (%s of %s chunks remain)" % (self.rpath,
                    self.lpath, self.nchunks, self.nchunks_orig)

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
                logger.debug('Written to %s, byte offset %s', lfile, offset)
            except Exception as e:
                # TODO : only some exceptions should be retriable
                logger.debug('Write failed %s, byte offset %s; %s, %s', lfile,
                             offset, e, e.args)
                tries += 1
                if tries >= retries:
                    logger.debug('Aborting %s, byte offset %s', lfile, offset)
                    raise
