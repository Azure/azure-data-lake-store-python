# -*- coding: utf-8 -*-
"""
The main file-system class and functionality.

Provides an pythonic interface to the Azure Data-lake Store, including
file-system commands with typical names and options, and a File object
which is compatible with the built-in File.
"""

# standard imports
import io
import logging
import os
import re
import time

# local imports
from .lib import DatalakeRESTInterface, auth, refresh_token
# from .utils import read_block, raises, ensure_writable

logger = logging.getLogger(__name__)

try:
    FileNotFoundError
except NameError:
    class FileNotFoundError(IOError):
        pass


def split_path(path):
    """
    Normalise AzureDL path string into bucket and key.

    Parameters
    ----------
    path : string
        Input path, like `azure://mybucket/path/to/file`

    Examples
    --------
    >>> split_path("azure://mybucket/path/to/file")
    ['mybucket', 'path/to/file']
    """
    if path.startswith('azure://'):
        path = path[8:]
    if '/' not in path:
        return path, ""
    else:
        return path.split('/', 1)


class AzureDLFileSystem(object):
    """
    Access Azure DataLake Store as if it were a file-system

    Parameters
    ----------
    store : str ("")
        Store name to connect to
    token : dict
        When setting up a new connection, this contains the authorization
        credentials (see `lib.auth()`).
    """
    _conn = {}
    _singleton = [None]

    def __init__(self, store, token=None):
        # store instance vars
        self.store = store
        self.token = token
        self.connect()
        self.dirs = {}
        AzureDLFileSystem._singleton[0] = self

    @classmethod
    def current(cls):
        """ Return the most recently created AzureDLFileSystem
        """
        if not cls._singleton[0]:
            raise ValueError('No current connection')
        else:
            return cls._singleton[0]

    def connect(self, refresh=False):
        """
        Establish connection object.

        Parameters
        ----------
        refresh : bool (True)
            To request a new token (good for 3600s)
        """
        if self.store in self._conn:
            token = self._conn[self.store]
            if refresh or time.time() - token['time'] > 3000:
                token = refresh_token(token)
        else:
            token = self.token
        self.azure = DatalakeRESTInterface(store_name=self.store,
                                           token=token['access'])
        self._conn[self.store] = token
        self.token = token

    def __setstate__(self, state):
        self.__dict__.update(state)
        self.connect()

    def open(self, path, mode='rb', block_size=2**25):
        """ Open a file for reading or writing

        Parameters
        ----------
        path: string
            Path of file on ADL
        mode: string
            One of 'rb' or 'wb'
        block_size: int
            Size of data-node blocks if reading
        """
        if 'b' not in mode:
            raise NotImplementedError("Text mode not supported, use mode='%s'"
                                      " and manage bytes" % (mode[0] + 'b'))
        return AzureDLFile(self, path, mode, block_size=block_size)

    def _ls(self, path):
        """ List files at given path """
        path = path.rstrip('/').lstrip('/')
        if path not in self.dirs:
            out = self.azure.call('LISTSTATUS', path)
            self.dirs[path] = out['FileStatuses']['FileStatus']
            for f in self.dirs[path]:
                bits = [b for b in [path, f['pathSuffix'].lstrip('/')] if b]
                f['name'] = ('/'.join(bits))
        return self.dirs[path]

    def ls(self, path, detail=False):
        """ List single directory with or without details """
        files = self._ls(path)
        if not files:
            raise FileNotFoundError(path)
        if detail:
            return files
        else:
            return [f['name'] for f in files]

    def info(self, path):
        """ File information
        """
        path = path.rstrip('/').lstrip('/')
        root, fname = os.path.split(path)
        myfile = [f for f in self._ls(root) if f['name'] == path]
        if len(myfile) == 1:
            return myfile[0]
        raise FileNotFoundError(path)

    def _walk(self, path):
        fi = self._ls(path).copy()
        for apath in fi:
            if apath['type'] == 'DIRECTORY':
                fi.extend(self._ls(apath['name']))
        return [f for f in fi if f['type'] == 'FILE']

    def walk(self, path):
        return [f['name'] for f in self._walk(path)]

    def glob(self, path):
        """
        Find files by glob-matching.

        Note that the bucket part of the path must not contain a "*"
        """
        path0 = path
        if '*' not in path:
            path = path.rstrip('/') + '/*'
        if '/' in path[:path.index('*')]:
            ind = path[:path.index('*')].rindex('/')
            root = path[:ind + 1]
        else:
            root = ''
        allfiles = self.walk(root)
        pattern = re.compile("^" + path.replace('//', '/')
                             .rstrip('/')
                             .replace('*', '[^/]*')
                             .replace('?', '.') + "$")
        out = [f for f in allfiles if re.match(pattern,
               f.replace('//', '/').rstrip('/'))]
        if not out:
            out = self.ls(path0)
        return out

    def du(self, path, total=False, deep=False):
        """ Bytes in keys at path """
        if deep:
            files = self._walk(path)
        else:
            files = self.ls(path, detail=True)
        if total:
            return sum(f.get('length', 0) for f in files)
        else:
            return {p['name']: p['length'] for p in files}

    def exists(self, path):
        """ Does such a file/directory exist? """
        out = self.azure.call("CHECKACCESS", path)

    def cat(self, path):
        """ Returns contents of file """
        with self.open(path, 'rb') as f:
            return f.read()

    def tail(self, path, size=1024):
        """ Return last bytes of file """
        length = self.info(path)['length']
        if size > length:
            return self.cat(path)
        with self.open(path, 'rb') as f:
            f.seek(length - size)
            return f.read(size)

    def head(self, path, size=1024):
        """ Return first bytes of file """
        with self.open(path, 'rb', block_size=size) as f:
            return f.read(size)

    def get(self, path, filename):
        """ Stream data from file at path to local filename """
        with self.open(path, 'rb') as f:
            with open(filename, 'wb') as f2:
                while True:
                    data = f.read(f.blocksize)
                    if len(data) == 0:
                        break
                    f2.write(data)

    def put(self, filename, path):
        """ Stream data from local filename to file at path """
        with open(filename, 'rb') as f:
            with self.open(path, 'wb') as f2:
                while True:
                    data = f.read(f2.blocksize)
                    if len(data) == 0:
                        break
                    f2.write(data)

    def mkdir(self, path):
        """ Make new directory """
        self.touch(path)

    def rmdir(self, path):
        """ Remove empty directory """
        pass

    def mv(self, path1, path2):
        """ Move file between locations on ADL """
        self.copy(path1, path2)
        self.rm(path1)

    def concat(self, filelist):
        pass

    merge = concat

    def copy(self, path1, path2):
        """ Copy file between locations on ADL """
        pass

    def rm(self, path, recursive=False):
        """
        Remove a file.

        Parameters
        ----------
        path : string
            The location to remove.
        recursive : bool (True)
            Whether to remove also all entries below, i.e., which are returned
            by `walk()`.
        """
        if not self.exists(path):
            raise FileNotFoundError(path)
        if recursive:
            files = reversed(sorted(self.walk(path)))
            [self.rm(afile) for afile in files]

    def invalidate_cache(self, path=None):
        if path is None:
            self.dirs.clear()
        else:
            self.dirs.pop(path, None)
            parent = path.rsplit('/', 1)[0]
            self.dirs.pop(parent, None)

    def touch(self, path):
        """
        Create empty key

        If path is a bucket only, attempt to create bucket.
        """
        self.open(path, 'wb')

    def read_block(self, fn, offset, length, delimiter=None):
        """ Read a block of bytes from an ADL file

        Starting at ``offset`` of the file, read ``length`` bytes.  If
        ``delimiter`` is set then we ensure that the read starts and stops at
        delimiter boundaries that follow the locations ``offset`` and ``offset
        + length``.  If ``offset`` is zero then we start at zero.  The
        bytestring returned WILL include the end delimiter string.

        If offset+length is beyond the eof, reads to eof.

        Parameters
        ----------
        fn: string
            Path to filename on ADL
        offset: int
            Byte offset to start read
        length: int
            Number of bytes to read
        delimiter: bytes (optional)
            Ensure reading starts and stops at delimiter bytestring

        Examples
        --------
        >>> adl.read_block('data/file.csv', 0, 13)  # doctest: +SKIP
        b'Alice, 100\\nBo'
        >>> adl.read_block('data/file.csv', 0, 13, delimiter=b'\\n')  # doctest: +SKIP
        b'Alice, 100\\nBob, 200\\n'

        Use ``length=None`` to read to the end of the file.
        >>> adl.read_block('data/file.csv', 0, None, delimiter=b'\\n')  # doctest: +SKIP
        b'Alice, 100\\nBob, 200\\nCharlie, 300'

        See Also
        --------
        distributed.utils.read_block
        """
        with self.open(fn, 'rb') as f:
            size = f.info()['length']
            if length is None:
                length = size
            if offset + length > size:
                length = size - offset
            bytes = read_block(f, offset, length, delimiter)
        return bytes


class AzureDLFile(object):
    """
    Open ADL key as a file. Data is only loaded and cached on demand.

    Parameters
    ----------
    azure : azure connection
    path : str
        location of file
    mode : str {'wb', 'rb', 'ab'}

    Examples
    --------
    >>> adl = AzureDLFileSystem()  # doctest: +SKIP
    >>> with adl.open('my-dir/my-file.txt', mode='rb') as f:  # doctest: +SKIP
    ...     f.read(10)  # doctest: +SKIP

    See Also
    --------
    `AzureDLFileSystem.open`: used to create ``AzureDLFile`` objects
    """

    def __init__(self, azure, path, mode='rb', blocksize=2**25):
        self.mode = mode
        if mode not in {'rb', 'wb', 'ab'}:
            raise NotImplementedError("File mode must be {'rb', 'wb', 'ab'}, not %s" % mode)
        self.blocksize = blocksize
        self.path = path
        self.azure = azure
        self.cache = b""
        self.loc = 0
        self.start = None
        self.end = None
        self.closed = False
        self.trim = True
        if mode == 'wb':
            out = self.azure.call('CREATE', 'temp/test', overwrite=True)
            self.url = out.headers['Location']
        if mode == 'ab':
            # TODO: op=APPEND does not return writing URL
            self.loc = self.info()['length']
            raise NotImplementedError
        if mode == 'rb':
            self.size = self.info()['length']

    def info(self):
        """ File information about this path """
        return self.azure.info(self.path)

    def tell(self):
        """ Current file location """
        return self.loc

    def seek(self, loc, whence=0):
        """ Set current file location

        Parameters
        ----------
        loc : int
            byte location
        whence : {0, 1, 2}
            from start of file, current location or end of file, resp.
        """
        if not self.mode == 'rb':
            raise ValueError('Seek only available in read mode')
        if whence == 0:
            nloc = loc
        elif whence == 1:
            nloc = self.loc + loc
        elif whence == 2:
            nloc = self.size + loc
        else:
            raise ValueError(
                "invalid whence (%s, should be 0, 1 or 2)" % whence)
        if nloc < 0:
            raise ValueError('Seek before start of file')
        self.loc = nloc
        return self.loc

    def readline(self, length=-1):
        """
        Read and return a line from the stream.

        If length is specified, at most size bytes will be read.
        """
        self._fetch(self.loc, self.loc + 1)
        while True:
            found = self.cache[self.loc - self.start:].find(b'\n') + 1
            if length > 0 and found > length:
                return self.read(length)
            if found:
                return self.read(found)
            if self.end > self.size:
                return self.read(length)
            self._fetch(self.start, self.end + self.blocksize)

    def __next__(self):
        return self.readline()

    next = __next__

    def __iter__(self):
        return self

    def readlines(self):
        """ Return all lines in a file as a list """
        return list(self)

    def _fetch(self, start, end):
        if self.start is None and self.end is None:
            # First read
            self.start = start
            self.end = end + self.blocksize
            self.cache = _fetch_range(self.azure.azure, self.path, start,
                                      self.end)
        if start < self.start:
            new = _fetch_range(self.azure.azure, self.path, start, self.start)
            self.start = start
            self.cache = new + self.cache
        if end > self.end:
            if self.end > self.size:
                return
            new = _fetch_range(self.azure.azure, self.path, self.end,
                               end + self.blocksize)
            self.end = end + self.blocksize
            self.cache = self.cache + new

    def read(self, length=-1):
        """
        Return data from cache, or fetch pieces as necessary

        Parameters
        ----------
        length : int (-1)
            Number of bytes to read; if <0, all remaining bytes.
        """
        if self.mode != 'rb':
            raise ValueError('File not in read mode')
        if length < 0:
            length = self.size
        if self.closed:
            raise ValueError('I/O operation on closed file.')
        self._fetch(self.loc, self.loc + length)
        out = self.cache[self.loc - self.start:
                         self.loc - self.start + length]
        self.loc += len(out)
        if self.trim:
            num = (self.loc - self.start) // self.blocksize - 1
            if num > 0:
                self.start += self.blocksize * num
                self.cache = self.cache[self.blocksize * num:]
        return out

    def write(self, data):
        """
        Write data to buffer.

        Buffer only sent to ADL on flush() or if buffer is bigger than blocksize.

        Parameters
        ----------
        data : bytes
            Set of bytes to be written.
        """
        if self.mode not in {'wb', 'ab'}:
            raise ValueError('File not in write mode')
        if self.closed:
            raise ValueError('I/O operation on closed file.')
        out = self.buffer.write(ensure_writable(data))
        self.loc += out
        if self.buffer.tell() > self.blocksize:
            self.flush()
        return out

    def flush(self, reopen=True):
        """
        Write buffered data to ADL.

        Uploads the current buffer, if it is larger than the block-size.

        Parameters
        ----------
        reopen : bool (True)
            If writing is incomplete, write data and immediately get new
            upload URL
        """
        if self.mode in {'wb', 'ab'} and not self.closed:
            if self.buffer.tell() < self.blocksize and not force:
                raise ValueError('Parts must be greater than %s',
                                 self.blocksize)
            if self.buffer.tell() == 0:
                # no data in the buffer to write
                return
            if force and self.forced:
                raise ValueError("Force flush cannot be called more than once")
            if force:
                self.forced = True

            self.buffer.seek(0)

            # TODO
            self.buffer = io.BytesIO()
        if reopen:
            # TODO
            pass

    def close(self):
        """ Close file

        If in write mode, key is only finalized upon close, and key will then
        be available to other processes.
        """
        if self.closed:
            return
        self.cache = None
        if self.mode in {'wb', 'ab'}:
            self.flush(reopen=False)
        self.closed = True

    def readable(self):
        """Return whether the AzureDLFile was opened for reading"""
        return self.mode == 'rb'

    def seekable(self):
        """Return whether the AzureDLFile is seekable (only in read mode)"""
        return self.readable()

    def writable(self):
        """Return whether the AzureDLFile was opened for writing"""
        return self.mode in {'wb', 'ab'}

    def __del__(self):
        self.close()

    def __str__(self):
        return "<ADL file: %s>" % (self.path)

    __repr__ = __str__

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()


def _fetch_range(rest, path, start, end, max_attempts=10):
    logger.debug("Fetch: path, %s-%s", path, start, end)
    for i in range(max_attempts):
        try:
            resp = rest.call('OPEN', path, offset=start, length=end-start)
            return resp
        except Exception as e:
            logger.debug('Exception %e on ADL download, retrying', e,
                         exc_info=True)
    raise RuntimeError("Max number of ADL retries exceeded")
