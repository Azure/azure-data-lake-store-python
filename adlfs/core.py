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
        self.azure = self.connect()

    def open(self, path, mode='rb', block_size=5 * 1024 ** 2):
        """ Open a file for reading or writing

        Parameters
        ----------
        path: string
            Path of file on S3
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
        if path not in self.dirs:
            out = self.azure.call('LISTSTATUS', path)
            self.dirs[path] = out['FileStatuses']['FileStatus']
            for f in self.dirs[path]:
                f['name'] = '/'.join([path, f['pathSuffix']])
        return self.dirs[path]

    def ls(self, path, detail=False):
        """ List single "directory" with or without details """
        files = self._ls(path)
        if not files:
            raise FileNotFoundError(path)
        if detail:
            return files
        else:
            return [f['name'] for f in files]

    def info(self, path, refresh=False):
        pass

    def walk(self, path, refresh=False):
        pass

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
            root = '/'
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
            files = self.walk(path)
            files = [self.info(f) for f in files]
        else:
            files = self.ls(path, detail=True)
        if total:
            return sum(f.get('Size', 0) for f in files)
        else:
            return {p['Key']: p['Size'] for p in files}

    def exists(self, path):
        """ Does such a file/directory exist? """
        out = self.azure.call("CHECKACCESS", path)

    def cat(self, path):
        """ Returns contents of file """
        with self.open(path, 'rb') as f:
            return f.read()

    def tail(self, path, size=1024):
        """ Return last bytes of file """
        length = self.info(path)['Size']
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

###########
    def mkdir(self, path):
        """ Make new bucket or empty key """
        self.touch(path)

    def rmdir(self, path):
        """ Remove empty key or bucket """
        bucket, key = split_path(path)
        if (key and self.info(path)['Size'] == 0) or not key:
            self.rm(path)
        else:
            raise IOError('Path is not directory-like', path)

    def mv(self, path1, path2):
        """ Move file between locations on S3 """
        self.copy(path1, path2)
        self.rm(path1)

    def merge(self, path, filelist):
        """ Create single S3 file from list of S3 files

        Uses multi-part, no data is downloaded. The original files are
        not deleted.

        Parameters
        ----------
        path : str
            The final file to produce
        filelist : list of str
            The paths, in order, to assemble into the final file.
        """
        bucket, key = split_path(path)
        mpu = self.s3.create_multipart_upload(Bucket=bucket, Key=key)
        out = [self.s3.upload_part_copy(Bucket=bucket, Key=key, UploadId=mpu['UploadId'],
                            CopySource=f, PartNumber=i+1) for (i, f) in enumerate(filelist)]
        parts = [{'PartNumber': i+1, 'ETag': o['CopyPartResult']['ETag']} for (i, o) in enumerate(out)]
        part_info = {'Parts': parts}
        self.s3.complete_multipart_upload(Bucket=bucket, Key=key,
                    UploadId=mpu['UploadId'], MultipartUpload=part_info)
        self.invalidate_cache(path)

    def copy(self, path1, path2):
        """ Copy file between locations on S3 """
        buc1, key1 = split_path(path1)
        buc2, key2 = split_path(path2)
        try:
            self.s3.copy_object(Bucket=buc2, Key=key2,
                                CopySource='/'.join([buc1, key1]))
        except (ClientError, ParamValidationError):
            raise IOError('Copy failed', (path1, path2))
        self.invalidate_cache(path2)

    def bulk_delete(self, pathlist):
        """
        Remove multiple keys with one call

        Parameters
        ----------
        pathlist : listof strings
            The keys to remove, must all be in the same bucket.
        """
        if not pathlist:
            return
        buckets = {split_path(path)[0] for path in pathlist}
        if len(buckets) > 1:
            raise ValueError("Bulk delete files should refer to only one bucket")
        bucket = buckets.pop()
        if len(pathlist) > 1000:
            for i in range((len(pathlist) // 1000) + 1):
                self.bulk_delete(pathlist[i*1000:(i+1)*1000])
            return
        delete_keys = {'Objects': [{'Key' : split_path(path)[1]} for path
                                   in pathlist]}
        try:
            self.s3.delete_objects(Bucket=bucket, Delete=delete_keys)
            for path in pathlist:
                self.invalidate_cache(path)
        except ClientError:
            raise IOError('Bulk delete failed')

    def rm(self, path, recursive=False):
        """
        Remove keys and/or bucket.

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
            self.bulk_delete(self.walk(path))
            if not self.exists(path):
                return
        bucket, key = split_path(path)
        if key:
            try:
                self.s3.delete_object(Bucket=bucket, Key=key)
            except ClientError:
                raise IOError('Delete key failed', (bucket, key))
            self.invalidate_cache(path)
        else:
            if not self.s3.list_objects(Bucket=bucket).get('Contents'):
                try:
                    self.s3.delete_bucket(Bucket=bucket)
                except ClientError:
                    raise IOError('Delete bucket failed', bucket)
                self.invalidate_cache(bucket)
                self.invalidate_cache('')
            else:
                raise IOError('Not empty', path)

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
        bucket, key = split_path(path)
        if key:
            self.s3.put_object(Bucket=bucket, Key=key)
            self.invalidate_cache(path)
        else:
            try:
                self.s3.create_bucket(Bucket=bucket)
                self.invalidate_cache('')
                self.invalidate_cache(bucket)
            except (ClientError, ParamValidationError):
                raise IOError('Bucket create failed', path)

    def read_block(self, fn, offset, length, delimiter=None):
        """ Read a block of bytes from an S3 file

        Starting at ``offset`` of the file, read ``length`` bytes.  If
        ``delimiter`` is set then we ensure that the read starts and stops at
        delimiter boundaries that follow the locations ``offset`` and ``offset
        + length``.  If ``offset`` is zero then we start at zero.  The
        bytestring returned WILL include the end delimiter string.

        If offset+length is beyond the eof, reads to eof.

        Parameters
        ----------
        fn: string
            Path to filename on S3
        offset: int
            Byte offset to start read
        length: int
            Number of bytes to read
        delimiter: bytes (optional)
            Ensure reading starts and stops at delimiter bytestring

        Examples
        --------
        >>> s3.read_block('data/file.csv', 0, 13)  # doctest: +SKIP
        b'Alice, 100\\nBo'
        >>> s3.read_block('data/file.csv', 0, 13, delimiter=b'\\n')  # doctest: +SKIP
        b'Alice, 100\\nBob, 200\\n'

        Use ``length=None`` to read to the end of the file.
        >>> s3.read_block('data/file.csv', 0, None, delimiter=b'\\n')  # doctest: +SKIP
        b'Alice, 100\\nBob, 200\\nCharlie, 300'

        See Also
        --------
        distributed.utils.read_block
        """
        with self.open(fn, 'rb') as f:
            size = f.info()['Size']
            if length is None:
                length = size
            if offset + length > size:
                length = size - offset
            bytes = read_block(f, offset, length, delimiter)
        return bytes


class AzureDLFile(object):
    """
    Open S3 key as a file. Data is only loaded and cached on demand.

    Parameters
    ----------
    s3 : boto3 connection
    bucket : string
        S3 bucket to access
    key : string
        S3 key to access
    blocksize : int
        read-ahead size for finding delimiters

    Examples
    --------
    >>> s3 = S3FileSystem()  # doctest: +SKIP
    >>> with s3.open('my-bucket/my-file.txt', mode='rb') as f:  # doctest: +SKIP
    ...     ...  # doctest: +SKIP

    See Also
    --------
    S3FileSystem.open: used to create ``S3File`` objects
    """

    def __init__(self, s3, path, mode='rb', block_size=5 * 2 ** 20):
        self.mode = mode
        if mode not in {'rb', 'wb', 'ab'}:
            raise NotImplementedError("File mode must be {'rb', 'wb', 'ab'}, not %s" % mode)
        self.path = path
        bucket, key = split_path(path)
        self.s3 = s3
        self.bucket = bucket
        self.key = key
        self.blocksize = block_size
        self.cache = b""
        self.loc = 0
        self.start = None
        self.end = None
        self.closed = False
        self.trim = True
        self.mpu = None
        if mode in {'wb', 'ab'}:
            self.buffer = io.BytesIO()
            self.parts = []
            self.size = 0
            if block_size < 5 * 2 ** 20:
                raise ValueError('Block size must be >=5MB')
            self.forced = False
            if mode == 'ab' and s3.exists(path):
                self.size = s3.info(path)['Size']
                if self.size < 5*2**20:
                    # existing file too small for multi-upload: download
                    self.write(s3.cat(path))
                else:
                    try:
                        self.mpu = s3.s3.create_multipart_upload(Bucket=bucket, Key=key)
                    except (ClientError, ParamValidationError) as e:
                        raise IOError('Open for write failed', path, e)
                    self.loc = self.size
                    out = self.s3.s3.upload_part_copy(Bucket=self.bucket, Key=self.key,
                                PartNumber=1, UploadId=self.mpu['UploadId'],
                                CopySource=path)
                    self.parts.append({'PartNumber': 1,
                                       'ETag': out['CopyPartResult']['ETag']})
        else:
            try:
                self.size = self.info()['Size']
            except (ClientError, ParamValidationError):
                raise IOError("File not accessible", path)

    def info(self):
        """ File information about this path """
        return self.s3.info(self.path)

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
            self.cache = _fetch_range(self.s3.s3, self.bucket, self.key,
                                      start, self.end)
        if start < self.start:
            new = _fetch_range(self.s3.s3, self.bucket, self.key,
                               start, self.start)
            self.start = start
            self.cache = new + self.cache
        if end > self.end:
            if self.end > self.size:
                return
            new = _fetch_range(self.s3.s3, self.bucket, self.key,
                               self.end, end + self.blocksize)
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

        Buffer only sent to S3 on flush() or if buffer is bigger than blocksize.

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

    def flush(self, force=False, retries=10):
        """
        Write buffered data to S3.

        Uploads the current buffer, if it is larger than the block-size.

        Due to S3 multi-upload policy, you can only safely force flush to S3
        when you are finished writing.  It is unsafe to call this function
        repeatedly.

        Parameters
        ----------
        force : bool
            When closing, write the last block even if it is smaller than
            blocks are allowed to be.
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
            part = len(self.parts) + 1
            i = 0

            try:
                self.mpu = self.mpu or self.s3.s3.create_multipart_upload(
                                Bucket=self.bucket, Key=self.key)
            except (ClientError, ParamValidationError) as e:
                raise IOError('Initating write failed: %s' % self.path, e)

            while True:
                try:
                    out = self.s3.s3.upload_part(Bucket=self.bucket,
                            PartNumber=part, UploadId=self.mpu['UploadId'],
                            Body=self.buffer.read(), Key=self.key)
                    break
                except S3_RETRYABLE_ERRORS:
                    if i < retries:
                        logger.debug('Exception %e on S3 write, retrying',
                                     exc_info=True)
                        i += 1
                        continue
                    else:
                        raise IOError('Write failed after %i retries' % retries,
                                      self)
                except Exception as e:
                    raise IOError('Write failed', self, e)
            self.parts.append({'PartNumber': part, 'ETag': out['ETag']})
            self.buffer = io.BytesIO()

    def close(self):
        """ Close file

        If in write mode, key is only finalized upon close, and key will then
        be available to other processes.
        """
        if self.closed:
            return
        self.cache = None
        if self.mode in {'wb', 'ab'}:
            if self.parts:
                self.flush(force=True)
                part_info = {'Parts': self.parts}
                self.s3.s3.complete_multipart_upload(Bucket=self.bucket,
                                                     Key=self.key,
                                                     UploadId=self.mpu[
                                                         'UploadId'],
                                                     MultipartUpload=part_info)
            else:
                self.buffer.seek(0)
                try:
                    self.s3.s3.put_object(Bucket=self.bucket, Key=self.key,
                                           Body=self.buffer.read())
                except (ClientError, ParamValidationError) as e:
                    raise IOError('Write failed: %s' % self.path, e)
            self.s3.invalidate_cache(self.path)
        self.closed = True

    def readable(self):
        """Return whether the S3File was opened for reading"""
        return self.mode == 'rb'

    def seekable(self):
        """Return whether the S3File is seekable (only in read mode)"""
        return self.readable()

    def writable(self):
        """Return whether the S3File was opened for writing"""
        return self.mode in {'wb', 'ab'}

    def __del__(self):
        self.close()

    def __str__(self):
        return "<S3File %s/%s>" % (self.bucket, self.key)

    __repr__ = __str__

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()


def _fetch_range(client, bucket, key, start, end, max_attempts=10):
    logger.debug("Fetch: %s/%s, %s-%s", bucket, key, start, end)
    for i in range(max_attempts):
        try:
            resp = client.get_object(Bucket=bucket, Key=key,
                                     Range='bytes=%i-%i' % (start, end - 1))
            return resp['Body'].read()
        except S3_RETRYABLE_ERRORS as e:
            logger.debug('Exception %e on S3 download, retrying', e,
                         exc_info=True)
            continue
        except ClientError as e:
            if e.response['Error'].get('Code', 'Unknown') in ['416',
                                                              'InvalidRange']:
                return b''
            else:
                raise
    raise RuntimeError("Max number of S3 retries exceeded")
