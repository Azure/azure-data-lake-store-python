# -*- coding: utf-8 -*-
# coding=utf-8
# --------------------------------------------------------------------------
# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See License.txt in the project root for
# license information.
# --------------------------------------------------------------------------

"""
The main file-system class and functionality.

Provides an pythonic interface to the Azure Data-lake Store, including
file-system commands with typical names and options, and a File object
which is compatible with the built-in File.
"""

# standard imports
import io
import logging
import sys
import time
import uuid


# local imports
from .exceptions import DatalakeBadOffsetException
from .exceptions import FileNotFoundError, PermissionError
from .lib import DatalakeRESTInterface
from .utils import ensure_writable, read_block
from .enums import ExpiryOptionType
from .retry import ExponentialRetryPolicy

if sys.version_info >= (3, 4):
    import pathlib
else:
    import pathlib2 as pathlib

logger = logging.getLogger(__name__)
valid_expire_types = [x.value for x in ExpiryOptionType]

class AzureDLFileSystem(object):
    """
    Access Azure DataLake Store as if it were a file-system

    Parameters
    ----------
    store_name : str ("")
        Store name to connect to
    token : credentials object
        When setting up a new connection, this contains the authorization
        credentials (see `lib.auth()`).
    url_suffix: str (None)
        Domain to send REST requests to. The end-point URL is constructed
        using this and the store_name. If None, use default.
    api_version: str (2016-11-01)
        The API version to target with requests. Changing this value will
        change the behavior of the requests, and can cause unexpected behavior or
        breaking changes. Changes to this value should be undergone with caution.
    kwargs: optional key/values
        See ``lib.auth()``; full list: tenant_id, username, password, client_id,
        client_secret, resource
    """
    _singleton = [None]

    def __init__(self, token=None, **kwargs):
        # store instance vars
        self.token = token
        self.kwargs = kwargs
        self.connect()
        self.dirs = {}
        self._emptyDirs = []
        AzureDLFileSystem._singleton[0] = self

    @classmethod
    def current(cls):
        """ Return the most recently created AzureDLFileSystem
        """
        if not cls._singleton[0]:
            return cls()
        else:
            return cls._singleton[0]

    def connect(self):
        """
        Establish connection object.
        """
        self.azure = DatalakeRESTInterface(token=self.token, **self.kwargs)
        self.token = self.azure.token

    def __setstate__(self, state):
        self.__dict__.update(state)
        self.connect()

    def open(self, path, mode='rb', blocksize=2**25, delimiter=None):
        """ Open a file for reading or writing

        Parameters
        ----------
        path: string
            Path of file on ADL
        mode: string
            One of 'rb', 'ab' or 'wb'
        blocksize: int
            Size of data-node blocks if reading
        delimiter: byte(s) or None
            For writing delimiter-ended blocks
        """
        if 'b' not in mode:
            raise NotImplementedError("Text mode not supported, use mode='%s'"
                                      " and manage bytes" % (mode[0] + 'b'))
        return AzureDLFile(self, AzureDLPath(path), mode, blocksize=blocksize,
                           delimiter=delimiter)

    def _ls(self, path, invalidate_cache=True):
        """ List files at given path """
        path = AzureDLPath(path).trim()
        key = path.as_posix()

        if invalidate_cache:
            self.invalidate_cache(key)

        if key not in self.dirs:
            out = self.azure.call('LISTSTATUS', key)
            self.dirs[key] = out['FileStatuses']['FileStatus']
            for f in self.dirs[key]:
                f['name'] = (path / f['pathSuffix']).as_posix()
        return self.dirs[key]

    def ls(self, path="", detail=False, invalidate_cache=True):
        """ List single directory with or without details """
        path = AzureDLPath(path)
        files = self._ls(path, invalidate_cache)
        if not files:
            # in this case we just invalidated the cache (if it was true), so no need to do it again
            inf = self.info(path, invalidate_cache=False)
            if inf['type'] == 'DIRECTORY':
                # always return an empty array in this case, because there are no entries underneath the folder
                return []

            raise FileNotFoundError(path)
        if detail:
            return files
        else:
            return [f['name'] for f in files]

    def info(self, path, invalidate_cache=True, expected_error_code=None):
        """ File information
        """
        path = AzureDLPath(path).trim()
        path_as_posix = path.as_posix()
        root = path.parent
        root_as_posix = root.as_posix()

        # in the case of getting info about the root itself or if the cache won't be hit
        # simply return the result of a GETFILESTATUS from the service
        if invalidate_cache or path_as_posix in {'/', '.'}:
            to_return = self.azure.call('GETFILESTATUS', path_as_posix, expected_error_code=expected_error_code)['FileStatus']
            to_return['name'] = path_as_posix

            # add the key/value pair back to the cache so long as it isn't the root
            if path_as_posix not in {'/', '.'}:
                if root_as_posix not in self.dirs:
                    self.dirs[root_as_posix] = [to_return]
                else:
                    found = False
                    for f in self.dirs[root_as_posix]:
                        if f['name'] == path_as_posix:
                            found = True
                            f = to_return
                            break
                    if not found:
                        self.dirs[root_as_posix].append(to_return)
            return to_return

        for f in self._ls(root, invalidate_cache):
            if f['name'] == path_as_posix:
                return f

        raise FileNotFoundError(path)

    def _walk(self, path, invalidate_cache=True):
        fi = list(self._ls(path, invalidate_cache))
        self._emptyDirs = []
        for apath in fi:
            if apath['type'] == 'DIRECTORY':
                sub_elements = self._ls(apath['name'], invalidate_cache)
                if not sub_elements:
                    self._emptyDirs.append(apath)
                else:
                    fi.extend(sub_elements)
        return [f for f in fi if f['type'] == 'FILE']

    def _empty_dirs_to_add(self):
        """ Returns directories found empty during walk. Only for internal use"""
        return self._emptyDirs

    def walk(self, path='', details=False, invalidate_cache=True):
        """ Get all files below given path
        """
        return [f if details else f['name'] for f in self._walk(path, invalidate_cache)]

    def glob(self, path, details=False, invalidate_cache=True):
        """
        Find files (not directories) by glob-matching.
        """
        path = AzureDLPath(path).trim()
        path_as_posix = path.as_posix()
        prefix = path.globless_prefix
        allfiles = self.walk(prefix, details, invalidate_cache)
        if prefix == path:
            return allfiles
        return [f for f in allfiles if AzureDLPath(f['name'] if details else f).match(path_as_posix)]

    def du(self, path, total=False, deep=False, invalidate_cache=True):
        """ Bytes in keys at path """
        if deep:
            files = self._walk(path, invalidate_cache)
        else:
            files = self.ls(path, detail=True, invalidate_cache=invalidate_cache)
        if total:
            return sum(f.get('length', 0) for f in files)
        else:
            return {p['name']: p['length'] for p in files}

    def df(self, path):
        """ Resource summary of path """
        path = AzureDLPath(path).trim()
        return self.azure.call('GETCONTENTSUMMARY', path.as_posix())['ContentSummary']

    def chmod(self, path, mod):
        """  Change access mode of path

        Note this is not recursive.

        Parameters
        ----------
        path: str
            Location to change
        mod: str
            Octal representation of access, e.g., "0777" for public read/write.
            See [docs](http://hadoop.apache.org/docs/r2.4.1/hadoop-project-dist/hadoop-hdfs/WebHDFS.html#Permission)
        """
        path = AzureDLPath(path).trim()
        self.azure.call('SETPERMISSION', path.as_posix(), permission=mod)
        self.invalidate_cache(path.as_posix())

    def set_expiry(self, path, expiry_option, expire_time=None):
        """
        Sets or removes the expiration time on the specified file.
        This operation can only be executed against files.

        Note: Folders are not supported.

        Parameters
        ----------
        path: str
            File path to set or remove expiration time
        expire_time: int
            The time that the file will expire, corresponding to the expiry_option that was set
        expiry_option: str
            Indicates the type of expiration to use for the file:
                1. NeverExpire: ExpireTime is ignored.
                2. RelativeToNow: ExpireTime is an integer in milliseconds representing the expiration date relative to when file expiration is updated.
                3. RelativeToCreationDate: ExpireTime is an integer in milliseconds representing the expiration date relative to file creation.
                4. Absolute: ExpireTime is an integer in milliseconds, as a Unix timestamp relative to 1/1/1970 00:00:00.
        """
        parms = {}
        value_to_use = [x for x in valid_expire_types if x.lower() == expiry_option.lower()]
        if len(value_to_use) != 1:
            raise ValueError('expiry_option must be one of: {}. Value given: {}'.format(valid_expire_types, expiry_option))

        if value_to_use[0] != ExpiryOptionType.never_expire.value and not expire_time:
            raise ValueError('expire_time must be specified if the expiry_option is not NeverExpire. Value of expiry_option: {}'.format(expiry_option))

        path = AzureDLPath(path).trim()
        parms['expiryOption'] = value_to_use[0]

        if expire_time:
            parms['expireTime'] = int(expire_time)

        self.azure.call('SETEXPIRY', path.as_posix(), is_extended=True, **parms)
        self.invalidate_cache(path.as_posix())

    def _acl_call(self, action, path, acl_spec=None, invalidate_cache=False):
        """
        Helper method for ACL calls to reduce code repetition

        Parameters
        ----------
        action: str
            The ACL action being executed. For example SETACL
        path: str
            The path the action is being executed on (file or folder)
        acl_spec: str
            The optional ACL specification to set on the path in the format
            '[default:]user|group|other:[entity id or UPN]:r|-w|-x|-,[default:]user|group|other:[entity id or UPN]:r|-w|-x|-,...'

            Note that for remove acl entries the permission (rwx) portion is not required.
        invalidate_cache: bool
            optionally indicates that the cache of files should be invalidated after this operation
            This should always be done for set and remove operations, since the state of the file or folder has changed.
        """
        parms = {}
        path = AzureDLPath(path).trim()
        posix_path = path.as_posix()
        if acl_spec:
            parms['aclSpec'] = acl_spec

        to_return = self.azure.call(action, posix_path, **parms)
        if invalidate_cache:
            self.invalidate_cache(posix_path)

        return to_return

    def set_acl(self, path, acl_spec):
        """
        Sets the Access Control List (ACL) for a file or folder.

        Note: this is not recursive, and applies only to the file or folder specified.

        Parameters
        ----------
        path: str
            Location to set the ACL on.
        acl_spec: str
            The ACL specification to set on the path in the format
            '[default:]user|group|other:[entity id or UPN]:r|-w|-x|-,[default:]user|group|other:[entity id or UPN]:r|-w|-x|-,...'
        """

        self._acl_call('SETACL', path, acl_spec, invalidate_cache=True)


    def modify_acl_entries(self, path, acl_spec):
        """
        Modifies existing Access Control List (ACL) entries on a file or folder.
        If the entry does not exist it is added, otherwise it is updated based on the spec passed in.
        No entries are removed by this process (unlike set_acl).

        Note: this is not recursive, and applies only to the file or folder specified.

        Parameters
        ----------
        path: str
            Location to set the ACL entries on.
        acl_spec: str
            The ACL specification to use in modifying the ACL at the path in the format
            '[default:]user|group|other:[entity id or UPN]:r|-w|-x|-,[default:]user|group|other:[entity id or UPN]:r|-w|-x|-,...'
        """
        self._acl_call('MODIFYACLENTRIES', path, acl_spec, invalidate_cache=True)


    def remove_acl_entries(self, path, acl_spec):
        """
        Removes existing, named, Access Control List (ACL) entries on a file or folder.
        If the entry does not exist already it is ignored.
        Default entries cannot be removed this way, please use remove_default_acl for that.
        Unnamed entries cannot be removed in this way, please use remove_acl for that.

        Note: this is not recursive, and applies only to the file or folder specified.

        Parameters
        ----------
        path: str
            Location to remove the ACL entries.
        acl_spec: str
            The ACL specification to remove from the ACL at the path in the format (note that the permission portion is missing)
            '[default:]user|group|other:[entity id or UPN],[default:]user|group|other:[entity id or UPN],...'
        """
        self._acl_call('REMOVEACLENTRIES', path, acl_spec, invalidate_cache=True)


    def get_acl_status(self, path):
        """
        Gets Access Control List (ACL) entries for the specified file or directory.

        Parameters
        ----------
        path: str
            Location to get the ACL.
        """
        return self._acl_call('MSGETACLSTATUS', path)['AclStatus']

    def remove_acl(self, path):
        """
        Removes the entire, non default, ACL from the file or folder, including unnamed entries.
        Default entries cannot be removed this way, please use remove_default_acl for that.

        Note: this is not recursive, and applies only to the file or folder specified.

        Parameters
        ----------
        path: str
            Location to remove the ACL.
        """
        self._acl_call('REMOVEACL', path, invalidate_cache=True)


    def remove_default_acl(self, path):
        """
        Removes the entire default ACL from the folder.
        Default entries do not exist on files, if a file
        is specified, this operation does nothing.

        Note: this is not recursive, and applies only to the folder specified.

        Parameters
        ----------
        path: str
            Location to set the ACL on.
        """
        self._acl_call('REMOVEDEFAULTACL', path, invalidate_cache=True)


    def chown(self, path, owner=None, group=None):
        """
        Change owner and/or owning group

        Note this is not recursive.

        Parameters
        ----------
        path: str
            Location to change
        owner: str
            UUID of owning entity
        group: str
            UUID of group
        """
        parms = {}
        if owner is None and group is None:
            raise ValueError('Must supply owner and/or group')
        if owner:
            parms['owner'] = owner
        if group:
            parms['group'] = group
        path = AzureDLPath(path).trim()
        self.azure.call('SETOWNER', path.as_posix(), **parms)
        self.invalidate_cache(path.as_posix())

    def exists(self, path, invalidate_cache=True):
        """ Does such a file/directory exist? """
        try:
            self.info(path, invalidate_cache, expected_error_code=404)
            return True
        except FileNotFoundError:
            return False

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
        with self.open(path, 'rb', blocksize=size) as f:
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

    def put(self, filename, path, delimiter=None):
        """ Stream data from local filename to file at path """
        with open(filename, 'rb') as f:
            with self.open(path, 'wb', delimiter=delimiter) as f2:
                while True:
                    data = f.read(f2.blocksize)
                    if len(data) == 0:
                        break
                    f2.write(data)

    def mkdir(self, path):
        """ Make new directory """
        path = AzureDLPath(path).trim()
        self.azure.call('MKDIRS', path.as_posix())
        self.invalidate_cache(path)

    def rmdir(self, path):
        """ Remove empty directory """
        if self.info(path)['type'] != "DIRECTORY":
            raise ValueError('Can only rmdir on directories')
        # should always invalidate the cache when checking to see if the directory is empty
        if self.ls(path, invalidate_cache=True):
            raise ValueError('Directory not empty: %s' % path)
        self.rm(path, False)

    def mv(self, path1, path2):
        """ Move file between locations on ADL """
        path1 = AzureDLPath(path1).trim()
        path2 = AzureDLPath(path2).trim()
        self.azure.call('RENAME', path1.as_posix(),
                        destination=path2.as_posix())
        self.invalidate_cache(path1)
        self.invalidate_cache(path2)

    def concat(self, outfile, filelist, delete_source=False):
        """ Concatenate a list of files into one new file

        Parameters
        ----------

        outfile : path
            The file which will be concatenated to. If it already exists,
            the extra pieces will be appended.
        filelist : list of paths
            Existing adl files to concatenate, in order
        delete_source : bool (False)
            If True, assume that the paths to concatenate exist alone in a
            directory, and delete that whole directory when done.
        """
        outfile = AzureDLPath(outfile).trim()
        filelist = ','.join(AzureDLPath(f).as_posix() for f in filelist)
        delete = 'true' if delete_source else 'false'
        self.azure.call('MSCONCAT', outfile.as_posix(),
                        data='sources='+filelist,
                        deleteSourceDirectory=delete)
        self.invalidate_cache(outfile)

    merge = concat

    def cp(self, path1, path2):
        """ Copy file between locations on ADL """
        # TODO: any implementation for this without download?
        raise NotImplementedError

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
        path = AzureDLPath(path).trim()
        # Always invalidate the cache when attempting to check existence of something to delete
        if not self.exists(path, invalidate_cache=True):
            raise FileNotFoundError(path)
        self.azure.call('DELETE', path.as_posix(), recursive=recursive)
        self.invalidate_cache(path)
        if recursive:
            matches = [p for p in self.dirs if p.startswith(path.as_posix())]
            [self.invalidate_cache(m) for m in matches]

    def invalidate_cache(self, path=None):
        """Remove entry from object file-cache"""
        if path is None:
            self.dirs.clear()
        else:
            path = AzureDLPath(path).trim()
            self.dirs.pop(path.as_posix(), None)
            parent = AzureDLPath(path.parent).trim()
            self.dirs.pop(parent.as_posix(), None)

    def touch(self, path):
        """
        Create empty file

        If path is a bucket only, attempt to create bucket.
        """
        with self.open(path, 'wb'):
            pass

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
            if offset >= size:
                return b''
            if length is None:
                length = size
            if offset + length > size:
                length = size - offset
            bytes = read_block(f, offset, length, delimiter)
        return bytes

    # ALIASES
    listdir = ls
    access = exists
    rename = mv
    stat = info
    unlink = remove = rm


class AzureDLFile(object):
    """
    Open ADL key as a file. Data is only loaded and cached on demand.

    Parameters
    ----------
    azure : azure connection
    path : AzureDLPath
        location of file
    mode : str {'wb', 'rb', 'ab'}
    blocksize : int
        Size of the write or read-ahead buffer. For writing, will be
        truncated to 4MB (2**22).
    delimiter : bytes or None
        If specified and in write mode, each flush will send data terminating
        on this bytestring, potentially leaving some data in the buffer.

    Examples
    --------
    >>> adl = AzureDLFileSystem()  # doctest: +SKIP
    >>> with adl.open('my-dir/my-file.txt', mode='rb') as f:  # doctest: +SKIP
    ...     f.read(10)  # doctest: +SKIP

    See Also
    --------
    AzureDLFileSystem.open: used to create AzureDLFile objects
    """

    def __init__(self, azure, path, mode='rb', blocksize=2**25,
                 delimiter=None):
        self.mode = mode
        if mode not in {'rb', 'wb', 'ab'}:
            raise NotImplementedError("File mode must be {'rb', 'wb', 'ab'}, not %s" % mode)
        self.path = path
        self.azure = azure
        self.cache = b""
        self.loc = 0
        self.delimiter = delimiter
        self.start = 0
        self.end = 0
        self.closed = False
        self.trim = True
        self.buffer = io.BytesIO()
        self.blocksize = blocksize
        self.first_write = True
        uniqueid = str(uuid.uuid4())
        self.filesessionid = uniqueid
        self.leaseid = uniqueid

        # always invalidate the cache when checking for existence of a file
        # that may be created or written to (for the first time).
        exists = self.azure.exists(path, invalidate_cache=True)

        # cannot create a new file object out of a directory
        if exists and self.info()['type'] == 'DIRECTORY':
            raise IOError('path: {} is a directory, not a file, and cannot be opened for reading or writing'.format(path))

        if mode == 'ab' and exists:
            self.loc = self.info()['length']
            self.first_write = False
        elif mode == 'rb':
            self.size = self.info()['length']
        else:
            self.blocksize = min(2**22, blocksize)

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
        if nloc > self.size:
            raise ValueError('ADLFS does not support seeking beyond file')
        self.loc = nloc
        return self.loc

    def readline(self, length=-1):
        """
        Read and return a line from the stream.

        If length is specified, at most size bytes will be read.
        """
        if length < 0:
            length = self.size

        line = b""
        while True:

            # if cache has last bytes of file and its read, return line and exit loop
            if self.end >= self.size and self.loc >= self.end:
                return line

            self._read_blocksize()

            found = self.cache[self.loc - self.start:].find(b'\n') + 1
            if found:
                partialLine = self.cache[self.loc-self.start: min(self.loc-self.start+found, self.loc-self.start+length)]
            else:
                partialLine = self.cache[self.loc-self.start:]

            self.loc += len(partialLine)
            line += partialLine

            if found:
                return line

    def __next__(self):
        out = self.readline()
        if not out:
            raise StopIteration
        return out

    next = __next__

    def __iter__(self):
        return self

    def readlines(self):
        """ Return all lines in a file as a list """
        return list(self)

    def _fetch(self, start, end):
        self.start = start
        self.end = min(end, self.size)
        response = _fetch_range_with_retry(
            self.azure.azure, self.path.as_posix(), self.start, self.end, filesessionid=self.filesessionid)
        self.cache = getattr(response, 'content', response)

    def _read_blocksize(self, offset=-1):
        """
        Reads next blocksize of data and updates the cache if read offset is not within cache otherwise nop

        Parameters
        ----------
        offset : int (-1)
            offset from where to read; if <0, last read location or beginning of file.
        :return:
        """
        if offset < 0:
            offset = self.loc
        if offset >= self.size:
            self.start = self.size
            self.end = self.size
            self.cache = b""
            return
        if offset >= self.start and offset < self.end:
            return
        if offset > self.size:
            raise ValueError('Read offset is outside the File')
        self._fetch(offset, offset + self.blocksize)

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

        out = b""
        while length > 0:
            self._read_blocksize()
            data_read = self.cache[self.loc - self.start:
                             min(self.loc - self.start + length, self.end - self.start)]
            out += data_read
            self.loc += len(data_read)
            length -= len(data_read)
            if self.loc >= self.size:
                length = 0

        return out

    read1 = read

    def write(self, data):
        """
        Write data to buffer.

        Buffer only sent to ADL on flush() or if buffer is bigger than
        blocksize.

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
        self.flush(syncFlag='DATA')
        return out


    def flush(self, syncFlag='METADATA', force=False):
        """
        Write buffered data to ADL.

        Without delimiter: Uploads the current buffer.

        With delimiter: writes an amount of data less than or equal to the
        block-size, which ends on the delimiter, until buffer is smaller than
        the blocksize. If there is no delimiter in a block uploads whole block.

        If force=True, flushes all data in the buffer, even if it doesn't end
        with a delimiter; appropriate when closing the file.
        """
        if not self.writable() or self.closed:
            return

        if not (syncFlag == 'METADATA' or syncFlag == 'DATA' or syncFlag == 'CLOSE'):
            raise ValueError('syncFlag must be one of these: METADAT, DATA or CLOSE')


        if self.buffer.tell() == 0:
            if force and self.first_write:
                _put_data_with_retry(
                    self.azure.azure,
                    'CREATE',
                    path=self.path.as_posix(),
                    data=None,
                    overwrite='true',
                    write='true',
                    syncFlag=syncFlag,
                    leaseid=self.leaseid,
                    filesessionid=self.filesessionid)
                self.first_write = False
            return

        self.buffer.seek(0)
        data = self.buffer.read()

        syncFlagLocal = 'DATA'
        while len(data) > self.blocksize:
            if self.delimiter:
                place = data[:self.blocksize].rfind(self.delimiter)
            else:
                place = -1
            if place < 0:
                # not found - write whole block
                limit = self.blocksize
            else:
                limit = place + len(self.delimiter)
            if self.first_write:
                _put_data_with_retry(
                    self.azure.azure,
                    'CREATE',
                    path=self.path.as_posix(),
                    data=data[:limit],
                    overwrite='true',
                    write='true',
                    syncFlag=syncFlagLocal,
                    leaseid=self.leaseid,
                    filesessionid=self.filesessionid)
                self.first_write = False
            else:
                _put_data_with_retry(
                    self.azure.azure,
                    'APPEND',
                    path=self.path.as_posix(),
                    data=data[:limit],
                    append='true',
                    syncFlag=syncFlagLocal,
                    leaseid=self.leaseid,
                    filesessionid=self.filesessionid)
            logger.debug('Wrote %d bytes to %s' % (limit, self))
            data = data[limit:]


        self.buffer = io.BytesIO(data)
        self.buffer.seek(0, 2)

        if force:
            zero_offset = self.tell() - len(data)
            if self.first_write:
                _put_data_with_retry(
                    self.azure.azure,
                    'CREATE',
                    path=self.path.as_posix(),
                    data=data,
                    overwrite='true',
                    write='true',
                    syncFlag=syncFlag,
                    leaseid=self.leaseid,
                    filesessionid=self.filesessionid)
                self.first_write = False
            else:
                _put_data_with_retry(
                    self.azure.azure,
                    'APPEND',
                    path=self.path.as_posix(),
                    data=data,
                    offset=zero_offset,
                    append='true',
                    syncFlag=syncFlag,
                    leaseid=self.leaseid,
                    filesessionid=self.filesessionid)
            logger.debug('Wrote %d bytes to %s' % (len(data), self))
            self.buffer = io.BytesIO()

    def close(self):
        """ Close file

        If in write mode, causes flush of any unwritten data.
        """
        logger.info("closing stream")
        if self.closed:
            return
        if self.writable():
            self.flush(syncFlag='CLOSE', force=True)
            self.azure.invalidate_cache(self.path.as_posix())
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

    def __str__(self):
        return "<ADL file: %s>" % (self.path.as_posix())

    __repr__ = __str__

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()


def _fetch_range(rest, path, start, end, stream=False, retry_policy=ExponentialRetryPolicy(), **kwargs):
    logger.debug('Fetch: %s, %s-%s', path, start, end)
    # if the caller gives a bad start/end combination, OPEN will throw and
    # this call will bubble it up
    return rest.call(
        'OPEN', path, offset=start, length=end-start, read='true', stream=stream, retry_policy=retry_policy, **kwargs)


def _fetch_range_with_retry(rest, path, start, end, stream=False, retries=10,
                            delay=0.01, backoff=3, **kwargs):
    err = None
    retry_policy = ExponentialRetryPolicy(max_retries=retries, exponential_retry_interval=delay, exponential_factor=backoff)
    try:
        return _fetch_range(rest, path, start, end, stream=False, retry_policy=retry_policy, **kwargs)
    except Exception as e:
        err = e
        exception = RuntimeError('Max number of ADL retries exceeded: exception ' + repr(err))
        rest.log_response_and_raise(None, exception)


def _put_data(rest, op, path, data, retry_policy=ExponentialRetryPolicy(), **kwargs):
    logger.debug('Put: %s %s, %s', op, path, kwargs)
    return rest.call(op, path=path, data=data, retry_policy=retry_policy, **kwargs)


def _put_data_with_retry(rest, op, path, data, retries=10, delay=0.01, backoff=3,
                         **kwargs):
    err = None
    retry_policy = ExponentialRetryPolicy(max_retries=retries, exponential_retry_interval=delay,
                                          exponential_factor=backoff)
    try:
        return _put_data(rest, op, path, data, retry_policy=retry_policy, **kwargs)
    except (PermissionError, FileNotFoundError) as e:
        rest.log_response_and_raise(None, e)
    except DatalakeBadOffsetException as e:
        rest.log_response_and_raise(None, e)
    except Exception as e:
        err = e
        logger.debug('Exception %s on ADL upload',
                     repr(err))
        exception = RuntimeError('Max number of ADL retries exceeded: exception ' + repr(err))
        rest.log_response_and_raise(None, exception)


class AzureDLPath(type(pathlib.PurePath())):
    """
    Subclass of native object-oriented filesystem path.

    This is used as a convenience class for reducing boilerplate and
    eliminating differences between system-dependent paths.

    We subclass the system's concrete pathlib class due to this issue:

    http://stackoverflow.com/questions/29850801/subclass-pathlib-path-fails

    Parameters
    ----------
    path : AzureDLPath or string
        location of file or directory

    Examples
    --------
    >>> p1 = AzureDLPath('/Users/foo')  # doctest: +SKIP
    >>> p2 = AzureDLPath(p1.name)  # doctest: +SKIP
    """

    def __contains__(self, s):
        """ Return whether string is contained in path. """
        return s in self.as_posix()

    def __getstate__(self):
        return self.as_posix()

    def __setstate__(self, state):
        self.__init__(state)

    @property
    def globless_prefix(self):
        """ Return shortest path prefix without glob quantifiers. """
        parts = []
        for part in self.parts:
            if any(q in part for q in ['*', '?']):
                break
            parts.append(part)
        return pathlib.PurePath(*parts)

    def startswith(self, prefix, *args, **kwargs):
        """ Return whether string starts with the prefix.

        This is equivalent to `str.startswith`.
        """
        return self.as_posix().startswith(prefix.as_posix(), *args, **kwargs)

    def trim(self):
        """ Return path without anchor (concatenation of drive and root). """
        return self.relative_to(self.anchor)
