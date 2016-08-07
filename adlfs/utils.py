# -*- coding: utf-8 -*-
# coding=utf-8
# --------------------------------------------------------------------------
# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See License.txt in the project root for
# license information.
# --------------------------------------------------------------------------

import array
from contextlib import contextmanager
from hashlib import md5
import logging
import os
import platform
import pytest
import shutil
import sys
import tempfile

PY2 = sys.version_info.major == 2

try:
    FileNotFoundError = FileNotFoundError
except NameError:
    class FileNotFoundError(IOError):
        pass

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
ch = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s - ADLFS - %(levelname)s'
                              ' - %(message)s')
ch.setFormatter(formatter)
logger.handlers = [ch]

WIN = platform.platform() == 'Windows'

if WIN:
    datadir = os.path.join(os.environ['APPDATA'], 'adlfs')
else:
    datadir = os.sep.join([os.path.expanduser("~"), '.config', 'adlfs'])

try:
    os.makedirs(datadir)
except:
    pass


def ensure_writable(b):
    if PY2 and isinstance(b, array.array):
        return b.tostring()
    return b


@contextmanager
def ignoring(*exceptions):
    try:
        yield
    except exceptions:
        pass


@contextmanager
def tmpfile(extension='', dir=None):
    extension = '.' + extension.lstrip('.')
    handle, filename = tempfile.mkstemp(extension, dir=dir)
    os.close(handle)
    os.remove(filename)

    try:
        yield filename
    finally:
        if os.path.exists(filename):
            if os.path.isdir(filename):
                shutil.rmtree(filename)
            else:
                with ignoring(OSError):
                    os.remove(filename)


@pytest.yield_fixture
def azure():
    from adlfs import AzureDLFileSystem
    from adlfs.lib import auth
    test_dir = 'azure_test_dir/'

    out = AzureDLFileSystem()
    out.mkdir(test_dir)
    try:
        yield out
    finally:
        out.rm(test_dir, recursive=True)


def read_block(f, offset, length, delimiter=None):
    """ Read a block of bytes from a file

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

    If using the ``delimiter=`` keyword argument we ensure that the read
    starts and stops at delimiter boundaries that follow the locations
    ``offset`` and ``offset + length``.  If ``offset`` is zero then we
    start at zero.  The bytestring returned WILL include the
    terminating delimiter string.

    Examples
    --------

    >>> from io import BytesIO  # doctest: +SKIP
    >>> f = BytesIO(b'Alice, 100\\nBob, 200\\nCharlie, 300')  # doctest: +SKIP
    >>> read_block(f, 0, 13)  # doctest: +SKIP
    b'Alice, 100\\nBo'

    >>> read_block(f, 0, 13, delimiter=b'\\n')  # doctest: +SKIP
    b'Alice, 100\\nBob, 200\\n'

    >>> read_block(f, 10, 10, delimiter=b'\\n')  # doctest: +SKIP
    b'Bob, 200\\nCharlie, 300'
    """
    if delimiter:
        f.seek(offset)
        seek_delimiter(f, delimiter, 2**16)
        start = f.tell()
        length -= start - offset

        f.seek(start + length)
        seek_delimiter(f, delimiter, 2**16)
        end = f.tell()
        eof = not f.read(1)

        offset = start
        length = end - start

    f.seek(offset)
    bytes = f.read(length)
    return bytes


def seek_delimiter(file, delimiter, blocksize):
    """ Seek current file to next byte after a delimiter bytestring

    This seeks the file to the next byte following the delimiter.  It does
    not return anything.  Use ``file.tell()`` to see location afterwards.

    Parameters
    ----------
    file: a file
    delimiter: bytes
        a delimiter like ``b'\n'`` or message sentinel
    blocksize: int
        Number of bytes to read from the file at once.
    """

    if file.tell() == 0:
        return

    last = b''
    while True:
        current = file.read(blocksize)
        if not current:
            return
        full = last + current
        try:
            i = full.index(delimiter)
            file.seek(file.tell() - (len(full) - i) + len(delimiter))
            return
        except ValueError:
            pass
        last = full[-len(delimiter):]


def tokenize(*args, **kwargs):
    """ Deterministic token

    >>> tokenize('Hello') == tokenize('Hello')
    True
    """
    if kwargs:
        args = args + (kwargs,)
    return md5(str(tuple(args)).encode()).hexdigest()


def commonprefix(paths):
    """ Find common directory for all paths

    Python's ``os.path.commonprefix`` will not return a valid directory path in
    some cases, so we wrote this convenience method.

    Examples
    --------

    >>> # os.path.commonprefix returns '/disk1/foo'
    >>> commonprefix(['/disk1/foobar', '/disk1/foobaz'])
    '/disk1'

    >>> commonprefix(['a/b/c', 'a/b/d', 'a/c/d'])
    'a'

    >>> commonprefix(['a/b/c', 'd/e/f', 'g/h/i'])
    ''
    """
    return os.path.dirname(os.path.commonprefix(paths))
