# -*- coding: utf-8 -*-
# coding=utf-8
# --------------------------------------------------------------------------
# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See License.txt in the project root for
# license information.
# --------------------------------------------------------------------------

from contextlib import contextmanager
from hashlib import md5
import os
import shutil
import tempfile
import uuid

import vcr


def _build_func_path_generator(function):
    import inspect
    module = os.path.basename(inspect.getfile(function)).replace('.py', '')
    return module + '/' + function.__name__


recording_path = os.path.join(os.path.dirname(__file__), 'recordings')

my_vcr = vcr.VCR(
    cassette_library_dir=recording_path,
    record_mode="once",
    func_path_generator=_build_func_path_generator,
    path_transformer=vcr.VCR.ensure_suffix('.yaml'),
    filter_headers=['authorization'],
    match_on=['uri', 'method'],
    )


@contextmanager
def open_azure(directory='azure_test_dir/'):
    from adlfs import AzureDLFileSystem

    fs = AzureDLFileSystem()
    if directory is None:
        yield fs
    else:
        subdirectory = os.path.join(directory, uuid.uuid4().hex[:8])
        fs.mkdir(subdirectory)
        try:
            yield fs
        finally:
            fs.rm(subdirectory, recursive=True)


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


def md5sum(fname, chunksize=4096):
    hashobj = md5()
    with open(fname, 'rb') as f:
        for chunk in iter(lambda: f.read(chunksize), b''):
            hashobj.update(chunk)
    return hashobj.hexdigest()
