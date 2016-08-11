# -*- coding: utf-8 -*-
# coding=utf-8
# --------------------------------------------------------------------------
# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See License.txt in the project root for
# license information.
# --------------------------------------------------------------------------

from contextlib import contextmanager
import os
import shutil
import tempfile

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
    )


@contextmanager
def open_azure():
    from adlfs import AzureDLFileSystem
    test_dir = 'azure_test_dir/'

    fs = AzureDLFileSystem()
    fs.mkdir(test_dir)
    try:
        yield fs
    finally:
        fs.rm(test_dir, recursive=True)


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
