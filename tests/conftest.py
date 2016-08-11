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

import pytest


@pytest.yield_fixture
def azure():
    from adlfs import AzureDLFileSystem
    test_dir = 'azure_test_dir/'

    out = AzureDLFileSystem()
    print("connected to filesystem")
    out.mkdir(test_dir)
    print("created {}".format(test_dir))
    try:
        yield out
    finally:
        out.rm(test_dir, recursive=True)
        print("removed {}".format(test_dir))


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
