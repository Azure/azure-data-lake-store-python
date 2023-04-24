# -*- coding: utf-8 -*-
# coding=utf-8
# --------------------------------------------------------------------------
# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See License.txt in the project root for
# license information.
# --------------------------------------------------------------------------

from contextlib import contextmanager
import copy
from hashlib import md5
import os
import shutil
import tempfile
import uuid

import pytest
import vcr

from azure.datalake.store.core import AzureDLPath
from tests import fake_settings, settings


def _build_func_path_generator(function):
    import inspect
    module = os.path.basename(inspect.getfile(function)).replace('.py', '')
    return module + '/' + function.__name__


def _scrub(val):
    val = val.replace(settings.STORE_NAME, fake_settings.STORE_NAME)
    val = val.replace(settings.TENANT_ID, fake_settings.TENANT_ID)
    val = val.replace(settings.SUBSCRIPTION_ID, fake_settings.SUBSCRIPTION_ID)
    val = val.replace(settings.RESOURCE_GROUP_NAME, fake_settings.RESOURCE_GROUP_NAME)
    return val


def _scrub_sensitive_request_info(request):
    request.uri = _scrub(request.uri.replace('%5C', '/'))
    return request


def _scrub_sensitive_response_info(response):
    response = copy.deepcopy(response)

    headers = response.get('headers')
    if headers:
        for name, val in headers.items():
            for i, v in enumerate(val):
                val[i] = _scrub(v)

    return response


recording_path = os.path.join(os.path.dirname(__file__), 'recordings')

my_vcr = vcr.VCR(
    cassette_library_dir=recording_path,
    record_mode=settings.RECORD_MODE,
    before_record=_scrub_sensitive_request_info,
    before_record_response=_scrub_sensitive_response_info,
    func_path_generator=_build_func_path_generator,
    path_transformer=vcr.VCR.ensure_suffix('.yaml'),
    filter_headers=['authorization'],
    )


def working_dir():
    if not hasattr(working_dir, "path"):
        unique_dir = 'azure_python_sdk_test_dir' + str(uuid.uuid4())
        working_dir.path = AzureDLPath(unique_dir)
    return working_dir.path


@pytest.yield_fixture()
def azure():
    from azure.datalake.store import AzureDLFileSystem
    fs = AzureDLFileSystem(token=settings.TOKEN, store_name=settings.STORE_NAME)

    # Clear filesystem cache to ensure we capture all requests from a test
    fs.invalidate_cache()

    yield fs

@pytest.yield_fixture()
def second_azure():
    from azure.datalake.store import AzureDLFileSystem
    fs = AzureDLFileSystem(token=settings.TOKEN, store_name=settings.STORE_NAME)

    # Clear filesystem cache to ensure we capture all requests from a test
    fs.invalidate_cache()
    yield fs


@contextmanager
def create_files(azure, number_of_files, root_path = working_dir(), prefix=''):
    import itertools
    from string import ascii_lowercase

    def generate_paths():
        def iter_all_strings():
            for size in itertools.count(1):
                for s in itertools.product(ascii_lowercase, repeat=size):
                    yield "".join(s)

        for s in itertools.islice(iter_all_strings(), number_of_files):
            s = AzureDLPath(prefix + s + ".txt")
            yield root_path / s
    for f in generate_paths():
        azure.touch(f)


@contextmanager
def azure_teardown(fs):
    try:
        fs.mkdir(working_dir())
        yield
    finally:
        # this is a best effort. If there is an error attempting to delete during cleanup,
        # print it, but it should not cause the test to fail.
        try:
            fs.rm(working_dir(), recursive=True)
            for path in fs.ls(working_dir(), invalidate_cache=False):
                if fs.exists(path, invalidate_cache=False):
                    fs.rm(path, recursive=True)
        except Exception as e:
            print('warning: cleanup failed with exception:')
            print(e)

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


def posix(*args):
    return AzureDLPath(*args).as_posix()
