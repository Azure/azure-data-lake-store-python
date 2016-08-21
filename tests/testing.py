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

import pytest
import vcr

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
    request.uri = _scrub(request.uri)
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
    record_mode="once",
    before_record=_scrub_sensitive_request_info,
    before_record_response=_scrub_sensitive_response_info,
    func_path_generator=_build_func_path_generator,
    path_transformer=vcr.VCR.ensure_suffix('.yaml'),
    filter_headers=['authorization'],
    )


def working_dir():
    if not hasattr(working_dir, "path"):
        working_dir.path = os.path.join('azure_test_dir', '')
    return working_dir.path


@pytest.yield_fixture()
def azure():
    from adlfs import AzureDLFileSystem
    fs = AzureDLFileSystem.current()

    # Clear filesystem cache to ensure we capture all requests from a test
    fs.invalidate_cache()

    yield fs


@contextmanager
def azure_teardown(fs):
    try:
        yield
    finally:
        for path in fs.ls(working_dir()):
            if fs.exists(path):
                fs.rm(path, recursive=True)


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
