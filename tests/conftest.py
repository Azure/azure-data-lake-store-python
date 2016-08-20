# -*- coding: utf-8 -*-
# coding=utf-8
# --------------------------------------------------------------------------
# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See License.txt in the project root for
# license information.
# --------------------------------------------------------------------------

import pytest

from adlfs import AzureDLFileSystem
from tests.testing import default_home


@pytest.fixture(scope="session")
def teardown_env():
    fs = AzureDLFileSystem.current()
    fs.rm(default_home(), recursive=True)


@pytest.fixture(scope="session", autouse=True)
def setup_env(request):
    fs = AzureDLFileSystem()
    fs.mkdir(default_home())
    request.addfinalizer(teardown_env)
