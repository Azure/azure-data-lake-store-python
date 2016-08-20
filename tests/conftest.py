# -*- coding: utf-8 -*-
# coding=utf-8
# --------------------------------------------------------------------------
# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See License.txt in the project root for
# license information.
# --------------------------------------------------------------------------

import pytest

from adlfs import AzureDLFileSystem
from tests.testing import working_dir


@pytest.fixture(scope="session", autouse=True)
def setup_env(request):
    home = working_dir()
    fs = AzureDLFileSystem()
    if not fs.exists(home):
        fs.mkdir(home)
