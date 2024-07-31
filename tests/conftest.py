# -*- coding: utf-8 -*-
# coding=utf-8
# --------------------------------------------------------------------------
# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See License.txt in the project root for
# license information.
# --------------------------------------------------------------------------

import pytest

from azure.datalake.store import AzureDLFileSystem
from tests import settings
from tests.testing import working_dir


@pytest.fixture(scope="session", autouse=True)
def setup_env(request):
    home = working_dir()
    fs = AzureDLFileSystem(store_name=settings.STORE_NAME, token_credential=settings.TOKEN_CREDEDENTIAL)
    if settings.RECORD_MODE != 'none':
        if not fs.exists(home):
            fs.mkdir(home)
        else:
            fs.rm(home, recursive=True)
            fs.mkdir(home)
