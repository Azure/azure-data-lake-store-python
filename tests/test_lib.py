# -*- coding: utf-8 -*-
# coding=utf-8
# --------------------------------------------------------------------------
# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See License.txt in the project root for
# license information.
# --------------------------------------------------------------------------

import pytest
import time

from azure.datalake.store.exceptions import DatalakeRESTException
from azure.datalake.store.lib import (DatalakeRESTInterface)

from tests import settings
from tests.testing import my_vcr

@pytest.fixture()
def token():
    return settings.TOKEN_CREDEDENTIAL

@pytest.fixture()
def rest(token):
    return DatalakeRESTInterface(settings.STORE_NAME, token)

def test_errors(token):
    no_rest = DatalakeRESTInterface("none", token)

    with pytest.raises(ValueError):
        # no such op
        no_rest.call('NONEXISTENT')

    with pytest.raises(ValueError):
        # too few parameters
        no_rest.call("RENAME")

    with pytest.raises(ValueError):
        # too many parameters
        no_rest.call('RENAME', many='', additional='', pars='')

    with pytest.raises(DatalakeRESTException):
        # no such store
        no_rest.call('GETCONTENTSUMMARY')


@my_vcr.use_cassette
def test_response(rest):
    out = rest.call('LISTSTATUS', '')
    assert out
