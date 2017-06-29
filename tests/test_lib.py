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
from azure.datalake.store.lib import (
    DataLakeCredential, DatalakeRESTInterface)

from tests import settings
from tests.testing import my_vcr

@pytest.fixture()
def token():
    return settings.TOKEN

@pytest.fixture()
def principal_token():
    return settings.PRINCIPAL_TOKEN

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
def test_auth_refresh(token):
    assert token.token['access']
    initial_access = token.token['access']
    initial_time = token.token['time']
    time.sleep(3)
    token.refresh_token()
    token2 = DataLakeCredential(token.token)
    assert token2.token['access']
    assert initial_access != token2.token['access']
    assert token2.token['time'] > initial_time

@my_vcr.use_cassette
def test_auth_refresh_for_service_principal(principal_token):
    assert principal_token.token['access']
    assert principal_token.token['secret']
    initial_access = principal_token.token['access']
    initial_time = principal_token.token['time']
    time.sleep(3)
    principal_token.refresh_token()
    token2 = DataLakeCredential(principal_token.token)
    assert token2.token['access']
    assert token2.token['secret']
    assert token2.token['secret'] == principal_token.token['secret']
    assert initial_access != token2.token['access']
    assert token2.token['time'] > initial_time

@my_vcr.use_cassette
def test_response(rest):
    out = rest.call('LISTSTATUS', '')
    assert out
