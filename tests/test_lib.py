# -*- coding: utf-8 -*-
# coding=utf-8
# --------------------------------------------------------------------------
# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See License.txt in the project root for
# license information.
# --------------------------------------------------------------------------

import pytest
import time

from adlfs.lib import (refresh_token, DatalakeRESTInterface,
                       DatalakeRESTException, ManagementRESTInterface)

from tests import settings
from tests.testing import my_vcr


@pytest.fixture()
def token():
    return settings.TOKEN


@pytest.fixture()
def rest(token):
    return DatalakeRESTInterface(settings.STORE_NAME, token['access'])


@pytest.fixture()
def management(token):
    return ManagementRESTInterface(settings.SUBSCRIPTION_ID,
                                   settings.RESOURCE_GROUP_NAME,
                                   token['access'])


def test_errors():
    no_rest = DatalakeRESTInterface("none", "blank")

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
    assert token['access']
    time.sleep(3)
    token2 = refresh_token(token)
    assert token2['access']
    assert token['access'] != token2['access']
    assert token2['time'] > token['time']


@my_vcr.use_cassette
def test_response(rest):
    out = rest.call('GETCONTENTSUMMARY')
    assert out


@my_vcr.use_cassette
def test_account_info(management):
    code, obj = management.info(settings.STORE_NAME)
    assert code == 200
    assert obj['id']
    assert obj['type'] == "Microsoft.DataLakeStore/accounts"


@my_vcr.use_cassette
def test_account_list_in_sub(management):
    code, obj = management.list_in_sub()
    assert code == 200
    assert obj['value']
    assert len(obj['value']) > 0
    accounts = obj['value']
    assert accounts[0]['type'] == "Microsoft.DataLakeStore/accounts"


@my_vcr.use_cassette
def test_account_list_in_res(management):
    code, obj = management.list_in_res()
    assert code == 200
    assert obj['value']
    assert len(obj['value']) > 0
    accounts = obj['value']
    assert accounts[0]['type'] == "Microsoft.DataLakeStore/accounts"
