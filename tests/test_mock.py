# -*- coding: utf-8 -*-
# coding=utf-8
# --------------------------------------------------------------------------
# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See License.txt in the project root for
# license information.
# --------------------------------------------------------------------------

"""
Tests to be executed with mock framework (HTTPretty) rather than actual server.

Do not use mock framework for functionality tests like end to end operation calls.
Only use it for specific internal logic which can't be tested on server.
Example: How will the logic behave in case of specific error from server side.

This was introduced to test the Retry Policy but can be carefully used for other tests as well.
"""

import responses
from requests import ConnectionError, ConnectTimeout, ReadTimeout, Timeout, HTTPError

from tests import settings
from azure.datalake.store.exceptions import DatalakeRESTException
from azure.datalake.store.lib import auth
from azure.datalake.store.lib import DataLakeCredential
from tests.testing import azure, azure_teardown, posix, working_dir
test_dir = working_dir()

a = posix(test_dir / 'a')
mock_url = 'https://%s.azuredatalakestore.net/webhdfs/v1/' % settings.STORE_NAME

def test_retry_read_timeout(azure):
    __test_retry_error(azure, 200, 2, body=ReadTimeout())

def test_retry_timeout(azure):
    __test_retry_error(azure, 200, 2, body=Timeout())

def test_retry_connection_error(azure):
    __test_retry_error(azure, 200, 2, body=ConnectionError())

def test_retry_connection_timeout(azure):
    __test_retry_error(azure, 200, 2, body=ConnectTimeout())

def test_retry_500(azure):
    __test_retry_error(azure, 500, 2)

def test_retry_401(azure):
    __test_retry_error(azure, 401, 3)

def test_retry_408(azure):
    __test_retry_error(azure, 408, 4)

def test_retry_429(azure):
    __test_retry_error(azure, 429, 2)

def test_retry_500_5retry(azure):
    __test_retry_error(azure, 500, 5)

def test_retry_500_6retry(azure):
    # exceeded max tries
    __test_retry_error(azure, 500, 6, is_exception_expected=True)

def test_retry_400(azure):
    __test_retry_error(azure, 400, 2, is_exception_expected=True)

def test_retry_501(azure):
    __test_retry_error(azure, 501, 2, is_exception_expected=True)

def test_retry_505(azure):
    __test_retry_error(azure, 505, 2, is_exception_expected=True)

def test_retry_200(azure):
    __test_retry_error(azure, 200, 1)

@responses.activate
def __test_retry_error(azure,
                       error_status,
                       total_tries,
                       is_exception_expected=False,
                       last_try_status=200,
                       body=""):
    mock_url_a = mock_url + a
    while total_tries>1:
        responses.add(responses.PUT, mock_url_a,
                      body=body, status=error_status)
        total_tries -= 1
    responses.add(responses.PUT, mock_url_a,
                  body="", status=last_try_status)

    # teardown not required in mock tests
    try:
        azure.mkdir(a)
        assert not is_exception_expected
    except DatalakeRESTException:
        assert is_exception_expected


@responses.activate
def __test_retry_auth(error_code, error_string, is_exception_expected, total_tries=4, last_try_status=200,
                      last_try_body=r'{"token_type":"Bearer","expires_in":"1","ext_expires_in":"1","expires_on":"1","not_before":"1","resource":"https://datalake.azure.net/","access_token":"a"}'):
    import re, adal
    end_point = re.compile("https:\/\/login\.microsoftonline\.com\/common\/discovery\/instance\?authorization_endpoint=.+")
    mock_url = "https://login.microsoftonline.com/" + settings.TENANT_ID+ "/oauth2/token"

    body_discovery = r'{"tenant_discovery_endpoint":"https://login.microsoftonline.com/'+ settings.TENANT_ID + '/.well-known/openid-configuration"}'
    while total_tries > 0:
        responses.add(responses.GET, end_point,
                  body=body_discovery,
                  status=200)

        responses.add(responses.POST, mock_url, body=r'{"error":"' + error_string + r'","error_description":"0","error_codes":[0],"timestamp":"0","trace_id":"0","correlation_id":"0"}'
                  , status=error_code)
        total_tries -= 1

    responses.add(responses.GET, end_point,
                  body=body_discovery,
                  status=200)
    responses.add(responses.POST, mock_url, body=last_try_body, status=last_try_status)
    try:
        token = auth()
        assert isinstance(token, DataLakeCredential)
        assert not is_exception_expected
    except (HTTPError, adal.adal_error.AdalError):
        assert is_exception_expected


def test_retry_auth_401():
    __test_retry_auth(error_code=401, error_string=r'invalid_client', is_exception_expected=True)


def test_retry_auth_104():
    __test_retry_auth(error_code=104, error_string=r'Connection Error', is_exception_expected=False, )
    __test_retry_auth(error_code=104, error_string=r'Connection Error', is_exception_expected=True, total_tries=6)


def test_retry_auth_429():
    __test_retry_auth(error_code=429, error_string=r'Too many requests', is_exception_expected=False)
    __test_retry_auth(error_code=429, error_string=r'Too many requests', is_exception_expected=True, total_tries=6)
