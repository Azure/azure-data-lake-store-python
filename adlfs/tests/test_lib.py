import os
import pytest
import requests.exceptions
import time

from adlfs.lib import (auth, refresh_token, DatalakeRESTInterface,
                       DatalakeRESTException)

(??)
@pytest.fixture()
def token():
    tenant_id = os.environ['azure_tenant_id']
    username = os.environ['azure_username']
    password = os.environ['azure_password']
    token = auth(tenant_id, username, password)
    return token


@pytest.fixture()
def rest(token):
    store_name = os.environ['azure_store_name']
    return DatalakeRESTInterface(store_name, token['access'])


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


def test_auth_refresh(token):
    assert token['access']
    time.sleep(3)
    token2 = refresh_token(token)
    assert token2['access']
    assert token['access'] != token2['access']
    assert token2['time'] > token['time']


def test_response(rest):
    out = rest.call('GETCONTENTSUMMARY')
    assert out
