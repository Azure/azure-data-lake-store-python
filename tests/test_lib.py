import os
import pytest
import requests.exceptions
import time

from adlfs.lib import (auth, refresh_token, DatalakeRESTInterface,
                       DatalakeRESTException, ManagementRESTInterface)


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


@pytest.fixture()
def management(token):
    subscription_id = os.environ['azure_subscription_id']
    resource_group_name = os.environ['azure_resource_group_name']
    return ManagementRESTInterface(subscription_id, resource_group_name, token['access'])


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


def test_account_info(management):
    account = os.environ['azure_store_name']
    code, obj = management.info(account)
    assert code == 200
    assert obj['id']
    assert obj['name'] == account
    assert obj['type'] == "Microsoft.DataLakeStore/accounts"


def test_account_list_in_sub(management):
    account = os.environ['azure_store_name']
    code, obj = management.list_in_sub()
    assert code == 200
    assert obj['value']
    assert len(obj['value']) == 1
    accounts = obj['value']
    assert accounts[0]['name'] == account
    assert accounts[0]['type'] == "Microsoft.DataLakeStore/accounts"


def test_account_list_in_res(management):
    account = os.environ['azure_store_name']
    code, obj = management.list_in_res()
    assert code == 200
    assert obj['value']
    assert len(obj['value']) == 1
    accounts = obj['value']
    assert accounts[0]['name'] == account
    assert accounts[0]['type'] == "Microsoft.DataLakeStore/accounts"
