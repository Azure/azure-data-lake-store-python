# -*- coding: utf-8 -*-
# coding=utf-8
# --------------------------------------------------------------------------
# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See License.txt in the project root for
# license information.
# --------------------------------------------------------------------------

"""
Low-level calls to REST end-points.

Specific interfaces to the Data-lake Store file- and management layers,
and authentication code.
"""

# standard imports
import json
import logging
import os
import requests
import requests.exceptions
import time

# 3rd party imports
import adal
import azure

client_id = "1950a258-227b-4e31-a9cf-717495945fc2"

logger = logging.getLogger(__name__)


class DatalakeRESTException(IOError):
    pass

default_tenant = os.environ.get('azure_tenant_id', "common")
default_username = os.environ.get('azure_username', None)
default_password = os.environ.get('azure_password', None)
default_client = os.environ.get('azure_client_id', None)
default_secret = os.environ.get('azure_client_secret', None)
default_resource = "https://management.core.windows.net/"
default_store = os.environ.get('azure_store_name', None)
default_suffix = os.environ.get('azure_url_suffix', '')


def refresh_token(token):
    """ Refresh an expired authorization token

    Parameters
    ----------
    token : dict
        Produced by `auth()` or `refresh_token`.
    """
    if token.get('refresh', False) is False:
        raise ValueError("Token cannot be aut-refreshed.")
    context = adal.AuthenticationContext('https://login.microsoftonline.com/' +
                                         token['tenant'])
    out = context.acquire_token_with_refresh_token(token['refresh'],
                                                   client_id=token['client'],
                                                   resource=token['resource'])
    out.update({'access': out['accessToken'], 'refresh': out['refreshToken'],
                'time': time.time(), 'tenant': token['tenant'],
                'resource': token['resource'], 'client': token['client']})
    return out


def auth(tenant_id=default_tenant, username=default_username,
         password=default_password, client_id=default_client,
         client_secret=default_secret, resource=default_resource, **kwargs):
    """ User/password authentication

    Parameters
    ----------

    tenant_id : str
        associated with the user's subscription, or "common"
    username : str
        active directory user
    password : str
        sign-in password
    client_id : str
        the service principal client
    client_secret : str
        the secret associated with the client_id
    resource : str
        resource for auth (e.g., https://management.core.windows.net/)
    kwargs : key/values
        Other parameters, see http://msrestazure.readthedocs.io/en/latest/msrestazure.html#module-msrestazure.azure_active_directory
        Examples: auth_uri, token_uri, keyring

    Returns
    -------
    auth dict
    """
    context = adal.AuthenticationContext('https://login.microsoftonline.com/' +
                                         tenant_id)

    if tenant_id is None:
        raise ValueError("tenant_id must be supplied for authentication")

    if username and password:
        out = context.acquire_token_with_username_password(resource, username,
                                                           password, client_id)
    elif client_id and client_secret:
        out = context.acquire_token_with_client_credentials(resource, client_id,
                                                            client_secret)
    else:
        raise ValueError("No authentication method found for credentials")
    out.update({'access': out['accessToken'], 'resource': resource,
                'refresh': out.get('refreshToken', False),
                'time': time.time(), 'tenant': tenant_id, 'client': client_id})
    return out


class ManagementRESTInterface:
    """ Call factory for account-level activities
    """
    ends = {}

    def __init__(self, subscription_id, resource_group_name, token=None,
                 **kwargs):
        self.subscription_id = subscription_id
        self.resource_group_name = resource_group_name
        self.token = token or auth(**kwargs)
        self.params = {'api-version': '2015-10-01-preview'}
        self.head = {
            'Authorization': 'Bearer ' + token['access'],
            'Content-Type': 'application/json'
        }
        self.url = ('https://management.azure.com/subscriptions/%s/'
                    'resourceGroups/%s/providers/Microsoft.DataLakeStore/' % (
                     subscription_id, resource_group_name))

    def create(self, account, location='eastus2', tags={}):
        body = json.dumps({
            "location": location,
            "tags": tags,
            "properties": {"configuration": {}}
        })
        url = self.url + 'accounts/' + account
        r = requests.put(url, headers=self.head, params=self.params, data=body)
        return r.status_code, r.json()

    def delete(self, account):
        url = self.url + 'accounts/' + account
        r = requests.delete(url, headers=self.head, params=self.params)
        return r.status_code, r.json()

    def list_in_sub(self):
        url = ('https://management.azure.com/subscriptions/%s/providers/'
               'Microsoft.DataLakeStore/accounts' % self.subscription_id)
        r = requests.get(url, headers=self.head, params=self.params)
        return r.status_code, r.json()

    def list_in_res(self):
        url = self.url + 'accounts'
        r = requests.get(url, headers=self.head, params=self.params)
        return r.status_code, r.json()

    def info(self, account):
        url = self.url + 'accounts/' + account
        r = requests.get(url, headers=self.head, params=self.params)
        return r.status_code, r.json()


class DatalakeRESTInterface:
    """ Call factory for webHDFS endpoints on ADLS

    Parameters
    ----------
    store_name: str
    token: dict
        from `auth()` or `refresh_token()` or other ADAL source
    url_suffix: str (None)
        Domain to send REST requests to. The end-point URL is constructed
        using this and the store_name. If None, use default.
    kwargs: optional arguments to auth
        See ``auth()``. Includes, e.g., username, password, tenant; will pull
        values from environment variables if not provided.
    """

    ends = {
        # OP: (HTTP method, required fields, allowed fields)
        'APPEND': ('post', set(), {'append'}),
        'CHECKACCESS': ('get', set(), {'fsaction'}),
        'CONCAT': ('post', {'sources'}, {'sources'}),
        'MSCONCAT': ('post', set(), {'deleteSourceDirectory'}),
        'CREATE': ('put', set(), {'overwrite', 'write'}),
        'DELETE': ('delete', set(), {'recursive'}),
        'GETCONTENTSUMMARY': ('get', set(), set()),
        'GETFILESTATUS': ('get', set(), set()),
        'LISTSTATUS': ('get', set(), set()),
        'MKDIRS': ('put', set(), set()),
        'OPEN': ('get', set(), {'offset', 'length', 'read'}),
        'RENAME': ('put', {'destination'}, {'destination'}),
        'TRUNCATE': ('post', {'newlength'}, {'newlength'}),
        'SETOWNER': ('put', set(), {'owner', 'group'}),
        'SETPERMISSION': ('put', set(), {'permission'})
    }

    def __init__(self, store_name=default_store, token=None,
                 url_suffix=default_suffix, **kwargs):
        url_suffix = url_suffix or "azuredatalakestore.net"
        if token is None:
            token = auth(**kwargs)
        self.token = token
        self.head = {'Authorization': 'Bearer ' + token['access']}
        self.url = 'https://%s.%s/webhdfs/v1/' % (store_name, url_suffix)

    def _check_token(self):
        if time.time() - self.token['time'] > self.token['expiresIn'] - 100:
            self.token = refresh_token(self.token)
            self.head = {'Authorization': 'Bearer ' + self.token['access']}


    def call(self, op, path='', **kwargs):
        """ Execute a REST call

        Parameters
        ----------
        op: str
            webHDFS operation to perform, one of `DatalakeRESTInterface.ends`
        path: str
            filepath on the remote system
        kwargs: dict
            other parameters, as defined by the webHDFS standard and
            https://msdn.microsoft.com/en-us/library/mt710547.aspx
        """
        if op not in self.ends:
            raise ValueError("No such op: %s", op)
        self._check_token()
        method, required, allowed = self.ends[op]
        data = kwargs.pop('data', b'')
        keys = set(kwargs)
        if required > keys:
            raise ValueError("Required parameters missing: %s",
                             required - keys)
        if keys - allowed > set():
            raise ValueError("Extra parameters given: %s",
                             keys - allowed)
        params = {'OP': op}
        params.update(kwargs)
        func = getattr(requests, method)
        url = self.url + path
        try:
            r = func(url, params=params, headers=self.head, data=data)
        except requests.exceptions.RequestException as e:
            raise DatalakeRESTException('HTTP error: %s', str(e))
        if r.status_code >= 400:
            raise DatalakeRESTException("Data-lake REST exception: %s, %s, %s" %
                                        (op, r.status_code, r.content.decode()))
        if r.content:
            if r.content.startswith(b'{'):
                try:
                    out = r.json()
                    if out.get('boolean', True) is False:
                        raise DatalakeRESTException('Operation failed: %s, %s',
                                                    op, path)
                except ValueError:
                    out = r.content
            else:
                # because byte-strings can happen to look like json
                out = r.content
        else:
            out = r
        return out

"""
Not yet implemented (or not applicable)
http://hadoop.apache.org/docs/stable/hadoop-project-dist/hadoop-hdfs/WebHDFS.html

GETFILECHECKSUM
GETHOMEDIRECTORY
GETDELEGATIONTOKEN n/a - use auth
GETDELEGATIONTOKENS n/a - use auth
GETXATTRS
LISTXATTRS
CREATESYMLINK n/a
SETREPLICATION n/a
SETTIMES
RENEWDELEGATIONTOKEN n/a - use auth
CANCELDELEGATIONTOKEN n/a - use auth
CREATESNAPSHOT
RENAMESNAPSHOT
SETXATTR
REMOVEXATTR
"""
