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
import requests
import requests.exceptions
import time

# 3rd party imports
import adal
import azure

client_id = "1950a258-227b-4e31-a9cf-717495945fc2"


class DatalakeRESTException(IOError):
    pass


def auth(tenant_id, username, password):
    """ User/password authentication

    Parameters
    ----------

    tenant_id : str
        associated with the user's subscription
    username : str
        active directory user
    password : str
        sign-in password

    Returns
    -------
    dict containing authorization token
    """
    # https://github.com/AzureAD/azure-activedirectory-library-for-python/tree/master/sample
    resource = "https://management.core.windows.net/"
    context = adal.AuthenticationContext('https://login.microsoftonline.com/' +
                                         tenant_id)
    out = context.acquire_token_with_username_password(resource, username,
                                                       password, client_id)
    token = {'access': out['accessToken'], 'refresh': out['refreshToken'],
             'time': time.time(), 'tenant': tenant_id}
    return token


def refresh_token(token):
    """ Refresh an expired authorization token

    Parameters
    ----------
    token : dict
        Produced by `auth()` or `refresh_token`.
    """
    tenant_id = token['tenant']
    refresh = token['refresh']
    out = requests.post("https://login.microsoftonline.com/%s/oauth2/token" %
                        tenant_id,
                        data=dict(grant_type='refresh_token',
                                  resource='https://management.core.windows.net/',
                                  client_id=client_id,
                                  refresh_token=refresh))
    out = out.json()
    token = {'access': out['access_token'], 'refresh': out['refresh_token'],
             'time': time.time(), 'tenant': tenant_id}
    return token


def auth_client_secret(tenant_id, client_id, client_secret):
    # from https://azure.microsoft.com/en-gb/documentation/articles/
    #   data-lake-store-get-started-rest-api/
    #   #how-do-i-authenticate-using-azure-active-directory/
    r = requests.post("https://login.microsoftonline.com/%s/oauth2/token" %
                      tenant_id,
                      params={'grant_type': 'client_credentials',
                              'resource': 'https://management.core.windows.net/',
                              'client_id': client_id,
                              'client_secret': client_secret})
    return r.status_code, r.json


class ManagementRESTInterface:
    """ Call factory for account-level activities
    """
    ends = {}

    def __init__(self, subscription_id, resource_group_name, token):
        self.subscription_id = subscription_id
        self.resource_group_name = resource_group_name
        self.token = token
        self.params = {'api-version': '2015-10-01-preview'}
        self.head = {
            'Authorization': 'Bearer ' + token,
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
    token: str
        from `auth()` or `refresh_token()`, the 'access' field.
    """

    ends = {
        # OP: (HTTP method, required fields, allowed fields)
        'APPEND': ('post', set(), set()),
        'CHECKACCESS': ('get', set(), {'fsaction'}),
        'CONCAT': ('post', {'sources'}, {'sources'}),
        'CREATE': ('put', set(), {'overwrite'}),
        'DELETE': ('delete', set(), {'recursive'}),
        'GETCONTENTSUMMARY': ('get', set(), set()),
        'GETFILESTATUS': ('get', set(), set()),
        'LISTSTATUS': ('get', set(), set()),
        'MKDIRS': ('put', set(), set()),
        'OPEN': ('get', set(), {'offset', 'length'}),
        'RENAME': ('put', {'destination'}, {'destination'}),
        'TRUNCATE': ('post', {'newlength'}, {'newlength'})
    }

    def __init__(self, store_name, token):
        self.head = {'Authorization': 'Bearer ' + token}
        self.url = ('https://%s.azuredatalakestore.net/webhdfs/v1/' %
                    store_name)

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
        # logger.debug('Call: (%s, %s, %s)' % (method, url, params))
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

Note that permissions, acl, group/owners can only be set at the root level.

GETFILECHECKSUM
GETHOMEDIRECTORY
GETDELEGATIONTOKEN n/a - use auth
GETDELEGATIONTOKENS n/a - use auth
GETXATTRS
LISTXATTRS
CREATESYMLINK n/a
SETREPLICATION n/a
SETOWNER
SETPERMISSION
SETTIMES
RENEWDELEGATIONTOKEN n/a - use auth
CANCELDELEGATIONTOKEN n/a - use auth
CREATESNAPSHOT
RENAMESNAPSHOT
SETXATTR
REMOVEXATTR
"""
