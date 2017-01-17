# -*- coding: utf-8 -*-
# coding=utf-8
# --------------------------------------------------------------------------
# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See License.txt in the project root for
# license information.
# --------------------------------------------------------------------------

import base64
import os
import time

from azure.datalake.store.lib import auth
from tests import fake_settings


RECORD_MODE = os.environ.get('RECORD_MODE', 'none').lower()

if RECORD_MODE == 'none':
    STORE_NAME = fake_settings.STORE_NAME
    TENANT_ID = fake_settings.TENANT_ID
    TOKEN = dict(
        access=str(base64.b64encode(os.urandom(1420))),
        refresh=str(base64.b64encode(os.urandom(718))),
        time=time.time(), client='common',
        resource="https://management.core.windows.net/",
        tenant=TENANT_ID, expiresIn=3600)
    SUBSCRIPTION_ID = fake_settings.SUBSCRIPTION_ID
    RESOURCE_GROUP_NAME = fake_settings.RESOURCE_GROUP_NAME
else:
    STORE_NAME = os.environ['azure_data_lake_store_name']
    TENANT_ID = os.environ.get('azure_tenant_id', 'common')
    TOKEN = auth(TENANT_ID,
                 os.environ['azure_username'],
                 os.environ['azure_password'])
    SUBSCRIPTION_ID = os.environ['azure_subscription_id']
    RESOURCE_GROUP_NAME = os.environ['azure_resource_group_name']
