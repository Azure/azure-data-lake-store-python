# -*- coding: utf-8 -*-
# coding=utf-8
# --------------------------------------------------------------------------
# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See License.txt in the project root for
# license information.
# --------------------------------------------------------------------------

import os
from azure.identity import DefaultAzureCredential
from tests import fake_settings
STORE_NAME =  os.environ['azure_data_lake_store_name']
TENANT_ID = fake_settings.TENANT_ID
SUBSCRIPTION_ID = fake_settings.SUBSCRIPTION_ID
RESOURCE_GROUP_NAME = fake_settings.RESOURCE_GROUP_NAME
RECORD_MODE = os.environ.get('RECORD_MODE', 'all').lower()
AZURE_ACL_TEST_APPID = os.environ.get('AZURE_ACL_TEST_APPID')
CLIENT_ID = os.environ['azure_service_principal']

if RECORD_MODE == 'none':
    STORE_NAME = fake_settings.STORE_NAME
    TENANT_ID = fake_settings.TENANT_ID
    TOKEN_CREDEDENTIAL = DefaultAzureCredential(exclude_shared_token_cache_credential=True)
    SUBSCRIPTION_ID = fake_settings.SUBSCRIPTION_ID
    RESOURCE_GROUP_NAME = fake_settings.RESOURCE_GROUP_NAME
else:
    STORE_NAME = os.environ['azure_data_lake_store_name']
    TENANT_ID = os.environ.get('azure_tenant_id', 'common')
    TOKEN_CREDEDENTIAL = DefaultAzureCredential(exclude_shared_token_cache_credential=True)
    # set the environment variable to empty to avoid confusion in auth
    SUBSCRIPTION_ID = os.environ['azure_subscription_id']
    RESOURCE_GROUP_NAME = os.environ['azure_resource_group_name']
