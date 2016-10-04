# -*- coding: utf-8 -*-
# coding=utf-8
# --------------------------------------------------------------------------
# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See License.txt in the project root for
# license information.
# --------------------------------------------------------------------------

import sys

import pytest

from azure.datalake.store.utils import WIN


@pytest.mark.skipif(sys.platform == 'win32', reason="requires non-windows")
def test_non_windows_platform():
    assert not WIN


@pytest.mark.skipif(sys.platform != 'win32', reason="requires windows")
def test_windows_platform():
    assert WIN
