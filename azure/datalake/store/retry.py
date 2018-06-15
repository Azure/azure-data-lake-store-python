# -*- coding: utf-8 -*-
# coding=utf-8
# --------------------------------------------------------------------------
# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See License.txt in the project root for
# license information.
# --------------------------------------------------------------------------

"""
Provides implementation of different Retry Policies
"""

# standard imports
import logging
import sys
import time

# local imports

logger = logging.getLogger(__name__)


class RetryPolicy:
    def should_retry(self):
        pass


class NoRetryPolicy(RetryPolicy):
    def should_retry(self):
        return False


class ExponentialRetryPolicy(RetryPolicy):

    # max_retries = 4
    # # Initial retry interval in seconds
    # exponential_retry_interval = 1
    # exponential_factor = 4

    def __init__(self, max_retries=4, exponential_retry_interval=1, exponential_factor=4):
        self.exponential_factor = exponential_factor
        self.max_retries = max_retries
        self.exponential_retry_interval = exponential_retry_interval

    def should_retry(self, status_code, last_exception, retry_count):
        if(status_code == 501
            or status_code == 505
            or (300 <= status_code < 500
                and status_code != 408
                and status_code != 429
                and status_code != 401)):
            return False

        if(last_exception is not None
            or status_code >= 500
            or status_code == 404
            or status_code == 408
            or status_code == 429
            or status_code == 401):
            if retry_count >= self.max_retries:
                return False
            time.sleep(self.exponential_retry_interval)
            self.exponential_retry_interval *= self.exponential_factor
            return True

        if 100 <= status_code < 300:
            return False

        return False
