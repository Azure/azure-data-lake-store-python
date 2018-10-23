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

    def __init__(self, max_retries=None, exponential_retry_interval=None, exponential_factor=None):
        self.exponential_factor = 4 if exponential_factor is None else exponential_factor
        self.max_retries = 4 if max_retries is None else max_retries
        self.exponential_retry_interval = 1 if exponential_retry_interval is None else exponential_retry_interval

    def should_retry(self, response, last_exception, retry_count):
        if retry_count >= self.max_retries:
            return False

        if last_exception is not None:
            self.__backoff()
            return True

        status_code = response.status_code

        if(status_code == 501
            or status_code == 505
            or (300 <= status_code < 500
                and status_code != 401
                and status_code != 408
                and status_code != 429)):
            return False

        if(status_code >= 500
            or status_code == 401
            or status_code == 408
            or status_code == 429
            or status_code == 104):
            self.__backoff()
            return True

        if 100 <= status_code < 300:
            return False

        return False

    def __backoff(self):
        time.sleep(self.exponential_retry_interval)
        self.exponential_retry_interval *= self.exponential_factor


from functools import wraps


def retry_decorator(retry_policy = None):
    import re, adal, requests
    from collections import namedtuple
    if retry_policy is None:
        retry_policy = ExponentialRetryPolicy()

    def deco_retry(func):
        @wraps(func)
        def f_retry(*args, **kwargs):
            retry_count = -1
            last_exception = None
            out = None
            while True:
                retry_count += 1
                try:
                    out = func(*args, **kwargs)
                except (adal.adal_error.AdalError, requests.HTTPError) as e:
                    # ADAL error corresponds to everything but 429, which bubbles up HTTP error.
                    last_exception = e
                    logger.exception("Retry count " + str(retry_count) + "Exception :" + str(last_exception))
                    if hasattr(e, 'error_response'):  # ADAL exception
                        response = e.error_response
                        http_code = re.search("http error: (\d+)", str(e))
                        if http_code is not None:  # Add status_code to response object for use in should_retry
                            Response = namedtuple("Response", list(response.keys()) + ['status_code'])
                            values = list(response.values()) + [int(http_code.group(1))]
                            response = Response(
                                *values)  # Construct response object with adal exception response and http code
                    if hasattr(e, 'response'):  # HTTP exception
                        response = e.response
                request_successful = last_exception is None or response.status_code == 401
                if request_successful or not retry_policy.should_retry(response, last_exception, retry_count):
                    break
            if out is None:
                logger.exception(last_exception)
                raise last_exception
            return out
        return f_retry
    return deco_retry
