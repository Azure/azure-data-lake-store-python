# -*- coding: utf-8 -*-
# coding=utf-8
# --------------------------------------------------------------------------
# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See License.txt in the project root for
# license information.
# --------------------------------------------------------------------------

import time

from tests.testing import azure
from adlfs.transfer import ADLTransferClient


def test_interrupt(azure):
    def transfer(adlfs, src, dst, offset, size, retries=5, shutdown_event=None):
        while shutdown_event and not shutdown_event.is_set():
            time.sleep(0.1)

    client = ADLTransferClient(azure, 'foobar', transfer=transfer, chunksize=1,
                               tmp_path=None)
    client.submit('foo', 'bar', 16)
    client.run(monitor=False)
    time.sleep(1)
    client.shutdown()
    client.monitor()

    assert client.progress[0].state != 'finished'
