# -*- coding: utf-8 -*-
# coding=utf-8
# --------------------------------------------------------------------------
# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See License.txt in the project root for
# license information.
# --------------------------------------------------------------------------

import os
import time

from azure.datalake.store.core import AzureDLPath
from azure.datalake.store.transfer import ADLTransferClient
from tests.testing import azure, posix


def test_shutdown(azure):
    def transfer(adlfs, src, dst, offset, size, blocksize, buffersize, retries=5, shutdown_event=None):
        time.sleep(1.0)
        while shutdown_event and not shutdown_event.is_set():
            time.sleep(0.1)
        return size, None

    client = ADLTransferClient(azure, transfer=transfer, chunksize=1,
                               chunked=False)
    client.submit('foo', 'bar', 16)
    client.run(monitor=False)
    time.sleep(0.1)
    client.shutdown()

    assert client.progress[0].state == 'finished'


def test_submit_and_run(azure):
    def transfer(adlfs, src, dst, offset, size, blocksize, buffersize, shutdown_event=None):
        time.sleep(0.1)
        return size, None

    client = ADLTransferClient(azure, transfer=transfer, chunksize=8,
                               chunked=False)

    client.submit('foo', 'bar', 16)
    client.submit('abc', '123', 8)

    nfiles = len(client.progress)
    assert nfiles == 2
    assert len([client.progress[i].chunks for i in range(nfiles)])

    assert all([client.progress[i].state == 'pending' for i in range(nfiles)])
    assert all([chunk.state == 'pending' for f in client.progress
                                         for chunk in f.chunks])

    expected = {('bar', 0), ('bar', 8), ('123', 0)}
    assert {(chunk.name, chunk.offset) for f in client.progress
                                       for chunk in f.chunks} == expected

    client.run()

    assert all([client.progress[i].state == 'finished' for i in range(nfiles)])
    assert all([chunk.state == 'finished' for f in client.progress
                                          for chunk in f.chunks])
    assert all([chunk.expected == chunk.actual for f in client.progress
                                               for chunk in f.chunks])


def test_update_progress(azure):
    """
    Upload a 32 bytes file in chunks of 8 and test that the progress is incrementally
    updated.
    """
    calls = []

    def recording_callback(progress, total):
        calls.append((progress, total))

    def transfer(adlfs, src, dst, offset, size, blocksize, buffersize, shutdown_event=None):
        time.sleep(0.01)
        return size, None

    client = ADLTransferClient(azure, transfer=transfer, chunksize=8,
                               chunked=True, progress_callback=recording_callback)

    client.submit('foo', AzureDLPath('bar'), 32)
    client.run()

    assert calls == [(8, 32), (16, 32), (24, 32), (32, 32)]


def test_temporary_path(azure):
    def transfer(adlfs, src, dst, offset, size, blocksize, buffersize):
        time.sleep(0.1)
        return size, None

    client = ADLTransferClient(azure, transfer=transfer, chunksize=8,
                               unique_temporary=False)
    client.submit('foo', AzureDLPath('bar'), 16)

    assert os.path.dirname(posix(client.progress[0].chunks[0].name)) == 'bar.segments'
