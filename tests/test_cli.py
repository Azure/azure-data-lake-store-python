# -*- coding: utf-8 -*-
# coding=utf-8
# --------------------------------------------------------------------------
# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See License.txt in the project root for
# license information.
# --------------------------------------------------------------------------

from __future__ import unicode_literals

from contextlib import contextmanager
import os

import pytest

from adlfs.cli import AzureDataLakeFSCommand
from tests.testing import azure, default_home, my_vcr


@pytest.yield_fixture()
def client(azure):
    yield AzureDataLakeFSCommand(azure)


@pytest.yield_fixture()
def azuredir(azure):
    d = os.path.join(default_home(), 'foo')
    azure.mkdir(d)
    try:
        yield d
    finally:
        azure.rm(d, recursive=True)


@pytest.yield_fixture()
def azurefile(azure):
    tmp = os.path.join(default_home(), 'foo', 'bar')
    with azure.open(tmp, 'wb') as f:
        f.write('123456'.encode())
    try:
        yield tmp
    finally:
        azure.rm(tmp)


def read_stdout(captured):
    out, _ = captured.readouterr()
    return out


@my_vcr.use_cassette
def test_cat(capsys, azure, client, azurefile):
    client.onecmd('cat ' + azurefile)
    assert read_stdout(capsys) == '123456'


@my_vcr.use_cassette
def test_chmod(capsys, azure, client, azurefile):
    client.onecmd('info ' + azurefile)
    assert 'permission       = 770' in read_stdout(capsys)

    client.onecmd('chmod 0550 ' + azurefile)
    assert not read_stdout(capsys)

    client.onecmd('info ' + azurefile)
    assert 'permission       = 550' in read_stdout(capsys)


@my_vcr.use_cassette
def test_df(capsys, azure, client):
    client.onecmd('df')
    out = read_stdout(capsys)
    assert len(out.strip().split('\n')) == 6
    assert 'quota' in out


@my_vcr.use_cassette
def test_du(capsys, azure, client, azurefile):
    client.onecmd('du ' + azurefile)
    out = read_stdout(capsys)
    assert len(out.strip().split('\n')) == 1


@my_vcr.use_cassette
def test_exists(capsys, azure, client, azurefile):
    client.onecmd('exists ' + azurefile)
    assert read_stdout(capsys) == 'True\n'


@my_vcr.use_cassette
def test_get(tmpdir, azure, client, azurefile):
    f = os.path.basename(azurefile)
    localfile = tmpdir.dirname + '/' + f

    client.onecmd(' '.join(['get', azurefile, tmpdir.dirname]))

    assert os.path.exists(localfile)

    with open(localfile, 'rb') as lf:
        content = lf.read()
        assert content == b'123456'


@my_vcr.use_cassette
def test_head(capsys, azure, client, azurefile):
    client.onecmd('head ' + azurefile)
    assert read_stdout(capsys) == '123456'


@my_vcr.use_cassette
def test_head_bytes(capsys, azure, client, azurefile):
    client.onecmd('head -c 3 ' + azurefile)
    assert read_stdout(capsys) == '123'


@my_vcr.use_cassette
def test_info(capsys, azure, client, azurefile):
    client.onecmd('info ' + azurefile)
    out = read_stdout(capsys)
    assert len(out.strip().split('\n')) == 11
    assert 'modificationTime' in out


@my_vcr.use_cassette
def test_ls(capsys, azure, client, azurefile):
    d = os.path.dirname(azurefile)
    f = os.path.basename(azurefile)

    client.onecmd('ls ' + d)
    assert read_stdout(capsys) == f + '\n'


@my_vcr.use_cassette
def test_ls_detailed(capsys, azure, client, azurefile):
    d = os.path.dirname(azurefile)
    f = os.path.basename(azurefile)

    client.onecmd('ls -l ' + d)
    out = read_stdout(capsys)
    assert len(out.strip().split('\n')) == 1
    assert f in out


@my_vcr.use_cassette
def test_mkdir_and_rmdir(capsys, azure, client, azuredir):
    d = azuredir + '/foo'

    client.onecmd('mkdir ' + d)
    assert not read_stdout(capsys)

    client.onecmd('info ' + d)
    assert 'DIRECTORY' in read_stdout(capsys)

    client.onecmd('rmdir ' + d)
    assert not read_stdout(capsys)

    client.onecmd('exists ' + d)
    assert read_stdout(capsys) == 'False\n'


@my_vcr.use_cassette
def test_mv(capsys, azure, client, azurefile):
    f = os.path.dirname(azurefile) + '/foo'

    client.onecmd(' '.join(['mv', azurefile, f]))
    assert not read_stdout(capsys)

    client.onecmd('exists ' + azurefile)
    assert read_stdout(capsys) == 'False\n'

    client.onecmd(' '.join(['mv', f, azurefile]))
    assert not read_stdout(capsys)

    client.onecmd('exists ' + azurefile)
    assert read_stdout(capsys) == 'True\n'


@my_vcr.use_cassette
def test_put(capsys, tmpdir, azure, client, azuredir):
    localfile = tmpdir.dirname + '/foo'

    with open(localfile, 'wb') as lf:
        lf.write(b'123456')

    client.onecmd(' '.join(['put', localfile, azuredir]))

    client.onecmd('head ' + azuredir + '/foo')
    assert read_stdout(capsys) == '123456'

    client.onecmd('rm ' + azuredir + '/foo')
    assert not read_stdout(capsys)


@my_vcr.use_cassette
def test_tail(capsys, azure, client, azurefile):
    client.onecmd('tail ' + azurefile)
    assert read_stdout(capsys) == '123456'


@my_vcr.use_cassette
def test_tail_bytes(capsys, azure, client, azurefile):
    client.onecmd('tail -c 3 ' + azurefile)
    assert read_stdout(capsys) == '456'


@my_vcr.use_cassette
def test_touch_and_rm(capsys, azure, client, azuredir):
    f = azuredir + '/foo'

    client.onecmd('touch ' + f)
    assert not read_stdout(capsys)

    client.onecmd('exists ' + f)
    assert read_stdout(capsys) == 'True\n'

    client.onecmd('rm ' + f)
    assert not read_stdout(capsys)

    client.onecmd('exists ' + f)
    assert read_stdout(capsys) == 'False\n'
