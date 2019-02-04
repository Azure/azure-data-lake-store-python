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

from samples.cli import AzureDataLakeFSCommand
from azure.datalake.store.exceptions import PermissionError
from tests.testing import azure, my_vcr, working_dir


@pytest.yield_fixture()
def client(azure):
    yield AzureDataLakeFSCommand(azure)


@contextmanager
def setup_dir(azure):
    d = str(working_dir() / 'foo')
    azure.mkdir(d)
    try:
        yield d
    finally:
        azure.rm(d, recursive=True)


@contextmanager
def setup_file(azure):
    tmp = str(working_dir() / 'foo' / 'bar')
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
def test_cat(capsys, azure, client):
    with setup_file(azure) as azurefile:
        client.onecmd('cat ' + azurefile)
        assert read_stdout(capsys) == '123456'


@my_vcr.use_cassette
def test_chgrp(capsys, azure, client):
    pass

@my_vcr.use_cassette
def test_chmod(capsys, azure, client):
    with setup_file(azure) as azurefile:
        client.onecmd('info ' + azurefile)
        assert 'permission       = 770' in read_stdout(capsys)

        client.onecmd('chmod 0550 ' + azurefile)
        assert not read_stdout(capsys)

        client.onecmd('info ' + azurefile)
        assert 'permission       = 550' in read_stdout(capsys)


@my_vcr.use_cassette
def test_chown(capsys, azure, client):
    pass 

@my_vcr.use_cassette
def test_df(capsys, azure, client):
    with setup_file(azure) as _:
        client.onecmd('df')
        out = read_stdout(capsys)
        assert len(out.strip().split('\n')) == 6
        assert 'quota' in out


@my_vcr.use_cassette
def test_du(capsys, azure, client):
    with setup_file(azure) as azurefile:
        client.onecmd('du ' + azurefile)
        out = read_stdout(capsys)
        assert len(out.strip().split('\n')) == 1


@my_vcr.use_cassette
def test_exists(capsys, azure, client):
    with setup_file(azure) as azurefile:
        client.onecmd('exists ' + azurefile)
        assert read_stdout(capsys) == 'True\n'


@my_vcr.use_cassette
def test_get(tmpdir, azure, client):
    with setup_file(azure) as azurefile:
        f = os.path.basename(azurefile)
        localfile = tmpdir.dirname + '/' + f

        client.onecmd(' '.join(['get', '-f', azurefile, tmpdir.dirname]))

        assert os.path.exists(localfile)

        with open(localfile, 'rb') as lf:
            content = lf.read()
            assert content == b'123456'


@my_vcr.use_cassette
def test_head(capsys, azure, client):
    with setup_file(azure) as azurefile:
        client.onecmd('head ' + azurefile)
        assert read_stdout(capsys) == '123456'


@my_vcr.use_cassette
def test_head_bytes(capsys, azure, client):
    with setup_file(azure) as azurefile:
        client.onecmd('head -c 3 ' + azurefile)
        assert read_stdout(capsys) == '123'


@my_vcr.use_cassette
def test_info(capsys, azure, client):
    with setup_file(azure) as azurefile:
        client.onecmd('info ' + azurefile)
        out = read_stdout(capsys)
        assert len(out.strip().split('\n')) == 13
        assert 'modificationTime' in out


@my_vcr.use_cassette
def test_ls(capsys, azure, client):
    with setup_file(azure) as azurefile:
        d = os.path.dirname(azurefile)
        f = os.path.basename(azurefile)

        client.onecmd('ls ' + d)
        assert read_stdout(capsys) == f + '\n'


@my_vcr.use_cassette
def test_ls_detailed(capsys, azure, client):
    with setup_file(azure) as azurefile:
        d = os.path.dirname(azurefile)
        f = os.path.basename(azurefile)

        client.onecmd('ls -l ' + d)
        out = read_stdout(capsys)
        assert len(out.strip().split('\n')) == 1
        assert f in out


@my_vcr.use_cassette
def test_mkdir_and_rmdir(capsys, azure, client):
    with setup_dir(azure) as azuredir:
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
def test_mv(capsys, azure, client):
    with setup_file(azure) as azurefile:
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
def test_put(capsys, tmpdir, azure, client):
    localfile = tmpdir.dirname + '/foo'

    with open(localfile, 'wb') as lf:
        lf.write(b'123456')

    with setup_dir(azure) as azuredir:
        client.onecmd(' '.join(['put', '-f', localfile, azuredir + '/foo']))

        client.onecmd('head ' + azuredir + '/foo')
        assert read_stdout(capsys).endswith('123456')

        client.onecmd('rm ' + azuredir + '/foo')
        assert not read_stdout(capsys)


@my_vcr.use_cassette
def test_tail(capsys, azure, client):
    with setup_file(azure) as azurefile:
        client.onecmd('tail ' + azurefile)
        assert read_stdout(capsys) == '123456'


@my_vcr.use_cassette
def test_tail_bytes(capsys, azure, client):
    with setup_file(azure) as azurefile:
        client.onecmd('tail -c 3 ' + azurefile)
        assert read_stdout(capsys) == '456'


@my_vcr.use_cassette
def test_touch_and_rm(capsys, azure, client):
    with setup_dir(azure) as azuredir:
        f = azuredir + '/foo'

        client.onecmd('touch ' + f)
        assert not read_stdout(capsys)

        client.onecmd('exists ' + f)
        assert read_stdout(capsys) == 'True\n'

        client.onecmd('rm ' + f)
        assert not read_stdout(capsys)

        client.onecmd('exists ' + f)
        assert read_stdout(capsys) == 'False\n'
