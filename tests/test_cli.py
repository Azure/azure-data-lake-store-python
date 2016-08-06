# -*- coding: utf-8 -*-
# coding=utf-8
# --------------------------------------------------------------------------
# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See License.txt in the project root for
# license information.
# --------------------------------------------------------------------------

from __future__ import unicode_literals

import os
import uuid

import pytest

from adlfs.cli import AzureDataLakeFSCommand
from adlfs.utils import azure


@pytest.yield_fixture
def azuredir(azure):
    d = 'azure_test_dir/' + uuid.uuid4().hex[:8]
    azure.mkdir(d)
    try:
        yield d
    finally:
        azure.rmdir(d)


@pytest.yield_fixture
def azurefile(azure, azuredir):
    tmpfile = azuredir + '/' + uuid.uuid4().hex[:8]
    with azure.open(tmpfile, 'wb') as f:
        f.write(b'123456')
    try:
        yield tmpfile
    finally:
        azure.rm(tmpfile)


@pytest.yield_fixture
def command(azure):
    yield AzureDataLakeFSCommand()


def test_cat(capsys, command, azurefile):
    command.onecmd('cat ' + azurefile)
    out, _ = capsys.readouterr()
    assert out == '123456\n'


def test_chmod(capsys, command, azurefile):
    command.onecmd('info ' + azurefile)
    out, _ = capsys.readouterr()
    assert 'permission       = 770' in out

    command.onecmd('chmod 0550 ' + azurefile)
    out, _ = capsys.readouterr()
    assert not out

    command.onecmd('info ' + azurefile)
    out, _ = capsys.readouterr()
    assert 'permission       = 550' in out


def test_df(capsys, command):
    command.onecmd('df')
    out, _ = capsys.readouterr()
    assert len(out.strip().split('\n')) == 6
    assert 'quota' in out


def test_du(capsys, command, azurefile):
    command.onecmd('du ' + azurefile)
    out, _ = capsys.readouterr()
    assert len(out.strip().split('\n')) == 1


def test_exists(capsys, command, azurefile):
    command.onecmd('exists ' + azurefile)
    out, _ = capsys.readouterr()
    assert out == 'True\n'


def test_get(command, azurefile, tmpdir):
    f = os.path.basename(azurefile)
    localfile = tmpdir.dirname + '/' + f

    command.onecmd(' '.join(['get', azurefile, tmpdir.dirname]))

    assert os.path.exists(localfile)

    with open(localfile, 'r') as lf:
        content = lf.read()
        assert content == '123456'


def test_head(capsys, command, azurefile):
    command.onecmd('head ' + azurefile)
    out, _ = capsys.readouterr()
    assert out == '123456\n'


def test_head_bytes(capsys, command, azurefile):
    command.onecmd('head -c 3 ' + azurefile)
    out, _ = capsys.readouterr()
    assert out == '123\n'


def test_info(capsys, command, azurefile):
    command.onecmd('info ' + azurefile)
    out, _ = capsys.readouterr()
    assert len(out.strip().split('\n')) == 11
    assert 'modificationTime' in out


def test_ls(capsys, command, azurefile):
    d = os.path.dirname(azurefile)
    f = os.path.basename(azurefile)

    command.onecmd('ls ' + d)
    out, _ = capsys.readouterr()
    assert out == f + '\n'


def test_ls_detailed(capsys, command, azurefile):
    d = os.path.dirname(azurefile)
    f = os.path.basename(azurefile)

    command.onecmd('ls -l ' + d)
    out, _ = capsys.readouterr()
    assert len(out.strip().split('\n')) == 1
    assert f in out


def test_mkdir_and_rmdir(capsys, command, azuredir):
    d = azuredir + '/foo'

    command.onecmd('mkdir ' + d)
    out, _ = capsys.readouterr()
    assert not out

    command.onecmd('info ' + d)
    out, _ = capsys.readouterr()
    assert 'DIRECTORY' in out

    command.onecmd('rmdir ' + d)
    out, _ = capsys.readouterr()
    assert not out

    command.onecmd('exists ' + d)
    out, _ = capsys.readouterr()
    assert out == 'False\n'


def test_mv(capsys, command, azurefile):
    f = os.path.dirname(azurefile) + '/foo'

    command.onecmd(' '.join(['mv', azurefile, f]))
    out, _ = capsys.readouterr()
    assert not out

    command.onecmd('exists ' + azurefile)
    out, _ = capsys.readouterr()
    assert out == 'False\n'

    command.onecmd(' '.join(['mv', f, azurefile]))
    out, _ = capsys.readouterr()
    assert not out

    command.onecmd('exists ' + azurefile)
    out, _ = capsys.readouterr()
    assert out == 'True\n'


def test_put(capsys, command, azuredir, tmpdir):
    localfile = tmpdir.dirname + '/foo'

    with open(localfile, 'wb') as lf:
        lf.write(b'123456')

    command.onecmd(' '.join(['put', localfile, azuredir]))

    command.onecmd('head ' + azuredir + '/foo')
    out, _ = capsys.readouterr()
    assert out == '123456\n'

    command.onecmd('rm ' + azuredir + '/foo')
    out, _ = capsys.readouterr()
    assert not out


def test_tail(capsys, command, azurefile):
    command.onecmd('tail ' + azurefile)
    out, _ = capsys.readouterr()
    assert out == '123456\n'


def test_tail_bytes(capsys, command, azurefile):
    command.onecmd('tail -c 3 ' + azurefile)
    out, _ = capsys.readouterr()
    assert out == '456\n'


def test_touch_and_rm(capsys, command, azuredir):
    f = azuredir + '/foo'

    command.onecmd('touch ' + f)
    out, _ = capsys.readouterr()
    assert not out

    command.onecmd('exists ' + f)
    out, _ = capsys.readouterr()
    assert out == 'True\n'

    command.onecmd('rm ' + f)
    out, _ = capsys.readouterr()
    assert not out

    command.onecmd('exists ' + f)
    out, _ = capsys.readouterr()
    assert out == 'False\n'
