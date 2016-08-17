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

from adlfs.cli import AzureDataLakeFSCommand

from tests.testing import default_home, my_vcr, open_azure


@contextmanager
def open_client(fs):
    yield AzureDataLakeFSCommand(fs)


def setup_test_dir(fs):
    d = os.path.join(default_home(), 'foo')
    fs.mkdir(d)
    return d


def setup_test_file(fs):
    tmp = os.path.join(default_home(), 'foo', 'bar')
    with fs.open(tmp, 'wb') as f:
        f.write('123456'.encode())
    return tmp


def read_stdout(captured):
    out, _ = captured.readouterr()
    return out


@my_vcr.use_cassette
def test_cat(capsys):
    with open_azure() as azure, open_client(azure) as command:
        azurefile = setup_test_file(azure)

        command.onecmd('cat ' + azurefile)
        assert read_stdout(capsys) == '123456'


@my_vcr.use_cassette
def test_chmod(capsys):
    with open_azure() as azure, open_client(azure) as command:
        azurefile = setup_test_file(azure)

        command.onecmd('info ' + azurefile)
        assert 'permission       = 770' in read_stdout(capsys)

        command.onecmd('chmod 0550 ' + azurefile)
        assert not read_stdout(capsys)

        command.onecmd('info ' + azurefile)
        assert 'permission       = 550' in read_stdout(capsys)


@my_vcr.use_cassette
def test_df(capsys):
    with open_azure() as azure, open_client(azure) as command:
        command.onecmd('df')
        out = read_stdout(capsys)
        assert len(out.strip().split('\n')) == 6
        assert 'quota' in out


@my_vcr.use_cassette
def test_du(capsys):
    with open_azure() as azure, open_client(azure) as command:
        azurefile = setup_test_file(azure)

        command.onecmd('du ' + azurefile)
        out = read_stdout(capsys)
        assert len(out.strip().split('\n')) == 1


@my_vcr.use_cassette
def test_exists(capsys):
    with open_azure() as azure, open_client(azure) as command:
        azurefile = setup_test_file(azure)

        command.onecmd('exists ' + azurefile)
        assert read_stdout(capsys) == 'True\n'


@my_vcr.use_cassette
def test_get(tmpdir):
    with open_azure() as azure, open_client(azure) as command:
        azurefile = setup_test_file(azure)

        f = os.path.basename(azurefile)
        localfile = tmpdir.dirname + '/' + f

        command.onecmd(' '.join(['get', azurefile, tmpdir.dirname]))

        assert os.path.exists(localfile)

        with open(localfile, 'rb') as lf:
            content = lf.read()
            assert content == b'123456'


@my_vcr.use_cassette
def test_head(capsys):
    with open_azure() as azure, open_client(azure) as command:
        azurefile = setup_test_file(azure)

        command.onecmd('head ' + azurefile)
        assert read_stdout(capsys) == '123456'


@my_vcr.use_cassette
def test_head_bytes(capsys):
    with open_azure() as azure, open_client(azure) as command:
        azurefile = setup_test_file(azure)

        command.onecmd('head -c 3 ' + azurefile)
        assert read_stdout(capsys) == '123'


@my_vcr.use_cassette
def test_info(capsys):
    with open_azure() as azure, open_client(azure) as command:
        azurefile = setup_test_file(azure)

        command.onecmd('info ' + azurefile)
        out = read_stdout(capsys)
        assert len(out.strip().split('\n')) == 11
        assert 'modificationTime' in out


@my_vcr.use_cassette
def test_ls(capsys):
    with open_azure() as azure, open_client(azure) as command:
        azurefile = setup_test_file(azure)

        d = os.path.dirname(azurefile)
        f = os.path.basename(azurefile)

        command.onecmd('ls ' + d)
        assert read_stdout(capsys) == f + '\n'


@my_vcr.use_cassette
def test_ls_detailed(capsys):
    with open_azure() as azure, open_client(azure) as command:
        azurefile = setup_test_file(azure)

        d = os.path.dirname(azurefile)
        f = os.path.basename(azurefile)

        command.onecmd('ls -l ' + d)
        out = read_stdout(capsys)
        assert len(out.strip().split('\n')) == 1
        assert f in out


@my_vcr.use_cassette
def test_mkdir_and_rmdir(capsys):
    with open_azure() as azure, open_client(azure) as command:
        azuredir = setup_test_dir(azure)

        d = azuredir + '/foo'

        command.onecmd('mkdir ' + d)
        assert not read_stdout(capsys)

        command.onecmd('info ' + d)
        assert 'DIRECTORY' in read_stdout(capsys)

        command.onecmd('rmdir ' + d)
        assert not read_stdout(capsys)

        command.onecmd('exists ' + d)
        assert read_stdout(capsys) == 'False\n'


@my_vcr.use_cassette
def test_mv(capsys):
    with open_azure() as azure, open_client(azure) as command:
        azurefile = setup_test_file(azure)

        f = os.path.dirname(azurefile) + '/foo'

        command.onecmd(' '.join(['mv', azurefile, f]))
        assert not read_stdout(capsys)

        command.onecmd('exists ' + azurefile)
        assert read_stdout(capsys) == 'False\n'

        command.onecmd(' '.join(['mv', f, azurefile]))
        assert not read_stdout(capsys)

        command.onecmd('exists ' + azurefile)
        assert read_stdout(capsys) == 'True\n'


@my_vcr.use_cassette
def test_put(capsys, tmpdir):
    with open_azure() as azure, open_client(azure) as command:
        azuredir = setup_test_dir(azure)
        localfile = tmpdir.dirname + '/foo'

        with open(localfile, 'wb') as lf:
            lf.write(b'123456')

        command.onecmd(' '.join(['put', localfile, azuredir]))

        command.onecmd('head ' + azuredir + '/foo')
        assert read_stdout(capsys) == '123456'

        command.onecmd('rm ' + azuredir + '/foo')
        assert not read_stdout(capsys)


@my_vcr.use_cassette
def test_tail(capsys):
    with open_azure() as azure, open_client(azure) as command:
        azurefile = setup_test_file(azure)

        command.onecmd('tail ' + azurefile)
        assert read_stdout(capsys) == '123456'


@my_vcr.use_cassette
def test_tail_bytes(capsys):
    with open_azure() as azure, open_client(azure) as command:
        azurefile = setup_test_file(azure)

        command.onecmd('tail -c 3 ' + azurefile)
        assert read_stdout(capsys) == '456'


@my_vcr.use_cassette
def test_touch_and_rm(capsys):
    with open_azure() as azure, open_client(azure) as command:
        azuredir = setup_test_dir(azure)
        f = azuredir + '/foo'

        command.onecmd('touch ' + f)
        assert not read_stdout(capsys)

        command.onecmd('exists ' + f)
        assert read_stdout(capsys) == 'True\n'

        command.onecmd('rm ' + f)
        assert not read_stdout(capsys)

        command.onecmd('exists ' + f)
        assert read_stdout(capsys) == 'False\n'
