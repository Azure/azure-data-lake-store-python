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

from tests.testing import my_vcr, open_azure


@contextmanager
def open_client(fs):
    yield AzureDataLakeFSCommand(fs)


def setup_test_dir(fs):
    d = 'azure_test_dir/foo'
    fs.mkdir(d)
    return d


def setup_test_file(fs):
    tmp = 'azure_test_dir/foo/bar'
    with fs.open(tmp, 'wb') as f:
        f.write(b'123456')
    return tmp


@my_vcr.use_cassette
def test_cat(capsys):
    with open_azure() as azure, open_client(azure) as command:
        azurefile = setup_test_file(azure)

        command.onecmd('cat ' + azurefile)
        out, _ = capsys.readouterr()
        assert out == '123456\n'


@my_vcr.use_cassette
def test_chmod(capsys):
    with open_azure() as azure, open_client(azure) as command:
        azurefile = setup_test_file(azure)

        command.onecmd('info ' + azurefile)
        out, _ = capsys.readouterr()
        assert 'permission       = 770' in out

        command.onecmd('chmod 0550 ' + azurefile)
        out, _ = capsys.readouterr()
        assert not out

        command.onecmd('info ' + azurefile)
        out, _ = capsys.readouterr()
        assert 'permission       = 550' in out


@my_vcr.use_cassette
def test_df(capsys):
    with open_azure() as azure, open_client(azure) as command:
        command.onecmd('df')
        out, _ = capsys.readouterr()
        assert len(out.strip().split('\n')) == 6
        assert 'quota' in out


@my_vcr.use_cassette
def test_du(capsys):
    with open_azure() as azure, open_client(azure) as command:
        azurefile = setup_test_file(azure)

        command.onecmd('du ' + azurefile)
        out, _ = capsys.readouterr()
        assert len(out.strip().split('\n')) == 1


@my_vcr.use_cassette
def test_exists(capsys):
    with open_azure() as azure, open_client(azure) as command:
        azurefile = setup_test_file(azure)

        command.onecmd('exists ' + azurefile)
        out, _ = capsys.readouterr()
        assert out == 'True\n'


@my_vcr.use_cassette
def test_get(tmpdir):
    with open_azure() as azure, open_client(azure) as command:
        azurefile = setup_test_file(azure)

        f = os.path.basename(azurefile)
        localfile = tmpdir.dirname + '/' + f

        command.onecmd(' '.join(['get', azurefile, tmpdir.dirname]))

        assert os.path.exists(localfile)

        with open(localfile, 'r') as lf:
            content = lf.read()
            assert content == '123456'


@my_vcr.use_cassette
def test_head(capsys):
    with open_azure() as azure, open_client(azure) as command:
        azurefile = setup_test_file(azure)

        command.onecmd('head ' + azurefile)
        out, _ = capsys.readouterr()
        assert out == '123456\n'


@my_vcr.use_cassette
def test_head_bytes(capsys):
    with open_azure() as azure, open_client(azure) as command:
        azurefile = setup_test_file(azure)

        command.onecmd('head -c 3 ' + azurefile)
        out, _ = capsys.readouterr()
        assert out == '123\n'


@my_vcr.use_cassette
def test_info(capsys):
    with open_azure() as azure, open_client(azure) as command:
        azurefile = setup_test_file(azure)

        command.onecmd('info ' + azurefile)
        out, _ = capsys.readouterr()
        assert len(out.strip().split('\n')) == 11
        assert 'modificationTime' in out


@my_vcr.use_cassette
def test_ls(capsys):
    with open_azure() as azure, open_client(azure) as command:
        azurefile = setup_test_file(azure)

        d = os.path.dirname(azurefile)
        f = os.path.basename(azurefile)

        command.onecmd('ls ' + d)
        out, _ = capsys.readouterr()
        assert out == f + '\n'


@my_vcr.use_cassette
def test_ls_detailed(capsys):
    with open_azure() as azure, open_client(azure) as command:
        azurefile = setup_test_file(azure)

        d = os.path.dirname(azurefile)
        f = os.path.basename(azurefile)

        command.onecmd('ls -l ' + d)
        out, _ = capsys.readouterr()
        assert len(out.strip().split('\n')) == 1
        assert f in out


@my_vcr.use_cassette
def test_mkdir_and_rmdir(capsys):
    with open_azure() as azure, open_client(azure) as command:
        azuredir = setup_test_dir(azure)

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


@my_vcr.use_cassette
def test_mv(capsys):
    with open_azure() as azure, open_client(azure) as command:
        azurefile = setup_test_file(azure)

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


@my_vcr.use_cassette
def test_put(capsys, tmpdir):
    with open_azure() as azure, open_client(azure) as command:
        azuredir = setup_test_dir(azure)
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


@my_vcr.use_cassette
def test_tail(capsys):
    with open_azure() as azure, open_client(azure) as command:
        azurefile = setup_test_file(azure)

        command.onecmd('tail ' + azurefile)
        out, _ = capsys.readouterr()
        assert out == '123456\n'


@my_vcr.use_cassette
def test_tail_bytes(capsys):
    with open_azure() as azure, open_client(azure) as command:
        azurefile = setup_test_file(azure)

        command.onecmd('tail -c 3 ' + azurefile)
        out, _ = capsys.readouterr()
        assert out == '456\n'


@my_vcr.use_cassette
def test_touch_and_rm(capsys):
    with open_azure() as azure, open_client(azure) as command:
        azuredir = setup_test_dir(azure)
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
