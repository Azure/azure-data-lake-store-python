# -*- coding: utf-8 -*-
# coding=utf-8
# --------------------------------------------------------------------------
# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See License.txt in the project root for
# license information.
# --------------------------------------------------------------------------

import io
import sys

import pytest
import datetime
from azure.datalake.store import utils
from azure.datalake.store.exceptions import PermissionError, FileNotFoundError
from tests.testing import azure, second_azure, azure_teardown, my_vcr, posix, tmpfile, working_dir, create_files
from tests.settings import AZURE_ACL_TEST_APPID

test_dir = working_dir()

a = posix(test_dir / 'a')
b = posix(test_dir / 'b')
c = posix(test_dir / 'c')
d = posix(test_dir / 'd')
specialCharFile = posix(test_dir / '12,+3#456?.txt')


@my_vcr.use_cassette
def test_simple(azure):
    with azure_teardown(azure):
        data = b'a' * (2**16)

        with azure.open(a, 'wb') as f:
            assert f.blocksize == 4*2**20
            l = f.write(data)
            assert l == len(data)

        with azure.open(a, 'rb') as f:
            out = f.read(len(data))
            assert len(data) == len(out)
            assert out == data


@my_vcr.use_cassette
def test_idempotent_connect(azure):
    azure.connect()
    azure.connect()


@my_vcr.use_cassette
def test_ls_touch(azure):
    with azure_teardown(azure):
        assert not azure.ls(test_dir, invalidate_cache=False)
        azure.touch(a)
        azure.touch(b)
        L = azure.ls(test_dir, True, invalidate_cache=False)
        assert set(d['name'] for d in L) == set([a, b])
        L = azure.ls(test_dir, False, invalidate_cache=False)
        assert set(L) == set([a, b])

@my_vcr.use_cassette
def test_ls_empty_with_details(azure):
    with azure_teardown(azure):
        assert not azure.ls(test_dir, invalidate_cache=False, detail=True)

@my_vcr.use_cassette
def test_ls_touch_invalidate_cache(azure, second_azure):
    with azure_teardown(azure):
        assert not azure.ls(test_dir, invalidate_cache=False)
        assert not second_azure.ls(test_dir, invalidate_cache=False)
        azure.touch(a)
        azure.touch(b)
        L = azure.ls(test_dir, True, invalidate_cache=False)
        assert not second_azure.ls(test_dir, invalidate_cache=False)
        L_second = second_azure.ls(test_dir, True, invalidate_cache=True)
        assert set(d['name'] for d in L) == set([a, b])
        assert L == L_second

@my_vcr.use_cassette
def test_ls_batched(azure):

    test_dir = working_dir() / 'abc'
    azure.mkdir(test_dir)
    with azure_teardown(azure):
        test_size = 10
        assert azure._ls(test_dir, batch_size=10) == []
        create_files(azure, number_of_files = 10, prefix='123', root_path=test_dir)
        with pytest.raises(ValueError):
            assert len(azure._ls(test_dir, batch_size=1)) == test_size

        assert len(azure._ls(test_dir, batch_size=9)) == test_size
        assert len(azure._ls(test_dir, batch_size=10)) == test_size
        assert len(azure._ls(test_dir, batch_size=11)) == test_size
        assert len(azure._ls(test_dir, batch_size=2)) == test_size
        assert len(azure._ls(test_dir, batch_size=100)) == test_size
        assert len(azure._ls(test_dir)) == test_size


@my_vcr.use_cassette
def test_rm(azure):
    with azure_teardown(azure):
        assert not azure.exists(a, invalidate_cache=False)
        azure.touch(a)
        assert azure.exists(a, invalidate_cache=False)
        azure.rm(a)
        assert not azure.exists(a, invalidate_cache=False)


@my_vcr.use_cassette
def test_pickle(azure):
    import pickle
    azure2 = pickle.loads(pickle.dumps(azure))

    assert azure2.token.signed_session().headers == azure.token.signed_session().headers


@my_vcr.use_cassette
def test_seek(azure):
    with azure_teardown(azure):
        with azure.open(a, 'wb') as f:
            f.write(b'123')

        with azure.open(a) as f:
            # assert False
            with pytest.raises(ValueError):
                f.seek(1000)
            with pytest.raises(ValueError):
                f.seek(-1)
            with pytest.raises(ValueError):
                f.seek(-5, 2)
            with pytest.raises(ValueError):
                f.seek(0, 10)
            f.seek(0)
            assert f.read(1) == b'1'
            f.seek(0)
            assert f.read(1) == b'1'
            f.seek(3)
            assert f.read(1) == b''
            f.seek(-1, 2)
            assert f.read(1) == b'3'
            f.seek(-1, 1)
            f.seek(-1, 1)
            assert f.read(1) == b'2'
            for i in range(4):
                assert f.seek(i) == i


@my_vcr.use_cassette
def test_concat(azure):
    aplus = a + "+file1"
    bplus = b + "+file2"
    cplus = c + "+res"
    with azure.open(aplus, 'wb') as f:
        f.write(b'hello ')
    with azure.open(bplus, 'wb') as f:
        f.write(b'world')
    try:
        azure.rm(cplus)
    except:
        pass

    azure.concat(cplus, [aplus, bplus])
    out = azure.cat(cplus)
    azure.rm(cplus)

    assert out == b'hello world'


@my_vcr.use_cassette
def test_bad_open(azure):
    with pytest.raises(IOError):
        azure.open('')


@my_vcr.use_cassette
def test_errors(azure):
    with pytest.raises((IOError, OSError)):
        azure.open(test_dir / 'shfoshf', 'rb')

    # This is totally OK: directory is silently created
    # Will need extend invalidate_cache
    # with pytest.raises((IOError, OSError)):
    #     azure.touch(test_dir / 'shfoshf' / 'x')

    with pytest.raises((IOError, OSError)):
        azure.rm(test_dir / 'shfoshf' / 'xxx')

    with pytest.raises((IOError, OSError)):
        azure.mv(test_dir / 'shfoshf' / 'x', test_dir / 'shfoshxbf' / 'y')

    # with pytest.raises(IOError):
    #    azure.chown('unknown', 'someone', 'group')

    # with pytest.raises(IOError):
    #     azure.chmod('unknonwn', 'rb')

    with pytest.raises(IOError):
        azure.rm(test_dir / 'unknown')


@my_vcr.use_cassette
def test_glob_walk(azure):
    with azure_teardown(azure):
        azure.mkdir(test_dir / 'c')
        azure.mkdir(test_dir / 'c' / 'd')
        filenames = ['a', 'a1', 'a2', 'a3', 'b1', 'c/x1', 'c/x2', 'c/d/x3']
        filenames = [test_dir / s for s in filenames]
        for fn in filenames:
            azure.touch(fn)

        assert set(azure.glob(test_dir / 'a*')) == {
            posix(test_dir / 'a'),
            posix(test_dir / 'a1'),
            posix(test_dir / 'a2'),
            posix(test_dir / 'a3')}

        assert set(azure.glob(test_dir / 'c' / '*')) == {
            posix(test_dir / 'c' / 'x1'),
            posix(test_dir / 'c' / 'x2')}

        assert (set(azure.glob(test_dir / 'c')) ==
                set(azure.glob(test_dir / 'c' / '')))

        assert set(azure.glob(test_dir / 'a')) == {posix(test_dir / 'a')}
        assert set(azure.glob(test_dir / 'a1')) == {posix(test_dir / 'a1')}

        assert set(azure.glob(test_dir / '*')) == {
            posix(test_dir / 'a'),
            posix(test_dir / 'a1'),
            posix(test_dir / 'a2'),
            posix(test_dir / 'a3'),
            posix(test_dir / 'b1')}

        assert set(azure.walk(test_dir, invalidate_cache=False)) == {
            posix(test_dir / 'a'),
            posix(test_dir / 'a1'),
            posix(test_dir / 'a2'),
            posix(test_dir / 'a3'),
            posix(test_dir / 'b1'),
            posix(test_dir / 'c' / 'x1'),
            posix(test_dir / 'c' / 'x2'),
            posix(test_dir / 'c' / 'd' / 'x3')}

        assert set(azure.walk(test_dir / 'c', invalidate_cache=False)) == {
            posix(test_dir / 'c' / 'x1'),
            posix(test_dir / 'c' / 'x2'),
            posix(test_dir / 'c' / 'd' / 'x3')}

        assert set(azure.walk(test_dir / 'c', invalidate_cache=False)) == set(azure.walk(test_dir / 'c', invalidate_cache=False))

        # test glob and walk with details=True
        glob_details = azure.glob(test_dir / '*', details=True, invalidate_cache=False)

        # validate that the objects are subscriptable
        assert glob_details[0]['name'] is not None
        assert glob_details[0]['type'] is not None

        walk_details = azure.walk(test_dir, details=True, invalidate_cache=False)
        assert walk_details[0]['name'] is not None
        assert walk_details[0]['type'] is not None

@my_vcr.use_cassette
def test_glob_walk_invalidate_cache(azure):
    with azure_teardown(azure):
        azure.mkdir(test_dir / 'c')
        azure.mkdir(test_dir / 'c' / 'd')
        filenames = ['a', 'a1', 'a2', 'a3', 'b1', 'c/x1', 'c/x2', 'c/d/x3']
        filenames = [test_dir / s for s in filenames]
        for fn in filenames:
            azure.touch(fn)

        assert set(azure.glob(test_dir / 'a*')) == {
            posix(test_dir / 'a'),
            posix(test_dir / 'a1'),
            posix(test_dir / 'a2'),
            posix(test_dir / 'a3')}

        assert set(azure.glob(test_dir / 'c' / '*')) == {
            posix(test_dir / 'c' / 'x1'),
            posix(test_dir / 'c' / 'x2')}

        assert (set(azure.glob(test_dir / 'c')) ==
                set(azure.glob(test_dir / 'c' / '')))

        assert set(azure.glob(test_dir / 'a')) == {posix(test_dir / 'a')}
        assert set(azure.glob(test_dir / 'a1')) == {posix(test_dir / 'a1')}

        assert set(azure.glob(test_dir / '*')) == {
            posix(test_dir / 'a'),
            posix(test_dir / 'a1'),
            posix(test_dir / 'a2'),
            posix(test_dir / 'a3'),
            posix(test_dir / 'b1')}

        assert set(azure.walk(test_dir, invalidate_cache=True)) == {
            posix(test_dir / 'a'),
            posix(test_dir / 'a1'),
            posix(test_dir / 'a2'),
            posix(test_dir / 'a3'),
            posix(test_dir / 'b1'),
            posix(test_dir / 'c' / 'x1'),
            posix(test_dir / 'c' / 'x2'),
            posix(test_dir / 'c' / 'd' / 'x3')}

        assert set(azure.walk(test_dir / 'c', invalidate_cache=True)) == {
            posix(test_dir / 'c' / 'x1'),
            posix(test_dir / 'c' / 'x2'),
            posix(test_dir / 'c' / 'd' / 'x3')}

        assert set(azure.walk(test_dir / 'c', invalidate_cache=True)) == set(azure.walk(test_dir / 'c', invalidate_cache=True))

        # test glob and walk with details=True
        glob_details = azure.glob(test_dir / '*', details=True, invalidate_cache=True)

        # validate that the objects are subscriptable
        assert glob_details[0]['name'] is not None
        assert glob_details[0]['type'] is not None

        walk_details = azure.walk(test_dir, details=True, invalidate_cache=True)
        assert walk_details[0]['name'] is not None
        assert walk_details[0]['type'] is not None

@my_vcr.use_cassette
def test_info(azure):
    with azure_teardown(azure):
        with azure.open(a, 'wb') as f:
            f.write(b'a' * 5)

        info = azure.info(a, invalidate_cache=False)
        assert info['length'] == 5
        assert info['name'] == a
        assert info['type'] == 'FILE'

        assert azure.info(test_dir, invalidate_cache=True)['type'] == 'DIRECTORY'

@my_vcr.use_cassette
def test_info_invalidate_cache(azure, second_azure):
    with azure_teardown(azure):
        # construct initial cache and ensure the file doesn't already exist
        assert not azure.exists(a, invalidate_cache=False)
        assert not second_azure.exists(a, invalidate_cache=False)

        with azure.open(a, 'wb') as f:
            f.write(b'a' * 5)

        # verify that it works in the fs that did the write and not on the other
        info = azure.info(a, invalidate_cache=False)
        with pytest.raises(FileNotFoundError):
            second_azure.info(a, invalidate_cache=False)

        # then invalidate
        second_info = second_azure.info(a, invalidate_cache=True)
        assert info['length'] == 5
        assert info['name'] == a
        assert info['type'] == 'FILE'

        assert info['length'] == second_info['length']
        assert info['name'] == second_info['name']
        assert info['type'] == second_info['type']

        # assure that the cache was properly repopulated on the info call
        assert second_azure.info(test_dir, invalidate_cache=False)['type'] == 'DIRECTORY'

@my_vcr.use_cassette
def test_df(azure):
    with azure_teardown(azure):
        with azure.open(a, 'wb') as f:
            f.write(b'a' * 10)
        with azure.open(b, 'wb') as f:
            f.write(b'a' * 10)

        result = azure.df(test_dir)
        assert result['fileCount'] > 0
        assert result['spaceConsumed'] > 0


@my_vcr.use_cassette
def test_move(azure):
    with azure_teardown(azure):
        azure.touch(a)
        assert azure.exists(a, invalidate_cache=False)
        assert not azure.exists(b, invalidate_cache=False)
        azure.mv(a, b)
        assert not azure.exists(a, invalidate_cache=False)
        assert azure.exists(b, invalidate_cache=False)


@my_vcr.use_cassette
@pytest.mark.xfail(reason='copy not implemented on ADL')
def test_copy(azure):
    with azure_teardown(azure):
        azure.touch(a)
        assert azure.exists(a, invalidate_cache=False)
        assert not azure.exists(b, invalidate_cache=False)
        azure.cp(a, b)
        assert azure.exists(a, invalidate_cache=False)
        assert azure.exists(b, invalidate_cache=False)


@my_vcr.use_cassette
def test_exists(azure):
    with azure_teardown(azure):
        assert not azure.exists(a, invalidate_cache=False)
        azure.touch(a)
        assert azure.exists(a, invalidate_cache=False)
        azure.rm(a)
        assert not azure.exists(a, invalidate_cache=False)

@my_vcr.use_cassette
def test_exists_remove_invalidate_cache(azure, second_azure):
    with azure_teardown(azure):
        # test to ensure file does not exist up front, cache doesn't matter
        assert not azure.exists(a, invalidate_cache=False)
        assert not second_azure.exists(a, invalidate_cache=False)
        azure.touch(a)
        # now ensure that it exists in the client that did the work, but not in the other
        assert azure.exists(a, invalidate_cache=False)
        assert not second_azure.exists(a, invalidate_cache=False)
        # now, with cache invalidation it should exist
        assert second_azure.exists(a, invalidate_cache=True)
        azure.rm(a)
        # same idea with remove. It should no longer exist (cache invalidated or not) in client 1, but still exist in client 2
        assert not azure.exists(a, invalidate_cache=False)
        assert second_azure.exists(a, invalidate_cache=False)
        # now ensure it does not exist when we do invalidate the cache
        assert not second_azure.exists(a, invalidate_cache=True)


@my_vcr.use_cassette
def test_cat(azure):
    with azure_teardown(azure):
        with azure.open(a, 'wb') as f:
            f.write(b'0123456789')
        assert azure.cat(a) == b'0123456789'
        with pytest.raises(IOError):
            azure.cat(b)


@my_vcr.use_cassette
def test_full_read(azure):
    with azure_teardown(azure):
        with azure.open(a, 'wb') as f:
            f.write(b'0123456789')

        with azure.open(a, 'rb') as f:
            assert len(f.read(4)) == 4
            assert len(f.read(4)) == 4
            assert len(f.read(4)) == 2

        with azure.open(a, 'rb') as f:
            assert len(f.read()) == 10

        with azure.open(a, 'rb') as f:
            assert f.tell() == 0
            f.seek(3)
            assert f.read(4) == b'3456'
            assert f.tell() == 7
            assert f.read(4) == b'789'
            assert f.tell() == 10

@my_vcr.use_cassette
def test_readinto(azure):
    with azure_teardown(azure):
        with azure.open(a, 'wb') as f:
            f.write(b'0123456789')

        with azure.open(a, 'rb') as f:
            buffer = bytearray(6)
            l = f.readinto(buffer)
            assert l == 6
            assert buffer == b'012345'

            buffer = bytearray(6)
            l = f.readinto(buffer)
            assert l == 4
            assert buffer == b'6789\x00\x00'

            buffer = bytearray(6)
            l = f.readinto(buffer)
            assert buffer == b'\x00\x00\x00\x00\x00\x00'
            assert l == 0

        with azure.open(a, 'rb') as f:
            buffer = bytearray(6)
            l = f.readinto(buffer)
            assert l == 6
            assert buffer == b'012345'

            l = f.readinto(buffer)
            assert l == 4
            assert buffer == b'678945' # 45 from previous buffer fill should not be overwritten


@my_vcr.use_cassette
def test_filename_specialchar(azure):
    with azure_teardown(azure):
        with azure.open(specialCharFile, 'wb') as f:
            f.write(b'0123456789')

        with azure.open(specialCharFile, 'rb') as f:
            assert len(f.read(4)) == 4
            assert len(f.read(4)) == 4
            assert len(f.read(4)) == 2

        with azure.open(specialCharFile, 'rb') as f:
            assert len(f.read()) == 10

        with azure.open(specialCharFile, 'rb') as f:
            assert f.tell() == 0
            f.seek(3)
            assert f.read(4) == b'3456'
            assert f.tell() == 7
            assert f.read(4) == b'789'
            assert f.tell() == 10


def __ready_and_read_file_for_cache_test(azure, data=b'0123456789abcdef'):
    with azure.open(a, 'wb') as f:
        f.write(data)

    f = azure.open(a, 'rb', blocksize=4)
    # start cache @ 2
    f.seek(2)
    # end cache @ 6
    f.read(4)
    return f


@my_vcr.use_cassette
def test_cache_read_overlapping_end(azure):
    with azure_teardown(azure):

        with __ready_and_read_file_for_cache_test(azure) as f:
            assert f.start == 2
            assert f.end == 6
            assert f.cache == b'2345'

            # offset/loc @ 4
            f.seek(4)
            data = f.read(4)

            assert data == b'4567'
            assert f.start == 6
            assert f.end == 10


@my_vcr.use_cassette
def test_cache_read_overlapping_start(azure):
    with azure_teardown(azure):

        with __ready_and_read_file_for_cache_test(azure) as f:
            assert f.start == 2
            assert f.end == 6
            assert f.cache == b'2345'

            # offset/loc @ 1
            f.seek(1)
            data = f.read(3)

            assert data == b'123'
            assert f.start == 1
            assert f.end == 5


@my_vcr.use_cassette
def test_cache_read_subset(azure):
    with azure_teardown(azure):

        with __ready_and_read_file_for_cache_test(azure) as f:
            assert f.start == 2
            assert f.end == 6
            assert f.cache == b'2345'

            # offset/loc @ 1
            f.seek(4)
            data = f.read(1)

            assert data == b'4'
            assert f.start == 2
            assert f.end == 6


@my_vcr.use_cassette
def test_cache_read_superset(azure):
    with azure_teardown(azure):

        with __ready_and_read_file_for_cache_test(azure) as f:
            assert f.start == 2
            assert f.end == 6
            assert f.cache == b'2345'

            # offset/loc @ 1
            f.seek(1)
            data = f.read(7)

            assert data == b'1234567'
            assert f.start == 5
            assert f.end == 9


@my_vcr.use_cassette
def test_cache_read_continuous_end(azure):
    with azure_teardown(azure):

        with __ready_and_read_file_for_cache_test(azure) as f:
            assert f.start == 2
            assert f.end == 6
            assert f.cache == b'2345'

            # offset/loc @ 1
            f.seek(6)
            data = f.read(2)

            assert data == b'67'
            assert f.start == 6
            assert f.end == 10


@my_vcr.use_cassette
def test_cache_read_continuous_start(azure):
    with azure_teardown(azure):

        with __ready_and_read_file_for_cache_test(azure) as f:
            assert f.start == 2
            assert f.end == 6
            assert f.cache == b'2345'

            # offset/loc @ 1
            f.seek(0)
            data = f.read(2)

            assert data == b'01'
            assert f.start == 0
            assert f.end == 4


@my_vcr.use_cassette
def test_cache_read_within_start(azure):
    with azure_teardown(azure):

        with __ready_and_read_file_for_cache_test(azure) as f:
            assert f.start == 2
            assert f.end == 6
            assert f.cache == b'2345'

            # offset/loc @ 1
            f.seek(2)
            data = f.read(2)

            assert data == b'23'
            assert f.start == 2
            assert f.end == 6


@my_vcr.use_cassette
def test_cache_read_within_end(azure):
    with azure_teardown(azure):

        with __ready_and_read_file_for_cache_test(azure) as f:
            assert f.start == 2
            assert f.end == 6
            assert f.cache == b'2345'

            # offset/loc @ 1
            f.seek(4)
            data = f.read(2)

            assert data == b'45'
            assert f.start == 2
            assert f.end == 6


@my_vcr.use_cassette
def test_cache_read_zero_start(azure):
    with azure_teardown(azure):

        with __ready_and_read_file_for_cache_test(azure) as f:
            assert f.start == 2
            assert f.end == 6
            assert f.cache == b'2345'

            # offset/loc @ 1
            f.seek(2)
            data = f.read(0)

            assert data == b''
            assert f.start == 2
            assert f.end == 6


@my_vcr.use_cassette
def test_cache_read_zero_end(azure):
    with azure_teardown(azure):

        with __ready_and_read_file_for_cache_test(azure) as f:
            assert f.start == 2
            assert f.end == 6
            assert f.cache == b'2345'

            # offset/loc @ 1
            f.seek(6)
            data = f.read(0)

            assert data == b''
            assert f.start == 2
            assert f.end == 6


@my_vcr.use_cassette
def test_cache_read_outside_end(azure):
    with azure_teardown(azure):

        with __ready_and_read_file_for_cache_test(azure) as f:
            assert f.start == 2
            assert f.end == 6
            assert f.cache == b'2345'

            # offset/loc @ 1
            f.seek(8)
            data = f.read(2)

            assert data == b'89'
            assert f.start == 8
            assert f.end == 12


@my_vcr.use_cassette
def test_cache_read_superset_big(azure):
    with azure_teardown(azure):

        with __ready_and_read_file_for_cache_test(azure) as f:
            assert f.start == 2
            assert f.end == 6
            assert f.cache == b'2345'

            # offset/loc @ 1
            f.seek(2)
            data = f.read(20)

            assert data == b'23456789abcdef'
            assert f.start == 14
            assert f.end == 16


@my_vcr.use_cassette
def test_cache_read_superset_verybig(azure):
    with azure_teardown(azure):
        data = b'0123456789abcdefghijklmnopqrstuvwxyz'
        with __ready_and_read_file_for_cache_test(azure, data) as f:
            assert f.start == 2
            assert f.end == 6
            assert f.cache == b'2345'

            # offset/loc @ 1
            f.seek(2)
            data = f.read(36)

            assert data == b'23456789abcdefghijklmnopqrstuvwxyz'
            assert f.start == 34
            assert f.end == 36


@my_vcr.use_cassette
def test_cache_read_multiple_reads(azure):
    with azure_teardown(azure):

        with __ready_and_read_file_for_cache_test(azure) as f:
            assert f.start == 2
            assert f.end == 6
            assert f.cache == b'2345'

            # offset/loc @ 1
            f.seek(1)
            data = f.read(3)
            assert data == b'123'
            assert f.start == 1
            assert f.end == 5
            data = f.read(3)
            assert data == b'456'
            assert f.start == 5
            assert f.end == 9
            data = f.read(3)

            assert data == b'789'
            assert f.start == 9
            assert f.end == 13
            data = f.read(3)
            assert data == b'abc'
            assert f.start == 9
            assert f.end == 13


@my_vcr.use_cassette
def test_cache_read_multiple_reads_big(azure):
    with azure_teardown(azure):

        data = b'0123456789abcdefghijklmnopqrstuvwxyz'
        with __ready_and_read_file_for_cache_test(azure, data) as f:
            assert f.start == 2
            assert f.end == 6
            assert f.cache == b'2345'

            # offset/loc @ 1
            f.seek(1)
            data = f.read(6)
            assert data == b'123456'
            assert f.start == 5
            assert f.end == 9
            data = f.read(9)

            assert data == b'789abcdef'
            assert f.start == 13
            assert f.end == 17
            data = f.read(6)
            assert data == b'ghijkl'
            assert f.start == 21
            assert f.end == 25
            data = f.read(9)
            assert data == b'mnopqrstu'
            assert f.start == 29
            assert f.end == 33
            data = f.read(9)
            assert data == b'vwxyz'
            assert f.start == 33
            assert f.end == 36


@my_vcr.use_cassette
def test_cache_read_full_read(azure):
    with azure_teardown(azure):

        with __ready_and_read_file_for_cache_test(azure) as f:
            assert f.start == 2
            assert f.end == 6
            assert f.cache == b'2345'

            # offset/loc @ 1
            f.seek(3)
            data = f.read(40)

            assert data == b'3456789abcdef'
            assert f.start == 14
            assert f.end == 16


@my_vcr.use_cassette
def test_cache_read_outside_start(azure):
    with azure_teardown(azure):

        with __ready_and_read_file_for_cache_test(azure) as f:
            assert f.start == 2
            assert f.end == 6
            assert f.cache == b'2345'

            # offset/loc @ 1
            f.seek(0)
            data = f.read(1)

            assert data == b'0'
            assert f.start == 0
            assert f.end == 4


@my_vcr.use_cassette
def test_tail_head(azure):
    with azure_teardown(azure):
        with azure.open(a, 'wb') as f:
            f.write(b'0123456789')

        assert azure.tail(a, 3) == b'789'
        assert azure.head(a, 3) == b'012'
        assert azure.tail(a, 100) == b'0123456789'

@my_vcr.use_cassette
def test_read_delimited_block(azure):
    fn = a
    delimiter = b'\n'
    data = delimiter.join([b'123', b'456', b'789'])    
    with azure_teardown(azure):
        with azure.open(fn, 'wb') as f:
            f.write(data)

        #TODO: add E2E validation with the transfer client once delimiters are hooked up
        assert azure.read_block(fn, 1, 2) == b'23'
        assert azure.read_block(fn, 0, 1, delimiter=b'\n') == b'1'
        assert azure.read_block(fn, 0, 2, delimiter=b'\n') == b'12'
        assert azure.read_block(fn, 0, 3, delimiter=b'\n') == b'123'
        assert azure.read_block(fn, 0, 4, delimiter=b'\n') == b'123\n'
        assert azure.read_block(fn, 0, 5, delimiter=b'\n') == b'123\n'
        assert azure.read_block(fn, 0, 8, delimiter=b'\n') == b'123\n456\n'
        assert azure.read_block(fn, 0, 100, delimiter=b'\n') == b'123\n456\n'
        assert azure.read_block(fn, 1, 1, delimiter=b'\n') == b'2'
        assert azure.read_block(fn, 1, 5, delimiter=b'\n') == b'23\n'
        assert azure.read_block(fn, 1, 8, delimiter=b'\n') == b'23\n456\n'

        azure.rm(fn)
        # test the negative cases of just the util read block
        with io.BytesIO(bytearray([1] * 2**22)) as data:
            with pytest.raises(IndexError):
                utils.read_block(data, 0, 2**22, delimiter=b'\n')
        
            # ensure it throws if the new line is past 4MB
            data.seek(2**22)
            data.write(b'\n')
            data.seek(0)
            with pytest.raises(IndexError):
                utils.read_block(data, 0, 1 + 2**22, delimiter=b'\n')

@my_vcr.use_cassette
def test_readline(azure):
    with azure_teardown(azure):
        with azure.open(a, 'wb') as f:
            f.write(b'\n'.join([b'123', b'456', b'789']))

        with azure.open(a) as f:
            assert f.readline() == b'123\n'
            assert f.readline() == b'456\n'
            assert f.readline() == b'789'
            assert f.readline() == b''


@my_vcr.use_cassette
def test_touch_exists(azure):
    with azure_teardown(azure):
        azure.touch(a)
        assert azure.exists(a, invalidate_cache=False)


@my_vcr.use_cassette
def test_write_in_read_mode(azure):
    with azure_teardown(azure):
        azure.touch(a)

        with azure.open(a, 'rb') as f:
            with pytest.raises(ValueError):
                f.write(b'123')


@my_vcr.use_cassette
def test_readlines(azure):
    with azure_teardown(azure):
        with azure.open(a, 'wb') as f:
            f.write(b'123\n456')

        with azure.open(a, 'rb') as f:
            lines = f.readlines()
            assert lines == [b'123\n', b'456']

        with azure.open(a, 'rb') as f:
            assert list(f) == lines

        with azure.open(a, 'wb') as f:
            with pytest.raises(ValueError):
                f.read()

        bigdata = [b'fe', b'fi', b'fo'] * 1000
        with azure.open(a, 'wb') as f:
            f.write(b'\n'.join(bigdata))
        with azure.open(a, 'rb') as f:
            lines = list(f)
        assert all(l in [b'fe\n', b'fi\n', b'fo', b'fo\n'] for l in lines)


@my_vcr.use_cassette
def test_put(azure):
    data = b'1234567890' * 100
    with azure_teardown(azure):
        with tmpfile() as fn:
            with open(fn, 'wb') as f:
                f.write(data)

            azure.put(fn, a)

            assert azure.cat(a) == data


@my_vcr.use_cassette
def test_get(azure):
    data = b'1234567890'
    with azure_teardown(azure):
        with tmpfile() as fn:
            with azure.open(a, 'wb') as f:
                f.write(data)

            azure.get(a, fn)

            with open(fn, 'rb') as f:
                data2 = f.read()
            assert data2 == data

        with pytest.raises(IOError):
            azure.get(b, fn)


@my_vcr.use_cassette
def test_du(azure):
    with azure_teardown(azure):
        with azure.open(a, 'wb') as f:
            f.write(b'123')
        with azure.open(b, 'wb') as f:
            f.write(b'4567')

        assert azure.du(test_dir) == {a: 3, b: 4}
        assert azure.du(test_dir, total=True) == 3 + 4


@my_vcr.use_cassette
def test_text_bytes(azure):
    with pytest.raises(NotImplementedError):
        azure.open(a, 'wt')

    with pytest.raises(NotImplementedError):
        azure.open(a, 'rt')


@my_vcr.use_cassette
def test_append(azure):
    with azure_teardown(azure):
        with azure.open(a, mode='ab') as f:
            f.write(b'123')
        with azure.open(a, mode='ab') as f:
            f.write(b'456')

        with azure.open(a, mode='rb') as f:
            assert f.read() == b'123456'

        with azure.open(a, mode='ab') as f:
            f.write(b'789')
        with azure.open(a, mode='rb') as f:
            assert f.read() == b'123456789'


@my_vcr.use_cassette
def test_write_empty(azure):
    with azure_teardown(azure):
        with azure.open(a, mode='wb') as f:
            f.write(b'')

        with azure.open(a, mode='rb') as f:
            assert f.read() == b''


@my_vcr.use_cassette
def test_write_blocks(azure):
    with azure_teardown(azure):
        with azure.open(a, mode='wb', blocksize=5) as f:
            f.write(b'000')
            assert f.buffer.tell() == 3
            f.write(b'000')  # forces flush
            assert f.buffer.tell() == 1
            f.write(b'000')
            assert f.tell() == 9
        assert azure.du(a)[a] == 9


@my_vcr.use_cassette
def test_skip_existing_block(azure):
    with azure.open(a, mode='wb') as f:
        f.write(b'0' * 15)

    with pytest.raises((IOError, RuntimeError)):
        with azure.open(a, mode='ab') as f:
            assert f.tell() == 15
            f.loc = 5  # not a user method
            f.write(b'blah')


@my_vcr.use_cassette
def test_gzip(azure):
    import gzip
    data = b'name,amount\nAlice,100\nBob,200'
    with azure_teardown(azure):
        with azure.open(a, mode='wb') as f:
            with gzip.GzipFile(fileobj=f) as g:
                g.write(b'name,amount\nAlice,100\nBob,200')

        with azure.open(a) as f:
            with gzip.GzipFile(fileobj=f) as g:
                bytes = g.read()

        assert bytes == data


@my_vcr.use_cassette
def test_fooable(azure):
    with azure_teardown(azure):
        azure.touch(a)

        with azure.open(a, mode='rb') as f:
            assert f.readable()
            assert f.seekable()
            assert not f.writable()

        with azure.open(a, mode='wb') as f:
            assert not f.readable()
            assert not f.seekable()
            assert f.writable()


@my_vcr.use_cassette
def test_closed(azure):
    with azure_teardown(azure):
        azure.touch(a)

        f = azure.open(a, mode='rb')
        assert not f.closed
        f.close()
        assert f.closed


@my_vcr.use_cassette
def test_TextIOWrapper(azure):
    with azure_teardown(azure):
        with azure.open(a, mode='wb') as f:
            f.write(b'1,2\n3,4\n5,6')

        with azure.open(a, mode='rb') as f:
            ff = io.TextIOWrapper(f)
            data = list(ff)

        assert data == ['1,2\n', '3,4\n', '5,6']


@my_vcr.use_cassette
def test_array(azure):
    from array import array
    data = array('B', [65] * 1000)

    with azure_teardown(azure):
        with azure.open(a, 'wb') as f:
            f.write(data)

        with azure.open(a, 'rb') as f:
            out = f.read()
            assert out == b'A' * 1000


def write_delimited_data(azure, delimiter):
    data = delimiter.join([b'123', b'456', b'789'])
    with azure.open(a, 'wb', delimiter=delimiter, blocksize=6) as f:
        f.write(b'123' + delimiter)
        assert f.buffer.tell() == 3 + len(delimiter)
        f.write(b'456' + delimiter)                    # causes flush, but will only write till 123 + delimiter
        assert f.buffer.tell() == 3 + len(delimiter)   # buffer will have b'456' + delimiter
        f.buffer, temp_buffer = io.BytesIO(), f.buffer # Emptry buffer so flush doesn't write any more data
        f.loc, temp_loc = 3 + len(delimiter), f.loc    # Fix location.
        f.flush(force=True) # To Sync metadata. Force is needed as there is no data in buffer

        assert azure.cat(a) == b'123' + delimiter
        f.buffer = temp_buffer
        f.loc = temp_loc
        # close causes forced flush
        f.write(b'789')

    assert azure.cat(a) == data


@my_vcr.use_cassette
def test_delimiters_newline(azure):
    with azure_teardown(azure):
        write_delimited_data(azure, b'\n')


@my_vcr.use_cassette
def test_delimiters_dash(azure):
    with azure_teardown(azure):
        write_delimited_data(azure, b'--')


@my_vcr.use_cassette
def test_chmod(azure):
    pass


@my_vcr.use_cassette
def test_chown(azure):
    pass

@my_vcr.use_cassette
def test_acl_management(azure):
    pass


@my_vcr.use_cassette
def test_modify_acl_entries(azure):
    with azure_teardown(azure):
        acluser = AZURE_ACL_TEST_APPID
        azure.touch(a)

        permission = "---"
        azure.modify_acl_entries(a, acl_spec="user:"+acluser+":"+permission)
        current_acl = azure.get_acl_status(a)
        aclspec = [s for s in current_acl['entries'] if acluser in s][0]
        assert aclspec.split(':')[-1] == permission

        permission = "rwx"
        azure.modify_acl_entries(a, acl_spec="user:" + acluser + ":" + permission)
        current_acl = azure.get_acl_status(a)
        aclspec = [s for s in current_acl['entries'] if acluser in s][0]
        assert aclspec.split(':')[-1] == permission


@my_vcr.use_cassette
def test_remove_acl_entries(azure):
    with azure_teardown(azure):
        acluser = AZURE_ACL_TEST_APPID
        azure.touch(a)

        permission = "rwx"
        azure.modify_acl_entries(a, acl_spec="user:"+acluser+":"+permission)
        current_acl = azure.get_acl_status(a)
        aclspec = [s for s in current_acl['entries'] if acluser in s]
        assert aclspec != []

        azure.remove_acl_entries(a, acl_spec="user:" + acluser)
        current_acl = azure.get_acl_status(a)
        aclspec = [s for s in current_acl['entries'] if acluser in s]
        assert aclspec == []

@my_vcr.use_cassette
def test_set_acl(azure):
    with azure_teardown(azure):
        acluser = AZURE_ACL_TEST_APPID
        azure.touch(a)
        set_acl_base ="user::rwx,group::rwx,other::---,"

        permission = "rwx"
        azure.set_acl(a, acl_spec=set_acl_base + "user:"+acluser+":"+permission)
        current_acl = azure.get_acl_status(a)
        aclspec = [s for s in current_acl['entries'] if acluser in s][0]
        assert len(current_acl['entries']) == 5
        assert aclspec.split(':')[-1] == permission

@my_vcr.use_cassette
def test_set_expiry(azure):
    with azure_teardown(azure):
        # this future time gives the milliseconds since the epoch that have occured as of 01/31/2030 at noon
        epoch_time = datetime.datetime.utcfromtimestamp(0)
        final_time = datetime.datetime(2030, 1, 31, 12)
        time_in_milliseconds = (final_time - epoch_time).total_seconds() * 1000
        
        # create the file
        azure.touch(a)
        
        # first get the existing expiry, which should be never
        initial_expiry = azure.info(a, invalidate_cache=True)['msExpirationTime']
        azure.set_expiry(a, 'Absolute', time_in_milliseconds)
        cur_expiry = azure.info(a, invalidate_cache=True)['msExpirationTime']
        # this is a range of +- 100ms because the service does a best effort to set it precisely, but there is
        # no guarantee that the expiry will be to the exact millisecond
        assert time_in_milliseconds - 100 <= cur_expiry <= time_in_milliseconds + 100
        assert initial_expiry != cur_expiry

        # now set it back to never expire and validate it is the same
        azure.set_expiry(a, 'NeverExpire')
        cur_expiry = azure.info(a)['msExpirationTime']
        assert initial_expiry == cur_expiry

        # now validate the fail cases
        # bad enum
        with pytest.raises(ValueError):
            azure.set_expiry(a, 'BadEnumValue')
        
        # missing time
        with pytest.raises(ValueError):
            azure.set_expiry(a, 'Absolute')

@pytest.mark.skipif(sys.platform != 'win32', reason="requires windows")
def test_backslash():
    from azure.datalake.store.core import AzureDLPath

    posix_abspath = '/foo/bar'
    posix_relpath = 'foo/bar'

    win_abspath = AzureDLPath('\\foo\\bar')
    win_relpath = AzureDLPath('foo\\bar')

    assert posix(win_abspath) == posix_abspath
    assert posix(win_abspath.trim()) == posix_relpath

    assert 'foo' in win_abspath
    assert 'foo' in win_relpath

    assert posix(AzureDLPath('\\*').globless_prefix) == '/'
    assert posix(AzureDLPath('\\foo\\*').globless_prefix) == '/foo'
    assert posix(AzureDLPath('\\foo\\b*').globless_prefix) == '/foo'


def test_forward_slash():
    from azure.datalake.store.core import AzureDLPath

    posix_abspath = '/foo/bar'
    posix_relpath = 'foo/bar'

    abspath = AzureDLPath('/foo/bar')
    relpath = AzureDLPath('foo/bar')

    assert posix(abspath) == posix_abspath
    assert posix(abspath.trim()) == posix_relpath

    assert 'foo' in abspath
    assert 'foo' in relpath

    assert posix(AzureDLPath('/*').globless_prefix) == '/'
    assert posix(AzureDLPath('/foo/*').globless_prefix) == '/foo'
    assert posix(AzureDLPath('/foo/b*').globless_prefix) == '/foo'


def test_DatalakeBadOffsetExceptionRecovery(azure):
    from azure.datalake.store.core import _put_data_with_retry
    data = b'abc'
    _put_data_with_retry(azure.azure, 'CREATE', a, data=data)
    _put_data_with_retry(azure.azure, 'APPEND', a, data=data, offset=len(data))
    _put_data_with_retry(azure.azure, 'APPEND', a, data=data, offset=len(data))
    assert azure.cat(a) == data*2
    _put_data_with_retry(azure.azure, 'APPEND', a, data=data)
    assert azure.cat(a) == data*3


def test_file_creation_open(azure):
    with azure_teardown(azure):
        if azure.exists(a):
            azure.rm(a)
        assert not azure.exists(a)
        f = azure.open(a, "wb")
        assert azure.exists(a)
        f.close()
        assert azure.info(a)['length'] == 0

