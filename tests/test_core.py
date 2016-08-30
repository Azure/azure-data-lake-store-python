# -*- coding: utf-8 -*-
# coding=utf-8
# --------------------------------------------------------------------------
# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See License.txt in the project root for
# license information.
# --------------------------------------------------------------------------

import io

import pytest

from tests.testing import azure, azure_teardown, my_vcr, tmpfile, working_dir

test_dir = working_dir()

a = test_dir + 'a'
b = test_dir + 'b'
c = test_dir + 'c'
d = test_dir + 'd'


@my_vcr.use_cassette
def test_simple(azure):
    with azure_teardown(azure):
        data = b'a' * (2**16)

        with azure.open(a, 'wb') as f:
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
        assert not azure.ls(test_dir)
        azure.touch(a)
        azure.touch(b)
        L = azure.ls(test_dir, True)
        assert set(d['name'] for d in L) == set([a, b])
        L = azure.ls(test_dir, False)
        assert set(L) == set([a, b])


@my_vcr.use_cassette
def test_rm(azure):
    with azure_teardown(azure):
        assert not azure.exists(a)
        azure.touch(a)
        assert azure.exists(a)
        azure.rm(a)
        assert not azure.exists(a)


@my_vcr.use_cassette
def test_pickle(azure):
    import pickle
    azure2 = pickle.loads(pickle.dumps(azure))

    assert azure2.token == azure.token


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
def test_bad_open(azure):
    with pytest.raises(IOError):
        azure.open('')


@my_vcr.use_cassette
def test_errors(azure):
    with pytest.raises((IOError, OSError)):
        azure.open(test_dir + 'shfoshf', 'rb')

    # This is totally OK: directory is silently created
    # Will need extend invalidate_cache
    # with pytest.raises((IOError, OSError)):
    #     azure.touch(test_dir + 'shfoshf/x')

    with pytest.raises((IOError, OSError)):
        azure.rm(test_dir + 'shfoshf/xxx')

    with pytest.raises((IOError, OSError)):
        azure.mv(test_dir + 'shfoshf/x', test_dir + 'shfoshxbf/y')

    # with pytest.raises(IOError):
    #    azure.chown('/unknown', 'someone', 'group')

    # with pytest.raises(IOError):
    #     azure.chmod('/unknonwn', 'rb')

    with pytest.raises(IOError):
        azure.rm(test_dir + '/unknown')


@my_vcr.use_cassette
def test_glob_walk(azure):
    with azure_teardown(azure):
        azure.mkdir(test_dir + 'c/')
        azure.mkdir(test_dir + 'c/d/')
        filenames = ['a', 'a1', 'a2', 'a3', 'b1', 'c/x1', 'c/x2', 'c/d/x3']
        filenames = [test_dir + s for s in filenames]
        for fn in filenames:
            azure.touch(fn)

        assert set(azure.glob(test_dir + 'a*')) == {test_dir + 'a',
                                                    test_dir + 'a1',
                                                    test_dir + 'a2',
                                                    test_dir + 'a3'}

        assert set(azure.glob(test_dir + 'c/*')) == {test_dir + 'c/x1',
                                                test_dir + 'c/x2'}
        assert (set(azure.glob(test_dir + 'c')) ==
                set(azure.glob(test_dir + 'c/')) ==
                set(azure.glob(test_dir + 'c/*')))

        assert set(azure.glob(test_dir + 'a')) == {test_dir + 'a'}
        assert set(azure.glob(test_dir + 'a1')) == {test_dir + 'a1'}

        assert set(azure.glob(test_dir + '*')) == {test_dir + 'a',
                                                test_dir + 'a1',
                                                test_dir + 'a2',
                                                test_dir + 'a3',
                                                test_dir + 'b1'}

        assert set(azure.walk(test_dir)) == {test_dir + 'a',
                                            test_dir + 'a1',
                                            test_dir + 'a2',
                                            test_dir + 'a3',
                                            test_dir + 'b1',
                                            test_dir + 'c/x1',
                                            test_dir + 'c/x2',
                                            test_dir + 'c/d/x3'}

        assert set(azure.walk(test_dir + 'c/')) == {test_dir + 'c/x1',
                                                    test_dir + 'c/x2',
                                                    test_dir + 'c/d/x3'}

        assert set(azure.walk(test_dir + 'c/')) == set(azure.walk(test_dir + 'c'))


@my_vcr.use_cassette
def test_info(azure):
    with azure_teardown(azure):
        with azure.open(a, 'wb') as f:
            f.write(b'a' * 5)

        info = azure.info(a)
        assert info['length'] == 5
        assert info['name'] == a
        assert info['type'] == 'FILE'

        assert azure.info(test_dir)['type'] == 'DIRECTORY'


@my_vcr.use_cassette
def test_df(azure):
    with azure_teardown(azure):
        with azure.open(a, 'wb') as f:
            f.write(b'a' * 10)
        with azure.open(b, 'wb') as f:
            f.write(b'a' * 10)

        result = azure.df()
        assert result['fileCount'] > 0
        assert result['spaceConsumed'] > 0


@my_vcr.use_cassette
def test_move(azure):
    with azure_teardown(azure):
        azure.touch(a)
        assert azure.exists(a)
        assert not azure.exists(b)
        azure.mv(a, b)
        assert not azure.exists(a)
        assert azure.exists(b)


@my_vcr.use_cassette
@pytest.mark.xfail(reason='copy not implemented on ADL')
def test_copy(azure):
    with azure_teardown(azure):
        azure.touch(a)
        assert azure.exists(a)
        assert not azure.exists(b)
        azure.cp(a, b)
        assert azure.exists(a)
        assert azure.exists(b)


@my_vcr.use_cassette
def test_exists(azure):
    with azure_teardown(azure):
        assert not azure.exists(a)
        azure.touch(a)
        assert azure.exists(a)
        azure.rm(a)
        assert not azure.exists(a)


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
def test_tail_head(azure):
    with azure_teardown(azure):
        with azure.open(a, 'wb') as f:
            f.write(b'0123456789')

        assert azure.tail(a, 3) == b'789'
        assert azure.head(a, 3) == b'012'
        assert azure.tail(a, 100) == b'0123456789'


@my_vcr.use_cassette
def test_read_delimited_block(azure):
    fn = '/tmp/test/a'
    delimiter = b'\n'
    data = delimiter.join([b'123', b'456', b'789'])

    with azure_teardown(azure):
        with azure.open(fn, 'wb') as f:
            f.write(data)

        assert azure.read_block(fn, 1, 2) == b'23'
        assert azure.read_block(fn, 0, 1, delimiter=b'\n') == b'123\n'
        assert azure.read_block(fn, 0, 2, delimiter=b'\n') == b'123\n'
        assert azure.read_block(fn, 0, 3, delimiter=b'\n') == b'123\n'
        assert azure.read_block(fn, 0, 5, delimiter=b'\n') == b'123\n456\n'
        assert azure.read_block(fn, 0, 8, delimiter=b'\n') == b'123\n456\n789'
        assert azure.read_block(fn, 0, 100, delimiter=b'\n') == b'123\n456\n789'
        assert azure.read_block(fn, 1, 1, delimiter=b'\n') == b''
        assert azure.read_block(fn, 1, 5, delimiter=b'\n') == b'456\n'
        assert azure.read_block(fn, 1, 8, delimiter=b'\n') == b'456\n789'

        for ols in [[(0, 3), (3, 3), (6, 3), (9, 2)],
                    [(0, 4), (4, 4), (8, 4)]]:
            out = [azure.read_block(fn, o, l, b'\n') for o, l in ols]
            assert b''.join(filter(None, out)) == data


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
        assert azure.exists(a)


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
            assert f.buffer.tell() == 0
            f.write(b'000')
            assert f.tell() == 9
        assert azure.du(a)[a] == 9


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
    data2 = data + delimiter
    with azure.open(a, 'wb', delimiter=delimiter, blocksize=6) as f:
        f.write(b'123' + delimiter)
        assert f.buffer.tell() == 3 + len(delimiter)
        f.write(b'456' + delimiter) # causes flush
        assert azure.cat(a) == b'123' + delimiter
        f.write(b'789')
    # close causes forced flush
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
    with azure_teardown(azure):
        azure.touch(a)

        assert azure.info(a)['permission'] == '770'

        azure.chmod(a, '0555')
        assert azure.info(a)['permission'] == '555'

        with pytest.raises((OSError, IOError)):
            with azure.open(a, 'ab') as f:
                f.write(b'data')

        azure.chmod(a, '0770')
        azure.rm(a)

        azure.mkdir(test_dir+'/deep')
        azure.touch(test_dir+'/deep/file')
        azure.chmod(test_dir+'/deep', '660')

        with pytest.raises((OSError, IOError)):
            azure.ls(test_dir+'/deep')

        azure.chmod(test_dir+'/deep', '770')
