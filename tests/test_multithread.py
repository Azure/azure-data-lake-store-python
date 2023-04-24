# -*- coding: utf-8 -*-
# coding=utf-8
# --------------------------------------------------------------------------
# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See License.txt in the project root for
# license information.
# --------------------------------------------------------------------------

from contextlib import contextmanager
import os
import pytest
import shutil
import tempfile

from azure.datalake.store.core import AzureDLPath
from azure.datalake.store.multithread import ADLDownloader, ADLUploader
from tests.testing import azure, azure_teardown, md5sum, my_vcr, posix, working_dir
from azure.datalake.store.transfer import ADLTransferClient
from tests.settings import AZURE_ACL_TEST_APPID

test_dir = working_dir()


@pytest.yield_fixture()
def tempdir():
    tmpdir = tempfile.mkdtemp()
    try:
        yield tmpdir
    finally:
        shutil.rmtree(tmpdir, True)


def linecount(infile):
    lines = 0
    with open(infile) as f:
        for line in f:
            lines += 1
    return lines

# TODO : when the uploader is ready, should place file in temp location
# rather than rely on file already in place.


@contextmanager
def setup_tree(azure, test_dir=None):
    if test_dir == None:
        test_dir = working_dir()
    for directory in ['', 'data/a', 'data/b']:
        azure.mkdir(test_dir / directory)
        for filename in ['x.csv', 'y.csv', 'z.txt']:
            with azure.open(test_dir / directory / filename, 'wb') as f:
                f.write(b'123456')
    azure.mkdir(test_dir / 'data/empty')
    azure.mkdir(test_dir / 'data/single/single')
    with azure.open(test_dir / 'data/single/single'/ 'single.txt', 'wb') as f:
        f.write(b'123456')
    try:
        yield
    finally:
        azure.rm(test_dir, recursive=True)


def create_remote_csv(fs, name, columns, colwidth, lines):
    from hashlib import md5
    from itertools import cycle, islice
    hashobj = md5()
    haystack = '0123456789ABCDEF'
    row = ','.join([ch * colwidth for ch in islice(cycle(haystack), columns)]) + '\n'
    row = row.encode('utf-8')
    fsize = 0
    with fs.open(name, 'wb') as f:
        for _ in range(0, lines):
            hashobj.update(row)
            f.write(row)
            fsize += len(row)
    return fsize, hashobj.hexdigest()


@my_vcr.use_cassette
def test_download_single_empty_file(tempdir, azure):
    with azure_teardown(azure):
        name = posix(test_dir, 'remote.csv')
        lines = 0 # the file should have no bytes in it
        size, checksum = create_remote_csv(azure, name, 10, 5, lines)
        fname = os.path.join(tempdir, 'local.csv')

        # single chunk
        try:
            down = ADLDownloader(azure, name, fname, 1, size + 10, overwrite=True)
            assert md5sum(fname) == checksum
            assert os.stat(fname).st_size == size
            assert linecount(fname) == lines
        finally:
            if os.path.isfile(fname):
                os.remove(fname)

@my_vcr.use_cassette
def test_download_single_file(tempdir, azure):
    with azure_teardown(azure):
        name = posix(test_dir, 'remote.csv')
        lines = 100
        fname = os.path.join(tempdir, 'local.csv')
        size, checksum = create_remote_csv(azure, name, 10, 5, lines)
        try:
            # single chunk
            down = ADLDownloader(azure, name, fname, 1, size + 10, overwrite=True)
            assert md5sum(fname) == checksum
            assert os.stat(fname).st_size == size
            assert linecount(fname) == lines
        finally:
            if os.path.isfile(fname):
                os.remove(fname)

        try:
            # multiple chunks, one thread
            down = ADLDownloader(azure, name, fname, 1, size // 5, overwrite=True)
            assert md5sum(fname) == checksum
            assert os.stat(fname).st_size == size
            assert linecount(fname) == lines
        finally:
            if os.path.isfile(fname):
                os.remove(fname)


@my_vcr.use_cassette
def test_download_single_to_dir(tempdir, azure):
    with azure_teardown(azure):
        name = posix(test_dir, 'remote.csv')
        lines = 100
        size, checksum = create_remote_csv(azure, name, 10, 5, lines)
        fname = os.path.join(tempdir, 'remote.csv')
        try:
            down = ADLDownloader(azure, name, tempdir, 1, 2**24, overwrite=True)
            assert md5sum(fname) == checksum
            assert os.stat(fname).st_size == size
            assert linecount(fname) == lines
        finally:
            if os.path.isfile(fname):
                os.remove(fname)

@my_vcr.use_cassette
def test_download_empty_directory(tempdir, azure):
    with setup_tree(azure):
        down = ADLDownloader(azure, test_dir, tempdir, 1, 2 ** 24, overwrite=True)
        dirname = os.path.join(tempdir, 'data/empty')
        assert os.path.isdir(dirname)

@my_vcr.use_cassette
def test_download_single_file_in_directory(tempdir, azure):
    with setup_tree(azure):
        down = ADLDownloader(azure, test_dir, tempdir, 1, 2 ** 24, overwrite=True)
        dirname = os.path.join(tempdir, 'data/single/single')
        assert os.path.isdir(dirname)
        assert os.path.isfile(os.path.join(dirname,'single.txt'))

@my_vcr.use_cassette
def test_download_many(tempdir, azure):
    with setup_tree(azure):
        down = ADLDownloader(azure, test_dir, tempdir, 1, 2**24, overwrite=True)
        nfiles = 0
        for dirpath, dirnames, filenames in os.walk(tempdir):
            nfiles += len(filenames)
        assert nfiles > 1

@my_vcr.use_cassette
def test_download_path(azure):
    with setup_tree(azure):
        down = ADLDownloader(
            azure,
            lpath="/lpath/test/testfolder",
            rpath='/' + test_dir.name,
            run=False)
        for lfile, rfile in down._file_pairs:
            if 'data' in lfile:
                lfile = AzureDLPath(lfile)
                assert lfile.as_posix().startswith('/lpath/test/testfolder/data')


@my_vcr.use_cassette
def test_download_glob(tempdir, azure):
    with setup_tree(azure):
        remote_path = test_dir / 'data' / 'a' / '*.csv'
        down = ADLDownloader(azure, remote_path, tempdir, run=False,
                             overwrite=True)
        file_pair_dict = dict(down._file_pairs)

        assert len(file_pair_dict.keys()) == 2

        lfiles = [os.path.relpath(f, tempdir) for f in file_pair_dict.keys()]
        assert sorted(lfiles) == sorted(['x.csv.inprogress', 'y.csv.inprogress'])

        remote_path = test_dir / 'data' / '*' / '*.csv'
        down = ADLDownloader(azure, remote_path, tempdir, run=False,
                             overwrite=True)

        file_pair_dict = dict(down._file_pairs)
        assert len(file_pair_dict.keys()) == 4

        lfiles = [os.path.relpath(f, tempdir) for f in file_pair_dict.keys()]
        assert sorted(lfiles) == sorted([
            os.path.join('a', 'x.csv.inprogress'),
            os.path.join('a', 'y.csv.inprogress'),
            os.path.join('b', 'x.csv.inprogress'),
            os.path.join('b', 'y.csv.inprogress')])

        remote_path = test_dir / 'data' / '*' / 'z.txt'
        down = ADLDownloader(azure, remote_path, tempdir, run=False,
                             overwrite=True)
        file_pair_dict = dict(down._file_pairs)
        assert len(file_pair_dict.keys()) == 2

        lfiles = [os.path.relpath(f, tempdir) for f in file_pair_dict.keys()]
        assert sorted(lfiles) == sorted([
            os.path.join('a', 'z.txt.inprogress'),
            os.path.join('b', 'z.txt.inprogress')])

@my_vcr.use_cassette
def test_download_glob_single_file(tempdir, azure):
    with setup_tree(azure):
        print("")
        remote_path = test_dir / 'data/single/single' / '*.txt'
        down = ADLDownloader(azure, remote_path, tempdir, run=False,
                             overwrite=True)
        file_pair_dict = dict(down._file_pairs)

        assert len(file_pair_dict) == 1

        lfiles = [os.path.relpath(f, tempdir) for f in file_pair_dict.keys()]
        assert sorted(lfiles) == sorted([os.path.join('single.txt.inprogress')])

        remote_path = test_dir / 'data/*/single' / 'single.txt'
        down = ADLDownloader(azure, remote_path, tempdir, run=False,
                             overwrite=True)
        file_pair_dict = dict(down._file_pairs)
        assert len(file_pair_dict) == 1

        lfiles = [os.path.relpath(f, tempdir) for f in file_pair_dict.keys()]
        assert sorted(lfiles) == sorted([os.path.join('single','single' ,'single.txt.inprogress')])


@my_vcr.use_cassette
def test_download_overwrite(tempdir, azure):
    with setup_tree(azure):
        with open(os.path.join(tempdir, 'x.csv'), 'w') as f:
            f.write('12345')

        with pytest.raises(OSError) as e:
            ADLDownloader(azure, test_dir, tempdir, 1, 2**24, run=False)
        assert os.path.split(tempdir)[1] in str(e)


@my_vcr.use_cassette
def test_save_down(tempdir, azure):
    with setup_tree(azure):
        down = ADLDownloader(azure, test_dir, tempdir, 1, 2**24, run=False,
                             overwrite=True)
        down.save()

        alldownloads = ADLDownloader.load()
        assert down.hash in alldownloads

        down.save(keep=False)
        alldownloads = ADLDownloader.load()
        assert down.hash not in alldownloads


@pytest.yield_fixture()
def local_files(tempdir):
    filenames = [os.path.join(tempdir, f) for f in ['bigfile', 'littlefile', 'emptyfile']]
    with open(filenames[0], 'wb') as f:
        for char in b"0 1 2 3 4 5 6 7 8 9".split():
            f.write(char * 1000)
    with open(filenames[1], 'wb') as f:
        f.write(b'0123456789')
    with open(filenames[2], 'wb') as f: # just open an empty file and close it
        f.close()
    nestpath = os.path.join(tempdir, 'nested1', 'nested2')
    os.makedirs(nestpath)
    for filename in ['a', 'b', 'c']:
        filenames.append(os.path.join(nestpath, filename))
        with open(os.path.join(nestpath, filename), 'wb') as f:
            f.write(b'0123456789')
    yield filenames

@my_vcr.use_cassette
def test_upload_one(local_files, azure):
    with azure_teardown(azure):
        bigfile, littlefile, emptyfile, a, b, c = local_files

        # transfer client w/ deterministic temporary directory
        from azure.datalake.store.multithread import put_chunk
        client = ADLTransferClient(azure, transfer=put_chunk,
                                   unique_temporary=False)

        # single chunk
        up = ADLUploader(azure, test_dir / 'littlefile', littlefile, nthreads=1,
                         overwrite=True)
        assert azure.info(test_dir / 'littlefile')['length'] == 10

        # multiple chunks, one thread
        size = 10000
        up = ADLUploader(azure, test_dir / 'bigfile', bigfile, nthreads=1,
                         chunksize=size//5, client=client, run=False,
                         overwrite=True)
        up.run()

        assert azure.info(test_dir / 'bigfile')['length'] == size

        azure.rm(test_dir / 'bigfile')

@my_vcr.use_cassette
def test_upload_single_file_in_dir(tempdir, azure):
    with azure_teardown(azure):
        lpath_dir = tempdir
        lfilename = os.path.join(lpath_dir, 'singlefile')
        with open(lfilename, 'wb') as f:
            f.write(b'0123456789')

        # transfer client w/ deterministic temporary directory
        from azure.datalake.store.multithread import put_chunk
        client = ADLTransferClient(azure, transfer=put_chunk,
                                   unique_temporary=False)

        up = ADLUploader(azure, test_dir / 'singlefiledir', lpath_dir, nthreads=1,
                         overwrite=True)
        assert azure.info(test_dir / 'singlefiledir' / 'singlefile')['length'] == 10
        azure.rm(test_dir / 'singlefiledir' / 'singlefile')

@my_vcr.use_cassette
def test_upload_one_empty_file(local_files, azure):
    with azure_teardown(azure):
        bigfile, littlefile, emptyfile, a, b, c = local_files

        # transfer client w/ deterministic temporary directory
        from azure.datalake.store.multithread import put_chunk
        client = ADLTransferClient(azure, transfer=put_chunk,
                                   unique_temporary=False)

        # single chunk, empty file
        up = ADLUploader(azure, test_dir / 'emptyfile', emptyfile, nthreads=1,
                         overwrite=True)
        assert azure.info(test_dir / 'emptyfile')['length'] == 0
        azure.rm(test_dir / 'emptyfile')

@my_vcr.use_cassette
def test_upload_many(local_files, azure):
    with azure_teardown(azure):
        bigfile, littlefile, emptyfile, a, b, c = local_files
        root = os.path.dirname(bigfile)

        # single thread
        up = ADLUploader(azure, test_dir, root, nthreads=1, overwrite=True)
        assert azure.info(test_dir / 'littlefile')['length'] == 10
        assert azure.cat(test_dir / 'nested1/nested2/a') == b'0123456789'
        assert len(azure.du(test_dir, deep=True)) == 6
        assert azure.du(test_dir, deep=True, total=True) == 10000 + 40


@my_vcr.use_cassette
def test_upload_glob(tempdir, azure):
    for directory in ['a', 'b']:
        d = os.path.join(tempdir, 'data', directory)
        os.makedirs(d)
        for data in ['x.csv', 'y.csv', 'z.txt']:
            with open(os.path.join(d, data), 'wb') as f:
                f.write(b'0123456789')

    with azure_teardown(azure):
        local_path = os.path.join(tempdir, 'data', 'a', '*.csv')
        up = ADLUploader(azure, test_dir, local_path, run=False,
                         overwrite=True)

        file_pair_dict = dict(up._file_pairs)
        assert len(file_pair_dict.keys()) == 2
        rfiles = [posix(AzureDLPath(f).relative_to(test_dir))
                  for f in file_pair_dict.values()]
        assert sorted(rfiles) == sorted(['x.csv', 'y.csv'])

        local_path = os.path.join(tempdir, 'data', '*', '*.csv')
        up = ADLUploader(azure, test_dir, local_path, run=False,
                         overwrite=True)

        file_pair_dict = dict(up._file_pairs)
        assert len(file_pair_dict.keys()) == 4

        rfiles = [posix(AzureDLPath(f).relative_to(test_dir))
                  for f in file_pair_dict.values()]
        assert sorted(rfiles) == sorted([
            posix('a', 'x.csv'),
            posix('a', 'y.csv'),
            posix('b', 'x.csv'),
            posix('b', 'y.csv')])

        local_path = os.path.join(tempdir, 'data', '*', 'z.txt')
        up = ADLUploader(azure, test_dir, local_path, run=False,
                         overwrite=True)

        file_pair_dict = dict(up._file_pairs)
        assert len(file_pair_dict.keys()) == 2

        rfiles = [posix(AzureDLPath(f).relative_to(test_dir))
                  for f in file_pair_dict.values()]

        assert sorted(rfiles) == sorted([posix('a', 'z.txt'), posix('b', 'z.txt')])


@my_vcr.use_cassette
def test_upload_overwrite(local_files, azure):
    bigfile, littlefile, emptyfile, a, b, c = local_files

    with azure_teardown(azure):
        # make the file already exist.
        azure.touch('/{}/littlefile'.format(test_dir.as_posix()))

        with pytest.raises(OSError) as e:
            ADLUploader(azure, test_dir, littlefile, nthreads=1)
        assert test_dir.as_posix() in str(e)


@my_vcr.use_cassette
def test_save_up(local_files, azure):
    bigfile, littlefile, emptyfile, a, b, c = local_files
    root = os.path.dirname(bigfile)

    up = ADLUploader(azure, '', root, 1, 1000000, run=False, overwrite=True)
    up.save()

    alluploads = ADLUploader.load()
    assert up.hash in alluploads

    up.save(keep=False)
    alluploads = ADLUploader.load()
    assert up.hash not in alluploads


@my_vcr.use_cassette
def test_download_root_folder(azure, tempdir):
    with setup_tree(azure):
        rpath = AzureDLPath('/'/test_dir / 'data/single/single'/ 'single.txt')
        ADLDownloader(azure, rpath=rpath, lpath=tempdir)
        assert os.path.isfile(os.path.join(tempdir, 'single.txt'))


@my_vcr.use_cassette
def test_upload_empty_folder(tempdir, azure):
    with azure_teardown(azure):
        os.mkdir(os.path.join(tempdir, "dir1"))
        os.mkdir(os.path.join(tempdir, "dir1", "b"))

        with open(os.path.join(tempdir, "dir1", "file.txt"), 'wb') as f:
            f.write(b'0123456789')

        # transfer client w/ deterministic temporary directory
        from azure.datalake.store.multithread import put_chunk
        client = ADLTransferClient(azure, transfer=put_chunk,
                                   unique_temporary=False)

        # single chunk, empty file
        up = ADLUploader(azure, test_dir / "dir1", os.path.join(tempdir, "dir1") , nthreads=1,
                         overwrite=True)
        assert azure.info(test_dir / "dir1" /"b")['type'] == 'DIRECTORY'
        azure.rm(test_dir / "dir1", recursive=True)


@my_vcr.use_cassette
def test_modify_acl_entries_recursive(azure):
    with setup_tree(azure):
        acluser = AZURE_ACL_TEST_APPID

        def check_acl_perms(path, permission, isdir=False):
            current_acl = azure.get_acl_status(path)
            acl_user_entry = [s for s in current_acl['entries'] if acluser in s]
            print(path, current_acl)
            if isdir:
                assert len(acl_user_entry) == 2
            else:
                assert len(acl_user_entry) == 1
            assert acl_user_entry[0].split(':')[-1] == permission

        files = list(azure.walk(test_dir))
        directories = list(set([x[0] for x in map(os.path.split, files)]))

        permission = "---"
        azure.modify_acl_entries(test_dir, acl_spec="default:user:"+acluser+":"+permission+",user:"+acluser+":"+permission, recursive=True, number_of_sub_process=2)
        for path in files:
            check_acl_perms(path, permission, False)

        for path in directories:
            check_acl_perms(path, permission, True)

        permission = "rwx"
        azure.modify_acl_entries(test_dir, acl_spec="default:user:"+acluser+":"+permission+",user:"+acluser+":"+permission, recursive=True, number_of_sub_process=2)
        for path in files:
            check_acl_perms(path, permission, False)

        for path in directories:
            check_acl_perms(path, permission, True)


@my_vcr.use_cassette
def test_set_acl_recusrive(azure):
    with setup_tree(azure):
        acluser = AZURE_ACL_TEST_APPID
        set_acl_base ="user::rwx,group::rwx,other::---,"

        def check_acl_perms(path, permission):
            current_acl = azure.get_acl_status(path)
            acl_user_entry = [s for s in current_acl['entries'] if acluser in s]
            assert len(acl_user_entry) == 1, "Path: "+path + " Acls: " + str(acl_user_entry)
            assert acl_user_entry[0].split(':')[-1] == permission

        files = list(azure.walk(test_dir))
        directories = list(set([x[0] for x in map(os.path.split, files)]))
        permission = "rwx"
        azure.set_acl(test_dir, acl_spec=set_acl_base + "user:"+acluser+":"+permission, recursive=True, number_of_sub_process=2)
        for path in files+directories:
            check_acl_perms(path, permission)


@my_vcr.use_cassette
def test_remove_acl_entries_recursive(azure):
    with setup_tree(azure):
        acluser = AZURE_ACL_TEST_APPID

        permission = "rwx"
        azure.modify_acl_entries(test_dir, acl_spec="user:"+acluser+":"+permission+",default:user:"+acluser+":"+permission, recursive=True, number_of_sub_process=2)

        files = list(azure.walk(test_dir))
        directories = list(set([x[0] for x in map(os.path.split, files)]))

        for path in files+directories:
            current_acl = azure.get_acl_status(path)
            acl_user_entry= [s for s in current_acl['entries'] if acluser in s]
            assert acl_user_entry != []

        azure.remove_acl_entries(test_dir, acl_spec="user:"+acluser+",default:user:"+acluser, recursive=True, number_of_sub_process=2)

        for path in files+directories:
            current_acl = azure.get_acl_status(path)
            acl_user_entry = [s for s in current_acl['entries'] if acluser in s]
            assert acl_user_entry == []
