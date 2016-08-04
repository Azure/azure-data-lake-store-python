#!/usr/bin/env python

# -*- coding: utf-8 -*-
# coding=utf-8
# --------------------------------------------------------------------------
# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See License.txt in the project root for
# license information.
# --------------------------------------------------------------------------

"""
An interface to be run from the command line/powershell.

This file is the only executable in the project.
"""

from __future__ import print_function

import argparse
import cmd
from datetime import datetime
import os
import stat
import sys

from adlfs.core import AzureDLFileSystem
from adlfs.lib import auth
from adlfs.multithread import ADLDownloader, ADLUploader


class AzureDataLakeFSCommand(cmd.Cmd, object):
    """Accept commands via an interactive prompt or the command line."""

    prompt = 'azure> '

    def __init__(self):
        super(AzureDataLakeFSCommand, self).__init__()

        self._tenant_id = os.environ['azure_tenant_id']
        self._username = os.environ['azure_username']
        self._password = os.environ['azure_password']
        self._store_name = os.environ['azure_store_name']
        self._token = auth(self._tenant_id, self._username, self._password)
        self._fs = AzureDLFileSystem(self._store_name, self._token)

    def do_close(self, line):
        return True

    def help_close(self):
        print("close\n")
        print("Exit the application")

    def do_cat(self, line):
        parser = argparse.ArgumentParser(prog="cat")
        parser.add_argument('files', type=str, nargs='+')
        args = parser.parse_args(line.split())

        for f in args.files:
            print(self._fs.cat(f))

    def help_cat(self):
        print("cat file ...\n")
        print("Display contents of files")

    def _display_dict(self, d):
        width = max([len(k) for k in d.keys()])
        for k, v in sorted(list(d.items())):
            print("{0:{width}} = {1}".format(k, v, width=width))

    def do_df(self, line):
        self._display_dict(self._fs.df())

    def help_df(self):
        print("df\n")
        print("Display Azure account statistics")

    def _truncate(self, num, fmt):
        return '{:{fmt}}'.format(num, fmt=fmt).rstrip('0').rstrip('.')

    def _format_size(self, num):
        for unit in ['B', 'K', 'M', 'G', 'T']:
            if abs(num) < 1024.0:
                return '{:>3s}{}'.format(self._truncate(num, '3.1f'), unit)
            num /= 1024.0
        return self._truncate(num, '.1f') + 'P'

    def do_du(self, line):
        parser = argparse.ArgumentParser(prog="du")
        parser.add_argument('files', type=str, nargs='*', default=[''])
        parser.add_argument('-r', '--recursive', action='store_true')
        parser.add_argument('-H', '--human-readable', action='store_true')
        args = parser.parse_args(line.split())

        for f in args.files:
            items = sorted(list(self._fs.du(f, deep=args.recursive).items()))
            for name, size in items:
                if args.human_readable:
                    print("{:7s} {}".format(self._format_size(size), name))
                else:
                    print("{:<9d} {}".format(size, name))

    def help_du(self):
        print("du [file ...]\n")
        print("Display disk usage statistics")

    def do_exists(self, line):
        parser = argparse.ArgumentParser(prog="exists")
        parser.add_argument('file', type=str)
        args = parser.parse_args(line.split())

        print(self._fs.exists(args.file))

    def help_exists(self):
        print("exists file\n")
        print("Check if file/directory exists")

    def do_get(self, line):
        parser = argparse.ArgumentParser(prog="get")
        parser.add_argument('files', type=str, nargs='+')
        args = parser.parse_args(line.split())

        if len(args.files) == 2:
            remote_path = args.files[0]
            local_path = args.files[1]
        else:
            remote_path = args.files[0]
            local_path = os.path.basename(args.files[0])
        ADLDownloader(self._fs, remote_path, local_path)

    def help_get(self):
        print("get remote-file [local-file]\n")
        print("Retrieve the remote file and store it locally")

    def do_head(self, line):
        parser = argparse.ArgumentParser(prog="head")
        parser.add_argument('files', type=str, nargs='+')
        parser.add_argument('-c', '--bytes', type=int, default=1024)
        args = parser.parse_args(line.split())

        for f in args.files:
            print(self._fs.head(f, size=args.bytes))

    def help_head(self):
        print("head [-c bytes] file ...\n")
        print("Display first bytes of a file")

    def do_info(self, line):
        parser = argparse.ArgumentParser(prog="info")
        parser.add_argument('files', type=str, nargs='+')
        args = parser.parse_args(line.split())

        for f in args.files:
            self._display_dict(self._fs.info(f))

    def help_info(self):
        print("info file ...\n")
        print("Display file information")

    def _display_item(self, item):
        mode = int(item['permission'], 8)

        if item['type'] == 'DIRECTORY':
            permissions = "d"
        elif item['type'] == 'SYMLINK':
            permissions = "l"
        else:
            permissions = "-"

        permissions += "r" if bool(mode & stat.S_IRUSR) else "-"
        permissions += "w" if bool(mode & stat.S_IWUSR) else "-"
        permissions += "x" if bool(mode & stat.S_IXUSR) else "-"
        permissions += "r" if bool(mode & stat.S_IRGRP) else "-"
        permissions += "w" if bool(mode & stat.S_IWGRP) else "-"
        permissions += "x" if bool(mode & stat.S_IXGRP) else "-"
        permissions += "r" if bool(mode & stat.S_IROTH) else "-"
        permissions += "w" if bool(mode & stat.S_IWOTH) else "-"
        permissions += "x" if bool(mode & stat.S_IXOTH) else "-"

        timestamp = item['modificationTime'] // 1000
        modified_at = datetime.fromtimestamp(timestamp).strftime('%b %d %H:%M')

        print("{} {} {} {:9d} {} {}".format(
            permissions,
            item['owner'][:8],
            item['group'][:8],
            item['length'],
            modified_at,
            os.path.basename(item['name'])))

    def do_ls(self, line):
        parser = argparse.ArgumentParser(prog="ls")
        parser.add_argument('dirs', type=str, nargs='*', default=[''])
        parser.add_argument('-l', '--detail', action='store_true')
        args = parser.parse_args(line.split())

        for d in args.dirs:
            for item in self._fs.ls(d, detail=args.detail):
                if args.detail:
                    self._display_item(item)
                else:
                    print(os.path.basename(item))

    def help_ls(self):
        print("ls [-l | --detail] [file ...]\n")
        print("List directory contents")

    def do_mkdir(self, line):
        parser = argparse.ArgumentParser(prog="mkdir")
        parser.add_argument('dirs', type=str, nargs='+', default=[''])
        args = parser.parse_args(line.split())

        for d in args.dirs:
            self._fs.mkdir(d)

    def help_mkdir(self):
        print("mkdir directory ...\n")
        print("Create directories")

    def do_mv(self, line):
        parser = argparse.ArgumentParser(prog="mv")
        parser.add_argument('files', type=str, nargs='+')
        args = parser.parse_args(line.split())

        self._fs.mv(args.files[0], args.files[1])

    def help_mv(self):
        print("mv from-path to-path\n")
        print("Rename from-path to to-path")

    def do_put(self, line):
        parser = argparse.ArgumentParser(prog="put")
        parser.add_argument('files', type=str, nargs='+')
        args = parser.parse_args(line.split())

        if len(args.files) == 2:
            local_path = args.files[0]
            remote_path = args.files[1]
        else:
            local_path = args.files[0]
            remote_path = os.path.basename(args.files[1])
        ADLUploader(self._fs, remote_path, local_path)

    def help_put(self):
        print("put local-file [remote-file]\n")
        print("Store a local file on the remote machine")

    def do_quit(self, line):
        return True

    def help_quit(self):
        print("quit\n")
        print("Exit the application")

    def do_rm(self, line):
        parser = argparse.ArgumentParser(prog="rm")
        parser.add_argument('files', type=str, nargs='+')
        parser.add_argument('-r', '--recursive', action='store_true')
        args = parser.parse_args(line.split())

        for f in args.files:
            self._fs.rm(f, recursive=args.recursive)

    def help_rm(self):
        print("rm [-r | --recursive] file ...\n")
        print("Remove directory entries")

    def do_rmdir(self, line):
        parser = argparse.ArgumentParser(prog="rmdir")
        parser.add_argument('dirs', type=str, nargs='+', default=[''])
        args = parser.parse_args(line.split())

        for d in args.dirs:
            self._fs.rmdir(d)

    def help_rmdir(self):
        print("rmdir directory ...\n")
        print("Remove directories")

    def do_tail(self, line):
        parser = argparse.ArgumentParser(prog="tail")
        parser.add_argument('files', type=str, nargs='+')
        parser.add_argument('-c', '--bytes', type=int, default=1024)
        args = parser.parse_args(line.split())

        for f in args.files:
            print(self._fs.tail(f, size=args.bytes))

    def help_tail(self):
        print("tail [-c bytes] file ...\n")
        print("Display last bytes of a file")

    def do_touch(self, line):
        parser = argparse.ArgumentParser(prog="touch")
        parser.add_argument('files', type=str, nargs='+')
        args = parser.parse_args(line.split())

        for f in args.files:
            self._fs.touch(f)

    def help_touch(self):
        print("touch file ...\n")
        print("Change file access and modification times")

    def do_EOF(self, line):
        return True


if __name__ == '__main__':
    if len(sys.argv) > 1:
        AzureDataLakeFSCommand().onecmd(' '.join(sys.argv[1:]))
    else:
        AzureDataLakeFSCommand().cmdloop()
