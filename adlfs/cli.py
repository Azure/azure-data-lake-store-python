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
        """close
        Exit the application"""
        return True

    def do_cat(self, line):
        """cat file ...
        Display contents of files"""
        parser = argparse.ArgumentParser(prog="cat")
        parser.add_argument('files', type=str, nargs='+')
        args = parser.parse_args(line.split(' '))

        for f in args.files:
            print(self._fs.cat(f))

    def _display_dict(self, d):
        width = max([len(k) for k in d.keys()])
        for k, v in sorted(list(d.items())):
            print("{0:{width}} = {1}".format(k, v, width=width))

    def do_df(self, line):
        """df
        Display Azure account statistics"""
        self._display_dict(self._fs.df())

    def do_du(self, line):
        """du [file ...]
        Display disk usage statistics"""
        parser = argparse.ArgumentParser(prog="du")
        parser.add_argument('files', type=str, nargs='*', default=[''])
        args = parser.parse_args(line.split(' '))

        for f in args.files:
            for name, size in sorted(list(self._fs.du(f).items())):
                print("{:<9d} {}".format(size, os.path.basename(name)))

    def do_get(self, line):
        """get remote-file [local-file]
        Retrieve the remote file and store it locally"""
        parser = argparse.ArgumentParser(prog="get")
        parser.add_argument('files', type=str, nargs='+')
        args = parser.parse_args(line.split(' '))

        if len(args.files) == 2:
            remote_path = args.files[0]
            local_path = args.files[1]
        else:
            remote_path = args.files[0]
            local_path = os.path.basename(args.files[0])
        ADLDownloader(self._fs, remote_path, local_path)

    def do_head(self, line):
        """head [-c bytes] file ...
        Display first bytes of a file"""
        parser = argparse.ArgumentParser(prog="head")
        parser.add_argument('files', type=str, nargs='+')
        parser.add_argument('-c', '--bytes', type=int, default=1024)
        args = parser.parse_args(line.split(' '))

        for f in args.files:
            print(self._fs.head(f, size=args.bytes))

    def do_info(self, line):
        """info file ...
        Display file information"""
        parser = argparse.ArgumentParser(prog="info")
        parser.add_argument('files', type=str, nargs='+')
        args = parser.parse_args(line.split(' '))

        for f in args.files:
            self._display_dict(self._fs.info(f))

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
        """ls [-l | --detail] [file ...]
        List directory contents"""
        parser = argparse.ArgumentParser(prog="ls")
        parser.add_argument('dirs', type=str, nargs='*', default=[''])
        parser.add_argument('-l', '--detail', action='store_true')
        args = parser.parse_args(line.split(' '))

        for d in args.dirs:
            for item in self._fs.ls(d, detail=args.detail):
                if args.detail:
                    self._display_item(item)
                else:
                    print(item)

    def do_mkdir(self, line):
        """mkdir directory ...
        Create directories"""
        parser = argparse.ArgumentParser(prog="mkdir")
        parser.add_argument('dirs', type=str, nargs='+', default=[''])
        args = parser.parse_args(line.split(' '))

        for d in args.dirs:
            self._fs.mkdir(d)

    def do_mv(self, line):
        """mv from-path to-path
        Rename from-path to to-path"""
        parser = argparse.ArgumentParser(prog="mv")
        parser.add_argument('files', type=str, nargs='+')
        args = parser.parse_args(line.split(' '))

        self.fs.mv(args.files[0], args.files[1])

    def do_put(self, line):
        """put local-file [remote-file]
        Store a local file on the remote machine"""
        parser = argparse.ArgumentParser(prog="put")
        parser.add_argument('files', type=str, nargs='+')
        args = parser.parse_args(line.split(' '))

        if len(args.files) == 2:
            local_path = args.files[0]
            remote_path = args.files[1]
        else:
            local_path = args.files[0]
            remote_path = os.path.basename(args.files[1])
        ADLUploader(self._fs, remote_path, local_path)

    def do_quit(self, line):
        """quit
        Exit the application"""
        return True

    def do_rm(self, line):
        """rm file ...
        Remove directory entries"""
        parser = argparse.ArgumentParser(prog="rm")
        parser.add_argument('files', type=str, nargs='+')
        args = parser.parse_args(line.split(' '))

        for f in args.files:
            self._fs.rm(f)

    def do_rmdir(self, line):
        """rmdir directory ...
        Remove directories"""
        parser = argparse.ArgumentParser(prog="rmdir")
        parser.add_argument('dirs', type=str, nargs='+', default=[''])
        args = parser.parse_args(line.split(' '))

        for d in args.dirs:
            self._fs.rmdir(d)

    def do_tail(self, line):
        """tail [-c bytes] file ...
        Display last bytes of a file"""
        parser = argparse.ArgumentParser(prog="tail")
        parser.add_argument('files', type=str, nargs='+')
        parser.add_argument('-c', '--bytes', type=int, default=1024)
        args = parser.parse_args(line.split(' '))

        for f in args.files:
            print(self._fs.tail(f, size=args.bytes))

    def do_EOF(self, line):
        return True


if __name__ == '__main__':
    if len(sys.argv) > 1:
        AzureDataLakeFSCommand().onecmd(' '.join(sys.argv[1:]))
    else:
        AzureDataLakeFSCommand().cmdloop()
