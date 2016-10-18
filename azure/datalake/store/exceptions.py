# -*- coding: utf-8 -*-
# coding=utf-8
# --------------------------------------------------------------------------
# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See License.txt in the project root for
# license information.
# --------------------------------------------------------------------------

try:
    FileNotFoundError = FileNotFoundError
except NameError:
    class FileNotFoundError(IOError):
        pass

try:
    FileExistsError = FileExistsError
except NameError:
    class FileExistsError(OSError):
        pass

try:
    PermissionError = PermissionError
except NameError:
    class PermissionError(OSError):
        pass

try:
    ConnectionError = ConnectionError
except NameError:
    class ConnectionError(Exception):
        pass

try:
    ConnectionResetError = ConnectionResetError
except NameError:
    class ConnectionResetError(ConnectionError):
        pass
