# -*- coding: utf-8 -*-
# coding=utf-8
# --------------------------------------------------------------------------
# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See License.txt in the project root for
# license information.
# --------------------------------------------------------------------------

"""
High performance multi-threaded module to up/download

Calls method in `core` with thread pool executor to ensure the network
is used to its maximum throughput.

Only implements upload and download of (massive) files and directory trees.
"""
from collections import namedtuple
from concurrent.futures import ThreadPoolExecutor
import logging
import multiprocessing
import os
import pickle
import time
import uuid

MAXRETRIES = 5

logger = logging.getLogger(__name__)


class DisjointState(object):
    """
    Collection of disjoint sets containing state about objects.
    """
    def __init__(self, *states):
        self._states = {state: set() for state in states}
        self._objects = {}

    @property
    def states(self):
        return list(self._states)

    @property
    def objects(self):
        return list(self._objects)

    def __iter__(self):
        return iter(self._objects.items())

    def __getitem__(self, obj):
        return self._objects[obj]

    def __setitem__(self, obj, state):
        if obj in self._objects:
            self._states[self._objects[obj]].discard(obj)
        self._states[state].add(obj)
        self._objects[obj] = state

    def contains_all(self, state):
        objs = self._states[state]
        return len(objs) > 0 and len(self.objects) - len(objs) == 0

    def __str__(self):
        status = " ".join(
            ["%s=%d" % (s, len(self._states[s])) for s in self._states])
        return "<DisjointState: " + status + ">"

    __repr__ = __str__


class ADLTransferClient(object):
    def __init__(self, adlfs, name, nthreads=None, chunksize=2**26,
                 transfer_path=None, persist_path=None, delimiter=None):
        """
        Parameters
        ----------
        adlfs: ADL filesystem instance
        name: str
            Unique ID used for persistence.
        nthreads: int [None]
            Number of threads to use. If None, uses the number of cores.
        chunksize: int [2**26]
            Number of bytes in each chunk for splitting big files. Files smaller
            than this number will always be transferred in a single thread.
        transfer_path: str [None]
            Path used for storing transferred chunks until chunks are gathered
            into a single file. If None, then each chunk will be written into
            the same file.
        persist_path: str [None]
            Path used for persisting a client's state. If None, then `save()`
            and `load()` will be empty operations.
        delimiter: byte(s) or None
            If set, will transfer blocks using delimiters, as well as split
            files for transferring on that delimiter.
        """
        self._adlfs = adlfs
        self._name = name
        self._nthreads = nthreads or multiprocessing.cpu_count()
        self._chunksize = chunksize
        self._chunkretries = 5
        self._transfer_path = transfer_path
        self._persist_path = persist_path
        self._pool = ThreadPoolExecutor(self._nthreads)
        self._files = {}
        self._fstates = DisjointState(
            'pending', 'transferring', 'merging', 'finished', 'cancelled',
            'errored')

    def submit(self, src, dst, nbytes):
        """
        All submitted files start in the `pending` state until `run()` is
        called.
        """
        self._fstates[(src, dst)] = 'pending'
        self._files[(src, dst)] = dict(
            nbytes=nbytes,
            start=None,
            stop=None,
            chunks={},
            cstates=DisjointState('running', 'finished', 'cancelled', 'errored'),
            merge=None)

    def _scatter(self, src, dst, transfer):
        """ Split a given file into chunks """
        dic = self._files[(src, dst)]
        self._fstates[(src, dst)] = 'transferring'
        for offset in list(range(0, dic['nbytes'], self._chunksize)):
            if self._transfer_path:
                name = os.path.join(
                    self._transfer_path,
                    uuid.uuid1().hex[:10] + "_%i" % offset)
            else:
                name = dst
            dic['cstates'][name] = 'running'
            dic['chunks'][name] = dict(
                future=self._pool.submit(transfer, self._adlfs, src, dst,
                                         offset, self._chunksize),
                retries=self._chunkretries,
                offset=offset)

    @property
    def progress(self):
        Chunk = namedtuple('Chunk', 'name state offset retries')
        File = namedtuple('File', 'src dst state nbytes start stop chunks')
        files = []
        for key in self._files:
            src, dst = key
            chunks = []
            for name in self._files[key]['chunks']:
                chunks.append(Chunk(
                    name=name,
                    state=self._files[key]['cstates'][name],
                    offset=self._files[key]['chunks'][name]['offset'],
                    retries=self._files[key]['chunks'][name]['retries']))
            files.append(File(
                src=src,
                dst=dst,
                state=self._fstates[key],
                nbytes=self._files[key]['nbytes'],
                start=self._files[key]['start'],
                stop=self._files[key]['stop'],
                chunks=chunks))
        return files

    def _update(self):
        for (src, dst), dic in self._files.items():
            if self._fstates[(src, dst)] == 'transferring':
                for name in list(dic['chunks']):
                    future = dic['chunks'][name]['future']
                    if future.done() and not future.cancelled():
                        dic['cstates'][name] = 'finished'
                    # if cancelled, retry if possible and decrement retries
                    # elif if exception, add to errored set
                    # else, add to cancelled set
                if dic['cstates'].contains_all('finished'):
                    # merge if possible
                    if self._merge:
                        self._fstates[(src, dst)] = 'merging'
                        chunks = [os.path.join(self._transfer_path, name) for name in list(dic['chunks'])]
                        dic['merge'] = self._pool.submit(self._merge, dst, chunks)
                    else:
                        dic['stop'] = time.time()
                        self._fstates[(src, dst)] = 'finished'
            elif self._fstates[(src, dst)] == 'merging':
                future = dic['merge']
                if future.done() and not future.cancelled():
                    logger.debug('File downloaded (%s -> %s)' % (src, dst))
                    dic['stop'] = time.time()
                    self._fstates[(src, dst)] = 'finished'
                # if cancelled, retry if possible and decrement retries
                # elif if exception, add to errored set
                # else, add to cancelled set
        self.save()

    def run(self, transfer, merge=None, nthreads=None, monitor=True, before_scatter=None):
        self._merge = merge
        self._nthreads = nthreads or self._nthreads
        for src, dst in self._files:
            self._files[(src, dst)]['start'] = time.time()
            self._fstates[(src, dst)] = 'transferring'
            if before_scatter:
                before_scatter(self._adlfs, src, dst)
            self._scatter(src, dst, transfer)
        if monitor:
            self.monitor()

    def _cancel(self):
        for dic in self._files.values():
            for transfer in dic['chunks'].values():
                transfer['future'].cancel()

    def _wait(self, poll=0.1, timeout=0):
        # loop until all files are transferred or timeout expires
        start = time.time()
        while not self._fstates.contains_all('finished'):
            if timeout > 0 and time.time() - start > timeout:
                break
            time.sleep(poll)
            self._update()

    def _shutdown(self, wait=True):
        self._pool.shutdown(wait)

    def _clear(self):
        for dic in self._files.values():
            for name in dic['chunks']:
                dic['chunks'][name]['future'] = None
        self._pool = None

    def monitor(self, poll=0.1, timeout=0):
        """ Wait for download to happen
        """
        try:
            self._wait(poll, timeout)
        except KeyboardInterrupt:
            logger.warning("%s suspended and persisted", self)
            self._cancel()
            self._shutdown(wait=True)
            self._update()
        self._clear()
        self.save()

    def __getstate__(self):
        return self.progress

    @staticmethod
    def load(filename):
        try:
            return pickle.load(open(filename, 'rb'))
        except:
            return {}

    def save(self, keep=True):
        if self._persist_path:
            all_downloads = self.load(self._persist_path)
            if not self._fstates.contains_all('finished') and keep:
                all_downloads[self._name] = self
            else:
                all_downloads.pop(self._name, None)
            with open(self._persist_path, 'wb') as f:
                pickle.dump(all_downloads, f)
