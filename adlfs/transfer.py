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
import threading
import time
import uuid

logger = logging.getLogger(__name__)

File = namedtuple('File', 'src dst state nbytes start stop chunks')
Chunk = namedtuple('Chunk', 'name state offset retries')


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

    def contains_none(self, *states):
        return all([len(self._states[state]) == 0 for state in states])

    def __str__(self):
        status = " ".join(
            ["%s=%d" % (s, len(self._states[s])) for s in self._states])
        return "<DisjointState: " + status + ">"

    __repr__ = __str__


class ADLTransferClient(object):
    def __init__(self, adlfs, name, transfer, merge=None, nthreads=None,
                 chunksize=2**26, tmp_path='/tmp', tmp_prefix='part_',
                 tmp_unique=True, persist_path=None, delimiter=None):
        """
        Parameters
        ----------
        adlfs: ADL filesystem instance
        name: str
            Unique ID used for persistence.
        transfer: callable
            Function or callable object invoked when transferring chunks.
        merge: callable [None]
            Function or callable object invoked when merging chunks. If None,
            then merging is skipped.
        nthreads: int [None]
            Number of threads to use. If None, uses the number of cores.
        chunksize: int [2**26]
            Number of bytes in each chunk for splitting big files. Files smaller
            than this number will always be transferred in a single thread.
        tmp_path: str ['/tmp']
            Path used for temporarily storing transferred chunks until chunks
            are gathered into a single file. If None, then each chunk will be
            written into the same file.
        tmp_prefix: str ['part_']
            If given and not None, this is used to provide a prefix to the
            temporary chunk.
        tmp_unique: bool [True]
            If True, then a unique ID will be generated to create a subdirectory
            containing the temporary chunks. Otherwise, all temporary chunks
            will be placed in `tmp_path`.
        persist_path: str [None]
            Path used for persisting a client's state. If None, then `save()`
            and `load()` will be empty operations.
        delimiter: byte(s) or None
            If set, will transfer blocks using delimiters, as well as split
            files for transferring on that delimiter.
        """
        self._adlfs = adlfs
        self._name = name
        self._transfer = transfer
        self._merge = merge
        self._nthreads = nthreads or multiprocessing.cpu_count()
        self._chunksize = chunksize
        self._chunkretries = 5
        self._tmp_path = tmp_path
        self._tmp_prefix = tmp_prefix
        self._tmp_unique = tmp_unique
        self._persist_path = persist_path
        self._pool = ThreadPoolExecutor(self._nthreads)
        self._shutdown_event = threading.Event()
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

    def _submit(self, fn, *args, **kwargs):
        kwargs['shutdown_event'] = self._shutdown_event
        return self._pool.submit(fn, *args, **kwargs)

    def _scatter(self, src, dst, transfer):
        """ Split a given file into chunks """
        dic = self._files[(src, dst)]
        self._fstates[(src, dst)] = 'transferring'
        offsets = list(range(0, dic['nbytes'], self._chunksize))
        for offset in offsets:
            if self._tmp_path and len(offsets) > 1:
                subdir = uuid.uuid1().hex[:10] if self._tmp_unique else ''
                prefix = self._tmp_prefix or ''
                name = os.path.join(
                    self._tmp_path,
                    subdir,
                    prefix + str(offset))
            else:
                name = dst
            logger.debug("Submitted %s, byte offset %d", name, offset)
            dic['cstates'][name] = 'running'
            dic['chunks'][name] = dict(
                future=self._submit(transfer, self._adlfs, src, name, offset,
                                    self._chunksize),
                retries=self._chunkretries,
                offset=offset)

    @property
    def progress(self):
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

    def _status(self, src, dst, nbytes, start, stop):
        elapsed = stop - start
        rate = nbytes / elapsed / 1024 / 1024
        logger.info("Transferred %s -> %s in %f seconds at %f MB/s",
                    src, dst, elapsed, rate)

    def _update(self):
        for (src, dst), dic in self._files.items():
            if self._fstates[(src, dst)] == 'transferring':
                for name in list(dic['chunks']):
                    future = dic['chunks'][name]['future']
                    if not future.done():
                        continue
                    if future.cancelled():
                        dic['cstates'][name] = 'cancelled'
                    elif future.exception():
                        dic['cstates'][name] = 'errored'
                    else:
                        dic['cstates'][name] = 'finished'
                if dic['cstates'].contains_all('finished'):
                    logger.debug("Chunks transferred")
                    chunks = list(dic['chunks'])
                    if self._merge and len(chunks) > 1:
                        logger.debug("Merging file: %s", self._fstates[(src, dst)])
                        self._fstates[(src, dst)] = 'merging'
                        dic['merge'] = self._submit(self._merge, dst, chunks)
                    else:
                        dic['stop'] = time.time()
                        self._fstates[(src, dst)] = 'finished'
                        self._status(src, dst, dic['nbytes'], dic['start'], dic['stop'])
                elif dic['cstates'].contains_none('running'):
                    logger.debug("Transfer failed: %s", dic['cstates'])
                    self._fstates[(src, dst)] = 'errored'
                else:
                    logger.debug("Transferring chunks: %s", dic['cstates'])
            elif self._fstates[(src, dst)] == 'merging':
                future = dic['merge']
                if not future.done():
                    continue
                if future.cancelled():
                    self._fstates[(src, dst)] = 'cancelled'
                elif future.exception():
                    self._fstates[(src, dst)] = 'errored'
                else:
                    dic['stop'] = time.time()
                    self._fstates[(src, dst)] = 'finished'
                    self._status(src, dst, dic['nbytes'], dic['start'], dic['stop'])
        self.save()

    def run(self, nthreads=None, monitor=True, before_scatter=None):
        self._nthreads = nthreads or self._nthreads
        for src, dst in self._files:
            self._files[(src, dst)]['start'] = time.time()
            self._fstates[(src, dst)] = 'transferring'
            if before_scatter:
                before_scatter(self._adlfs, src, dst)
            self._scatter(src, dst, self._transfer)
        if monitor:
            self.monitor()

    def _cancel(self):
        for dic in self._files.values():
            for transfer in dic['chunks'].values():
                transfer['future'].cancel()
            if dic['merge']:
                dic['merge'].cancel()

    def _wait(self, poll=0.1, timeout=0):
        start = time.time()
        while not self._fstates.contains_none('pending', 'transferring', 'merging'):
            if timeout > 0 and time.time() - start > timeout:
                break
            time.sleep(poll)
            self._update()

    def _clear(self):
        for dic in self._files.values():
            for name in dic['chunks']:
                dic['chunks'][name]['future'] = None
            dic['merge'] = None
        self._pool = None

    def shutdown(self):
        self._shutdown_event.set()
        self._cancel()
        self._pool.shutdown(wait=True)
        self._update()

    def monitor(self, poll=0.1, timeout=0):
        """ Wait for download to happen
        """
        try:
            self._wait(poll, timeout)
        except KeyboardInterrupt:
            logger.warning("%s suspended and persisted", self)
            self.shutdown()
        self._clear()
        self.save()

    def __getstate__(self):
        return {'files': self.progress.copy()}

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
