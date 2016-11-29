# -*- coding: utf-8 -*-
# Copyright (c) 2016 Civic Knowledge. This file is licensed under the terms of the
# MIT License, included in this distribution as LICENSE.txt

"""
"""

import os
import stat

import time
import zlib
import msgpack
from six import iteritems, text_type
from functools import reduce

from gzipfile import GzipFile

import base
from os.path import exists

MAX_CACHE = 10000

class RowpackWriter(object):
    MAGIC = base.MAGIC
    VERSION = base.VERSION
    FILE_HEADER_FORMAT = base.FILE_HEADER_FORMAT
    FILE_HEADER_FORMAT_SIZE = base.FILE_HEADER_FORMAT_SIZE

    def __init__(self, path,  mode='wb', schema=None, meta=None):

        self.path = path

        self.mode = mode

        self.meta = meta if meta is not None else {}

        self.schema = schema

        self.magic = self.MAGIC
        self.version = self.VERSION
        self.n_rows = 0
        self.n_cols = 0
        self.data_start = 0
        self.data_end = 0
        self.meta_end = 0

        self.writable = False

        self._fh = None
        self._zfh = None

        self.open()

        self.cache = []

    def open(self):

        if self._fh is None:

            if self.mode.startswith('r+') and exists(self.path):
                from reader import RowpackReader
                with RowpackReader(self.path) as r:
                    self.data_start = r.data_start
                    self.data_end = r.data_end
                    self.meta_end = r.meta_end
                    self.n_rows = r.n_rows
                    self.n_cols = r.n_cols

                    self.schema = r.schema
                    self.meta = r.meta

                self._fh = open(self.path, self.mode)

            else:
                self._fh = open(self.path, self.mode)
                self.write_file_header() # Writes mostly empty header. Will re-write later.

                self.data_start = self._fh.tell()
                self.data_end = self._fh.tell()

                self.writable = True

                self._zfh = GzipFile(fileobj=self._fh, compresslevel=9)  # Compressor for writing rows



    def close_zfh(self):

        if self._zfh is not None:
            self._zfh.close()
            self._zfh = None

            self.data_end = self._fh.tell() # Closing Gzip writes mroe data

    def close(self):

        if self._fh is not None:

            self.flush()

            self.close_zfh()

            self.write_meta() # Seeks to end of file

            self.write_file_header() # Seeks to start of file

            self._fh.close()
            self._fh = None


    def write_file_header(self):
        """Write the magic number, version and the file_header dictionary.  """

        magic = self.magic
        if isinstance(magic, text_type):
            magic = magic.encode('utf-8')

        if self.schema:
            self.n_cols = len(self.schema.headers)

        self._fh.seek(0)

        hdf = self.FILE_HEADER_FORMAT.pack(magic, self.VERSION, self.n_rows,self.n_cols,
                                           self.data_start, self.data_end, self.meta_end)

        assert len(hdf) == self.FILE_HEADER_FORMAT_SIZE

        self._fh.write(hdf)

        assert self._fh.tell() == self.FILE_HEADER_FORMAT_SIZE, (self._fh.tell(), self.FILE_HEADER_FORMAT_SIZE)

    def write_meta(self):

        self.flush()

        self._fh.seek(self.data_end)

        d = {
            'meta': self.meta if self.meta else {},
            'schema': self.schema.to_rows() if self.schema else []
        }

        b = msgpack.packb(d, encoding='utf-8')

        self._fh.write(b)

        self.meta_end = self._fh.tell()

        self.writable = False


    def write_row(self, row):
        """Store a single row in the cache, to be written later"""
        self.cache.append(row)

        if len(self.cache) > MAX_CACHE:
            self.flush()

    def write_rows(self, rows):
        """Write a block of rows"""
        from util import encode_obj
        from .exceptions import RowpackError

        if not self.writable:
            raise RowpackError("Can't write to existing file; can only update metadata" )

        self.n_rows += len(rows)
        self._zfh.write(msgpack.packb(rows, default=encode_obj, encoding='utf-8'))


    def flush(self):

        if self.cache:
            self.write_rows(self.cache)
            self.cache = []


    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

        if exc_val:
            return False

