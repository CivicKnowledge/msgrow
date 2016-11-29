# -*- coding: utf-8 -*-
# Copyright (c) 2016 Civic Knowledge. This file is licensed under the terms of the
# MIT License, included in this distribution as LICENSE.txt

"""
"""

import base
import msgpack
import struct
from util import decode_obj

class RowpackReader(object):

    MAGIC = base.MAGIC
    VERSION = base.VERSION
    FILE_HEADER_FORMAT = base.FILE_HEADER_FORMAT
    FILE_HEADER_FORMAT_SIZE = base.FILE_HEADER_FORMAT_SIZE

    def __init__(self, path, mode='rb'):
        self.path = path
        self.mode = mode

        self.magic = self.MAGIC
        self.version = self.VERSION
        self.n_rows = 0
        self.n_cols = 0
        self.data_start = 0
        self.data_end = 0
        self.meta_end = 0

        self._fh = None
        self.unpacker = None

        self.meta = {}

        self.open()

    def open(self):

        if self._fh is None:
            self._fh = open(self.path, self.mode)

            self.read_file_header()

            self.read_meta()


    def close(self):
        if self._fh is not None:

            self._fh.close()
            self._fh = None

    def read_file_header(self):
        try:
            self.magic, self.version, self.n_rows, self.n_cols, self.data_start, self.data_end, self.meta_end = \
                self.FILE_HEADER_FORMAT.unpack(self._fh.read(self.FILE_HEADER_FORMAT_SIZE))
        except struct.error as e:
            raise IOError('Failed to read file header; {}; path = {}'.format(e, self.parent.path))

    def read_meta(self):
        from rowpack import Schema
        import binascii

        curr = self._fh.tell()

        self._fh.seek(self.data_end)

        assert self._fh.tell() == self.data_end

        b = self._fh.read(self.meta_end-self.data_end)

        assert len(b) == self.meta_end-self.data_end

        d = msgpack.unpackb(b, encoding='utf-8')

        self.meta = d['meta']
        self.schema = Schema.from_rows(d['schema'])

        self._fh.seek(curr)

    def read(self, size=None):
        """Read from the compressed section of the file"""

        if size:
            return self._zfh.read(size)
        else:
            return self._zfh.read()

    @property
    def headers(self):
        return self.schema.headers

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

        if exc_val:
            return False

    def _unpacker(self):

        from gzipfile import GzipFile

        self._fh.seek(self.data_start)

        zfh = GzipFile(fileobj=self._fh, compresslevel=9, end_of_data=self.data_end)  # Compressor for writing rows

        unpacker = msgpack.Unpacker(zfh, object_hook=decode_obj,
                                    use_list=False,
                                    encoding='utf-8')

        return zfh, unpacker

    def __iter__(self):

        zfh, unpacker = self._unpacker()

        for rows in unpacker:
            for row in rows:
                yield row

        zfh.close()

