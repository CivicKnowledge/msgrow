# -*- coding: utf-8 -*-
# Copyright (c) 2016 Civic Knowledge. This file is licensed under the terms of the
# MIT License, included in this distribution as LICENSE.txt

"""
"""

import base
from base import MPRowsFile
import msgpack
from gzip import GzipFile
import six
import struct

class RowpackReader(object):

    MAGIC = base.MAGIC
    VERSION = base.VERSION
    FILE_HEADER_FORMAT = base.FILE_HEADER_FORMAT
    FILE_HEADER_FORMAT_SIZE = base.FILE_HEADER_FORMAT_SIZE

    def __init__(self, path, mode='rb', compress=True):
        self.path = path
        self.mode = mode

        self.magic = self.MAGIC
        self.version = self.VERSION
        self.n_rows = 0
        self.n_cols = 0
        self.data_start = 0
        self.data_end = 0
        self._compress = compress

        self._fh = None
        self._zfh = None
        self.unpacker = None

        self.open()

    def open(self):
        from util import decode_obj

        if self._fh is None:
            self._fh = open(self.path, self.mode)

            self.read_file_header()
            self.read_schema()

            # Creating the GzipFile object will also write the Gzip header, about 21 bytes of data.
            if self._compress:
                self._zfh = GzipFile(fileobj=self._fh, compresslevel=9)  # Compressor for writing rows
            else:
                self._zfh = self._fh

            self.unpacker = msgpack.Unpacker(self._zfh, object_hook=decode_obj,
                                             use_list=False,
                                             encoding='utf-8')

    def read_file_header(self):
        try:
            self.magic, self.version, self.n_rows, self.n_cols, self.data_start, self.data_end = \
                self.FILE_HEADER_FORMAT.unpack(self._fh.read(self.FILE_HEADER_FORMAT_SIZE))
        except struct.error as e:
            raise IOError('Failed to read file header; {}; path = {}'.format(e, self.parent.path))

    def read_schema(self):
        from rowpack import Schema

        self._fh.seek(self.FILE_HEADER_FORMAT_SIZE)

        rows = msgpack.unpackb(self._fh.read(self.data_start-self.FILE_HEADER_FORMAT_SIZE),
                               encoding='utf-8')

        self.schema = Schema.from_rows(rows)

    def read(self, size=None):
        """Read from the compressed section of the file"""

        if size:
            return self._zfh.read(size)
        else:
            return self._zfh.read()


    def __iter__(self):

        self._fh.seek(self.data_start)

        for rows in self.unpacker:
            for row in rows:
                yield row


class MPRReader(object):
    """
    Read an MPR file

    """
    MAGIC = base.MAGIC
    VERSION = base.VERSION
    FILE_HEADER_FORMAT = base.FILE_HEADER_FORMAT
    FILE_HEADER_FORMAT_SIZE = base.FILE_HEADER_FORMAT_SIZE


    def __init__(self, parent, fh, compress=True):
        """Reads the file_header and prepares for iterating over rows"""

        self.parent = parent
        self._fh = fh
        self._compress = compress
        self._headers = None
        self.data_start = 0
        self.meta_start = 0
        self.data_start_row = 0
        self.data_end_row = 0

        self.pos = 0  # Row position for next read, starts at 1, since header is always 0

        self.n_rows = 0
        self.n_cols = 0

        self._in_iteration = False

        MPRowsFile.read_file_header(self, self._fh)

        try:
            self.data_start = int(self._fh.tell())

            assert self.data_start == self.FILE_HEADER_FORMAT_SIZE
        except AttributeError:
            # The pyfs HTTP filesystem doesn't have tell()
            self.data_start = self.FILE_HEADER_FORMAT_SIZE

        if self._compress:
            self._zfh = GzipFile(fileobj=self._fh, end_of_data=self.meta_start)
        else:
            self._zfh = self._fh

        self.unpacker = msgpack.Unpacker(self._zfh, object_hook=MPRowsFile.decode_obj,
                                         use_list=False,
                                         encoding='utf-8')

        self._meta = None

    @property
    def path(self):
        return self.parent.path

    @property
    def syspath(self):
        return self.parent.syspath

    @property
    def info(self):
        return MPRowsFile._info(self)

    @property
    def meta(self):

        if self._meta is None:

            # Using the _fh b/c I suspect that the GzipFile attached to self._zfh has state that would
            # get screwed up if you read from a new position
            self._meta = MPRowsFile.read_meta(self, self._fh)

        return self._meta

    @property
    def is_finalized(self):
        try:
            return self.meta['process']['finalized']
        except KeyError:  # Old version, doesn't have 'process' key
            return False

    @property
    def columns(self):
        """Return columns."""
        return MPRowsFile._columns(self)

    @property
    def headers(self):
        """Return the headers (column names)."""
        return [e.name for e in MPRowsFile._columns(self)]

    @property
    def raw(self):
        """A raw iterator, which ignores the data start and stop rows and returns all rows, as rows"""

        self._fh.seek(self.data_start)

        try:
            self._in_iteration = True

            for rows in self.unpacker:
                for row in rows:
                    yield row
                    self.pos += 1

        finally:
            self._in_iteration = False
            self.close()

    @property
    def meta_raw(self):
        """self self.raw interator, but returns a tuple with the rows classified"""

        rs = self.meta['row_spec']

        hr = rs['header_rows'] or []
        cr = rs['comment_rows'] or []
        sr = rs['start_row'] or self.data_start_row
        er = rs['end_row'] or self.data_end_row

        for i, row in enumerate(self.raw):

            if i in hr:
                label = 'H'
            elif i in cr:
                label = 'C'
            elif sr <= i <= er:
                label = 'D'
            else:
                label = 'B'

            yield (i, self.pos, label), row

    @property
    def rows(self):
        """Iterator for reading rows"""

        self._fh.seek(self.data_start)

        _ = self.headers  # Get the header, but don't return it.

        try:
            self._in_iteration = True

            while True:
                for row in next(self.unpacker):
                    if self.data_start_row <= self.pos <= self.data_end_row:
                        yield row

                    self.pos += 1

        finally:
            self._in_iteration = False

    def _get_row_proxy(self):
        from ambry_sources.sources import RowProxy, GeoRowProxy
        if 'geometry' in self.headers:
            rp = GeoRowProxy(self.headers)
        else:
            rp = RowProxy(self.headers)

        return rp

    def __iter__(self):
        """Iterator for reading rows as RowProxy objects

        WARNING: This routine returns RowProxy objects. RowProxy objects
        are reused, so if you construct a list directly from the output from this method, the list will have
        multiple copies of a single RowProxy, which will have as an inner row the last result row. If you will
        be directly constructing a list, use a getter that extracts the inner row, or which converted the RowProxy
        to a dict.

        """

        self._fh.seek(self.data_start)

        rp = self._get_row_proxy()

        try:
            self._in_iteration = True
            while True:
                rows = next(self.unpacker)

                for row in rows:
                    if self.data_start_row <= self.pos <= self.data_end_row:
                        yield rp.set_row(row)

                    self.pos += 1

        finally:
            self._in_iteration = False

    def select(self, predicate=None, headers=None):
        """
        Select rows from the reader using a predicate to select rows and and itemgetter to return a
        subset of elements
        :param predicate: If defined, a callable that is called for each row and if it returns true, the
        row is included in the output.
        :param getter: If defined, a list or tuple of header names to return from each row
        :return: iterable of results

        WARNING: This routine works from the reader iterator, which returns RowProxy objects. RowProxy objects
        are reused, so if you construct a list directly from the output from this method, the list will have
        multiple copies of a single RowProxy, which will have as an inner row the last result row. If you will
        be directly constructing a list, use a getter that extracts the inner row, or which converted the RowProxy
        to a dict:

            list(s.datafile.select(lambda r: r.stusab == 'CA' ))

        """

        if headers:

            from operator import itemgetter

            ig = itemgetter(*headers)

            rp = self._get_row_proxy()

            getter = lambda r: rp.set_row(ig(r.dict))

        else:

            getter = None

        if getter is not None and predicate is not None:
            return six.moves.map(getter, filter(predicate, iter(self)))

        elif getter is not None and predicate is None:
            return six.moves.map(getter, iter(self))

        elif getter is None and predicate is not None:
            return six.moves.filter(predicate, self)

        else:
            return iter(self)

    def close(self):
        if self._fh:
            self.meta  # In case caller wants to read mea after close.
            self._fh.close()
            self._fh = None
            if self.parent:
                self.parent._reader = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

        if exc_val:
            return False
