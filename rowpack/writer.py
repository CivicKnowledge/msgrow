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
from ambry_sources.util import get_perm, is_group_readable

import base
from base import MPRowsFile

MAX_CACHE = 100

class RowpackWriter(object):
    MAGIC = base.MAGIC
    VERSION = base.VERSION
    FILE_HEADER_FORMAT = base.FILE_HEADER_FORMAT
    FILE_HEADER_FORMAT_SIZE = base.FILE_HEADER_FORMAT_SIZE

    def __init__(self, path, schema, mode='wb', compress=True):

        self.path = path
        self.mode = mode
        self.schema = schema
        self._compress = compress

        self.magic = self.MAGIC
        self.version = self.VERSION
        self.n_rows = 0
        self.n_cols = 0
        self.data_start = 0
        self.data_end = 0

        self._fh = None
        self._zfh = None

        self.open()

        self.cache = []

    def open(self):

        if self._fh is None:
            self._fh = open(self.path, self.mode)

            self.write_file_header() # Writes mostly empty header. Will re-write later.
            self.write_schema()

            self.data_start = self._fh.tell()

            # Creating the GzipFile object will also write the Gzip header, about 21 bytes of data.
            if self._compress:
                self._zfh = GzipFile(fileobj=self._fh, compresslevel=9)  # Compressor for writing rows
            else:
                self._zfh = self._fh

    def close(self):
        if self._fh is not None:

            self.flush()

            if self._compress and self._zfh:
                self._zfh.close()

            self._zfh = None

            self.data_end = self._fh.tell()

            self.write_file_header() # Seeks to start of file

            self._fh.close()
            self._fh = None

    def write_file_header(self):
        """Write the magic number, version and the file_header dictionary.  """

        magic = self.magic
        if isinstance(magic, text_type):
            magic = magic.encode('utf-8')

        hdf = self.FILE_HEADER_FORMAT.pack(magic, self.VERSION, self.n_rows,self.n_cols, self.data_start, self.data_end)

        assert len(hdf) == self.FILE_HEADER_FORMAT_SIZE

        self._fh.seek(0)

        self._fh.write(hdf)

        assert self._fh.tell() == self.FILE_HEADER_FORMAT_SIZE, (self._fh.tell(), self.FILE_HEADER_FORMAT_SIZE)

    def write_schema(self):

        self._fh.seek(self.FILE_HEADER_FORMAT_SIZE)

        self._fh.write(msgpack.packb(self.schema.to_rows(), default=MPRowsFile.encode_obj, encoding='utf-8'))

    def write(self, s):
        """Write to the compressed section of the file"""
        return self._zfh.write(s)

    def write_row(self, row):

        self.cache.append(row)

        if len(self.cache) > MAX_CACHE:
            self.flush()

    def write_rows(self, rows):
        from util import encode_obj
        self._zfh.write(msgpack.packb(rows, default=encode_obj, encoding='utf-8'))

    def flush(self):

        if self.cache:
            self.write_rows(self.cache)
            self.cache = []



class MPRWriter(object):

    MAGIC = MPRowsFile.MAGIC
    VERSION = MPRowsFile.VERSION
    FILE_HEADER_FORMAT = MPRowsFile.FILE_HEADER_FORMAT
    FILE_HEADER_FORMAT_SIZE = MPRowsFile.FILE_HEADER_FORMAT.size
    META_TEMPLATE = MPRowsFile.META_TEMPLATE
    SCHEMA_TEMPLATE = MPRowsFile.SCHEMA_TEMPLATE

    # In most tests, the block size doesn't matter much, with 1000 row blocks having the same performance of
    # 10 row blocks. This seems to be because for the test rows, the cost of managing the cache is similar to the
    # cost of writing.
    # There is, however, a very large gain from writing a collection of rows as a single block with insert_rows()

    BLOCK_SIZE = 1000  # Size of blocks of rows to write

    def __init__(self, parent, fh, compress=True):

        from copy import deepcopy
        import re

        assert fh

        self.parent = parent
        self._fh = fh
        self._compress = compress

        self._zfh = None  # Compressor for writing rows
        self.version = self.VERSION
        self.magic = self.MAGIC
        self.data_start = self.FILE_HEADER_FORMAT_SIZE
        self.meta_start = 0
        self.data_start_row = 0
        self.data_end_row = None

        self.n_rows = 0
        self.n_cols = 0

        self.cache = []

        try:
            #  Try to read an existing file
            MPRowsFile.read_file_header(self, self._fh)

            self._fh.seek(self.meta_start)

            data = self._fh.read()

            self.meta = msgpack.unpackb(zlib.decompress(data), encoding='utf-8')

            self._fh.seek(self.meta_start)

        except IOError:
            # No, doesn exist, or is corrupt
            self._fh.seek(0)

            self.meta_start = self.data_start

            self.meta = deepcopy(self.META_TEMPLATE)

            self.write_file_header()  # Get moved to the start of row data.

        # Creating the GzipFile object will also write the Gzip header, about 21 bytes of data.
        if self._compress:
            self._zfh = GzipFile(fileobj=self._fh, compresslevel=9)  # Compressor for writing rows
        else:
            self._zfh = self._fh

        self.header_mangler = lambda name: re.sub('_+', '_', re.sub('[^\w_]', '_', name.strip()).lower()).rstrip('_')

        if self.n_rows == 0:
            self.meta['about']['create_time'] = time.time()

    @property
    def info(self):
        return MPRowsFile._info(self)

    @property
    def path(self):
        return self.parent.path

    @property
    def syspath(self):
        return self.parent.syspath

    def set_col_val(name_or_pos, **kwargs):
        pass

    @property
    def headers(self):
        """Return the headers rows

        """
        return [e.name for e in MPRowsFile._columns(self)]

    @headers.setter
    def headers(self, headers):
        """Set column names"""

        if not headers:
            return

        assert isinstance(headers,  (tuple, list)), headers

        for i, row in enumerate(MPRowsFile._columns(self, len(headers))):
            try:
                row.name = headers[i]
            except KeyError:
                row.name = 'col{}'.format(i)

        assert self.meta['schema'][0] == MPRowsFile.SCHEMA_TEMPLATE

    @property
    def columns(self):
        """Return the headers rows

        """
        return MPRowsFile._columns(self)

    @columns.setter
    def columns(self, headers):

        for i, row in enumerate(MPRowsFile._columns(self, len(headers))):

            h = headers[i]

            if isinstance(h, dict):
                raise NotImplementedError()
            else:
                row.name = h if h else 'column{}'.format(i)

    def column(self, name_or_pos):

        for h in self.columns:

            if name_or_pos == h.pos or name_or_pos == h.name:
                return h

        raise KeyError("Didn't find '{}' as either a name nor a position in file '{}' "
                       .format(name_or_pos, self.path))

    def _write_rows(self, rows=None):

        rows, clear_cache = (self.cache, True) if not rows else (rows, False)

        if not rows:
            return

        try:
            self._zfh.write(msgpack.packb(rows, default=MPRowsFile.encode_obj, encoding='utf-8'))
        except IOError as e:
            raise IOError("Can't write row to file '{}': {}".format(self.syspath, e))

        # Hope that the max # of cols is found in the first 100 rows
        # FIXME! This won't work if rows is an interator.
        self.n_cols = reduce(max, (len(e) for e in rows[:100]), self.n_cols)

        if clear_cache:
            self.cache = []

        self._fix_permissions()

    def _fix_permissions(self):
        """ Adds read permission to each directory in the mpr path to user group.

        Note:
            This is the required thing for postgres FDW. Also you need to add postgres system user to group of
            the user who executes ambry_sources.

        """
        syspath = self.syspath
        if syspath:
            parts = syspath.split(os.sep)
            parts[0] = os.sep
            for i, dir_ in enumerate(parts):
                if dir_ == '/':
                    continue
                path = parts[:i]
                path.append(dir_)
                path = os.path.join(*path)
                if not is_group_readable(path):
                    os.chmod(path, get_perm(path) | stat.S_IRGRP | stat.S_IXGRP)

    def insert_row(self, row):

        self.n_rows += 1

        self.cache.append(row)

        if True or len(self.cache) >= self.BLOCK_SIZE:
            self._write_rows()

    def insert_rows(self, rows):
        """ Insert a list of rows. Don't insert iterators. """

        self.n_rows += len(rows)

        self._write_rows(rows)

    def load_rows(self, source, callback=None, limit=None):
        """Load rows from an iterator"""

        for i, row in enumerate(iter(source), 1):
            self.insert_row(row)
            if callback:
                callback(i)
            if limit and i > limit:
                break

        self._write_rows()

        # If the source has a headers property, and it's defined, then
        # use it for the headers. This often has to be called after iteration, because
        # the source may have the header as the first row
        try:
            if source.headers:
                self.headers = [self.header_mangler(h) for h in source.headers]

        except AttributeError:
            pass

    def finalize(self):
        """Mark the loading of the file as finished. """
        self.meta['process']['finalized'] = True

    @property
    def is_finalized(self):
        return self.meta['process']['finalized'] is True

    def close(self):

        if self._fh:

            self._write_rows()

            # First close the Gzip file, so it can flush, etc.

            if self._compress and self._zfh:
                self._zfh.close()

            self._zfh = None

            self.meta_start = self._fh.tell()

            self.write_file_header()
            self._fh.seek(self.meta_start)

            self.write_meta()

            self._fh.close()
            self._fh = None

            if self.parent:
                self.parent._writer = None

    def write_file_header(self):
        """Write the magic number, version and the file_header dictionary.  """
        MPRowsFile.write_file_header(self, self._fh)

    def write_meta(self):
        MPRowsFile.write_meta(self, self._fh)

    def set_types(self, ti):
        """ Set Types from a type intuiter object. """

        results = {int(r['position']): r for r in ti._dump()}
        for i in range(len(results)):

            for k, v in iteritems(results[i]):
                k = {'count': 'type_count'}.get(k, k)
                self.column(i + 1)[k] = v

            if not self.column(i + 1).type:
                self.column(i + 1).type = results[i]['resolved_type']

    def set_stats(self, stats):
        """Copy stats into the schema"""

        for name, stat_set in iteritems(stats.dict):
            row = self.column(name)

            for k, v in iteritems(stat_set.dict):
                k = {'count': 'stat_count'}.get(k, k)
                row[k] = v

    def set_source_spec(self, spec):
        """Set the metadata coresponding to the SourceSpec, excluding the row spec parts. """

        ms = self.meta['source']

        ms['url'] = spec.url
        ms['fetch_time'] = spec.download_time
        ms['file_type'] = spec.filetype
        ms['url_type'] = spec.urltype
        ms['encoding'] = spec.encoding

        me = self.meta['excel']
        me['workbook'] = spec.segment

        if spec.columns:

            for i, sc in enumerate(spec.columns, 1):
                c = self.column(i)

                if c.name:
                    assert self.header_mangler(sc.name) == c.name, \
                        '`{}` column name from spec does not match to `{}` column'.format(sc.name, c.name)

                c.start = sc.start
                c.width = sc.width

    def set_row_spec(self, ri_or_ss):
        """Set the row spec and schema from a RowIntuiter object or a SourceSpec"""

        from itertools import islice
        from operator import itemgetter
        from ambry_sources.intuit import RowIntuiter

        def set_descriptions(w, descriptions):

            for c, d in zip(w.columns, descriptions):
                col = w.column(c.name)
                d = d.replace('\n', ' ').replace('\r', ' ')
                col.description = d

        if isinstance(ri_or_ss, RowIntuiter):
            ri = ri_or_ss

            with self.parent.writer as w:

                w.data_start_row = ri.start_line
                w.data_end_row = ri.end_line if ri.end_line else None

                w.meta['row_spec']['header_rows'] = ri.header_lines
                w.meta['row_spec']['comment_rows'] = ri.comment_lines
                w.meta['row_spec']['start_row'] = ri.start_line
                w.meta['row_spec']['end_row'] = ri.end_line
                w.meta['row_spec']['data_pattern'] = ri.data_pattern_source

                set_descriptions(w, [h for h in ri.headers])

                w.headers = [self.header_mangler(h) for h in ri.headers]

        else:
            ss = ri_or_ss

            with self.parent.reader as r:
                # If the header lines are specified, we need to also coalesce them ad
                # set the header
                if ss.header_lines:

                    max_header_line = max(ss.header_lines)
                    rows = list(islice(r.raw, max_header_line + 1))

                    header_lines = itemgetter(*ss.header_lines)(rows)

                    if not isinstance(header_lines[0], (list, tuple)):
                        header_lines = [header_lines]

                else:
                    header_lines = None

            with self.parent.writer as w:

                w.data_start_row = ss.start_line
                w.data_end_row = ss.end_line if ss.end_line else None

                w.meta['row_spec']['header_rows'] = ss.header_lines
                w.meta['row_spec']['comment_rows'] = None
                w.meta['row_spec']['start_row'] = ss.start_line
                w.meta['row_spec']['end_row'] = ss.end_line
                w.meta['row_spec']['data_pattern'] = None

                if header_lines:
                    set_descriptions(w, [h for h in RowIntuiter.coalesce_headers(header_lines)])
                    w.headers = [self.header_mangler(h) for h in RowIntuiter.coalesce_headers(header_lines)]

        # Now, look for the end line.
        if False:
            # FIXME: Maybe later ...
            r = self.parent.reader
            # Look at the last 100 rows, but don't start before the start row.
            test_rows = 100
            start = max(r.data_start_row, r.data_end_row - test_rows)

            end_rows = list(islice(r.raw, start, None))

            ri.find_end(end_rows)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

        if exc_val:
            return False

