# -*- coding: utf-8 -*-
# Copyright (c) 2016 Civic Knowledge. This file is licensed under the terms of the
# MIT License, included in this distribution as LICENSE.txt

"""
"""


import struct
import time
import zlib
import msgpack
from six import text_type

def new_mpr(fs, path, stats=None):
    from os.path import split, splitext

    assert bool(fs)

    dn, file_ext = split(path)
    fn, ext = splitext(file_ext)

    if fs and not fs.exists(dn):
        fs.makedir(dn, recursive=True)

    if not ext:
        ext = '.msg'

    return MPRowsFile(fs, path)


EXTENSION = '.mpr'
VERSION = 1
MAGIC = 'AMBRMPDF'

# 8s: Magic Number, H: Version,
# I: Number of rows, I: number of columns
# Q: Start of row data. Q: End of row data
FILE_HEADER_FORMAT = struct.Struct('>8sHIIQQ')

FILE_HEADER_FORMAT_SIZE = FILE_HEADER_FORMAT.size


class MPRowsFile(object):
    """The Message Pack Rows File format holds a collection of arrays, in message pack format, along with a
    dictionary of values. The format is designed for holding tabular data in an efficient, compressed form,
    and for associating it with metadata. """

    EXTENSION = '.mpr'
    VERSION = 1
    MAGIC = 'AMBRMPDF'

    # 8s: Magic Number, H: Version,  I: Number of rows, I: number of columns
    # Q: Position of end of rows / Start of meta,
    # I: Data start row, I: Data end row
    FILE_HEADER_FORMAT = struct.Struct('>8sHIIQII')

    FILE_HEADER_FORMAT_SIZE = FILE_HEADER_FORMAT.size

    # These are all of the keys for the  schema. The schema is a collection of rows, with these
    # keys being the first, followed by one row per column.
    SCHEMA_TEMPLATE = [
        'pos',
        'name',
        'type',
        'description',
        'start',
        'width',

        # types
        'position',
        'header',
        'length',
        'has_codes',
        'type_count',  # Note! Row Intuiter object call this 'count'

        'ints',
        'floats',
        'strs',
        'unicode',
        'nones',
        'datetimes',
        'dates',
        'times',
        'strvals',

        # Stats
        'flags',
        'lom',
        'resolved_type',
        'stat_count',  # Note! Stat object calls this 'count'
        'nuniques',
        'mean',
        'std',
        'min',
        'p25',
        'p50',
        'p75',
        'max',
        'skewness',
        'kurtosis',
        'hist',
        'text_hist',
        'uvalues']

    META_TEMPLATE = {

        'schema': [SCHEMA_TEMPLATE],
        'about': {
            'create_time': None,  # Timestamp when file was  created.
            'load_time': None  # Length of time MPRowsFile.load_rows ran, in seconds()
        },
        'geo': {
            'srs': None,
            'bb': None
        },
        'excel': {
            'datemode': None,
            'worksheet': None
        },
        'source': {
            'url': None,
            'fetch_time': None,
            'file_type': None,
            'url_type': None,
            'inner_file': None,
            'encoding': None
        },
        'row_spec': {
            'header_rows': None,
            'comment_rows': None,
            'start_row': None,
            'end_row': None,
            'data_pattern': None
        },
        'comments': {
            'header': None,
            'footer': None
        },
        'process': {
            'finalized': False
        },
        'warnings': []
    }

    def __init__(self, url_or_fs, path=None):
        """

        :param url_or_fs:
        :param path:
        :return:
        """

        from fs.opener import opener

        if path:
            self._fs, self._path = url_or_fs, path
        else:
            self._fs, self._path = opener.parse(url_or_fs)

        self._writer = None
        self._reader = None

        self._compress = True

        self._process = None  # Process name for report_progress
        self._start_time = 0

        if not self._path.endswith(self.EXTENSION):
            self._path = self._path + self.EXTENSION

    @property
    def path(self):
        return self._path

    @property
    def syspath(self):
        if self.exists and self._fs.hassyspath(self.path):
            return self._fs.getsyspath(self.path)
        else:
            return None

    @property
    def url(self):
        from fs.errors import NoPathURLError

        try:
            self._fs.getpathurl(self.path)
        except NoPathURLError:
            return self._fs.getsyspath(self.path)


    @staticmethod
    def encode_obj(obj):

        try:
            if isinstance(obj, datetime.datetime):
                return {'__datetime__': True, 'value': tuple(obj.timetuple()[:6])}
            elif isinstance(obj, datetime.date):
                return {'__date__': True, 'value': (obj.year, obj.month, obj.day)}
            elif isinstance(obj, datetime.time):
                return {'__time__': True, 'value': (obj.hour, obj.minute, obj.second)}
        except ValueError as e:
            # Pandas time series can have a "Not A Time" value of 'NaT', but I don't want to have this
            # module depend on pandas

            if str(obj) == 'NaT':
                return None
            else:
                raise


        if hasattr(obj, 'render'):
            return obj.render()
        elif hasattr(obj, '__str__'):
            return str(obj)
        else:
            raise Exception('Unknown type on encode: {}, {}'.format(type(obj), obj))

    @staticmethod
    def decode_obj(obj):

        if '__datetime__' in obj:
            obj = datetime.datetime(*obj['value'])
        elif '__time__' in obj:
            obj = datetime.time(*obj['value'])
        elif '__date__' in obj:
            obj = datetime.date(*obj['value'])
        else:
            raise Exception('Unknown type on decode: {} '.format(obj))

        return obj

    @classmethod
    def read_file_header(cls, o, fh):
        try:
            o.magic, o.version, o.n_rows, o.n_cols, o.meta_start, o.data_start_row, o.data_end_row = \
                cls.FILE_HEADER_FORMAT.unpack(fh.read(cls.FILE_HEADER_FORMAT_SIZE))
        except struct.error as e:
            raise IOError('Failed to read file header; {}; path = {}'.format(e, o.parent.path))

    @classmethod
    def write_file_header(cls, o, fh):
        """Write the magic number, version and the file_header dictionary.  """

        int(o.data_start_row)
        magic = cls.MAGIC
        if isinstance(magic, text_type):
            magic = magic.encode('utf-8')

        hdf = cls.FILE_HEADER_FORMAT.pack(magic, cls.VERSION, o.n_rows, o.n_cols, o.meta_start,
                                          o.data_start_row,  o.data_end_row if o.data_end_row else o.n_rows)

        assert len(hdf) == cls.FILE_HEADER_FORMAT_SIZE

        fh.seek(0)

        fh.write(hdf)

        assert fh.tell() == cls.FILE_HEADER_FORMAT_SIZE, (fh.tell(), cls.FILE_HEADER_FORMAT_SIZE)

    @classmethod
    def read_meta(cls, o, fh):

        pos = fh.tell()

        fh.seek(o.meta_start)

        # Using the _fh b/c I suspect that the GzipFile attached to self._zfh has state that would
        # get screwed up if you read from a new position

        data = fh.read()
        if data:
            meta = msgpack.unpackb(zlib.decompress(data), encoding='utf-8')
        else:
            meta = {}
        fh.seek(pos)
        return meta

    @classmethod
    def write_meta(cls, o, fh):

        o.meta['schema'][0] == MPRowsFile.SCHEMA_TEMPLATE

        fh.seek(o.meta_start)  # Should probably already be there.
        fhb = zlib.compress(msgpack.packb(o.meta, encoding='utf-8'))
        fh.write(fhb)

    @classmethod
    def _columns(cls, o, n_cols=0):
        """ Wraps columns from meta['schema'] with RowProxy and generates them.

        Args:
            o (any having .meta dict attr):

        Generates:
            RowProxy: column wrapped with RowProxy

        """

        from ambry_sources.sources.util import RowProxy

        s = o.meta['schema']

        assert len(s) >= 1  # Should always have header row.
        if o.meta['schema'][0] != MPRowsFile.SCHEMA_TEMPLATE:
            raise AssertionError(
                'Object schema does not match to template. object schema: {}, template: {}'
                .format(o.meta['schema'][0], MPRowsFile.SCHEMA_TEMPLATE))

        # n_cols here is for columns in the data table, which are rows in the headers table
        n_cols = max(n_cols, o.n_cols, len(s)-1)

        for i in range(1, n_cols+1):
            # Normally, we'd only create one of these, and set the row on the singleton for
            # each row. But in this case, the caller may turn the output of the method into a list,
            # in which case all of the rows would have the values of the last one.
            rp = RowProxy(s[0])
            try:
                row = s[i]
            except IndexError:
                # Extend the row, but make sure the pos value is set property.
                ext_row = [i, 'col{}'.format(i)] + [None] * (len(s[0]) - 2)
                s.append(ext_row)
                row = s[i]

            yield rp.set_row(row)

        assert o.meta['schema'][0] == MPRowsFile.SCHEMA_TEMPLATE

    @property
    def info(self):
        return self._info(self.reader)

    @classmethod
    def _info(cls, o):

        return dict(
            version=o.version,
            data_start_pos=o.data_start,
            meta_start_pos=o.meta_start,
            rows=o.n_rows,
            cols=o.n_cols,
            header_rows=o.meta['row_spec']['header_rows'],
            data_start_row=o.data_start_row,
            data_end_row=o.data_end_row,
            comment_rows=o.meta['row_spec']['comment_rows'],
            headers=o.headers
        )

    @property
    def exists(self):
        """ Returns True if mpr file (self.path) exists in the filesystem (self._fs). False otherwise. """

        return self._fs.exists(self.path)

    def remove(self):
        if self.exists:
            from fs.s3fs import S3FS
            assert not isinstance(self._fs, S3FS) # Let's not be deleteing from remotes.
            self._fs.remove(self.path)
            self.close()

    def close(self):

        if self._reader:
            self._reader.close()

        if self._writer:
            self._reader.close()

    @property
    def meta(self):

        if not self.exists:
            return None

        with self.reader as r:
            return r.meta

    @property
    def is_finalized(self):
        with self.reader as r:
            return r.is_finalized

    @property
    def stats(self):
        return (self.meta or {}).get('stats')

    @property
    def n_rows(self):

        if not self.exists:
            return None

        with self.reader as r:
            return r.n_rows

    @property
    def headers(self):

        if not self.exists:
            return None

        with self.reader as r:
            return r.headers

    def run_type_intuiter(self):
        """Run the Type Intuiter and store the results back into the metadata"""
        from .intuit import TypeIntuiter

        try:
            self._process = 'intuit_type'
            self._start_time = time.time()

            with self.reader as r:
                ti = TypeIntuiter().process_header(r.headers).run(r.rows, r.n_rows)

            with self.writer as w:
                w.set_types(ti)
        finally:
            self._process = 'none'

    def run_row_intuiter(self):
        """Run the row intuiter and store the results back into the metadata"""
        from .intuit import RowIntuiter
        from itertools import islice

        try:
            self._process = 'intuit_rows'
            self._start_time = time.time()

            with self.reader as r:
                if r.n_rows == 0:
                    return

                head = list(islice(r.raw, RowIntuiter.N_TEST_ROWS))
                n_rows = r.n_rows

            with self.reader as r:
                # Reset the iterator to get the tail
                if RowIntuiter.N_TEST_ROWS < r.n_rows:
                    tail = list(islice(r.raw, r.n_rows - RowIntuiter.N_TEST_ROWS, r.n_rows))
                else:
                    tail = list(islice(r.raw, 0, r.n_rows))

            ri = RowIntuiter().run(head, tail, n_rows)

            with self.writer as w:
                w.set_row_spec(ri)

        finally:
            self._process = 'none'

    def run_stats(self):
        """Run the stats process and store the results back in the metadata"""
        from .stats import Stats

        try:
            self._process = 'run_stats'
            self._start_time = time.time()

            with self.reader as r:
                if r.n_rows == 0:
                    return
                columns = [(c.name, c.type) for c in r.columns]
                stats = Stats(columns, r.n_rows).run(r, sample_from=r.n_rows)

            with self.writer as w:
                w.set_stats(stats)

        finally:
            self._process = 'none'

        return stats

    def load_rows(self, source, spec=None, intuit_rows=None,
                  intuit_type=True, run_stats=True, callback=False, limit=None):
        try:

            # The spec should always be part of the source
            assert spec is None

            self._load_rows(source,
                            intuit_rows=intuit_rows,
                            intuit_type=intuit_type, run_stats=run_stats,
                            callback=callback, limit=limit)
        except:
            raise
            self.writer.close()
            self.remove()
            raise

        return self

    def _load_rows(self, source, intuit_rows=None, intuit_type=True, run_stats=True,
                   callback=None, limit=None):
        from .exceptions import RowIntuitError

        if self.n_rows:
            raise MPRError(
                "Can't load_rows into {}; rows already loaded. n_rows = {}"
                .format(self.path, self.n_rows))

        spec = getattr(source, 'spec', None)

        # None means to determine True or False from the existence of a row spec
        if intuit_rows is None:

            if spec is None:
                intuit_rows = True
            elif spec.has_rowspec:
                intuit_rows = False
            else:
                intuit_rows = True

        try:

            self._process = 'load_rows'
            self._start_time = time.time()

            with self.writer as w:

                w.load_rows(source, callback=callback, limit=limit)

                if spec:
                    w.set_source_spec(spec)

            if intuit_rows:
                try:
                    self.run_row_intuiter()
                except RowIntuitError:
                    with self.writer as w:
                        w.meta['warnings'].append('Failed to intuit rows. Should set row classifications manually. ')

                    pass

            elif spec:

                with self.writer as w:
                    w.set_row_spec(spec)
                    assert w.meta['schema'][0] == MPRowsFile.SCHEMA_TEMPLATE

            if source.meta:
                with self.writer as w:
                    for c, m in zip(w.columns, source.meta['columns']):
                        assert c.pos == m['position']

                        #assert c.name == m['name'] # True for SocrataSource, maybe not if there are others in the future

                        col = w.column(c.name)

                        col.description = m['description']


            if intuit_type:
                self.run_type_intuiter()

            if run_stats:
                self.run_stats()

            with self.writer as w:

                if not w.data_end_row:
                    w.data_end_row = w.n_rows

                w.finalize()

        finally:
            self._process = None

        return self

    def open(self, mode='rb'):
        """Open the file, and return a file-like pyfilesystem object"""
        return self._fs.open(self.path, mode=mode)

    def set_contents(self, data='', errors=None, chunk_size=65536):
        """Pass-though to the PySilesystem setcontents function"""
        return self._fs.setcontents(self.path,  data,  errors=errors, chunk_size=chunk_size)

    @property
    def reader(self):
        if not self._reader:
            self._reader = MPRReader(self, self._fs.open(self.path, mode='rb'), compress=self._compress)
        return self._reader

    def __iter__(self):
        """Iterate over a reader"""

        # There is probably a more efficient way in python 2 to do this than to have another yield loop,
        # but just returning the reader iterator doesn't work. It should probably be yield from in Python 3
        with self.reader as r:
            for row in r:
                yield row

    def select(self, predicate=None, headers=None):
        """Iterate the results from the reader's select() method"""

        with self.reader as r:
            for row in r.select(predicate, headers):
                yield row

    @property
    def writer(self):
        from os.path import dirname
        if not self._writer:
            self._process = 'write'
            if self._fs.exists(self.path):
                mode = 'r+b'
            else:
                mode = 'wb'

            if not self._fs.exists(dirname(self.path)):
                self._fs.makedir(dirname(self.path), recursive=True, allow_recreate=True)

            self._writer = MPRWriter(self, self._fs.open(self.path, mode=mode), compress=self._compress)

        return self._writer

    def report_progress(self):
        """
        This function can be called from a higher level to report progress. It is usually called from an alarm
        signal handler which is installed just before starting a load_rows operation:

        >>> import signal
        >>> f = MPRowsFile('tmp://foobar')
        >>> def handler(signum, frame):
        >>>     print "Loading: %s, %s rows" % f.report_progress()
        >>> f.load_rows([i,i,i] for i in range(1000))

        :return: Tuple: (process description, #records, #total records, #rate)
        """

        rec = total = rate = 0

        if self._process in ('load_rows', 'write') and self._writer:
            rec = self._writer.n_rows
            rate = round(float(rec) / float(time.time() - self._start_time), 2)

        elif self._reader:
            rec = self._reader.pos
            total = self._reader.data_end_row
            rate = round(float(rec) / float(time.time() - self._start_time), 2)

        return (self._process, rec, total, rate)

