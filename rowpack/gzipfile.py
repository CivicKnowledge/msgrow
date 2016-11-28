# -*- coding: utf-8 -*-
# Copyright (c) 2016 Civic Knowledge. This file is licensed under the terms of the
# MIT License, included in this distribution as LICENSE.txt

"""

A Hacked GZIP file implementation

"""

import gzip
import six


class GzipFile(gzip.GzipFile):
    """A Hacked GzipFile that will read only one gzip member and properly handle extra data afterward,
    by ignoring it"""

    def __init__(self, filename=None, mode=None, compresslevel=9, fileobj=None, mtime=None, end_of_data=None):
        super(GzipFile, self).__init__(filename, mode, compresslevel, fileobj, mtime)
        self._end_of_data = end_of_data

    def _read(self, size=1024):
        """Alters the _read method to stop reading new gzip members when we've reached the end of the row data. """

        if self._new_member and self._end_of_data and self.fileobj.tell() >= self._end_of_data:
            if six.PY3:
                return None
            else:
                raise EOFError('Reached EOF')
        else:
            return super(GzipFile, self)._read(size)

