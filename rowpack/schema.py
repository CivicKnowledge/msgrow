# -*- coding: utf-8 -*-
# Copyright (c) 2016 Civic Knowledge. This file is licensed under the terms of the
# MIT License, included in this distribution as LICENSE.txt

"""
"""

import sys
from six import text_type

class Column(object):

    def __init__(self, **kwargs):

        self.pos = text_type(kwargs.get('pos'))
        self.name = text_type(kwargs.get('name'))
        self.description = text_type(kwargs.get('description'))

        try:
            self.datatype = text_type(kwargs.get('datatype').__name__)
        except:
            self.datatype = text_type(kwargs.get('datatype'))

        # Reserve space by using the largest integers. These will force msgpack to
        # write the largest size integer

        self.count = int(kwargs.get('count', sys.maxint))
        self.min = float(kwargs.get('min', 'nan'))
        self.mean = float(kwargs.get('mean', 'nan'))
        self.max = float(kwargs.get('max', 'nan'))
        self.std = float(kwargs.get('std', 'nan'))
        self.nuniques = int(kwargs.get('nuniques', sys.maxint))

        # FIXME. The reserved space should be for up to 100 elements of the
        # datatype, not 2K
        self.uvalues = kwargs.get('uvalues', None)

    def __str__(self):

        return "<col {} {} {}>".format(self.pos, self.name, self.datatype)


    def dict(self):

        return self.__dict__


class Schema(object):

    def __init__(self):

        self.columns = []

    def append(self, c):

        c.pos = len(self.columns)
        self.columns.append(c)

    def add_column(self, **kwargs):

        self.append(Column(**kwargs))

    @property
    def headers(self):
        """Return the list of column names"""
        return [c.name for c in self.columns]

    @property
    def headers(self):
        """Return the list of column names"""
        return [c.name for c in self.columns]


    def to_rows(self):

        return [c.dict() for c in self.columns]

    def __iter__(self):
        return iter(self.columns)

    @classmethod
    def from_rows(cls, rows):

        s = cls()

        for row in rows:
            c = Column(**row)
            s.columns.append(c)

        return s




