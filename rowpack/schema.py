# -*- coding: utf-8 -*-
# Copyright (c) 2016 Civic Knowledge. This file is licensed under the terms of the
# MIT License, included in this distribution as LICENSE.txt

"""
"""

import sys
from six import text_type, binary_type
import datetime
from tabulate import tabulate

types_map = {
    'int': int,
    'float': float,
    'str': binary_type,
    'text': text_type,
    'date': datetime.date,
    'time': datetime.time,
    'datetime': datetime.datetime
}

class Column(object):

    def __init__(self, **kwargs):

        self.pos = text_type(kwargs.get('pos'))
        self.name = text_type(kwargs.get('name'))
        self.description = text_type(kwargs.get('description'))

        try:
            self.datatype = text_type(kwargs.get('datatype').__name__)
        except:
            self.datatype = text_type(kwargs.get('datatype'))

        self.count = float(kwargs.get('count', 'nan'))
        self.min = float(kwargs.get('min', 'nan'))
        self.mean = float(kwargs.get('mean', 'nan'))
        self.median = float(kwargs.get('median', kwargs.get('p50', 'nan')))
        self.max = float(kwargs.get('max', 'nan'))
        self.std = float(kwargs.get('std', 'nan'))
        self.nuniques = float(kwargs.get('nuniques', 'nan'))
        self.uvalues = kwargs.get('uvalues', None)

    def __str__(self):

        return "<col {} {} {}>".format(self.pos, self.name, self.datatype)

    @property
    def python_type(self):
        return types_map.get(self.datatype, binary_type)


    @property
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

    def __getitem__(self, item):
        return self.columns[item]

    @property
    def headers(self):
        """Return the list of column names"""
        return [c.name for c in self.columns]

    @property
    def headers(self):
        """Return the list of column names"""
        return [c.name for c in self.columns]

    def to_rows(self):
        return [c.dict for c in self.columns]

    def __iter__(self):
        return iter(self.columns)

    @classmethod
    def from_rows(cls, rows):

        s = cls()

        for row in rows:
            c = Column(**row)
            s.columns.append(c)

        return s

    def __str__(self):
        from operator import itemgetter
        schema_fields = ['pos', 'name', 'datatype', 'count', 'nuniques', 'min', 'mean', 'max', 'std', 'description']
        schema_getter = itemgetter(*schema_fields)

        return (tabulate((schema_getter(s.dict) for s in self.columns), schema_fields))





