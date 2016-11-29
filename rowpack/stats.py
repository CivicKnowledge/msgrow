# -*- coding: utf-8 -*-
# Copyright (c) 2016 Civic Knowledge. This file is licensed under the terms of the
# MIT License, included in this distribution as LICENSE.txt

"""

"""

from . import RowpackReader, RowpackWriter
from tableintuit import Stats, RowIntuiter, TypeIntuiter

def run_stats(path, update=True):

    from tableintuit import Stats
    from . import RowpackReader

    with RowpackReader(path) as r:
        stats_schema = [(c.name, c.python_type) for c in r.schema]
        headers = r.headers
        stats = Stats(stats_schema).run(dict(zip(headers, row)) for row in r)

        schema = r.schema

    if update:
        with RowpackWriter(path, 'r+b') as w:
            for c in schema:
                s = stats[c.name]

                c.count = float(s.n)
                c.nuniques = float(s.nuniques)
                c.min = float(s.min if s.min is not None else 'nan')
                c.mean = float(s.mean if s.mean is not None else 'nan')
                c.median = float(s.p50 if s.p50 is not None else 'nan')
                c.max = float(s.max if s.max is not None else 'nan')
                c.std = float(s.stddev if s.stddev is not None else 'nan')

                c.uvalues = s.uvalues

            w.schema = schema

    return stats


def intuit_rows(path, update=True):

    from itertools import islice

    with RowpackReader(path) as r:

        ri = RowIntuiter()
        ri.run(list(islice(r, 1000)))

    with RowpackWriter(path, 'r+b') as w:
        w.meta['rowspec'] = ri.spec
        w.meta['headers'] = ri.headers

    return ri


def intuit_types(path, update=True):

    from itertools import islice
    from tableintuit import SelectiveRowGenerator
    from . import Schema

    with RowpackReader(path) as r:

        if 'rowspec' in r.meta:
            rs = r.meta['rowspec']
        else:
            rs = {}

        rows = list(islice(r, 1000))

        srg = SelectiveRowGenerator(rows, **rs)

        ti = TypeIntuiter().run(list(srg))

    if update:
        with RowpackWriter(path, 'r+b') as w:
            s = Schema()
            for k,v in ti.columns.items():
                s.add_column(name = v.header, datatype = v.resolved_type_name)

            w.schema = s

            w.meta['types'] = list(ti.to_rows())

    return ti
