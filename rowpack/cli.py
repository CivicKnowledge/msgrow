# -*- coding: utf-8 -*-
# Copyright (c) 2016 Civic Knowledge. This file is licensed under the terms of the
# MIT License, included in this distribution as LICENSE.txt

"""

CLI program

"""

import argparse
from itertools import islice

from six import binary_type

import tabulate

from rowpack import RowpackReader
from .__meta__ import __version__


def rowpack_make_arg_parser(parser=None):

    if not parser:
        parser = argparse.ArgumentParser(
            prog='ampr',
            description='Ambry Message Pack Rows file access version:'.format(__version__))

    parser.add_argument('-m', '--meta', action='store_true',
                        help='Show metadata')
    parser.add_argument('-s', '--schema', action='store_true',
                        help='Show the schema')
    parser.add_argument('-H', '--head', action='store_true',
                        help='Display the first 10 records. Will only display 80 chars wide')
    parser.add_argument('-T', '--tail', action='store_true',
                        help='Display the first last 10 records. Will only display 80 chars wide')
    parser.add_argument('-r', '--records', action='store_true',
                        help='Output the records in tabular format')
    parser.add_argument('-R', '--raw', action='store_true',
                        help='For the sample output, use the raw iterator')
    parser.add_argument('-c', '--csv', help='Output the entire file as CSV')
    parser.add_argument('-l', '--limit', help='The number of rows to output for CSV ')

    parser.add_argument('path', nargs=1, type=binary_type, help='File path')

    return parser


def rowpack(args=None):
    from operator import itemgetter
    from datetime import datetime

    if not args:
        parser = rowpack_make_arg_parser()
        args = parser.parse_args()

    schema_fields = ['pos', 'name',  'datatype', 'count',  'nuniques' , 'min', 'mean', 'max', 'std', 'description' ]
    schema_getter = itemgetter(*schema_fields)

    types_fields = ['header', 'type_count', 'length',  'floats',  'ints', 'unicode',  'strs', 'dates',
                    'times', 'datetimes', 'nones', 'has_codes']

    types_getter = itemgetter(*types_fields)

    stats_fields_all = ['name', 'stat_count', 'nuniques', 'mean', 'min', 'p25', 'p50', 'p75', 'max', 'std',
                        'uvalues', 'lom',  'skewness', 'kurtosis', 'flags', 'hist', 'text_hist']

    stats_fields = ['name', 'lom', 'stat_count', 'nuniques', 'mean', 'min', 'p25', 'p50', 'p75',
                    'max', 'std', 'text_hist']

    stats_getter = itemgetter(*stats_fields)

    path = args.path[0]

    if args.csv:
        import unicodecsv as csv

        with RowpackReader(path) as r:
            limit = int(args.limit) if args.limit else None

            if args.csv == '-':
                from sys import stdout
                out_f = stdout
            else:
                out_f = open(args.csv, 'wb')

            w = csv.writer(out_f)

            if r.headers:
                w.writerow(r.headers)

            for i, row in enumerate(r):
                w.writerow(row)

                if limit and i >= limit:
                    break

            if args.csv != '-':
                out_f.close()

        return

    if args.meta:
        import json
        with RowpackReader(path) as r:
            print json.dumps(r.meta,indent=4)

        return

    def pm(l, m):
        """Print, maybe"""
        if not m:
            return
        m = binary_type(m).strip()
        if m:
            print('{:<12s}: {}'.format(l, m))

    with RowpackReader(path) as r:

        pm('MPR File', path)
        pm('version', r.version)
        pm('rows', r.n_rows)
        pm('cols', r.n_cols)
        pm('headers', r.headers)

    if args.schema:
        print('\nSCHEMA')
        with RowpackReader(path) as r:

            print(tabulate.tabulate((schema_getter(s.dict) for s in r.schema), schema_fields))
        return


    if args.head or args.tail:
        with f.reader as r:
            print('\nHEAD' if args.head else '\nTAIL')
            MAX_LINE = 80
            headers = []

            # Only show so may cols as will fit in an 80 char line.
            for h in r.headers:
                if len(' '.join(headers+[h])) > MAX_LINE:
                    break
                headers.append(h)

            itr = r.raw if args.raw else r.rows

            rows = []

            start, end = (None, 10) if args.head else (r.n_rows-10, r.n_rows)

            slc = islice(itr, start, end)

            rows = [(i,)+row[:len(headers)] for i, row in enumerate(slc, start if start else 0)]

            print(tabulate.tabulate(rows, ['#'] + headers))

    elif args.records:

        with f.reader as r:

            acc = []
            try:
                for i, row in enumerate(r.rows, 1):

                    if i % 30 == 0:
                        print (tabulate.tabulate(acc, r.headers))
                        acc = []
                    else:
                        acc.append(row)

                    if args.limit and i > int(args.limit):
                        if acc:
                            print (tabulate.tabulate(acc, r.headers))
                            acc = []
                        break
                if acc:
                    print (tabulate.tabulate(acc, r.headers))

            except KeyboardInterrupt:
                import sys
                sys.exit(0)


def ingest_make_arg_parser(parser=None):

    if not parser:
        parser = argparse.ArgumentParser(
            prog='rpingest',
            description='Ingest tabular data into a rowpack file. version:'.format(__version__))

    parser.add_argument('url',  type=binary_type, help='Input url')
    parser.add_argument('path', type=binary_type, help='Output file path')

    return parser


def rpingest(args=None):
    from fs.opener import fsopendir
    import tempfile
    from rowgenerators import RowGenerator
    from . import RowpackWriter

    cache = fsopendir(tempfile.gettempdir())

    if not args:
        parser = ingest_make_arg_parser()
        args = parser.parse_args()

    print args.__dict__

    gen = RowGenerator(cache=cache, **args.__dict__)

    with RowpackWriter(args.path) as rpw:
        for row in gen:
            rpw.write_row(row)

        print "Wrote {} rows".format(rpw.n_rows)


if __name__ == '__main__':
    rowpack()
