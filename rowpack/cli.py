# -*- coding: utf-8 -*-
# Copyright (c) 2016 Civic Knowledge. This file is licensed under the terms of the
# MIT License, included in this distribution as LICENSE.txt

"""

CLI program

"""

import argparse
from itertools import islice
import sys

from six import binary_type

import tabulate

from . import RowpackWriter, RowpackReader, intuit_types, run_stats, ingest

from .__meta__ import __version__


def mk_row_types(r):
    if 'rowspec' not in r.meta:
        return {}

    rt_end = 20
    row_types = {i: '' for i in range(rt_end)}
    rs = r.meta['rowspec']
    for i in rs.get('headers', []):
        row_types[i] = 'H'
    for i in rs.get('comments', []):
        row_types[i] = 'C'

    if rs.get('start', False):
        for i in range(int(rs.get('start', rt_end)), rt_end):
            row_types[i] = 'D'

    if rs.get('end', False):
        end = int(rs['end'])
        row_types[end] = 'E'

        for i in list(row_types.keys()):
            if i > end:
                del row_types[i]

        for i in range(r.n_rows - rt_end, r.n_rows):
            row_types[i] = ''

        for i in range(r.n_rows - rt_end, int(rs.get('end'))):
            row_types[i] = 'D'
    elif r.n_rows > rt_end:
        for i in range(r.n_rows - rt_end, r.n_rows):
            row_types[i] = 'D'

    return row_types


def head_tail(r, head=True):
    """Return head or tail rows, for display and editing"""
    MAX_LINE = 80

    row_types = mk_row_types(r)

    # Only show so many cols as will fit in an 80 char line.
    if r.headers:
        headers = []
        for h in r.headers:
            if len(' '.join(headers + [h])) > MAX_LINE:
                break
            headers.append(h)
    else:
        headers = range(1, 11)

    start, end = (None, 15) if head else (r.n_rows - 15, r.n_rows)

    slc = islice(r, start, end)

    rows = [(row_types.get(i), i,) + row[:len(headers)] for i, row in enumerate(slc, start if start else 0)]

    return rows, headers


def edit_types(r, line, type):
    def ua(l, a):
        """Unique append"""
        s = set(l)
        s.add(a)
        return list(s)

    if 'rowspec' not in r.meta:
        r.meta['rowspec'] = {
            'start': 1,
            'headers': [0],
            'comments': [],
            'end': None
        }

    rs = r.meta['rowspec']

    if type == 'D':
        rs['start'] = line

    elif type == 'H':
        rs['headers'] = ua(rs['headers'], line)
        try:
            rs['comments'].remove(line)
        except ValueError:
            pass

        if rs['start'] <= line:
            rs['start'] = max(rs['headers'] + rs['comments']) + 1

        try:
            rs['comments'].remove(line)
        except ValueError:
            pass

    elif type == 'C':
        rs['comments'] = ua(rs['comments'], line)
        if rs['start'] <= line:
            rs['start'] = max(rs['headers'] + rs['comments']) + 1

        try:
            rs['headers'].remove(line)
        except ValueError:
            pass

    elif type == 'E':

        rs['end'] = line

        if line <= rs['start']:
            rs['start'] = line - 1

        for l in list(rs['headers']):
            if l >= line:
                rs['headers'].remove(l)

        for l in list(rs['comments']):
            if l >= line:
                rs['comments'].remove(l)

    elif type == 'X':
        try:
            rs['headers'].remove(line)
        except ValueError:
            pass
        try:
            rs['comments'].remove(line)
        except ValueError:
            pass

        if rs['start'] == line:
            rs['start'] = max(rs['headers'] + rs['comments']) + 1

        if rs['end'] == line:
            rs['end'] = None


def edit(path, head):
    while True:

        with RowpackReader(path) as r:
            rows, headers = head_tail(r, head)

        print(tabulate.tabulate(rows, ['T', '#'] + headers))

        while True:
            line_n = raw_input("Line: ")

            if line_n.lower() == 'q':
                return

            try:
                line_n = int(line_n)
            except ValueError:
                continue

            break

        while True:
            type = raw_input("Type: ")

            if type.lower() == 'q':
                break

            type = type.upper()

            if type not in ['C', 'H', 'X', 'D', 'E']:
                continue

            break

        with RowpackWriter(path, 'r+b') as w:
            edit_types(w, line_n, type)


def resolve_url(ss, cache):
    """Return a list of sub-components of a Spec, such as files in a ZIP archive,
    or worksheed in a spreadsheet"""

    from rowgenerators.fetch import inspect

    while True:
        specs = inspect(ss, cache, callback=progress_callback)

        if not specs or len(specs) <= 1:
            return ss

        print "\nURL has multiple internal files. Select one:"
        for i, e in enumerate(specs):
            print i, e.url_str()

        while True:
            i = raw_input('Select: ')
            try:
                ss = specs[int(i)]
                break
            except ValueError:
                print "ERROR: enter an integer"
            except IndexError:
                print "ERROR: entry out of range"


def row_spec_str(path=None, r=None):
    if r is None:
        r = RowpackReader(path)
        close = True
    else:
        close = False

    if not 'rowspec' in r.meta:
        return ''

    rs = r.meta['rowspec']

    out = ''

    if rs.get('headers'):
        out += ('headers=' + ','.join(str(i) for i in rs['headers']) + ' ')

    if rs.get('comments'):
        out += ('comments=' + ','.join(str(i) for i in rs['comments']) + ' ')

    if rs.get('start'):
        out += ('start=' + str(rs['start']) + ' ')

    if rs.get('end'):
        out += ('end=' + str(rs['end']) + ' ')

    if close:
        r.close()

    return out


def get_cache():
    from fs.opener import fsopendir
    import tempfile

    return fsopendir(tempfile.gettempdir())


def ingest_cb(v):
    print v


def rpingest(args=None):
    parser = argparse.ArgumentParser(
        prog='rpingest',
        description='Ingest tabular data into a rowpack file. version:'.format(__version__))

    parser.add_argument('url', type=binary_type, help='Input url')
    parser.add_argument('path', nargs='?', type=binary_type, help='Output file path')

    args = parser.parse_args()

    path, encoding, warnings = ingest(args.url, args.path, get_cache(), cb=ingest_cb, url_resolver=resolve_url)
    print "Ingested ", path
    if warnings:
        for w in warnings:
            print w


def progress_callback(actitity, arg1, arg2):

    print actitity, arg1, arg2


def rowpack(args=None):
    from operator import itemgetter
    import sys

    parser = argparse.ArgumentParser(
        prog='rowpack',
        description='Ambry Message Pack Rows file access. Version: {}'.format(__version__))

    group = parser.add_mutually_exclusive_group()

    parser.add_argument('-f', '--info', action='store_true',
                        help='Show general information header')

    group.add_argument('-m', '--meta', action='store_true',
                       help='Show metadata')
    group.add_argument('-t', '--types', action='store_true',
                       help='Show types table')
    group.add_argument('-s', '--schema', action='store_true',
                       help='Show the schema')
    group.add_argument('-r', '--rowspec', action='store_true',
                       help='Print the rowspec')
    group.add_argument('-H', '--head', action='store_true',
                       help='Display the first 10 records. Will only display 80 chars wide')
    group.add_argument('-T', '--tail', action='store_true',
                       help='Display the first last 10 records. Will only display 80 chars wide')
    parser.add_argument('-e', '--edit', action='store_true',
                        help='With -H or -T, edit line types')
    group.add_argument('-I', '--intuition', action='store_true',
                       help='Run type intuition and stats')
    group.add_argument('-c', '--csv', help='Output the entire file as CSV')
    parser.add_argument('-R', '--raw', action='store_true',
                        help='With --csv, return all rows, ignoring rowspec')

    group.add_argument('-b', '--table', action='store_true',
                       help='Display a selection of records in a table')
    group.add_argument('-l', '--limit', help='The number of rows to output for CSV ')

    group.add_argument('-p', '--inspect', action='store_true',
                       help='Inspect a URL and report on the available lower-level URLs ')

    group = parser.add_argument_group()
    group.add_argument('-i', '--ingest', action='store_true',
                       help='Ingest a url and write the result to a rowpack file. ')
    group.add_argument('-a', '--all', action='store_true', help='With -i ingest all of the files; don\'t ask to resolve url')
    group.add_argument('-o', '--output',  help='With -i, the name of the output file to write the rowpack file to')

    group.add_argument('--encoding', help='With -i, Set ingestion encoding')
    group.add_argument('--filetype', help='With -i, Set the type of the file that will be imported')
    group.add_argument('--urlfiletype', help='With -i, Set the type of the file that will be downloaded')

    parser.add_argument('path', nargs='?', type=binary_type, help='File path')

    args = parser.parse_args()


    schema_fields = ['pos', 'name', 'datatype', 'count', 'nuniques', 'min', 'mean', 'max', 'std', 'description']
    schema_getter = itemgetter(*schema_fields)

    if args.ingest and not args.all:
        path, encoding, warnings = ingest(args.path, args.output, get_cache(),
                                          encoding=args.encoding, filetype=args.filetype, urlfiletype=args.urlfiletype,
                                          cb=ingest_cb,
                                          url_resolver=resolve_url)

        print "Ingested ", path
        if warnings:
            print "Warnings for {}".format(path)
            for w in warnings:
                print "    ", w

        return
    elif args.ingest:

        from rowgenerators import enumerate_contents, SourceError

        for ss in enumerate_contents(args.path, get_cache(), callback=progress_callback):
            try:
                path, encoding, warnings = ingest(ss.url_str(),
                                                  cache=get_cache(),
                                                  cb=ingest_cb,
                                                  url_resolver=None)
                print "Ingested ", path
                if warnings:
                    print "Warnings for {}".format(path)
                    for w in warnings:
                        print "    ", w

            except SourceError as e:
                print "WARN: Failed to ingest {}: {}".format(ss.url_str(), e)

        return


    # All of the remaining options require a path

    if args.inspect:
        from rowgenerators import enumerate_contents

        for s in enumerate_contents(args.path, get_cache(), callback=progress_callback):
            print s.url_str()

        return

    path = args.path

    def pm(l, m):
        """Print, maybe"""
        if not m:
            return
        m = binary_type(m).strip()
        if m:
            print('{:<12s}: {}'.format(l, m))

    def show_info():
        with RowpackReader(path) as r:
            print "Rowpack file"
            pm('version', r.version)
            pm('path', path)
            pm('URL:', r.meta.get('url'))
            pm('rows', r.n_rows)
            pm('cols', r.n_cols)
            pm('headers', r.headers)
            pm('rowspec', row_spec_str(r=r))

    if not path:
        print "ERROR: must specify a path"
        sys.exit(1)

    if args.csv:
        import unicodecsv as csv
        from rowgenerators import SelectiveRowGenerator

        with RowpackReader(path) as r:
            limit = int(args.limit) if args.limit else None

            if not args.raw:
                rg = SelectiveRowGenerator(r, **r.meta['rowspec'])
            else:
                rg = r

            if args.csv == '-':
                from sys import stdout
                out_f = stdout
            else:
                out_f = open(args.csv, 'wb')

            w = csv.writer(out_f)

            try:
                for i, row in enumerate(rg):
                    w.writerow(row)

                    if limit and i >= limit:
                        break
            except IOError as e:
                print "ERROR: ", e

            if args.csv != '-':
                out_f.close()

        return

    if args.info:
        show_info()
        info_shown = True
    else:
        info_shown = False

    if args.intuition:
        print "Run type intuition"
        intuit_types(path)
        print "Run stats"
        run_stats(path)
        return

    if args.meta:
        import json
        with RowpackReader(path) as r:
            d = dict(r.meta.items())
            try:
                del d['types']  # Types is a lot of data
            except KeyError:
                pass

            print json.dumps(d, indent=4)

        return

    if args.types:
        from operator import itemgetter

        type_fields = ['position', 'header', 'length', 'resolved_type', 'has_codes',
                       'count', 'ints', 'floats', 'strs', 'unicode',
                       'nones', 'datetimes', 'dates', 'times']
        table_header = ['#', 'header', 'size', 'resolved_type', 'codes', 'count',
                        'ints', 'floats', 'strs', 'uni', 'nones', 'dt', 'dates', 'times']

        trans = dict(zip(type_fields, table_header))

        ig = itemgetter(*type_fields)

        with RowpackReader(path) as r:
            types_rows = r.meta.get('types', [])
            rows = [ig(row) for row in types_rows]

            print tabulate.tabulate(rows, table_header)

        return

    if args.rowspec:
        print row_spec_str(path)
        return

    if args.schema:
        print('\nSCHEMA')
        with RowpackReader(path) as r:
            print(tabulate.tabulate((schema_getter(s.dict) for s in r.schema), schema_fields))
        return

    if args.head or args.tail:

        print('\nHEAD' if args.head else '\nTAIL')
        if args.edit:
            edit(path, args.head)
            print "\n"
            print row_spec_str(path)

        else:
            with RowpackReader(path) as r:
                rows, headers = head_tail(r, args.head)
                print(tabulate.tabulate(rows, ['T', '#'] + headers))

        return

    elif args.table:

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

                sys.exit(0)

    # If there are no other options, show info
    if not info_shown:
        show_info()



def mkmetatab():

    from metatab import MetatabDoc
    from rowpack import RowpackFormatError

    parser = argparse.ArgumentParser(
        prog='mkmetatab',
        description='Create a metatab file from one or more rowpack files. Version: {}'.format(__version__))

    parser.add_argument('paths', nargs='+', type=binary_type, help='File paths')

    args = parser.parse_args()

    doc = MetatabDoc()
    root = doc.new_section('Root')
    source_sec = doc.new_section('Sources', 'name start end headers comments encoding'.split())
    sch = doc.new_section('Schema', 'datatype description'.split())

    root.new_term('Declare', 'http://assets.metatab.org/metatab.csv')
    root.new_term('Title', '')

    source_names = set()

    for path in args.paths:

        try:
            with RowpackReader(path) as r:
                meta = r.meta

                if not meta.get('url'):
                    continue

                rowspec = meta.get('rowspec',{})

                base_name = name = path.replace('.rp','')
                name_index = 0
                while name in source_names:
                    name_index += 1
                    name = base_name+'-'+str(name_index)

                source_sec.new_term('Datafile',
                                    meta.get('url'),
                                    name=name,
                                    start=rowspec.get('start',1),
                                    end=rowspec.get('end',''),
                                    headers=','.join(str(e) for e in rowspec.get('headers',[0])),
                                    comments=','.join(str(e) for e in rowspec.get('comments', [])),
                                    encoding=meta.get('encoding'));


                if len(list(r.schema)):
                    t = sch.new_term('Table', name)

                    for c in r.schema:
                        t.new_child('Column', c.name, datatype=c.datatype)


        except RowpackFormatError:
            print "WARN: Not a rowpack file: {} ".format(path)

    doc.write_csv('metatab.csv')

if __name__ == '__main__':
    rowpack()
    sys.exit(0)
