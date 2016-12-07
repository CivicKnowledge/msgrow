# -*- coding: utf-8 -*-
# Copyright (c) 2016 Civic Knowledge. This file is licensed under the terms of the
# MIT License, included in this distribution as LICENSE.txt

"""
Try to automatically ingest row data from a URL into a Rowpack file.
"""

from . import RowpackWriter, RowpackReader, intuit_rows, intuit_types, run_stats, IngestionError
from os.path import abspath

def get_cache():
    from fs.opener import fsopendir
    import tempfile

    return fsopendir(tempfile.gettempdir())


def ingest(url, path=None, cache=None, encoding=None, filetype=None, urlfiletype=None, cb=None):
    """

    :param url:
    :param path:
    :param cache:
    :param encoding:
    :param filetype:
    :param urlfiletype:
    :return:
    """

    from rowgenerators import SourceSpec

    from tableintuit.exceptions import RowIntuitError
    import sys

    warnings = []

    # There are certainly better ways to do this, like chardet or UnicodeDammit,
    # but in several years, I've never seen a data file that wasn't ascii, utf8 or latin1,
    # so i'm punting. Until there is a better solution, users should use a caracter detecting program,
    # then explicitly set the encoding parameter.

    if encoding is None:
        encodings = ('ascii', 'utf8', 'latin1')
    else:
        encodings = (encoding,)

    if cache is None:
        cache = get_cache()

    in_path = path

    for encoding in encodings:

        d = dict(
            url=url,
            encoding=encoding,
            filetype=filetype,
            urlfiletype=urlfiletype
        )

        ss = resolve_url(SourceSpec(**d), cache)
        gen = ss.get_generator(cache)

        if not in_path:
            path = abspath(ss.file_name + '.rp')
        else:
            path = in_path

        try:
            with RowpackWriter(path) as w:
                for row in gen:
                    w.write_row(row)
                w.meta['encoding'] = encoding
                w.meta['url'] = url
                break
        except UnicodeDecodeError:
            warnings.append("WARNING: encoding failed, trying another")
            if cb:
                cb(warnings[-1])
            continue

    else:
        raise IngestionError("ERROR: all encodings failed")

    # Need to re-open b/c n_rows isn't set until the writer is closed
    with RowpackReader(path) as r:
        if cb:
            cb("Wrote {} rows".format(r.n_rows))

    try:
        ri = intuit_rows(path)

        if ri.start_line < 1:
            warnings.append("WARNING: Row intuition could not find start line; skipping type intuition and stats"+
                            "Set row types manually with -H -e ")
            if cb:
                cb(warnings[-1])
        else:
            intuit_types(path)
            run_stats(path)

    except RowIntuitError as e:
        raise

    with RowpackWriter(path, 'r+b') as w:
        w.meta['sourcespec'] = ss.dict

    return path, encoding, warnings

def resolve_url(ss, cache):
    """Return a list of sub-components of a Spec, such as files in a ZIP archive,
    or worksheed in a spreadsheet"""

    from rowgenerators.fetch import inspect

    while True:
        specs = inspect(ss, cache)

        if not specs:
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
