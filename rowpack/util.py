# -*- coding: utf-8 -*-
# Copyright (c) 2016 Civic Knowledge. This file is licensed under the terms of the
# MIT License, included in this distribution as LICENSE.txt

"""
"""

import datetime


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