# -*- coding: utf-8 -*-
# Copyright (c) 2016 Civic Knowledge. This file is licensed under the terms of the
# MIT License, included in this distribution as LICENSE.txt

"""
"""


import struct


EXTENSION = '.rowpack'
VERSION = 2
MAGIC = 'AMBRMPDF'

# 8s: Magic Number, H: Version,
# I: Number of rows, I: number of columns
# Q: Start of row data. Q: End of row data Q: End of metadata
FILE_HEADER_FORMAT = struct.Struct('>8sHIIQQQ')

FILE_HEADER_FORMAT_SIZE = FILE_HEADER_FORMAT.size


