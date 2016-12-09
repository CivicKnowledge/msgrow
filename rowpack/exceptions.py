# -*- coding: utf-8 -*-
# Copyright (c) 2016 Civic Knowledge. This file is licensed under the terms of the
# MIT License, included in this distribution as LICENSE.txt

"""

Exceptions

"""


class RowpackError(Exception):
    pass

class IngestionError(RowpackError):
    pass

class RowpackFormatError(RowpackError):
    pass
