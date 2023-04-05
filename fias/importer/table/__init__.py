# coding: utf-8
from __future__ import unicode_literals, absolute_import

import re
from typing import Union

from .table import BadTableError, Table
from .xml import XMLTable

table_xml_pattern = r'((?P<region>\d{2})/)?as_(?P<deleted>del_)?(?P<name>[a-z_]+)_(?P<date>\d+)_(?P<uuid>[a-z0-9-]{36}).xml'
table_xml_re = re.compile(table_xml_pattern, re.I)


class BadTableNameError(Exception):
    pass


class TableFactory(object):

    @staticmethod
    def parse(filename: str, extra: dict) -> Union[Table, None]:
        m = table_xml_re.match(filename)
        if m is not None:
            cls = XMLTable
            return cls(filename=filename, **m.groupdict(), **extra)

        return None
