# coding: utf-8
from __future__ import absolute_import, unicode_literals

import re
from typing import Any, Union

from .table import BadTableError, Table, UnregisteredTable
from .xml import XMLTable

table_xml_pattern = (
    r"((?P<region>\d{2})/)?as_(?P<deleted>del_)?(?P<name>[a-z_]+)_(?P<date>\d+)_(?P<uuid>[a-z0-9-]{36}).xml"
)
table_xml_re = re.compile(table_xml_pattern, re.I)


__all__ = ["BadTableError", "TableFactory"]


class BadTableNameError(Exception):
    pass


class TableFactory(object):
    @staticmethod
    def parse(filename: str, extra: Any) -> Union[Table, None]:
        m = table_xml_re.match(filename)
        if m is not None:
            params = m.groupdict() | extra
            try:
                return XMLTable(filename=filename, **params)
            except UnregisteredTable:
                pass

        return None
