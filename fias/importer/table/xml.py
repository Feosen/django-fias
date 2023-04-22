# coding: utf-8
from __future__ import absolute_import, unicode_literals

import datetime
from typing import Any, Dict, Set, Type, Union

from django.db import models
from lxml import etree

from ...models import AbstractModel
from .table import AbstractTableList, BadTableError, RowConvertor, Table, TableIterator

_bom_header = b"\xef\xbb\xbf"


field_map: Dict[str, Dict[str, str]] = {
    # 'house': {
    #    'id': 'recordid',
    # }
}


class XMLIterator(TableIterator):
    def __init__(self, fd: Any, model: Type[AbstractModel], row_convertor: RowConvertor):
        super(XMLIterator, self).__init__(fd=fd, model=model, row_convertor=row_convertor)

        self.related_fields = dict(
            {
                (f.name, f.remote_field.model)
                for f in self.model._meta.get_fields()  # type: ignore
                if f.one_to_one or f.many_to_one
            }
        )

        self.uuid_fields = dict({(f.name, f) for f in self.model._meta.get_fields() if isinstance(f, models.UUIDField)})

        self.date_fields = dict({(f.name, f) for f in self.model._meta.get_fields() if isinstance(f, models.DateField)})

        self.int_fields = dict(
            {(f.name, f) for f in self.model._meta.get_fields() if isinstance(f, models.IntegerField)}
        )

        self.boolean_fields = dict(
            {(f.name, f) for f in self.model._meta.get_fields() if isinstance(f, models.BooleanField)}
        )

        self._context = etree.iterparse(self._fd)

    def format_row(self, row: Dict[str, Any]) -> Dict[str, Any]:
        res = {}
        for key, value in row.items():
            key = key.lower()
            if key in self.uuid_fields:
                res[key] = value or None
            elif key in self.date_fields:
                if value == "" or value is None:
                    res[key] = None
                else:
                    try:
                        _date = datetime.datetime.strptime(value, "%Y-%m-%d").date()
                    except ValueError:
                        _date = datetime.datetime.strptime(value, "%d.%m.%y %H:%M:%S").date()
                    res[key] = _date
            elif key in self.related_fields:
                if value == "":
                    value = None
                res[f"{key}_id"] = value
            elif key in self.int_fields:
                if value == "":
                    value = None
                else:
                    value = int(value)
                res[key] = value
            elif key in self.boolean_fields:
                res[key] = value in ("1", "y", "yes", "t", "true", "on", "+")
            else:
                res[key] = value
        return res

    def get_next(self) -> Union[AbstractModel, None]:
        event, row = next(self._context)
        item = self.process_row(row)
        row.clear()
        while row.getprevious() is not None:
            del row.getparent()[0]

        return item


class XMLRowConvertor(RowConvertor):
    field_map: Dict[str, str]
    fields: Set[str]
    values: Dict[str, Any]

    def __init__(self, model: Type[AbstractModel], table_name: str, extra: Union[Dict[str, Any], None] = None):
        super(XMLRowConvertor, self).__init__()
        self.field_map = field_map.get(table_name, {})
        self.fields = set(f.name for f in model._meta.get_fields())
        self.values = {}
        if extra is not None:
            self.values.update(extra)

    def convert(self, row: Dict[str, Any]) -> Dict[str, Any]:
        for k, v in self.field_map.items():
            row[v] = row.pop(k)
        return self.values | row

    def clear(self, row: Dict[str, Any]) -> Dict[str, Any]:
        for k in set(row.keys()) - self.fields:
            del row[k]
        return row


class XMLTable(Table):
    iterator_class: Type[TableIterator] = XMLIterator
    row_convertor_class: Type[RowConvertor] = XMLRowConvertor

    def __init__(
        self,
        filename: str,
        name: str,
        ver: int,
        deleted: Union[bool, None] = None,
        region: Union[str, None] = None,
        **kwargs: Any,
    ):
        super(XMLTable, self).__init__(filename, name, ver, deleted, region, **kwargs)

    def rows(self, tablelist: AbstractTableList) -> TableIterator:
        if self.deleted:
            raise StopIteration

        xml = self.open(tablelist=tablelist)

        # workaround for XMLSyntaxError: Document is empty, line 1, column 1
        bom = xml.read(3)
        if bom != _bom_header:
            xml = self.open(tablelist=tablelist)
        else:
            # log.info('Fixed wrong BOM header')
            pass

        try:
            row_convertor = self.row_convertor_class(self.model, self.name, {"ver": self.ver, "region": self.region})
            return self.iterator_class(xml, self.model, row_convertor)
        except etree.XMLSyntaxError as e:
            raise BadTableError("Error occured during opening table `{0}`: {1}".format(self.name, str(e)))
