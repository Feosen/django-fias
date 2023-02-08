# coding: utf-8
from __future__ import unicode_literals, absolute_import

import datetime
from typing import Type, Iterable

from django.db import models
from django.db.models import Model
from lxml import etree

from .table import BadTableError, Table, TableIterator, AbstractTableList, RowConvertor

_bom_header = b'\xef\xbb\xbf'


field_map = {
    #'house': {
    #    'id': 'recordid',
    #}
}


class XMLIterator(TableIterator):

    def __init__(self, fd, model: Type[Model], row_convertor: RowConvertor):
        super(XMLIterator, self).__init__(fd=fd, model=model, row_convertor=row_convertor)

        self.related_fields = dict({
            (f.name, f.remote_field.model) for f in self.model._meta.get_fields()
            if f.one_to_one or f.many_to_one
        })

        self.uuid_fields = dict({
            (f.name, f) for f in self.model._meta.get_fields()
            if isinstance(f, models.UUIDField)
        })

        self.date_fields = dict({
            (f.name, f) for f in self.model._meta.get_fields()
            if isinstance(f, models.DateField)
        })

        self.int_fields = dict({
            (f.name, f) for f in self.model._meta.get_fields()
            if isinstance(f, models.IntegerField)
        })

        self.boolean_fields = dict({
            (f.name, f) for f in self.model._meta.get_fields()
            if isinstance(f, models.BooleanField)
        })

        self._context = etree.iterparse(self._fd)

    def format_row(self, row):
        for key, value in row.items():
            key = key.lower()
            if key in self.uuid_fields:
                yield (key, value or None)
            elif key in self.date_fields:
                if value == '' or value is None:
                    yield (key, None)
                else:
                    try:
                        _date = datetime.datetime.strptime(value, "%Y-%m-%d").date()
                    except ValueError:
                        _date = datetime.datetime.strptime(value, "%d.%m.%y %H:%M:%S").date()
                    yield (key, _date)
            elif key in self.related_fields:
                if value == '':
                    value = None
                yield ('{0}_id'.format(key), value)
            elif key in self.int_fields:
                if value == '':
                    value = None
                else:
                    value = int(value)
                yield (key, value)
            elif key in self.boolean_fields:
                yield (key, value in ('1', 'y', 'yes', 't', 'true', 'on', '+'))
            else:
                yield (key, value)

    def get_next(self):
        event, row = next(self._context)
        item = self.process_row(row)
        row.clear()
        while row.getprevious() is not None:
            del row.getparent()[0]

        return item


class XMLRowConvertor(RowConvertor):
    field_map: dict
    fields: set
    values: dict

    def __init__(self, model: Type[Model], region: str, table_name: str):
        super(XMLRowConvertor, self).__init__()
        self.field_map = field_map.get(table_name, {})
        self.fields = set(f.name for f in model._meta.get_fields())
        self.values = {}
        if region:
            self.values['region'] = region

    def convert(self, row: dict) -> dict:
        for k, v in self.field_map.items():
            row[v] = row.pop(k)
        return self.values | row

    def clear(self, row: dict) -> dict:
        for k in set(row.keys()) - self.fields:
            del row[k]
        return row


class XMLTable(Table):
    iterator = XMLIterator
    row_convertor = XMLRowConvertor

    def __init__(self, filename: str, **kwargs):
        super(XMLTable, self).__init__(filename=filename, **kwargs)

    def rows(self, tablelist: AbstractTableList) -> Iterable:
        if self.deleted:
            return []

        xml = self.open(tablelist=tablelist)

        # workaround for XMLSyntaxError: Document is empty, line 1, column 1
        bom = xml.read(3)
        if bom != _bom_header:
            xml = self.open(tablelist=tablelist)
        else:
            #log.info('Fixed wrong BOM header')
            pass

        try:
            return self.iterator(xml, self.model, self.row_convertor(self.model, self.region, self.name))
        except etree.XMLSyntaxError as e:
            raise BadTableError('Error occured during opening table `{0}`: {1}'.format(self.name, str(e)))
