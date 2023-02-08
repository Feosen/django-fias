# coding: utf-8
from __future__ import unicode_literals, absolute_import

from typing import Union, Type, Iterable, Any, IO, Callable, Tuple

from django.db import connections, router
from django.db.models import Model

from fias.config import TABLE_ROW_FILTERS, TableName
from fias.models import (AddrObjType, AddrObj, AddrObjParam, House, AddHouseType, HouseType, HouseParam, ParamType,
                         AdmHierarchy, MunHierarchy)

table_names = {
    TableName.HOUSE: House,
    TableName.HOUSE_TYPE: HouseType,
    TableName.ADD_HOUSE_TYPE: AddHouseType,
    TableName.HOUSE_PARAM: HouseParam,
    TableName.ADDR_OBJ: AddrObj,
    TableName.ADDR_OBJ_TYPE: AddrObjType,
    TableName.ADDR_OBJ_PARAM: AddrObjParam,
    TableName.PARAM_TYPE: ParamType,
    TableName.ADM_HIERARCHY: AdmHierarchy,
    TableName.MUN_HIERARCHY: MunHierarchy,
}

name_trans = {
    'houses': TableName.HOUSE,
    'house_types': TableName.HOUSE_TYPE,
    'addhouse_types': TableName.ADD_HOUSE_TYPE,
    'addr_obj_types': TableName.ADDR_OBJ_TYPE,
    'param_types': TableName.PARAM_TYPE,
    'houses_params': TableName.HOUSE_PARAM,
    'addr_obj_params': TableName.ADDR_OBJ_PARAM,
}


class BadTableError(Exception):
    pass


class ParentLookupException(Exception):
    pass


class RowConvertor(object):

    def convert(self, row: dict) -> dict:
        raise NotImplementedError()

    def clear(self, row: dict) -> dict:
        raise NotImplementedError()


class TableIterator(object):
    _fd: Any
    model: Type[Model]
    row_convertor: RowConvertor
    _filters: Tuple[Callable[[Model], Union[None, Model]]]

    _reverse_table_names = {v._meta.object_name: k for k, v in table_names.items()}

    def __init__(self, fd: Any, model: Type[Model], row_convertor: RowConvertor):
        self._fd = fd
        self.model = model
        self.row_convertor = row_convertor
        self._filters = TABLE_ROW_FILTERS.get(self._reverse_table_names[self.model._meta.object_name], tuple())

    def __iter__(self):
        if self.model is None:
            return []

        return self

    def get_context(self):
        raise NotImplementedError()

    def get_next(self):
        raise NotImplementedError()

    def format_row(self, row):
        raise NotImplementedError()

    def process_row(self, row: dict) -> Union[Model, None]:
        try:
            row = dict(self.format_row(row))
        except ParentLookupException as e:
            return None

        row = self.row_convertor.convert(row)
        row = self.row_convertor.clear(row)

        item = self.model(**row)
        for filter_func in self._filters:
            item = filter_func(item)
            if item is None:
                return None

        return item

    def __next__(self):
        return self.get_next()

    next = __next__


class AbstractTableList:
    def open(self, filename: str) -> IO:
        raise NotImplementedError()


class Table(object):
    name: str = None
    model: Type[Model] = None
    deleted: bool = False
    region: Union[str, None] = None
    iterator: TableIterator = TableIterator

    def __init__(self, filename: str, **kwargs):
        self.filename = filename

        name = kwargs['name'].lower()

        self.name = name_trans.get(name, name)
        self.model = table_names.get(self.name)

        self.deleted = bool(kwargs.get('deleted', False))
        self.region = kwargs.get('region', None)

    def _truncate(self, model: Type[Model]) -> None:
        db_table = model._meta.db_table
        connection = connections[router.db_for_write(model)]
        cursor = connection.cursor()

        if connection.vendor == 'postgresql':
            cursor.execute('TRUNCATE TABLE {0} RESTART IDENTITY CASCADE'.format(db_table))
        elif connection.vendor == 'mysql':
            cursor.execute('TRUNCATE TABLE `{0}`'.format(db_table))
        else:
            cursor.execute('DELETE FROM {0}'.format(db_table))

    def truncate(self) -> None:
        self._truncate(self.model)

    def open(self, tablelist: AbstractTableList) -> IO:
        return tablelist.open(self.filename)

    def rows(self, tablelist: AbstractTableList) -> Iterable:
        raise NotImplementedError()
