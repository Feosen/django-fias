# coding: utf-8
from __future__ import absolute_import, annotations, unicode_literals

from typing import IO, Any, Callable, Dict, Iterable, Type, Union

from django.db import connections, router

from fias.config import TABLE_ROW_FILTERS, TableName
from fias.models import (
    AbstractModel,
    AddHouseType,
    AddrObj,
    AddrObjParam,
    AddrObjType,
    AdmHierarchy,
    House,
    HouseParam,
    HouseType,
    MunHierarchy,
    ParamType,
)

table_names: Dict[TableName, Type[AbstractModel]] = {
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

assert len(table_names) == len(TableName)


def get_model(table: TableName) -> Type[AbstractModel]:
    return table_names[table]


name_trans: Dict[str, str] = {
    "houses": TableName.HOUSE,
    "house_types": TableName.HOUSE_TYPE,
    "addhouse_types": TableName.ADD_HOUSE_TYPE,
    "addr_obj_types": TableName.ADDR_OBJ_TYPE,
    "param_types": TableName.PARAM_TYPE,
    "houses_params": TableName.HOUSE_PARAM,
    "addr_obj_params": TableName.ADDR_OBJ_PARAM,
}


class UnregisteredTable(Exception):
    pass


class BadTableError(Exception):
    pass


class ParentLookupException(Exception):
    pass


class RowConvertor(object):
    def __init__(self, *args: Any, **kwargs: Any):
        pass

    def convert(self, row: Dict[str, Any]) -> Dict[str, Any]:
        raise NotImplementedError()

    def clear(self, row: Dict[str, Any]) -> Dict[str, Any]:
        raise NotImplementedError()


class TableIterator:
    _fd: Any
    model: Type[AbstractModel]
    row_convertor: RowConvertor
    _filters: Union[Iterable[Callable[[AbstractModel], Union[None, AbstractModel]]], None]

    _reverse_table_names = {v._meta.object_name: k for k, v in table_names.items()}

    def __init__(self, fd: Any, model: Type[AbstractModel], row_convertor: RowConvertor):
        self._fd = fd
        self.model = model
        self.row_convertor = row_convertor
        self._filters = TABLE_ROW_FILTERS.get(self._reverse_table_names[self.model._meta.object_name], None)

    def __iter__(self) -> Union[TableIterator]:
        return self

    def get_next(self) -> Union[AbstractModel, None]:
        raise NotImplementedError()

    def format_row(self, row: Dict[str, Any]) -> Dict[str, Any]:
        raise NotImplementedError()

    def process_row(self, row: Dict[str, Any]) -> Union[AbstractModel, None]:
        try:
            row = dict(self.format_row(row))
        except ParentLookupException:
            return None

        row = self.row_convertor.convert(row)
        row = self.row_convertor.clear(row)

        item: AbstractModel = self.model(**row)
        if self._filters is not None:
            for filter_func in self._filters:
                filtered_item = filter_func(item)
                if filtered_item is None:
                    return None
                item = filtered_item

        return item

    def __next__(self) -> Union[AbstractModel, None]:
        return self.get_next()

    next = __next__


class AbstractTableList:
    def open(self, filename: str) -> IO[bytes]:
        raise NotImplementedError()

    def __getstate__(self) -> Dict[str, Any]:
        raise NotImplementedError()

    def __setstate__(self, state: Dict[str, Any]) -> None:
        raise NotImplementedError()


class Table(object):
    name: TableName
    model: Type[AbstractModel]
    deleted: bool
    region: Union[str, None]
    ver: int
    iterator_class: Type[TableIterator] = TableIterator

    def __init__(
        self, filename: str, name: str, ver: int, deleted: bool | None = None, region: str | None = None, **kwargs: Any
    ):
        self.filename = filename

        name_lower = name.lower()
        try:
            self.name = TableName(name_trans.get(name_lower, name_lower))
        except ValueError:
            raise UnregisteredTable(name)

        self.model = table_names[self.name]
        self.deleted = bool(deleted)
        self.region = region
        self.ver = ver

    def _truncate(self, model: Type[AbstractModel]) -> None:
        db_table = model._meta.db_table
        connection = connections[router.db_for_write(model)]
        cursor = connection.cursor()

        if connection.vendor == "postgresql":
            cursor.execute(f"TRUNCATE TABLE {db_table} RESTART IDENTITY CASCADE")
        elif connection.vendor == "mysql":
            cursor.execute(f"TRUNCATE TABLE `{db_table}`")
        else:
            cursor.execute(f"DELETE FROM {db_table}")

    def truncate(self) -> None:
        self._truncate(self.model)

    def open(self, tablelist: AbstractTableList) -> IO[bytes]:
        return tablelist.open(self.filename)

    def rows(self, tablelist: AbstractTableList) -> TableIterator:
        raise NotImplementedError()
