# coding: utf-8
from __future__ import unicode_literals, absolute_import

import datetime
from dataclasses import dataclass
from typing import Tuple, List, Union, Type, Dict

from django.db import connections
from django.db.models import Model, AutoField
from django.db.models.fields import AutoFieldMixin

from fias import models as s_models
from fias.importer.table.table import AbstractTableList, Table
from fias.importer.validators import validators
from target import models as t_models
from target.config import DATABASE_ALIAS
from target.importer.signals import (
    pre_import_table, post_import_table
)


def sql_builder(connection, dst: Type[Model], src: Type[Model], raw_filter: str = None,
                field_map: Dict[str, str] = None, params: Type[Model] = None, type_map: List[Tuple[str, int]] = None,
                hierarchy: List[Tuple[Type[Model], str]] = None) -> str:
    pk_name = 'objectid'

    dst_fields = [f.column for f in dst._meta.fields if not isinstance(f, AutoFieldMixin)]
    if field_map is None:
        field_map = {}
    src_fields = list(map(lambda f: f'{field_map.get(f)} AS {f}' if f in field_map else f, dst_fields))

    if hierarchy is not None:
        h_s = []
        for i, (t, f) in enumerate(hierarchy):
            h_s.append(f'LEFT JOIN (SELECT {pk_name}, parentobjid AS {f} FROM {t._meta.db_table}'
                       f' WHERE isactive = true) AS h{i} USING ({pk_name})')
        hierarchy_s = ' '.join(h_s)
    else:
        hierarchy_s = ''

    if params is not None:
        param_type_ids_s = ', '.join(map(lambda i: f'({i})', (i for _, i in type_map)))
        ct_field_names = [pk_name] + [n for n, _ in type_map]
        ct_s = ', '.join(map(lambda f: f'{f} {dst._meta.get_field(f).db_type(connection)}', ct_field_names))
        params_s = f"""
        LEFT JOIN crosstab(
            'SELECT {pk_name}, typeid, value FROM {params._meta.db_table} ORDER BY objectid, typeid',
            'SELECT typeids FROM (values {param_type_ids_s}) t(typeids)'
        ) AS ct({ct_s}) USING ({pk_name})
        """
    else:
        params_s = ''

    if raw_filter is not None:
        where_s = f' WHERE {raw_filter}'
    else:
        where_s = ''

    return f"""
        INSERT INTO {dst._meta.db_table} ({", ".join(dst_fields)})
        SELECT {", ".join(src_fields)}
        FROM {src._meta.db_table}
        {params_s}
        {hierarchy_s}
        {where_s}
        """


_map = (
    (t_models.HouseType, s_models.HouseType, None, None, None, None, None),
    (t_models.HouseAddType, s_models.AddHouseType, None, None, None, None, None),
    (t_models.AddrObj, s_models.AddrObj, None, {'aolevel': 'level'}, s_models.AddrObjParam,
     [('okato', 6), ('oktmo', 7)], [(s_models.MunHierarchy, 'owner_mun'), (s_models.AdmHierarchy, 'owner_adm')]),
    (t_models.House, s_models.House, "region != '78'", None, s_models.HouseParam,
     [('postalcode', 5), ('okato', 6), ('oktmo', 7)], [(s_models.MunHierarchy, 'owner_mun'),
                                                       (s_models.AdmHierarchy, 'owner_adm')]),
    (t_models.House78, s_models.House, "region = '78'", None, s_models.HouseParam,
     [('postalcode', 5), ('okato', 6), ('oktmo', 7)], [(s_models.MunHierarchy, 'owner_mun'),
                                                       (s_models.AdmHierarchy, 'owner_adm')]),
)


def truncate():
    # TODO: optimization
    for table, _, _ in _main_models:
        table.objects.all().delete()


class TableLoader(object):

    def __init__(self):
        self.today = datetime.date.today()

    def validate(self, table: Table, item: Model):
        if item is None or item.pk is None:
            return False

        return validators.get(table.name, lambda x, **kwargs: True)(item, today=self.today)

    def load(self):
        pre_import_table.send(sender=self.__class__, table=None)
        self.do_load()
        post_import_table.send(sender=self.__class__, table=None)

    def do_load(self):
        connection = connections[DATABASE_ALIAS]
        for params in _map:
            with connection.cursor() as cursor:
                raw_sql = sql_builder(connection, *params)
                cursor.execute(raw_sql)


class TableUpdater(TableLoader):

    def __init__(self, limit: int = 10000):
        self.upd_limit = 100
        super(TableUpdater, self).__init__(limit=limit)

    def do_load(self, tablelist: AbstractTableList, table: Table):
        raise NotImplementedError
