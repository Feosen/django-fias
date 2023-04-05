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


@dataclass
class ParamCfg:
    model: Type[Model]
    pk: str
    type_map: List[Tuple[str, int]]


@dataclass
class HierarchyCfg:
    model: Type[Model]
    pk: str
    parent_pk: str
    parent_pk_as: str


class SqlBuilder:

    @staticmethod
    def create(connection, dst: Type[Model], dst_pk: str, src: Type[Model], src_pk: str, raw_filter: str = None,
               field_map: Dict[str, str] = None, params: ParamCfg = None,
               hierarchy: List[HierarchyCfg] = None) -> str:

        dst_fields = [f.column for f in dst._meta.fields if not isinstance(f, AutoFieldMixin)]
        all_src_fields = {f.column for f in src._meta.fields if not isinstance(f, AutoFieldMixin)}
        if field_map is None:
            field_map = {}
        src_fields = []
        for f in dst_fields:
            if f in field_map:
                f = f'{src._meta.db_table}.{field_map.get(f)} AS {f}'
            elif f in all_src_fields:
                f = f'{src._meta.db_table}.{f}'
            src_fields.append(f)

        if hierarchy is not None:
            h_s = []
            for i, h_cfg in enumerate(hierarchy):
                h_s.append(f'''LEFT JOIN (SELECT {h_cfg.pk}, {h_cfg.parent_pk} AS {h_cfg.parent_pk_as}
                           FROM {h_cfg.model._meta.db_table}
                           WHERE isactive = true) AS h{i}
                           ON h{i}.{h_cfg.pk} = {src._meta.db_table}.{src_pk}''')
            hierarchy_s = ' '.join(h_s)
        else:
            hierarchy_s = ''

        if params is not None:
            param_type_ids_s = ', '.join(map(lambda i: f'({i})', (i for _, i in params.type_map)))
            ct_field_names = [dst_pk] + [n for n, _ in params.type_map]
            ct_s = ', '.join(map(lambda f: f'{f} {dst._meta.get_field(f).db_type(connection)}', ct_field_names))
            params_s = f"""
            LEFT JOIN crosstab(
                'SELECT {params.pk}, typeid, value FROM {params.model._meta.db_table} ORDER BY {params.pk}, typeid',
                'SELECT typeids FROM (values {param_type_ids_s}) t(typeids)'
            ) AS ct({ct_s}) ON {src._meta.db_table}.{src_pk} = ct.{params.pk}
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

    @staticmethod
    def delete_on_field(dst: Type[Model], dst_field: str, src: Type[Model], src_field: str) -> str:
        return f"""
            DELETE FROM {dst._meta.db_table}
            WHERE {dst_field} NOT IN (SELECT {src_field} FROM {src._meta.db_table})
            """


@dataclass
class Cfg:
    dst: Type[Model]
    dst_pk: str
    src: Type[Model]
    src_pk: str
    raw_filter: Union[None, str]
    field_map: Union[None, Dict[str, str]]
    params: Union[None, ParamCfg]
    hierarchy: Union[None, List[HierarchyCfg]]


_map: List[Cfg] = [
    Cfg(t_models.HouseType, 'id', s_models.HouseType, 'id', None, None, None, None),
    Cfg(t_models.HouseAddType, 'id', s_models.AddHouseType, 'id', None, None, None, None),
    Cfg(t_models.AddrObj, 'objectid', s_models.AddrObj, 'objectid', None, {'aolevel': 'level'},
        ParamCfg(s_models.AddrObjParam, 'objectid', [('okato', 6), ('oktmo', 7)]),
        [HierarchyCfg(s_models.MunHierarchy, 'objectid', 'parentobjid', 'owner_mun'),
         HierarchyCfg(s_models.AdmHierarchy, 'objectid', 'parentobjid', 'owner_adm')]),
    Cfg(t_models.House, 'objectid', s_models.House, 'objectid', "region != '78'", None,
        ParamCfg(s_models.HouseParam, 'objectid', [('postalcode', 5), ('okato', 6), ('oktmo', 7)]),
        [HierarchyCfg(s_models.MunHierarchy, 'objectid', 'parentobjid', 'owner_mun'),
         HierarchyCfg(s_models.AdmHierarchy, 'objectid', 'parentobjid', 'owner_adm')]),
    Cfg(t_models.House78, 'objectid', s_models.House, 'objectid', "region = '78'", None,
        ParamCfg(s_models.HouseParam, 'objectid', [('postalcode', 5), ('okato', 6), ('oktmo', 7)]),
        [HierarchyCfg(s_models.MunHierarchy, 'objectid', 'parentobjid', 'owner_mun'),
         HierarchyCfg(s_models.AdmHierarchy, 'objectid', 'parentobjid', 'owner_adm')]),
]


def truncate():
    # TODO: optimization
    for table, _, _ in _main_models:
        table.objects.all().delete()


class TableLoader(object):
    def load(self):
        pre_import_table.send(sender=self.__class__, table=None)
        self.do_load()
        post_import_table.send(sender=self.__class__, table=None)

    def do_load(self):
        connection = connections[DATABASE_ALIAS]
        for cfg in _map:
            with connection.cursor() as cursor:
                raw_sql = SqlBuilder.create(connection, cfg.dst, cfg.dst_pk, cfg.src, cfg.src_pk, cfg.raw_filter,
                                            cfg.field_map, cfg.params, cfg.hierarchy)
                cursor.execute(raw_sql)


class TableUpdater(TableLoader):

    def do_load(self):
        connection = connections[DATABASE_ALIAS]
        for cfg in _map:
            pass
            # TODO: delete rows
            with connection.cursor() as cursor:
                raw_sql = SqlBuilder.delete_on_field(cfg.dst, cfg.dst_pk, cfg.src, cfg.src_pk)
                cursor.execute(raw_sql)
            # TODO: update rows
            # TODO: create rows
