# coding: utf-8
from __future__ import unicode_literals, absolute_import

import logging
from dataclasses import dataclass
from typing import Tuple, List, Union, Type, Dict, Any, cast, TYPE_CHECKING

from django.db import connections, transaction
from django.db.backends.base.base import BaseDatabaseWrapper
from django.db.models import Model
from django.db.models.fields import AutoFieldMixin, Field

from target.config import DATABASE_ALIAS
from target.importer.signals import pre_import_table, post_import_table

logger = logging.getLogger(__name__)


if TYPE_CHECKING:
    _Field = Field[Any, Any]
else:
    _Field = Field


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
    def filter_value(t1: Type[Model], f1: str, op: str, value: Any) -> str:
        if isinstance(value, str):
            value = f"'{value}'"
        return f"{t1._meta.db_table}.{f1} {op} {value}"

    @staticmethod
    def filter_query(t1: Type[Model], f1: str, include: bool, query: str) -> str:
        if not include:
            _not = "NOT "
        else:
            _not = ""
        return f"{t1._meta.db_table}.{f1} {_not}IN ({query})"

    @classmethod
    def insert(
        cls, dst: Type[Model], fields: Union[None, List[str]], values: Union[None, List[Any]], query: Union[None, str]
    ) -> str:
        if fields is None:
            fields = [f.column for f in dst._meta.fields if not isinstance(f, AutoFieldMixin)]
        return f"INSERT INTO {dst._meta.db_table} ({', '.join(fields)}) {values or query}"

    @classmethod
    def create(
        cls,
        connection: BaseDatabaseWrapper,
        dst: Type[Model],
        dst_pk: str,
        src: Type[Model],
        src_pk: str,
        filters: Union[None, List[str]],
        field_map: Union[None, Dict[str, str]],
        params: Union[None, ParamCfg],
        hierarchy: Union[None, List[HierarchyCfg]],
    ) -> str:
        dst_fields = [f.column for f in dst._meta.fields if not isinstance(f, AutoFieldMixin)]
        select = cls.select(connection, dst, src, src_pk, dst_fields, filters, field_map, params, hierarchy)
        return f"INSERT INTO {dst._meta.db_table} ({', '.join(dst_fields)}) {select}"

    @classmethod
    def update(
        cls,
        connection: BaseDatabaseWrapper,
        dst: Type[Model],
        dst_pk: str,
        src: Type[Model],
        src_pk: str,
        filters: Union[None, List[str]],
        field_map: Union[None, Dict[str, str]],
        params: Union[None, ParamCfg],
        hierarchy: Union[None, List[HierarchyCfg]],
    ) -> str:
        src_fields = [f.column for f in dst._meta.fields if not isinstance(f, AutoFieldMixin)]
        dst_fields = [f.column for f in dst._meta.fields if not (isinstance(f, AutoFieldMixin) or f.name == dst_pk)]
        tmp_select_table = "tmp_select_table"
        fields = ", ".join(map(lambda f: f"{f} = {tmp_select_table}.{f}", dst_fields))
        select = cls.select(connection, dst, src, src_pk, src_fields, filters, field_map, params, hierarchy)
        return (
            f"UPDATE {dst._meta.db_table} SET {fields}"
            f" FROM ({select}) AS {tmp_select_table}"
            f" WHERE {dst._meta.db_table}.{dst_pk} = {tmp_select_table}.{src_pk}"
        )

    @classmethod
    def select(
        cls,
        connection: BaseDatabaseWrapper,
        dst: Type[Model],
        src: Type[Model],
        src_pk: str,
        fields: List[str],
        filters: Union[None, List[str]],
        field_map: Union[None, Dict[str, str]],
        params: Union[None, ParamCfg],
        hierarchy: Union[None, List[HierarchyCfg]],
    ) -> str:
        all_src_fields = {f.column for f in src._meta.fields if not isinstance(f, AutoFieldMixin)}
        if field_map is None:
            field_map = {}
        src_fields = []
        for f in fields:
            if f in field_map:
                f = f"{src._meta.db_table}.{field_map.get(f)} AS {f}"
            elif f in all_src_fields:
                f = f"{src._meta.db_table}.{f}"
            src_fields.append(f)

        if hierarchy is not None:
            h_s = []
            for i, h_cfg in enumerate(hierarchy):
                h_s.append(
                    f"""LEFT JOIN (SELECT {h_cfg.pk}, {h_cfg.parent_pk} AS {h_cfg.parent_pk_as}
                           FROM {h_cfg.model._meta.db_table}
                           WHERE isactive = true) AS h{i}
                           ON h{i}.{h_cfg.pk} = {src._meta.db_table}.{src_pk}"""
                )
            hierarchy_s = " ".join(h_s)
        else:
            hierarchy_s = ""

        if params is not None:
            param_type_ids_s = ", ".join(map(lambda i: f"({i})", (i for _, i in params.type_map)))
            ct_field_names = [src_pk] + [n for n, _ in params.type_map]
            ct_fields = [dst._meta.get_field(f) for f in ct_field_names]
            if not all(map(lambda f: isinstance(f, Field), ct_fields)):
                raise ValueError
            ct_s = ", ".join(map(lambda f: f"{f.name} {f.db_type(connection)}", cast(List[_Field], ct_fields)))
            params_s = f"""
            LEFT JOIN crosstab(
                'SELECT {params.pk}, typeid, value FROM {params.model._meta.db_table} ORDER BY {params.pk}, typeid',
                'SELECT typeids FROM (values {param_type_ids_s}) t(typeids)'
            ) AS ct({ct_s}) ON {src._meta.db_table}.{src_pk} = ct.{params.pk}
            """
        else:
            params_s = ""

        if filters is not None:
            where_s = f' WHERE {" AND ".join(filters)}'
        else:
            where_s = ""

        return f"SELECT {', '.join(src_fields)} FROM {src._meta.db_table} {params_s} {hierarchy_s} {where_s}"

    @staticmethod
    def delete_on_field(dst: Type[Model], dst_field: str, src: Type[Model], src_field: str) -> str:
        return (
            f"DELETE FROM {dst._meta.db_table}"
            f" WHERE {dst_field} NOT IN (SELECT {src_field} FROM {src._meta.db_table})"
        )


@dataclass
class Cfg:
    dst: Type[Model]
    dst_pk: str
    src: Type[Model]
    src_pk: str
    filters: Union[None, List[Tuple[str, str, Any]]]
    field_map: Union[None, Dict[str, str]]
    params: Union[None, ParamCfg]
    hierarchy: Union[None, List[HierarchyCfg]]


def truncate(cfg: Cfg) -> None:
    cfg.dst.objects.all().delete()


class TableLoader(object):
    def load(self, cfg: Cfg, ver: int = 0) -> None:
        logger.info(f'Table "{cfg.dst._meta.object_name}" is loading.')
        pre_import_table.send(sender=self.__class__, cfg=cfg)
        self.do_load(cfg, ver)
        post_import_table.send(sender=self.__class__, cfg=cfg)
        logger.info(f'Table "{cfg.dst._meta.object_name}" has been loaded.')

    def do_load(self, cfg: Cfg, ver: int) -> None:
        connection = connections[DATABASE_ALIAS]
        with connection.cursor() as cursor:
            if cfg.filters is not None:
                filters = list(map(lambda f_op_v: SqlBuilder.filter_value(cfg.src, *f_op_v), cfg.filters))
            else:
                filters = None
            raw_sql = SqlBuilder.create(
                connection, cfg.dst, cfg.dst_pk, cfg.src, cfg.src_pk, filters, cfg.field_map, cfg.params, cfg.hierarchy
            )
            cursor.execute(raw_sql)


class TableUpdater(TableLoader):
    def do_load(self, cfg: Cfg, ver: int) -> None:
        connection = connections[DATABASE_ALIAS]
        with transaction.atomic():
            # Delete rows
            with connection.cursor() as cursor:
                query = SqlBuilder.delete_on_field(cfg.dst, cfg.dst_pk, cfg.src, cfg.src_pk)
                cursor.execute(query)

            current_obj_sql = SqlBuilder.select(
                connection, cfg.dst, cfg.dst, cfg.dst_pk, [cfg.dst_pk], None, None, None, None
            )
            if cfg.filters is not None:
                base_filters = list(map(lambda f_op_v: SqlBuilder.filter_value(cfg.src, *f_op_v), cfg.filters))
            else:
                base_filters = []
            # Update rows
            with connection.cursor() as cursor:
                filters = base_filters + [SqlBuilder.filter_value(cfg.src, "ver", ">", ver)]
                query = SqlBuilder.update(
                    connection,
                    cfg.dst,
                    cfg.dst_pk,
                    cfg.src,
                    cfg.src_pk,
                    filters,
                    cfg.field_map,
                    cfg.params,
                    cfg.hierarchy,
                )
                cursor.execute(query)

            # Create rows
            with connection.cursor() as cursor:
                filters = base_filters + [SqlBuilder.filter_query(cfg.src, cfg.src_pk, False, current_obj_sql)]
                query = SqlBuilder.create(
                    connection,
                    cfg.dst,
                    cfg.dst_pk,
                    cfg.src,
                    cfg.src_pk,
                    filters,
                    cfg.field_map,
                    cfg.params,
                    cfg.hierarchy,
                )
                cursor.execute(query)
