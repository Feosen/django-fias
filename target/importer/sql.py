# coding: utf-8
from __future__ import absolute_import, unicode_literals

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Dict, List, Tuple, Type, Union, cast

from django.db.backends.base.base import BaseDatabaseWrapper
from django.db.models import Model
from django.db.models.fields import AutoFieldMixin, Field

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
        elif value is None:
            value = "NULL"
        return f"{t1._meta.db_table}.{f1} {op} {value}"

    @staticmethod
    def filter_query(t1: Type[Model], f1: str, include: bool, query: str) -> str:
        if not include:
            _not = "NOT "
        else:
            _not = ""
        return f"{t1._meta.db_table}.{f1} {_not}IN ({query})"

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

        hierarchy_fields = [h.parent_pk_as for h in hierarchy] if hierarchy is not None else []
        src_fields = []
        for field_name in fields:
            if field_name in field_map:
                field_name = f"{src._meta.db_table}.{field_map.get(field_name)} AS {field_name}"
            elif field_name in all_src_fields:
                field_name = f"{src._meta.db_table}.{field_name}"
            if field_name in hierarchy_fields:
                field_name = f"COALESCE({field_name}, 0) AS {field_name}"
            src_fields.append(field_name)

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
            ct_l = []
            for field in cast(List[_Field], ct_fields):
                db_type = field.db_type(connection)
                if db_type is not None:
                    ct_l.append(f"{field.name} {db_type.upper()}")
                else:
                    raise ValueError
            ct_s = ", ".join(ct_l)
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
        table = dst._meta.db_table
        if dst._meta.pk is None:
            raise ValueError
        pk_field_name = dst._meta.pk.column
        other_table = src._meta.db_table
        return f"""
            DELETE
            FROM {table}
            WHERE {pk_field_name} IN (
            SELECT {table}.{pk_field_name}
            FROM {table} LEFT JOIN {other_table} ON {table}.{dst_field} = {other_table}.{src_field}
            WHERE {other_table}.{src_field} IS NULL
            )"""
