# coding: utf-8
from __future__ import absolute_import, unicode_literals

import logging
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Dict, List, Tuple, Type, Union

from django.db import connections, transaction
from django.db.models import Model
from django.db.models.fields import Field

from target.config import DATABASE_ALIAS
from target.importer.signals import post_import_table, pre_import_table
from target.importer.sql import HierarchyCfg, ParamCfg, SqlBuilder

logger = logging.getLogger(__name__)


if TYPE_CHECKING:
    _Field = Field[Any, Any]
else:
    _Field = Field


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
    def load(self, cfg: Cfg, first_call: bool = True, last_call: bool = True) -> None:
        logger.info(f'Table "{cfg.dst._meta.object_name}" is loading.')
        pre_import_table.send(sender=self.__class__, cfg=cfg)
        self.do_load(cfg, first_call, last_call)
        post_import_table.send(sender=self.__class__, cfg=cfg)
        logger.info(f'Table "{cfg.dst._meta.object_name}" has been loaded.')

    def do_load(self, cfg: Cfg, first_call: bool, last_call: bool) -> None:
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
    def do_load(self, cfg: Cfg, first_call: bool, last_call: bool) -> None:
        connection = connections[DATABASE_ALIAS]
        with transaction.atomic():
            # Delete rows
            if first_call:
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
                filters = base_filters
                query = SqlBuilder.update(
                    connection,
                    cfg.dst,
                    cfg.dst_pk,
                    cfg.src,
                    cfg.src_pk,
                    filters or None,
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
