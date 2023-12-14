# coding: utf-8
from __future__ import absolute_import, unicode_literals

import copy
import logging
from dataclasses import dataclass
from typing import Any, Callable, Generator, Iterable, List, Tuple, Type

from django.db.models import Max, Min

from fias import models as s_models
from gar_loader.indexes import remove_indexes_from_model, restore_indexes_for_model
from target import models as t_models
from target.config import LOAD_HOUSE_78_ONLY, LOAD_HOUSE_BULK_SIZE
from target.importer.loader import Cfg, TableLoader, TableUpdater
from target.importer.loader import truncate as table_truncate
from target.importer.signals import (
    post_drop_indexes,
    post_import,
    post_restore_indexes,
    post_update,
    pre_drop_indexes,
    pre_import,
    pre_restore_indexes,
    pre_update,
)
from target.importer.sql import HierarchyCfg, ParamCfg
from target.models import AbstractHouse

logger = logging.getLogger(__name__)


@dataclass
class TableCfg:
    cfg: Cfg
    fn: Callable[[], Iterable[Tuple[Cfg, str]]] | None


def id_gen(first: int, last: int, step: int) -> Generator[Tuple[int, int], None, None]:
    assert last >= first >= 0
    while first <= last:
        prev_first = first
        first += step
        yield prev_first, min(first, last + 1)


def bulk_house_factory(target: Type[AbstractHouse]) -> Callable[[], Iterable[Tuple[Cfg, str]]]:
    assert target in (t_models.House78, t_models.House)

    extra_filters = []
    if target == t_models.House78:
        extra_filters.append(("region", "=", "78"))

    def build_cfg(args: Tuple[int, int]) -> Tuple[Cfg, str]:
        min_objectid, max_objectid = args

        filters: List[Tuple[str, str, Any]] = extra_filters + [
            ("objectid", ">=", min_objectid),
            ("objectid", "<", max_objectid),
        ]

        cfg = Cfg(
            target,
            "objectid",
            s_models.House,
            "objectid",
            filters,
            None,
            ParamCfg(
                s_models.HouseParam,
                "objectid",
                [("postalcode", 5), ("okato", 6), ("oktmo", 7)],
                filters,
            ),
            [
                HierarchyCfg(
                    s_models.AdmHierarchy,
                    "objectid",
                    "parentobjid",
                    "owner_adm",
                    filters,
                ),
            ],
        )
        desc = f"objectid in [{min_objectid}; {max_objectid})"
        return cfg, desc

    def bulk_houses() -> Iterable[Tuple[Cfg, str]]:
        house_statistic = s_models.House.objects.aggregate(min=Min("objectid"), max=Max("objectid"))
        return map(build_cfg, id_gen(house_statistic["min"], house_statistic["max"], LOAD_HOUSE_BULK_SIZE))

    return bulk_houses


def get_table_cfg() -> List[TableCfg]:
    table_cfg: List[TableCfg] = [
        TableCfg(Cfg(t_models.HouseType, "id", s_models.HouseType, "id", None, None, None, None), None),
        TableCfg(Cfg(t_models.HouseAddType, "id", s_models.AddHouseType, "id", None, None, None, None), None),
        TableCfg(
            Cfg(
                t_models.AddrObj,
                "objectid",
                s_models.AddrObj,
                "objectid",
                None,
                {"aolevel": "level"},
                ParamCfg(s_models.AddrObjParam, "objectid", [("okato", 6), ("oktmo", 7)], None),
                [
                    HierarchyCfg(s_models.AdmHierarchy, "objectid", "parentobjid", "owner_adm", None),
                ],
            ),
            None,
        ),
    ]

    house_78_cfg = Cfg(
        t_models.House78,
        "objectid",
        s_models.House,
        "objectid",
        [("region", "=", "78")],
        None,
        ParamCfg(
            s_models.HouseParam,
            "objectid",
            [("postalcode", 5), ("okato", 6), ("oktmo", 7)],
            [("region", "=", "78")],
        ),
        [
            HierarchyCfg(s_models.AdmHierarchy, "objectid", "parentobjid", "owner_adm", [("region", "=", "78")]),
        ],
    )

    house_cfg = Cfg(
        t_models.House,
        "objectid",
        s_models.House,
        "objectid",
        None,
        None,
        ParamCfg(s_models.HouseParam, "objectid", [("postalcode", 5), ("okato", 6), ("oktmo", 7)], None),
        [
            HierarchyCfg(s_models.AdmHierarchy, "objectid", "parentobjid", "owner_adm", None),
        ],
    )

    if LOAD_HOUSE_BULK_SIZE <= 0:
        table_cfg.append(TableCfg(house_78_cfg, None))
    else:
        table_cfg.append(TableCfg(house_78_cfg, bulk_house_factory(t_models.House78)))

    if not LOAD_HOUSE_78_ONLY:
        if LOAD_HOUSE_BULK_SIZE <= 0:
            table_cfg.append(TableCfg(house_cfg, None))
        else:
            table_cfg.append(TableCfg(house_cfg, bulk_house_factory(t_models.House)))

    return table_cfg


def load_complete_data(truncate: bool = False, keep_indexes: bool = False, keep_pk: bool = True) -> None:
    ver = s_models.Status.objects.order_by("ver").first()
    if ver is None:
        raise ValueError
    logger.info(f"Loading data v.{ver.ver_id}.")
    pre_import.send(sender=object.__class__, version=ver.ver_id)

    for t_cfg in get_table_cfg():
        # Очищаем таблицу перед импортом
        if truncate:
            table_truncate(t_cfg.cfg)

        # Удаляем индексы из модели перед импортом
        if not keep_indexes:
            pre_drop_indexes.send(sender=object.__class__, cfg=t_cfg.cfg)
            remove_indexes_from_model(model=t_cfg.cfg.dst, pk=True)
            post_drop_indexes.send(sender=object.__class__, cfg=t_cfg.cfg)

        # Импортируем все таблицы модели
        if t_cfg.fn is None:
            loader = TableLoader()
            loader.load(t_cfg.cfg)
        else:
            for cfg, desc in t_cfg.fn():
                logger.info(f"Range {desc}.")
                loader = TableLoader()
                loader.load(cfg)
        status, created = t_models.Status.objects.get_or_create(id=1, defaults={"ver": ver.ver_id})
        if not created:
            status.ver = ver.ver_id
            status.full_clean()
            status.save()

        # Восстанавливаем удалённые индексы
        if not keep_indexes:
            pre_restore_indexes.send(sender=object.__class__, cfg=t_cfg.cfg)
            restore_indexes_for_model(model=t_cfg.cfg.dst, pk=True)
            post_restore_indexes.send(sender=object.__class__, cfg=t_cfg.cfg)

    post_import.send(sender=object.__class__, version=ver.ver_id)
    logger.info(f"Data v.{ver.ver_id} loaded.")


def update_data() -> None:
    ver = s_models.Status.objects.order_by("ver").first()
    if ver is None:
        raise ValueError

    logger.info(f"Updating from v.{ver.ver_id}.")
    pre_update.send(sender=object.__class__, version=ver.ver_id)

    t_status = t_models.Status.objects.get()

    for t_cfg in get_table_cfg():
        cfg = t_cfg.cfg
        if issubclass(cfg.src, s_models.AbstractObj):
            cfg = copy.deepcopy(cfg)
            if cfg.filters is None:
                cfg.filters = []
            cfg.filters.append(("tree_ver", ">=", ver.ver_id))
        loader = TableUpdater()
        loader.load(cfg, t_status.ver)

    t_status.ver = ver.ver_id
    t_status.full_clean()
    t_status.save()

    post_update.send(sender=object.__class__, version=ver.ver_id)
    logger.info(f"Data v.{ver.ver_id} is updated.")
