# coding: utf-8
from __future__ import absolute_import, unicode_literals

import copy
import logging
from typing import List

from fias import models as s_models
from gar_loader.indexes import remove_indexes_from_model, restore_indexes_for_model
from target import models as t_models
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

logger = logging.getLogger(__name__)


_table_cfg: List[Cfg] = [
    Cfg(t_models.HouseType, "id", s_models.HouseType, "id", None, None, None, None),
    Cfg(t_models.HouseAddType, "id", s_models.AddHouseType, "id", None, None, None, None),
    Cfg(
        t_models.AddrObj,
        "objectid",
        s_models.AddrObj,
        "objectid",
        None,
        {"aolevel": "level"},
        ParamCfg(s_models.AddrObjParam, "objectid", [("okato", 6), ("oktmo", 7)]),
        [
            HierarchyCfg(s_models.AdmHierarchy, "objectid", "parentobjid", "owner_adm"),
        ],
    ),
    Cfg(
        t_models.House,
        "objectid",
        s_models.House,
        "objectid",
        None,
        None,
        ParamCfg(s_models.HouseParam, "objectid", [("postalcode", 5), ("okato", 6), ("oktmo", 7)]),
        [
            HierarchyCfg(s_models.AdmHierarchy, "objectid", "parentobjid", "owner_adm"),
        ],
    ),
    Cfg(
        t_models.House78,
        "objectid",
        s_models.House,
        "objectid",
        [("region", "=", "78")],
        None,
        ParamCfg(s_models.HouseParam, "objectid", [("postalcode", 5), ("okato", 6), ("oktmo", 7)]),
        [
            HierarchyCfg(s_models.AdmHierarchy, "objectid", "parentobjid", "owner_adm"),
        ],
    ),
]


def load_complete_data(truncate: bool = False, keep_indexes: bool = False, keep_pk: bool = True) -> None:
    ver = s_models.Status.objects.order_by("ver").first()
    if ver is None:
        raise ValueError
    logger.info(f"Loading data v.{ver.ver_id}.")
    pre_import.send(sender=object.__class__, version=ver.ver_id)

    for cfg in _table_cfg:
        # Очищаем таблицу перед импортом
        if truncate:
            table_truncate(cfg)

        # Удаляем индексы из модели перед импортом
        if not keep_indexes:
            pre_drop_indexes.send(sender=object.__class__, cfg=cfg)
            remove_indexes_from_model(model=cfg.dst, pk=True)
            post_drop_indexes.send(sender=object.__class__, cfg=cfg)

        # Импортируем все таблицы модели
        loader = TableLoader()
        loader.load(cfg)
        status, created = t_models.Status.objects.get_or_create(id=1, defaults={"ver": ver.ver_id})
        if not created:
            status.ver = ver.ver_id
            status.full_clean()
            status.save()

        # Восстанавливаем удалённые индексы
        if not keep_indexes:
            pre_restore_indexes.send(sender=object.__class__, cfg=cfg)
            restore_indexes_for_model(model=cfg.dst, pk=True)
            post_restore_indexes.send(sender=object.__class__, cfg=cfg)

    post_import.send(sender=object.__class__, version=ver.ver_id)
    logger.info(f"Data v.{ver.ver_id} loaded.")


def update_data() -> None:
    ver = s_models.Status.objects.order_by("ver").first()
    if ver is None:
        raise ValueError

    logger.info(f"Updating from v.{ver.ver_id}.")
    pre_update.send(sender=object.__class__, version=ver.ver_id)

    t_status = t_models.Status.objects.get()

    for cfg in _table_cfg:
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
