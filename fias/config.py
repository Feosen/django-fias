# coding: utf-8
from __future__ import absolute_import, unicode_literals

import os
import re
from enum import StrEnum
from importlib import import_module
from typing import Callable, Dict, Final, Iterable, List, Tuple, Union

from django.apps import apps
from django.conf import settings
from django.core.exceptions import ImproperlyConfigured
from django.db.utils import DEFAULT_DB_ALIAS

from fias.enum import CEnumMeta
from fias.models import AbstractModel

__all__ = ["DEFAULT_DB_ALIAS", "DATABASE_ALIAS", "PARAM_MAP", "PROXY", "VALIDATE_HOUSE_PARAM_IDS", "TableName"]


ALL: Final = "__all__"


# Table internal names
class TableName(StrEnum, metaclass=CEnumMeta):
    HOUSE_TYPE = "house_type"
    ADD_HOUSE_TYPE = "add_house_type"
    ADDR_OBJ_TYPE = "addr_obj_type"
    PARAM_TYPE = "param_type"
    ADDR_OBJ = "addr_obj"
    ADDR_OBJ_PARAM = "addr_obj_param"
    HOUSE = "house"
    HOUSE_PARAM = "house_param"
    ADM_HIERARCHY = "adm_hierarchy"
    MUN_HIERARCHY = "mun_hierarchy"


TABLES_STATS: Tuple[TableName, ...] = (
    TableName.HOUSE_TYPE,
    TableName.ADD_HOUSE_TYPE,
    TableName.ADDR_OBJ_TYPE,
    TableName.PARAM_TYPE,
)

TABLES_DEFAULT: Tuple[TableName, ...] = (
    TableName.HOUSE,
    TableName.HOUSE_PARAM,
    TableName.ADDR_OBJ,
    TableName.ADDR_OBJ_PARAM,
    TableName.ADM_HIERARCHY,
    TableName.MUN_HIERARCHY,
)

# from_table: (to_table, ((param_type_id, field_name), (param_type_id, field_name),)
PARAM_MAP: Dict[TableName, Tuple[TableName, Tuple[Tuple[int, str], ...]]] = {
    TableName.HOUSE_PARAM: (
        TableName.HOUSE,
        (
            (5, "postalcode"),
            (6, "okato"),
            (7, "oktmo"),
        ),
    ),
    TableName.ADDR_OBJ_PARAM: (
        TableName.ADDR_OBJ,
        (
            (6, "okato"),
            (7, "oktmo"),
        ),
    ),
}

TABLES = TABLES_STATS
if hasattr(settings, "FIAS_TABLES"):
    TABLES += tuple(x for x in TABLES_DEFAULT if x.lower() in list(set(getattr(settings, "FIAS_TABLES"))))
else:
    TABLES += TABLES_DEFAULT


# Auto area
re_region = re.compile(r"\d\d")

for src, (dst, params) in PARAM_MAP.items():
    assert isinstance(src, TableName)
    src_model = apps.get_model("fias", src.replace("_", ""))
    dst_model = apps.get_model("fias", dst.replace("_", ""))
    for _, field_name in params:
        pass  # TODO: finish it
        # assert dst_model._meta.get_field(field_name) is not None

REGIONS: Union[Iterable[str], str]
if hasattr(settings, "FIAS_REGIONS"):
    if not (
        isinstance(settings.FIAS_REGIONS, tuple)
        and all(map(lambda r: isinstance(r, str) and re_region.match(r), settings.FIAS_REGIONS))
        or settings.FIAS_REGIONS == ALL
    ):
        raise ImproperlyConfigured('FIAS_REGIONS must be tuple of str or "__all__".')
    REGIONS = settings.FIAS_REGIONS
else:
    REGIONS = ALL

HOUSE_TYPES: Union[Iterable[int], str]
if hasattr(settings, "FIAS_HOUSE_TYPES"):
    if not (
        isinstance(settings.FIAS_HOUSE_TYPES, tuple)
        and all(map(lambda x: isinstance(x, int), settings.FIAS_HOUSE_TYPES))
        or settings.FIAS_HOUSE_TYPES == ALL
    ):
        raise ImproperlyConfigured('FIAS_HOUSE_TYPES must be tuple of int or "__all__".')
    HOUSE_TYPES = settings.FIAS_HOUSE_TYPES
else:
    HOUSE_TYPES = ALL

DATABASE_ALIAS = getattr(settings, "FIAS_DATABASE_ALIAS", DEFAULT_DB_ALIAS)

if DATABASE_ALIAS not in settings.DATABASES:
    raise ImproperlyConfigured(f"FIAS: database alias `{DATABASE_ALIAS}` was not found in DATABASES")
elif DATABASE_ALIAS != DEFAULT_DB_ALIAS and "fias.routers.FIASRouter" not in settings.DATABASE_ROUTERS:
    raise ImproperlyConfigured(
        "FIAS: for use external database add `fias.routers.FIASRouter`"
        " into `DATABASE_ROUTERS` list in your settings.py"
    )

VALIDATE_HOUSE_PARAM_IDS: Iterable[int] = getattr(settings, "FIAS_VALIDATE_HOUSE_PARAM_IDS", (6, 7))
if not (
    isinstance(VALIDATE_HOUSE_PARAM_IDS, tuple) and all(map(lambda x: isinstance(x, int), VALIDATE_HOUSE_PARAM_IDS))
):
    raise ImproperlyConfigured("FIAS_VALIDATE_HOUSE_PARAM_IDS must be tuple of int.")


"""
см. fias.importer.filters
указывается список путей к функциям-фильтрам
фильтры применяются к *каждому* объекту
один за другим, пока не закончатся,
либо пока какой-нибудь из них не вернёт None
если фильтр вернул None, объект не импортируется в БД

пример:

FIAS_TABLE_ROW_FILTERS = {
    'addrobj': (
        'fias.importer.filters.example_filter_yaroslavl_region',
    ),
    'house': (
        'fias.importer.filters.example_filter_yaroslavl_region',
    ),
}
"""
row_filters: Dict[TableName, List[str]] = getattr(settings, "FIAS_TABLE_ROW_FILTERS", {})
TABLE_ROW_FILTERS: Dict[TableName, List[Callable[[AbstractModel], Union[AbstractModel, None]]]] = {}
_DEFAULT_TABLE_ROW_FILTERS: Dict[TableName, List[str]] = {
    TableName.HOUSE: [
        "fias.importer.filters.filter_is_actual",
    ],
    TableName.HOUSE_PARAM: [
        "fias.importer.filters.filter_house_param",
    ],
    TableName.ADDR_OBJ: [
        "fias.importer.filters.filter_is_actual",
        "fias.importer.filters.replace_quotes_in_names",
    ],
    TableName.ADDR_OBJ_PARAM: [
        "fias.importer.filters.filter_addr_obj_param",
    ],
}

if settings.FIAS_HOUSE_TYPES != ALL:
    _DEFAULT_TABLE_ROW_FILTERS[TableName.HOUSE].append("fias.importer.filters.filter_house_type")

for cfg in _DEFAULT_TABLE_ROW_FILTERS, row_filters:
    for flt_table, flt_list in cfg.items():
        if flt_table not in TableName:
            raise ImproperlyConfigured(f"Wrong table name {flt_table}.")
        if flt_table in TABLES:
            for flt_path in flt_list:
                try:
                    module_name, unused, func_name = flt_path.rpartition(".")
                    flt_module = import_module(module_name)
                    flt_func = getattr(flt_module, func_name)
                except (ImportError, AttributeError):
                    raise ImproperlyConfigured("Table row filter module `{0}` does not exists".format(flt_path))
                else:
                    TABLE_ROW_FILTERS.setdefault(TableName(flt_table), []).append(flt_func)

# SUDS Proxy Support
_http_proxy = os.environ.get("http_proxy")
_https_proxy = os.environ.get("https_proxy")

PROXY = {}
if _http_proxy:
    PROXY["http"] = _http_proxy
if _https_proxy:
    PROXY["https"] = _https_proxy
