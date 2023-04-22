# coding: utf-8
from __future__ import absolute_import, unicode_literals

from typing import Union

from fias import config
from fias.config import PARAM_MAP, TableName
from fias.models import AddrObj, AddrObjParam, House, HouseParam
from fias.models.common import AbstractObj
from fias.models.hierarchy import AbstractHierarchy


def filter_hierarchy_is_isactual(item: AbstractObj) -> Union[AbstractObj, None]:
    if item.isactual:
        return item
    return None


def filter_hierarchy_is_active(item: AbstractHierarchy) -> Union[AbstractHierarchy, None]:
    if item.isactive:
        return item
    return None


def filter_obj_is_actual_and_active(item: AbstractObj) -> Union[AbstractObj, None]:
    if item.isactive and item.isactual:
        return item
    return None


def filter_house_type(item: House) -> Union[House, None]:
    if item.housetype in config.HOUSE_TYPES:
        return item
    return None


_house_param_ids = {p_id for p_id, _ in PARAM_MAP[TableName.HOUSE_PARAM][1]}


def filter_house_param(item: HouseParam) -> Union[HouseParam, None]:
    if item.typeid in _house_param_ids:
        return item
    return None


_addr_obj_param_ids = {p_id for p_id, _ in PARAM_MAP[TableName.ADDR_OBJ_PARAM][1]}


def filter_addr_obj_param(item: AddrObjParam) -> Union[AddrObjParam, None]:
    if item.typeid in _addr_obj_param_ids:
        return item
    return None


def replace_quotes_in_names(item: AddrObj) -> AddrObj:
    item.name = item.name.replace("&quot;", '"')
    return item
