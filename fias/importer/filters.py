# coding: utf-8
from __future__ import unicode_literals, absolute_import

from typing import Union

from fias import config
from fias.config import TableName, PARAM_MAP
from fias.models import AddrObjParam, House, HouseParam, AddrObj
from fias.models.common import AbstractObj
from fias.models.hierarchy import AbstractHierarchy


def filter_hierarchy_is_active(item: AbstractHierarchy) -> Union[AbstractHierarchy, None]:
    if item.isactive:
        return item


def filter_obj_is_actual_and_active(item: AbstractObj) -> Union[AbstractObj, None]:
    if item.isactive and item.isactual:
        return item


def filter_house_type(item: House) -> Union[House, None]:
    if item.housetype in config.HOUSE_TYPES:
        return item


_house_param_ids = {p_id for p_id, _ in PARAM_MAP[TableName.HOUSE_PARAM][1]}


def filter_house_param(item: HouseParam) -> Union[HouseParam, None]:
    if item.typeid in _house_param_ids:
        return item


_addr_obj_param_ids = {p_id for p_id, _ in PARAM_MAP[TableName.ADDR_OBJ_PARAM][1]}


def filter_addr_obj_param(item: AddrObjParam) -> Union[AddrObjParam, None]:
    if item.typeid in _addr_obj_param_ids:
        return item


def replace_quotes_in_names(item: AddrObj) -> AddrObj:
    item.name = item.name.replace('&quot;', '"')
    return item
