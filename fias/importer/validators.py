# coding: utf-8
from __future__ import absolute_import, unicode_literals

from datetime import date
from typing import Callable, List, Tuple

from fias.config import STORE_INACTIVE_TABLES, TableName
from fias.importer.table import get_model
from fias.models import AbstractModel
from fias.models.common import AbstractIsActiveModel, AbstractObj

__all__ = ["get_common_validator", "get_create_validator", "get_update_validator"]


def new_common_validator(item: AbstractModel, today: date) -> bool:
    return item.startdate <= today < item.enddate


def new_obj_validator(item: AbstractModel, today: date) -> bool:
    assert isinstance(item, AbstractObj)
    return item.isactual


def new_isactive_validator(item: AbstractModel, today: date) -> bool:
    assert isinstance(item, AbstractIsActiveModel)
    return item.isactive


ValidatorType = Callable[[AbstractModel, date], bool]

_ValidatorMapType = List[Tuple[List[TableName], ValidatorType]]

_validators_create: _ValidatorMapType = [
    (
        [
            TableName.ADDR_OBJ,
            TableName.ADDR_OBJ_PARAM,
            TableName.HOUSE,
            TableName.HOUSE_PARAM,
            TableName.ADM_HIERARCHY,
            TableName.MUN_HIERARCHY,
        ],
        new_common_validator,
    ),
    ([TableName.ADDR_OBJ, TableName.HOUSE], new_obj_validator),
    (
        list(
            t for t in TableName if t not in STORE_INACTIVE_TABLES and issubclass(get_model(t), AbstractIsActiveModel)
        ),
        new_isactive_validator,
    ),
]

_validators_update: _ValidatorMapType = []


def common_validator(item: AbstractModel, today: date) -> bool:
    return item.pk is not None


def get_common_validator(name: TableName) -> ValidatorType:
    return common_validator


def _get_validators(name: TableName, validator_map: _ValidatorMapType) -> ValidatorType:
    validators = [v for names, v in validator_map if name in names]

    def validate(item: AbstractModel, today: date) -> bool:
        for validator in validators:
            if not validator(item, today):
                return False
        return True

    return validate


def get_create_validator(name: TableName) -> ValidatorType:
    return _get_validators(name, _validators_create)


def get_update_validator(name: TableName) -> ValidatorType:
    return _get_validators(name, _validators_update)
