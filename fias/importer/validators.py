# coding: utf-8
from __future__ import absolute_import, unicode_literals

from datetime import date
from typing import Any

from fias.config import TableName
from fias.models import AbstractModel
from fias.models.common import AbstractObj

__all__ = ["validate"]

assert issubclass(AbstractObj, AbstractModel)


def common_validator(item: AbstractModel, today: date, **kwargs: Any) -> bool:
    return item.startdate < today < item.enddate


def chained_validator(item: AbstractObj, today: date, **kwargs: Any) -> bool:
    return (
        # not item.nextid and  # TODO: Does it required?
        item.isactual
        and common_validator(item, today=today, **kwargs)
    )


def validate(name: TableName, item: AbstractModel, today: date, **kwargs: Any) -> bool:
    if name in (TableName.ADDR_OBJ_PARAM, TableName.HOUSE_PARAM, TableName.ADM_HIERARCHY, TableName.MUN_HIERARCHY):
        return common_validator(item, today, **kwargs)
    elif name in (TableName.ADDR_OBJ, TableName.HOUSE):
        return isinstance(item, AbstractObj) and chained_validator(item, today, **kwargs)
    else:
        return True
