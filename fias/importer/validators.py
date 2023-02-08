# coding: utf-8
from __future__ import unicode_literals, absolute_import


__all__ = ['validators']

from fias.config import TableName


def common_validator(item, today, **kwargs):
    return item.startdate < today < item.enddate


def chained_validator(item, today, **kwargs):
    return (
        #not item.nextid and  # TODO: Does it required?
        item.isactual and
        common_validator(item, today=today, **kwargs)
    )


validators = {
    TableName.ADDR_OBJ: chained_validator,
    TableName.ADDR_OBJ_PARAM: common_validator,
    TableName.HOUSE: chained_validator,
    TableName.HOUSE_PARAM: common_validator,
    TableName.ADM_HIERARCHY: common_validator,
    TableName.MUN_HIERARCHY: common_validator,
}
