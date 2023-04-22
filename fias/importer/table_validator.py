from typing import Any, Set, Type, cast

from django.core.exceptions import ImproperlyConfigured
from django.dispatch import receiver

from fias import config
from fias.config import ALL, PARAM_MAP, TableName
from fias.importer.signals import post_import_table
from fias.importer.table.table import Table
from fias.models import HouseType, ParamType


@receiver(post_import_table)
def post_import_table_validator(sender: type, table: Table, **kwargs: Any) -> None:
    found_ids: Set[int]
    lost_ids: Set[int]

    if table.name == TableName.HOUSE_TYPE and config.HOUSE_TYPES != ALL:
        assert isinstance(config.HOUSE_TYPES, tuple)
        found_ids = set(
            cast(Type[HouseType], table.model)
            .objects.filter(id__in=config.HOUSE_TYPES, isactive=True)
            .values_list("id", flat=True)
        )
        lost_ids = set(config.HOUSE_TYPES) - found_ids
        if lost_ids:
            raise ImproperlyConfigured(f'No active types with ID {", ".join(map(str, lost_ids))}.')
    elif table.name == TableName.PARAM_TYPE:
        cfg_ids: Set[int] = set()
        for _, (_, param_ids) in PARAM_MAP.items():
            cfg_ids |= {p_id for p_id, _ in param_ids}
        found_ids = set(
            cast(Type[ParamType], table.model)
            .objects.filter(id__in=cfg_ids, isactive=True)
            .values_list("id", flat=True)
        )
        lost_ids = cfg_ids - found_ids
        if lost_ids:
            raise ImproperlyConfigured(f'No active params with ID {", ".join(map(str, lost_ids))}.')
