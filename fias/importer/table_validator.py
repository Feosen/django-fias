from django.core.exceptions import ImproperlyConfigured
from django.dispatch import receiver

from fias import config
from fias.config import PARAM_MAP, TableName
from fias.importer.signals import post_import_table
from fias.importer.table.table import Table


@receiver(post_import_table)
def post_import_table_validator(sender, table: Table, **kwargs):
    if table.name == TableName.HOUSE_TYPE:
        ids = set(table.model.objects
                  .filter(id__in=config.HOUSE_TYPES, isactive=True)
                  .values_list('id', flat=True))
        lost_ids = set(config.HOUSE_TYPES) - ids
        if lost_ids:
            raise ImproperlyConfigured(f'No active types with ID {", ".join(map(str, lost_ids))}.')
    elif table.name == TableName.PARAM_TYPE:
        cfg_ids = set()
        for _, (_, param_ids) in PARAM_MAP.items():
            cfg_ids |= {p_id for p_id, _ in param_ids}
        ids = set(table.model.objects
                  .filter(id__in=cfg_ids, isactive=True)
                  .values_list('id', flat=True))
        lost_ids = cfg_ids - ids
        if lost_ids:
            raise ImproperlyConfigured(f'No active params with ID {", ".join(map(str, lost_ids))}.')
