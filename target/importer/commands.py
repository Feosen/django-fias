# coding: utf-8
from __future__ import unicode_literals, absolute_import

from pathlib import Path
from typing import Tuple

from django.db.models import Min

from fias.importer.indexes import remove_indexes_from_model, restore_indexes_for_model
from fias.importer.log import log
from fias.importer.source import *
from fias.importer.table import BadTableError
from fias import models as s_models
from target import models as t_models
from target.importer.loader import TableLoader, TableUpdater, truncate as table_truncate
from target.importer.signals import (
    pre_drop_indexes, post_drop_indexes,
    pre_restore_indexes, post_restore_indexes,
    pre_update, post_update
)


def load_complete_data(truncate: bool = False, keep_indexes: bool = False):

    # TODO: restore
    # pre_import.send(sender=object.__class__, version=Status.objects.last().ver)

    # Очищаем таблицу перед импортом
    if truncate:
        table_truncate()

    # Удаляем индексы из модели перед импортом
    if not keep_indexes:
        # TODO: finish it
        pre_drop_indexes.send(sender=object.__class__)
        remove_indexes_from_model(model=first_table.model)
        post_drop_indexes.send(sender=object.__class__)

    # Импортируем все таблицы модели
    loader = TableLoader()
    loader.load()
    s_status = s_models.Status.objects.order_by('ver').first()
    status, created = t_models.Status.objects.get_or_create(id=1, defaults={'ver': s_status.ver_id})
    if not created:
        status.ver = s_status.ver_id
        status.full_clean()
        status.save()

    # Восстанавливаем удалённые индексы
    # TODO: finish it
    if not keep_indexes:
        pre_restore_indexes.send(sender=object.__class__, table=first_table)
        restore_indexes_for_model(model=first_table.model)
        post_restore_indexes.send(sender=object.__class__, table=first_table)

    # TODO: restore
    #post_import.send(sender=object.__class__, version=Status.objects.last().ver)


def update_data(keep_indexes: bool = False):
    # TODO: restore
    # pre_import.send(sender=object.__class__, version=Status.objects.last().ver)

    # Удаляем индексы из модели перед импортом
    if not keep_indexes:
        # TODO: finish it
        pre_drop_indexes.send(sender=object.__class__)
        remove_indexes_from_model(model=first_table.model)
        post_drop_indexes.send(sender=object.__class__)

    loader = TableUpdater()
    loader.load()
    s_status = s_models.Status.objects.order_by('ver').first()
    status = t_models.Status.objects.get()
    status.ver = s_status.ver_id
    status.full_clean()
    status.save()

    # Восстанавливаем удалённые индексы
    # TODO: finish it
    if not keep_indexes:
        pre_restore_indexes.send(sender=object.__class__, table=first_table)
        restore_indexes_for_model(model=first_table.model)
        post_restore_indexes.send(sender=object.__class__, table=first_table)

    # TODO: restore
    # post_import.send(sender=object.__class__, version=Status.objects.last().ver)
