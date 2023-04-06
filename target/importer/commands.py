# coding: utf-8
from __future__ import unicode_literals, absolute_import

from fias import models as s_models
from fias.importer.indexes import remove_indexes_from_model, restore_indexes_for_model
from target import models as t_models
from target.importer.loader import TableLoader, TableUpdater, truncate as table_truncate
from target.importer.signals import (
    pre_drop_indexes, post_drop_indexes,
    pre_restore_indexes, post_restore_indexes,
    pre_update, post_update, pre_import, post_import
)
from target.models import Status


def load_complete_data(truncate: bool = False, keep_indexes: bool = False):

    pre_import.send(sender=object.__class__, version=Status.objects.get().ver)

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

    post_import.send(sender=object.__class__, version=Status.objects.get().ver)


def update_data(keep_indexes: bool = False):
    pre_update.send(sender=object.__class__, version=Status.objects.get().ver)

    # Удаляем индексы из модели перед импортом
    if not keep_indexes:
        # TODO: finish it
        pre_drop_indexes.send(sender=object.__class__)
        remove_indexes_from_model(model=first_table.model)
        post_drop_indexes.send(sender=object.__class__)

    t_status = t_models.Status.objects.get()

    loader = TableUpdater()
    loader.load(t_status.ver)
    s_status = s_models.Status.objects.order_by('ver').first()

    t_status.ver = s_status.ver_id
    t_status.full_clean()
    t_status.save()

    # Восстанавливаем удалённые индексы
    # TODO: finish it
    if not keep_indexes:
        pre_restore_indexes.send(sender=object.__class__, table=first_table)
        restore_indexes_for_model(model=first_table.model)
        post_restore_indexes.send(sender=object.__class__, table=first_table)

    post_update.send(sender=object.__class__, version=Status.objects.get().ver)
