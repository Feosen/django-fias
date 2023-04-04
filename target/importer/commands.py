# coding: utf-8
from __future__ import unicode_literals, absolute_import

from pathlib import Path
from typing import Tuple

from django.db.models import Min

from fias import config
from fias.importer.indexes import remove_indexes_from_model, restore_indexes_for_model
from target.importer.loader import TableLoader, TableUpdater, truncate as table_truncate
from fias.importer.log import log
from fias.importer.source import *
from fias.importer.table import BadTableError
from fias.models import Status, Version
from target.importer.signals import (
    pre_drop_indexes, post_drop_indexes,
    pre_restore_indexes, post_restore_indexes,
    pre_import, post_import, pre_update, post_update
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

    # Восстанавливаем удалённые индексы
    # TODO: finish it
    if not keep_indexes:
        pre_restore_indexes.send(sender=object.__class__, table=first_table)
        restore_indexes_for_model(model=first_table.model)
        post_restore_indexes.send(sender=object.__class__, table=first_table)

    # TODO: restore
    #post_import.send(sender=object.__class__, version=Status.objects.last().ver)


def update_data(keep_indexes: bool = False):
    tablelist = get_tablelist(path=path, version=version, data_format=data_format, tempdir=tempdir)

    processed_models = []
    for tbl in get_table_names(tables):
        # Пропускаем таблицы, которых нет в архиве
        if tbl not in tablelist.tables:
            continue

        processed_models.append(tablelist.tables[tbl][0].model)

        for table in tablelist.tables[tbl]:
            try:
                st = Status.objects.get(table=table.name, region=table.region)
            except Status.DoesNotExist:
                log.info(f'Can not update table `{table.name}`, region `{table.region}`: no data in database. Skipping…')
                continue
            if st.ver.ver >= tablelist.version.ver:
                log.info(f'Update of the table `{table.name}` is not needed [{st.ver.ver} <= {tablelist.version.ver}]. Skipping…')
                continue
            loader = TableUpdater(limit=limit)
            try:
                loader.load(tablelist=tablelist, table=table)
            except BadTableError as e:
                if skip:
                    log.error(str(e))
                else:
                    raise
            st.ver = tablelist.version
            st.save()

    remove_orphans(processed_models)


def manual_update_data(path: Path = None, skip: bool = False, data_format: str = 'xml', limit: int = 1000,
                       tables: Tuple[str] = None, tempdir: Path = None):
    min_version = Status.objects.filter(table__in=get_table_names(None)).aggregate(Min('ver'))['ver__min']

    version_map = {}

    for child in path.iterdir():
        tablelist = get_tablelist(path=child, version=None, data_format=data_format, tempdir=tempdir)
        version_map[tablelist.version] = child

    if min_version is not None:
        min_ver = Version.objects.get(ver=min_version)

        for version in Version.objects.filter(ver__gt=min_version).order_by('ver'):
            try:
                src = version_map[version]
            except KeyError:
                raise TableListLoadingError(f'No file for version {version}.')

            pre_update.send(sender=object.__class__, before=min_ver, after=version)

            update_data(
                path=src, version=version, skip=skip,
                data_format=data_format, limit=limit,
                tables=tables, tempdir=tempdir,
            )

            post_update.send(sender=object.__class__, before=min_ver, after=version)
            min_ver = version
    else:
        raise TableListLoadingError('Not available. Please import the data before updating')


def auto_update_data(skip: bool = False, data_format: str = 'xml', limit: int = 1000, tables: Tuple[str] = None,
                     tempdir: Path = None):
    min_version = Status.objects.filter(table__in=get_table_names(None)).aggregate(Min('ver'))['ver__min']

    if min_version is not None:
        min_ver = Version.objects.get(ver=min_version)

        for version in Version.objects.filter(ver__gt=min_version).order_by('ver'):
            pre_update.send(sender=object.__class__, before=min_ver, after=version)

            url = getattr(version, 'delta_{0}_url'.format(data_format))
            update_data(
                path=url, version=version, skip=skip,
                data_format=data_format, limit=limit,
                tables=tables, tempdir=tempdir,
            )

            post_update.send(sender=object.__class__, before=min_ver, after=version)
            min_ver = version
    else:
        raise TableListLoadingError('Not available. Please import the data before updating')
