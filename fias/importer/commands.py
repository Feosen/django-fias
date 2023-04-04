# coding: utf-8
from __future__ import unicode_literals, absolute_import

from pathlib import Path
from typing import Tuple, Union, List, Type

from django.core.exceptions import ValidationError
from django.core.validators import URLValidator
from django.db.models import Min, Model

from fias import config
from fias.importer.indexes import remove_indexes_from_model, restore_indexes_for_model
from fias.importer.loader import TableLoader, TableUpdater
from fias.importer.log import log
from fias.importer.signals import (
    pre_drop_indexes, post_drop_indexes,
    pre_restore_indexes, post_restore_indexes,
    pre_import, post_import, pre_update, post_update
)
from fias.importer.source import *
from fias.importer.table import BadTableError
from fias.models import Status, Version
from fias.typing import Url


def get_tablelist(path: Union[Path, Url, None], version: Version = None, data_format: str = 'xml', tempdir: Path = None):
    assert data_format in ['xml', 'dbf'], \
        'Unsupported data format: `{0}`. Available choices: {1}'.format(data_format, ', '.join(['xml', 'dbf']))

    if path is None:
        latest_version = Version.objects.latest('dumpdate')
        url = getattr(latest_version, 'complete_{0}_url'.format(data_format))

        tablelist = RemoteArchiveTableList(src=url, version=latest_version, tempdir=tempdir)

    else:
        try:
            URLValidator()(path)
            tablelist = RemoteArchiveTableList(src=path, version=version, tempdir=tempdir)
        except ValidationError:
            path = Path(path)
            if path.is_file():
                tablelist = LocalArchiveTableList(src=path, version=version, tempdir=tempdir)
            elif path.is_dir():
                tablelist = DirectoryTableList(src=path, version=version, tempdir=tempdir)
            else:
                raise TableListLoadingError(f'Path `{path}` is not valid table list source')

    return tablelist


def get_table_names(tables: Union[Tuple[str], None]):
    return tables if tables else config.TABLES


def remove_orphans(models: List[Type[Model]]) -> None:
    for model in models:
        model.objects.delete_orphans()


def load_complete_data(path: str = None, data_format: str = 'xml', truncate: bool = False, limit: int = 10000,
                       tables: Tuple[str] = None, keep_indexes: bool = False, tempdir: Path = None):
    tablelist = get_tablelist(path=path, data_format=data_format, tempdir=tempdir)

    pre_import.send(sender=object.__class__, version=tablelist.version)

    processed_models = []

    for tbl in get_table_names(tables):
        # Пропускаем таблицы, которых нет в архиве
        if tbl not in tablelist.tables:
            continue

        st_qs = Status.objects.all()
        if config.REGIONS != config.ALL:
            st_qs = st_qs.filter(table=tbl, region__in=config.REGIONS)
        if st_qs.exists():
            if truncate:
                st_qs.delete()
            else:
                st = st_qs[0]
                log.warning(f'Table `{st.table}` has version `{st.ver}`. '
                            'Please use --truncate for replace '
                            'all table contents. Skipping...')
                continue
        # Берём для работы любую таблицу с именем tbl
        first_table = tablelist.tables[tbl][0]
        processed_models.append(first_table.model)

        # Очищаем таблицу перед импортом
        if truncate:
            first_table.truncate()

        # Удаляем индексы из модели перед импортом
        if not keep_indexes:
            pre_drop_indexes.send(sender=object.__class__, table=first_table)
            remove_indexes_from_model(model=first_table.model)
            post_drop_indexes.send(sender=object.__class__, table=first_table)

        # Импортируем все таблицы модели
        for table in tablelist.tables[tbl]:
            loader = TableLoader(limit=limit)
            loader.load(tablelist=tablelist, table=table)
            st = Status(region=table.region, table=tbl, ver=tablelist.version)
            st.save()

        # Восстанавливаем удалённые индексы
        if not keep_indexes:
            pre_restore_indexes.send(sender=object.__class__, table=first_table)
            restore_indexes_for_model(model=first_table.model)
            post_restore_indexes.send(sender=object.__class__, table=first_table)

    remove_orphans(processed_models)

    post_import.send(sender=object.__class__, version=tablelist.version)


def update_data(path: Path = None, version: Version = None, skip: bool = False, data_format: str = 'xml',
                limit: int = 1000, tables: Tuple[str] = None, tempdir: Path = None):
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
