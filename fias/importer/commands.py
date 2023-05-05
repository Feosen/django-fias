# coding: utf-8
from __future__ import absolute_import, unicode_literals

import logging
from pathlib import Path
from typing import List, Tuple, Type, Union

from django.core.exceptions import ValidationError
from django.core.validators import URLValidator
from django.db.models import Min

from fias import config
from fias.importer.loader import TableLoader, TableUpdater
from fias.importer.signals import (
    post_drop_indexes,
    post_import,
    post_restore_indexes,
    post_update,
    pre_drop_indexes,
    pre_import,
    pre_restore_indexes,
    pre_update,
)
from fias.importer.source import (
    DirectoryTableList,
    LocalArchiveTableList,
    RemoteArchiveTableList,
    TableList,
    TableListLoadingError,
)
from fias.importer.table import BadTableError
from fias.models import AbstractIsActiveModel, AbstractModel, Status, Version
from gar_loader.indexes import remove_indexes_from_model, restore_indexes_for_model

logger = logging.getLogger(__name__)


def get_tablelist(
    path: Union[Path, str, None] = None,
    version: Union[Version, None] = None,
    data_format: str = "xml",
    tempdir: Union[Path, None] = None,
) -> TableList:
    supported_formats = ("xml",)
    assert (
        data_format in supported_formats
    ), f'Unsupported data format: `{data_format}`. Available choices: {", ".join(supported_formats)}'

    tablelist: Union[TableList]
    if path is None:
        latest_version = Version.objects.latest("dumpdate")
        url = getattr(latest_version, f"complete_{data_format}_url")

        tablelist = RemoteArchiveTableList(src=url, version=latest_version, tempdir=tempdir)

    else:
        try:
            url = str(path)
            URLValidator()(url)
            tablelist = RemoteArchiveTableList(src=url, version=version, tempdir=tempdir)
        except ValidationError:
            path = Path(path)
            if path.is_file():
                tablelist = LocalArchiveTableList(src=path, version=version, tempdir=tempdir)
            elif path.is_dir():
                tablelist = DirectoryTableList(src=path, version=version, tempdir=tempdir)
            else:
                raise TableListLoadingError(f"Path `{path}` is not valid table list source")

    return tablelist


def get_table_names(tables: Union[Tuple[str, ...], None]) -> Tuple[str, ...]:
    return tables if tables else config.TABLES


def remove_orphans(models: List[Type[AbstractModel]]) -> None:
    # There are two levels of model hierarchy, so we can remove orphans in any order.
    for model in models:
        model.objects.delete_orphans()


def remove_not_active(models: List[Type[AbstractModel]]) -> None:
    for model in models:
        if issubclass(model, AbstractIsActiveModel):
            model.objects.filter(isactive=False).delete()


def update_tree_ver(models: List[Type[AbstractModel]], min_ver: int) -> None:
    for model in models:
        model.objects.update_tree_ver(min_ver)


def load_complete_data(
    path: str | None = None,
    data_format: str = "xml",
    truncate: bool = False,
    limit: int = 10000,
    tables: Union[Tuple[str, ...], None] = None,
    keep_indexes: bool = False,
    keep_pk: bool = True,
    tempdir: Union[Path, None] = None,
) -> None:
    tablelist = get_tablelist(path=path, data_format=data_format, tempdir=tempdir)

    logger.info(f"Loading data v.{tablelist.version}.")
    pre_import.send(sender=object.__class__, version=tablelist.version)

    processed_models = []

    for tbl in get_table_names(tables):
        # Пропускаем таблицы, которых нет в архиве
        if tbl not in tablelist.tables:
            continue

        st_qs = Status.objects.filter(table=tbl)
        if config.REGIONS != config.ALL:
            st_qs = st_qs.filter(region__in=config.REGIONS)
        if st_qs.exists():
            if truncate:
                st_qs.delete()
            else:
                st = st_qs[0]
                logger.warning(
                    f"Table `{st.table}` has version `{st.ver}`. "
                    "Please use --truncate for replace "
                    "all table contents. Skipping..."
                )
                continue
        # Берём для работы любую таблицу с именем tbl
        first_table = tablelist.tables[tbl][0]
        processed_models.append(first_table.model)

        # Очищаем таблицу перед импортом
        if truncate:
            first_table.truncate()

        process_pk = not keep_pk

        # Удаляем индексы из модели перед импортом
        if not keep_indexes:
            pre_drop_indexes.send(sender=object.__class__, table=first_table)
            remove_indexes_from_model(model=first_table.model, pk=process_pk)
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
            restore_indexes_for_model(model=first_table.model, pk=process_pk)
            post_restore_indexes.send(sender=object.__class__, table=first_table)

    update_tree_ver(processed_models, 0)
    remove_not_active(processed_models)
    remove_orphans(processed_models)

    post_import.send(sender=object.__class__, version=tablelist.version)
    logger.info(f"Data v.{tablelist.version} loaded.")


def update_data(
    path: Union[Path, None] = None,
    version: Union[Version, None] = None,
    skip: bool = False,
    data_format: str = "xml",
    limit: int = 10000,
    tables: Union[Tuple[str, ...], None] = None,
    tempdir: Union[Path, None] = None,
) -> None:
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
                logger.info(
                    f"Can not update table `{table.name}`, region `{table.region}`: no data in database. Skipping…"
                )
                continue
            if st.ver.ver >= tablelist.version.ver:
                logger.info(
                    (
                        f"Update of the table `{table.name}` is not needed "
                        f"[{st.ver.ver} >= {tablelist.version.ver}]. Skipping…"
                    )
                )
                continue
            loader = TableUpdater(limit=limit)
            try:
                loader.load(tablelist=tablelist, table=table)
            except BadTableError as e:
                if skip:
                    logger.error(str(e))
                else:
                    raise
            st.ver = tablelist.version
            st.save()

    update_tree_ver(processed_models, tablelist.version.ver)
    remove_not_active(processed_models)
    remove_orphans(processed_models)


def manual_update_data(
    path: Path,
    skip: bool = False,
    data_format: str = "xml",
    limit: int = 1000,
    tables: Union[Tuple[str, ...], None] = None,
    tempdir: Union[Path, None] = None,
) -> None:
    min_version = Status.objects.filter(table__in=get_table_names(None)).aggregate(Min("ver"))["ver__min"]

    version_map = {}

    for child in path.iterdir():
        tablelist = get_tablelist(path=child, version=None, data_format=data_format, tempdir=tempdir)
        version_map[tablelist.version] = child

    if min_version is not None:
        min_ver = Version.objects.get(ver=min_version)

        for version in Version.objects.filter(ver__gt=min_version).order_by("ver"):
            try:
                src = version_map[version]
            except KeyError:
                raise TableListLoadingError(f"No file for version {version}.")

            logger.info(f"Updating from v.{min_ver} to v.{version}.")
            pre_update.send(sender=object.__class__, before=min_ver, after=version)

            update_data(
                path=src,
                version=version,
                skip=skip,
                data_format=data_format,
                limit=limit,
                tables=tables,
                tempdir=tempdir,
            )

            post_update.send(sender=object.__class__, before=min_ver, after=version)
            logger.info(f"Data v.{min_ver} is updated to v.{version}.")
            min_ver = version
    else:
        raise TableListLoadingError("Not available. Please import the data before updating")


def auto_update_data(
    skip: bool,
    data_format: str = "xml",
    limit: int = 10000,
    tables: Union[Tuple[str, ...] | None] = None,
    tempdir: Union[Path, None] = None,
) -> None:
    min_version = Status.objects.filter(table__in=get_table_names(None)).aggregate(Min("ver"))["ver__min"]

    if min_version is not None:
        min_ver = Version.objects.get(ver=min_version)

        for version in Version.objects.filter(ver__gt=min_version).order_by("ver"):
            pre_update.send(sender=object.__class__, before=min_ver, after=version)

            url = getattr(version, "delta_{0}_url".format(data_format))
            update_data(
                path=url,
                version=version,
                skip=skip,
                data_format=data_format,
                limit=limit,
                tables=tables,
                tempdir=tempdir,
            )

            post_update.send(sender=object.__class__, before=min_ver, after=version)
            logger.info(f"Data v.{min_ver} is updated to v.{version}.")
            min_ver = version
    else:
        raise TableListLoadingError("Not available. Please import the data before updating")
