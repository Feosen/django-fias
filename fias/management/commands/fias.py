# coding: utf-8
from __future__ import absolute_import, unicode_literals

import os
import sys
from pathlib import Path
from typing import Any, List, Tuple, Union

from django.conf import settings
from django.utils.translation import activate

from fias.config import TABLES
from fias.importer.commands import (
    auto_update_data,
    load_complete_data,
    manual_update_data,
    validate_house_params,
)
from fias.importer.source import TableListLoadingError
from fias.importer.version import fetch_version_info
from fias.models import Status
from gar_loader.compat import BaseCommandCompatible


class Command(BaseCommandCompatible):
    help = "Fill or update FIAS database"
    usage_str = (
        "Usage: ./manage.py fias [--src <path|filename|url|AUTO> [--truncate]"
        " [--i-know-what-i-do]]"
        " [--update [--skip]]"
        " [--format <xml>] [--limit=<N>] [--tables=<{0}>]"
        " [--update-version-info <yes|no>]"
        " [--keep-indexes <yes|pk|no>]"
        " [--tempdir <path>]"
        "".format(",".join(TABLES))
    )

    arguments_dictionary = {
        "--src": {
            "action": "store",
            "dest": "src",
            "default": None,
            "help": "Load dir|file|url into DB. If not specified, the source is automatically selected",
        },
        "--truncate": {
            "action": "store_true",
            "dest": "truncate",
            "default": False,
            "help": "Truncate tables before loading data",
        },
        "--i-know-what-i-do": {
            "action": "store_true",
            "dest": "doit",
            "default": False,
            "help": "If data exist in any table, you should confirm their removal and replacement"
            ", as this may result in the removal of related data from other tables!",
        },
        "--update": {
            "action": "store_true",
            "dest": "update",
            "default": False,
            "help": "Update database from http://fias.nalog.ru",
        },
        "--skip": {
            "action": "store_true",
            "dest": "skip",
            "default": False,
            "help": "Skip the bad delta files when upgrading",
        },
        "--format": {
            "action": "store",
            "dest": "fmt",
            "type": str,
            "choices": ["xml"],
            "default": "xml",
            "help": "Preferred source data format. Possible choices: xml",
        },
        "--limit": {
            "action": "store",
            "dest": "limit",
            "type": int,
            "default": 10000,
            "help": "Limit rows for bulk operations. Default value: 10000",
        },
        "--tables": {
            "action": "store",
            "dest": "tables",
            "default": None,
            "help": "Comma-separated list of tables to import",
        },
        "--update-version-info": {
            "action": "store",
            "dest": "update_version_info",
            "type": str,
            "choices": ["yes", "no"],
            "default": "yes",
            "help": "Update list of available database versions from http://fias.nalog.ru",
        },
        "--keep-indexes": {
            "action": "store",
            "type": str,
            "choices": ["yes", "pk", "no"],
            "default": "yes",
            "help": "Do not drop indexes",
        },
        "--tempdir": {
            "action": "store",
            "dest": "tempdir",
            "default": None,
            "help": "Path to the temporary files directory",
        },
        "--house-param-report": {
            "action": "store",
            "dest": "house_param_report",
            "type": Path,
            "default": None,
            "help": "Output CSV file path",
        },
        "--house-param-region": {
            "action": "store",
            "dest": "house_param_regions",
            "type": str,
            "default": "__all__",
            "help": "Region to scan [,]",
        },
    }

    def handle(
        self,
        src: str,
        truncate: bool,
        doit: bool,
        update: bool,
        skip: bool,
        fmt: str,
        limit: int,
        tables: str,
        update_version_info: str,
        keep_indexes: str,
        tempdir: str,
        house_param_report: Path,
        house_param_regions: str,
        **options: Any,
    ) -> None:
        remote = False
        src_path: Union[str, None]
        if src and src.lower() == "auto":
            src_path = None
            remote = True
        else:
            src_path = src

        if not any([src_path, remote, update]):
            self.error(self.usage_str)

        tempdir_path: Union[Path, None]

        if tempdir:
            tempdir_path = Path(tempdir)
            if not tempdir_path.exists():
                self.error(f"Directory `{tempdir_path}` does not exists.")
            elif not tempdir_path.is_dir():
                self.error(f"Path `{tempdir_path}` is not a directory.")
            elif not os.access(tempdir_path, os.W_OK):
                self.error(f"Directory `{tempdir_path}` is not writeable")
        else:
            tempdir_path = None

        # TODO: какая-то нелогичная логика получилась. Надо бы поправить.
        if (src_path or remote) and Status.objects.count() > 0 and not doit and not update:
            self.error(
                "One of the tables contains data. Truncate all FIAS tables manually "
                "or enter key --i-know-what-i-do, to clear the table by means of Django ORM"
            )

        if update_version_info == "yes":
            fetch_version_info(update_all=True)

        # Force Russian language for internationalized projects
        if settings.USE_I18N:
            activate("ru")

        tables_set = set(tables.split(",")) if tables else set()

        if not tables_set.issubset(set(TABLES)):
            diff = ", ".join(tables_set.difference(set(TABLES)))
            self.error("Tables `{0}` are not listed in the FIAS_TABLES and can not be processed".format(diff))
        tables_tuple: Tuple[str, ...] = tuple(str(x) for x in TABLES if x in tables_set)

        keep_regular_indexes = keep_indexes == "yes"
        keep_pk_indexes = keep_indexes != "no"

        validate_hp = house_param_report is not None

        typed_house_param_regions: List[str] | str = house_param_regions
        if typed_house_param_regions != "__all__":
            typed_house_param_regions = house_param_regions.split(",")
        least_new_version: int | None = None

        if (src_path or remote) and not update:
            try:
                load_complete_data(
                    path=src_path,
                    data_format=fmt,
                    truncate=truncate,
                    limit=limit,
                    tables=tables_tuple,
                    keep_indexes=keep_regular_indexes,
                    keep_pk=keep_pk_indexes,
                    tempdir=tempdir_path,
                )
            except TableListLoadingError as e:
                self.error(str(e))

        if update:
            try:
                if src_path:
                    least_new_version = manual_update_data(
                        path=Path(src_path),
                        skip=skip,
                        data_format=fmt,
                        limit=limit,
                        tables=tables_tuple,
                        tempdir=tempdir_path,
                    )
                else:
                    least_new_version = auto_update_data(
                        skip=skip, data_format=fmt, limit=limit, tables=tables_tuple, tempdir=tempdir_path
                    )
            except TableListLoadingError as e:
                self.error(str(e))
        if validate_hp:
            validate_house_params(house_param_report, least_new_version, typed_house_param_regions)

    def error(self, message: str, code: int | None = 1) -> None:
        print(message)
        sys.exit(code)
