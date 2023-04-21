# coding: utf-8
from __future__ import unicode_literals, absolute_import

import datetime
from pathlib import Path
from typing import List, Type, IO, Dict, Union, Any

from django.utils.functional import cached_property

from fias.importer.signals import pre_load, post_load
from fias.importer.table import TableFactory
from fias.models import Version
from .wrapper import SourceWrapper
from ..table.table import AbstractTableList, Table
from ... import config


class TableListLoadingError(Exception):
    pass


class TableList(AbstractTableList):
    wrapper_class: Type[SourceWrapper] = SourceWrapper
    wrapper: SourceWrapper

    date: Union[datetime.date, None] = None
    version_info: Union[Version, None] = None

    def __init__(self, src: Any, version: Union[Version, None] = None, tempdir: Union[Path, None] = None):
        self.version_info = version
        self.tempdir = tempdir

        if version is not None:
            assert isinstance(version, Version), 'version must be an instance of Version model'

            self.date = version.dumpdate

        pre_load.send(sender=self.__class__, src=src)
        self.wrapper = self.load_data(src)
        post_load.send(sender=self.__class__, wrapper=self.wrapper)

    def load_data(self, source: Any) -> SourceWrapper:
        return self.wrapper_class(source=source)

    def get_table_list(self) -> List[str]:
        return self.wrapper.get_file_list()

    @cached_property
    def tables(self) -> Dict[str, List[Table]]:
        table_list: Dict[str, List[Table]] = {}
        for filename in self.get_table_list():
            table = TableFactory.parse(filename=filename, extra={'ver': self.version.ver})
            if table is None or (config.ALL != config.REGIONS
                                 and table.region is not None and table.region not in config.REGIONS):
                continue
            table_list.setdefault(table.name, []).append(table)

        return table_list

    def get_date_info(self, name: str) -> datetime.date:
        return self.wrapper.get_date_info(filename=name)

    @property
    def dump_date(self) -> datetime.date:
        if self.date is None:
            self.date = self.wrapper.get_date()

        return self.date

    def open(self, filename: str) -> IO[bytes]:
        return self.wrapper.open(filename=filename)

    @property
    def version(self) -> Version:
        if self.version_info is None:
            self.version_info = Version.objects.nearest_by_date(self.dump_date)
        return self.version_info
