# coding: utf-8
from __future__ import absolute_import, unicode_literals

from pathlib import Path

from .tablelist import TableList, TableListLoadingError
from .wrapper import DirectoryWrapper, SourceWrapper


class EmptyDirError(TableListLoadingError):
    pass


class DirectoryTableList(TableList):
    wrapper_class = DirectoryWrapper

    def load_data(self, source: Path) -> SourceWrapper:
        return self.wrapper_class(source=source, is_temporary=False)
