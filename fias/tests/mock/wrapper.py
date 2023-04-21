# coding: utf-8
from __future__ import unicode_literals, absolute_import

import os
from typing import Dict, Any, List, IO

from fias.importer.source.wrapper import SourceWrapper
from ..info import FAKE_FILES, FAKE_DIR_PATH

from datetime import date


class Wrapper(SourceWrapper):
    def __init__(self, source: str, **kwargs: Any):
        super().__init__(source, **kwargs)
        self.source = None

    def get_date_info(self, filename: str) -> date:
        return date(2000, 1, int(filename[-1]) + 1)

    def get_file_list(self) -> List[str]:
        return FAKE_FILES

    def open(self, filename: str) -> IO[bytes]:
        return open(FAKE_DIR_PATH / filename, "rb")
