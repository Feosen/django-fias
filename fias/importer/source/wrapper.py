# coding: utf-8
from __future__ import absolute_import, unicode_literals

import datetime
import re
import shutil
from pathlib import Path
from typing import IO, Any, List, Union
from zipfile import ZipFile

from rarfile import NoRarEntry, RarFile


class SourceWrapper(object):
    source: Any = None
    _re_date = re.compile(r"[/\w]+_(\d{8})_.+")

    def __init__(self, source: Any, **kwargs: Any):
        super().__init__(**kwargs)
        self.source = source

    def get_date_info(self, filename: str) -> datetime.date:
        raise NotImplementedError()

    def get_date(self) -> datetime.date:
        try:
            date = datetime.datetime.strptime(self.source.read("version.txt").decode("utf-8"), "%Y.%m.%d")
        except (KeyError, NoRarEntry):
            dates_s = set()
            for file_name in self.get_file_list():
                match = self._re_date.match(file_name)
                if match is not None:
                    dates_s.add(match.group(1))
            date = sorted(map(lambda x: datetime.datetime.strptime(x, "%Y%m%d"), dates_s))[-1]
        return date

    def get_file_list(self) -> List[str]:
        raise NotImplementedError()

    def open(self, filename: str) -> IO[bytes]:
        raise NotImplementedError()


class DirectoryWrapper(SourceWrapper):
    is_temporary = False
    source: Path

    def __init__(self, source: Path, is_temporary: bool = False, **kwargs: Any):
        super(DirectoryWrapper, self).__init__(source=source.absolute(), **kwargs)
        self.is_temporary = is_temporary

    def get_date(self) -> datetime.datetime:
        raise NotImplementedError()

    def get_date_info(self, filename: str) -> datetime.date:
        st = (self.source / filename).stat()
        return datetime.datetime.fromtimestamp(st.st_mtime)

    def get_file_list(self) -> List[str]:
        return [f.name for f in self.source.iterdir() if (not f.name.startswith(".") and (self.source / f).is_file())]

    def get_full_path(self, filename: str) -> Path:
        return self.source / filename

    def open(self, filename: str) -> IO[bytes]:
        return open(self.get_full_path(filename), "rb")

    def __del__(self) -> None:
        if self.is_temporary:
            shutil.rmtree(self.source, ignore_errors=True)


class RarArchiveWrapper(SourceWrapper):
    source: Union[RarFile, ZipFile]

    def __init__(self, source: Union[RarFile, ZipFile], **kwargs: Any):
        super(RarArchiveWrapper, self).__init__(source=source, **kwargs)

    def get_date_info(self, filename: str) -> datetime.date:
        info = self.source.getinfo(filename)
        return datetime.date(*info.date_time[0:3])

    def get_file_list(self) -> List[str]:
        return self.source.namelist()

    def open(self, filename: str) -> IO[bytes]:
        return self.source.open(filename)
