# coding: utf-8
from __future__ import absolute_import, unicode_literals

import logging
import os
import tempfile
import urllib
import zipfile
from pathlib import Path
from urllib.error import HTTPError
from urllib.request import urlretrieve

import rarfile
from django.conf import settings
from progress.bar import Bar

from fias.importer.signals import post_download, pre_download

from .tablelist import TableList, TableListLoadingError
from .wrapper import RarArchiveWrapper, SourceWrapper

logger = logging.getLogger(__name__)

if os.name == "nt":
    DEFAULT_UNRAR_TOOL = "unrar.exe"
else:
    DEFAULT_UNRAR_TOOL = "unrar"

rarfile.UNRAR_TOOL = getattr(settings, "FIAS_UNRAR_TOOL", DEFAULT_UNRAR_TOOL)


class BadArchiveError(TableListLoadingError):
    pass


class RetrieveError(TableListLoadingError):
    pass


class LocalArchiveTableList(TableList):
    wrapper_class = RarArchiveWrapper

    @staticmethod
    def unpack(archive: rarfile.RarFile, tempdir: Path | None = None) -> str:
        path = tempfile.mkdtemp(dir=tempdir)
        archive.extractall(path)
        return path

    def load_data(self, source: str) -> SourceWrapper:
        source_path = Path(source)
        try:
            archive = rarfile.RarFile(source_path)
        # except (rarfile.NotRarFile, rarfile.BadRarFile) as e:
        except Exception as e1:
            try:
                archive = zipfile.ZipFile(source_path)
            except Exception as e2:
                raise BadArchiveError(f"Archive: `{source_path}` corrupted or is not rar,zip-archive; {e1}; {e2}")

        if not archive.namelist():
            raise BadArchiveError(f"Archive: `{source_path}`, is empty")

        return self.wrapper_class(source=archive)


class DlProgressBar(Bar):  # type: ignore
    message = "Downloading: "
    suffix = "%(index)d/%(max)d. ETA: %(elapsed)s"
    hide_cursor = False


class RemoteArchiveTableList(LocalArchiveTableList):
    download_progress_class = DlProgressBar

    def load_data(self, source: str) -> SourceWrapper:
        progress = self.download_progress_class()

        def update_progress(count: int, block_size: int, total_size: int) -> None:
            progress.goto(int(count * block_size * 100 / total_size))

        if self.tempdir is not None:
            p = urllib.parse.urlparse(source)
            tmp_path = self.tempdir / p.path.split("/")[-1]
        else:
            tmp_path = None
        logger.info(f"Downloading from {source}.")
        pre_download.send(sender=self.__class__, url=source)
        try:
            path = urlretrieve(source, filename=tmp_path, reporthook=update_progress)[0]
        except HTTPError as e:
            raise RetrieveError(
                'Can not download data archive at url `{0}`. Error occurred: "{1}"'.format(source, str(e))
            )
        progress.finish()
        post_download.send(sender=self.__class__, url=source, path=path)
        logger.info(f"Downloaded from {source} into {path}.")

        return super(RemoteArchiveTableList, self).load_data(source=path)
