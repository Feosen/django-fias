# coding: utf-8
from __future__ import unicode_literals, absolute_import

import tempfile
import zipfile
from pathlib import Path
from urllib.error import HTTPError
from urllib.request import urlretrieve

import rarfile
from django.conf import settings
from progress.bar import Bar

from fias.importer.signals import (
    pre_download, post_download,
)
from .tablelist import TableList, TableListLoadingError
from .wrapper import RarArchiveWrapper

# Задаем UNRAR_TOOL глобально
rarfile.UNRAR_TOOL = getattr(settings, 'FIAS_UNRAR_TOOL', 'unrar')


class BadArchiveError(TableListLoadingError):
    pass


class RetrieveError(TableListLoadingError):
    pass


class LocalArchiveTableList(TableList):
    wrapper_class = RarArchiveWrapper

    @staticmethod
    def unpack(archive: rarfile.RarFile, tempdir=None):
        path = tempfile.mkdtemp(dir=tempdir)
        archive.extractall(path)
        return path

    def load_data(self, source: Path):
        try:
            archive = rarfile.RarFile(source)
        # except (rarfile.NotRarFile, rarfile.BadRarFile) as e:
        except Exception as e1:
            try:
                archive = zipfile.ZipFile(source)
            except Exception as e2:
                raise BadArchiveError('Archive: `{}` corrupted or is not rar,zip-archive; {}; {}'.format(
                    source, e1, e2))

        if not archive.namelist():
            raise BadArchiveError('Archive: `{}`, is empty'.format(source))

        return self.wrapper_class(source=archive)


class DlProgressBar(Bar):
    message = 'Downloading: '
    suffix = '%(index)d/%(max)d. ETA: %(elapsed)s'
    hide_cursor = False


class RemoteArchiveTableList(LocalArchiveTableList):
    download_progress_class = DlProgressBar

    def load_data(self, source: str):
        progress = self.download_progress_class()

        def update_progress(count, block_size: int, total_size: int):
            progress.goto(int(count * block_size * 100 / total_size))

        pre_download.send(sender=self.__class__, url=source)
        try:
            path = Path(urlretrieve(source, reporthook=update_progress)[0])
        except HTTPError as e:
            raise RetrieveError('Can not download data archive at url `{0}`. Error occurred: "{1}"'.format(
                source, str(e)
            ))
        progress.finish()
        post_download.send(sender=self.__class__, url=source, path=path)

        return super(RemoteArchiveTableList, self).load_data(source=path)
