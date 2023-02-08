# coding: utf-8
from __future__ import unicode_literals, absolute_import

import zipfile

from django.conf import settings

import rarfile
import tempfile
from progress.bar import Bar

from fias.compat import urlretrieve, HTTPError
from fias.importer.signals import (
    pre_download, post_download,
    pre_unpack, post_unpack,
)
from .tablelist import TableList, TableListLoadingError
from .directory import DirectoryTableList
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
    def unpack(archive, tempdir=None):
        path = tempfile.mkdtemp(dir=tempdir)
        archive.extractall(path)
        return path

    def load_data(self, source):
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

    def load_data(self, source):
        progress = self.download_progress_class()

        def update_progress(count, block_size, total_size):
            progress.goto(int(count * block_size * 100 / total_size))

        pre_download.send(sender=self.__class__, url=source)
        try:
            path = urlretrieve(source, reporthook=update_progress)[0]
        except HTTPError as e:
            raise RetrieveError('Can not download data archive at url `{0}`. Error occurred: "{1}"'.format(
                source, str(e)
            ))
        progress.finish()
        post_download.send(sender=self.__class__, url=source, path=path)

        return super(RemoteArchiveTableList, self).load_data(source=path)
