# coding: utf-8
from __future__ import unicode_literals, absolute_import

import datetime
import os
import shutil
import tempfile
from pathlib import Path

import rarfile
from django.test import TestCase

from fias.importer.source.wrapper import (
    SourceWrapper,
    DirectoryWrapper,
    RarArchiveWrapper,
)
from .info import FAKE_DIR_PATH, FAKE_ARCHIVE_PATH, FAKE_FILES


class TestSourceWrapper(TestCase):

    def setUp(self) -> None:
        self.wrapper = SourceWrapper(None)

    def test_getting_date_info(self) -> None:
        self.assertRaises(NotImplementedError, self.wrapper.get_date_info, filename=None)

    def test_getting_file_list(self) -> None:
        self.assertRaises(NotImplementedError, self.wrapper.get_file_list)

    def test_opening_file(self) -> None:
        self.assertRaises(NotImplementedError, self.wrapper.open, filename=None)


class TestDirectoryWrapper(TestCase):
    wrapper: SourceWrapper

    def setUp(self) -> None:
        self.wrapper = DirectoryWrapper(FAKE_DIR_PATH)

    def test_getting_file_list(self) -> None:
        filelist = self.wrapper.get_file_list()

        self.assertEqual(set(filelist), set(FAKE_FILES))

    def test_getting_date_info(self) -> None:
        date_info = self.wrapper.get_date_info(FAKE_FILES[0])

        self.assertIsInstance(date_info, datetime.date)

    def test_opening_file(self) -> None:
        filename = FAKE_FILES[0]
        fd = self.wrapper.open(filename=filename)

        self.assertEqual(Path(fd.name).name, filename)

        data = fd.read()

        self.assertEqual(data.decode('utf-8'), filename)


class TestTemporaryDirectoryWrapper(TestCase):

    def setUp(self) -> None:
        tmp = tempfile.mktemp()
        shutil.copytree(FAKE_DIR_PATH, tmp)

        self.wrapper = DirectoryWrapper(Path(tmp), is_temporary=True)

    def test_deleting_temporary_data(self) -> None:
        source = self.wrapper.source
        self.assertTrue(Path(source).exists())

        del self.wrapper

        self.assertFalse(Path(source).exists())


class TestArchiveWrapper(TestDirectoryWrapper):

    def setUp(self) -> None:
        self.wrapper = RarArchiveWrapper(rarfile.RarFile(FAKE_ARCHIVE_PATH))

