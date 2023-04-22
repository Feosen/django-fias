# coding: utf-8
from __future__ import absolute_import, unicode_literals

from pathlib import Path

DATA_PATH = Path(__file__).parent / "data"

FAKE_SOURCE_PATH = DATA_PATH / "fake"
FAKE_DIR_PATH = FAKE_SOURCE_PATH / "directory"
FAKE_ARCHIVE_PATH = FAKE_SOURCE_PATH / "archive.rar"
FAKE_FILES = ["file0", "file1", "file2"]
