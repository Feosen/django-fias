# coding: utf-8
from __future__ import unicode_literals, absolute_import

from django.dispatch import Signal

pre_fetch_version = Signal()
post_fetch_version = Signal()

pre_download = Signal()
post_download = Signal()

pre_unpack = Signal()
post_unpack = Signal()

pre_load = Signal()
post_load = Signal()

pre_drop_indexes = Signal()
post_drop_indexes = Signal()

pre_restore_indexes = Signal()
post_restore_indexes = Signal()

pre_import_table = Signal()
post_import_table = Signal()

pre_import = Signal()
post_import = Signal()

pre_update = Signal()
post_update = Signal()
