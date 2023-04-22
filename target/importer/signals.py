# coding: utf-8
from __future__ import absolute_import, unicode_literals

from django.dispatch import Signal

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
