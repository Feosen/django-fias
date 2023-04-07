
# coding: utf-8
from __future__ import unicode_literals, absolute_import

import sys

from django.conf import settings
from django.utils.translation import activate

from fias.importer.source import TableListLoadingError
from gar_loader.compat import BaseCommandCompatible
from target.importer.commands import load_complete_data, update_data
from target.models import House, House78, AddrObj, HouseType, HouseAddType


class Command(BaseCommandCompatible):
    help = 'Fill or update target database'
    usage_str = 'Usage: ./manage.py target' \
                ' [--truncate]' \
                ' [--i-know-what-i-do]]' \
                ' [--update [--skip]]'

    arguments_dictionary = {
        "--truncate": {
            "action": "store_true",
            "dest": "truncate",
            "default": False,
            "help": "Truncate tables before loading data"
        },
        "--i-know-what-i-do": {
            "action": "store_true",
            "dest": "doit",
            "default": False,
            "help": "If data exist in any table, you should confirm their removal and replacement"
                    ", as this may result in the removal of related data from other tables!"
        },
        "--update": {
            "action": "store_true",
            "dest": "update",
            "default": False,
            "help": "Update database"
        },
        "--keep-indexes": {
            "action": "store_true",
            "dest": "keep_indexes",
            "default": False,
            "help": "Do not disable indexes before data import"
        },
    }

    def handle(self, *args, **options):
        truncate = options.pop('truncate')
        doit = options.pop('doit')

        update = options.pop('update')

        has_data = all(map(lambda x: x.objects.exists(), (House, House78, AddrObj, HouseType, HouseAddType)))
        if has_data and not doit and not update:
            self.error('One of the tables contains data. Truncate all target tables manually '
                       'or enter key --i-know-what-i-do, to clear the table by means of Django ORM')

        # Force Russian language for internationalized projects
        if settings.USE_I18N:
            activate('ru')

        keep_indexes = options.pop('keep_indexes')

        if update:
            try:
                update_data()
            except TableListLoadingError as e:
                self.error(str(e))

        else:
            try:
                load_complete_data(truncate=truncate, keep_indexes=keep_indexes)
            except TableListLoadingError as e:
                self.error(str(e))

    def error(self, message, code=1):
        print(message)
        sys.exit(code)
