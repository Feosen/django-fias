# coding: utf-8
from __future__ import unicode_literals, absolute_import

import sys
from typing import Any

from fias.config import TABLES
from fias.models import Status, Version
from fias.importer.version import fetch_version_info

from gar_loader.compat import BaseCommandCompatible


class Command(BaseCommandCompatible):
    help = 'Fill or update FIAS database'
    usage_str = 'Usage: ./manage.py fiasinfo --db-version [--update-version-info <yes|no>]'

    arguments_dictionary = {
        "--db-version": {
            "action": "store_true",
            "dest": "show_ver",
            "default": False,
            "help": "Show version info"
        },
        "--update-version-info": {
            "action": "store",
            "dest": "fetch",
            "type": str,
            "choices": ["yes", "no"],
            "default": "yes",
            "help": "Update list of available database versions from http://fias.nalog.ru"
        }
    }

    def handle(self, show_ver: bool, fetch: str, **options: Any) -> None:
        if fetch == 'yes':
            fetch_version_info(update_all=True)

        if show_ver:
            latest_version = Version.objects.all().latest('dumpdate')
            print('Latest version: {0}'.format(latest_version))
            statuses = dict((s.table, s) for s in Status.objects.select_related('ver').all())
            for table in TABLES:
                ver = statuses[table].ver.ver if table in statuses else 0
                print('Table: {0}, version: {1}, actual: {2}'.format(
                    table,
                    ver if ver else 'unknown',
                    'No' if ver < latest_version.ver else 'Yes'
                ))

    def error(self, message: str, code: int = 1) -> None:
        print(message)
        sys.exit(code)
