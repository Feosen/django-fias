# coding: utf-8
from __future__ import unicode_literals, absolute_import

import sys
from typing import Any, Dict

from fias.config import TABLES
from fias.importer.commands import get_tablelist

from gar_loader.compat import BaseCommandCompatible


class Command(BaseCommandCompatible):
    help = "Утилита для поиска дубликатов по первичному ключу"
    usage_str = "Usage: ./manage.py --key <pk> --src <path> --table <table>"

    arguments_dictionary = {
        "--key": {"action": "store", "dest": "pk", "help": "List duplicates by PK"},
        "--src": {"action": "store", "dest": "src", "help": "Source directory for duplicates search"},
        "--table": {
            "action": "store",
            "dest": "table",
            "type": str,
            "choices": list(TABLES),
            "help": "Table to search for duplicates",
        },
    }

    def handle(self, key: str, src: str, table: str, **options: Any) -> None:
        if not any([key, src, table]):
            self.error(self.usage_str)

        tablelist = get_tablelist(path=src, data_format="xml")
        for tbl in tablelist.tables[table]:
            for item in tbl.rows(tablelist=tablelist):
                if item is not None and item.pk == key:
                    print(item)
                    print(item.__dict__)

    def error(self, message: str, code: int = 1) -> None:
        print(message)
        sys.exit(code)
