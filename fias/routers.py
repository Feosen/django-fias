# coding: utf-8
from __future__ import unicode_literals, absolute_import

from typing import Any, Union, Type

from django.db.models import Model

from fias.config import DEFAULT_DB_ALIAS, DATABASE_ALIAS


class FIASRouter(object):
    app_labels = ("fias",)
    ALLOWED_REL = ["AddrObj"]

    def db_for_read(self, model: Type[Model], **hints: Any) -> Union[str, None]:
        if model._meta.app_label in self.app_labels:
            return DATABASE_ALIAS
        return None

    def db_for_write(self, model: Type[Model], **hints: Any) -> Union[str, None]:
        if model._meta.app_label in self.app_labels:
            return DATABASE_ALIAS
        else:
            # TODO: check it!
            """\
            Странный хак, но без него
            джанго не может правильно определить БД для записи\
            """
            try:
                if hints["instance"]._meta.object_name == "AddrObj":
                    return DEFAULT_DB_ALIAS
            except KeyError:
                pass
        return None

    def allow_relation(
        self, obj1: Union[Model, Type[Model]], obj2: Union[Model, Type[Model]], **hints: Any
    ) -> Union[bool, None]:
        """\
        Разрешить связи из других бд к таблицам ФИАС
        но запретить ссылаться из бд ФИАС в другие БД
        """

        if obj1._meta.app_label in self.app_labels and obj2._meta.app_label in self.app_labels:
            return True
        elif obj1._meta.app_label in self.app_labels and obj1._meta.object_name in self.ALLOWED_REL:
            return True
        return None

    def allow_migrate(
        self, db: str, app_label: str, model: Type[Model] | None = None, **hints: Any
    ) -> Union[bool, None]:
        """Разрешить синхронизацию моделей в базе ФИАС"""
        if app_label in self.app_labels:
            return db == DATABASE_ALIAS
        # elif db == DATABASE_ALIAS:
        #    return False

        return None
