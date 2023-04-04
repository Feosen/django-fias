# coding: utf-8
from __future__ import unicode_literals, absolute_import

from target.config import DEFAULT_DB_ALIAS, DATABASE_ALIAS


class TargetRouter(object):
    app_labels = ('target',)
    ALLOWED_REL = ['AddrObj']
    
    def db_for_read(self, model, **hints):
        if model._meta.app_label in self.app_labels:
            return DATABASE_ALIAS
        return None

    def db_for_write(self, model, **hints):
        if model._meta.app_label in self.app_labels:
            return DATABASE_ALIAS
        else:
            # TODO: check it!
            """\
            Странный хак, но без него
            джанго не может правильно определить БД для записи\
            """
            raise ValueError
            try:
                if hints['instance']._meta.object_name == 'AddrObj':
                    return DEFAULT_DB_ALIAS
            except KeyError:
                pass
        return None

    def allow_relation(self, obj1, obj2, **hints):
        """\
        Разрешить связи из других бд к таблицам ФИАС
        но запретить ссылаться из бд ФИАС в другие БД
        """

        if obj1._meta.app_label in self.app_labels and obj2._meta.app_label in self.app_labels:
            return True
        elif obj1._meta.app_label in self.app_labels and obj1._meta.object_name in self.ALLOWED_REL:
            return True
        return None

    def allow_migrate(self, db, app_label, model=None, **hints):
        """Разрешить синхронизацию моделей в базе ФИАС"""
        if app_label in self.app_labels:
            return db == DATABASE_ALIAS
        #elif db == DATABASE_ALIAS:
        #    return False

        return None
