# coding: utf-8
from __future__ import absolute_import, unicode_literals

from django.conf import settings
from django.core.exceptions import ImproperlyConfigured
from django.db.utils import DEFAULT_DB_ALIAS

__all__ = ["MANAGE", "DEFAULT_DB_ALIAS", "DATABASE_ALIAS"]


DATABASE_ALIAS = getattr(settings, "TARGET_DATABASE_ALIAS", DEFAULT_DB_ALIAS)
MANAGE = getattr(settings, "TARGET_MANAGE", True) or settings.TEST

if DATABASE_ALIAS not in settings.DATABASES:
    raise ImproperlyConfigured(f"TARGET: database alias `{DATABASE_ALIAS}` was not found in DATABASES")
elif DATABASE_ALIAS != DEFAULT_DB_ALIAS and "target.routers.TargetRouter" not in settings.DATABASE_ROUTERS:
    raise ImproperlyConfigured(
        "TARGET: for use external database add `target.routers.FIASRouter`"
        " into `DATABASE_ROUTERS` list in your settings.py"
    )