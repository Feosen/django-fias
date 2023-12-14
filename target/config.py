# coding: utf-8
from __future__ import absolute_import, unicode_literals

from django.conf import settings
from django.core.exceptions import ImproperlyConfigured
from django.db.utils import DEFAULT_DB_ALIAS

__all__ = ["MANAGE", "DEFAULT_DB_ALIAS", "DATABASE_ALIAS", "LOAD_HOUSE_78_ONLY", "LOAD_HOUSE_BULK_SIZE"]


DATABASE_ALIAS: str = getattr(settings, "TARGET_DATABASE_ALIAS", DEFAULT_DB_ALIAS)
MANAGE: bool = getattr(settings, "TARGET_MANAGE", True) or settings.TEST
LOAD_HOUSE_78_ONLY: bool = getattr(settings, "TARGET_LOAD_HOUSE_78_ONLY", False)
LOAD_HOUSE_BULK_SIZE: int = getattr(settings, "TARGET_LOAD_HOUSE_BULK_SIZE", 0)

if DATABASE_ALIAS not in settings.DATABASES:
    raise ImproperlyConfigured(f"TARGET: database alias `{DATABASE_ALIAS}` was not found in DATABASES")
elif DATABASE_ALIAS != DEFAULT_DB_ALIAS and "target.routers.TargetRouter" not in settings.DATABASE_ROUTERS:
    raise ImproperlyConfigured(
        "TARGET: for use external database add `target.routers.FIASRouter`"
        " into `DATABASE_ROUTERS` list in your settings.py"
    )
