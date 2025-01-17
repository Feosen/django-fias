# coding: utf-8
from __future__ import absolute_import, unicode_literals

from django.db import models
from django.test import TestCase

from fias.config import DATABASE_ALIAS, DEFAULT_DB_ALIAS
from fias.routers import FIASRouter

from .mock.models import FakeModel, FakeModel2

fias = __import__("fias.models")
fias_models = [
    "AddrObjType",
    "AddrObj",
    "AddrObjParam",
    "House",
    "HouseType",
    "HouseParam",
    "ParamType",
    "AdmHierarchy",
    "MunHierarchy",
    "Version",
    "Status",
]


class TestRouter(TestCase):
    def setUp(self) -> None:
        self.router = FIASRouter()
        self.models = (getattr(fias.models, attr) for attr in fias_models)
        self.rels = (getattr(fias.models, attr) for attr in FIASRouter.ALLOWED_REL)

    def test_read_db(self) -> None:
        for model in self.models:
            if issubclass(model, models.Model):
                self.assertEquals(DATABASE_ALIAS, self.router.db_for_read(model))

        self.assertIsNone(self.router.db_for_read(FakeModel))

    def test_write_db(self) -> None:
        for model in self.models:
            if issubclass(model, models.Model):
                self.assertEquals(DATABASE_ALIAS, self.router.db_for_write(model))

        self.assertIsNone(self.router.db_for_write(FakeModel))

    def test_relation(self) -> None:
        for m1 in self.models:
            for m2 in self.models:
                self.assertTrue(self.router.allow_relation(m1, m2))

        for m in self.rels:
            self.assertFalse(self.router.allow_relation(FakeModel, m))
            self.assertTrue(self.router.allow_relation(m, FakeModel))

        self.assertIsNone(self.router.allow_relation(FakeModel, FakeModel2))
        self.assertIsNone(self.router.allow_relation(FakeModel2, FakeModel))

    def test_migrate(self) -> None:
        method = self.router.allow_migrate

        for model in self.models:
            if issubclass(model, models.Model):
                self.assertTrue(method(DATABASE_ALIAS, model._meta.app_label, model))
                self.assertFalse(method(DEFAULT_DB_ALIAS, model._meta.app_label, model))

        self.assertFalse(method(DATABASE_ALIAS, FakeModel._meta.app_label, FakeModel))
        self.assertIsNone(method(DEFAULT_DB_ALIAS, FakeModel._meta.app_label, FakeModel))
