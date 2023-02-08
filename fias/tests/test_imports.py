# coding: utf-8
from __future__ import unicode_literals, absolute_import

from unittest import TestCase
from importlib import import_module
import types


class TestImports(TestCase):

    def _import(self, path):
        return import_module(path)

    def test_importer_source(self):
        self.assertIsInstance(self._import('fias.importer.source'), types.ModuleType)

    def test_importer_table(self):
        self.assertIsInstance(self._import('fias.importer.table'), types.ModuleType)

    def test_importer(self):
        self.assertIsInstance(self._import('fias.importer'), types.ModuleType)

    def test_management_fias(self):
        self.assertIsInstance(self._import('fias.management.commands.fias'), types.ModuleType)

    def test_management_fiasinfo(self):
        self.assertIsInstance(self._import('fias.management.commands.fiasinfo'), types.ModuleType)

    def test_models(self):
        self.assertIsInstance(self._import('fias.models'), types.ModuleType)

    def test_routers(self):
        self.assertIsInstance(self._import('fias.routers'), types.ModuleType)
