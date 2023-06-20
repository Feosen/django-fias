# coding: utf-8
from __future__ import absolute_import, unicode_literals

from typing import List, Tuple, Type

from django.db import connections
from django.db.models import Model
from django.test import TestCase

from fias.config import DATABASE_ALIAS
from fias.models import House
from gar_loader.indexes import remove_indexes_from_model, restore_indexes_for_model


class TestIndexes(TestCase):
    databases = {"default", "gar"}

    @staticmethod
    def _get_constraints(model: Type[Model]) -> Tuple[List[str], List[str]]:
        connection = connections[DATABASE_ALIAS]
        with connection.cursor() as cursor:
            table_name = model._meta.db_table
            pk_constraints = []
            other_constraints = []
            for constraint, params in connection.introspection.get_constraints(cursor, table_name).items():
                if params["primary_key"]:
                    pk_constraints.append(constraint)
                else:
                    other_constraints.append(constraint)
            return pk_constraints, other_constraints

    def test_indexes_with_pk(self) -> None:
        pk, other = self._get_constraints(House)
        self.assertListEqual(["fias_house_pkey"], pk)
        self.assertListEqual(
            ["fias_house_region78_idx", "fias_house_tree_ve_4bdad3_idx", "fias_house_ver_4fd60c_idx"], other
        )

        remove_indexes_from_model(House, True)
        pk, other = self._get_constraints(House)
        self.assertListEqual([], pk)
        self.assertListEqual([], other)

        restore_indexes_for_model(House, True)
        pk, other = self._get_constraints(House)
        self.assertListEqual(["fias_house_objectid_38079c90_pk"], pk)
        self.assertListEqual(
            ["fias_house_region78_idx", "fias_house_tree_ve_4bdad3_idx", "fias_house_ver_4fd60c_idx"], other
        )

    def test_indexes_without_pk(self) -> None:
        pk, other = self._get_constraints(House)
        self.assertListEqual(["fias_house_pkey"], pk)
        self.assertListEqual(
            ["fias_house_region78_idx", "fias_house_tree_ve_4bdad3_idx", "fias_house_ver_4fd60c_idx"], other
        )

        remove_indexes_from_model(House, False)
        pk, other = self._get_constraints(House)
        self.assertListEqual(["fias_house_pkey"], pk)
        self.assertListEqual([], other)

        restore_indexes_for_model(House, False)
        pk, other = self._get_constraints(House)
        self.assertListEqual(["fias_house_pkey"], pk)
        self.assertListEqual(
            ["fias_house_region78_idx", "fias_house_tree_ve_4bdad3_idx", "fias_house_ver_4fd60c_idx"], other
        )
