# coding: utf-8
from __future__ import absolute_import, unicode_literals

import datetime

from django.test import TestCase

from fias.config import TableName
from fias.importer.validators import (
    get_common_validator,
    get_create_validator,
    get_update_validator,
)
from fias.models import AddrObj

today = datetime.date.today()
diff = datetime.timedelta(1)
yesterday = today - diff
tomorrow = today + diff


class TestCommonValidator(TestCase):
    def setUp(self) -> None:
        super().setUp()
        self.common_validator = get_common_validator(TableName.ADDR_OBJ)

    def test_no_pk_model(self) -> None:
        m = AddrObj(startdate=yesterday, enddate=tomorrow, isactive=True, isactual=True)
        self.assertFalse(self.common_validator(m, today))

    def test_valid_model(self) -> None:
        m = AddrObj(pk=1, startdate=yesterday, enddate=tomorrow, isactive=True, isactual=True)
        self.assertTrue(self.common_validator(m, today))


class TestCreateValidator(TestCase):
    def setUp(self) -> None:
        super().setUp()
        self.create_validator = get_create_validator(TableName.ADDR_OBJ)

    def test_startdate_tomorrow(self) -> None:
        m = AddrObj(pk=1, startdate=tomorrow, enddate=tomorrow, isactive=True, isactual=True)
        self.assertFalse(self.create_validator(m, today))

    def test_enddate_yesterday(self) -> None:
        m = AddrObj(pk=1, startdate=yesterday, enddate=yesterday, isactive=True, isactual=True)
        self.assertFalse(self.create_validator(m, today))

    def test_both_today(self) -> None:
        m = AddrObj(pk=1, startdate=today, enddate=today, isactive=True, isactual=True)
        self.assertFalse(self.create_validator(m, today))

    def test_not_active(self) -> None:
        m = AddrObj(pk=1, startdate=yesterday, enddate=tomorrow, isactive=False, isactual=True)
        self.assertFalse(self.create_validator(m, today))

    def test_not_actual(self) -> None:
        m = AddrObj(pk=1, startdate=yesterday, enddate=tomorrow, isactive=True, isactual=False)
        self.assertFalse(self.create_validator(m, today))

    def test_valid_model(self) -> None:
        m = AddrObj(pk=1, startdate=yesterday, enddate=tomorrow, isactive=True, isactual=True)
        self.assertTrue(self.create_validator(m, today))


class TestUpdateValidator(TestCase):
    def setUp(self) -> None:
        super().setUp()
        self.update_validator = get_update_validator(TableName.ADDR_OBJ)

    def test_valid_model(self) -> None:
        m = AddrObj(pk=1, startdate=yesterday, enddate=tomorrow, isactive=True, isactual=True)
        self.assertTrue(self.update_validator(m, today))
