# coding: utf-8
from __future__ import unicode_literals, absolute_import

import datetime
import uuid
from django.test import TestCase

from fias.importer.validators import (
    common_validator,
    chained_validator,
)
from fias.models import *

today = datetime.date.today()
diff = datetime.timedelta(1)
yesterday = today - diff
tomorrow = today + diff


class TestCommonValidator(TestCase):
    def test_startdate_tomorrow(self) -> None:
        m = AddrObj(startdate=tomorrow, enddate=tomorrow)
        self.assertFalse(common_validator(m, today=today))

    def test_enddate_yesterday(self) -> None:
        m = AddrObj(startdate=yesterday, enddate=yesterday)
        self.assertFalse(common_validator(m, today=today))

    def test_both_today(self) -> None:
        m = AddrObj(startdate=today, enddate=today)
        self.assertFalse(common_validator(m, today=today))

    def test_valid_model(self) -> None:
        m = AddrObj(startdate=yesterday, enddate=tomorrow)
        self.assertTrue(common_validator(m, today=today))


class TestAddrObjValidator(TestCase):
    def test_nextid(self) -> None:
        m = AddrObj(startdate=yesterday, enddate=tomorrow)
        self.assertFalse(chained_validator(m, today=today))

    def test_valid(self) -> None:
        m = AddrObj(startdate=yesterday, enddate=tomorrow, isactual=True)
        self.assertTrue(chained_validator(m, today=today))
