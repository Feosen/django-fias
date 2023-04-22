# coding: utf-8
from __future__ import absolute_import, unicode_literals

from django.db.models import Model


class FakeModel(Model):
    class Meta:
        app_label = "nofias"


class FakeModel2(Model):
    class Meta:
        app_label = "nofias"
