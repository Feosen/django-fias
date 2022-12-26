# coding: utf-8
from __future__ import unicode_literals, absolute_import

from django.urls import path

from .views import *

urlpatterns = [
   path(r'suggest.json', SphinxResponseView.as_view(), name='suggest'),
   path(r'suggest-area.json', GetAreasListView.as_view(), name='suggest-area'),
]
