# coding: utf-8
from __future__ import unicode_literals, absolute_import

from django.urls import path

from .views import *

urlpatterns = [
    path(r'suggest.json', EmptyResponseListView.as_view()),
    path(r'suggest-area.json', EmptyResponseListView.as_view()),
]
