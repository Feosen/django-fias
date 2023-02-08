# coding: utf-8
from __future__ import unicode_literals, absolute_import

from django.db import models

from fias.models.common import AbstractType, AbstractObj, AbstractParam

__all__ = ['House', 'HouseType', 'AddHouseType', 'HouseParam']


class HouseType(AbstractType):
    """
    Сведения по типам домов
    """

    class Meta(AbstractType.Meta):
        abstract = False
        app_label = 'fias'
        verbose_name = 'тип дома'
        verbose_name_plural = 'типы домов'


class AddHouseType(AbstractType):
    """
    Сведения по дополнительным типам домов
    """

    class Meta(AbstractType.Meta):
        abstract = False
        app_label = 'fias'
        verbose_name = 'дополнительный тип дома'
        verbose_name_plural = 'дополнительные типы домов'


class House(AbstractObj):
    """
    Сведения по номерам домов улиц городов и населенных пунктов
    """

    class Meta(AbstractObj.Meta):
        abstract = False
        app_label = 'fias'
        verbose_name = 'номер дома'
        verbose_name_plural = 'номера домов'
        indexes = [models.Index(fields=['objectid'])]

    housenum = models.CharField(verbose_name='номер дома', max_length=20, blank=True, null=True)
    addnum1 = models.CharField(verbose_name='дополнительный номер дома 1', max_length=20, blank=True, null=True)
    addnum2 = models.CharField(verbose_name='дополнительный номер дома 2', max_length=20, blank=True, null=True)
    housetype = models.IntegerField(verbose_name='основной тип дома')
    addtype1 = models.IntegerField(verbose_name='дополнительный тип номера дома 1', blank=True, null=True)
    addtype2 = models.IntegerField(verbose_name='дополнительный тип номера дома 2', blank=True, null=True)


class HouseParam(AbstractParam):
    """
    Параметры домов
    """

    class Meta(AbstractParam.Meta):
        abstract = False
        app_label = 'fias'
        verbose_name = 'параметр дома'
        verbose_name_plural = 'параметры домов'
