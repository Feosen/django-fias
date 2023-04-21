# coding: utf-8
from __future__ import unicode_literals, absolute_import, annotations

from django.db import models

from fias.models.common import AbstractType, AbstractObj, AbstractParam
from .fields import BigIntegerRefField

__all__ = ['AddrObj', 'AddrObjParam', 'AddrObjType']


class AddrObjType(AbstractType):
    """
    Сведения по типам домов
    """
    level = models.IntegerField(verbose_name='уровень адресного объекта')

    class Meta(AbstractType.Meta):
        abstract = False
        verbose_name = 'тип дома'
        verbose_name_plural = 'типы домов'


class AddrObj(AbstractObj):
    """
    Классификатор адресообразующих элементов
    """
    name = models.TextField(verbose_name='наименование')
    level = models.PositiveIntegerField(verbose_name='Уровень адресного объект')
    typename = models.TextField(verbose_name='Краткое наименование типа объекта')

    class Meta(AbstractObj.Meta):
        abstract = False
        verbose_name = 'адресообразующий элемент'
        verbose_name_plural = 'адресообразующие элементы'


class AddrObjParam(AbstractParam):
    """
    Параметры домов
    """
    objectid = BigIntegerRefField([(AddrObj, 'objectid')], verbose_name='глобальный уникальный идентификатор объекта')

    class Meta(AbstractParam.Meta):
        abstract = False
        verbose_name = 'параметр адресообразующего элемента'
        verbose_name_plural = 'параметры адресообразующих элементов'
