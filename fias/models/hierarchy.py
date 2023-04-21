# coding: utf-8
from __future__ import unicode_literals, absolute_import, annotations

from django.db import models

from fias.models.fields import BigIntegerRefField
from .addr_obj import AddrObj
from .common import AbstractIsActiveModel
from .house import House

__all__ = ['AbstractHierarchy', 'AdmHierarchy', 'MunHierarchy']


class AbstractHierarchy(AbstractIsActiveModel):
    region = models.CharField(verbose_name='код региона', max_length=2)
    objectid = BigIntegerRefField(to=[(AddrObj, 'objectid'), (House, 'objectid')],
                                  verbose_name='глобальный уникальный идентификатор объекта')
    parentobjid = models.BigIntegerField(verbose_name='идентификатор родительского объекта')

    class Meta(AbstractIsActiveModel.Meta):
        abstract = True


class AdmHierarchy(AbstractHierarchy):
    """
    Иерархия в административном делении
    """

    class Meta(AbstractHierarchy.Meta):
        abstract = False
        verbose_name = 'сведения по иерархии в административном делении'
        verbose_name_plural = 'сведения по иерархии в административном делении'
        indexes = [models.Index(fields=['objectid', 'parentobjid'])]


class MunHierarchy(AbstractHierarchy):
    """
    Иерархия в муниципальном делении
    """

    class Meta(AbstractHierarchy.Meta):
        abstract = False
        verbose_name = 'сведения по иерархии в муниципальном делении'
        verbose_name_plural = 'сведения по иерархии в муниципальном делении'
        indexes = [models.Index(fields=['objectid', 'parentobjid'])]
