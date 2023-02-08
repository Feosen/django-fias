# coding: utf-8
from __future__ import unicode_literals, absolute_import

from django.db import models

__all__ = ['AbstractModel', 'AbstractObj', 'AbstractParam', 'AbstractType', 'ParamType']


class AbstractModel(models.Model):
    updatedate = models.DateField(verbose_name='дата внесения (обновления) записи')
    startdate = models.DateField(verbose_name='начало действия записи')
    enddate = models.DateField(verbose_name='окончание действия записи')

    class Meta:
        abstract = True


class AbstractObj(AbstractModel):
    region = models.CharField(verbose_name='код региона', max_length=2)
    isactive = models.BooleanField(verbose_name='статус активности')
    isactual = models.BooleanField(verbose_name='Статус актуальности')
    objectid = models.BigIntegerField(verbose_name='глобальный уникальный идентификатор объекта', primary_key=True)
    objectguid = models.UUIDField(verbose_name='глобальный уникальный идентификатор адресного объекта')

    class Meta:
        abstract = True
        indexes = [models.Index(fields=['objectguid'])]


class AbstractType(AbstractModel):
    id = models.SmallAutoField(verbose_name='id', primary_key=True)
    name = models.CharField(verbose_name='наименование', max_length=255)
    shortname = models.CharField(verbose_name='краткое наименование', max_length=255, blank=True, null=True)
    desc = models.CharField(verbose_name='описание', max_length=255, blank=True, null=True)
    isactive = models.BooleanField(verbose_name='статус активности')

    def __str__(self):
        return self.name

    class Meta:
        abstract = True


class AbstractParam(AbstractModel):
    region = models.CharField(verbose_name='код региона', max_length=2)
    objectid = models.BigIntegerField(verbose_name='глобальный уникальный идентификатор объекта')
    typeid = models.SmallIntegerField(verbose_name='тип')
    value = models.CharField(verbose_name='значение', max_length=250)

    class Meta:
        abstract = True
        indexes = [models.Index(fields=['objectid']), models.Index(fields=['typeid'])]


class ParamType(AbstractType):

    class Meta(AbstractType.Meta):
        abstract = False
        app_label = 'fias'
        verbose_name = 'тип параметра'
        verbose_name_plural = 'типы параметров'
