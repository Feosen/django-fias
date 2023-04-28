# coding: utf-8
from __future__ import absolute_import, annotations, unicode_literals

from typing import Tuple, TypeVar

from django.db import connections, models

from fias.models.fields import RefFieldMixin

__all__ = ["AbstractModel", "AbstractIsActiveModel", "AbstractObj", "AbstractParam", "AbstractType", "ParamType"]

_M = TypeVar("_M", bound="AbstractModel", covariant=True)


class Manager(models.Manager[_M]):
    def delete_orphans(self) -> Tuple[int, dict[str, int]]:
        res_c: int = 0
        res_d: dict[str, int] = {}
        for field in self.model._meta.get_fields():
            if isinstance(field, RefFieldMixin):
                # TODO: profile it
                qs = self.get_queryset()
                for cfg in field.to:
                    qs = qs.exclude(**{f"{field.name}__in": cfg[0].objects.all()})
                c, d = qs.delete()
                res_c += c
                res_d |= d
        return res_c, res_d

    def update_tree_ver(self, min_ver: int) -> None:
        src_table = self.model._meta.db_table
        for field in self.model._meta.get_fields():
            if isinstance(field, RefFieldMixin):
                for model, pk_field_name in field.to:
                    dst_table = model._meta.db_table
                    raw_sql = f"""UPDATE {dst_table}
                               SET tree_ver = {src_table}.ver
                               FROM {src_table}
                               WHERE {dst_table}.{pk_field_name} = {src_table}.{pk_field_name}
                               AND {src_table}.ver >= {min_ver}
                               AND {src_table}.ver > tree_ver"""
                    connection = connections[self.db]
                    with connection.cursor() as cursor:
                        cursor.execute(raw_sql)


class AbstractModel(models.Model):
    ver = models.IntegerField(verbose_name="версия")
    updatedate = models.DateField(verbose_name="дата внесения (обновления) записи")
    startdate = models.DateField(verbose_name="начало действия записи")
    enddate = models.DateField(verbose_name="окончание действия записи")

    objects: Manager[AbstractModel] = Manager()

    class Meta:
        abstract = True
        app_label = "fias"


class AbstractIsActiveModel(AbstractModel):
    isactive = models.BooleanField(verbose_name="статус активности")

    class Meta(AbstractModel.Meta):
        abstract = True


class AbstractObj(AbstractIsActiveModel):
    region = models.CharField(verbose_name="код региона", max_length=2)
    isactual = models.BooleanField(verbose_name="статус актуальности")
    objectid = models.BigIntegerField(verbose_name="глобальный уникальный идентификатор объекта", primary_key=True)
    objectguid = models.UUIDField(verbose_name="глобальный уникальный идентификатор адресного объекта")
    tree_ver = models.IntegerField(verbose_name="версия набора")

    class Meta(AbstractIsActiveModel.Meta):
        abstract = True
        indexes = [models.Index(fields=["objectguid"])]


class AbstractType(AbstractIsActiveModel):
    id = models.SmallAutoField(verbose_name="id", primary_key=True)
    name = models.CharField(verbose_name="наименование", max_length=255)
    shortname = models.CharField(verbose_name="краткое наименование", max_length=255, blank=True, null=True)
    desc = models.CharField(verbose_name="описание", max_length=255, blank=True, null=True)

    class Meta(AbstractIsActiveModel.Meta):
        abstract = True

    def __str__(self) -> str:
        return self.name


class AbstractParam(AbstractModel):
    region = models.CharField(verbose_name="код региона", max_length=2)
    typeid = models.SmallIntegerField(verbose_name="тип")
    value = models.CharField(verbose_name="значение", max_length=250)

    class Meta:
        abstract = True
        indexes = [models.Index(fields=["objectid"]), models.Index(fields=["typeid"])]


class ParamType(AbstractType):
    class Meta(AbstractType.Meta):
        abstract = False
        verbose_name = "тип параметра"
        verbose_name_plural = "типы параметров"
