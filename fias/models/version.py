# coding: utf-8
from __future__ import unicode_literals, absolute_import

from django.db import models

__all__ = ['Version', 'Status']


class VersionManager(models.Manager):

    def nearest_by_date(self, date):
        try:
            return self.get_queryset().filter(dumpdate=date).latest('dumpdate')
        except Version.DoesNotExist:
            return self.get_queryset().filter(dumpdate__gte=date).earliest('dumpdate')


class Version(models.Model):

    class Meta:
        app_label = 'fias'

    objects = VersionManager()

    ver = models.IntegerField(primary_key=True)
    date = models.DateField(db_index=True, blank=True, null=True)
    dumpdate = models.DateField(db_index=True)

    complete_xml_url = models.CharField(max_length=255)
    delta_xml_url = models.CharField(max_length=255, blank=True, null=True)

    def __str__(self):
        return '{0} from {1}'.format(self.ver, self.dumpdate)


class Status(models.Model):

    class Meta:
        app_label = 'fias'
        constraints = [
            models.UniqueConstraint(fields=['region', 'table'], name='unique_region_table')
        ]

    # Null for house_type and other common tables.
    region = models.CharField(verbose_name='регион', max_length=2, null=True, blank=True)
    table = models.CharField(verbose_name='таблица', max_length=15)
    ver = models.ForeignKey(Version, verbose_name='версия', on_delete=models.CASCADE)
