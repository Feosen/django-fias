# coding: utf-8
from __future__ import unicode_literals, absolute_import

import datetime
import json
import urllib.request

import zeep.client
from django.core.exceptions import ImproperlyConfigured
from requests.exceptions import HTTPError
from zeep.exceptions import XMLSyntaxError

from fias.config import PROXY
from fias.importer.signals import pre_fetch_version, post_fetch_version
from fias.models import Version


wsdl_source = "http://fias.nalog.ru/WebServices/Public/DownloadService.asmx?WSDL"
json_source = "http://fias.nalog.ru/WebServices/Public/GetAllDownloadFileInfo"


def parse_item_as_dict(item: dict, update_all=False):
    """
    Разбор данных о версии как словаря
    """
    ver, created = Version.objects.get_or_create(
        ver=item['VersionId'],
        dumpdate=datetime.datetime.strptime(item['TextVersion'][-10:], "%d.%m.%Y").date(),
    )
    if created or update_all:
        ver.complete_xml_url = item.get('GarXMLFullURL', None)
        ver.delta_xml_url = item.get('GarXMLDeltaURL', None)
        ver.save()


def parse_item_as_object(item, update_all=False):
    """
    Разбор данных о версии, как объекта
    """
    ver, created = Version.objects.get_or_create(
        ver=item.VersionId,
        dumpdate=datetime.datetime.strptime(item.TextVersion[-10:], "%d.%m.%Y").date(),
    )
    if created or update_all:
        ver.complete_xml_url = item.GarXMLFullURL
        ver.delta_xml_url = item.GarXMLDeltaURL

        ver.save()


def iter_version_info(result):
    if hasattr(result, 'DownloadFileInfo'):
        for item in result.DownloadFileInfo:
            yield item
    else:
        for item in result:
            yield item


class ClientContainer:
    _client: 'zeep.client.Client' = None

    def get_client(self):
        if self._client is None:
            try:
                try:
                    from zeep.client import Client
                    from zeep import __version__ as zver
                    z_major, z_minor, z_sub = list(map(int, zver.split('.')))

                    if z_minor < 20:
                        parse_func = parse_item_as_object
                    elif z_minor > 20:
                        parse_func = parse_item_as_dict

                    self._client = Client(wsdl=wsdl_source)
                except ImportError:
                    try:
                        from suds.client import Client

                        parse_func = parse_item_as_dict
                        _client = Client(url=wsdl_source, proxy=PROXY or None)

                    except ImportError:
                        raise ImproperlyConfigured('Не найдено подходящей библиотеки для работы с WSDL.'
                                                   ' Пожалуйста установите zeep или suds!')
            except HTTPError:
                print('Сайт не отвечает при запросе WSDL')
            except Exception as e:
                print(e)
                raise e
        return self._client


_cc = ClientContainer()


def fetch_version_info(update_all=False):

    pre_fetch_version.send(object.__class__)

    try:
        result = _cc.get_client().service.GetAllDownloadFileInfo()
    except XMLSyntaxError:
        with urllib.request.urlopen(json_source) as url:
            result = json.loads(url.read().decode())
            parse_func = parse_item_as_dict
    for item in iter_version_info(result=result):
        parse_func(item=item, update_all=update_all)

    post_fetch_version.send(object.__class__)
