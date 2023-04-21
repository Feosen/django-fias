# coding: utf-8
from __future__ import unicode_literals, absolute_import

import datetime
import json
import logging
import urllib.request
from dataclasses import dataclass
from typing import List, Dict, Any, Iterable, Protocol

from django.core.exceptions import ImproperlyConfigured
from requests.exceptions import HTTPError
from zeep.exceptions import XMLSyntaxError

from fias.config import PROXY
from fias.importer.signals import pre_fetch_version, post_fetch_version
from fias.models import Version

logger = logging.getLogger(__name__)

WSDL_SOURCE = "http://fias.nalog.ru/WebServices/Public/DownloadService.asmx?WSDL"
JSON_SOURCE = "http://fias.nalog.ru/WebServices/Public/GetAllDownloadFileInfo"


class ObjFileInfo:
    VersionId: int
    TextVersion: str
    GarXMLFullURL: str
    GarXMLDeltaURL: str


class ObjResponse:
    DownloadFileInfo: List[ObjFileInfo]


@dataclass
class VersionInfo:
    version_id: int
    text_version: str
    complete_xml_url: str
    delta_xml_url: str


class Parser:
    @staticmethod
    def parse(item: Any) -> VersionInfo:
        raise NotImplemented


class WsdlParser(Parser):
    @staticmethod
    def parse(item: ObjFileInfo) -> VersionInfo:
        return VersionInfo(item.VersionId, item.TextVersion, item.GarXMLFullURL, item.GarXMLDeltaURL)


class JsonParser(Parser):
    @staticmethod
    def parse(item: Dict[str, Any]) -> VersionInfo:
        return VersionInfo(
            item["VersionId"], item["TextVersion"], item.get("GarXMLFullURL", None), item.get("GarXMLDeltaURL", None)
        )


class Client:
    parser: Parser

    def __init__(self, parser: Parser):
        self.parser = parser

    def version_info(self) -> Iterable[VersionInfo]:
        raise NotImplemented


class ServiceProtocol(Protocol):
    def GetAllDownloadFileInfo(self) -> ObjResponse:
        raise NotImplemented


class WsdlClientProtocol(Protocol):
    @property
    def service(self) -> ServiceProtocol:
        raise NotImplemented


class WsdlClient(Client):
    client: WsdlClientProtocol

    def __init__(self, parser: Parser, client: WsdlClientProtocol):
        super().__init__(parser)
        self.client = client

    def version_info(self) -> Iterable[VersionInfo]:
        info = self.client.service.GetAllDownloadFileInfo()
        for item in info.DownloadFileInfo:
            yield self.parser.parse(item)


class JsonClient(Client):
    def version_info(self) -> Iterable[VersionInfo]:
        with urllib.request.urlopen(JSON_SOURCE) as url:
            info = json.loads(url.read().decode())
        for item in info:
            yield self.parser.parse(item)


def json_client_factory() -> Client:
    return JsonClient(JsonParser())


def wsdl_client_factory() -> Client:
    try:
        try:
            from zeep.client import Client as ZeepClient
            from zeep import __version__ as zver

            z_major, z_minor, z_sub = list(map(int, zver.split(".")))

            zeep_client = ZeepClient(wsdl=WSDL_SOURCE)  # type: ignore
            if z_minor < 20:
                return WsdlClient(WsdlParser(), zeep_client)
            else:
                return WsdlClient(JsonParser(), zeep_client)

        except ImportError:
            pass
        try:
            from suds.client import Client as SudsClient

            return WsdlClient(WsdlParser(), SudsClient(url=WSDL_SOURCE, proxy=PROXY or None))

        except ImportError:
            raise ImproperlyConfigured(
                "Не найдено подходящей библиотеки для работы с WSDL." " Пожалуйста установите zeep или suds!"
            )
    except HTTPError as e:
        print("Сайт не отвечает при запросе WSDL")
        raise e
    except Exception as e:
        print(e)
        raise e


def update_or_create_version(item: VersionInfo, update_all: bool) -> None:
    ver, created = Version.objects.get_or_create(
        ver=item.version_id,
        dumpdate=datetime.datetime.strptime(item.text_version[-10:], "%d.%m.%Y").date(),
    )
    if created or update_all:
        ver.complete_xml_url = item.complete_xml_url
        ver.delta_xml_url = item.delta_xml_url
        ver.save()


def fetch_version_info(update_all: bool = False) -> None:
    logger.info("Version info updating.")
    pre_fetch_version.send(object.__class__)
    try:
        for item in wsdl_client_factory().version_info():
            update_or_create_version(item, update_all)
    except XMLSyntaxError:
        for item in json_client_factory().version_info():
            update_or_create_version(item, update_all)
    post_fetch_version.send(object.__class__)
    logger.info("Version info updated.")
