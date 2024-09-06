# coding: utf-8
from __future__ import absolute_import, unicode_literals

import datetime
import json
import logging
import urllib.request
from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Protocol

from fias.importer.signals import post_fetch_version, pre_fetch_version
from fias.models import Version

logger = logging.getLogger(__name__)

JSON_SOURCE = "https://fias.nalog.ru/WebServices/Public/GetAllDownloadFileInfo"


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
        raise NotImplementedError


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
        raise NotImplementedError


class ServiceProtocol(Protocol):
    def GetAllDownloadFileInfo(self) -> ObjResponse:
        raise NotImplementedError


class WsdlClientProtocol(Protocol):
    @property
    def service(self) -> ServiceProtocol:
        raise NotImplementedError


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
    for item in json_client_factory().version_info():
        update_or_create_version(item, update_all)
    post_fetch_version.send(object.__class__)
    logger.info("Version info updated.")
