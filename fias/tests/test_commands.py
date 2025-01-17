import shutil
from concurrent.futures import ThreadPoolExecutor
from datetime import date
from http.client import HTTPMessage
from multiprocessing.context import BaseContext
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import TYPE_CHECKING, Any, Callable, Dict, List, Tuple, Union
from unittest import mock
from uuid import UUID

from django.core.management import call_command
from django.test import TestCase, TransactionTestCase

from fias.config import TableName
from fias.importer.commands import validate_house_params
from fias.models import (
    AddHouseType,
    AddrObj,
    AddrObjParam,
    AddrObjType,
    AdmHierarchy,
    House,
    HouseParam,
    HouseType,
    MunHierarchy,
    ParamType,
    Status,
    Version,
)

BASE_DIR = Path(__file__).resolve().parent
TEMPDIR = BASE_DIR


if TYPE_CHECKING:
    BaseTestMixin = TestCase
else:
    BaseTestMixin = object


class MockProcessPoolExecutor(ThreadPoolExecutor):
    def __init__(
        self,
        max_workers: Union[int, None] = None,
        mp_context: Union[BaseContext, None] = None,
        initializer: Union[Callable[[None], None], None] = None,
        initargs: Tuple[Any, ...] = (),
        *,
        max_tasks_per_child: Union[int, None] = None
    ):
        super().__init__(max_workers=max_workers, thread_name_prefix="", initializer=initializer, initargs=initargs)


class ReportTestMixin(BaseTestMixin):
    report_path: Path = BASE_DIR / Path("delete_me.csv")
    reference_report_path: Path

    def validate_report(self) -> None:
        with self.report_path.open(encoding="utf-8") as s_file:
            s_csv = s_file.read()
        with self.reference_report_path.open(encoding="utf-8") as t_file:
            t_csv = t_file.read()
        self.assertEqual(t_csv, s_csv)

    def tearDown(self) -> None:
        self.report_path.unlink(True)
        super().tearDown()


class CommandCreateTestCase(ReportTestMixin, TransactionTestCase):
    databases = {"default", "gar"}
    reference_report_path: Path = BASE_DIR / Path("data/test_fias_create.csv")

    def test_fias_create(self) -> None:
        Version.objects.create(ver=20221125, dumpdate=date(2022, 11, 25), complete_xml_url="complete_xml_url")

        src = BASE_DIR / Path("data/fake/gar_99.rar")
        args: List[Any] = []
        opts: Dict[str, Any] = {
            "src": str(src),
            "tempdir": str(TEMPDIR),
            "update_version_info": False,
            "keep_indexes": "no",
            "house_param_regions": ["99"],
            "house_param_report": self.report_path,
        }

        # Can not run ProcessPoolExecutor inside tests=(
        with mock.patch("fias.importer.commands.ProcessPoolExecutor", MockProcessPoolExecutor):
            call_command("fias", *args, **opts)

        self.assertEqual(14, HouseType.objects.count())
        ht = HouseType.objects.get(id=7)
        self.assertEqual("Строение", ht.name)
        self.assertEqual("стр.", ht.shortname)
        self.assertEqual("Строение", ht.desc)
        self.assertEqual(date(1900, 1, 1), ht.updatedate)
        self.assertEqual(date(1900, 1, 1), ht.startdate)
        self.assertEqual(date(2079, 6, 6), ht.enddate)
        self.assertTrue(ht.isactive)
        self.assertEqual(20221125, ht.ver)

        self.assertEqual(4, AddHouseType.objects.count())
        aht = AddHouseType.objects.get(id=1)
        self.assertEqual("Корпус", aht.name)
        self.assertEqual("к.", aht.shortname)
        self.assertEqual("Корпус", aht.desc)
        self.assertEqual(date(2015, 12, 25), aht.updatedate)
        self.assertEqual(date(2015, 12, 25), aht.startdate)
        self.assertEqual(date(2079, 6, 6), aht.enddate)
        self.assertTrue(aht.isactive)
        self.assertEqual(20221125, aht.ver)

        self.assertEqual(1, House.objects.count())
        h = House.objects.get(objectid=19273112)
        self.assertEqual("99", h.region)
        self.assertTrue(h.isactive)
        self.assertTrue(h.isactual)
        self.assertEqual(UUID("f818e827-a3e1-486b-8fa1-47adda987e9c"), h.objectguid)
        self.assertEqual("30", h.housenum)
        self.assertEqual("1", h.addnum1)
        self.assertIsNone(h.addnum2)
        self.assertEqual(2, h.housetype)
        self.assertEqual(1, h.addtype1)
        self.assertIsNone(h.addtype2)
        self.assertEqual(date(2019, 7, 16), h.updatedate)
        self.assertEqual(date(2012, 7, 23), h.startdate)
        self.assertEqual(date(2079, 6, 6), h.enddate)
        self.assertEqual(20221125, h.ver)
        self.assertEqual(20221125, h.tree_ver)

        self.assertEqual(3, HouseParam.objects.count())
        hp = HouseParam.objects.get(id=119564345)
        self.assertEqual("99", hp.region)
        self.assertEqual(19273112, hp.objectid)
        self.assertEqual(7, hp.typeid)
        self.assertEqual("55000000", hp.value)
        self.assertEqual(date(2019, 7, 16), hp.updatedate)
        self.assertEqual(date(2012, 7, 23), hp.startdate)
        self.assertEqual(date(2079, 6, 6), hp.enddate)
        self.assertEqual(20221125, hp.ver)

        self.assertEqual(419, AddrObjType.objects.count())
        aot = AddrObjType.objects.get(id=423)
        self.assertEqual("Город", aot.name)
        self.assertEqual("г", aot.shortname)
        self.assertEqual("Город", aot.desc)
        self.assertEqual(date(2022, 10, 14), aot.updatedate)
        self.assertEqual(date(2015, 11, 5), aot.startdate)
        self.assertEqual(date(2022, 9, 30), aot.enddate)
        self.assertEqual(2, aot.level)
        self.assertTrue(aot.isactive)
        self.assertEqual(20221125, aot.ver)

        self.assertEqual(4, AddrObj.objects.count())
        self.assertListEqual(
            [1456531, 1456532, 1456865, 1460768],
            list(AddrObj.objects.order_by("objectid").values_list("objectid", flat=True)),
        )

        ao = AddrObj.objects.get(objectid=1456865)
        self.assertEqual("99", ao.region)
        self.assertTrue(ao.isactive)
        self.assertTrue(ao.isactual)
        self.assertEqual(UUID("b35f8e9f-35a5-4f5d-a216-f363f41aa585"), ao.objectguid)
        self.assertEqual("5-й", ao.name)
        self.assertEqual("мкр", ao.typename)
        self.assertEqual(7, ao.level)
        self.assertEqual(date(2022, 8, 12), ao.updatedate)
        self.assertEqual(date(2022, 8, 12), ao.startdate)
        self.assertEqual(date(2079, 6, 6), ao.enddate)
        self.assertEqual(20221125, ao.ver)
        self.assertEqual(20221125, ao.tree_ver)

        ao1 = AddrObj.objects.get(objectid=1456532)
        self.assertEqual("Школьная", ao1.name)
        self.assertEqual(date(2019, 7, 16), ao1.updatedate)
        self.assertEqual(date(1900, 1, 1), ao1.startdate)
        self.assertEqual(date(2079, 6, 6), ao1.enddate)
        self.assertEqual(20221125, ao1.ver)
        self.assertEqual(20221125, ao1.tree_ver)

        self.assertEqual(8, AddrObjParam.objects.count())
        aop = AddrObjParam.objects.get(id=22510497)
        self.assertEqual("99", aop.region)
        self.assertEqual(1456865, aop.objectid)
        self.assertEqual(6, aop.typeid)
        self.assertEqual("55000000000", aop.value)
        self.assertEqual(date(2019, 7, 16), aop.updatedate)
        self.assertEqual(date(1900, 1, 1), aop.startdate)
        self.assertEqual(date(2079, 6, 6), aop.enddate)
        self.assertEqual(20221125, aop.ver)

        self.assertEqual(5, AdmHierarchy.objects.count())
        self.assertTrue(AdmHierarchy.objects.filter(objectid=1456865).exists())

        ah = AdmHierarchy.objects.get(id=123607639)
        self.assertEqual("99", ah.region)
        self.assertEqual(1456865, ah.objectid)
        self.assertEqual(1460768, ah.parentobjid)
        self.assertTrue(ah.isactive)
        self.assertEqual(date(2022, 8, 12), ah.updatedate)
        self.assertEqual(date(2022, 8, 12), ah.startdate)
        self.assertEqual(date(2079, 6, 6), ah.enddate)
        self.assertEqual(20221125, ah.ver)

        self.assertEqual(4, MunHierarchy.objects.count())
        self.assertFalse(MunHierarchy.objects.filter(objectid=1456865).exists())

        mh = MunHierarchy.objects.get(id=107886321)
        self.assertEqual("99", mh.region)
        self.assertEqual(1456531, mh.objectid)
        self.assertEqual(1460768, mh.parentobjid)
        self.assertTrue(mh.isactive)
        self.assertEqual(date(1900, 1, 1), mh.updatedate)
        self.assertEqual(date(1900, 1, 1), mh.startdate)
        self.assertEqual(date(2079, 6, 6), mh.enddate)
        self.assertEqual(20221125, mh.ver)

        self.assertEqual(1, Version.objects.count())
        ver = Version.objects.get()

        self.assertEqual(10, Status.objects.count())

        ht_s = Status.objects.get(table=TableName.HOUSE_TYPE)
        self.assertIsNone(ht_s.region)
        self.assertEqual(ver, ht_s.ver)

        h_s = Status.objects.get(table=TableName.HOUSE)
        self.assertEqual("99", h_s.region)
        self.assertEqual(ver, h_s.ver)

        self.validate_report()


class CommandUpdateTestCase(ReportTestMixin, TransactionTestCase):
    databases = {"default", "gar"}
    fixtures = ["fias/tests/data/fixtures/gar_99.json"]
    reference_report_path: Path = BASE_DIR / Path("data/test_fias_update.csv")

    def test_fias_remote_update(self) -> None:
        self.assertTrue(AdmHierarchy.objects.filter(objectid=1456865).exists())
        self.assertFalse(MunHierarchy.objects.filter(objectid=1456865).exists())
        self.assertEqual("55000000000", AddrObjParam.objects.get(objectid=1460768, typeid=6).value)

        src_map = {
            "https://fias-file.nalog.ru/downloads/2022.11.29/gar_delta_xml.zip": BASE_DIR
            / Path("data/fake/deltas/gar_delta_99_20221128.rar"),
            "https://fias-file.nalog.ru/downloads/2022.12.02/gar_delta_xml.zip": BASE_DIR
            / Path("data/fake/deltas/gar_delta_99_20221202.rar"),
        }

        def _download(
            url: str, file_name: Path, reporthook: Callable[[int, int, int], None]
        ) -> Tuple[Path, HTTPMessage]:
            src_file = src_map[url]
            shutil.copy2(src_file, file_name)
            return file_name, HTTPMessage()

        with mock.patch("fias.importer.source.archive.Downloader.download", side_effect=_download) as mock_download:
            # Can not run ProcessPoolExecutor inside tests=(
            with mock.patch("fias.importer.commands.ProcessPoolExecutor", MockProcessPoolExecutor):
                with TemporaryDirectory() as temp_dir:
                    args: List[Any] = []
                    opts: Dict[str, Any] = {
                        "tempdir": str(temp_dir),
                        "update": True,
                        "update_version_info": False,
                        "house_param_regions": ["99"],
                        "house_param_report": self.report_path,
                    }
                    call_command("fias", *args, **opts)

                    self.assertEqual(2, mock_download.call_count)

        self.validate()

    def test_fias_local_update(self) -> None:
        self.assertTrue(AdmHierarchy.objects.filter(objectid=1456865).exists())
        self.assertFalse(MunHierarchy.objects.filter(objectid=1456865).exists())
        self.assertEqual("55000000000", AddrObjParam.objects.get(objectid=1460768, typeid=6).value)

        src = BASE_DIR / Path("data/fake/deltas")
        args: List[Any] = []
        opts: Dict[str, Any] = {
            "src": str(src),
            "tempdir": str(TEMPDIR),
            "update": True,
            "update_version_info": False,
            "house_param_regions": ["99"],
            "house_param_report": self.report_path,
        }

        # Can not run ProcessPoolExecutor inside tests=(
        with mock.patch("fias.importer.commands.ProcessPoolExecutor", MockProcessPoolExecutor):
            call_command("fias", *args, **opts)

        self.validate()

    def validate(self) -> None:
        self.assertEqual(14, HouseType.objects.count())
        ht = HouseType.objects.get(id=7)
        self.assertEqual("Строение", ht.name)
        self.assertEqual("стр.", ht.shortname)
        self.assertEqual("Строение", ht.desc)
        self.assertEqual(date(1900, 1, 1), ht.updatedate)
        self.assertEqual(date(1900, 1, 1), ht.startdate)
        self.assertEqual(date(2079, 6, 6), ht.enddate)
        self.assertTrue(ht.isactive)
        self.assertEqual(20221202, ht.ver)

        self.assertEqual(4, AddHouseType.objects.count())
        aht = AddHouseType.objects.get(id=1)
        self.assertEqual("Корпус", aht.name)
        self.assertEqual("к.", aht.shortname)
        self.assertEqual("Корпус", aht.desc)
        self.assertEqual(date(2015, 12, 25), aht.updatedate)
        self.assertEqual(date(2015, 12, 25), aht.startdate)
        self.assertEqual(date(2079, 6, 6), aht.enddate)
        self.assertTrue(aht.isactive)
        self.assertEqual(20221202, aht.ver)

        self.assertEqual(2, House.objects.count())
        h = House.objects.get(objectid=157269039)
        self.assertEqual("99", h.region)
        self.assertTrue(h.isactive)
        self.assertTrue(h.isactual)
        self.assertEqual(UUID("d3beec37-2c9e-4e7e-987d-a8eb827d7084"), h.objectguid)
        self.assertEqual("70а", h.housenum)
        self.assertIsNone(h.addnum1)
        self.assertIsNone(h.addnum2)
        self.assertEqual(5, h.housetype)
        self.assertIsNone(h.addtype1)
        self.assertIsNone(h.addtype2)
        self.assertEqual(date(2022, 11, 28), h.updatedate)
        self.assertEqual(date(2022, 11, 28), h.startdate)
        self.assertEqual(date(2079, 6, 6), h.enddate)
        self.assertEqual(20221129, h.ver)
        self.assertEqual(20221202, h.tree_ver)

        self.assertEqual(5, HouseParam.objects.count())
        hp = HouseParam.objects.get(id=1346933308)
        self.assertEqual("99", hp.region)
        self.assertEqual(157269039, hp.objectid)
        self.assertEqual(6, hp.typeid)
        self.assertEqual("55000000000", hp.value)
        self.assertEqual(date(2022, 11, 28), hp.updatedate)
        self.assertEqual(date(2022, 11, 28), hp.startdate)
        self.assertEqual(date(2079, 6, 6), hp.enddate)
        self.assertEqual(20221129, hp.ver)

        self.assertEqual(419, AddrObjType.objects.count())
        aot = AddrObjType.objects.get(id=423)
        self.assertEqual("Город", aot.name)
        self.assertEqual("г", aot.shortname)
        self.assertEqual("Город", aot.desc)
        self.assertEqual(date(2022, 10, 14), aot.updatedate)
        self.assertEqual(date(2015, 11, 5), aot.startdate)
        self.assertEqual(date(2022, 9, 30), aot.enddate)
        self.assertEqual(2, aot.level)
        self.assertTrue(aot.isactive)
        self.assertEqual(20221202, aot.ver)

        self.assertEqual(4, AddrObj.objects.count())
        self.assertListEqual(
            [1456532, 1456865, 1460768, 157289164],
            list(AddrObj.objects.order_by("objectid").values_list("objectid", flat=True)),
        )

        ao = AddrObj.objects.get(objectid=157289164)
        self.assertEqual("99", ao.region)
        self.assertTrue(ao.isactive)
        self.assertTrue(ao.isactual)
        self.assertEqual(UUID("48ae0ae8-8235-4b9c-a535-4e271fce450b"), ao.objectguid)
        self.assertEqual("№22 сад Юбилейный", ao.name)
        self.assertEqual("тер.", ao.typename)
        self.assertEqual(7, ao.level)
        self.assertEqual(date(2022, 11, 28), ao.updatedate)
        self.assertEqual(date(2022, 11, 28), ao.startdate)
        self.assertEqual(date(2079, 6, 6), ao.enddate)
        self.assertEqual(20221129, ao.ver)
        self.assertEqual(20221129, ao.tree_ver)

        ao1 = AddrObj.objects.get(objectid=1456532)
        self.assertEqual("Школьная-2", ao1.name)
        self.assertEqual(date(2022, 11, 28), ao1.updatedate)
        self.assertEqual(date(2022, 11, 28), ao1.startdate)
        self.assertEqual(date(2079, 6, 6), ao1.enddate)
        self.assertEqual(20221129, ao1.ver)
        self.assertEqual(20221129, ao1.tree_ver)

        ao2 = AddrObj.objects.get(objectid=1456865)
        self.assertEqual(20221125, ao2.ver)
        self.assertEqual(20221129, ao2.tree_ver)

        ao3 = AddrObj.objects.get(objectid=1460768)
        self.assertEqual(20221125, ao3.ver)
        self.assertEqual(20221129, ao3.tree_ver)

        self.assertEqual(20221129, AddrObj.objects.get(objectid=1460768).tree_ver)

        # TODO: как исключать из загрузки параметры удалённых объектов?
        self.assertEqual(8, AddrObjParam.objects.count())
        aop = AddrObjParam.objects.get(id=1362268938)
        self.assertEqual("99", aop.region)
        self.assertEqual(157289164, aop.objectid)
        self.assertEqual(7, aop.typeid)
        self.assertEqual("46737000611", aop.value)
        self.assertEqual(date(2022, 11, 28), aop.updatedate)
        self.assertEqual(date(2022, 11, 28), aop.startdate)
        self.assertEqual(date(2079, 6, 6), aop.enddate)
        self.assertEqual(20221129, aop.ver)
        self.assertEqual("55000000001", AddrObjParam.objects.get(objectid=1460768, typeid=6).value)

        self.assertEqual(5, AdmHierarchy.objects.count())
        self.assertFalse(AdmHierarchy.objects.filter(objectid=1456865).exists())

        ah = AdmHierarchy.objects.get(id=184786086)
        self.assertEqual("99", ah.region)
        self.assertEqual(157289164, ah.objectid)
        self.assertEqual(1460768, ah.parentobjid)
        self.assertTrue(ah.isactive)
        self.assertEqual(date(2022, 11, 28), ah.updatedate)
        self.assertEqual(date(2022, 11, 28), ah.startdate)
        self.assertEqual(date(2079, 6, 6), ah.enddate)
        self.assertEqual(20221129, ah.ver)

        ah1 = AdmHierarchy.objects.get(id=84785687)
        self.assertEqual(1460768, ah1.objectid)
        self.assertEqual(None, ah1.parentobjid)

        self.assertEqual(6, MunHierarchy.objects.count())
        self.assertTrue(MunHierarchy.objects.filter(objectid=1456865).exists())

        mh = MunHierarchy.objects.get(id=184786086)
        self.assertEqual("99", mh.region)
        self.assertEqual(157289164, ah.objectid)
        self.assertEqual(1460768, ah.parentobjid)
        self.assertTrue(mh.isactive)
        self.assertEqual(date(2022, 11, 28), ah.updatedate)
        self.assertEqual(date(2022, 11, 28), ah.startdate)
        self.assertEqual(date(2079, 6, 6), mh.enddate)
        self.assertEqual(20221129, mh.ver)

        self.assertEqual(4, Version.objects.count())
        # TODO: всё ещё сомнения по поводу поиска ближайшей версии
        ver = Version.objects.get(ver=20221202)

        self.assertEqual(10, Status.objects.count())

        ht_s = Status.objects.get(table=TableName.HOUSE_TYPE)
        self.assertIsNone(ht_s.region)
        self.assertEqual(ver, ht_s.ver)

        h_s = Status.objects.get(table=TableName.HOUSE)
        self.assertEqual("99", h_s.region)
        self.assertEqual(ver, h_s.ver)

        self.validate_report()


class CommandValidateHouseParamsTestCase(ReportTestMixin, TestCase):
    databases = {"default", "gar"}
    params: List[HouseParam]
    param_types: List[ParamType]
    reference_report_path: Path = BASE_DIR / Path("data/test_validate_house_params.csv")

    def setUp(self) -> None:
        super().setUp()
        self.param_types = [
            ParamType(
                id=6,
                name="ОКАТО",
                desc="ОКАТО",
                isactive=True,
                ver=20220112,
                updatedate=date(2022, 1, 12),
                startdate=date(2022, 1, 12),
                enddate=date(2079, 6, 6),
            ),
            ParamType(
                id=7,
                name="ОКТМО",
                desc="ОКТМО",
                isactive=True,
                ver=20220112,
                updatedate=date(2022, 1, 12),
                startdate=date(2022, 1, 12),
                enddate=date(2079, 6, 6),
            ),
        ]
        for pt in self.param_types:
            pt.full_clean()
            pt.save()
        # 0-4 symbols are important
        self.params = [
            HouseParam(
                objectid=1,
                ver=20220112,
                updatedate=date(2022, 1, 12),
                startdate=date(2022, 1, 12),
                enddate=date(2079, 6, 6),
                region=78,
                typeid=6,
                value="40290000000",
            ),
            HouseParam(
                objectid=2,
                ver=20220112,
                updatedate=date(2022, 1, 12),
                startdate=date(2022, 1, 12),
                enddate=date(2079, 6, 6),
                region=78,
                typeid=6,
                value="40090000000",
            ),
            HouseParam(
                objectid=3,
                ver=20220112,
                updatedate=date(2022, 1, 12),
                startdate=date(2022, 1, 12),
                enddate=date(2079, 6, 6),
                region=78,
                typeid=6,
                value="40000000000",
            ),
            HouseParam(
                objectid=4,
                ver=20220112,
                updatedate=date(2022, 1, 12),
                startdate=date(2022, 1, 12),
                enddate=date(2079, 6, 6),
                region=78,
                typeid=7,
                value="40395000",
            ),
            HouseParam(
                objectid=5,
                ver=20220112,
                updatedate=date(2022, 1, 12),
                startdate=date(2022, 1, 12),
                enddate=date(2079, 6, 6),
                region=78,
                typeid=7,
                value="40095000",
            ),
            HouseParam(
                objectid=6,
                ver=20220112,
                updatedate=date(2022, 1, 12),
                startdate=date(2022, 1, 12),
                enddate=date(2079, 6, 6),
                region=78,
                typeid=7,
                value="40000000",
            ),
            HouseParam(
                objectid=7,
                ver=20220110,
                updatedate=date(2022, 1, 10),
                startdate=date(2022, 1, 10),
                enddate=date(2079, 6, 6),
                region=78,
                typeid=7,
                value="40000000",
            ),
            HouseParam(
                objectid=8,
                ver=20220112,
                updatedate=date(2022, 1, 12),
                startdate=date(2022, 1, 12),
                enddate=date(2079, 6, 6),
                region=50,
                typeid=6,
                value="46437000000",
            ),
            HouseParam(
                objectid=9,
                ver=20220112,
                updatedate=date(2022, 1, 12),
                startdate=date(2022, 1, 12),
                enddate=date(2079, 6, 6),
                region=50,
                typeid=6,
                value="46037000000",
            ),
        ]
        for p in self.params:
            p.full_clean()
            p.save()

    def tearDown(self) -> None:
        for pt in self.param_types:
            pt.delete()
        for p in self.params:
            p.delete()
        super().tearDown()

    def test_cmd_validate_house_params(self) -> None:
        args: List[Any] = []
        opts: Dict[str, Any] = {"output": self.report_path, "regions": ["78"], "min_ver": 20220112}
        call_command("validate_house_params", *args, **opts)

        self.validate_report()

    def test_validate_house_params(self) -> None:
        validate_house_params(self.report_path, 20220112, ["78"])

        self.validate_report()
