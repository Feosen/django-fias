from typing import Any, Dict, List
from uuid import UUID

from django.core.management import call_command
from django.test import TestCase

from target.models import AddrObj, House, House78, HouseAddType, HouseType, Status


class CommandCreateTestCase(TestCase):
    databases = {"default", "gar"}
    fixtures = ["target/tests/data/fixtures/gar_99.json"]

    def test_target_create(self) -> None:
        self.assertEqual(0, Status.objects.count())

        args: List[Any] = []
        opts: Dict[str, Any] = {
            "keep_indexes": "no",
        }
        call_command("target", *args, **opts)

        self.assertEqual(7, HouseType.objects.count())
        ht = HouseType.objects.get(id=7)
        self.assertEqual("Строение", ht.name)
        self.assertEqual("стр.", ht.shortname)

        self.assertEqual(3, HouseAddType.objects.count())
        aht = HouseAddType.objects.get(id=1)
        self.assertEqual("Корпус", aht.name)
        self.assertEqual("к.", aht.shortname)

        self.assertEqual(0, House78.objects.count())

        self.assertEqual(1, House.objects.count())
        h = House.objects.get()
        self.assertEqual("99", h.region)
        self.assertEqual(1456532, h.owner_adm)
        self.assertEqual(19273112, h.objectid)
        self.assertEqual(UUID("f818e827-a3e1-486b-8fa1-47adda987e9c"), h.objectguid)
        self.assertEqual("30", h.housenum)
        self.assertEqual("1", h.addnum1)
        self.assertEqual(None, h.addnum2)
        self.assertEqual(2, h.housetype)
        self.assertEqual(1, h.addtype1)
        self.assertEqual(None, h.addtype2)
        self.assertEqual("468321", h.postalcode)
        self.assertEqual("55000000000", h.okato)
        self.assertEqual("55000000", h.oktmo)

        self.assertEqual(4, AddrObj.objects.count())
        self.assertListEqual(
            [1456531, 1456532, 1456865, 1460768],
            list(AddrObj.objects.order_by("objectid").values_list("objectid", flat=True)),
        )

        ao = AddrObj.objects.get(objectid=1456865)
        self.assertEqual("99", ao.region)
        self.assertEqual(1460768, ao.owner_adm)
        self.assertEqual(7, ao.aolevel)
        self.assertEqual(UUID("b35f8e9f-35a5-4f5d-a216-f363f41aa585"), ao.objectguid)
        self.assertEqual("5-й", ao.name)
        self.assertEqual("мкр", ao.typename)
        self.assertEqual("55000000000", ao.okato)
        self.assertEqual("55000000", ao.oktmo)

        ao1 = AddrObj.objects.get(objectid=1456532)
        self.assertEqual("99", ao1.region)
        self.assertEqual(1460768, ao1.owner_adm)
        self.assertEqual(8, ao1.aolevel)
        self.assertEqual(UUID("3d855628-80ed-4ca5-a9de-378a76876acf"), ao1.objectguid)
        self.assertEqual("Школьная", ao1.name)
        self.assertEqual("ул", ao1.typename)
        self.assertEqual("55000000000", ao1.okato)
        self.assertEqual("55000000", ao1.oktmo)

        self.assertEqual(20221125, Status.objects.get().ver)


class CommandUpdateTestCase(TestCase):
    databases = {"default", "gar"}
    fixtures = ["target/tests/data/fixtures/gar_99_u.json"]

    def test_target_create(self) -> None:
        self.assertEqual(20221125, Status.objects.get().ver)
        ao = AddrObj.objects.get(objectid=1456865)
        self.assertEqual(1460768, ao.owner_adm)
        self.assertEqual("55000000000", AddrObj.objects.get(objectid=1460768).okato)

        args: List[Any] = []
        opts: Dict[str, Any] = {"update": True}
        call_command("target", *args, **opts)

        self.assertEqual(7, HouseType.objects.count())
        ht = HouseType.objects.get(id=7)
        self.assertEqual("Строение", ht.name)
        self.assertEqual("стр.", ht.shortname)

        self.assertEqual(3, HouseAddType.objects.count())
        aht = HouseAddType.objects.get(id=1)
        self.assertEqual("Корпус", aht.name)
        self.assertEqual("к.", aht.shortname)

        self.assertEqual(0, House78.objects.count())

        self.assertEqual(2, House.objects.count())
        h = House.objects.get(objectid=157269039)
        self.assertEqual("99", h.region)
        self.assertEqual(1456532, h.owner_adm)
        self.assertEqual(UUID("d3beec37-2c9e-4e7e-987d-a8eb827d7084"), h.objectguid)
        self.assertEqual("70а", h.housenum)
        self.assertEqual(None, h.addnum1)
        self.assertEqual(None, h.addnum2)
        self.assertEqual(5, h.housetype)
        self.assertEqual(None, h.addtype1)
        self.assertEqual(None, h.addtype2)
        self.assertEqual(None, h.postalcode)
        self.assertEqual("55000000000", h.okato)
        self.assertEqual("55000000", h.oktmo)

        self.assertEqual(4, AddrObj.objects.count())
        self.assertListEqual(
            [1456532, 1456865, 1460768, 157289164],
            list(AddrObj.objects.order_by("objectid").values_list("objectid", flat=True)),
        )

        ao = AddrObj.objects.get(objectid=1456865)
        self.assertEqual("99", ao.region)
        self.assertEqual(0, ao.owner_adm)
        self.assertEqual(7, ao.aolevel)
        self.assertEqual(UUID("b35f8e9f-35a5-4f5d-a216-f363f41aa585"), ao.objectguid)
        self.assertEqual("5-й", ao.name)
        self.assertEqual("мкр", ao.typename)
        self.assertEqual("55000000000", ao.okato)
        self.assertEqual("55000000", ao.oktmo)

        ao1 = AddrObj.objects.get(objectid=157289164)
        self.assertEqual("99", ao1.region)
        self.assertEqual(UUID("48ae0ae8-8235-4b9c-a535-4e271fce450b"), ao1.objectguid)
        self.assertEqual("№22 сад Юбилейный", ao1.name)
        self.assertEqual("тер.", ao1.typename)
        self.assertEqual(7, ao1.aolevel)

        ao2 = AddrObj.objects.get(objectid=1456532)
        self.assertEqual("99", ao2.region)
        self.assertEqual(1460768, ao2.owner_adm)
        self.assertEqual(8, ao2.aolevel)
        self.assertEqual(UUID("3d855628-80ed-4ca5-a9de-378a76876acf"), ao2.objectguid)
        self.assertEqual("Школьная-2", ao2.name)
        self.assertEqual("ул", ao2.typename)
        self.assertEqual("55000000000", ao2.okato)
        self.assertEqual("55000000", ao2.oktmo)

        self.assertEqual("55000000001", AddrObj.objects.get(objectid=1460768).okato)

        self.assertEqual(20221129, Status.objects.get().ver)
