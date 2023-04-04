from uuid import UUID

from django.core.management import call_command
from django.test import TestCase

from target.models import *


class CommandCreateTestCase(TestCase):
    databases = ['default', 'gar']
    fixtures = ["target/tests/data/fixtures/gar_99.json"]

    def test_target_create(self):
        args = []
        opts = {}
        call_command('target', *args, **opts)

        self.assertEqual(14, HouseType.objects.count())
        ht = HouseType.objects.get(id=7)
        self.assertEqual('Строение', ht.name)
        self.assertEqual('стр.', ht.shortname)

        self.assertEqual(4, HouseAddType.objects.count())
        aht = HouseAddType.objects.get(id=1)
        self.assertEqual('Корпус', aht.name)
        self.assertEqual('к.', aht.shortname)

        self.assertEqual(0, House78.objects.count())

        self.assertEqual(1, House.objects.count())
        h = House.objects.get()
        self.assertEqual('99', h.region)
        self.assertEqual(1456532, h.owner_adm)
        self.assertEqual(1456532, h.owner_mun)
        self.assertEqual(19273112, h.objectid)
        self.assertEqual(UUID('f818e827-a3e1-486b-8fa1-47adda987e9c'), h.objectguid)
        self.assertEqual('30', h.housenum)
        self.assertEqual('1', h.addnum1)
        self.assertEqual(None, h.addnum2)
        self.assertEqual(2, h.housetype)
        self.assertEqual(1, h.addtype1)
        self.assertEqual(None, h.addtype2)
        self.assertEqual('468321', h.postalcode)
        self.assertEqual('55000000000', h.okato)
        self.assertEqual('55000000', h.oktmo)

        self.assertEqual(3, AddrObj.objects.count())
        ao = AddrObj.objects.get(objectid=1456865)
        self.assertEqual('99', ao.region)
        self.assertEqual(1460768, ao.owner_adm)
        self.assertEqual(1460768, ao.owner_mun)
        self.assertEqual(7, ao.aolevel)
        self.assertEqual(UUID('b35f8e9f-35a5-4f5d-a216-f363f41aa585'), ao.objectguid)
        self.assertEqual('5-й', ao.name)
        self.assertEqual('мкр', ao.typename)
        self.assertEqual('55000000000', ao.okato)
        self.assertEqual('55000000', ao.oktmo)


class CommandUpdateTestCase(TestCase):
    databases = ['default', 'gar']
    fixtures = ["target/tests/data/fixtures/gar_99_u.json"]

    def test_target_create(self):
        args = []
        opts = {
            'update': True
        }
        call_command('target', *args, **opts)

        self.assertEqual(14, HouseType.objects.count())
        ht = HouseType.objects.get(id=7)
        self.assertEqual('Строение', ht.name)
        self.assertEqual('стр.', ht.shortname)

        self.assertEqual(4, HouseAddType.objects.count())
        aht = HouseAddType.objects.get(id=1)
        self.assertEqual('Корпус', aht.name)
        self.assertEqual('к.', aht.shortname)

        self.assertEqual(0, House78.objects.count())

        self.assertEqual(1, House.objects.count())
        h = House.objects.get()
        self.assertEqual('99', h.region)
        self.assertEqual(1456532, h.owner_adm)
        self.assertEqual(1456532, h.owner_mun)
        self.assertEqual(19273112, h.objectid)
        self.assertEqual(UUID('f818e827-a3e1-486b-8fa1-47adda987e9c'), h.objectguid)
        self.assertEqual('30', h.housenum)
        self.assertEqual('1', h.addnum1)
        self.assertEqual(None, h.addnum2)
        self.assertEqual(2, h.housetype)
        self.assertEqual(1, h.addtype1)
        self.assertEqual(None, h.addtype2)
        self.assertEqual('468321', h.postalcode)
        self.assertEqual('55000000000', h.okato)
        self.assertEqual('55000000', h.oktmo)

        self.assertEqual(3, AddrObj.objects.count())
        ao = AddrObj.objects.get(objectid=1456865)
        self.assertEqual('99', ao.region)
        self.assertEqual(1460768, ao.owner_adm)
        self.assertEqual(1460768, ao.owner_mun)
        self.assertEqual(7, ao.aolevel)
        self.assertEqual(UUID('b35f8e9f-35a5-4f5d-a216-f363f41aa585'), ao.objectguid)
        self.assertEqual('5-й', ao.name)
        self.assertEqual('мкр', ao.typename)
        self.assertEqual('55000000000', ao.okato)
        self.assertEqual('55000000', ao.oktmo)
