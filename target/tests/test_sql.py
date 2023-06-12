import re

from django.db import connections
from django.test import TestCase

from fias import models as s_models
from target import models as t_models
from target.config import DATABASE_ALIAS
from target.importer.sql import HierarchyCfg, ParamCfg, SqlBuilder


class SqlBuilderTestCase(TestCase):
    @staticmethod
    def strip(s: str) -> str:
        s = s.strip()
        s = re.sub(r"[\r\n ]+", " ", s)
        s = re.sub(r"\( ", "(", s)
        s = re.sub(r" \)", ")", s)
        return s

    def test_filter_value(self) -> None:
        self.assertEqual("gar_house.name IS NULL", SqlBuilder.filter_value(t_models.House, "name", "IS", None))
        self.assertEqual("gar_house.name = 'home'", SqlBuilder.filter_value(t_models.House, "name", "=", "home"))
        self.assertEqual("gar_house.objectid = 656", SqlBuilder.filter_value(t_models.House, "objectid", "=", 656))

    def test_filter_query(self) -> None:
        self.assertEqual(
            "gar_house.name IN (SELECT 1)", SqlBuilder.filter_query(t_models.House, "name", True, "SELECT 1")
        )
        self.assertEqual(
            "gar_house.name NOT IN (SELECT 1)", SqlBuilder.filter_query(t_models.House, "name", False, "SELECT 1")
        )

    def test_create(self) -> None:
        target = """
            INSERT INTO gar_addrobj (region, owner_adm, aolevel, objectid, objectguid, name, typename, okato,
                oktmo)
            SELECT fias_addrobj.region, COALESCE(owner_adm, 0) AS owner_adm,
                fias_addrobj.level AS aolevel, fias_addrobj.objectid, fias_addrobj.objectguid, fias_addrobj.name,
                fias_addrobj.typename, okato, oktmo
            FROM fias_addrobj
            LEFT JOIN crosstab(
                'SELECT objectid, typeid, value FROM fias_addrobjparam ORDER BY objectid, typeid',
                'SELECT typeids FROM (values (6), (7)) t(typeids)'
                ) AS ct(objectid BIGINT, okato VARCHAR(11), oktmo VARCHAR(11))
                ON fias_addrobj.objectid = ct.objectid
            LEFT JOIN (
                SELECT objectid, parentobjid AS owner_adm
                FROM fias_admhierarchy
                WHERE isactive = true
                ) AS h0 ON h0.objectid = fias_addrobj.objectid"""

        connection = connections[DATABASE_ALIAS]
        result = SqlBuilder.create(
            connection,
            t_models.AddrObj,
            "objectid",
            s_models.AddrObj,
            "objectid",
            None,
            {"aolevel": "level"},
            ParamCfg(s_models.AddrObjParam, "objectid", [("okato", 6), ("oktmo", 7)]),
            [
                HierarchyCfg(s_models.AdmHierarchy, "objectid", "parentobjid", "owner_adm"),
            ],
        )
        self.assertEqual(self.strip(target), self.strip(result))

    def test_update(self) -> None:
        target = """
            UPDATE gar_addrobj
            SET region = tmp_select_table.region,
                owner_adm = tmp_select_table.owner_adm,
                aolevel = tmp_select_table.aolevel,
                objectguid = tmp_select_table.objectguid,
                name = tmp_select_table.name,
                typename = tmp_select_table.typename,
                okato = tmp_select_table.okato,
                oktmo = tmp_select_table.oktmo
            FROM (
                SELECT fias_addrobj.region, COALESCE(owner_adm, 0) AS owner_adm,
                    fias_addrobj.level AS aolevel, fias_addrobj.objectid,
                    fias_addrobj.objectguid, fias_addrobj.name, fias_addrobj.typename, okato, oktmo
                FROM fias_addrobj
                LEFT JOIN crosstab(
                    'SELECT objectid, typeid, value FROM fias_addrobjparam ORDER BY objectid, typeid',
                    'SELECT typeids FROM (values (6), (7)) t(typeids)'
                    ) AS ct(objectid BIGINT, okato VARCHAR(11), oktmo VARCHAR(11))
                    ON fias_addrobj.objectid = ct.objectid
                LEFT JOIN (
                    SELECT objectid, parentobjid AS owner_adm
                    FROM fias_admhierarchy
                    WHERE isactive = true
                    ) AS h0 ON h0.objectid = fias_addrobj.objectid
                WHERE fias_addrobj.tree_ver > 2222 AND fias_addrobj.ver > 1111
                ) AS tmp_select_table
            WHERE gar_addrobj.objectid = tmp_select_table.objectid
            """

        connection = connections[DATABASE_ALIAS]
        result = SqlBuilder.update(
            connection,
            t_models.AddrObj,
            "objectid",
            s_models.AddrObj,
            "objectid",
            [
                SqlBuilder.filter_value(s_models.AddrObj, "tree_ver", ">", 2222),
                SqlBuilder.filter_value(s_models.AddrObj, "ver", ">", 1111),
            ],
            {"aolevel": "level"},
            ParamCfg(s_models.AddrObjParam, "objectid", [("okato", 6), ("oktmo", 7)]),
            [
                HierarchyCfg(s_models.AdmHierarchy, "objectid", "parentobjid", "owner_adm"),
            ],
        )
        self.assertEqual(self.strip(target), self.strip(result))

    def test_select(self) -> None:
        target = """
            SELECT fias_addrobj.region, COALESCE(owner_adm, 0) AS owner_adm,
                fias_addrobj.level AS aolevel, fias_addrobj.objectid, fias_addrobj.objectguid, fias_addrobj.name,
                fias_addrobj.typename, okato, oktmo
            FROM fias_addrobj
            LEFT JOIN crosstab(
                'SELECT objectid, typeid, value FROM fias_addrobjparam ORDER BY objectid, typeid',
                'SELECT typeids FROM (values (6), (7)) t(typeids)'
                ) AS ct(objectid BIGINT, okato VARCHAR(11), oktmo VARCHAR(11))
                ON fias_addrobj.objectid = ct.objectid
            LEFT JOIN (
                SELECT objectid, parentobjid AS owner_adm
                FROM fias_admhierarchy
                WHERE isactive = true
                ) AS h0 ON h0.objectid = fias_addrobj.objectid
            """

        connection = connections[DATABASE_ALIAS]
        result = SqlBuilder.select(
            connection,
            t_models.AddrObj,
            s_models.AddrObj,
            "objectid",
            [
                "region",
                "owner_adm",
                "aolevel",
                "objectid",
                "objectguid",
                "name",
                "typename",
                "okato",
                "oktmo",
            ],
            None,
            {"aolevel": "level"},
            ParamCfg(s_models.AddrObjParam, "objectid", [("okato", 6), ("oktmo", 7)]),
            [
                HierarchyCfg(s_models.AdmHierarchy, "objectid", "parentobjid", "owner_adm"),
            ],
        )
        self.assertEqual(self.strip(target), self.strip(result))

    def test_delete_on_field(self) -> None:
        target = """
                DELETE
                FROM gar_house
                WHERE id IN (
                    SELECT gar_house.id
                    FROM gar_house
                    LEFT JOIN gar_house78 ON gar_house.owner_adm = gar_house78.owner_mun
                    WHERE gar_house78.owner_mun IS NULL
                )
            """
        result = SqlBuilder.delete_on_field(t_models.House, "owner_adm", t_models.House78, "owner_mun")
        self.assertEqual(self.strip(target), self.strip(result))
