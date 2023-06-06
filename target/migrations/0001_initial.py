# Generated by Django 4.1.4 on 2023-04-04 21:26
from typing import List

from django.db import migrations, models
from django.db.migrations.operations.base import Operation

from target.config import MANAGE

state_operations: List[Operation] = [
    migrations.CreateModel(
        name="AddrObj",
        fields=[
            (
                "id",
                models.AutoField(primary_key=True, serialize=False, verbose_name="id"),
            ),
            ("region", models.CharField(max_length=2, verbose_name="код региона")),
            (
                "owner_adm",
                models.BigIntegerField(verbose_name="административная иерархия"),
            ),
            (
                "owner_mun",
                models.BigIntegerField(verbose_name="муниципальная иерархия"),
            ),
            (
                "aolevel",
                models.IntegerField(verbose_name="уровень адресного объекта"),
            ),
            (
                "objectid",
                models.BigIntegerField(verbose_name="глобальный уникальный идентификатор объекта"),
            ),
            (
                "objectguid",
                models.UUIDField(verbose_name="глобальный уникальный идентификатор адресного объекта"),
            ),
            (
                "name",
                models.TextField(blank=True, null=True, verbose_name="наименование"),
            ),
            (
                "typename",
                models.TextField(
                    blank=True,
                    null=True,
                    verbose_name="краткое наименование типа объекта",
                ),
            ),
            (
                "okato",
                models.CharField(blank=True, max_length=11, null=True, verbose_name="ОКАТО"),
            ),
            (
                "oktmo",
                models.CharField(blank=True, max_length=11, null=True, verbose_name="ОКТМО"),
            ),
        ],
        options={
            "verbose_name": "адресный объект",
            "verbose_name_plural": "адресные объекты",
            "db_table": "gar_addrobj",
            "abstract": False,
        },
    ),
    migrations.CreateModel(
        name="House",
        fields=[
            (
                "id",
                models.AutoField(primary_key=True, serialize=False, verbose_name="id"),
            ),
            ("region", models.CharField(max_length=2, verbose_name="код региона")),
            (
                "owner_adm",
                models.BigIntegerField(verbose_name="административная иерархия"),
            ),
            (
                "owner_mun",
                models.BigIntegerField(verbose_name="муниципальная иерархия"),
            ),
            (
                "objectid",
                models.BigIntegerField(verbose_name="глобальный уникальный идентификатор объекта"),
            ),
            (
                "objectguid",
                models.UUIDField(verbose_name="глобальный уникальный идентификатор адресного объекта"),
            ),
            (
                "housenum",
                models.TextField(blank=True, null=True, verbose_name="номер дома"),
            ),
            (
                "addnum1",
                models.TextField(
                    blank=True,
                    null=True,
                    verbose_name="дополнительный номер дома 1",
                ),
            ),
            (
                "addnum2",
                models.TextField(
                    blank=True,
                    null=True,
                    verbose_name="дополнительный номер дома 2",
                ),
            ),
            ("housetype", models.SmallIntegerField(verbose_name="основной тип дома")),
            (
                "addtype1",
                models.SmallIntegerField(
                    blank=True,
                    null=True,
                    verbose_name="дополнительный тип номера дома 1",
                ),
            ),
            (
                "addtype2",
                models.SmallIntegerField(
                    blank=True,
                    null=True,
                    verbose_name="дополнительный тип номера дома 2",
                ),
            ),
            (
                "postalcode",
                models.CharField(
                    blank=True,
                    max_length=6,
                    null=True,
                    verbose_name="почтовый индекс",
                ),
            ),
            (
                "okato",
                models.CharField(blank=True, max_length=11, null=True, verbose_name="ОКАТО"),
            ),
            (
                "oktmo",
                models.CharField(blank=True, max_length=11, null=True, verbose_name="ОКТМО"),
            ),
        ],
        options={
            "verbose_name": "номер дома",
            "verbose_name_plural": "номера домов",
            "db_table": "gar_house",
            "abstract": False,
        },
    ),
    migrations.CreateModel(
        name="House78",
        fields=[
            (
                "id",
                models.AutoField(primary_key=True, serialize=False, verbose_name="id"),
            ),
            ("region", models.CharField(max_length=2, verbose_name="код региона")),
            (
                "owner_adm",
                models.BigIntegerField(verbose_name="административная иерархия"),
            ),
            (
                "owner_mun",
                models.BigIntegerField(verbose_name="муниципальная иерархия"),
            ),
            (
                "objectid",
                models.BigIntegerField(verbose_name="глобальный уникальный идентификатор объекта"),
            ),
            (
                "objectguid",
                models.UUIDField(verbose_name="глобальный уникальный идентификатор адресного объекта"),
            ),
            (
                "housenum",
                models.TextField(blank=True, null=True, verbose_name="номер дома"),
            ),
            (
                "addnum1",
                models.TextField(
                    blank=True,
                    null=True,
                    verbose_name="дополнительный номер дома 1",
                ),
            ),
            (
                "addnum2",
                models.TextField(
                    blank=True,
                    null=True,
                    verbose_name="дополнительный номер дома 2",
                ),
            ),
            ("housetype", models.SmallIntegerField(verbose_name="основной тип дома")),
            (
                "addtype1",
                models.SmallIntegerField(
                    blank=True,
                    null=True,
                    verbose_name="дополнительный тип номера дома 1",
                ),
            ),
            (
                "addtype2",
                models.SmallIntegerField(
                    blank=True,
                    null=True,
                    verbose_name="дополнительный тип номера дома 2",
                ),
            ),
            (
                "postalcode",
                models.CharField(
                    blank=True,
                    max_length=6,
                    null=True,
                    verbose_name="почтовый индекс",
                ),
            ),
            (
                "okato",
                models.CharField(blank=True, max_length=11, null=True, verbose_name="ОКАТО"),
            ),
            (
                "oktmo",
                models.CharField(blank=True, max_length=11, null=True, verbose_name="ОКТМО"),
            ),
        ],
        options={
            "verbose_name": "дом в г.Санкт-Петербург",
            "verbose_name_plural": "дома в г.Санкт-Петербург",
            "db_table": "gar_house78",
            "abstract": False,
        },
    ),
    migrations.CreateModel(
        name="HouseAddType",
        fields=[
            (
                "id",
                models.SmallIntegerField(primary_key=True, serialize=False, verbose_name="id"),
            ),
            (
                "name",
                models.TextField(blank=True, null=True, verbose_name="наименование"),
            ),
            (
                "shortname",
                models.TextField(blank=True, null=True, verbose_name="краткое наименование"),
            ),
        ],
        options={
            "verbose_name": "дополнительный тип номера дома",
            "verbose_name_plural": "дополнительные типы номера дома",
            "db_table": "gar_house_addtypes",
            "abstract": False,
        },
    ),
    migrations.CreateModel(
        name="HouseType",
        fields=[
            (
                "id",
                models.SmallIntegerField(primary_key=True, serialize=False, verbose_name="id"),
            ),
            (
                "name",
                models.TextField(blank=True, null=True, verbose_name="наименование"),
            ),
            (
                "shortname",
                models.TextField(blank=True, null=True, verbose_name="краткое наименование"),
            ),
        ],
        options={
            "verbose_name": "тип номера дома",
            "verbose_name_plural": "типы номера дома",
            "db_table": "gar_house_types",
            "abstract": False,
        },
    ),
    migrations.CreateModel(
        name="Status",
        fields=[
            (
                "id",
                models.BigAutoField(
                    auto_created=True,
                    primary_key=True,
                    serialize=False,
                    verbose_name="ID",
                ),
            ),
            ("ver", models.IntegerField(verbose_name="версия")),
        ],
        options={
            "verbose_name": "версия",
            "verbose_name_plural": "версии",
        },
    ),
    migrations.AddIndex(
        model_name="house78",
        index=models.Index(fields=["objectid"], name="gar_house78_objecti_c911b3_idx"),
    ),
    migrations.AddIndex(
        model_name="house78",
        index=models.Index(fields=["objectguid"], name="gar_house78_objectg_7747b0_idx"),
    ),
    migrations.AddIndex(
        model_name="house78",
        index=models.Index(fields=["owner_adm"], name="gar_house78_owner_a_a33afd_idx"),
    ),
    migrations.AddIndex(
        model_name="house78",
        index=models.Index(fields=["owner_mun"], name="gar_house78_owner_m_318e68_idx"),
    ),
    migrations.AddIndex(
        model_name="house",
        index=models.Index(fields=["objectid"], name="gar_house_objecti_1911ad_idx"),
    ),
    migrations.AddIndex(
        model_name="house",
        index=models.Index(fields=["objectguid"], name="gar_house_objectg_a48da7_idx"),
    ),
    migrations.AddIndex(
        model_name="house",
        index=models.Index(fields=["owner_adm"], name="gar_house_owner_a_f74513_idx"),
    ),
    migrations.AddIndex(
        model_name="house",
        index=models.Index(fields=["owner_mun"], name="gar_house_owner_m_fb80bc_idx"),
    ),
    migrations.AddIndex(
        model_name="addrobj",
        index=models.Index(fields=["objectid"], name="gar_addrobj_objecti_ee18f2_idx"),
    ),
    migrations.AddIndex(
        model_name="addrobj",
        index=models.Index(fields=["objectguid"], name="gar_addrobj_objectg_d682c5_idx"),
    ),
    migrations.AddIndex(
        model_name="addrobj",
        index=models.Index(fields=["owner_adm"], name="gar_addrobj_owner_a_6877c5_idx"),
    ),
    migrations.AddIndex(
        model_name="addrobj",
        index=models.Index(fields=["owner_mun"], name="gar_addrobj_owner_m_3af51f_idx"),
    ),
]


database_operations: List[Operation] = []
if MANAGE:
    database_operations = state_operations


class Migration(migrations.Migration):
    initial = True

    dependencies = []

    operations = [
        migrations.SeparateDatabaseAndState(database_operations, state_operations),
    ]
