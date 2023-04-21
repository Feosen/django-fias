from django.db import models

__all__ = ("AddrObj", "House", "House78", "HouseType", "HouseAddType", "Status")


class AbstractModel(models.Model):
    class Meta:
        managed = True
        abstract = True


class Status(AbstractModel):
    class Meta(AbstractModel.Meta):
        abstract = False
        app_label = "target"
        verbose_name = "версия"
        verbose_name_plural = "версии"

    ver = models.IntegerField(verbose_name="версия")


class AddrObj(AbstractModel):
    id = models.AutoField(verbose_name="id", primary_key=True)  # own, R
    region = models.CharField(verbose_name="код региона", max_length=2)  # T(=2), R
    owner_adm = models.BigIntegerField(verbose_name="административная иерархия")  # as objectid, R
    owner_mun = models.BigIntegerField(verbose_name="муниципальная иерархия")  # as objectid, R
    aolevel = models.IntegerField(verbose_name="уровень адресного объекта")  # T(1-10), R
    objectid = models.BigIntegerField(verbose_name="глобальный уникальный идентификатор объекта")  # N(19), R
    objectguid = models.UUIDField(verbose_name="глобальный уникальный идентификатор адресного объекта")  # T(36), R
    name = models.TextField(verbose_name="наименование", blank=True, null=True)  # T(1-250), R
    typename = models.TextField(verbose_name="краткое наименование типа объекта", blank=True, null=True)  # T(1-50), R
    okato = models.CharField(verbose_name="ОКАТО", max_length=11, blank=True, null=True)  # T(=11), R
    oktmo = models.CharField(verbose_name="ОКТМО", max_length=11, blank=True, null=True)  # T(=11), R

    class Meta(AbstractModel.Meta):
        db_table = "gar_addrobj"
        app_label = "target"
        verbose_name = "адресный объект"
        verbose_name_plural = "адресные объекты"
        indexes = [
            models.Index(fields=["objectid"]),
            models.Index(fields=["objectguid"]),
            models.Index(fields=["owner_adm"]),
            models.Index(fields=["owner_mun"]),
        ]


class AbstractHouse(AbstractModel):
    id = models.AutoField(verbose_name="id", primary_key=True)  # own, R
    region = models.CharField(verbose_name="код региона", max_length=2)  # T(=2), R
    owner_adm = models.IntegerField(verbose_name="административная иерархия")  # as objectid, R
    owner_mun = models.IntegerField(verbose_name="муниципальная иерархия")  # as objectid, R
    objectid = models.BigIntegerField(verbose_name="глобальный уникальный идентификатор объекта")  # N(19), R
    objectguid = models.UUIDField(verbose_name="глобальный уникальный идентификатор адресного объекта")  # T(36), R
    housenum = models.TextField(verbose_name="номер дома", blank=True, null=True)  # T(1-50), O
    addnum1 = models.TextField(verbose_name="дополнительный номер дома 1", blank=True, null=True)  # T(1-50), O
    addnum2 = models.TextField(verbose_name="дополнительный номер дома 2", blank=True, null=True)  # T(1-50), O
    housetype = models.IntegerField(verbose_name="основной тип дома")  # N(2), O
    addtype1 = models.IntegerField(verbose_name="дополнительный тип номера дома 1", blank=True, null=True)  # N(2), O
    addtype2 = models.IntegerField(verbose_name="дополнительный тип номера дома 2", blank=True, null=True)  # N(2), O
    postalcode = models.CharField(verbose_name="почтовый индекс", max_length=6, blank=True, null=True)  # T(6)
    okato = models.CharField(verbose_name="ОКАТО", max_length=11, blank=True, null=True)  # T(=11), R
    oktmo = models.CharField(verbose_name="ОКТМО", max_length=11, blank=True, null=True)  # T(=11), R

    class Meta(AbstractModel.Meta):
        abstract = True


class House(AbstractHouse):
    class Meta(AbstractModel.Meta):
        db_table = "gar_house"
        app_label = "target"
        verbose_name = "номер дома"
        verbose_name_plural = "номера домов"
        indexes = [
            models.Index(fields=["objectid"]),
            models.Index(fields=["objectguid"]),
            models.Index(fields=["owner_adm"]),
            models.Index(fields=["owner_mun"]),
        ]


class House78(AbstractHouse):
    class Meta(AbstractModel.Meta):
        db_table = "gar_house78"
        app_label = "target"
        verbose_name = "дом в г.Санкт-Петербург"
        verbose_name_plural = "дома в г.Санкт-Петербург"
        indexes = [
            models.Index(fields=["objectid"]),
            models.Index(fields=["objectguid"]),
            models.Index(fields=["owner_adm"]),
            models.Index(fields=["owner_mun"]),
        ]


class HouseType(AbstractModel):
    id = models.SmallIntegerField(verbose_name="id", primary_key=True)
    name = models.TextField(verbose_name="наименование", blank=True, null=True)
    shortname = models.TextField(verbose_name="краткое наименование", blank=True, null=True)

    class Meta(AbstractModel.Meta):
        db_table = "gar_house_types"
        verbose_name = "тип номера дома"
        verbose_name_plural = "типы номера дома"


class HouseAddType(AbstractModel):
    id = models.SmallIntegerField(verbose_name="id", primary_key=True)
    name = models.TextField(verbose_name="наименование", blank=True, null=True)
    shortname = models.TextField(verbose_name="краткое наименование", blank=True, null=True)

    class Meta(AbstractModel.Meta):
        db_table = "gar_house_addtypes"
        verbose_name = "дополнительный тип номера дома"
        verbose_name_plural = "дополнительные типы номера дома"
