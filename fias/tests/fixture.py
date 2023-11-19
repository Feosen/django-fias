import xml.etree.ElementTree as ET
from pathlib import Path
from typing import Any, Callable, Dict, List, Tuple
from uuid import UUID, uuid4
from zipfile import ZipFile


class Fixture:
    class IdGenerator:
        __id: int

        def __init__(self, start: int = 1):
            self.__id = start

        def __next__(self) -> int:
            self.__id += 1
            return self.__id

    @staticmethod
    def _create_file_name(table_name: str, date: str) -> str:
        return f"AS_{table_name}_{date}_{uuid4()}.XML"

    @staticmethod
    def _house_type_data(
        id: int,
        name: str | None = None,
        short_name: str | None = None,
        desc: str | None = None,
        update_date: str = "2019-07-16",
        start_date: str = "2019-07-16",
        end_date: str = "2079-06-06",
        is_active: bool = True,
    ) -> Dict[str, str]:
        return {
            "ID": str(id),
            "NAME": name or f"Name {id}",
            "SHORTNAME": short_name or f"Short Name {id}",
            "DESC": desc or f"Desc {id}",
            "UPDATEDATE": update_date,
            "STARTDATE": start_date,
            "ENDDATE": end_date,
            "ISACTIVE": "1" if is_active else "0",
        }

    @staticmethod
    def _house_data(
        id: int,
        object_id: int,
        object_guid: UUID | None = None,
        change_id: int = 1,
        house_num: int = 1,
        house_type: int = 1,
        add_num_1: int | None = None,
        add_type_1: int | None = None,
        add_num_2: int | None = None,
        add_type_2: int | None = None,
        oper_type: int = 10,
        prev_id: int = 0,
        next_id: int = 0,
        update_date: str = "2019-07-16",
        start_date: str = "2019-07-16",
        end_date: str = "2079-06-06",
        is_actual: bool = True,
        is_active: bool = True,
    ) -> Dict[str, str]:
        return {
            "ID": str(id),
            "OBJECTID": str(object_id),
            "OBJECTGUID": str(object_guid or uuid4()),
            "CHANGEID": str(change_id),
            "HOUSENUM": str(house_num),
            "HOUSETYPE": str(house_type),
            "ADDNUM1": str(add_num_1) if add_num_1 else "",
            "ADDTYPE1": str(add_type_1) if add_type_1 else "",
            "ADDNUM2": str(add_num_2) if add_num_2 else "",
            "ADDTYPE2": str(add_type_2) if add_type_2 else "",
            "OPERTYPEID": str(oper_type),
            "PREVID": str(prev_id),
            "NEXTID": str(next_id),
            "UPDATEDATE": update_date,
            "STARTDATE": start_date,
            "ENDDATE": end_date,
            "ISACTUAL": "1" if is_actual else "0",
            "ISACTIVE": "1" if is_active else "0",
        }

    @classmethod
    def create(cls, path: Path, version: int, houses_per_region: int = 1000) -> None:
        if path.is_dir():
            raise ValueError("path must be file")

        MT = List[Tuple[str, str, Callable[..., Dict[str, str]], Dict[str, Any], int]]
        data_table_map: MT = [
            ("HOUSES", "HOUSE", cls._house_data, {"house_type": 2}, houses_per_region),
        ]

        metadata_table_map: MT = [
            ("HOUSETYPES", "HOUSETYPE", cls._house_type_data, {}, 10),
        ]

        version_str = str(version)
        date_str = f"{version_str[0:4]}-{version_str[4:6]}-{version_str[6:8]}"

        id_gen = cls.IdGenerator()
        object_id_gen = cls.IdGenerator()
        with ZipFile(path, "w") as myzip:
            for table_name, item_name, fn, kwargs, count in metadata_table_map:
                root = ET.Element(table_name)
                for i in range(1, count + 1):
                    elem = ET.Element(item_name)
                    elem.attrib = fn(id=next(id_gen), start_date=date_str, update_date=date_str, **kwargs)
                    root.append(elem)
                file_name = cls._create_file_name(table_name, version_str)
                myzip.writestr(f"{file_name}", ET.tostring(root))

            for r in range(1, 100):
                region_name = f"{r:02}"
                myzip.mkdir(region_name)
                for table_name, item_name, fn, kwargs, count in data_table_map:
                    root = ET.Element(table_name)
                    for i in range(1, count + 1):
                        elem = ET.Element(item_name)
                        elem.attrib = fn(
                            id=next(id_gen),
                            object_id=next(object_id_gen),
                            start_date=date_str,
                            update_date=date_str,
                            **kwargs,
                        )
                        root.append(elem)
                    file_name = cls._create_file_name(table_name, version_str)
                    myzip.writestr(f"{region_name}/{file_name}", ET.tostring(root))
