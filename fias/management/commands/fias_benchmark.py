# coding: utf-8
from __future__ import absolute_import, unicode_literals

from pathlib import Path
from tempfile import TemporaryDirectory
from time import monotonic
from typing import Any, Dict, List

from django.core.management import call_command

from fias.models import House, Status, Version
from fias.tests.fixture import Fixture
from gar_loader.compat import BaseCommandCompatible


class Command(BaseCommandCompatible):
    help = "Test fias execution time."
    usage_str = "Usage: ./manage.py fias_benchmark"

    def __init_db(self) -> None:
        args: List[Any] = []
        kwargs = {
            "database": "gar",
        }
        call_command("migrate", *args, **kwargs)

    def __init_load_data(self) -> None:
        version = Version(
            ver=20221129, date=None, dumpdate="2022-11-29", complete_xml_url="fake_url", delta_xml_url="fake_url"
        )
        version.full_clean()
        version.save()

    def __init_update_data(self) -> None:
        version = Version(
            ver=20221125, date=None, dumpdate="2022-11-25", complete_xml_url="fake_url", delta_xml_url="fake_url"
        )
        version.full_clean()
        version.save()

        version_1 = Version(
            ver=20221129, date=None, dumpdate="2022-11-29", complete_xml_url="fake_url", delta_xml_url="fake_url"
        )
        version_1.full_clean()
        version_1.save()

        for r in range(1, 100):
            s = Status(table="house", region=f"{r:02}", ver=version)
            s.full_clean()
            s.save()

    def __clear_data(self) -> None:
        args: List[Any] = []
        kwargs = {
            "database": "gar",
            "interactive": False,
        }
        call_command("flush", *args, **kwargs)

    def handle(
        self,
        **options: Any,
    ) -> None:
        if Status.objects.exists():
            raise ValueError("Database is not empty.")

        self.__init_db()
        houses_per_region = 100

        with TemporaryDirectory() as delta_dir:
            Fixture.create(Path(delta_dir) / Path("delta.zip"), 20221129, houses_per_region)
            base_args: List[Any] = []
            base_kwargs: Dict[str, Any] = {
                "update_version_info": False,
            }

            cfgs: List[Dict[str, Any]] = [
                {"update": True, "threads": 1, "src": str(delta_dir)},
                {"update": True, "threads": None, "src": str(delta_dir)},
                {"update": False, "threads": 1, "src": str(Path(delta_dir) / Path("delta.zip"))},
                {"update": False, "threads": None, "src": str(Path(delta_dir) / Path("delta.zip"))},
            ]

            results: List[str] = []
            for kwargs in cfgs:
                if kwargs["update"]:
                    self.__init_update_data()
                else:
                    self.__init_load_data()

                start = monotonic()
                call_command("fias", *base_args, **base_kwargs | kwargs)
                stop = monotonic()

                house_count = House.objects.count()
                error = 99 * houses_per_region != house_count
                self.__clear_data()
                results.append(f"update {kwargs['update']}, threads {kwargs['threads']}: {stop - start}")
                if error:
                    raise ValueError(f"Something wrong: house count = {house_count}")

            for r in results:
                print(r)
