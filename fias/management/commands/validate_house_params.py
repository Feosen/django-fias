from pathlib import Path
from typing import Any, List

from fias.importer.commands import validate_house_params
from gar_loader.compat import BaseCommandCompatible


class Command(BaseCommandCompatible):
    help = "Search invalid house parameters"
    usage_str = (
        "Usage: ./manage.py validate_house_params --output <path|filename>" " [--min_ver <int>]" " [--region <str>]"
    )

    arguments_dictionary = {
        "--output": {
            "action": "store",
            "dest": "output",
            "type": Path,
            "help": "Output CSV file path",
        },
        "--min_ver": {
            "action": "store",
            "dest": "min_ver",
            "type": int,
            "default": None,
            "help": "Minimum version for searching",
        },
        "--region": {
            "action": "store",
            "dest": "regions",
            "type": str,
            "default": "__all__",
            "help": "Region to scan [,]",
        },
    }

    def handle(self, output: Path, min_ver: int | None, regions: str, **options: Any) -> None:
        typed_regions: List[str] | str = regions
        if typed_regions != "__all__":
            typed_regions = regions.split(",")
        validate_house_params(output, min_ver, typed_regions)
