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
        "--regions": {
            "action": "store",
            "dest": "regions",
            "nargs": "+",
            "type": str,
            "help": "Region to scan (space separated)",
        },
    }

    def handle(self, output: Path, min_ver: int | None, regions: List[str] | None, **options: Any) -> None:
        validate_house_params(output, min_ver, regions)
