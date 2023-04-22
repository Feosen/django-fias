# coding: utf-8
from __future__ import absolute_import, unicode_literals

from argparse import ArgumentParser
from typing import Any, Dict

from django.core.management.base import BaseCommand


class BaseCommandCompatible(BaseCommand):
    arguments_dictionary: Dict[str, Any] = {}

    def add_arguments(self, parser: ArgumentParser) -> None:
        for command, arguments in self.arguments_dictionary.items():
            parser.add_argument(command, **arguments)
