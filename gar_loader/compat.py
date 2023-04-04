#coding: utf-8
from __future__ import unicode_literals, absolute_import

from django.core.management.base import BaseCommand


class BaseCommandCompatible(BaseCommand):
    arguments_dictionary = {}

    def add_arguments(self, parser):
        for command, arguments in self.arguments_dictionary.items():
            parser.add_argument(command, **arguments)

    def handle(self, *args, **options):
        """
        The actual logic of the command. Subclasses must implement
        this method.
        """
        raise NotImplementedError('subclasses of BaseCommand must provide a handle() method')
