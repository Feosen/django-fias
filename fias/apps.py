﻿from django.apps import AppConfig


class FiasConfig(AppConfig):
    name = "fias"

    # Signal receivers registration
    # noinspection PyUnresolvedReferences
    def ready(self) -> None:
        import fias.importer.table_validator  # noqa
