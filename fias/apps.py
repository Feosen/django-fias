from django.apps import AppConfig


class DevicesConfig(AppConfig):
    name = 'fias'

    # Signal receivers registration
    # noinspection PyUnresolvedReferences
    def ready(self):
        import fias.importer.table_validator
