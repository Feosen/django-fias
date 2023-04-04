from typing import Type, List, Tuple, NewType

from django.core.exceptions import ImproperlyConfigured
from django.db import models


class RefFieldMixin:
    Cfg = NewType('Cfg', Tuple[Type[models.Model], str])

    to: List[Cfg]

    def __init__(self, to: List[Cfg], *args, **kwargs):
        self.to = to
        for model, field_name in self.to:
            if not  model._meta.get_field(field_name).primary_key:
                raise ImproperlyConfigured(f'{model}.{field_name} must be primary key.')
        super().__init__(*args, **kwargs)

    def deconstruct(self):
        name, path, args, kwargs = super().deconstruct()
        kwargs["to"] = self.to
        return name, path, args, kwargs


class BigIntegerRefField(RefFieldMixin, models.BigIntegerField):
    pass
