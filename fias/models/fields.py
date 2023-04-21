from typing import Type, List, Tuple, Any, Mapping, TYPE_CHECKING

from django.core.exceptions import ImproperlyConfigured
from django.db import models

if TYPE_CHECKING:
    BaseFieldMixin = models.Field[int, int]
    BigIntegerField = models.BigIntegerField[int, int]
else:
    BaseFieldMixin = object
    BigIntegerField = models.BigIntegerField


class RefFieldMixin(BaseFieldMixin):
    _Cfg = Tuple[Type[models.Model], str]

    to: List[_Cfg]

    def __init__(self, to: List[_Cfg], verbose_name: str | None = None, *args: Any, **kwargs: Any):
        self.to = to
        for model, field_name in self.to:
            field = model._meta.get_field(field_name)
            if not (hasattr(field, 'primary_key') and field.primary_key):
                raise ImproperlyConfigured(f'{model}.{field_name} must be primary key.')
        super().__init__(verbose_name, *args, **kwargs)

    def deconstruct(self) -> Tuple[str, str, List[Any], Mapping[str, Any]]:
        name, path, args, kwargs = super().deconstruct()
        kwargs["to"] = self.to
        return name, path, args, kwargs


class BigIntegerRefField(RefFieldMixin, BigIntegerField):
    pass
