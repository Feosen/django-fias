# coding: utf-8
from __future__ import unicode_literals, absolute_import

from dataclasses import dataclass
from typing import Type, Dict, Any, TYPE_CHECKING, Iterable, Union, Protocol, Generator, Tuple

from django.db import connections
from django.db import models
from django.db.models import ManyToOneRel, OneToOneRel, ManyToManyRel, ForeignObjectRel, ForeignKey, ManyToManyField
from django.db.models.fields.related import RelatedField
from django.db.models.options import Options
from django.db.utils import ProgrammingError

# TODO: refactor it!
from fias.config import DATABASE_ALIAS

if TYPE_CHECKING:
    _Field = models.Field[Any, Any]
    _Options = Options[models.Model]

else:
    _Field = models.Field
    _Options = Options


def get_simple_field(field: _Field) -> _Field:
    params: Dict[str, Any] = {
        'db_index': False,
        'primary_key': False,
        'unique': field.unique,
        'blank': field.blank,
        'null': field.null,
    }

    if isinstance(field, models.ForeignKey):
        params.update({
            'to': field.remote_field.model,
            'on_delete': field.remote_field.on_delete,
        })
    elif isinstance(field, models.CharField):
        params.update({
            'max_length': field.max_length,
        })
    elif isinstance(field, RelatedField):
        raise NotImplementedError('Only ForeignKey and OneToOne related fields supported')
    simple_field = field.__class__(**params)
    simple_field.column = field.column
    simple_field.model = field.model

    return simple_field


def get_all_related_objects(opts: _Options) -> Iterable[ForeignObjectRel]:
    return [r for r in opts.related_objects if not r.field.many_to_many]  # type: ignore


def get_all_related_many_to_many_objects(opts: _Options) -> Iterable[ForeignObjectRel]:
    return [r for r in opts.related_objects if r.field.many_to_many]  # type: ignore


def get_indexed_fields(model: Type[models.Model]) -> Iterable[Tuple[_Field, _Field]]:
    for field in model._meta.fields:
        # TODO: PK
        # Не удаляем индекс у первичных ключей и полей,
        # на которые есть ссылки из других моделей
        if field.primary_key and any(
                [rel for rel in get_all_related_objects(model._meta) if rel.field_name == field.name]
        ):
            continue

        # TODO: at this time django-stubs lacks Field().db_index: bool
        if field.db_index or field.unique:  # type: ignore
            yield field, get_simple_field(field)


def change_indexes_for_model(model: Type[models.Model], field_from: _Field, field_to: _Field) -> None:
    con = connections[DATABASE_ALIAS]
    ed = con.schema_editor()

    try:
        ed.alter_field(model, field_from, field_to)
    except ProgrammingError as e:
        print(str(e))


def remove_indexes_from_model(model: Type[models.Model]) -> None:
    for field, simple_field in get_indexed_fields(model=model):
        change_indexes_for_model(model=model, field_from=field, field_to=simple_field)


def restore_indexes_for_model(model: Type[models.Model]) -> None:
    for field, simple_field in get_indexed_fields(model=model):
        change_indexes_for_model(model=model, field_from=simple_field, field_to=field)
