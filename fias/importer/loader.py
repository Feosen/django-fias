# coding: utf-8
from __future__ import absolute_import, unicode_literals

import datetime
import logging
from sys import stdout
from typing import Any, List

from django import db
from django.conf import settings
from django.db import IntegrityError
from progress import Infinite

from fias.config import TableName
from fias.importer.signals import post_import_table, pre_import_table
from fias.importer.table.table import AbstractTableList, Table
from fias.importer.validators import (
    get_common_validator,
    get_create_validator,
    get_update_validator,
)
from fias.models import AbstractModel

logger = logging.getLogger(__name__)


class LoadingBar(Infinite):  # type: ignore
    file = stdout
    check_tty: bool = False

    text: str = (
        "T: %(table)s."
        " L: %(loaded)d | U: %(updated)d"
        " | S: %(skipped)d[E:%(errors)d]"
        " | R: %(depth)d[%(stack_str)s]"
        " \tFN: %(filename)s"
    )

    loaded: int = 0
    updated: int = 0
    skipped: int = 0
    errors: int = 0
    depth: int = 0
    stack: List[str]
    stack_str: str = "0"
    hide_cursor: bool = False

    def __init__(self, message: str | None = None, table: str = "unknown", filename: str = "unknown", **kwargs: Any):
        self.table = table
        self.filename = filename
        self.stack = []
        super(LoadingBar, self).__init__(message=message, **kwargs)

    def __getitem__(self, key: str) -> Any:
        if key.startswith("_"):
            return None
        return getattr(self, key, None)

    def update(
        self,
        loaded: int = 0,
        updated: int = 0,
        skipped: int = 0,
        errors: int = 0,
        regress_depth: int = 0,
        regress_len: int = 0,
        regress_iteration: int = 0,
    ) -> None:
        if loaded:
            self.loaded = loaded
        if updated:
            self.updated = updated
        if skipped:
            self.skipped = skipped
        if errors:
            self.errors = errors

        self.depth = regress_depth
        if not self.depth:
            self.stack_str = "0"
        else:
            regress_len_s = f"{regress_iteration}:{regress_len}"
            stack_len = len(self.stack)
            if stack_len == self.depth:
                self.stack[self.depth - 1] = regress_len_s
            elif stack_len < self.depth:
                self.stack.append(regress_len_s)
            else:
                self.stack = self.stack[0 : self.depth]
                self.stack[self.depth - 1] = regress_len_s

            self.stack_str = "/".join(self.stack)

        ln = self.text % self
        self.writeln(ln)


class TableLoader(object):
    def __init__(self, limit: int = 10000):
        self.limit = int(limit)
        self.counter = 0
        self.upd_counter = 0
        self.skip_counter = 0
        self.err_counter = 0
        self.today = datetime.date.today()

    def regressive_create(self, table: Table, objects: List[AbstractModel], bar: LoadingBar, depth: int = 1) -> None:
        count = len(objects)
        batch_len = count // 3 or 1
        batch_count = count // batch_len
        if batch_count * batch_len < count:
            batch_count += 1

        for i in range(0, batch_count):
            batch = objects[i * batch_len : (i + 1) * batch_len]
            bar.update(regress_depth=depth, regress_len=batch_len, regress_iteration=i + 1)
            try:
                table.model.objects.bulk_create(batch)
            except (IntegrityError, ValueError):
                if batch_len <= 1:
                    self.counter -= 1
                    self.skip_counter += 1
                    self.err_counter += 1
                    bar.update(loaded=self.counter, skipped=self.skip_counter, errors=self.err_counter)
                    if batch_len > 0:
                        obj_s = {f.name: getattr(batch[0], f.attname) for f in table.model._meta.fields}
                        logger.warning(f'Region {table.region} table "{table.name}" skip invalid object {obj_s}.')
                    continue
                else:
                    self.regressive_create(table, batch, bar=bar, depth=depth + 1)

    def create(self, table: Table, objects: List[AbstractModel], bar: LoadingBar) -> None:
        try:
            table.model.objects.bulk_create(objects)
        except (IntegrityError, ValueError):
            self.regressive_create(table, objects, bar)

        #  Обнуляем индикатор регрессии
        bar.update(regress_depth=0, regress_len=0)
        if settings.DEBUG:
            db.reset_queries()

    def load(self, tablelist: AbstractTableList, table: Table) -> None:
        logger.info(f'Region {table.region} table "{table.name}" is loading.')
        pre_import_table.send(sender=self.__class__, table=table)
        self.do_load(tablelist=tablelist, table=table)
        post_import_table.send(sender=self.__class__, table=table)
        logger.info(f'Region {table.region} table "{table.name}" has been loaded.')

    def do_load(self, tablelist: AbstractTableList, table: Table) -> None:
        bar = LoadingBar(table=table.name, filename=table.filename)
        bar.update()

        tn = TableName(table.name)
        common_validator = get_common_validator(tn)
        create_validator = get_create_validator(tn)

        objects = set()
        for item in table.rows(tablelist=tablelist):
            if item is None or not (common_validator(item, self.today) and create_validator(item, self.today)):
                self.skip_counter += 1

                if self.skip_counter and self.skip_counter % self.limit == 0:
                    bar.update(skipped=self.skip_counter)
                continue

            objects.add(item)
            self.counter += 1

            if self.counter and self.counter % self.limit == 0:
                self.create(table, list(objects), bar=bar)
                objects.clear()
                bar.update(loaded=self.counter, skipped=self.skip_counter)

        if objects:
            self.create(table, list(objects), bar=bar)

        bar.update(loaded=self.counter, skipped=self.skip_counter)
        bar.finish()


class TableUpdater(TableLoader):
    def __init__(self, limit: int = 10000):
        self.upd_limit = 100
        super(TableUpdater, self).__init__(limit=limit)

    def do_load(self, tablelist: AbstractTableList, table: Table) -> None:
        bar = LoadingBar(table=table.name, filename=table.filename)

        model = table.model

        tn = TableName(table.name)
        common_validator = get_common_validator(tn)
        create_validator = get_create_validator(tn)
        update_validator = get_update_validator(tn)

        objects = set()
        for item in table.rows(tablelist=tablelist):
            if item is None or not common_validator(item, self.today):
                self.skip_counter += 1
                continue

            try:
                old_obj = model.objects.get(pk=item.pk)
            except model.DoesNotExist:
                if not create_validator(item, self.today):
                    self.skip_counter += 1
                    continue
                else:
                    objects.add(item)
                    self.counter += 1

            else:
                if not update_validator(item, self.today):
                    self.skip_counter += 1
                    continue
                if old_obj.updatedate < item.updatedate:
                    item.save()
                    self.upd_counter += 1

            if self.counter and self.counter % self.limit == 0:
                self.create(table, list(objects), bar=bar)
                objects.clear()
                bar.update(loaded=self.counter)

            if self.upd_counter and self.upd_counter % self.upd_limit == 0:
                bar.update(updated=self.upd_counter)

        if objects:
            self.create(table, list(objects), bar=bar)

        bar.update(loaded=self.counter, updated=self.upd_counter, skipped=self.skip_counter)
        bar.finish()
