# coding: utf-8
from __future__ import unicode_literals, absolute_import

import datetime
import logging
from sys import stdout
from typing import List, Any

from django import db
from django.conf import settings
from django.db import IntegrityError
from progress import Infinite

from fias.config import REMOVE_NOT_ACTUAL, TableName
from fias.importer.signals import pre_import_table, post_import_table
from fias.importer.table.table import AbstractTableList, Table
from fias.importer.validators import validate
from fias.models import AbstractModel, AbstractIsActiveModel

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

    def validate(self, table: Table, item: AbstractModel) -> bool:
        if item.pk is None:
            return False

        try:
            tn = TableName(table.name)
            return validate(tn, item, today=self.today)
        except ValueError:
            return True

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
            except (IntegrityError, ValueError) as e:
                if batch_len <= 1:
                    self.counter -= 1
                    self.skip_counter += 1
                    self.err_counter += 1
                    bar.update(loaded=self.counter, skipped=self.skip_counter, errors=self.err_counter)
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

        objects = set()
        for item in table.rows(tablelist=tablelist):
            if item is None or not self.validate(table, item):
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

        if REMOVE_NOT_ACTUAL and issubclass(table.model, AbstractIsActiveModel):
            table.model.objects.filter(isactive=False).delete()

        bar.update(loaded=self.counter, skipped=self.skip_counter)
        bar.finish()


class TableUpdater(TableLoader):
    def __init__(self, limit: int = 10000):
        self.upd_limit = 100
        super(TableUpdater, self).__init__(limit=limit)

    def do_load(self, tablelist: AbstractTableList, table: Table) -> None:
        bar = LoadingBar(table=table.name, filename=table.filename)

        model = table.model
        objects = set()
        for item in table.rows(tablelist=tablelist):
            if item is None or not self.validate(table, item):
                self.skip_counter += 1
                continue

            try:
                old_obj = model.objects.get(pk=item.pk)
            except model.DoesNotExist:
                objects.add(item)
                self.counter += 1
            else:
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

        if REMOVE_NOT_ACTUAL and issubclass(model, AbstractIsActiveModel):
            model.objects.filter(isactive=False).delete()

        bar.update(loaded=self.counter, updated=self.upd_counter, skipped=self.skip_counter)
        bar.finish()
