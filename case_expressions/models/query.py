from __future__ import unicode_literals

from django.db import connections, models, transaction
from django.db.models import sql
from django.db.models.query_utils import Q
from django.db.models.sql.constants import CURSOR
from .expressions import UpdateModelList


class BulkUpdateQuerySet(models.QuerySet):
    """
    A QuerySet that adds methods for bulk updating model instances.
    Heavily based on models.QuerySet.bulk_create and it's helper methods.
    """
    def bulk_update(self, objs, update_fields=None, batch_size=None):
        """
        Updates each of the instances in the database. This does *not* call
        save() on each of the instances, and does not send any pre/post save
        signals.
        """
        assert batch_size is None or batch_size > 0
        if self.model._meta.parents:
            raise ValueError("Can't bulk update an inherited model")
        if not objs:
            return objs
        if any(o.pk is None for o in objs):
            raise ValueError("Can't bulk update instances without a pk")
        self._for_write = True
        non_pk_fields = [f for f in self.model._meta.local_concrete_fields if not f.primary_key]
        if update_fields:
            non_pk_fields = [f for f in non_pk_fields
                             if f.name in update_fields or f.attname in update_fields]
        with transaction.atomic(using=self.db, savepoint=False):
            self._batched_update(objs, non_pk_fields, batch_size)

    def _update_many(self, objs, fields, using=None):
        """
        Updates many records for the given model. This uses UpdateQuery to
        generate the query and UpdateValuesList to generate CASE expressions
        for settings the values.
        """
        if using is None:
            using = self.db
        query = sql.UpdateQuery(self.model)
        query.add_update_values({f.name: (UpdateModelList(objs, f)) for f in fields})
        query.add_q(Q(pk__in=(o.pk for o in objs)))
        self._result_cache = None
        return query.get_compiler(using=using).execute_sql(CURSOR)
    _update_many.alters_data = True
    _update_many.queryset_only = False

    def _batched_update(self, objs, fields, batch_size):
        """
        A little helper method for bulk_update to update the bulk one batch
        at a time in a loop.
        """
        if not objs:
            return
        ops = connections[self.db].ops

        # re-use DatabaseOperations.bulk_batch_size, by passing a list of
        # fields (with duplicates) that have placeholders in the query.
        def iter_placeholder_fields(fields, pk_field):
            for f in fields:
                yield pk_field
                yield f
            yield pk_field

        placeholder_fields = tuple(iter_placeholder_fields(fields, self.model._meta.pk))
        batch_size = (batch_size or max(ops.bulk_batch_size(placeholder_fields, objs), 1))
        for batch in [objs[i:i + batch_size] for i in range(0, len(objs), batch_size)]:
            self._update_many(batch, fields=fields, using=self.db)
            # # or without helper method
            # self.model._base_manager.using(self.db)\
            #     .filter(pk__in=(o.pk for o in batch))\
            #     .update(**{f.name: (UpdateModelList(batch, f)) for f in fields})
