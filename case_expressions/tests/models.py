from __future__ import unicode_literals

from django.db import models
from ..models.query import BulkUpdateQuerySet


class CaseTestModel(models.Model):
    integer = models.IntegerField()
    string = models.CharField(max_length=100)


class BulkUpdateQuerySetTestModel(models.Model):
    integer = models.IntegerField()
    string = models.CharField(max_length=100)
    boolean = models.BooleanField(default=False)

    objects = models.Manager.from_queryset(BulkUpdateQuerySet)()
