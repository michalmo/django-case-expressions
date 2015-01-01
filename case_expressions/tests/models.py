from __future__ import unicode_literals

from django.db import models
from django.utils.encoding import python_2_unicode_compatible

from ..models.query import BulkUpdateQuerySet
from .case_m import CaseTestModel, FKCaseTestModel


@python_2_unicode_compatible
class BulkUpdateQuerySetTestModel(models.Model):
    integer = models.IntegerField()
    string = models.CharField(max_length=100)
    boolean = models.BooleanField(default=False)

    objects = models.Manager.from_queryset(BulkUpdateQuerySet)()

    def __str__(self):
        return "%i, %s, %s" % (self.integer, self.string, self.boolean)
