from __future__ import unicode_literals

from operator import attrgetter
from unittest import TestCase, skip

from django.db import connection, models
from django.db.backends.sqlite3.base import DatabaseWrapper
from django.db.models import F, Q, Value, CharField
from django.db.models.expressions import SearchedCase, SimpleCase
from django.db.models.sql.query import Query
from django.db.models.sql.compiler import SQLCompiler
from django.test import TestCase as DjangoTestCase

from ..models.expressions import UpdateModelList
from .models import CaseTestModel, BulkUpdateQuerySetTestModel
from .case_t import CaseExpressionTests as CaseExpressionIntegrationTests


class CaseExpressionTestCase(TestCase):
    def setUp(self):
        self.query = Query(CaseTestModel)
        self.connection = DatabaseWrapper({}, 'test')
        self.compiler = SQLCompiler(self.query, connection, 'test')

    def assertGeneratedSqlEqual(self, expression, expected_sql, expected_params):
        expression = expression.resolve_expression(self.query)
        sql, params = expression.as_sql(self.compiler, self.connection)
        self.assertEqual(sql, expected_sql, "The 'as_sql' method did not return the expected SQL.")
        self.assertEqual(params, expected_params, "The 'as_sql' method did not return the expected params.")


class SearchedCaseUnitTests(CaseExpressionTestCase):
    def test_values(self):
        self.assertGeneratedSqlEqual(
            SearchedCase([(Q(integer__gt=0), Value('positive')),
                          (Q(integer__lt=0), Value('negative')),
                          (Q(integer=0), Value('zero'))],
                         output_field=CharField()),

            'CASE WHEN "tests_casetestmodel"."integer" > %s THEN %s '
            'WHEN "tests_casetestmodel"."integer" < %s THEN %s '
            'WHEN "tests_casetestmodel"."integer" = %s THEN %s END',

            [0, 'positive', 0, 'negative', 0, 'zero'])

    def test_values_with_default(self):
        self.assertGeneratedSqlEqual(
            SearchedCase([(Q(integer__gt=0), Value('positive')),
                          (Q(integer__lt=0), Value('negative'))],
                         default=Value('zero'),
                         output_field=CharField()),

            'CASE WHEN "tests_casetestmodel"."integer" > %s THEN %s '
            'WHEN "tests_casetestmodel"."integer" < %s THEN %s '
            'ELSE %s END',

            [0, 'positive', 0, 'negative', 'zero'])

    def test_empty_values(self):
        self.assertGeneratedSqlEqual(SearchedCase(), 'NULL', ())

    def test_empty_values_with_default(self):
        self.assertGeneratedSqlEqual(
            SearchedCase(default=Value('I cannot count'), output_field=CharField()),
            '%s', ['I cannot count'])


class SimpleCaseUnitTests(CaseExpressionTestCase):
    def test_values(self):
        self.assertGeneratedSqlEqual(
            SimpleCase('integer',
                       [(Value(1), Value('one')), (Value(2), Value('two')), (Value(3), Value('three'))],
                       output_field=CharField()),

            'CASE "tests_casetestmodel"."integer" '
            'WHEN %s THEN %s WHEN %s THEN %s WHEN %s THEN %s END',

            [1, 'one', 2, 'two', 3, 'three'])

    def test_values_with_default(self):
        self.assertGeneratedSqlEqual(
            SimpleCase('integer', [(Value(1), Value('one')), (Value(2), Value('two'))],
                       default=Value('I cannot count that high'),
                       output_field=CharField()),

            'CASE "tests_casetestmodel"."integer" '
            'WHEN %s THEN %s WHEN %s THEN %s '
            'ELSE %s END',

            [1, 'one', 2, 'two', 'I cannot count that high'])

    def test_empty_values(self):
        self.assertGeneratedSqlEqual(
            SimpleCase('integer', output_field=CharField()),
            'NULL', ())

    def test_empty_values_with_default(self):
        self.assertGeneratedSqlEqual(
            SimpleCase('integer', default=Value('I cannot count'), output_field=CharField()),
            '%s', ['I cannot count'])


class UpdateFieldListUnitTests(CaseExpressionTestCase):
    def test_objects(self):
        self.assertGeneratedSqlEqual(
            UpdateModelList([CaseTestModel(pk=1, integer=10),
                             CaseTestModel(pk=2, integer=20),
                             CaseTestModel(pk=3, integer=30)],
                            CaseTestModel._meta.get_field('integer')),

            'CASE "tests_casetestmodel"."id" '
            'WHEN %s THEN %s WHEN %s THEN %s WHEN %s '
            'THEN %s END',

            [1, 10, 2, 20, 3, 30])

    @skip('Not implemented yet')
    def test_objects_type_conversion(self):
        self.assertGeneratedSqlEqual(
            UpdateModelList([CaseTestModel(pk=1, integer='10'),
                             CaseTestModel(pk=2, integer='20'),
                             CaseTestModel(pk=3, integer='30')],
                            CaseTestModel._meta.get_field('integer')),

            'CASE "tests_casetestmodel"."id" '
            'WHEN %s THEN %s WHEN %s THEN %s WHEN %s '
            'THEN %s END',

            [1, 10, 2, 20, 3, 30])

    def test_empty_objects(self):
        self.assertGeneratedSqlEqual(
            UpdateModelList([], output_field=CaseTestModel._meta.get_field('integer')),
            'NULL', ())


class BulkUpdateQuerySetIntegrationTests(DjangoTestCase):
    @classmethod
    def setUpTestData(cls):
        cls.model1 = BulkUpdateQuerySetTestModel.objects.create(integer=1, string='1')
        cls.model2 = BulkUpdateQuerySetTestModel.objects.create(integer=2, string='2')
        cls.model3 = BulkUpdateQuerySetTestModel.objects.create(integer=3, string='3')

    def test_bulk_update_a_single_field(self):
        self.model1.integer = 0
        self.model2.integer = 0
        self.model3.integer = 0
        self.model1.string = 'one'
        self.model2.string = 'two'
        self.model3.string = 'three'
        self.model2.boolean = True
        self.model1.boolean = True
        self.model3.boolean = True
        BulkUpdateQuerySetTestModel.objects.bulk_update(
            [self.model1, self.model2, self.model3],
            update_fields=['string'])

        self.assertQuerysetEqual(
            BulkUpdateQuerySetTestModel.objects.order_by('pk'),
            [(1, 1, 'one', False), (2, 2, 'two', False), (3, 3, 'three', False)],
            transform=attrgetter('pk', 'integer', 'string', 'boolean'))

    def test_bulk_update_multiple_fields(self):
        self.model1.integer = 3
        self.model2.integer = 2
        self.model3.integer = 1
        self.model1.string = 'three'
        self.model2.string = 'two'
        self.model3.string = 'one'
        self.model2.boolean = True
        self.model1.boolean = True
        self.model3.boolean = True
        BulkUpdateQuerySetTestModel.objects.bulk_update(
            [self.model1, self.model2, self.model3],
            update_fields=['integer', 'string'])

        self.assertQuerysetEqual(
            BulkUpdateQuerySetTestModel.objects.order_by('pk'),
            [(1, 3, 'three', False), (2, 2, 'two', False), (3, 1, 'one', False)],
            transform=attrgetter('pk', 'integer', 'string', 'boolean'))

    def test_bulk_update_all_fields(self):
        self.model1.integer = 3
        self.model2.integer = 2
        self.model3.integer = 1
        self.model1.string = 'three'
        self.model2.string = 'two'
        self.model3.string = 'one'
        self.model2.boolean = True
        self.model1.boolean = True
        self.model3.boolean = True
        BulkUpdateQuerySetTestModel.objects.bulk_update(
            [self.model1, self.model2, self.model3])

        self.assertQuerysetEqual(
            BulkUpdateQuerySetTestModel.objects.order_by('pk'),
            [(1, 3, 'three', True), (2, 2, 'two', True), (3, 1, 'one', True)],
            transform=attrgetter('pk', 'integer', 'string', 'boolean'))
