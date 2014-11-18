from __future__ import unicode_literals

from operator import attrgetter
from unittest import TestCase, skip

from django.db import connection, models
from django.db.backends.sqlite3.base import DatabaseWrapper
from django.db.models import F, Q, CharField
from django.db.models.sql.query import Query
from django.db.models.sql.compiler import SQLCompiler
from django.test import TestCase as DjangoTestCase
from .models import CaseTestModel, BulkUpdateQuerySetTestModel
from ..models.expressions import Case, SimpleCase, UpdateModelList


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


class CaseUnitTests(CaseExpressionTestCase):
    def test_values(self):
        self.assertGeneratedSqlEqual(
            Case([(Q(integer__gt=0), "positive"),
                  (Q(integer__lt=0), "negative"),
                  (Q(integer=0), "zero")],
                 output_field=CharField()),

            'CASE WHEN "tests_casetestmodel"."integer" > %s THEN %s '
            'WHEN "tests_casetestmodel"."integer" < %s THEN %s '
            'WHEN "tests_casetestmodel"."integer" = %s THEN %s END',

            [0, 'positive', 0, 'negative', 0, 'zero'])

    def test_values_with_default(self):
        self.assertGeneratedSqlEqual(
            Case([(Q(integer__gt=0), "positive"),
                  (Q(integer__lt=0), "negative")],
                 default="zero",
                 output_field=CharField()),

            'CASE WHEN "tests_casetestmodel"."integer" > %s THEN %s '
            'WHEN "tests_casetestmodel"."integer" < %s THEN %s '
            'ELSE %s END',

            [0, 'positive', 0, 'negative', 'zero'])

    def test_empty_values(self):
        self.assertGeneratedSqlEqual(Case(), 'NULL', ())

    def test_empty_values_with_default(self):
        self.assertGeneratedSqlEqual(
            Case(default="I cannot count", output_field=CharField()),
            '%s', ['I cannot count'])


class SimpleCaseUnitTests(CaseExpressionTestCase):
    def test_values(self):
        self.assertGeneratedSqlEqual(
            SimpleCase('integer', [(1, "one"), (2, "two"), (3, "three")],
                       output_field=CharField()),

            'CASE "tests_casetestmodel"."integer" '
            'WHEN %s THEN %s WHEN %s THEN %s WHEN %s THEN %s END',

            [1, 'one', 2, 'two', 3, 'three'])

    def test_values_with_default(self):
        self.assertGeneratedSqlEqual(
            SimpleCase('integer', [(1, "one"), (2, "two")],
                       default='I cannot count that high',
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
            SimpleCase('integer', default='I cannot count', output_field=CharField()),
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


class CaseExpressionIntegrationTests(DjangoTestCase):
    def setUp(self):
        self.model1 = CaseTestModel.objects.create(integer=1, string='1')
        self.model2 = CaseTestModel.objects.create(integer=2, string='2')
        self.model3 = CaseTestModel.objects.create(integer=3, string='3')

    def test_annotate(self):
        self.assertQuerysetEqual(
            CaseTestModel.objects.annotate(text=SimpleCase(
                'integer', [(1, 'one'), (2, 'two')],
                default='other',
                output_field=models.CharField())).order_by('pk'),
            [(1, 'one'), (2, 'two'), (3, 'other')],
            transform=attrgetter('id', 'text'))

    def test_annotate_with_F_object(self):
        self.assertQuerysetEqual(
            CaseTestModel.objects.annotate(f_test=SimpleCase(
                'integer',
                [(1, F('integer') + 1),
                 (2, F('integer') + 3)],
                default=F('integer'))).order_by('pk'),
            [(1, 2), (2, 5), (3, 3)],
            transform=attrgetter('id', 'f_test'))

    def test_aggregate(self):
        CaseTestModel.objects.create(integer=2, string='2')
        CaseTestModel.objects.create(integer=3, string='3')
        CaseTestModel.objects.create(integer=3, string='3')

        self.assertEqual(
            CaseTestModel.objects.aggregate(
                one=models.Sum(SimpleCase(
                    'integer', [(1, 1)],
                    default=0,
                    output_field=models.IntegerField())),
                two=models.Sum(SimpleCase(
                    'integer', [(2, 1)],
                    default=0,
                    output_field=models.IntegerField())),
                three=models.Sum(SimpleCase(
                    'integer', [(3, 1)],
                    default=0,
                    output_field=models.IntegerField()))
                ),
            {'one': 1, 'two': 2, 'three': 3})

    def test_aggregate_with_F_object(self):
        CaseTestModel.objects.create(integer=2, string='2')
        CaseTestModel.objects.create(integer=3, string='3')
        CaseTestModel.objects.create(integer=3, string='3')

        self.assertEqual(
            CaseTestModel.objects.aggregate(
                one=models.Sum(SimpleCase(
                    'integer', [(1, F('integer'))],
                    default=0)),
                two=models.Sum(SimpleCase(
                    'integer', [(2, F('integer') - 1)],
                    default=0)),
                three=models.Sum(SimpleCase(
                    'integer', [(3, F('integer') + 1)],
                    default=0))
                ),
            {'one': 1, 'two': 2, 'three': 12})

    def test_update(self):
        CaseTestModel.objects.update(
            string=Case([(Q(integer__lt=2), 'less than 2'),
                         (Q(integer__gt=2), 'greater than 2')],
                        default='equal to 2',
                        output_field=CaseTestModel._meta.get_field('string')))

        self.assertQuerysetEqual(
            CaseTestModel.objects.all().order_by('pk'),
            [(1, 'less than 2'), (2, 'equal to 2'), (3, 'greater than 2')],
            transform=attrgetter('id', 'string'))

    def test_update_with_F_object(self):
        CaseTestModel.objects.update(
            integer=Case([(Q(integer__lt=2), F('integer') * -2),
                          (Q(integer__gt=2), F('integer') * 2)],
                         default=0))

        self.assertQuerysetEqual(
            CaseTestModel.objects.all().order_by('pk'),
            [(1, -2), (2, 0), (3, 6)],
            transform=attrgetter('id', 'integer'))


class BulkUpdateQuerySetIntegrationTests(DjangoTestCase):
    def setUp(self):
        self.model1 = BulkUpdateQuerySetTestModel.objects.create(integer=1, string='1')
        self.model2 = BulkUpdateQuerySetTestModel.objects.create(integer=2, string='2')
        self.model3 = BulkUpdateQuerySetTestModel.objects.create(integer=3, string='3')

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
