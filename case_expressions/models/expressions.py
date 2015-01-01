from __future__ import unicode_literals

from django.db.models.expressions import ExpressionNode, SimpleCase, Value


class UpdateModelList(ExpressionNode):
    """
    An expression representing multiple instances' values. Resolves to a
    SimpleCase expression.
    """
    def __init__(self, objects, output_field=None):
        super(UpdateModelList, self).__init__(output_field=output_field)
        self.objects = objects or []

    def resolve_expression(self, query=None, allow_joins=True, reuse=None, summarize=False):
        c = SimpleCase('pk')
        c = c.resolve_expression(query, allow_joins, reuse, summarize)

        # bypass resolving all these values, since it slows down the query
        # generation by a factor of two
        def create_resolved_case_tuple(obj, output_field=self.output_field, attname=self.output_field.attname):
            value = getattr(obj, attname)
            if hasattr(value, 'resolve_expression'):
                return Value(obj.pk), value.resolve_expression(query, allow_joins, reuse, summarize)
            else:
                return Value(obj.pk), Value(value, output_field=output_field)

        c.cases = list(map(create_resolved_case_tuple, self.objects))
        return c
