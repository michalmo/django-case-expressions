from __future__ import unicode_literals

from django.db.models.expressions import ExpressionNode, F, Value
from django.db.models.query_utils import Q
from django.db.models.sql.where import WhereNode, AND


class BaseCaseExpression(ExpressionNode):
    class NoDefault:
        pass

    def __init__(self, values=None, default=NoDefault(), output_field=None):
        super(BaseCaseExpression, self).__init__(output_field)
        self.values = self.init_values(values)

        if isinstance(default, self.NoDefault):
            default = None
        elif not hasattr(default, 'resolve_expression'):
            # everything must be resolvable to an expression
            default = Value(default)

        self.default = default

    def init_values(self, values):
        raise NotImplementedError("Subclasses must implement init_values()")

    def get_source_expressions(self):
        source_expressions = [value for condition, value in self.values]
        if self.default is not None:
            source_expressions.append(self.default)
        return source_expressions

    def set_source_expressions(self, exprs):
        if self.default is not None:
            self.default = exprs[-1]
            exprs = exprs[:-1]
        self.values = [(condition, new_value) for new_value, (condition, value) in zip(exprs, self.values)]

    def resolve_expression(self, query=None, allow_joins=True, reuse=None, summarize=False):
        c = self.copy()
        c.is_summary = summarize
        values = []
        for condition, value in c.values:
            if hasattr(condition, 'resolve_expression'):
                condition = condition.resolve_expression(query, allow_joins, reuse, summarize)
            value = value.resolve_expression(query, allow_joins, reuse, summarize)
            values.append((condition, value))
        c.values = values
        if c.default is not None:
            c.default = c.default.resolve_expression(query, allow_joins, reuse, summarize)
        return c

    def predicate_sql(self, compiler, connection):
        raise NotImplementedError("Subclasses must implement predicate_sql()")

    def condition_sql(self, condition, compiler, connection):
        raise NotImplementedError("Subclasses must implement condition_sql()")

    def value_sql(self, value, compiler, connection):
        return value.as_sql(compiler, connection)

    def as_sql(self, compiler, connection):
        if not self.values:
            if self.default is not None:
                return self.default.as_sql(compiler, connection)
            return 'NULL', ()
        output_field = self.output_field
        predicate_sql, predicate_params = self.predicate_sql(compiler, connection)
        result = ['CASE']
        result_params = []
        if predicate_sql:
            result.append(predicate_sql)
        result_params.extend(predicate_params)
        for condition, value in self.values:
            condition_sql, condition_params = self.condition_sql(condition, compiler, connection)
            value_sql, value_params = self.value_sql(value, compiler, connection)
            result.append('WHEN %s THEN %s' % (condition_sql, value_sql))
            result_params.extend(condition_params)
            result_params.extend(value_params)
        if self.default is not None:
            default_sql, default_params = self.default.as_sql(compiler, connection)
            result.append('ELSE %s' % default_sql)
            result_params.extend(default_params)
        result.append('END')
        return (
            # cast the whole case expression if required
            connection.ops.field_cast_sql(
                output_field.db_type(connection),
                output_field.get_internal_type()) % ' '.join(result),
            result_params)


class Case(BaseCaseExpression):
    """
    An SQL searched CASE expression:

        CASE
            WHEN n > 0
                THEN 'positive'
            WHEN n < 0
                THEN 'negative'
            ELSE 'zero'
        END
    """
    def __init__(self, values=None, default=BaseCaseExpression.NoDefault(), output_field=None, where=WhereNode):
        super(Case, self).__init__(values, default, output_field)
        self.where_class = where

    def init_values(self, values):
        init_values = []

        if values:
            for condition, value in values:
                if not isinstance(condition, Q):
                    raise TypeError("Conditions must be Q objects.")
                if not hasattr(value, 'resolve_expression'):
                    # everything must be resolvable to an expression
                    value = Value(value)
                init_values.append((condition, value))

        return init_values

    def predicate_sql(self, compiler, connection):
        return '', ()

    def condition_sql(self, condition, compiler, connection):
        query = compiler.query
        clause, require_inner = query._add_q(condition, query.used_aliases)
        when = self.where_class()
        when.add(clause, AND)
        return compiler.compile(when)


class SimpleCase(BaseCaseExpression, F):
    """
    An SQL simple CASE expression:

        CASE n
            WHEN 1
                THEN 'one'
            WHEN 2
                THEN 'two'
            ELSE 'I cannot count that high'
        END
    """
    def __init__(self, name, values=None, default=BaseCaseExpression.NoDefault(), output_field=None):
        super(SimpleCase, self).__init__(values, default, output_field)
        F.__init__(self, name)
        self.col = None

    def init_values(self, values):
        init_values = []

        if values:
            for condition, value in values:
                if not hasattr(condition, 'resolve_expression'):
                    # everything must be resolvable to an expression
                    condition = Value(condition)
                if not hasattr(value, 'resolve_expression'):
                    # everything must be resolvable to an expression
                    value = Value(value)
                init_values.append((condition, value))

        return init_values

    def resolve_expression(self, query=None, allow_joins=True, reuse=None, summarize=False):
        c = super(SimpleCase, self).resolve_expression(query, allow_joins, reuse, summarize)
        c.col = F.resolve_expression(self, query, allow_joins, reuse, summarize)
        return c

    def predicate_sql(self, compiler, connection):
        return self.col.as_sql(compiler, connection)

    def condition_sql(self, condition, compiler, connection):
        return condition.as_sql(compiler, connection)


class UpdateModelList(ExpressionNode):
    """
    An expression representing multiple instances' values. Resolves to a
    SimpleCase expression.
    """
    def __init__(self, objects, output_field=None):
        super(UpdateModelList, self).__init__(output_field=output_field)
        self.objects = objects or []

    def resolve_expression(self, query=None, allow_joins=True, reuse=None, summarize=False):
        c = SimpleCase('pk', output_field=self.output_field)
        c = c.resolve_expression(query, allow_joins, reuse, summarize)

        # bypass resolving all these values, since it slows down the query
        # generation by a factor of two
        def create_resolved_case_tuple(obj, attname=self._output_field.attname):
            value = getattr(obj, attname)
            if hasattr(value, 'resolve_expression'):
                return Value(obj.pk), value.resolve_expression(query, allow_joins, reuse, summarize)
            else:
                return Value(obj.pk), Value(value)

        c.values = list(map(create_resolved_case_tuple, self.objects))
        return c
