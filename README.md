# Django Case Expressions

This is a prototype implementation of SQL case expressions for the Django ORM
using the new [Query Expressions API](https://docs.djangoproject.com/en/dev/ref/models/expressions/)
as described in the [proposal](https://groups.google.com/d/msg/django-developers/a5ADv59TkBQ/euX_L0dab-4J).

**This is pre-alpha code. It is probably not feature complete and the API has
not been finalized.**

## Usage

### Searched case expression

The general form CASE expression (called a searched case in the SQL spec),
which evaluates to the value provided for the first condition that is true.

```python
from case_expressions.models.expressions import Case

Case([(Q(value__gt=0), "positive"),
      (Q(value__lt=0), "negative")],
     default="zero",
     output_field=CharField())
```

Which generates SQL like this:

```sql
CASE WHEN value > 0 THEN 'positive' 
WHEN value < 0 THEN 'negative' 
ELSE 'zero' END
```

### Simple case expression

A simple case in the SQL spec. An operand is tested for equality with the
condition values, and the result value for the first matching condition value
is returned.

```python
from case_expressions.models.expressions import SimpleCase

SimpleCase('value', [(1, "one"), (2, "two")],
           default='I cannot count that high',
           output_field=CharField())
```

Which generates SQL like this:

```sql
CASE value
WHEN 1 THEN 'one' 
WHEN 2 THEN 'two' 
ELSE 'I cannot count that high' END
```

### Conditional annotation

You can use a `Case` of `SimpleCase` object in an annotation to create a
conditional annotation.

The following example annotates the returned model instances with a
`status_text` attribute that has a human readable version of the `status`
column's value.

```python
from case_expressions.models.expressions import SimpleCase

MyModel.objects.annotate(
    status_text=SimpleCase(
        'status',
        [('S', 'Started'), ('R', 'Running'), ('F', 'Finished')],
        default='Unknown',
        output_field=CharField()))
```

### Conditional aggregation

`Case` and `SimpleCase` can be used in an aggregate function.
 
The following example creates two aggregates. One the sum of `value` columns
of rows for which the `payed` column is true, and the other the sum over rows
for which the `payed` column is false.

```python
from case_expressions.models.expressions import SimpleCase

MyModel.objects.aggregate(
    total_payed=Sum(SimpleCase(
        'payed', [('True', F('value'))], default=0,
        output_field=IntegerField())),
    total_outstanding=Sum(SimpleCase(
        'payed', [('False', F('value'))], default=0,
        output_field=IntegerField())))
```

### Conditional update

`Case` and `SimpleCase` can also be used in `QuerySet.update`.

The following example increments all `value` columns by an amount that depends
on the value of the `type` column.

```python
MyModel.objects.update(
    value=Case([(Q(type='A'), F('value') + 4,
                (Q(type='B'), F('value') + 2],
               default=F('value') + 1,
               output_field=MyModel._meta.get_field('value')))
```

### Bulk update

There is also a subclass of `QuerySet` called `BulkUpdateQuerySet` which
exposes a `bulk_update` method for saving multiple model instances using a
single `UPDATE` query.

```python
from django.db import models
from case_expressions.models.query import BulkUpdateQuerySet

class MyModel(models.Model):
    ...
    objects = models.Manager.from_queryset(BulkUpdateQuerySet)()

instances = MyModel.objects.all()
for i, instance in enumerate(instances):
    instance.value = i

MyModel.objects.bulk_update(instances, update_fields=['value'])
```

## Running tests

```bash
$ cd case_expressions/tests
$ python runtests.py
```
