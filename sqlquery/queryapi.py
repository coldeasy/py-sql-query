"""
"""
from sqlquery import _querybuilder
from sqlquery._querybuilder import QueryBuilder


SQLFunction = _querybuilder.SQLFunction
InvalidQueryException = _querybuilder.InvalidQueryException


def AND(*conditions):
    """
    Similar to the :py:func:`.XOR` function except the boolean operator is `AND`
    """
    assert conditions
    return _querybuilder.logical_and(conditions)


def OR(*conditions):
    """
    Similar to the :py:func:`.XOR` function except the boolean operator is `OR`
    """
    assert conditions
    return _querybuilder.logical_or(conditions)


def XOR(*conditions):
    """
    Creates an XOR clause between all conditions, e.g. 

    ::

        x <> 1 XOR y <> 2

    *conditions* should be a list of column names.
    """
    assert conditions
    return _querybuilder.logical_xor(conditions)


def ASC(field):
    """
    Similar to the :py:func:`.DESC` function except creates an ascending order
    by clause.
    """
    assert field
    return _querybuilder.order_ascending(field)


def DESC(field):
    """
    Creates a descending order by clause on a field which can be used with the
    :py:meth:`.QueryBuilder.order_by` function.
    *field* is be a column name.
    """
    assert field
    return _querybuilder.order_descending(field)


def COUNT(field=None):
    """
    The count function for a select clause, e.g. `SELECT COUNT(x) ...`
    *field* is an optional argument which should be column name. If it is not
    present then `COUNT(1)` is generated
    """
    return _querybuilder.count(field)


def select(*names):
    """
    Create a select clause, e.g. `SELECT ...`
    Each value in *names* should be a string identifying a column.
    """
    return QueryBuilder().select(*names)


def update(**data):
    """
    Create an update clause, e.g. `UPDATE ...`

    Each key/value in *data* should represent a column/value that will be
    updated. The key should be a string, and value can/should be any type
    that is consumed by your DB client library.

    Note that no conversion is done on these values, so, for example, if you
    pass a `datetime` instance, then that will remain a `datetime` instance
    when you get back the list of arguments along with the query string in
    :py:meth:`.QueryBuilder.sql`.
    """
    return QueryBuilder().update(**data)


def insert(*data):
    """
    Create an insert clause, e.g. `INSERT INTO ...`

    Each value in *data* should be dictionary with key/values representing
    columns/values. The format of this should be the same as in
    :py:func:`.update`.
    """
    return QueryBuilder().insert(*data)


def insert_ignore(*data):
    """
    The same interface as :py:func:`.insert`, however a `INSERT IGNORE`
    statement is generated rather than an `INSERT`.
    """
    return QueryBuilder().insert_ignore(*data)


def replace(*data):
    """
    The same interface as :py:func:`.insert`, however a `REPLACE` statement
    is generated rather than an `INSERT`.
    """
    return QueryBuilder().replace(*data)


def delete():
    """
    Create a delete clause, e.g. `DELETE ...`
    """
    return QueryBuilder().delete()
