"""
"""
from sqlquery import _querybuilder


SQLFunction = _querybuilder.SQLFunction
InvalidQueryException = _querybuilder.InvalidQueryException


def AND(*conditions):
    """
    Similar to the :py:func:`XOR` function except the boolean operator is `AND`
    """
    assert conditions
    return _querybuilder.logical_and(conditions)


def OR(*conditions):
    """
    Similar to the :py:func:`XOR` function except the boolean operator is `OR`
    """
    assert conditions
    return _querybuilder.logical_or(conditions)


def XOR(*conditions):
    """
    Creates an XOR clause between all conditions, e.g. `x <> 1 XOR y <> 2`.
    *conditions* should be a list of column names.
    """
    assert conditions
    return _querybuilder.logical_xor(conditions)


def ASC(field):
    """
    Similar to the :py:func:`DESC` function except creates an ascending order
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
    *names* here should be a list of column names that will selected.
    """
    return _querybuilder.QueryBuilder().select(*names)


def update(**data):
    """
    Create an update clause, e.g. `UPDATE ...`
    *data* here should be a list of key value pairs that will used for
    updating.
    """
    return _querybuilder.QueryBuilder().update(**data)


def insert(*data, **col_values):
    """
    Create an insert clause, e.g. `INSERT INTO ...`
    If set, *col_values* should a all key/values that will be inserted as a
    row.
    If set, *data* should contain a list of dicts with all key/values that
    will be inserted as a row.
    E.g.
        k
    """
    return _querybuilder.QueryBuilder().insert(*data, **col_values)


def insert_ignore(*data, **col_values):
    """
    Create an insert ignore clause, e.g. `INSERT IGNORE INTO ...`
    See :py:func:`insert` for arguments
    """
    return _querybuilder.QueryBuilder().insert_ignore(*data, **col_values)


def replace(*data, **col_values):
    """
    Create a replace clause, e.g. `REPLACE INTO ...`
    See :py:func:`insert` for arguments
    """
    return _querybuilder.QueryBuilder().replace(*data, **col_values)


def delete():
    """
    Create a delete clause, e.g. `DELETE ...`
    """
    return _querybuilder.QueryBuilder().delete()
