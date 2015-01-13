import string
import itertools
import collections
from collections import namedtuple
from sqlquery.sqlencoding import (
    serialize_query_tokens,
    in_brackets,
    quoted,
    encode_field,
    encode_func,
    encode_table_name,
    convert_op,
    convert_func,
    SQL_NULL,
    SQL_ASC,
    SQL_DESC,
    SQL_AND,
    SQL_OR,
    SQL_XOR,
    SQL_JOIN_TYPE_INNER
)


class InvalidQueryException(Exception):
    pass


def logical_and(conditions):
    return _LogicalOperator(conditions, SQL_AND)


def logical_or(conditions):
    return _LogicalOperator(conditions, SQL_OR)


def logical_xor(conditions):
    return _LogicalOperator(conditions, SQL_XOR)


def count(field):
    return SQLFunction(convert_func("count"), field or Scalar('1'))


def order_descending(field):
    return _SQLOrdering(field, SQL_DESC)


def order_ascending(field):
    return _SQLOrdering(field, SQL_ASC)


class Scalar(object):
    def __init__(self, value):
        self.value = value

    def __str__(self):
        return self.value

    def __repr__(self):
        return self.value


class _LogicalOperator(object):
    def __init__(self, conditions, operator):
        self.conditions = conditions
        self.operator = operator


class SQLFunction(object):
    def __init__(self, function, *fields):
        self.function = function
        self.fields = fields


class _SQLOrdering(object):
    def __init__(self, field, direction):
        self.field = field
        self.direction = direction


JoinOptions = namedtuple(
    'JoinOptions',
    [
        'join_type',
        'main_field',
        'join_field',
        'table',
        'table_alias'
    ]
)


QueryData = namedtuple(
    'QueryData',
    [
        'select',
        'update',
        'insert',
        'delete',
        'table',
        'where',
        'order_by',
        'group_by',
        'having',
        'offset',
        'limit',
        'duplicate_key_update',
        'insert_ignore',
        'insert_replace',
        'join',
        'table_alias',
    ]
)


_empty_query_data = QueryData(**{field: None for field in QueryData._fields})


class QueryBuilder(object):
    def __init__(self, query_data=None):
        self._query_data = query_data or _empty_query_data
        self._table_alias_gen = itertools.cycle(string.ascii_lowercase)
        self._query_data = self._query_data._replace(
            table_alias=(next(self._table_alias_gen))
        )

    def copy(self, new_query_data):
        """
        """
        return self.__class__(new_query_data)

    def _replace(self, **kwargs):
        return self.copy(self._query_data._replace(**kwargs))

    def select(self, *names):
        """
        """
        return self._replace(select=tuple(names))

    def update(self, **data):
        """
        """
        return self._replace(update=data)

    def replace(self, *data, **col_values):
        """
        """
        ret = self.insert(*data, **col_values)
        return ret._replace(insert_replace=True)

    def insert_ignore(self, *data, **col_values):
        """
        """
        ret = self.insert(*data, **col_values)
        return ret._replace(insert_ignore=True)

    def insert(self, *data, **col_values):
        """
        """
        assert all(isinstance(x, dict) for x in data)
        if col_values:
            data += (col_values,)
        return self._replace(insert=data)

    def delete(self):
        """
        """
        return self._replace(delete=True)

    def on_table(self, table):
        """
        """
        return self._replace(table=table)

    def on_duplicate_key_update(self, **col_values):
        """
        """
        return self._replace(duplicate_key_update=(True, col_values))

    def where(self, *conditions):
        """
        """
        assert conditions
        return self._replace(where=logical_and(conditions))

    def join(self, join_table, main_field, join_field=None):
        """
        Joins the current query with the given *join_table* on *join_field*
        which is a field on *join_table* and *main_field* which is a field
        represented on the current table.
        If *join_field* is not given then it uses *main_field* on the
        *join_table*.
        """
        return self._replace(
            join=JoinOptions(
                join_type=SQL_JOIN_TYPE_INNER,
                main_field=main_field,
                join_field=join_field or main_field,
                table=join_table,
                table_alias=next(self._table_alias_gen)
            )
        )

    def having(self, *conditions):
        """
        """
        assert conditions
        return self._replace(having=logical_and(conditions))

    def order_by(self, *fields):
        """
        """
        assert all(
            [isinstance(field, (basestring, _SQLOrdering))
             for field in fields]
        )
        return self._replace(order_by=tuple(fields))

    def group_by(self, *fields):
        """
        """
        assert all(
            [isinstance(field, basestring) for field in fields]
        )
        return self._replace(group_by=tuple(fields))

    def offset(self, count):
        """
        """
        return self._replace(offset=int(count))

    def limit(self, count):
        """
        """
        return self._replace(limit=int(count))

    def compiler(self):
        """
        """
        return SQLCompiler(self._query_data)

    def sql(self):
        """
        """
        return SQLCompiler(self._query_data).sql()


def _query_joiner(query, iterable, join_with=", "):
    for index, data in enumerate(iterable):
        yield data
        if len(iterable) > 1 and index < len(iterable) - 1:
            query.append(join_with)


class QueryString(list):
    def append(self, value, spaced_left=False, spaced_right=False):
        super(QueryString, self).append(value)


class SQLCompiler(object):
    def __init__(self, query_data):
        self.query_data = query_data

    def _encode_main_table_name(self, include_alias=True):
        return encode_table_name(
            self.query_data.table,
            self.query_data.table_alias,
            include_alias=include_alias
        )

    def _encode_join_table_name(self, include_alias=True):
        return encode_table_name(
            self.query_data.join.table,
            self.query_data.join.table_alias,
            include_alias=include_alias
        )

    def _encode_field(self, field, include_alias=True):
        return encode_field(
            field,
            self.query_data.table,
            self.query_data.table_alias,
            include_alias=include_alias
        )

    def _encode_join_field(self, field, include_alias=True):
        return encode_field(
            field,
            self.query_data.join.table,
            self.query_data.join.table_alias,
            include_alias=include_alias
        )

    def _smart_encode_field(self, field, include_alias=True):
        if (
            self.query_data.join and
            isinstance(field, basestring) and
            field.startswith(self.query_data.join.table + '.')
        ):
            return self._encode_join_field(field, include_alias=include_alias)

        return self._encode_field(field, include_alias=include_alias)

    def _generate_join(self):
        query = [
            self.query_data.join.join_type,
            self._encode_join_table_name(),
            u"ON",
            self._encode_field(self.query_data.join.main_field),
            u"=",
            self._encode_join_field(self.query_data.join.join_field),
        ]

        return query

    def _generate_update(self):
        query = [
            u"UPDATE",
            self._encode_main_table_name(),
        ]
        if self.query_data.join:
            query.extend(self._generate_join())

        query.append("SET")
        args = []

        for field in _query_joiner(query, self.query_data.update):
            query.append(self._encode_field(field))
            query.append(u"=")
            query.append(u"%s")
            args.append(self.query_data.update[field])

        return query, args

    def _generate_insert(self):
        if self.query_data.insert_ignore:
            insert = u"INSERT IGNORE INTO"
        elif self.query_data.insert_replace:
            insert = u"REPLACE INTO"
        else:
            insert = u"INSERT INTO"

        query = [
            insert,
            self._encode_main_table_name(include_alias=False),
        ]
        columns = self.query_data.insert[0].keys()
        with in_brackets(query):
            query.append(u", ".join(map(quoted, columns)))
        query.append(u"VALUES")

        args = []
        for col_values in self.query_data.insert:
            if len(col_values.keys()) != len(columns):
                raise InvalidQueryException("Invalid number of column values")

            with in_brackets(query):
                for col in _query_joiner(query, columns):
                    query.append(u"%s")
                    args.append(col_values[col])

        if self.query_data.duplicate_key_update:
            query.append(u"ON DUPLICATE KEY UPDATE")
            update_col_values = self.query_data.duplicate_key_update[1]
            if not update_col_values:
                for col in _query_joiner(query, columns):
                    query.append(u"{0}=VALUES({0})".format(quoted(col)))
            else:
                for col in _query_joiner(query, update_col_values):
                    query.append(u"{}=VALUES(%s)".format(quoted(col)))
                    args.append(update_col_values[col])

        return query, args

    def _generate_select(self):
        query = [u"SELECT"]
        for field in _query_joiner(query, self.query_data.select):
            if isinstance(field, SQLFunction):
                query.append(field.function)
                with in_brackets(query):
                    for field in _query_joiner(query, field.fields):
                        query.append(self._smart_encode_field(field))
            else:
                query.append(self._smart_encode_field(field))

        query.extend(["FROM", self._encode_main_table_name()])
        if self.query_data.join:
            query.extend(self._generate_join())
        return query, []

    @staticmethod
    def _parse_field_spec(field_spec):
        """
        Returns (func, field, op)
        """
        try:
            field, func, op = field_spec.split('__')
            return (func, field, op)
        except ValueError:
            try:
                field, op = field_spec.split('__')
                return (None, field, op)
            except ValueError:
                raise InvalidQueryException(
                    "Invalid where clause <{}>".format(field_spec)
                )

    def _generate_single_where_clause(self, field_op, value):
        func, field, op = self._parse_field_spec(field_op)

        if func:
            field = encode_func(func, self._smart_encode_field(field))
        else:
            field = self._smart_encode_field(field)

        clause = []
        with in_brackets(clause):
            clause.extend([field, convert_op(op)])
            args = [value]
            if (
                not isinstance(value, basestring) and
                isinstance(value, collections.Iterable)
            ):
                clause.append(u"({})".format(u",".join([u"%s"] * len(value))))
            elif value is None:
                clause.append(SQL_NULL)
                # we get rid of the value as it is represented as null
                del args[:]
            else:
                clause.append(u"%s")

        return clause, args

    def _generate_where_tableclause(self, clause):
        query, args = [], []
        for sub_clause in _query_joiner(
            query, clause.conditions, clause.operator
        ):
            if isinstance(sub_clause, _LogicalOperator):
                with in_brackets(query):
                    query2, args2 = self._generate_where_tableclause(
                        sub_clause
                    )
                    query.extend(query2)
                    args.extend(args2)
            else:
                if isinstance(sub_clause, (tuple, list)):
                    assert len(sub_clause) == 2
                    field_op, value = sub_clause
                elif isinstance(sub_clause, dict):
                    assert len(sub_clause) == 1
                    field_op, value = sub_clause.items()
                else:
                    raise InvalidQueryException("Unknown where element %s"
                                                % sub_clause)

                clause, clause_args = self._generate_single_where_clause(
                    field_op, value
                )
                query.extend(clause)
                args.extend(clause_args)

        return query, args

    def _generate_where(self):
        if not self.query_data.where:
            return [], []

        clause, args = self._generate_where_tableclause(self.query_data.where)
        clause.insert(0, u"WHERE")
        return clause, args

    def _generate_offset(self):
        if self.query_data.offset is None:
            return [], []

        return [u"OFFSET %s"], [self.query_data.offset]

    def _generate_limit(self):
        if self.query_data.limit is None:
            return [], []

        return [u"LIMIT %s"], [self.query_data.limit]

    def _generate_order_by(self):
        if not self.query_data.order_by:
            return [], []

        query = [u"ORDER BY"]
        for order_by in _query_joiner(query, self.query_data.order_by):
            if isinstance(order_by, basestring):
                query.append(self._smart_encode_field(order_by))
            else:
                query.extend(
                    [
                        self._smart_encode_field(order_by.field),
                        order_by.direction
                    ]
                )

        return query, []

    def _generate_group_by(self):
        if not self.query_data.group_by:
            return [], []

        query = [u"GROUP BY"]
        for field in _query_joiner(query, self.query_data.group_by):
            query.append(self._smart_encode_field(field))

        return query, []

    def _generate_having(self):
        if not self.query_data.having:
            return [], []

        clause, args = self._generate_where_tableclause(self.query_data.having)
        clause.insert(0, u"HAVING")
        return clause, args

    def _generate_query_operation(self):
        if self.query_data.select:
            return self._generate_select()

        if self.query_data.delete is True:
            return u"DELETE", []

        if self.query_data.update is not None:
            return self._generate_update()

        if self.query_data.insert is not None:
            return self._generate_insert()

        raise InvalidQueryException

    def sql(self):
        if not self.query_data.table:
            raise Exception("requires both select and from")

        main = self._generate_query_operation()
        where = self._generate_where()
        group_by = self._generate_group_by()
        having = self._generate_having()
        order_by = self._generate_order_by()
        offset = self._generate_offset()
        limit = self._generate_limit()

        sql, sql_args = zip(
            main, where, group_by, having, order_by, offset, limit
        )

        return (
            serialize_query_tokens(itertools.chain(*sql)),
            tuple(itertools.chain(*sql_args))
        )
