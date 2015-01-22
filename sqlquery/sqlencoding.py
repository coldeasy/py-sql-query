import contextlib

from six import string_types


OPERATOR_MAPPING = {
    'eq': "<=>",
    'neq': "<>",
    'gte': ">=",
    'gt': ">",
    'lt': "<",
    'is': "IS",
    'isnot': "IS NOT",
    'like': "LIKE",
    'in': "IN",
}

FUNC_MAPPING = {
    'count': "COUNT",
    'avg': "AVG",
    'max': "MAX",
    'min': "MIN",
    'sum': "SUM",
    'utcnow': "UTC_TIMESTAMP",
    'unix_timestamp': "UNIX_TIMESTAMP",
}


SQL_NULL = "NULL"
SQL_ASC = "ASC"
SQL_DESC = "DESC"
SQL_AND = "AND"
SQL_OR = "OR"
SQL_XOR = "XOR"

SQL_JOIN_TYPE_OUTER = "OUTER JOIN"
SQL_JOIN_TYPE_INNER = "INNER JOIN"


class _Func(str):
    pass


@contextlib.contextmanager
def in_brackets(query):
    query.append("(")
    yield
    query.append(")")


def quoted(element):
    if element.startswith("`") and element.endswith("`"):
        return element
    return u"`{!s}`".format(element)


def encode_func_name(funcname):
    sql_func = FUNC_MAPPING.get(funcname, funcname)
    return _Func(sql_func)


def encode_field(field, table_name, table_alias, include_alias=True):
    if not include_alias or not isinstance(field, string_types):
        return quoted(field.value)

    prefix = table_alias
    if field.startswith(table_name + '.'):
        field = field[len(table_name + '.'):]
        prefix = table_alias

    return quoted(prefix) + '.' + quoted(field)


def encode_table_name(table_name, table_alias, table_schema,
                      include_alias=True):
    if table_schema:
        name = "{}.{}".format(quoted(table_schema), quoted(table_name))
    else:
        name = quoted(table_name)

    if not include_alias:
        return name

    return (
        name
        + " AS "
        + quoted(table_alias)
    )


def convert_op(op):
    return OPERATOR_MAPPING[op]


def convert_func(func):
    return FUNC_MAPPING[func]


def is_space(value):
    return len(value.strip()) == 0


def is_function(value):
    return isinstance(value, _Func)


def should_skip_next_space(token, next_token):
    return (
        is_function(token) or
        token.endswith("(") or
        next_token.startswith(")") or
        token.endswith(" ") or
        next_token.startswith(" ") or
        token.endswith(",") or
        next_token.startswith(",")
    )


def spaced_query(query):
    if not isinstance(query, list):
        query = list(query)

    final_index = len(query) - 1
    for index, token in enumerate(query):
        if is_space(token) and index in (0, final_index):
            continue

        yield token

        if index == final_index:
            continue

        if should_skip_next_space(token, query[index + 1]):
            continue

        yield " "


def serialize_query_tokens(query):
    return u"".join(map(str, spaced_query(query)))
