from collections import OrderedDict
from mock import patch
from unittest import TestCase

from sqlquery import queryapi
from sqlquery.queryapi import COUNT, AND, OR, XOR, ASC, DESC
from sqlquery.queryapi import InvalidQueryException
from sqlquery._querybuilder import QueryBuilder
from sqlquery.sqlencoding import BasicEncodings


serialize_query_tokens = BasicEncodings().serialize_query_tokens


def _ordered_dict_from_dict(unordered_dict):
    ordered = OrderedDict()
    for key in sorted(unordered_dict.keys()):
        ordered[key] = unordered_dict[key]

    return ordered


def _ordered_copy(self, **kwargs):
    if 'update' in kwargs:
        kwargs['update'] = _ordered_dict_from_dict(kwargs['update'])
    if 'insert' in kwargs:
        kwargs['insert'] = [
            _ordered_dict_from_dict(row)
            for row in kwargs['insert']
        ]

    for list_entry in ('select', 'having', 'group_by', 'where'):
        if list_entry in kwargs:
            if isinstance(kwargs[list_entry], tuple):
                kwargs[list_entry] = tuple(sorted(kwargs[list_entry]))
            elif isinstance(kwargs[list_entry], list):
                kwargs[list_entry] = sorted(kwargs[list_entry])
            else:
                kwargs[list_entry] = kwargs[list_entry]

    return QueryBuilder(self._query_data._replace(**kwargs))


class _BaseTestCase(TestCase):
    def setUp(self):
        self._patch_start_query_builder_replace()
        self.builder = QueryBuilder()

    def tearDown(self):
        self.__patched.__exit__()

    def _patch_start_query_builder_replace(self):
        self.__patched = patch.object(QueryBuilder, '_replace', _ordered_copy)
        self.__patched.__enter__()


class SQLCompilerSelectTestCase(_BaseTestCase):
    def test__generate_select_single_element(self):
        compiler = self.builder.select("test").on_table("table").compiler()

        sql, args = compiler._generate_select()

        self.assertEqual(
            ("SELECT `a`.`test` FROM `table` AS `a`", []),
            (serialize_query_tokens(sql), args)
        )

    def test__generate_select_multiple_element(self):
        compiler = self.builder.select(
            "test", "test2", "test3"
        ).on_table("table").compiler()

        sql, args = compiler._generate_select()

        self.assertEqual(
            ("SELECT `a`.`test`, `a`.`test2`, `a`.`test3` "
             "FROM `table` AS `a`", []),
            (serialize_query_tokens(sql), args)
        )

    def test__generate_select_count(self):
        compiler = self.builder.select(COUNT()).on_table("table").compiler()

        sql, args = compiler._generate_select()

        self.assertEqual(
            ("SELECT COUNT(1) FROM `table` AS `a`", []),
            (serialize_query_tokens(sql), args)
        )

    def test__generate_select_count_field(self):
        compiler = self.builder.select(
            COUNT('users')
        ).on_table("table").compiler()

        sql, args = compiler._generate_select()

        self.assertEqual(
            ("SELECT COUNT(`a`.`users`) FROM `table` AS `a`", []),
            (serialize_query_tokens(sql), args)
        )

    def test__generate_select_with_schema(self):
        compiler = self.builder.select(
            'users'
        ).on_table("table", schema='myschema').compiler()

        sql, args = compiler._generate_select()

        self.assertEqual(
            ("SELECT `a`.`users` FROM `myschema`.`table` AS `a`", []),
            (serialize_query_tokens(sql), args)
        )

    def test__generate_select_no_element_raises(self):
        with self.assertRaises(InvalidQueryException):
            self.builder.select().on_table(
                "table"
            ).sql()


class SQLCompilerFuncsTestCase(_BaseTestCase):
    def test_aggregate_funcs(self):
        for func in ("COUNT", "MAX", "MIN", "SUM"):
            sql, args = self.builder.select(
                getattr(queryapi, func)("test")
            ).on_table("table").sql()

            self.assertEqual(
                "SELECT {}(`a`.`test`) FROM `table` AS `a`".format(func),
                sql
            )
            self.assertEqual(
                (),
                args
            )

    def test_scalar_funcs(self):
        for (func, sqlfunc) in (
            ("UTCNOW", "UTC_TIMESTAMP"),
            ("UNIX_TIMESTAMP", "UNIX_TIMESTAMP"),
        ):
            sql, args = self.builder.select(
                getattr(queryapi, func)()
            ).on_table("table").sql()

            self.assertEqual(
                "SELECT {}() FROM `table` AS `a`".format(sqlfunc),
                sql
            )
            self.assertEqual(
                (),
                args
            )


class SQLCompilerWhereTestCase(_BaseTestCase):
    def setUp(self):
        super(SQLCompilerWhereTestCase, self).setUp()
        self.basic_select = self.builder.select("test").on_table("table")
        self.op_mapping = {
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

    def test__generate_all_where_ops(self):
        for (op, sql_op) in self.op_mapping.items():
            compiler = self.basic_select.where(
                ("test1__" + op, 2)
            ).compiler()

            sql, args = compiler._generate_where()

            self.assertEqual(
                "WHERE (`a`.`test1` {} %s)".format(sql_op),
                serialize_query_tokens(sql)
            )
            self.assertEqual(
                [2],
                args
            )

    def test__generate_where_in_literals(self):
        compiler = self.basic_select.where(
            ("test1__in", [1, 2, 3])
        ).compiler()

        sql, args = compiler._generate_where()

        self.assertEqual(
            "WHERE (`a`.`test1` IN (%s,%s,%s))",
            serialize_query_tokens(sql)
        )
        self.assertEqual(args, [1, 2, 3])

    def test__generate_where_in_query(self):
        compiler = self.basic_select.where(
            (
                "test1__in",
                self.builder.select("id").on_table("table2").where(
                    ("test2__eq", 4)
                )
            )
        ).compiler()

        sql, args = compiler._generate_where()

        self.assertEqual(
            "WHERE (`a`.`test1` IN (SELECT `b`.`id` FROM `table2` AS `b` "
            "WHERE (`b`.`test2` <=> %s)))",
            serialize_query_tokens(sql)
        )
        self.assertEqual(args, [4])

    def test__generate_is_null(self):
        for (op, sql_op) in {"is": "IS NULL", "isnot": "IS NOT NULL"}.items():
            compiler = self.basic_select.where(
                ("test1__" + op, None)
            ).compiler()

            sql, args = compiler._generate_where()

            self.assertEqual(
                "WHERE (`a`.`test1` {})".format(sql_op),
                serialize_query_tokens(sql)
            )
            self.assertEqual(args, [])

    def test__generate_where_multiple_anded_simple(self):
        compiler = self.basic_select.where(
            ('test1__eq', 1),
            ('test2__eq', 2)
        ).compiler()

        sql, args = compiler._generate_where()

        self.assertEqual(
            "WHERE (`a`.`test1` <=> %s) AND (`a`.`test2` <=> %s)",
            serialize_query_tokens(sql)
        )
        self.assertEqual(
            [1, 2],
            args
        )

    def test__generate_where_multiple_ored(self):
        compiler = self.basic_select.where(
            OR(('test1__eq', 1), ('test2__eq', 2))
        ).compiler()

        sql, args = compiler._generate_where()

        self.assertEqual(
            "WHERE ((`a`.`test1` <=> %s) OR (`a`.`test2` <=> %s))",
            serialize_query_tokens(sql)
        )
        self.assertEqual(
            [1, 2],
            args
        )

    def test__generate_where_multiple_anded(self):
        compiler = self.basic_select.where(
            AND(('test1__eq', 1), ('test2__eq', 2))
        ).compiler()

        sql, args = compiler._generate_where()

        self.assertEqual(
            "WHERE ((`a`.`test1` <=> %s) AND (`a`.`test2` <=> %s))",
            serialize_query_tokens(sql)
        )
        self.assertEqual(
            [1, 2],
            args
        )

    def test__generate_where_multiple_anded_one_ored(self):
        compiler = self.basic_select.where(
            AND(OR(('test1__eq', 1), ('test1__eq', 10)), ('test2__eq', 2))
        ).compiler()

        sql, args = compiler._generate_where()

        self.assertEqual(
            "WHERE (((`a`.`test1` <=> %s) OR (`a`.`test1` <=> %s)) "
            "AND (`a`.`test2` <=> %s))",
            serialize_query_tokens(sql)
        )
        self.assertEqual(
            [1, 10, 2],
            args
        )

    def test__generate_where_multiple_complex(self):
        compiler = self.basic_select.where(
            AND(
                OR(('test1__eq', 1), ('test1__eq', 10)),
                AND(
                    ('test3__eq', 1), ('test5__eq', 10),
                    OR(('test1__eq', 1), ('test1__eq', 10))
                ),
                XOR(('test3__eq', 1), ('test5__eq', 10))
            )
        ).compiler()

        sql, args = compiler._generate_where()

        self.assertEqual(
            "WHERE ("
            "((`a`.`test1` <=> %s) OR (`a`.`test1` <=> %s)) "
            "AND ((`a`.`test3` <=> %s) AND (`a`.`test5` <=> %s) "
            "AND ((`a`.`test1` <=> %s) OR (`a`.`test1` <=> %s))) "
            "AND ((`a`.`test3` <=> %s) XOR (`a`.`test5` <=> %s)))",
            serialize_query_tokens(sql)
        )
        self.assertEqual(
            [1, 10, 1, 10, 1, 10, 1, 10],
            args
        )


class SQLCompilerHavingTestCase(_BaseTestCase):
    # Most of having functionality is already covered by `where` cases
    def setUp(self):
        super(SQLCompilerHavingTestCase, self).setUp()
        self.basic_select = self.builder.select("test").on_table("table")

    def test__generate_having_aggregate_func(self):
        compiler = self.basic_select.where(
            OR(("test1__is", None), ("test2__is", None))
        ).group_by("test1").having((COUNT("test1"), "gt", 1)).compiler()

        sql, args = compiler._generate_having()

        self.assertEqual(
            "HAVING (COUNT(`a`.`test1`) > %s)",
            serialize_query_tokens(sql)
        )
        self.assertEqual(args, [1])


class SQLCompilerInsertTestCase(_BaseTestCase):
    def _iter_each_insert_fun(self):
        for (fun, query) in [
            (self.builder.insert, "INSERT"),
            (self.builder.insert_ignore, "INSERT IGNORE"),
            (self.builder.replace, "REPLACE"),
        ]:
            yield fun, query

    def test__generate_insert_single_row(self):
        for fun, query in self._iter_each_insert_fun():
            compiler = fun(dict(test=1, test2=2)).on_table("table").compiler()

            sql, args = compiler._generate_insert()

            self.assertEqual(
                query + " INTO `table` (`test`, `test2`) VALUES (%s, %s)",
                serialize_query_tokens(sql)
            )
            self.assertEqual(
                [1, 2],
                args
            )

    def test__generate_insert_multiple_rows(self):
        for fun, query in self._iter_each_insert_fun():
            compiler = fun(
                dict(test=1, test2=2),
                dict(test=3, test2=4),
                dict(test=5, test2=6),
            ).on_table("table").compiler()

            sql, args = compiler._generate_insert()

            self.assertEqual(
                query + " INTO `table` (`test`, `test2`) VALUES "
                "(%s, %s) (%s, %s) (%s, %s)",
                serialize_query_tokens(sql)
            )
            self.assertEqual(
                [1, 2, 3, 4, 5, 6],
                args
            )

    def test__generate_insert_both_arg_and_kwarg(self):
        for fun, query in self._iter_each_insert_fun():
            compiler = fun(
                dict(test=1, test2=2),
                dict(test=5, test2=6)
            ).on_table("table").compiler()

            sql, args = compiler._generate_insert()

            self.assertEqual(
                query + " INTO `table` (`test`, `test2`) VALUES "
                "(%s, %s) (%s, %s)",
                serialize_query_tokens(sql)
            )
            self.assertEqual(
                [1, 2, 5, 6],
                args
            )


class SQLCompilerUpdateTestCase(_BaseTestCase):
    def test__generate_update_single_field(self):
        compiler = self.builder.update(
            test=1
        ).on_table("table").compiler()

        sql, args = compiler._generate_update()

        self.assertEqual(
            "UPDATE `table` AS `a` SET `a`.`test` = %s",
            serialize_query_tokens(sql)
        )
        self.assertEqual(
            [1],
            args
        )

    def test__generate_update_multiple_fields(self):
        compiler = self.builder.update(
            test=1, test2=2
        ).on_table("table").compiler()

        sql, args = compiler._generate_update()

        self.assertEqual(
            "UPDATE `table` AS `a` SET `a`.`test` = %s, `a`.`test2` = %s",
            serialize_query_tokens(sql)
        )
        self.assertEqual(
            [1, 2],
            args
        )


class SQLCompilerOffsetTestCase(_BaseTestCase):
    def test__generate_offset(self):
        compiler = self.builder.select(
            "test"
        ).on_table("table").offset(10).compiler()

        sql, args = compiler._generate_offset()

        self.assertEqual(
            "OFFSET %s",
            serialize_query_tokens(sql)
        )
        self.assertEqual(
            [10],
            args
        )


class SQLCompilerLimitTestCase(_BaseTestCase):
    def test__generate_limit(self):
        compiler = self.builder.select(
            "test"
        ).on_table("table").limit(10).compiler()

        sql, args = compiler._generate_limit()

        self.assertEqual(
            "LIMIT %s",
            serialize_query_tokens(sql)
        )
        self.assertEqual(
            [10],
            args
        )


class SQLCompilerOrderByTestCase(_BaseTestCase):
    def test__generate_order_by(self):
        compiler = self.builder.select(
            "test"
        ).on_table("table").order_by("test").compiler()

        sql, args = compiler._generate_order_by()

        self.assertEqual(
            "ORDER BY `a`.`test`",
            serialize_query_tokens(sql)
        )
        self.assertEqual(args, [])

    def test__generate_order_by_multiple(self):
        compiler = self.builder.select(
            "test"
        ).on_table("table").order_by("test", "test2").compiler()

        sql, args = compiler._generate_order_by()

        self.assertEqual(
            "ORDER BY `a`.`test`, `a`.`test2`",
            serialize_query_tokens(sql)
        )
        self.assertEqual(args, [])

    def test__generate_order_by_directional(self):
        for (dir_fun, sql_dir) in [(ASC, "ASC"), (DESC, "DESC")]:
            compiler = self.builder.select(
                "test"
            ).on_table("table").order_by(
                dir_fun("test"), dir_fun("test2")
            ).compiler()

            sql, args = compiler._generate_order_by()

            self.assertEqual(
                "ORDER BY `a`.`test` {0}, `a`.`test2` {0}".format(sql_dir),
                serialize_query_tokens(sql)
            )
            self.assertEqual(args, [])

    def test__generate_order_by_one_asc_one_desc(self):
        compiler = self.builder.select(
            "test"
        ).on_table("table").order_by(
            DESC("test"), ASC("test2")
        ).compiler()

        sql, args = compiler._generate_order_by()

        self.assertEqual(
            "ORDER BY `a`.`test` DESC, `a`.`test2` ASC",
            serialize_query_tokens(sql)
        )
        self.assertEqual(args, [])


class SQLCompilerGroupByTestCase(_BaseTestCase):
    def test__generate_group_by(self):
        compiler = self.builder.select(
            "test"
        ).on_table("table").group_by("test").compiler()

        sql, args = compiler._generate_group_by()

        self.assertEqual(
            "GROUP BY `a`.`test`",
            serialize_query_tokens(sql)
        )
        self.assertEqual(args, [])

    def test__generate_group_by_multiple(self):
        compiler = self.builder.select(
            "test"
        ).on_table("table").group_by("test", "test2").compiler()

        sql, args = compiler._generate_group_by()

        self.assertEqual(
            "GROUP BY `a`.`test`, `a`.`test2`",
            serialize_query_tokens(sql)
        )
        self.assertEqual(args, [])


class SQLCompilerCompositeTestCase(_BaseTestCase):
    def test__generate_full_select_query_ordered_by(self):
        compiler = self.builder.select(
            "test"
        ).on_table("table").where(
            ('test__eq', 1), ('test2__eq', 2)
        ).limit(10).offset(10).order_by("test").compiler()

        sql, args = compiler.sql()

        self.assertEqual(
            "SELECT `a`.`test` FROM `table` AS `a` WHERE (`a`.`test` <=> %s) "
            "AND (`a`.`test2` <=> %s) ORDER BY `a`.`test` OFFSET %s LIMIT %s",
            sql
        )
        self.assertEqual(
            (1, 2, 10, 10),
            args
        )

    def test__generate_full_select_query_having_group_by(self):
        compiler = self.builder.select(
            "test"
        ).on_table("table").where(
            ('test__eq', 1), ('test2__eq', 2)
        ).limit(10).offset(10).group_by("test").having(
            ('test2__eq', 2)
        ).compiler()

        sql, args = compiler.sql()

        self.assertEqual(
            "SELECT `a`.`test` FROM `table` AS `a` WHERE (`a`.`test` <=> %s) "
            "AND (`a`.`test2` <=> %s) GROUP BY `a`.`test` "
            "HAVING (`a`.`test2` <=> %s) OFFSET %s LIMIT %s",
            sql
        )
        self.assertEqual(
            # Having occurs before limit/offset
            (1, 2, 2, 10, 10),
            args
        )


class SQLCompilerJoinTestCase(_BaseTestCase):
    def test__generate_full_join_simple_select(self):
        compiler = self.builder.select(
            "test", "test2"
        ).on_table("table").join(
            "table2",
            "field1",
        ).where(
            ('test__eq', 1), ('test2__eq', 2)
        ).limit(10).offset(10).order_by("test").compiler()

        sql, args = compiler.sql()

        self.assertEqual(
            "SELECT `a`.`test`, `a`.`test2` FROM `table` AS `a` "
            "INNER JOIN `table2` AS `b` ON `a`.`field1` = `b`.`field1` "
            "WHERE (`a`.`test` <=> %s) AND (`a`.`test2` <=> %s) "
            "ORDER BY `a`.`test` OFFSET %s LIMIT %s",
            sql
        )
        self.assertEqual(
            (1, 2, 10, 10),
            args
        )

    def test__generate_full_join_complex_select(self):
        compiler = self.builder.select(
            "test", "test2", "z_other.field1"
        ).on_table("table").join(
            "z_other",
            "field1",
        ).where(
            ('test__eq', 1), ('test2__eq', 2)
        ).limit(10).offset(10).order_by("test").compiler()

        sql, args = compiler.sql()

        self.assertEqual(
            "SELECT `a`.`test`, `a`.`test2`, `b`.`field1` "
            "FROM `table` AS `a` "
            "INNER JOIN `z_other` AS `b` ON `a`.`field1` = `b`.`field1` "
            "WHERE (`a`.`test` <=> %s) AND (`a`.`test2` <=> %s) "
            "ORDER BY `a`.`test` OFFSET %s LIMIT %s",
            sql
        )
        self.assertEqual(
            (1, 2, 10, 10),
            args
        )

    def test__generate_full_join_complex_select_complex_where(self):
        compiler = self.builder.select(
            "test", "test2", "z_other.field1"
        ).on_table("table").join(
            "z_other",
            "field1",
        ).where(
            ('test__eq', 1), ('test2__eq', 2), ('z_other.field1__eq', 'mont')
        ).limit(10).offset(10).order_by("test").compiler()

        sql, args = compiler.sql()

        self.assertEqual(
            "SELECT `a`.`test`, `a`.`test2`, `b`.`field1` "
            "FROM `table` AS `a` "
            "INNER JOIN `z_other` AS `b` ON `a`.`field1` = `b`.`field1` "
            "WHERE (`a`.`test` <=> %s) AND (`a`.`test2` <=> %s) "
            "AND (`b`.`field1` <=> %s) "
            "ORDER BY `a`.`test` OFFSET %s LIMIT %s",
            sql
        )
        self.assertEqual(
            (1, 2, 'mont', 10, 10),
            args
        )
