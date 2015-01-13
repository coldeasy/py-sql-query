from unittest import TestCase

from sqlquery.queryapi import COUNT, AND, OR, XOR, ASC, DESC
from sqlquery.queryapi import InvalidQueryException
from sqlquery._querybuilder import QueryBuilder, serialize_query_tokens


class SQLCompilerSelectTestCase(TestCase):
    def setUp(self):
        self.builder = QueryBuilder()

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
            ("SELECT COUNT(`1`) FROM `table` AS `a`", []),
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

    def test__generate_select_no_element_raises(self):
        with self.assertRaises(InvalidQueryException):
            self.builder.select().on_table(
                "table"
            ).sql()


class SQLCompilerWhereTestCase(TestCase):
    def setUp(self):
        self.builder = QueryBuilder()
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
        for (op, sql_op) in self.op_mapping.iteritems():
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


class SQLCompilerHavingTestCase(TestCase):
    # Most of having functionality is already covered by `where` cases
    def setUp(self):
        self.builder = QueryBuilder()
        self.basic_select = self.builder.select("test").on_table("table")

    def test__generate_having_aggregate_func(self):
        compiler = self.basic_select.where(
            OR(("test1__is", None), ("test2__is", None))
        ).group_by("test1").having(("test1__count__gt", 1)).compiler()

        sql, args = compiler._generate_having()

        self.assertEqual(
            "HAVING (COUNT(`a`.`test1`) > %s)",
            serialize_query_tokens(sql)
        )
        self.assertEqual(args, [1])


class SQLCompilerInsertTestCase(TestCase):
    def setUp(self):
        self.builder = QueryBuilder()

    def _iter_each_insert_fun(self):
        for (fun, query) in [
            (self.builder.insert, "INSERT"),
            (self.builder.insert_ignore, "INSERT IGNORE"),
            (self.builder.replace, "REPLACE"),
        ]:
            yield fun, query

    def test__generate_insert_single_row(self):
        for fun, query in self._iter_each_insert_fun():
            compiler = fun(test=1, test2=2).on_table("table").compiler()

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
                test=5, test2=6
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


class SQLCompilerUpdateTestCase(TestCase):
    def setUp(self):
        self.builder = QueryBuilder()

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


class SQLCompilerOffsetTestCase(TestCase):
    def setUp(self):
        self.builder = QueryBuilder()

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


class SQLCompilerLimitTestCase(TestCase):
    def setUp(self):
        self.builder = QueryBuilder()

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


class SQLCompilerOrderByTestCase(TestCase):
    def setUp(self):
        self.builder = QueryBuilder()

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


class SQLCompilerGroupByTestCase(TestCase):
    def setUp(self):
        self.builder = QueryBuilder()

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


class SQLCompilerCompositeTestCase(TestCase):
    def setUp(self):
        self.builder = QueryBuilder()

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


class SQLCompilerJoinTestCase(TestCase):
    def setUp(self):
        self.builder = QueryBuilder()

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
            "test", "test2", "table2.field1"
        ).on_table("table").join(
            "table2",
            "field1",
        ).where(
            ('test__eq', 1), ('test2__eq', 2)
        ).limit(10).offset(10).order_by("test").compiler()

        sql, args = compiler.sql()

        self.assertEqual(
            "SELECT `a`.`test`, `a`.`test2`, `b`.`field1` "
            "FROM `table` AS `a` "
            "INNER JOIN `table2` AS `b` ON `a`.`field1` = `b`.`field1` "
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
            "test", "test2", "table2.field1"
        ).on_table("table").join(
            "table2",
            "field1",
        ).where(
            ('test__eq', 1), ('test2__eq', 2), ('table2.field1__eq', 'mont')
        ).limit(10).offset(10).order_by("test").compiler()

        sql, args = compiler.sql()

        self.assertEqual(
            "SELECT `a`.`test`, `a`.`test2`, `b`.`field1` "
            "FROM `table` AS `a` "
            "INNER JOIN `table2` AS `b` ON `a`.`field1` = `b`.`field1` "
            "WHERE (`a`.`test` <=> %s) AND (`a`.`test2` <=> %s) "
            "AND (`b`.`field1` <=> %s) "
            "ORDER BY `a`.`test` OFFSET %s LIMIT %s",
            sql
        )
        self.assertEqual(
            (1, 2, 'mont', 10, 10),
            args
        )
