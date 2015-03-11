from sqlquery.sqlencoding import ANSIEncodings

from tests import BaseTestCase

serialize_query_tokens = ANSIEncodings().serialize_query_tokens


class ANSIEncodingsTestCase(BaseTestCase):
    def test__generate_select_single_element(self):
        compiler = self.builder.select("test").on_table("table").compiler(
            encoder=ANSIEncodings()
        )

        sql, args = compiler._generate_select()

        self.assertEqual(
            ('SELECT "a"."test" FROM "table" AS "a"', []),
            (serialize_query_tokens(sql), args)
        )
