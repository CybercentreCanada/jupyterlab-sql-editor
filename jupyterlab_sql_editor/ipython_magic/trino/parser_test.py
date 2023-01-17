import unittest
from unittest import TestCase

from pyspark.sql.types import (
    ArrayType,
    BooleanType,
    LongType,
    MapType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)
from trino.parser import trino_column_lexer, trino_column_parser


class LexerTest(TestCase):
    def test_scalar(self):
        s = "varchar"
        trino_column_lexer.input(s)
        self.assertEqual(trino_column_lexer.token().type, "VARCHAR")

    def test_varchar_size(self):
        s = "varchar(32)"
        trino_column_lexer.input(s)
        self.assertEqual(trino_column_lexer.token().type, "VARCHAR")
        self.assertEqual(trino_column_lexer.token().type, "LPAREN")
        self.assertEqual(trino_column_lexer.token().type, "NUMBER")
        self.assertEqual(trino_column_lexer.token().type, "RPAREN")

    def test_timestamp_with_zone(self):
        s = "timestamp(6) with time zone"
        trino_column_lexer.input(s)
        self.assertEqual(trino_column_lexer.token().type, "TIMESTAMP")
        self.assertEqual(trino_column_lexer.token().type, "LPAREN")
        self.assertEqual(trino_column_lexer.token().type, "NUMBER")
        self.assertEqual(trino_column_lexer.token().type, "RPAREN")
        self.assertEqual(trino_column_lexer.token().type, "SPACE")
        self.assertEqual(trino_column_lexer.token().type, "WITH")
        self.assertEqual(trino_column_lexer.token().type, "SPACE")
        self.assertEqual(trino_column_lexer.token().type, "TIME")
        self.assertEqual(trino_column_lexer.token().type, "SPACE")
        self.assertEqual(trino_column_lexer.token().type, "ZONE")

    def test_array_scalar(self):
        s = "array(varchar)"
        trino_column_lexer.input(s)
        self.assertEqual(trino_column_lexer.token().type, "ARRAY")
        self.assertEqual(trino_column_lexer.token().type, "LPAREN")
        self.assertEqual(trino_column_lexer.token().type, "VARCHAR")
        self.assertEqual(trino_column_lexer.token().type, "RPAREN")

    def test_row_two_fields(self):
        s = "row(x bigint, Abc timestamp)"
        trino_column_lexer.input(s)
        self.assertEqual(trino_column_lexer.token().type, "ROW")
        self.assertEqual(trino_column_lexer.token().type, "LPAREN")
        self.assertEqual(trino_column_lexer.token().type, "NAME_PART")
        self.assertEqual(trino_column_lexer.token().type, "SPACE")
        self.assertEqual(trino_column_lexer.token().type, "BIGINT")
        self.assertEqual(trino_column_lexer.token().type, "COMMA_SPACE")
        self.assertEqual(trino_column_lexer.token().type, "NAME_PART")
        self.assertEqual(trino_column_lexer.token().type, "SPACE")
        self.assertEqual(trino_column_lexer.token().type, "TIMESTAMP")
        self.assertEqual(trino_column_lexer.token().type, "RPAREN")

    def test_row_keyword_and_space(self):
        s = "row(x y varchar timestamp(6) with time zone)"
        trino_column_lexer.input(s)
        self.assertEqual(trino_column_lexer.token().type, "ROW")
        self.assertEqual(trino_column_lexer.token().type, "LPAREN")
        self.assertEqual(trino_column_lexer.token().type, "NAME_PART")
        self.assertEqual(trino_column_lexer.token().type, "SPACE")
        self.assertEqual(trino_column_lexer.token().type, "NAME_PART")
        self.assertEqual(trino_column_lexer.token().type, "SPACE")
        self.assertEqual(trino_column_lexer.token().type, "VARCHAR")
        self.assertEqual(trino_column_lexer.token().type, "SPACE")
        self.assertEqual(trino_column_lexer.token().type, "TIMESTAMP")
        self.assertEqual(trino_column_lexer.token().type, "LPAREN")
        self.assertEqual(trino_column_lexer.token().type, "NUMBER")
        self.assertEqual(trino_column_lexer.token().type, "RPAREN")
        self.assertEqual(trino_column_lexer.token().type, "SPACE")
        self.assertEqual(trino_column_lexer.token().type, "WITH")
        self.assertEqual(trino_column_lexer.token().type, "SPACE")
        self.assertEqual(trino_column_lexer.token().type, "TIME")
        self.assertEqual(trino_column_lexer.token().type, "SPACE")
        self.assertEqual(trino_column_lexer.token().type, "ZONE")

    def test_row_unicode(self):
        s = "row(çÖÝ♥ varchar)"
        trino_column_lexer.input(s)
        self.assertEqual(trino_column_lexer.token().type, "ROW")
        self.assertEqual(trino_column_lexer.token().type, "LPAREN")
        self.assertEqual(trino_column_lexer.token().type, "NAME_PART")
        self.assertEqual(trino_column_lexer.token().type, "SPACE")
        self.assertEqual(trino_column_lexer.token().type, "VARCHAR")
        self.assertEqual(trino_column_lexer.token().type, "RPAREN")


class ParserTest(TestCase):
    def test_scalar(self):
        s = "varchar"
        t = trino_column_parser.parse(s)
        self.assertEqual(t, StringType())

    def test_varchar_size(self):
        s = "varchar(32)"
        t = trino_column_parser.parse(s)
        self.assertEqual(t, StringType())

    def test_boolean(self):
        s = "boolean"
        t = trino_column_parser.parse(s)
        self.assertEqual(t, BooleanType())

    def test_timestamp(self):
        s = "timestamp"
        t = trino_column_parser.parse(s)
        self.assertEqual(t, TimestampType())

    def test_timestamp_with_zone(self):
        s = "timestamp(6) with time zone"
        t = trino_column_parser.parse(s)
        self.assertEqual(t, TimestampType())

    def test_array_scalar(self):
        s = "array(varchar)"
        t = trino_column_parser.parse(s)
        self.assertEqual(t, ArrayType(StringType()))

    def test_row(self):
        s = "row(x bigint)"
        t = trino_column_parser.parse(s)
        self.assertEqual(t, StructType([StructField("x", LongType())]))

    def test_row_unicode(self):
        s = "row(çÖÝ♥ varchar)"
        t = trino_column_parser.parse(s)
        self.assertEqual(t, StructType([StructField("çÖÝ♥", StringType())]))

    def test_row_fparen(self):
        s = "row(ab(c)d varchar)"
        t = trino_column_parser.parse(s)
        # not support paren inside the field name
        self.assertEqual(t, None)

    def test_row_two_fields(self):
        s = "row(x bigint, Abc timestamp)"
        t = trino_column_parser.parse(s)
        self.assertEqual(
            t,
            StructType([StructField("x", LongType()), StructField("Abc", TimestampType())]),
        )

    def test_row_fspace(self):
        s = "row(x y bigint)"
        t = trino_column_parser.parse(s)
        self.assertEqual(t, StructType([StructField("x y", LongType())]))

    def test_row_fkeyword(self):
        s = "row(x varchar bigint)"
        t = trino_column_parser.parse(s)
        self.assertEqual(t, StructType([StructField("x varchar", LongType())]))

    def test_map(self):
        s = "map(varchar, bigint)"
        t = trino_column_parser.parse(s)
        self.assertEqual(t, MapType(StringType(), LongType()))

    def test_row_with_array(self):
        s = "row(f1 array(bigint))"
        t = trino_column_parser.parse(s)
        self.assertEqual(t, StructType([StructField("f1", ArrayType(LongType()))]))

    def test_row_with_row(self):
        s = "row(f1 row(ff1 bigint))"
        t = trino_column_parser.parse(s)
        self.assertEqual(
            t,
            StructType([StructField("f1", StructType([StructField("ff1", LongType())]))]),
        )


if __name__ == "__main__":
    unittest.main()
