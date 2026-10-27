"""Typed declarations are lossless; native integration tests own evaluation."""

from datetime import date, datetime, timezone
from decimal import Decimal, localcontext
from pathlib import Path
import sys
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

import shardloom as sl
from shardloom.query import _sql_literal


class TypedExpressionDeclarations(unittest.TestCase):
    def test_grouping_set_declarations_keep_units_empty_sets_and_stage_order(self) -> None:
        from shardloom import functions as f

        frame = sl.read_vortex("input.vortex")
        cases = [
            (frame.grouping_sets(["g", "r"], ["g"], [], ["g"]), "GROUPING SETS((g,r),(g),(),(g))"),
            (frame.grouping_sets(["g"], ["g"], distinct=True), "DISTINCT GROUPING SETS((g),(g))"),
            (frame.rollup(("g", "r"), "c"), "ROLLUP((g,r),c)"),
            (frame.cube("g", "r"), "CUBE(g,r)"),
            (frame.rollup(), "ROLLUP()"),
        ]
        for grouped, clause in cases:
            with self.subTest(clause=clause):
                result = grouped.agg(n=f.count())
                self.assertEqual(result.operations[-2].grouping_clause, clause)
                self.assertIn(f" GROUP BY {clause}", result._relation_statement())
                self.assertIn(f" GROUP BY {clause}", result.select("n")._relation_statement())
                self.assertIsNone(result._sql_local_source_statement())
        self.assertEqual(str(f.grouping("g", "r").alias("mask")), "GROUPING(g,r) AS mask")
        self.assertEqual(str(f.grouping_id("r", "g")), "GROUPING_ID(r,g)")
        with self.assertRaises(ValueError):
            frame.grouping_sets()
        with self.assertRaises(ValueError):
            f.grouping()
        with self.assertRaises(ValueError):
            f.grouping_id(*(["g"] * 65))
        with self.assertRaises(TypeError):
            frame.grouping_sets(["g"], distinct="true")

    def test_aggregate_filter_declaration_preserves_dependencies(self) -> None:
        from shardloom import functions as f

        condition = sl.col("enabled") == True
        filtered_sum = f.sum("amount").filter(condition)
        self.assertEqual(filtered_sum.sql, "SUM(amount) FILTER (WHERE enabled = true)")
        self.assertEqual(filtered_sum.source_bindings, condition.source_bindings)
        self.assertEqual(str(filtered_sum.over(sl.Window.partition_by("team"))),
                         "SUM(amount) FILTER (WHERE enabled = true) OVER (PARTITION BY team)")
        ordered = f.string_agg("label", ",", order_by=[("rank", False, None)]).filter(condition)
        self.assertEqual(ordered.sql,
                         "STRING_AGG(label,',' ORDER BY rank ASC) FILTER (WHERE enabled = true)")
        self.assertEqual(str(ordered.alias("labels")),
                         "STRING_AGG(label,',' ORDER BY rank ASC) FILTER (WHERE enabled = true) AS labels")
        with self.assertRaises(TypeError):
            f.sum("amount").filter("enabled")

    def test_collection_declarations_preserve_literal_separators_and_local_order(self) -> None:
        from shardloom import functions as f

        for declaration in (f.array_agg, f.list):
            self.assertEqual(str(declaration("value")), "ARRAY_AGG(value)")
            self.assertEqual(str(declaration("value", order_by=[("key", True, "last")])),
                             "ARRAY_AGG(value ORDER BY key DESC NULLS LAST)")
        self.assertEqual(str(f.string_agg("value", "東京,'; ORDER BY other")),
                         "STRING_AGG(value,'東京,''; ORDER BY other')")
        self.assertEqual(str(f.string_agg("value", "", order_by=[("key", False, "first")])),
                         "STRING_AGG(value,'' ORDER BY key ASC NULLS FIRST)")
        for separator in (None, 1, f.col("separator")):
            with self.subTest(separator=repr(separator)), self.assertRaises(TypeError):
                f.string_agg("value", separator)

    def test_decimal_metadata_and_text_do_not_depend_on_python_context(self) -> None:
        with localcontext() as context:
            context.prec = 2
            for value, expected in [
                ("12345678901234567890.123456789012345678", "CAST('12345678901234567890.123456789012345678' AS decimal128(38,18))"),
                ("1.2300", "CAST('1.2300' AS decimal128(5,4))"),
                ("-0.000", "CAST('-0.000' AS decimal128(3,3))"),
                ("1E+4", "CAST('10000' AS decimal128(5,0))"),
                ("1E-38", "CAST('0.00000000000000000000000000000000000001' AS decimal128(38,38))"),
            ]:
                with self.subTest(value=value):
                    self.assertEqual(_sql_literal(Decimal(value)), expected)
        for value in ["NaN", "sNaN", "Infinity", "-Infinity", "1E+1000000", "1E-1000000", "1" * 39]:
            with self.subTest(value=value), self.assertRaises(ValueError):
                _sql_literal(Decimal(value))

    def test_typed_literal_columns_lower_to_native_sql(self) -> None:
        context = sl.ShardLoomContext()
        for source in [context.read_vortex("input.vortex"), context.read_csv("input.csv")]:
            frame = source.with_columns(
                amount=Decimal("12.30"),
                payload=b"\x00\xff",
                day=date(1969, 12, 31),
                instant=datetime(1970, 1, 1, 0, 0, 1, tzinfo=timezone.utc),
            )
            self.assertIsInstance(frame, sl.LazyFrame)
            statement = frame._relation_statement()
            self.assertIsNotNone(statement)
            for declaration in [
                "CAST('12.30' AS decimal128(4,2)) AS amount",
                "X'00ff' AS payload",
                "DATE '1969-12-31' AS day",
                "TIMESTAMP '1970-01-01T00:00:01Z' AS instant",
            ]:
                self.assertIn(declaration, statement)

    def test_composed_binary_decimal_and_nullable_case_declarations(self) -> None:
        amount = sl.col("amount").cast("decimal128(10,2)")
        expressions = {
            "adjusted": amount + Decimal("1.20"),
            "decoded": sl.col("payload").cast("utf8").unhex(),
            "decoded_base64": sl.col("payload").cast("utf8").from_base64(),
            "size": sl.col("payload").byte_length(),
            "integer": amount.try_cast("uint64"),
            "selected": sl.case_when(sl.col("id") > 0, amount, None),
            "filled": amount.fill_null(Decimal("1.20")),
            "coalesced": amount.fill_null(sl.col("other")),
            "nullable": amount.null_if(None),
            "day_offset": sl.col("day").date_add_days(sl.col("offset")),
            "full_day_offset": sl.col("day").date_add_days(4_294_967_295),
            "full_timestamp_offset": sl.col("instant").timestamp_add_seconds(18_446_744_073_709),
        }
        frame = sl.ShardLoomContext().read_vortex("input.vortex").with_columns(expressions)
        statement = frame._relation_statement()
        self.assertIsNotNone(statement)
        for expression in expressions.values():
            self.assertIn(expression.sql, statement)
        self.assertIn("ELSE NULL END", statement)


if __name__ == "__main__":
    unittest.main()
