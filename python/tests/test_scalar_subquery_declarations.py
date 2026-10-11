from __future__ import annotations

import json
import sys
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

import shardloom as sl
from shardloom.models import OutputEnvelope


class ScalarSubqueryDeclarationTests(unittest.TestCase):
    def setUp(self) -> None:
        self.client = sl.ShardLoomClient(binary="unused-shardloom", memory_gb=4, max_parallelism=2)
        self.context = sl.ShardLoomContext(self.client)
        self.outer = self.context.read_vortex("outer.vortex")
        self.inner = self.context.read_csv("inner.data", schema={"value": "int64"})
        self.other = self.context.read_json("other.jsonl", schema={"value": "int64"})

    @staticmethod
    def reply() -> SimpleNamespace:
        return SimpleNamespace(envelope=OutputEnvelope.from_field_mapping({
            "result_jsonl": "", "result_payload_complete": "true", "output_row_count": "0",
            "result_schema_json": json.dumps({"Struct": [{"names": ["value"], "dtypes": [{"Primitive": ["i64", True]}]}, False]}),
            "result_schema_format": "vortex.dtype.serde.v1",
            "fallback_attempted": "false", "external_engine_invoked": "false",
        }, command="run"))

    def test_scalar_declaration_is_inert_and_preserves_the_complete_inner_query(self) -> None:
        with mock.patch.object(self.client, "public_workflow_run") as run, mock.patch.object(self.client, "vortex_prepare") as prepare:
            inner = self.inner.filter(sl.col("value") > 3).select("value").distinct().sort("value").limit(1)
            expression = sl.scalar_subquery(inner)
            self.assertEqual(expression.sql, f"({inner._relation_statement()})")
            self.assertEqual(expression.source_bindings, inner._declared_sources())
            self.assertEqual(sl.scalar_subquery(self.context.sql("SELECT 7")).sql, "(SELECT 7)")
            run.assert_not_called()
            prepare.assert_not_called()
        for value in ["SELECT 7", None, self.inner.source, 7]:
            with self.subTest(value=type(value).__name__), self.assertRaisesRegex(TypeError, "LazyFrame or SqlWorkflow"):
                sl.scalar_subquery(value)

    def test_composition_preserves_both_sides_and_predicate_sources(self) -> None:
        left = sl.scalar_subquery(self.inner.select("value"))
        right = sl.scalar_subquery(self.other.select("value"))
        expressions = [
            left + right, left.fill_null(right), left.null_if(right),
            sl.case_when(left > right, left, right), left.date_add_days(right),
            left.date_sub_days(right), left.timestamp_add_seconds(right),
            left.timestamp_sub_seconds(right), left.date_diff_days(right),
            left.timestamp_diff_seconds(right),
        ]
        for expression in expressions:
            with self.subTest(expression=expression.sql):
                self.assertEqual({source.uri for source in expression.source_bindings}, {"inner.data", "other.jsonl"})
        for expression in [-left, left.abs(), left.cast("int64"), left.is_null(), left > 3]:
            self.assertEqual(expression.source_bindings, left.source_bindings)
        self.assertIsInstance(sl.count_distinct("value"), str)
        distinct = sl.count_distinct(left)
        self.assertIsInstance(distinct, sl.ColumnExpression)
        self.assertEqual(distinct.source_bindings, left.source_bindings)

    def test_string_and_source_free_subqueries_reach_native_type_admission(self) -> None:
        for source in [self.context.sql("SELECT '東京'"), self.inner.select("CAST(value AS utf8) AS label")]:
            value = sl.scalar_subquery(source)
            for expression in [value, value.lower(), value.substr(1, 2), value.left(1), value.right(1), value.replace("東", "西"), sl.concat(value, "!")]:
                with self.subTest(expression=expression.sql):
                    frame = self.outer.with_column("label", expression)
                    self.assertIn(value.sql, frame._relation_statement())
                    self.assertEqual({item.uri for item in frame._declared_sources()}, {"outer.vortex", *(item.uri for item in value.source_bindings)})

    def test_all_declaration_sites_and_writers_submit_owned_sources_once(self) -> None:
        value = sl.scalar_subquery(self.inner.select("value"))
        expected = {
            "outer.vortex": {"input_format": "vortex"},
            "inner.data": {"input_format": "csv", "source_schema": self.inner.source.schema},
        }
        window = sl.WindowExpression(f"FIRST_VALUE({value}) OVER (ORDER BY value) AS first", value.source_bindings)
        for base in [self.outer, self.context.sql("SELECT * FROM 'outer.vortex'", input="outer.vortex", input_format="vortex")]:
            def submitted_sources(run):
                args = run.call_args.kwargs
                sources = dict(args["source_bindings"])
                if isinstance(base, sl.SqlWorkflow):
                    self.assertEqual(args["input_uri"], "outer.vortex")
                    self.assertEqual(args["input_format"], "vortex")
                    sources[args["input_uri"]] = {"input_format": args["input_format"]}
                return sources

            workflows = [
                base.select(value), base.with_column("scalar", value),
                base.with_columns({"scalar": value}), base.filter(sl.col("value") == value),
                base.agg(n=sl.count_distinct(value)), base.group_by("value").agg(n=sl.count_distinct(value)),
                base.window(window),
            ]
            for workflow in workflows:
                with self.subTest(operations=workflow.operation_summary), mock.patch.object(self.client, "public_workflow_run", return_value=self.reply()) as run:
                    workflow.collect(check=True, memory_gb=2, max_parallelism=1)
                    self.assertEqual(run.call_count, 1)
                    self.assertIn(value.sql, run.call_args.kwargs["sql_statement"])
                    self.assertEqual(submitted_sources(run), expected)
                    for extension in ["vortex", "json", "jsonl", "csv", "parquet", "arrow_ipc", "avro", "orc"]:
                        getattr(workflow, f"write_{extension}")(f"result.{extension}", check=True)
                        self.assertEqual(submitted_sources(run), expected)
                    self.assertEqual(run.call_count, 9)

    def test_scalar_fragments_cannot_escape_the_enclosing_expression(self) -> None:
        for expression in [
            "(SELECT 1) FROM 'secret.csv'", "(SELECT 1); SELECT 2",
            "(SELECT 1 /* comment */)", "(SELECT 1 -- comment\n)",
            "(SELECT 1)) AS scalar FROM 'secret.csv'", "(SELECT 1",
        ]:
            with self.subTest(expression=expression):
                self.assertIsNone(self.outer.select(expression)._relation_statement())


if __name__ == "__main__":
    unittest.main()
