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
from shardloom.query import _native_relational_sql_candidate


class NativeRelationalCollectionTests(unittest.TestCase):
    def setUp(self) -> None:
        self.client = sl.ShardLoomClient(binary="unused-shardloom")
        self.context = sl.ShardLoomContext(self.client)

    @staticmethod
    def reply(rows: list[dict[str, object]]) -> SimpleNamespace:
        return SimpleNamespace(envelope=OutputEnvelope.from_field_mapping({
            "result_jsonl": "".join(json.dumps(row, ensure_ascii=False) + "\n" for row in rows),
            "output_row_count": str(len(rows)), "result_payload_complete": "true",
            "resident_relational_handle_retained": "true",
            "fallback_attempted": "false", "external_engine_invoked": "false",
        }, command="run"))

    def test_sql_submits_the_whole_statement_once_for_all_relational_families(self) -> None:
        statements = [
            "SELECT f.id,d.dim_label,f.metric FROM 'facts.vortex' AS f JOIN 'dims.vortex' AS d ON f.dim_key = d.dim_key LIMIT 10",
            "SELECT key FROM 'a.csv' UNION ALL SELECT key FROM 'b.jsonl' LIMIT 10",
            "SELECT key, ROW_NUMBER() OVER (ORDER BY key DESC) AS position FROM 'a.vortex'",
            "SELECT key FROM 'a.csv' WHERE key IN (SELECT key FROM 'b.jsonl' WHERE EXISTS (SELECT 1 FROM 'c.vortex' WHERE key = outer.key))",
        ]
        rows = [{"renamed": "港-λ", "missing": None}, {"renamed": "duplicate", "missing": 7}]
        for statement in statements:
            with self.subTest(statement=statement), mock.patch.object(
                self.client, "public_workflow_run", return_value=self.reply(rows)
            ) as run, mock.patch.object(self.client, "vortex_prepare") as prepare:
                workflow = self.context.sql(statement)
                report = workflow.collect(check=True, memory_gb=3, max_parallelism=1)
                self.assertEqual(report.result_rows, tuple(rows))
                self.assertEqual(report.result_rows, tuple(rows))
                self.assertEqual(run.call_count, 1)
                self.assertEqual(run.call_args.kwargs["sql_statement"], statement)
                self.assertEqual(run.call_args.kwargs["memory_gb"], 3)
                self.assertEqual(run.call_args.kwargs["max_parallelism"], 1)
                self.assertEqual(run.call_args.kwargs["materialization_policy"], "bounded")
                self.assertNotIn("vortex_primitive", run.call_args.kwargs)
                self.assertNotIn("native_vortex_provider_scenario", run.call_args.kwargs)
                prepare.assert_not_called()
                for extension in ["vortex", "json", "jsonl", "csv", "parquet", "arrow_ipc", "avro", "orc"]:
                    getattr(workflow, f"write_{extension}")(f"result.{extension}", check=True)
                    self.assertEqual(run.call_args.kwargs["sql_statement"], statement)
                    self.assertEqual(run.call_args.kwargs["materialization_policy"], "bounded")
                self.assertEqual(run.call_count, 9)

    def test_native_and_mixed_dataframe_relations_reach_the_same_route(self) -> None:
        native = self.context.read_vortex("a.vortex", schema={"key": "int64"})
        compat = self.context.read_csv("b.csv", schema={"key": "int64"})
        statements = [
            (native.join(compat, on="key", how="left").select("f.key", "d.key"), "LEFT JOIN"),
            (native.select("key").window("RANK() OVER (ORDER BY key DESC) AS ranked").sort("ranked"), "OVER"),
            (native.select("key").filter(sl.col("key").isin_source("b.csv", "key")), "IN (SELECT"),
            (native.select("key").union_all(compat.select("key")), "UNION ALL"),
            (native.select("key").intersect(compat.select("key")), "INTERSECT"),
            (native.select("key").except_(compat.select("key")), "EXCEPT"),
        ]
        for workflow, operator in statements:
            with self.subTest(operator=operator), mock.patch.object(
                self.client, "public_workflow_run", return_value=self.reply([{ "key": 7 }])
            ) as run:
                self.assertEqual(workflow.collect(check=True).result_rows, ({"key": 7},))
                self.assertIn(operator, run.call_args.kwargs["sql_statement"])
                self.assertEqual(run.call_count, 1)
                workflow.write_vortex("result.vortex", check=True)
                self.assertIn(operator, run.call_args.kwargs["sql_statement"])
                self.assertEqual(run.call_count, 2)

    def test_python_objects_preserve_empty_and_nonempty_payloads_without_reexecution(self) -> None:
        frame = self.context.read_vortex("a.vortex").select("key").window("ROW_NUMBER() OVER (ORDER BY key) AS position")
        statement = "SELECT key FROM 'a.csv' INTERSECT SELECT key FROM 'b.vortex'"
        for workflow in [frame, self.context.sql(statement)]:
            for rows in [[], [{"key": 9, "position": 1}]]:
                with self.subTest(workflow=workflow, rows=rows), mock.patch.object(
                    self.client, "public_workflow_run", return_value=self.reply(rows)
                ) as run:
                    self.assertEqual(workflow.to_python_objects(check=True), tuple(rows))
                    self.assertEqual(run.call_count, 1)

    def test_dataframe_source_declarations_survive_join_set_limit_and_writes(self) -> None:
        native = self.context.read_vortex("left.vortex", schema={"key": "utf8"})
        typed = self.context.read_csv("right.data", schema={"key": "utf8", "count": "int64"})
        expected = {
            "left.vortex": {"input_format": "vortex"},
            "right.data": {"input_format": "csv", "source_schema": typed.source.schema},
        }
        workflows = [
            native.join(typed, on="key").select("f.key"),
            native.select("key").union_all(typed.select("key")).limit(8),
            native.select("key").intersect(typed.select("key")).limit(8),
            native.select("key").except_(typed.select("key")).limit(8),
            native.select("key").concat(typed.select("key")).limit(8),
        ]
        for workflow in workflows:
            with self.subTest(workflow=workflow), mock.patch.object(
                self.client, "public_workflow_run", return_value=self.reply([{"key": "001"}])
            ) as run, mock.patch.object(self.client, "public_workflow_route") as route:
                workflow.collect(check=True)
                self.assertEqual(run.call_args.kwargs["source_bindings"], expected)
                self.assertIsNone(run.call_args.kwargs.get("source_schema"))
                workflow.write_vortex("result.vortex", check=True)
                self.assertEqual(run.call_args.kwargs["source_bindings"], expected)
                workflow.run(check=True)
                self.assertEqual(run.call_args.kwargs["source_bindings"], expected)
                workflow.route(check=True)
                self.assertEqual(route.call_args.kwargs["source_bindings"], expected)
                self.assertTrue(_native_relational_sql_candidate(route.call_args.kwargs["sql_statement"]))

    def test_conflicting_source_declarations_fail_before_execution(self) -> None:
        strings = self.context.read_csv("same.csv", schema={"key": "utf8"}).select("key")
        integers = self.context.read_csv("same.csv", schema={"key": "int64"}).select("key")
        workflow = strings.union_all(integers)
        with mock.patch.object(self.client, "public_workflow_run") as run:
            with self.assertRaisesRegex(ValueError, "conflicting format or schema"):
                workflow.collect(check=True)
            run.assert_not_called()

    def test_sql_composition_preserves_primary_typed_source_binding(self) -> None:
        from shardloom.query import SqlWorkflow

        typed = self.context.read_csv("typed.data", schema={"key": "utf8"})
        source = SqlWorkflow("SELECT key FROM 'typed.data'", self.client,
                             input_uri="typed.data", input_format="csv",
                             source_bindings=(typed.source,))
        result = source.union_all(typed.select("key")).filter(sl.col("key") == "001")
        with mock.patch.object(self.client, "public_workflow_run", return_value=self.reply([])) as run:
            result.collect(check=True)
            self.assertEqual(run.call_args.kwargs["source_bindings"], {
                "typed.data": {"input_format": "csv", "source_schema": typed.source.schema},
            })
        conflict = SqlWorkflow(source.statement, self.client, input_uri="typed.data",
                               input_format="json", source_bindings=(typed.source,))
        with self.assertRaisesRegex(ValueError, "conflicting format or schema"):
            conflict.union(typed)

    def test_sql_having_never_moves_across_order_limit_or_set_stages(self) -> None:
        frame = self.context.read_vortex("left.vortex").select("key")
        grouped = frame.union_all(frame).group_by("key").agg(n="count(*)")
        self.assertIn("HAVING n > 1", grouped.having(sl.col("n") > 1).statement)
        for workflow in [grouped.limit(1), grouped.sort("n"), grouped.union_all(grouped)]:
            with self.subTest(statement=workflow.statement), self.assertRaisesRegex(ValueError, "HAVING must follow"):
                workflow.having(sl.col("n") > 1)
        later_filter = grouped.limit(1).filter(sl.col("n") > 1)
        self.assertIn("LIMIT 1) AS", later_filter.statement)
        self.assertTrue(later_filter.statement.endswith("WHERE n > 1"))

    def test_typed_compatibility_join_without_limit_uses_every_native_writer(self) -> None:
        left = self.context.read_csv("left.csv", schema={"key": "utf8"})
        right = self.context.read_csv("right.data", schema={"key": "utf8"})
        workflow = left.join(right, on="key").select("f.key AS key")
        with mock.patch.object(self.client, "public_workflow_run", return_value=self.reply([])) as run:
            for extension in ["vortex", "json", "jsonl", "csv", "parquet", "arrow_ipc", "avro", "orc"]:
                getattr(workflow, f"write_{extension}")(f"result.{extension}", check=True)
                self.assertEqual(run.call_args.kwargs["source_bindings"], {
                    "left.csv": {"input_format": "csv", "source_schema": left.source.schema},
                    "right.data": {"input_format": "csv", "source_schema": right.source.schema},
                })
            self.assertEqual(run.call_count, 8)

    def test_subquery_helpers_retain_nested_adapter_contracts(self) -> None:
        frame = self.context.read_vortex("left.vortex")
        typed = self.context.read_csv("right.data", schema={"key": "utf8", "value": "int64"})
        inner = self.context.read_json("inner.jsonl", schema={"key": "utf8"})
        predicates = [
            sl.col("key").isin_source(typed, "key"),
            sl.col("key").not_in_source(typed, "key"),
            sl.col("key").any_source("=", typed, "key"),
            sl.col("key").all_source("!=", typed, "key"),
            sl.row_in_source(("key", "value"), typed, ("key", "value")),
            sl.row_not_in_source(("key", "value"), typed, ("key", "value")),
            sl.exists_source(typed, select=1),
            sl.not_exists_source(typed, select=1),
        ]
        nested = sl.col("key").isin_source(
            inner, "key", where=sl.col("key").isin_source(typed, "key")
        )
        for predicate in predicates:
            for combined in [predicate & nested, predicate | nested, ~(predicate & nested)]:
                workflow = frame.filter(combined).select("key")
                with self.subTest(predicate=str(combined)), mock.patch.object(
                    self.client, "public_workflow_run", return_value=self.reply([])
                ) as run:
                    workflow.collect(check=True)
                    self.assertEqual(run.call_args.kwargs["source_bindings"], {
                        "left.vortex": {"input_format": "vortex"},
                        "right.data": {"input_format": "csv", "source_schema": typed.source.schema},
                        "inner.jsonl": {"input_format": "jsonl", "source_schema": inner.source.schema},
                    })

    def test_dispatch_ignores_relational_words_inside_escaped_values_and_paths(self) -> None:
        self.assertFalse(_native_relational_sql_candidate("SELECT value FROM 'join-over-union.vortex' WHERE label = 'select '' join except' LIMIT 1"))
        self.assertTrue(_native_relational_sql_candidate("SELECT value FROM 'input.vortex' WHERE value IN (SELECT value FROM 'right.vortex')"))

    def test_subquery_helpers_never_discard_transformed_source_operations(self) -> None:
        source = self.context.read_vortex("right.vortex")
        for transformed in [source.filter(sl.col("key") > 5), source.limit(1), source.select("key")]:
            with self.subTest(operations=transformed.operations):
                predicate = sl.col("key").isin_source(transformed, "key")
                self.assertIn(f"({transformed._relation_statement()}) AS _sl_subquery", predicate.sql)
        self.assertIn("right.vortex", sl.col("key").isin_source(source, "key").sql)

    def test_flat_join_renderer_never_moves_input_stages_after_the_join(self) -> None:
        source = self.context.read_vortex("left.vortex")
        right = self.context.read_vortex("right.vortex")
        for before in [source.filter(sl.col("key") > 1), source.limit(1), source.sort("key"), source.select("key")]:
            for kind in ["inner", "left", "right", "full"]:
                workflow = before.join(right, on="key", how=kind).select("f.key", "d.key")
                with self.subTest(operations=before.operations, kind=kind):
                    statement = workflow._native_relational_statement()
                    self.assertIsNotNone(statement)
                    self.assertIn("FROM (SELECT", statement)
                    self.assertLess(statement.index("left.vortex"), statement.index("JOIN"))

    def test_transformed_right_operand_keeps_sql_and_source_declarations(self) -> None:
        left = self.context.read_vortex("left.vortex")
        right = self.context.read_csv("right.data", schema={"key": "utf8"}).sort("key").limit(2)
        workflow = left.limit(3).join(right, on="key", how="left").select("f.key AS key")
        statement = workflow._native_relational_statement()
        self.assertIn(f"({right._relation_statement()}) AS d", statement)
        with mock.patch.object(self.client, "public_workflow_run", return_value=self.reply([])) as run:
            workflow.collect(check=True)
            self.assertEqual(run.call_args.kwargs["sql_statement"], statement)
            self.assertEqual(run.call_args.kwargs["source_bindings"]["right.data"], {
                "input_format": "csv", "source_schema": right.source.schema,
            })

    def test_repeated_stages_and_set_branch_limits_preserve_nesting(self) -> None:
        frame = self.context.read_vortex("left.vortex")
        stages = (
            frame.sort("key").limit(2).filter(sl.col("key") > 0).select("key")
            .window("ROW_NUMBER() OVER (ORDER BY key DESC) AS ranked")
            .filter(sl.col("ranked") <= 1).select("key")
        )
        statement = stages._native_relational_statement()
        self.assertGreater(statement.count("SELECT"), 5)
        self.assertIn("LIMIT 2) AS", statement)
        self.assertIn("WHERE ranked <= 1", statement)
        union = frame.limit(2).union_all(frame.sort("key", descending=True).limit(1))
        self.assertIn("LIMIT 2) AS _sl_set_left", union.statement)
        self.assertIn("LIMIT 1) AS _sl_set_right", union.statement)

    def test_set_results_compose_and_retain_all_input_declarations_without_io(self) -> None:
        left = self.context.read_vortex("left.vortex").select("key")
        right = self.context.read_csv("right.data", schema={"key": "utf8"}).select("key")
        combined = left.union_all(right).limit(4)
        with mock.patch.object(self.client, "public_workflow_run") as run, mock.patch.object(
            self.client, "vortex_prepare"
        ) as prepare:
            result = (combined.filter(sl.col("key") != "missing").distinct().sort("key")
                      .with_column("label", sl.col("key")).select("key", "label"))
            grouped = result.group_by("label").agg(n="count(*)").having(sl.col("n") > 1)
            joined = left.join(combined, on="key").select("f.key")
            repeated = combined.intersect(left).union(right).except_(left)
            predicate = sl.col("key").isin_source(combined, "key")
            for statement in [result.statement, grouped.statement, joined._relation_statement(), repeated.statement, predicate.sql]:
                self.assertIn("LIMIT 4", statement)
                self.assertIn("right.data", statement)
            run.assert_not_called()
            prepare.assert_not_called()
        with mock.patch.object(self.client, "public_workflow_run", return_value=self.reply([])) as run:
            grouped.collect(check=True)
            self.assertEqual(run.call_args.kwargs["source_bindings"]["right.data"], {
                "input_format": "csv", "source_schema": right.source.schema,
            })
            combined.limit(0).collect(check=True)
            self.assertTrue(run.call_args.kwargs["sql_statement"].endswith("LIMIT 0"))

    def test_unknown_schema_computed_replacement_is_explicit_and_keeps_stage_dependencies(self) -> None:
        frame = self.context.read_vortex("input.vortex")
        changed = frame.limit(2).with_column("key", sl.col("key") + 1).with_column("key", sl.col("key") * 2)
        statement = changed._relation_statement()
        self.assertEqual(statement.count("REPLACE OR ADD"), 2)
        self.assertIn("REPLACE OR ADD (key + 1 AS key)", statement)
        self.assertIn("REPLACE OR ADD (key * 2 AS key)", statement)
        self.assertIn("LIMIT 2)", statement)


if __name__ == "__main__":
    unittest.main()
