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

    def test_unary_families_compose_after_renamed_ordered_limited_inputs(self) -> None:
        original = self.context.read_csv("input.data", schema={"key": "int64", "amount": "int64"})
        prefix = original.sort("amount", descending=True).limit(4).select("key AS id", "amount AS value")
        calls = [
            (prefix.select("id").distinct().select("id"), "DISTINCT_ROWS"),
            (prefix.tail(2).select("id"), "TAIL"),
            (prefix.sample(2, seed=7).select("id"), "SAMPLE"),
            (prefix.sample(frac=0.5, weights="value", random_state=11, replace=True).select("id"), "SAMPLE"),
            (prefix.drop_duplicates("id", keep="last").select("value"), "DROP_DUPLICATES"),
            (prefix.duplicated("id", keep=False).select("duplicated"), "DUPLICATED"),
            (prefix.mask(sl.col("value") < 3, 0).select("value"), "REWRITE"),
            (prefix.fillna(method="ffill", limit=2).select("value"), "REWRITE"),
            (prefix.replace({"value": {3: 9}}).select("value"), "REWRITE"),
            (prefix.reset_index().select("index", "id"), "REWRITE"),
            (prefix.melt(id_vars="id").select("variable", "value"), "MELT"),
            (prefix.rolling(2, min_periods=1, center=True).sum("value", alias="total").select("total"), "ROLLING"),
        ]
        for workflow, function in calls:
            with self.subTest(function=function, operations=workflow.operations):
                statement = workflow._native_relational_statement()
                self.assertIn(f"FROM {function}((", statement)
                self.assertIn("LIMIT 4)", statement)
                self.assertIn("key AS id,amount AS value", statement)
                with mock.patch.object(self.client, "public_workflow_run", return_value=self.reply([])) as run, mock.patch.object(self.client, "vortex_prepare") as prepare:
                    workflow.collect(check=True, memory_gb=3, max_parallelism=2)
                    self.assertEqual(run.call_args.kwargs["sql_statement"], statement)
                    self.assertEqual(run.call_args.kwargs["source_bindings"], {"input.data": {"input_format": "csv", "source_schema": original.source.schema}})
                    self.assertEqual(run.call_args.kwargs["memory_gb"], 3)
                    self.assertEqual(run.call_args.kwargs["max_parallelism"], 2)
                    prepare.assert_not_called()

    def test_successive_unary_stages_share_output_names_and_keep_their_positions(self) -> None:
        source = self.context.read_csv("input.csv", schema={"id": "int64", "value": "int64"})
        suffix = source.sort("value").limit(4).tail(2)
        result = suffix.drop_duplicates("id").reset_index().fillna(method="ffill")
        statement = result._relation_statement()
        self.assertEqual(statement.count("FROM REWRITE(("), 2)
        self.assertIn("FROM DROP_DUPLICATES((SELECT * FROM TAIL((", statement)
        self.assertIn('"target_column":"index"', statement)
        self.assertIn('"columns":["id","value","index"]', statement)
        left = source.tail(2).limit(1)._relation_statement()
        right = source.limit(1).tail(2)._relation_statement()
        self.assertIn("TAIL((SELECT * FROM 'input.csv'), 2)", left)
        self.assertTrue(left.endswith("LIMIT 1"))
        self.assertIn("LIMIT 1), 2)", right)
        indexed = suffix.melt(id_vars="id", value_vars="value", ignore_index=False)
        self.assertIn('"id_columns":["index","id"]', indexed._relation_statement())

    def test_dynamic_pivot_composes_after_renaming_and_submits_the_complete_statement(self) -> None:
        source = self.context.read_csv(
            "o'clock.csv", schema={"key": "int64", "kind": "utf8", "value": "int64"}
        )
        pivot = (source.sort("value", descending=True).limit(4)
                 .select("key AS entity", "kind AS category", "value AS amount")
                 .pivot_table(index="entity", columns="category", values="amount", aggfunc="sum"))
        workflow = pivot.filter(sl.col("pivot_a") > 3.0).sort("entity").select("entity", "pivot_a")
        statement = workflow._native_relational_statement()
        self.assertIn("FROM PIVOT((", statement)
        self.assertIn("key AS entity,kind AS category,value AS amount", statement)
        self.assertIn("LIMIT 4)", statement)
        self.assertIn("o''clock.csv", statement)
        self.assertIn('"pivot_column":"category"', statement)
        self.assertTrue(statement.startswith("SELECT entity,pivot_a FROM ("))
        expected = [{"entity": 1, "pivot_a": 5.0}]
        with mock.patch.object(self.client, "public_workflow_run", return_value=self.reply(expected)) as run, mock.patch.object(self.client, "vortex_prepare") as prepare:
            self.assertEqual(workflow.collect(check=True).result_rows, tuple(expected))
            for extension in ["vortex", "json", "jsonl", "csv", "parquet", "arrow_ipc", "avro", "orc"]:
                getattr(workflow, f"write_{extension}")(f"out.{extension}", check=True)
            self.assertEqual(run.call_count, 9)
            for call in run.call_args_list:
                self.assertEqual(call.kwargs["sql_statement"], statement)
                self.assertEqual(call.kwargs["source_bindings"], {
                    "o'clock.csv": {"input_format": "csv", "source_schema": source.source.schema},
                })
            prepare.assert_not_called()

    def test_dynamic_pivot_unknown_columns_remain_native_and_construction_is_inert(self) -> None:
        source = self.context.read_vortex("missing.vortex")
        with mock.patch.object(self.client, "public_workflow_run") as run, mock.patch.object(self.client, "vortex_prepare") as prepare:
            pivot = source.limit(3).pivot_table(index="entity", columns="category", values="amount", aggfunc="sum")
            changed = pivot.with_column("total", sl.col("pivot_a") + 1.0)
            self.assertIn("SELECT * REPLACE OR ADD", changed._native_relational_statement())
            self.assertIn("FROM PIVOT((", changed._native_relational_statement())
            from shardloom._relational_sql import frame_stages
            self.assertIsNone(frame_stages(pivot).columns)
            melted = pivot.melt(id_vars="entity", value_vars="pivot_a", var_name="category", value_name="amount")
            repeated = melted.pivot_table(index="entity", columns="category", values="amount", aggfunc="sum").select("pivot_pivot_a")
            statement = repeated._native_relational_statement()
            self.assertEqual(statement.count("FROM PIVOT(("), 2)
            self.assertIn("FROM MELT((", statement)
            run.assert_not_called()
            prepare.assert_not_called()

    def test_dynamic_pivot_preserves_join_and_set_source_declarations(self) -> None:
        source = self.context.read_csv("facts.csv", schema={"entity": "int64", "category": "utf8", "amount": "int64"})
        other = self.context.read_vortex("other.vortex", schema={"entity": "int64"})
        pivot = source.pivot_table(index="entity", columns="category", values="amount", aggfunc="sum")
        for index, workflow in enumerate([pivot.join(other, on="entity").select("f.entity", "f.pivot_a"), pivot.select("entity").union_all(other)]):
            with self.subTest(index=index), mock.patch.object(self.client, "public_workflow_run", return_value=self.reply([])) as run:
                statement = workflow._relation_statement()
                self.assertIn("FROM PIVOT((", statement)
                workflow.collect(check=True)
                self.assertEqual(run.call_count, 1)
                self.assertEqual(run.call_args.kwargs["sql_statement"], statement)
                self.assertEqual(set(run.call_args.kwargs["source_bindings"]), {"facts.csv", "other.vortex"})

    def test_unary_rendering_retains_escaped_json_and_join_set_operands(self) -> None:
        source = self.context.read_csv("o'clock.data", schema={"key": "utf8", "value": "utf8"})
        changed = source.sort("key").limit(3).replace({"value": {"isn't,(join)": "it's fine"}})
        statement = changed._relation_statement()
        self.assertIn("o''clock.data", statement)
        self.assertIn("isn''t,(join)", statement)
        self.assertIn("it''s fine", statement)
        right = self.context.read_vortex("right.vortex").select("key").tail(2)
        for result in [changed.join(right, on="key").select("f.key"), changed.select("key").union_all(right)]:
            statement = result._relation_statement()
            self.assertIn("TAIL((", statement)
            self.assertIn("REWRITE((", statement)
            with mock.patch.object(self.client, "public_workflow_run", return_value=self.reply([])) as run:
                for extension in ["vortex", "json", "jsonl", "csv", "parquet", "arrow_ipc", "avro", "orc"]:
                    getattr(result, f"write_{extension}")(f"out.{extension}", check=True)
                    self.assertEqual(run.call_args.kwargs["sql_statement"], statement)
                    self.assertEqual(set(run.call_args.kwargs["source_bindings"]), {"o'clock.data", "right.vortex"})

    def test_melt_without_id_columns_uses_the_preceding_output(self) -> None:
        source = self.context.read_vortex("missing.vortex", schema={"identifier": "uint64"})
        prefix = (source.sort("identifier", descending=True).select("identifier").distinct()
                  .drop_duplicates("identifier").tail(65_541).sample(frac=1.0, seed=7)
                  .duplicated("identifier").reset_index()
                  .rolling(3, min_periods=1, center=True).sum("index", alias="total"))
        with mock.patch.object(self.client, "public_workflow_run") as run, mock.patch.object(
            self.client, "vortex_prepare"
        ) as prepare, mock.patch.object(
            sl.LazyFrame, "_unsupported_operation", side_effect=AssertionError("melt rejected")
        ):
            for options in [{"value_vars": "total"}, {}, {"value_vars": "total", "ignore_index": False}, {"ignore_index": False}]:
                with self.subTest(options=options):
                    result = prefix.melt(**options)
                    statement = result._relation_statement()
                    self.assertIn('"value_columns":["total"]', statement)
                    expected_ids = '["index"]' if options.get("ignore_index") is False else '[]'
                    self.assertIn('"id_columns":' + expected_ids, statement)
                    self.assertIn("FROM MELT((SELECT * FROM", statement)
                    self.assertIn("FROM ROLLING((", statement)
            unknown = self.context.read_vortex("unknown.vortex").limit(2).melt(value_vars="total")
            self.assertIn('"id_columns":[]', unknown._relation_statement())
            run.assert_not_called()
            prepare.assert_not_called()

    def test_unary_binds_unknown_schemas_natively_without_preparation(self) -> None:
        source = self.context.read_vortex("missing.vortex").limit(3)
        with mock.patch.object(self.client, "public_workflow_run") as run, mock.patch.object(self.client, "vortex_prepare") as prepare:
            for result in [source.drop_duplicates(), source.duplicated(), source.mask(sl.col("x") > 0, 0), source.reset_index()]:
                self.assertIsNotNone(result._native_relational_statement())
            run.assert_not_called()
            prepare.assert_not_called()

    def test_standalone_unary_strategies_remain_selected_where_order_is_admitted(self) -> None:
        source = self.context.read_vortex("input.vortex", schema={"id": "int64", "value": "int64"})
        for result in [source.tail(2), source.select("id").sample(2, seed=7), source.drop_duplicates("id").limit(2), source.duplicated("id").limit(2), source.reset_index().limit(2), source.melt(id_vars="id", value_vars="value").limit(2), source.rolling(2).sum("value").limit(2)]:
            with self.subTest(operations=result.operations):
                self.assertIsNotNone(result._vortex_primitive_shape())
                self.assertIsNone(result._native_relational_statement())

    def test_prepare_preserves_compatible_source_schema_without_overriding_native_types(self) -> None:
        schema = {"label": "utf8", "items": "list<struct<code:list<int64>>>"}
        for frame, expected in [
            (self.context.read_csv("typed.data", schema=schema), tuple(schema.items())),
            (self.context.read_json("typed.jsonl", schema=schema), tuple(schema.items())),
            (self.context.read_vortex("typed.vortex", schema=schema), None),
        ]:
            with self.subTest(source=frame.source.source_format), mock.patch.object(
                self.client, "public_workflow_prepare", return_value=self.reply([])
            ) as prepare:
                frame.prepare("prepared.vortex", check=False)
                self.assertEqual(prepare.call_args.kwargs["source_schema"], expected)
                self.assertEqual(prepare.call_args.kwargs["output_ref"], "prepared.vortex")
                prepare.assert_called_once()

    def test_repeated_explode_keeps_nested_stage_order_and_source_declarations(self) -> None:
        source = self.context.read_csv("nested.data", schema={
            "id": "int64", "items": "list<struct<code:list<int64>>>",
        })
        prefix = source.sort("id", descending=True).limit(3).select("id AS key", "items AS groups")
        once = prefix.explode("groups.code")
        twice = once.explode("code").tail(2).select("key", "code")
        statement = twice._native_relational_statement()
        self.assertEqual(statement.count("FROM EXPLODE(("), 2)
        self.assertIn("LIMIT 3)", statement)
        self.assertIn('"column":"groups","element_field":"code","output_column":"code"', statement)
        self.assertIn("FROM TAIL((SELECT * FROM EXPLODE((", statement)
        renamed = once.with_column("next_key", sl.col("key") + 1)._relation_statement()
        self.assertIn("SELECT key,code,key + 1 AS next_key", renamed)
        with mock.patch.object(self.client, "public_workflow_run", return_value=self.reply([])) as run, mock.patch.object(self.client, "vortex_prepare") as prepare:
            twice.collect(check=True, memory_gb=3, max_parallelism=2)
            self.assertEqual(run.call_args.kwargs["sql_statement"], statement)
            self.assertEqual(run.call_args.kwargs["source_bindings"], {
                "nested.data": {"input_format": "csv", "source_schema": source.source.schema},
            })
            for extension in ["vortex", "json", "jsonl", "csv", "parquet", "arrow_ipc", "avro", "orc"]:
                getattr(twice, f"write_{extension}")(f"out.{extension}", check=True)
                self.assertEqual(run.call_args.kwargs["sql_statement"], statement)
            prepare.assert_not_called()
        native = self.context.read_vortex("nested.vortex", schema=dict(source.source.schema))
        self.assertIsNotNone(native.select("id", "items").explode("items").limit(2)._vortex_primitive_shape())


if __name__ == "__main__":
    unittest.main()
