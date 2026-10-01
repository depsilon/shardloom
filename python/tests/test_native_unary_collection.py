from __future__ import annotations

import json
import sys
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from shardloom import ShardLoomClient, ShardLoomContext, VortexWorkflowExecutionReport
from shardloom.models import OutputEnvelope


class NativeUnaryCollectionTests(unittest.TestCase):
    def setUp(self) -> None:
        self.client = ShardLoomClient(binary="unused-shardloom")
        self.frame = ShardLoomContext(self.client).read_vortex(
            "renamed-source.vortex",
            schema={"shipment": "int64", "amount": "int64", "label": "utf8", "items": "list<int64>"},
        )

    @staticmethod
    def reply(rows: list[dict[str, object]]) -> SimpleNamespace:
        return SimpleNamespace(envelope=OutputEnvelope.from_field_mapping({
            "result_jsonl": "".join(json.dumps(row, ensure_ascii=False) + "\n" for row in rows),
            "output_row_count": str(len(rows)),
            "result_payload_complete": "true",
            "resident_source_opens": "1",
            "resident_unary_handle_retained": "true",
            "fallback_attempted": "false",
            "external_engine_invoked": "false",
        }, command="run"))

    def test_unary_collect_uses_bounded_public_route_and_exposes_its_exact_rows(self) -> None:
        selected = self.frame.select(["shipment", "amount"])
        cases = [
            (selected.distinct(), "distinct"),
            (selected.drop_duplicates(subset=["shipment"], keep="last"), "drop_duplicates"),
            (selected.duplicated(subset=["shipment"], keep=False), "duplicate_mask"),
            (selected.tail(2), "tail"),
            (selected.sample(n=2, random_state=7), "sample"),
            (selected.eval("amount = amount + 1"), "expression_project"),
            (selected.melt(id_vars=["shipment"], value_vars=["amount"]), "melt"),
            (self.frame.select(["shipment", "items"]).explode("items"), "explode"),
            (self.frame.pivot_table(index="shipment", columns="label", values="amount", aggfunc="sum"), "pivot"),
            (self.frame.select(["amount"]).rolling(window=2).sum("amount", alias="total"), "rolling_window"),
        ]
        rows = [{"renamed": "港-λ", "maximum": 18_446_744_073_709_551_615, "missing": None}]
        for frame, primitive in cases:
            with self.subTest(primitive=primitive), mock.patch.object(
                self.client, "public_workflow_run", return_value=self.reply(rows)
            ) as run:
                report = frame.collect(check=True)
                self.assertIsInstance(report, VortexWorkflowExecutionReport)
                self.assertEqual(report.result_rows, tuple(rows))
                self.assertEqual(report.result_rows, tuple(rows))
                self.assertFalse(report.fallback_attempted)
                self.assertFalse(report.external_engine_invoked)
                self.assertEqual(run.call_count, 1)
                self.assertEqual(run.call_args.kwargs["vortex_primitive"], primitive)
                self.assertEqual(run.call_args.kwargs["materialization_policy"], "bounded")

    def test_to_python_objects_executes_one_native_unary_call_and_keeps_empty_results(self) -> None:
        frame = self.frame.select(["shipment"]).distinct()
        for rows in [[], [{"shipment": 9}, {"shipment": None}]]:
            with self.subTest(rows=rows), mock.patch.object(
                self.client, "public_workflow_run", return_value=self.reply(rows)
            ) as run:
                self.assertEqual(frame.to_python_objects(check=True), tuple(rows))
                self.assertEqual(run.call_count, 1)
                self.assertEqual(run.call_args.kwargs["requested_output"], "collect")
                self.assertEqual(run.call_args.kwargs["vortex_primitive"], "distinct")

    def test_admitted_unary_filters_reach_collect_and_every_writer(self) -> None:
        import shardloom as sl

        filtered = self.frame.filter(sl.col("shipment") >= 2)
        selected = filtered.select(["shipment", "amount"])
        cases = [
            (selected.distinct(), "distinct"),
            (selected.drop_duplicates(subset=["shipment"], keep="last"), "drop_duplicates"),
            (selected.sample(n=2, random_state=7), "sample"),
            (selected.eval("amount = amount + 1"), "expression_project"),
            (selected.melt(id_vars=["shipment"], value_vars=["amount"]), "melt"),
            (filtered.select(["shipment", "items"]).explode("items"), "explode"),
            (filtered.pivot_table(index="shipment", columns="label", values="amount", aggfunc="sum"), "pivot"),
            (filtered.select("amount").rolling(window=2).sum("amount", alias="total"), "rolling_window"),
        ]
        for frame, primitive in cases:
            with self.subTest(primitive=primitive), mock.patch.object(
                self.client, "public_workflow_run", return_value=self.reply([])
            ) as run:
                self.assertIsInstance(frame, sl.LazyFrame)
                frame.collect(check=True)
                for extension in ["vortex", "parquet", "arrow_ipc", "avro", "orc", "json", "jsonl", "csv"]:
                    getattr(frame, f"write_{extension}")(f"result.{extension}", check=True)
                self.assertEqual(run.call_count, 9)
                for call in run.call_args_list:
                    self.assertEqual(call.kwargs["vortex_primitive"], primitive)
                    self.assertEqual(call.kwargs["vortex_predicate"], "gte:shipment:2")
                    self.assertEqual(call.kwargs["materialization_policy"], "bounded")

    def test_result_rows_requires_a_real_payload_and_rejects_nonobject_jsonl(self) -> None:
        from shardloom.errors import ShardLoomProtocolError

        for fields in [{"output_row_count": "0"}, {"result_jsonl": "[1,2]\n"}, {"result_jsonl": "{broken\n"}]:
            report = VortexWorkflowExecutionReport(
                self.frame, "collect", OutputEnvelope.from_field_mapping(fields),
            )
            with self.subTest(fields=fields), self.assertRaises(ShardLoomProtocolError):
                _ = report.result_rows

    def test_to_python_objects_preserves_native_failure_without_another_call(self) -> None:
        from shardloom import UnsupportedWorkflowOperationReport

        failed = OutputEnvelope.from_field_mapping(
            {"reason": "collect exceeds 65,536 rows; use an explicit streaming export"},
            command="run", status="error",
        )
        workflows = [
            self.frame.select("shipment").distinct(),
            ShardLoomContext(self.client).sql("SELECT DISTINCT shipment FROM 'renamed-source.vortex'"),
        ]
        for workflow in workflows:
            with self.subTest(workflow=type(workflow).__name__), mock.patch.object(
                self.client, "public_workflow_run", return_value=SimpleNamespace(envelope=failed)
            ) as run:
                report = workflow.to_python_objects(check=False)
                self.assertIsInstance(report, UnsupportedWorkflowOperationReport)
                self.assertIs(report.envelope, failed)
                self.assertEqual(run.call_count, 1)

    def test_sql_distinct_collection_uses_the_same_bounded_row_payload(self) -> None:
        workflow = ShardLoomContext(self.client).sql(
            "SELECT DISTINCT shipment FROM 'renamed-source.vortex'"
        )
        rows = [{"shipment": 1}, {"shipment": None}]
        with mock.patch.object(self.client, "public_workflow_run", return_value=self.reply(rows)) as run:
            self.assertEqual(workflow.to_python_objects(check=True), tuple(rows))
            self.assertEqual(run.call_count, 1)
            self.assertEqual(run.call_args.kwargs["vortex_primitive"], "distinct")
            self.assertEqual(run.call_args.kwargs["materialization_policy"], "bounded")

    def test_route_inspection_keeps_unary_payload_for_all_eight_writer_formats(self) -> None:
        frame = self.frame.select("shipment").distinct()
        for extension in ["vortex", "parquet", "arrow_ipc", "avro", "orc", "json", "jsonl", "csv"]:
            with self.subTest(extension=extension), mock.patch.object(
                self.client, "public_workflow_route", return_value=object()
            ) as route:
                frame.route(requested_output=f"write_{extension}", output_ref=f"result.{extension}")
                self.assertEqual(route.call_count, 1)
                self.assertEqual(route.call_args.kwargs["vortex_primitive"], "distinct")
                self.assertEqual(route.call_args.kwargs["materialization_policy"], "bounded")

    def test_native_array_projection_reaches_existing_structured_writer(self) -> None:
        import shardloom as sl

        with mock.patch.object(self.client, "public_workflow_run", return_value=self.reply([])) as run:
            workflow = self.frame.select("shipment").with_columns({"items": sl.array(7, None, 8)})
            self.assertIsInstance(workflow, sl.LazyFrame)
            run.assert_not_called()
            workflow.limit(2).write_vortex("lists.vortex", check=False)
            self.assertEqual(run.call_count, 1)
            self.assertEqual(run.call_args.kwargs["requested_output"], "write_vortex")
            self.assertEqual(run.call_args.kwargs["materialization_policy"], "bounded")
            self.assertIn("ARRAY[7,NULL,8]", run.call_args.kwargs["plan_summary"])
            self.assertEqual(run.call_args.kwargs["vortex_primitive"], "expression_project")
            self.assertEqual(run.call_args.kwargs["vortex_source_order_limit"], 2)
            self.assertEqual(run.call_args.kwargs["vortex_columns"], ("shipment",))
            self.assertEqual(json.loads(run.call_args.kwargs["vortex_expression_projection"]), {
                "structured_columns": [{"name": "shipment", "source": "shipment"},
                                       {"name": "items", "array": [7, None, 8]}],
            })

    def test_native_structured_payload_preserves_literals_and_source_dependencies(self) -> None:
        import shardloom as sl

        frame = self.frame.select("shipment").with_columns({
            "tags": sl.array("港,'λ", None, True, 18_446_744_073_709_551_615),
            "details": sl.struct("label", "amount"),
        })
        payload = frame._native_vortex_structured_export_payload()
        self.assertEqual(payload["vortex_columns"], ("shipment", "label", "amount"))
        self.assertEqual(json.loads(payload["vortex_expression_projection"]), {
            "structured_columns": [
                {"name": "shipment", "source": "shipment"},
                {"name": "tags", "array": ["港,'λ", None, True, 18_446_744_073_709_551_615]},
                {"name": "details", "struct": ["label", "amount"]},
            ],
        })
        overflow = self.frame.select("shipment").with_columns({"too_large": sl.array(1 << 64)})
        self.assertIsNone(overflow._native_vortex_structured_export_payload())


if __name__ == "__main__":
    unittest.main()
