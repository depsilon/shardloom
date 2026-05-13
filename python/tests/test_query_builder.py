from __future__ import annotations

import json
import sys
import tempfile
import textwrap
import unittest
from pathlib import Path

import shardloom as sl
from shardloom import LazyFrame, ShardLoomClient, ShardLoomContext


class LazyWorkflowBuilderTests(unittest.TestCase):
    def fake_cli(self, body: str) -> list[str]:
        tempdir = tempfile.TemporaryDirectory()
        self.addCleanup(tempdir.cleanup)
        path = Path(tempdir.name) / "fake_shardloom.py"
        path.write_text(body, encoding="utf-8")
        return [sys.executable, str(path)]

    def test_top_level_readers_are_lazy_and_build_operation_summary(self) -> None:
        frame = (
            sl.read_csv(
                "events.csv",
                schema={"id": "int64", "amount": "float64"},
                binary=["definitely-missing-shardloom"],
            )
            .filter("id > 0")
            .select(["id", "amount"])
            .limit(10)
        )
        json_frame = sl.read_json(
            "events.ndjson",
            schema={"payload": "string"},
            binary=["definitely-missing-shardloom"],
        )

        self.assertIsInstance(frame, LazyFrame)
        self.assertEqual(frame.source_format, "csv")
        self.assertEqual(frame.source.schema_map["id"], "int64")
        self.assertEqual(json_frame.source_format, "json")
        self.assertEqual(
            frame.operation_summary,
            "read_csv(events.csv) -> filter(id > 0) -> select(id,amount) -> limit(10)",
        )

    def test_lazy_builder_validates_empty_operations(self) -> None:
        frame = sl.read_parquet("orders.parquet", binary=["definitely-missing-shardloom"])

        with self.assertRaises(ValueError):
            frame.filter("")
        with self.assertRaises(ValueError):
            frame.select([])
        with self.assertRaises(TypeError):
            frame.limit(True)
        with self.assertRaises(ValueError):
            frame.limit(-1)
        with self.assertRaises(ValueError):
            sl.read_vortex(
                "orders.vortex",
                client=ShardLoomClient(binary=["shardloom"]),
                binary=["shardloom"],
            )

    def test_context_readers_reuse_context_client_for_plan_inspection(self) -> None:
        binary = self.fake_cli(
            textwrap.dedent(
                """
                import json, sys

                assert sys.argv[1:] == [
                    "input-plan",
                    "customers.parquet",
                    "--format",
                    "json",
                ], sys.argv
                print(json.dumps({
                    "schema_version": "shardloom.output.v2",
                    "command": "input-plan",
                    "status": "success",
                    "summary": "input plan report",
                    "human_text": "input plan",
                    "fallback": {"attempted": False, "allowed": False, "engine": None, "reason": "disabled"},
                    "diagnostics": [],
                    "fields": [
                        {"key": "plan_only", "value": "true"},
                        {"key": "data_read", "value": "false"},
                        {"key": "fallback_execution_allowed", "value": "false"}
                    ],
                }))
                """
            )
        )
        ctx = ShardLoomContext(ShardLoomClient(binary=binary))

        plan = ctx.read_parquet("customers.parquet").select("customer_id").plan()

        self.assertEqual(plan.command, "input-plan")
        self.assertTrue(plan.field_bool("plan_only"))
        self.assertFalse(plan.field_bool("data_read"))
        self.assertFalse(plan.fallback.attempted)

    def test_lazy_workflow_report_collects_explain_estimate_and_certify_surfaces(self) -> None:
        expected_workflow = (
            "read_vortex(orders.vortex) -> filter(gte:value:3) -> "
            "select(metric,value) -> limit(5)"
        )
        binary = self.fake_cli(
            textwrap.dedent(
                f"""
                import json, sys

                args = sys.argv[1:]
                status = "success"
                command = None
                fields = []
                diagnostics = []
                returncode = 0

                if args == ["vortex-read-plan", "orders.vortex", "--format", "json"]:
                    command = "vortex-read-plan"
                    fields = [
                        {{"key": "plan_only", "value": "true"}},
                        {{"key": "data_read", "value": "false"}},
                        {{"key": "data_materialized", "value": "false"}},
                        {{"key": "fallback_execution_allowed", "value": "false"}}
                    ]
                elif args == ["explain", {expected_workflow!r}, "--format", "json"]:
                    command = "explain"
                    status = "unsupported"
                    returncode = 1
                    fields = [
                        {{"key": "mode", "value": "plan_only"}},
                        {{"key": "materialization_boundary_reported", "value": "false"}},
                        {{"key": "fallback_execution_allowed", "value": "false"}}
                    ]
                    diagnostics = [{{
                        "code": "UnsupportedSql",
                        "severity": "error",
                        "category": "unsupported_feature",
                        "message": "unsupported",
                        "feature": "planning",
                        "reason": "Real planning is not implemented yet.",
                        "suggested_next_step": "inspect capabilities",
                        "fallback": {{"attempted": False, "allowed": False, "engine": None, "reason": "disabled"}}
                    }}]
                elif args == ["estimate", {expected_workflow!r}, "--format", "json"]:
                    command = "estimate"
                    status = "unsupported"
                    returncode = 1
                    fields = [
                        {{"key": "mode", "value": "plan_only"}},
                        {{"key": "fallback_execution_allowed", "value": "false"}}
                    ]
                    diagnostics = [{{
                        "code": "UnsupportedSql",
                        "severity": "error",
                        "category": "unsupported_feature",
                        "message": "unsupported",
                        "feature": "estimation",
                        "reason": "Native estimate planning is not implemented yet.",
                        "suggested_next_step": "inspect capabilities",
                        "fallback": {{"attempted": False, "allowed": False, "engine": None, "reason": "disabled"}}
                    }}]
                elif args == ["execution-certificate-plan", "--format", "json"]:
                    command = "execution-certificate-plan"
                    fields = [
                        {{"key": "certificate_evaluation_performed", "value": "false"}},
                        {{"key": "fallback_execution_allowed", "value": "false"}}
                    ]
                elif args == ["native-io-envelope-plan", "--format", "json"]:
                    command = "native-io-envelope-plan"
                    fields = [
                        {{"key": "materialization_boundary_reported", "value": "true"}},
                        {{"key": "per_path_certificate_required", "value": "true"}},
                        {{"key": "fallback_execution_allowed", "value": "false"}}
                    ]
                elif args == ["capabilities", "certification", "--format", "json"]:
                    command = "capabilities"
                    fields = [
                        {{"key": "scope", "value": "certification"}},
                        {{"key": "certification_status", "value": "planned"}},
                        {{"key": "fallback_execution_allowed", "value": "false"}}
                    ]
                else:
                    raise AssertionError(args)

                print(json.dumps({{
                    "schema_version": "shardloom.output.v2",
                    "command": command,
                    "status": status,
                    "summary": "ok",
                    "human_text": "ok",
                    "fallback": {{"attempted": False, "allowed": False, "engine": None, "reason": "disabled"}},
                    "diagnostics": diagnostics,
                    "fields": fields,
                }}))
                sys.exit(returncode)
                """
            )
        )
        workflow = (
            sl.read_vortex("orders.vortex", client=ShardLoomClient(binary=binary))
            .filter("gte:value:3")
            .select("metric", "value")
            .limit(5)
        )

        report = workflow.unsupported_report()

        self.assertEqual(report.input_plan.command, "vortex-read-plan")
        self.assertEqual(report.explain.status, "unsupported")
        self.assertEqual(report.estimate.status, "unsupported")
        self.assertEqual(
            report.certification.execution_certificate_plan.command,
            "execution-certificate-plan",
        )
        self.assertFalse(report.fallback_attempted)
        self.assertIn(
            "Real planning is not implemented yet.",
            report.unsupported_reasons,
        )
        self.assertIn(
            "Native estimate planning is not implemented yet.",
            report.unsupported_reasons,
        )
        self.assertIn(
            "native-io-envelope-plan:materialization_boundary_reported=true",
            report.materialization_boundaries,
        )


if __name__ == "__main__":
    unittest.main()
