from __future__ import annotations

import os
import sys
import tempfile
import textwrap
import unittest
from pathlib import Path

from shardloom import ShardLoomClient, ShardLoomCommandError, ShardLoomProtocolError


class ShardLoomClientTests(unittest.TestCase):
    def fake_cli(self, body: str) -> list[str]:
        tempdir = tempfile.TemporaryDirectory()
        self.addCleanup(tempdir.cleanup)
        path = Path(tempdir.name) / "fake_shardloom.py"
        path.write_text(body, encoding="utf-8")
        return [sys.executable, str(path)]

    def test_status_appends_json_format_and_parses_fields(self) -> None:
        binary = self.fake_cli(
            textwrap.dedent(
                """
                import json, sys
                assert sys.argv[1:] == ["status", "--format", "json"], sys.argv
                print(json.dumps({
                    "schema_version": "shardloom.output.v1",
                    "command": "status",
                    "status": "success",
                    "summary": "ok",
                    "human_text": "ok",
                    "fallback": {"attempted": False, "allowed": False, "engine": None, "reason": "disabled"},
                    "diagnostics": [],
                    "fields": [{"key": "engine", "value": "shardloom"}],
                }))
                """
            )
        )

        result = ShardLoomClient(binary=binary).status()

        self.assertEqual(result.command, "status")
        self.assertEqual(result.status, "success")
        self.assertFalse(result.fallback.attempted)
        self.assertEqual(result.field_map["engine"], "shardloom")

    def test_capabilities_scope_uses_explicit_scope(self) -> None:
        binary = self.fake_cli(
            textwrap.dedent(
                """
                import json, sys
                assert sys.argv[1:] == ["capabilities", "python", "--format", "json"], sys.argv
                print(json.dumps({
                    "schema_version": "shardloom.output.v1",
                    "command": "capabilities",
                    "status": "success",
                    "summary": "ok",
                    "human_text": "ok",
                    "fallback": {"attempted": False, "allowed": False, "engine": None, "reason": "disabled"},
                    "diagnostics": [],
                    "fields": [{"key": "scope", "value": "python"}],
                }))
                """
            )
        )

        result = ShardLoomClient(binary=binary).capabilities("python")

        self.assertEqual(result.field_map["scope"], "python")

    def test_vortex_run_passes_explicit_runtime_command(self) -> None:
        binary = self.fake_cli(
            textwrap.dedent(
                """
                import json, sys
                assert sys.argv[1:] == ["vortex-run", "file.vortex", "count", "8", "2", "--format", "json"], sys.argv
                print(json.dumps({
                    "schema_version": "shardloom.output.v1",
                    "command": "vortex-run",
                    "status": "success",
                    "summary": "ok",
                    "human_text": "ok",
                    "fallback": {"attempted": False, "allowed": False, "engine": None, "reason": "disabled"},
                    "diagnostics": [],
                    "fields": [{"key": "fallback_execution_allowed", "value": "false"}],
                }))
                """
            )
        )

        result = ShardLoomClient(binary=binary).vortex_run(
            "file.vortex", "count", memory_gb=8, max_parallelism=2
        )

        self.assertEqual(result.command, "vortex-run")
        self.assertEqual(result.field_map["fallback_execution_allowed"], "false")
        self.assertEqual(result.field("fallback_execution_allowed"), "false")
        self.assertTrue(result.field_bool("fallback_execution_allowed") is False)

    def test_unsupported_envelope_raises_with_diagnostics_and_fallback(self) -> None:
        binary = self.fake_cli(
            textwrap.dedent(
                """
                import json, sys
                print(json.dumps({
                    "schema_version": "shardloom.output.v1",
                    "command": "capabilities",
                    "status": "unsupported",
                    "summary": "unsupported",
                    "human_text": "unsupported",
                    "fallback": {"attempted": False, "allowed": False, "engine": None, "reason": "disabled"},
                    "diagnostics": [{
                        "code": "UnsupportedSql",
                        "severity": "error",
                        "category": "unsupported_feature",
                        "message": "unsupported",
                        "feature": "sql",
                        "reason": "not implemented",
                        "suggested_next_step": "inspect capabilities",
                        "fallback": {"attempted": False, "allowed": False, "engine": None, "reason": "disabled"}
                    }],
                    "fields": [],
                }))
                sys.exit(1)
                """
            )
        )

        with self.assertRaises(ShardLoomCommandError) as raised:
            ShardLoomClient(binary=binary).capabilities("sql")

        error = raised.exception
        self.assertEqual(error.returncode, 1)
        self.assertFalse(error.envelope.fallback.attempted)
        self.assertEqual(error.envelope.diagnostics[0].code, "UnsupportedSql")

    def test_invalid_json_raises_protocol_error(self) -> None:
        binary = self.fake_cli("print('not-json')")

        with self.assertRaises(ShardLoomProtocolError):
            ShardLoomClient(binary=binary).status()

    def test_missing_envelope_fields_raise_protocol_error(self) -> None:
        binary = self.fake_cli("print('{\"schema_version\":\"shardloom.output.v1\"}')")

        with self.assertRaises(ShardLoomProtocolError):
            ShardLoomClient(binary=binary).status()

    def test_successful_plan_with_error_diagnostic_remains_inspectable(self) -> None:
        binary = self.fake_cli(
            textwrap.dedent(
                """
                import json
                print(json.dumps({
                    "schema_version": "shardloom.output.v1",
                    "command": "status",
                    "status": "success",
                    "summary": "ok",
                    "human_text": "ok",
                    "fallback": {"attempted": False, "allowed": False, "engine": None, "reason": "disabled"},
                    "diagnostics": [{
                        "code": "Example",
                        "severity": "error",
                        "category": "invalid_input",
                        "message": "bad",
                        "feature": None,
                        "reason": None,
                        "suggested_next_step": None,
                        "fallback": {"attempted": False, "allowed": False, "engine": None, "reason": "disabled"}
                    }],
                    "fields": [],
                }))
                """
            )
        )

        result = ShardLoomClient(binary=binary).status()

        self.assertFalse(result.is_error)
        self.assertTrue(result.has_error_diagnostics)

    def test_non_json_format_is_rejected(self) -> None:
        client = ShardLoomClient(binary=["shardloom"])

        with self.assertRaises(ValueError):
            client.run(["status", "--format", "text"])

    def test_explicit_binary_overrides_env_default(self) -> None:
        binary = self.fake_cli(
            textwrap.dedent(
                """
                import json
                print(json.dumps({
                    "schema_version": "shardloom.output.v1",
                    "command": "api-compat-plan",
                    "status": "success",
                    "summary": "ok",
                    "human_text": "ok",
                    "fallback": {"attempted": False, "allowed": False, "engine": None, "reason": "disabled"},
                    "diagnostics": [],
                    "fields": [],
                }))
                """
            )
        )
        old = os.environ.get("SHARDLOOM_BIN")
        os.environ["SHARDLOOM_BIN"] = "ignored-test-value"
        try:
            result = ShardLoomClient(binary=binary).api_compat_plan()
        finally:
            if old is None:
                os.environ.pop("SHARDLOOM_BIN", None)
            else:
                os.environ["SHARDLOOM_BIN"] = old

        self.assertEqual(result.command, "api-compat-plan")

    def test_env_binary_is_resolved_from_client_environment(self) -> None:
        client = ShardLoomClient(env={"SHARDLOOM_BIN": "custom-shardloom", "PATH": ""})

        command = client._command(["status"])

        self.assertEqual(command[0], "custom-shardloom")

    def test_from_repo_resolves_target_binary_lazily(self) -> None:
        tempdir = tempfile.TemporaryDirectory()
        self.addCleanup(tempdir.cleanup)
        root = Path(tempdir.name)
        target = root / "target" / "debug"
        target.mkdir(parents=True)
        (target / "shardloom").write_text("", encoding="utf-8")
        (target / "shardloom.exe").write_text("", encoding="utf-8")

        client = ShardLoomClient.from_repo(root, profile_order=("debug",))
        command = client._command(["status"])

        self.assertTrue(command[0].endswith(("shardloom", "shardloom.exe")))
        self.assertIn(str(target), command[0])

    def test_traditional_analytics_vortex_run_passes_explicit_inputs(self) -> None:
        binary = self.fake_cli(
            textwrap.dedent(
                """
                import json, sys
                assert sys.argv[1:] == [
                    "traditional-analytics-vortex-run",
                    "selective filter",
                    "fact.vortex",
                    "dim.vortex",
                    "--format",
                    "json",
                ], sys.argv
                print(json.dumps({
                    "schema_version": "shardloom.output.v1",
                    "command": "traditional-analytics-vortex-run",
                    "status": "success",
                    "summary": "ok",
                    "human_text": "ok",
                    "fallback": {"attempted": False, "allowed": False, "engine": None, "reason": "disabled"},
                    "diagnostics": [],
                    "fields": [{"key": "rows_scanned", "value": "42"}],
                }))
                """
            )
        )

        result = ShardLoomClient(binary=binary).traditional_analytics_vortex_run(
            "selective filter", "fact.vortex", "dim.vortex"
        )

        self.assertEqual(result.field_int("rows_scanned"), 42)

    def test_live_etl_smoke_dispatches_csv_and_vortex_modes(self) -> None:
        csv_binary = self.fake_cli(
            textwrap.dedent(
                """
                import json, sys
                assert sys.argv[1:] == [
                    "traditional-analytics-run",
                    "csv/file ingest",
                    "fact.csv",
                    "dim.csv",
                    "--workspace",
                    "work",
                    "--format",
                    "json",
                ], sys.argv
                print(json.dumps({
                    "schema_version": "shardloom.output.v1",
                    "command": "traditional-analytics-run",
                    "status": "success",
                    "summary": "ok",
                    "human_text": "ok",
                    "fallback": {"attempted": False, "allowed": False, "engine": None, "reason": "disabled"},
                    "diagnostics": [],
                    "fields": [{"key": "input_format", "value": "csv"}],
                }))
                """
            )
        )
        csv_result = ShardLoomClient(binary=csv_binary).live_etl_smoke(
            "csv/file ingest",
            "fact.csv",
            "dim.csv",
            input_format="csv",
            workspace="work",
        )
        self.assertEqual(csv_result.command, "traditional-analytics-run")

        vortex_binary = self.fake_cli(
            textwrap.dedent(
                """
                import json, sys
                assert sys.argv[1:] == [
                    "traditional-analytics-vortex-run",
                    "wide projection",
                    "fact.vortex",
                    "dim.vortex",
                    "--format",
                    "json",
                ], sys.argv
                print(json.dumps({
                    "schema_version": "shardloom.output.v1",
                    "command": "traditional-analytics-vortex-run",
                    "status": "success",
                    "summary": "ok",
                    "human_text": "ok",
                    "fallback": {"attempted": False, "allowed": False, "engine": None, "reason": "disabled"},
                    "diagnostics": [],
                    "fields": [{"key": "input_format", "value": "vortex"}],
                }))
                """
            )
        )
        vortex_result = ShardLoomClient(binary=vortex_binary).live_etl_smoke(
            "wide projection",
            "fact.vortex",
            "dim.vortex",
            input_format="vortex",
        )
        self.assertEqual(vortex_result.command, "traditional-analytics-vortex-run")

    def test_live_etl_smoke_rejects_unknown_format(self) -> None:
        with self.assertRaises(ValueError):
            ShardLoomClient(binary=["shardloom"]).live_etl_smoke(
                "csv/file ingest", "fact.parquet", "dim.parquet", input_format="parquet"
            )

    def test_live_etl_csv_to_vortex_replay_runs_import_then_native_replay(self) -> None:
        binary = self.fake_cli(
            textwrap.dedent(
                """
                import json, sys
                args = sys.argv[1:]
                if args == [
                    "traditional-analytics-run",
                    "selective filter",
                    "fact.csv",
                    "dim.csv",
                    "--workspace",
                    "work",
                    "--format",
                    "json",
                ]:
                    print(json.dumps({
                        "schema_version": "shardloom.output.v1",
                        "command": "traditional-analytics-run",
                        "status": "success",
                        "summary": "csv ok",
                        "human_text": "csv ok",
                        "fallback": {"attempted": False, "allowed": False, "engine": None, "reason": "disabled"},
                        "diagnostics": [],
                        "fields": [
                            {"key": "fact_vortex_path", "value": "work/fact.vortex"},
                            {"key": "dim_vortex_path", "value": "work/dim.vortex"},
                        ],
                    }))
                elif args == [
                    "traditional-analytics-vortex-run",
                    "selective filter",
                    "work/fact.vortex",
                    "work/dim.vortex",
                    "--format",
                    "json",
                ]:
                    print(json.dumps({
                        "schema_version": "shardloom.output.v1",
                        "command": "traditional-analytics-vortex-run",
                        "status": "success",
                        "summary": "native ok",
                        "human_text": "native ok",
                        "fallback": {"attempted": False, "allowed": False, "engine": None, "reason": "disabled"},
                        "diagnostics": [],
                        "fields": [{"key": "source_format", "value": "vortex"}],
                    }))
                else:
                    raise AssertionError(args)
                """
            )
        )

        result = ShardLoomClient(binary=binary).live_etl_csv_to_vortex_replay(
            "selective filter",
            "fact.csv",
            "dim.csv",
            workspace="work",
        )

        self.assertEqual(result.csv_import.command, "traditional-analytics-run")
        self.assertEqual(
            result.native_vortex.command if result.native_vortex else None,
            "traditional-analytics-vortex-run",
        )
        self.assertEqual(result.fact_vortex_path, "work/fact.vortex")
        self.assertEqual(result.dim_vortex_path, "work/dim.vortex")
        self.assertTrue(result.native_replay_ran)
        self.assertFalse(result.fallback_attempted)

    def test_live_etl_csv_to_vortex_replay_can_skip_native_replay(self) -> None:
        binary = self.fake_cli(
            textwrap.dedent(
                """
                import json, sys
                assert sys.argv[1:] == [
                    "traditional-analytics-run",
                    "selective filter",
                    "fact.csv",
                    "dim.csv",
                    "--workspace",
                    "work",
                    "--format",
                    "json",
                ], sys.argv
                print(json.dumps({
                    "schema_version": "shardloom.output.v1",
                    "command": "traditional-analytics-run",
                    "status": "success",
                    "summary": "csv ok",
                    "human_text": "csv ok",
                    "fallback": {"attempted": False, "allowed": False, "engine": None, "reason": "disabled"},
                    "diagnostics": [],
                    "fields": [
                        {"key": "fact_vortex_path", "value": "work/fact.vortex"},
                        {"key": "dim_vortex_path", "value": "work/dim.vortex"},
                    ],
                }))
                """
            )
        )

        result = ShardLoomClient(binary=binary).live_etl_csv_to_vortex_replay(
            "selective filter",
            "fact.csv",
            "dim.csv",
            workspace="work",
            replay_native=False,
        )

        self.assertIsNone(result.native_vortex)
        self.assertFalse(result.native_replay_ran)

    def test_live_etl_csv_to_vortex_replay_requires_emitted_vortex_paths(self) -> None:
        binary = self.fake_cli(
            textwrap.dedent(
                """
                import json
                print(json.dumps({
                    "schema_version": "shardloom.output.v1",
                    "command": "traditional-analytics-run",
                    "status": "success",
                    "summary": "csv ok",
                    "human_text": "csv ok",
                    "fallback": {"attempted": False, "allowed": False, "engine": None, "reason": "disabled"},
                    "diagnostics": [],
                    "fields": [],
                }))
                """
            )
        )

        with self.assertRaises(ShardLoomProtocolError):
            ShardLoomClient(binary=binary).live_etl_csv_to_vortex_replay(
                "selective filter",
                "fact.csv",
                "dim.csv",
                workspace="work",
            )

    def test_dynamic_work_shaping_and_sizing_feedback_commands(self) -> None:
        binary = self.fake_cli(
            textwrap.dedent(
                """
                import json, sys
                assert sys.argv[1:] == [
                    "dynamic-work-shaping-plan",
                    "memory-pressure",
                    "--format",
                    "json",
                ], sys.argv
                print(json.dumps({
                    "schema_version": "shardloom.output.v1",
                    "command": "dynamic-work-shaping-plan",
                    "status": "success",
                    "summary": "ok",
                    "human_text": "ok",
                    "fallback": {"attempted": False, "allowed": False, "engine": None, "reason": "disabled"},
                    "diagnostics": [],
                    "fields": [{"key": "profile", "value": "memory-pressure"}],
                }))
                """
            )
        )

        result = ShardLoomClient(binary=binary).dynamic_work_shaping_plan(
            "memory-pressure"
        )

        self.assertEqual(result.field("profile"), "memory-pressure")

        feedback_binary = self.fake_cli(
            textwrap.dedent(
                """
                import json, sys
                assert sys.argv[1:] == [
                    "sizing-feedback-plan",
                    "8",
                    "task-too-large,memory-pressure-high",
                    "--format",
                    "json",
                ], sys.argv
                print(json.dumps({
                    "schema_version": "shardloom.output.v1",
                    "command": "sizing-feedback-plan",
                    "status": "success",
                    "summary": "ok",
                    "human_text": "ok",
                    "fallback": {"attempted": False, "allowed": False, "engine": None, "reason": "disabled"},
                    "diagnostics": [],
                    "fields": [{"key": "target_task_bytes_changed", "value": "true"}],
                }))
                """
            )
        )
        feedback = ShardLoomClient(binary=feedback_binary).sizing_feedback_plan(
            8, ["task-too-large", "memory-pressure-high"]
        )
        self.assertTrue(feedback.field_bool("target_task_bytes_changed"))

    def test_benchmark_and_world_class_plan_helpers(self) -> None:
        binary = self.fake_cli(
            textwrap.dedent(
                """
                import json, sys
                assert sys.argv[1:] == [
                    "benchmark-claim-evidence-plan",
                    "traditional-analytics",
                    "--format",
                    "json",
                ], sys.argv
                print(json.dumps({
                    "schema_version": "shardloom.output.v1",
                    "command": "benchmark-claim-evidence-plan",
                    "status": "success",
                    "summary": "ok",
                    "human_text": "ok",
                    "fallback": {"attempted": False, "allowed": False, "engine": None, "reason": "disabled"},
                    "diagnostics": [],
                    "fields": [{"key": "scope", "value": "traditional-analytics"}],
                }))
                """
            )
        )

        result = ShardLoomClient(binary=binary).benchmark_claim_evidence_plan(
            "traditional-analytics"
        )

        self.assertEqual(result.field("scope"), "traditional-analytics")

    def test_input_adapter_and_input_plan_helpers(self) -> None:
        adapters_binary = self.fake_cli(
            textwrap.dedent(
                """
                import json, sys
                assert sys.argv[1:] == ["input-adapters", "--format", "json"], sys.argv
                print(json.dumps({
                    "schema_version": "shardloom.output.v1",
                    "command": "input-adapters",
                    "status": "success",
                    "summary": "ok",
                    "human_text": "ok",
                    "fallback": {"attempted": False, "allowed": False, "engine": None, "reason": "disabled"},
                    "diagnostics": [],
                    "fields": [
                        {"key": "critical_structured_adapter_order", "value": "native_vortex,parquet,arrow_ipc,csv,jsonl"},
                        {"key": "parquet_status", "value": "planned"}
                    ],
                }))
                """
            )
        )

        adapters = ShardLoomClient(binary=adapters_binary).input_adapters()

        self.assertEqual(adapters.command, "input-adapters")
        self.assertEqual(adapters.field("parquet_status"), "planned")

        plan_binary = self.fake_cli(
            textwrap.dedent(
                """
                import json, sys
                assert sys.argv[1:] == ["input-plan", "file://tmp/data.parquet", "--format", "json"], sys.argv
                print(json.dumps({
                    "schema_version": "shardloom.output.v1",
                    "command": "input-plan",
                    "status": "success",
                    "summary": "ok",
                    "human_text": "ok",
                    "fallback": {"attempted": False, "allowed": False, "engine": None, "reason": "disabled"},
                    "diagnostics": [],
                    "fields": [{"key": "plan_only", "value": "true"}],
                }))
                """
            )
        )

        input_plan = ShardLoomClient(binary=plan_binary).input_plan(
            "file://tmp/data.parquet"
        )

        self.assertEqual(input_plan.command, "input-plan")
        self.assertTrue(input_plan.field_bool("plan_only"))


if __name__ == "__main__":
    unittest.main()
