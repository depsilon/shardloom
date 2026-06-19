from __future__ import annotations

import importlib.util
import json
import sys
import tempfile
import textwrap
import unittest
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]


def load_scope_module():
    module_path = REPO_ROOT / "scripts" / "check_v1_front_door_runtime_scope.py"
    spec = importlib.util.spec_from_file_location(
        "check_v1_front_door_runtime_scope_for_test",
        module_path,
    )
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    try:
        spec.loader.exec_module(module)
    finally:
        sys.modules.pop(spec.name, None)
    return module


def load_scenario_support_module():
    module_path = REPO_ROOT / "examples" / "local-python-benchmark-scenarios" / "scenario_support.py"
    spec = importlib.util.spec_from_file_location(
        "local_python_benchmark_scenario_support_for_test",
        module_path,
    )
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    try:
        spec.loader.exec_module(module)
    finally:
        sys.modules.pop(spec.name, None)
    return module


class V1FrontDoorRuntimeScopeTests(unittest.TestCase):
    def fake_cli(self) -> list[str]:
        tempdir = tempfile.TemporaryDirectory()
        self.addCleanup(tempdir.cleanup)
        path = Path(tempdir.name) / "fake_shardloom.py"
        path.write_text(
            textwrap.dedent(
                """
                import json
                import sys


                def envelope(command, status, diagnostics=None):
                    diagnostics = diagnostics or []
                    result_jsonl = '{"id":1,"group_key":10,"value":90}\\n'
                    print(json.dumps({
                        "schema_version": "shardloom.output.v2",
                        "command": command,
                        "status": status,
                        "summary": status,
                        "human_text": status,
                        "fallback": {
                            "attempted": False,
                            "allowed": False,
                            "engine": None,
                            "reason": "disabled",
                        },
                        "diagnostics": diagnostics,
                        "result": {"fields": []},
                        "result_refs": [],
                        "artifacts": [],
                        "artifact_refs": [],
                        "certificates": [],
                        "policy": {"fields": []},
                        "lifecycle": {"fields": []},
                        "capability_snapshot": {"fields": []},
                        "fields": [
                            {"key": "result_jsonl", "value": result_jsonl},
                            {"key": "output_row_count", "value": "1"},
                            {"key": "claim_gate_status", "value": "fixture_smoke_only"},
                            {"key": "blocker_id", "value": "cg21.workflow.v1_front_door_scope"},
                            {"key": "required_evidence", "value": "v1_front_door_runtime_scope"},
                            {"key": "runtime_execution", "value": "false"},
                            {"key": "data_read", "value": "false"},
                            {"key": "write_io", "value": "false"},
                            {"key": "timing_scope", "value": "hot_runtime"},
                            {"key": "source_format", "value": "csv"},
                            {"key": "output_format", "value": "jsonl"},
                            {"key": "source_read_millis", "value": "0.1"},
                            {"key": "fallback_attempted", "value": "false"},
                            {"key": "external_engine_invoked", "value": "false"},
                        ],
                    }))


                args = sys.argv[1:]
                command = args[0] if args else "missing"
                text = " ".join(args).lower()
                if command == "workflow-unsupported-plan":
                    envelope(
                        command,
                        "unsupported",
                        diagnostics=[{
                            "code": "SL_UNSUPPORTED_SQL",
                            "severity": "error",
                            "category": "unsupported_feature",
                            "message": "unsupported workflow operation",
                            "feature": "workflow_unsupported_plan",
                            "reason": "not in v1 scope",
                            "suggested_next_step": "inspect v1 front-door runtime scope",
                            "fallback": {
                                "attempted": False,
                                "allowed": False,
                                "engine": None,
                                "reason": "disabled",
                            },
                        }],
                    )
                    sys.exit(1)
                if "raw_event_time" in text and "date32" in text:
                    envelope(
                        command,
                        "error",
                        diagnostics=[{
                            "code": "SL_UNSUPPORTED_CAST",
                            "severity": "error",
                            "category": "unsupported_feature",
                            "message": "date32 cast rejected current data",
                            "feature": "cast",
                            "reason": "malformed timestamp",
                            "suggested_next_step": "clean source field before date32 cast",
                            "fallback": {
                                "attempted": False,
                                "allowed": False,
                                "engine": None,
                                "reason": "disabled",
                            },
                        }],
                    )
                    sys.exit(1)
                envelope(command, "success")
                """
            ),
            encoding="utf-8",
        )
        return [sys.executable, str(path)]

    def test_scope_validator_passes_current_repo_contract(self) -> None:
        module = load_scope_module()

        report = module.build_report(REPO_ROOT)

        self.assertEqual(report["status"], "passed", report["blockers"])
        self.assertEqual(
            report["schema_version"],
            "shardloom.v1_front_door_runtime_scope_report.v1",
        )
        self.assertTrue(report["scoped_local_front_door_parity_supported"])
        self.assertTrue(report["all_no_fallback_no_external_engine"])
        self.assertFalse(report["performance_equivalence_claim_allowed"])
        self.assertIn("selective_filter", report["example_scenario_ids"])
        self.assertEqual(report["expected_error_scenario_ids"], [])

    def test_documented_benchmark_scenarios_execute_sequentially_through_python_surface(self) -> None:
        scenario_support = load_scenario_support_module()
        with tempfile.TemporaryDirectory() as tempdir:
            run_dir = Path(tempdir) / "run"

            payload = scenario_support.run_scenarios(
                repo_root=REPO_ROOT,
                run_dir=run_dir,
                binary=self.fake_cli(),
                profile_order=("release", "debug"),
            )

        self.assertTrue(payload["passed"], json.dumps(payload, indent=2, sort_keys=True))
        self.assertEqual(payload["scenario_count"], 9)
        by_name = {row["name"]: row for row in payload["results"]}
        self.assertEqual(
            set(by_name),
            {
                "selective_filter",
                "filter_projection_limit",
                "group_by_aggregation",
                "hash_join",
                "global_top_n",
                "clean_cast_filter_write",
                "malformed_timestamp_cast",
                "null_heavy_aggregate",
                "nested_json_field_scan",
            },
        )
        self.assertFalse(by_name["malformed_timestamp_cast"]["expected_error"])
        for result in payload["results"]:
            self.assertFalse(result["fallback_attempted"], result)
            self.assertFalse(result["external_engine_invoked"], result)
            self.assertIn("python_wall_millis", result["timing_components"])

    def test_unsupported_front_door_shapes_return_no_fallback_reports(self) -> None:
        source_path = str(REPO_ROOT / "python" / "src")
        if source_path not in sys.path:
            sys.path.insert(0, source_path)
        from shardloom import context
        from shardloom import UnsupportedWorkflowOperationReport

        ctx = context(repo_root=REPO_ROOT, binary=self.fake_cli())
        frame = ctx.read_csv("events.csv", schema={"id": "int64"})
        unsupported_reports = [
            frame.sql("SELECT * FROM remote_table"),
            frame.query("id > @threshold", threshold=10),
            frame.merge("remote.parquet", on="id", how="outer", indicator=True),
        ]

        for report in unsupported_reports:
            self.assertIsInstance(report, UnsupportedWorkflowOperationReport)
            self.assertFalse(report.runtime_execution)
            self.assertFalse(report.data_read)
            self.assertFalse(report.write_io)
            self.assertFalse(report.fallback_attempted)
            self.assertFalse(report.external_engine_invoked)
            self.assertIsNotNone(report.blocker_id)


if __name__ == "__main__":
    unittest.main()
