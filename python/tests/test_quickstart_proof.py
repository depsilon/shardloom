from __future__ import annotations

import sys
import tempfile
import textwrap
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from shardloom import QuickstartProofReport, ShardLoomClient, quickstart_proof

_FAKE_CLI_ENVELOPE_PRELUDE = textwrap.dedent(
    """
    import json as _shardloom_json

    _shardloom_original_json_dumps = _shardloom_json.dumps

    def _shardloom_fill_typed_envelope(value):
        if isinstance(value, dict) and value.get("schema_version") == "shardloom.output.v2":
            value = dict(value)
            value.setdefault("result", {"fields": value.get("fields", [])})
            value.setdefault("result_refs", [])
            value.setdefault("artifacts", [])
            value.setdefault("artifact_refs", [])
            value.setdefault("certificates", [])
            value.setdefault("policy", {"fields": []})
            value.setdefault("lifecycle", {"fields": []})
            value.setdefault("capability_snapshot", {"fields": []})
        return value

    def _shardloom_json_dumps(value, *args, **kwargs):
        return _shardloom_original_json_dumps(
            _shardloom_fill_typed_envelope(value),
            *args,
            **kwargs,
        )

    _shardloom_json.dumps = _shardloom_json_dumps
    """
)


class QuickstartProofTests(unittest.TestCase):
    def fake_cli(self, body: str) -> list[str]:
        tempdir = tempfile.TemporaryDirectory()
        self.addCleanup(tempdir.cleanup)
        path = Path(tempdir.name) / "fake_shardloom.py"
        path.write_text(_FAKE_CLI_ENVELOPE_PRELUDE + "\n" + body, encoding="utf-8")
        return [sys.executable, str(path)]

    def test_quickstart_proof_collects_planning_and_optional_execution(self) -> None:
        binary = self.fake_cli(
            textwrap.dedent(
                """
                import json, sys

                args = sys.argv[1:]

                def emit(command, fields=None, *, status="success", diagnostics=None, returncode=0):
                    print(json.dumps({
                        "schema_version": "shardloom.output.v2",
                        "command": command,
                        "status": status,
                        "summary": "ok",
                        "human_text": "ok",
                        "fallback": {"attempted": False, "allowed": False, "engine": None, "reason": "disabled"},
                        "diagnostics": diagnostics or [],
                        "fields": [
                            {"key": "fallback_execution_allowed", "value": "false"},
                            *(fields or []),
                        ],
                    }))
                    sys.exit(returncode)

                command = args[0]
                if args == ["status", "--format", "json"]:
                    emit("status", [
                        {"key": "cli_binary_version", "value": "0.1.0-test"},
                        {"key": "surface_components", "value": "python_api"},
                    ])
                if args == ["capabilities", "python", "--format", "json"]:
                    emit("capabilities", [{"key": "scope", "value": "python"}])
                if args == ["capabilities", "deployment", "--format", "json"]:
                    emit("capabilities", [{"key": "scope", "value": "deployment"}])
                if command == "capabilities" and args[-2:] == ["--format", "json"]:
                    emit("capabilities", [
                        {"key": "scope", "value": args[1]},
                        {"key": "capability_status", "value": "planned"},
                        {"key": "fallback_attempted", "value": "false"},
                    ])
                if args == ["input-adapters", "--format", "json"]:
                    emit("input-adapters", [
                        {"key": "plan_only", "value": "true"},
                        {"key": "critical_structured_adapter_order", "value": "native_vortex,parquet,csv,jsonl"},
                        {"key": "write_io", "value": "false"},
                    ])
                if command in {"vortex-read-plan", "input-plan"} and args[-2:] == ["--format", "json"]:
                    emit(command, [
                        {"key": "plan_only", "value": "true"},
                        {"key": "capability_status", "value": "planned"},
                        {"key": "data_read", "value": "false"},
                        {"key": "data_materialized", "value": "false"},
                        {"key": "object_store_io", "value": "false"},
                        {"key": "write_io", "value": "false"},
                        {"key": "native_vortex", "value": "false"},
                        {"key": "execution", "value": "not_performed"},
                    ])
                if command in {"explain", "estimate"} and args[-2:] == ["--format", "json"]:
                    emit(command, [
                        {"key": "execution", "value": "not_performed"},
                    ], status="unsupported", diagnostics=[{
                        "code": "SL_NOT_IMPLEMENTED",
                        "severity": "error",
                        "category": "unsupported_feature",
                        "message": "unsupported",
                        "feature": command,
                        "reason": f"{command} is report-only",
                        "suggested_next_step": "inspect readiness",
                        "fallback": {"attempted": False, "allowed": False, "engine": None, "reason": "disabled"},
                    }], returncode=1)
                if args == ["execution-certificate-plan", "--format", "json"]:
                    emit("execution-certificate-plan", [{"key": "execution", "value": "not_performed"}])
                if args == ["native-io-envelope-plan", "--format", "json"]:
                    emit("native-io-envelope-plan", [
                        {"key": "materialization_boundary_reported", "value": "true"},
                        {"key": "data_read", "value": "false"},
                        {"key": "write_io", "value": "false"},
                    ])
                if command in {
                    "vortex-output-plan",
                    "translation-plan",
                    "plan-export",
                    "vortex-write-intent-plan",
                    "vortex-output-payload-plan",
                    "vortex-staged-manifest-file-plan",
                    "vortex-commit-marker-plan",
                    "vortex-commit-intent-plan",
                    "vortex-commit-protocol-plan",
                    "vortex-local-commit-recovery-plan",
                    "table-intelligence-plan",
                    "table-compat-plan",
                    "layout-health-plan",
                    "compaction-plan",
                    "cg9-catalog-metadata-gate",
                    "object-store-request-plan",
                    "object-store-range-plan",
                    "object-store-coalesce-plan",
                    "object-store-schedule-plan",
                    "object-store-checkpoint-retry-plan",
                    "object-store-commit-plan",
                    "correctness-plan",
                    "benchmark-claim-evidence-plan",
                    "world-class-sufficiency-plan",
                } and args[-2:] == ["--format", "json"]:
                    fields = [
                        {"key": "execution", "value": "not_performed"},
                        {"key": "plan_only", "value": "true"},
                        {"key": "data_read", "value": "false"},
                        {"key": "data_materialized", "value": "false"},
                        {"key": "object_store_io", "value": "false"},
                        {"key": "write_io", "value": "false"},
                        {"key": "output_data_written", "value": "false"},
                        {"key": "manifest_written", "value": "false"},
                        {"key": "manifest_committed", "value": "false"},
                        {"key": "upstream_vortex_write_called", "value": "false"},
                        {"key": "fallback_attempted", "value": "false"},
                    ]
                    if command == "benchmark-claim-evidence-plan":
                        fields.append({"key": "claim_evidence_status", "value": "needs_evidence"})
                    emit(command, fields)
                if command == "run" and args[1:6] == ["cli", "--input", "fixture.vortex", "--input-format", "vortex"] and args[-2:] == ["--format", "json"]:
                    primitive = args[args.index("--vortex-primitive") + 1]
                    internal = {
                        "count": "vortex-run",
                        "count_where": "vortex-count-where",
                        "filter": "vortex-filter",
                        "project": "vortex-project",
                        "filter_project": "vortex-filter-project",
                    }[primitive]
                    emit("run", [
                        {"key": "public_workflow_route_attached", "value": "true"},
                        {"key": "public_workflow_resolved_internal_command", "value": internal},
                        {"key": "local_primitive_native_io_certified", "value": "true"},
                        {"key": "local_primitive_native_io_fallback_attempted", "value": "false"},
                        {"key": "local_primitive_execution_certificate_correctness_passed", "value": "true"},
                        {"key": "local_primitive_execution_certificate_fallback_attempted", "value": "false"},
                    ])
                raise AssertionError(args)
                """
            )
        )

        report = quickstart_proof(
            ShardLoomClient(binary=binary),
            fixture="fixture.vortex",
            run_local_vortex=True,
            memory_gb=1,
            max_parallelism=2,
        )

        self.assertIsInstance(report, QuickstartProofReport)
        self.assertFalse(report.fallback_attempted)
        self.assertTrue(report.all_no_write_planning)
        self.assertTrue(report.local_execution_ran)
        self.assertTrue(report.local_execution_certified)
        self.assertIn("vortex-run", report.commands)
        self.assertEqual(report.workflow_report.explain.status, "unsupported")
        self.assertIn("explain is report-only", report.workflow_report.unsupported_reasons)


if __name__ == "__main__":
    unittest.main()
