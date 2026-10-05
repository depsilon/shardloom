from __future__ import annotations

import importlib.util
import sys
import unittest
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]


def load_checker():
    scripts_path = str(REPO_ROOT / "scripts")
    if scripts_path not in sys.path:
        sys.path.insert(0, scripts_path)
    path = REPO_ROOT / "scripts" / "check_v1_correctness_conformance.py"
    spec = importlib.util.spec_from_file_location("v1_correctness_conformance_test", path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    try:
        spec.loader.exec_module(module)
    finally:
        sys.modules.pop(spec.name, None)
    return module


class V1CorrectnessConformanceReportTests(unittest.TestCase):
    def setUp(self) -> None:
        self.checker = load_checker()

    def vortex_report(self) -> dict:
        return {
            "schema_version": "shardloom.v1_vortex_runtime_scope_report.v1",
            "status": "passed",
            "supported_primitive_route_ids": [f"primitive_{index}" for index in range(11)],
            "evidence_class": "declarative_specification",
            "runtime_execution_performed": False,
            "performance_evidence_produced": False,
            "user_route_ids": ["native_vortex_query", "object_store_lakehouse_runtime"],
            "user_route_rows": [
                {
                    "route_id": "native_vortex_query",
                    "owner": "shared_native_workflow",
                    "route_runtime_status": "global_runtime_supported",
                    "fallback_attempted": False,
                    "external_engine_invoked": False,
                },
                {
                    "route_id": "object_store_lakehouse_runtime",
                    "owner": "external_environment_gate",
                    "route_runtime_status": "external_environment_gate_pending",
                    "fallback_attempted": False,
                    "external_engine_invoked": False,
                },
            ],
            "local_vortex_primitive_v1_scope_ready": True,
            "local_vortex_primitive_all_runtime_supported": True,
            "local_vortex_primitive_all_no_fallback_no_external_engine": True,
            "user_route_v1_vortex_scope_ready": True,
            "all_no_fallback_no_external_engine": True,
            "claim_gate_status": "not_claim_grade",
            "performance_claim_allowed": False,
            "production_claim_allowed": False,
            "spark_replacement_claim_allowed": False,
            "fallback_attempted": False,
            "external_engine_invoked": False,
        }

    def source_report(self) -> dict:
        return {
            "schema_version": "shardloom.v1_source_prepared_state_scope_report.v1",
            "status": "passed",
            "evidence_class": "declarative_specification",
            "runtime_execution_performed": False,
            "performance_evidence_produced": False,
            "report_id": "prod-v1-1c.source_prepared_state_scope",
            "canonical_route": (
                "declared input or source-free expression -> native Vortex admission -> "
                "native_vortex_unified_plan -> typed result or declared sink"
            ),
            "route_ids": ["native_vortex_query"],
            "supported_input_formats": ["csv", "json", "jsonl", "parquet", "arrow-ipc", "avro", "orc"],
            "state_owner": "ResidentVortexSession",
            "reuse_scope": "native_session_or_explicit_vortex_artifact",
            "reuse_policy": "validate_source_generation_and_declaration_before_each_execution",
            "query_answers_cached": False,
            "invalidation_case_ids": [
                "first_request",
                "same_source_same_declaration",
                "source_changed",
                "memory_declaration_changed",
                "resource_policy_changed",
                "missing_artifact",
                "artifact_changed",
            ],
            "golden_fixture_paths": ["one", "two", "three"],
            "required_runtime_fields": ["plan", "operation", "fallback", "external"],
            "unsupported_boundary_ids": ["global_hidden_cache"],
            "all_no_fallback_no_external_engine": True,
            "v1_scope_ready": True,
            "claim_gate_status": "not_claim_grade",
            "performance_claim_allowed": False,
            "production_claim_allowed": False,
            "spark_replacement_claim_allowed": False,
            "fallback_attempted": False,
            "external_engine_invoked": False,
        }

    def test_vortex_validator_accepts_declarative_routes_and_external_gate(self) -> None:
        summary, blockers = self.checker._validate_vortex(self.vortex_report())
        self.assertEqual(blockers, [])
        self.assertEqual(summary["primitive_route_count"], 11)
        self.assertFalse(summary["runtime_execution_performed"])
        self.assertFalse(summary["performance_evidence_produced"])

    def test_vortex_validator_rejects_runtime_claims_or_lost_external_boundary(self) -> None:
        report = self.vortex_report()
        report["runtime_execution_performed"] = True
        report["user_route_rows"][1]["owner"] = "shared_native_workflow"
        _, blockers = self.checker._validate_vortex(report)
        self.assertTrue(any("runtime_execution_performed" in item for item in blockers))
        self.assertTrue(any("externally gated" in item for item in blockers))

    def test_source_validator_accepts_current_session_reuse_contract(self) -> None:
        summary, blockers = self.checker._validate_source(self.source_report())
        self.assertEqual(blockers, [])
        self.assertEqual(summary["route_count"], 1)
        self.assertEqual(summary["state_owner"], "ResidentVortexSession")
        self.assertFalse(summary["query_answers_cached"])

    def test_source_validator_rejects_hidden_cache_and_stale_reuse_policy(self) -> None:
        report = self.source_report()
        report["query_answers_cached"] = True
        report["reuse_policy"] = "global_cache"
        report["invalidation_case_ids"].append("global_cache_hit")
        _, blockers = self.checker._validate_source(report)
        self.assertTrue(any("query_answers_cached" in item for item in blockers))
        self.assertTrue(any("reuse_policy" in item for item in blockers))
        self.assertTrue(any("invalidation_case_ids mismatch" in item for item in blockers))

    def test_output_scope_cannot_substitute_declarations_for_runtime_evidence(self) -> None:
        report = {
            "schema_version": "shardloom.v1_local_output_sink_scope_report.v1",
            "status": "passed",
            "supported_output_formats": ["json", "jsonl", "csv", "parquet", "arrow-ipc", "avro", "orc", "vortex"],
            "user_write_methods": ["write", "write_json", "write_jsonl", "write_csv", "write_parquet", "write_arrow_ipc", "write_avro", "write_orc", "write_vortex", "fanout"],
            "output_route_ids": ["native_vortex_query"],
            "evidence_class": "declarative_contract",
            "declarative_contract_ready": True,
            "runtime_evidence_verified": False,
            "fallback_attempted": False,
            "external_engine_invoked": False,
            "production_claim_allowed": False,
            "performance_claim_allowed": False,
            "spark_replacement_claim_allowed": False,
        }
        summary, blockers = self.checker._validate_output(report)
        self.assertEqual(blockers, [])
        self.assertEqual(summary["output_route_count"], 1)
        self.assertFalse(summary["runtime_evidence_verified"])
        for field, value in (
            ("runtime_evidence_verified", True),
            ("runtime_evidence_verified", None),
            ("evidence_class", "benchmark_replay"),
            ("declarative_contract_ready", False),
            ("output_route_ids", ["retired_benchmark_route"]),
        ):
            with self.subTest(field=field, value=value):
                _, blockers = self.checker._validate_output({**report, field: value})
                self.assertTrue(blockers)


if __name__ == "__main__":
    unittest.main()
