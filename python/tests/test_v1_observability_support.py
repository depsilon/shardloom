from __future__ import annotations

import importlib.util
import sys
import unittest
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]


def load_checker(script: str = "check_v1_observability_support.py"):
    scripts_path = str(REPO_ROOT / "scripts")
    if scripts_path not in sys.path:
        sys.path.insert(0, scripts_path)
    path = REPO_ROOT / "scripts" / script
    spec = importlib.util.spec_from_file_location(f"{path.stem}_test", path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    try:
        spec.loader.exec_module(module)
    finally:
        sys.modules.pop(spec.name, None)
    return module


def route_report() -> dict:
    return {
        "schema_version": "shardloom.user_route_capability_report.v1",
        "status": "passed",
        "report_kind": "static_capability_discovery",
        "runtime_execution_performed": False,
        "performance_evidence_produced": False,
        "claim_gate_status": "not_claim_grade",
        "route_count": 2,
        "route_ids": ["native_vortex_query", "object_store_lakehouse_runtime"],
        "rows": [
            {
                "route_id": "native_vortex_query",
                "owner": "shared_native_workflow",
                "route_runtime_status": "global_runtime_supported",
                "fallback_attempted": False,
                "external_engine_invoked": False,
                "performance_claim_allowed": False,
                "production_claim_allowed": False,
                "spark_replacement_claim_allowed": False,
            },
            {
                "route_id": "object_store_lakehouse_runtime",
                "owner": "GAR-RUNTIME-IMPL-6D:last_order.object_store_lakehouse_catalog",
                "route_runtime_status": "external_environment_gate_pending",
                "fallback_attempted": False,
                "external_engine_invoked": False,
                "performance_claim_allowed": False,
                "production_claim_allowed": False,
                "spark_replacement_claim_allowed": False,
            },
        ],
        "public_front_door_route_count": 4,
        "public_front_door_route_rows": [
            {
                "front_door_id": front_door_id,
                "owning_route_id": "native_vortex_query",
                "fallback_attempted": False,
                "external_engine_invoked": False,
            }
            for front_door_id in (
                "local_source_vortex_middle_front_door",
                "native_vortex_front_door",
                "declared_memory_front_door",
                "source_free_sql_front_door",
            )
        ],
        "public_route_reuse_matrix_count": 9,
        "public_route_reuse_matrix_rows": [
            {
                "operation_family": operation_family,
                "primary_route_id": "native_vortex_query",
                "typed_result_or_sink_contract": "complete_typed_rows_or_committed_declared_output",
                "fallback_attempted": False,
                "external_engine_invoked": False,
            }
            for operation_family in (
                "filter_project_limit",
                "group_aggregate",
                "join",
                "ordered_rows",
                "distinct",
                "string_expressions",
                "casts_and_nulls",
                "declared_sinks",
                "memory_and_source_free",
            )
        ],
        "fallback_attempted": False,
        "external_engine_invoked": False,
        "all_no_fallback_no_external_engine": True,
        "performance_claim_allowed": False,
        "production_claim_allowed": False,
        "spark_replacement_claim_allowed": False,
        "acceptance_summary": {
            "shared_native_route_owns_public_front_doors": True,
            "public_route_reuse_matrix_complete": True,
            "static_discovery_is_not_runtime_or_performance_proof": True,
            "no_fallback_no_external_engine": True,
        },
    }


class V1ObservabilityUserRouteReportTests(unittest.TestCase):
    def setUp(self) -> None:
        self.checker = load_checker()

    def test_actual_route_report_satisfies_observability_contract(self) -> None:
        producer = load_checker("check_user_route_capability_report.py")
        report = producer.build_report(REPO_ROOT)
        self.assertEqual(report["status"], "passed")
        self.assertEqual(report["blockers"], [])
        summary, blockers = self.checker.validate_user_route_report(report)
        self.assertEqual(blockers, [])
        self.assertEqual(summary["status"], "passed")

    def test_validator_rejects_missing_or_invalid_route_safety_fields(self) -> None:
        for field in (
            "runtime_execution_performed", "performance_evidence_produced",
            "fallback_attempted", "external_engine_invoked",
        ):
            for invalid in (None, True, 0, "false"):
                with self.subTest(field=field, invalid=invalid):
                    report = route_report()
                    if invalid is None:
                        report.pop(field)
                    else:
                        report[field] = invalid
                    _, blockers = self.checker.validate_user_route_report(report)
                    self.assertTrue(any(field in item for item in blockers), blockers)
        for invalid in (None, False, 1, "true"):
            with self.subTest(field="all_no_fallback_no_external_engine", invalid=invalid):
                report = route_report()
                if invalid is None:
                    report.pop("all_no_fallback_no_external_engine")
                else:
                    report["all_no_fallback_no_external_engine"] = invalid
                _, blockers = self.checker.validate_user_route_report(report)
                self.assertTrue(any("all_no_fallback_no_external_engine" in item for item in blockers))

    def test_validator_rejects_external_status_change_without_owner_change(self) -> None:
        report = route_report()
        report["rows"][1]["route_runtime_status"] = "global_runtime_supported"
        _, blockers = self.checker.validate_user_route_report(report)
        self.assertTrue(any("externally gated" in item for item in blockers))

    def test_validator_accepts_static_route_declarations_without_runtime_claims(self) -> None:
        summary, blockers = self.checker.validate_user_route_report(route_report())
        self.assertEqual(blockers, [])
        self.assertEqual(summary["status"], "passed")
        self.assertEqual(summary["route_count"], 2)
        self.assertEqual(summary["public_front_door_route_count"], 4)
        self.assertEqual(summary["public_route_reuse_matrix_count"], 9)
        self.assertEqual(summary["report_kind"], "static_capability_discovery")
        self.assertFalse(summary["runtime_execution_performed"])

    def test_validator_rejects_external_route_admission_and_execution_fallback(self) -> None:
        report = route_report()
        report["rows"][1]["owner"] = "shared_native_workflow"
        report["public_front_door_route_rows"][0]["fallback_attempted"] = True
        report["runtime_execution_performed"] = True
        _, blockers = self.checker.validate_user_route_report(report)
        self.assertTrue(any("externally gated" in item for item in blockers))
        self.assertTrue(any("fallback_attempted" in item for item in blockers))
        self.assertTrue(any("runtime_execution_performed" in item for item in blockers))

    def test_validator_rejects_missing_shared_route_coverage_or_claim_boundary(self) -> None:
        report = route_report()
        report["public_route_reuse_matrix_rows"].pop()
        report["acceptance_summary"]["static_discovery_is_not_runtime_or_performance_proof"] = False
        _, blockers = self.checker.validate_user_route_report(report)
        self.assertTrue(any("nine shared-route" in item for item in blockers))
        self.assertTrue(any("static_discovery_is_not_runtime_or_performance_proof" in item for item in blockers))


if __name__ == "__main__":
    unittest.main()
