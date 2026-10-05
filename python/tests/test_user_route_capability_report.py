from __future__ import annotations

import importlib.util
import sys
import unittest
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]


def load_route_module():
    module_path = REPO_ROOT / "scripts" / "check_user_route_capability_report.py"
    spec = importlib.util.spec_from_file_location("route_report_for_test", module_path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    try:
        spec.loader.exec_module(module)
    finally:
        sys.modules.pop(spec.name, None)
    return module


class UserRouteCapabilityReportTests(unittest.TestCase):
    def test_current_report_is_static_and_uses_one_shared_native_route(self) -> None:
        module = load_route_module()
        report = module.build_report(REPO_ROOT)
        self.assertEqual(report["status"], "passed", report["blockers"])
        self.assertEqual(report["report_kind"], "static_capability_discovery")
        self.assertFalse(report["runtime_execution_performed"])
        self.assertFalse(report["performance_evidence_produced"])
        self.assertTrue(report["acceptance_summary"]["static_discovery_is_not_runtime_or_performance_proof"])
        self.assertEqual(set(report["route_ids"]), {"native_vortex_query", "object_store_lakehouse_runtime"})
        self.assertEqual(report["public_front_door_route_count"], 4)
        self.assertEqual(report["public_route_reuse_matrix_count"], 9)
        self.assertFalse(report["fallback_attempted"])
        self.assertFalse(report["external_engine_invoked"])
        by_id = {row["route_id"]: row for row in report["rows"]}
        self.assertEqual(by_id["native_vortex_query"]["owner"], "shared_native_workflow")
        self.assertEqual(by_id["native_vortex_query"]["route_runtime_status"], "global_runtime_supported")
        self.assertEqual(by_id["object_store_lakehouse_runtime"]["route_runtime_status"], "external_environment_gate_pending")
        self.assertNotIn("local_file_benchmark_rows", report)
        self.assertNotIn("v1_example_scenario_ids", report)

    def test_route_validator_rejects_wrong_owner_missing_examples_and_claims(self) -> None:
        module = load_route_module()
        report = module.load_report(REPO_ROOT)
        rows = [module.row_payload(row) for row in report.rows]
        native = next(row for row in rows if row["route_id"] == "native_vortex_query")
        native["owner"] = "format_specific_runtime"
        native["input_examples"] = ["input.csv"]
        native["performance_claim_allowed"] = True
        blockers = module.validate_rows(report, rows)
        self.assertTrue(any("owned by shared_native_workflow" in item for item in blockers))
        self.assertTrue(any("missing input example" in item for item in blockers))
        self.assertTrue(any("performance_claim_allowed" in item for item in blockers))

    def test_front_door_and_reuse_validators_reject_split_execution_or_retired_fields(self) -> None:
        module = load_route_module()
        report = module.load_report(REPO_ROOT)
        route_rows = [module.row_payload(row) for row in report.rows]
        doors = [module.public_front_door_row_payload(row) for row in report.public_front_door_route_rows]
        doors[0]["owning_route_id"] = "format_specific_runtime"
        doors[1]["external_engine_invoked"] = True
        doors[2]["public_user_surface"] = "ctx.unknown_context_method()"
        door_blockers = module.validate_public_front_door_routes(doors, route_rows)
        self.assertTrue(any("shared native query route" in item for item in door_blockers))
        self.assertTrue(any("no-fallback" in item for item in door_blockers))
        self.assertTrue(any("references missing Context methods" in item for item in door_blockers))

        reuse = [module.public_route_reuse_matrix_row_payload(row) for row in report.public_route_reuse_matrix_rows]
        reuse[0]["primary_route_id"] = "compatibility_sql_runtime"
        reuse[0]["shared_runtime_spine"] = "sql-local-source-smoke"
        reuse[1]["typed_result_or_sink_contract"] = "decoded_arrow_only"
        reuse[2]["alternate_route_ids"] = ["duckdb"]
        reuse_blockers = module.validate_public_route_reuse_matrix(reuse, route_rows)
        self.assertTrue(any("native_vortex_query as its primary route" in item for item in reuse_blockers))
        self.assertTrue(any("shared native runtime spine" in item for item in reuse_blockers))
        self.assertTrue(any("typed result or declared sink contract" in item for item in reuse_blockers))
        self.assertTrue(any("retired alternate-route" in item for item in reuse_blockers))


if __name__ == "__main__":
    unittest.main()
