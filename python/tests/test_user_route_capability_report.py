from __future__ import annotations

import importlib.util
import sys
import unittest
from pathlib import Path
from types import SimpleNamespace


REPO_ROOT = Path(__file__).resolve().parents[2]


def load_route_module():
    module_path = REPO_ROOT / "scripts" / "check_user_route_capability_report.py"
    spec = importlib.util.spec_from_file_location(
        "check_user_route_capability_report_for_test",
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


class UserRouteCapabilityReportTests(unittest.TestCase):
    def test_current_route_report_names_vortex_boundaries_and_no_fallback(self) -> None:
        module = load_route_module()

        report = module.build_report(REPO_ROOT)

        self.assertEqual(report["status"], "passed", report["blockers"])
        self.assertEqual(report["schema_version"], "shardloom.user_route_capability_report.v1")
        self.assertGreaterEqual(report["route_count"], 15)
        self.assertEqual(report["unsupported_local_benchmark_route_ids"], [])
        self.assertTrue(report["all_no_fallback_no_external_engine"])
        self.assertFalse(report["flexible_anything_claim_allowed"])
        self.assertFalse(report["performance_equivalence_claim_allowed"])
        self.assertFalse(report["production_claim_allowed"])
        self.assertFalse(report["spark_replacement_claim_allowed"])
        self.assertTrue(report["local_vortex_primitive_all_runtime_supported"])
        self.assertTrue(report["local_vortex_primitive_all_no_fallback_no_external_engine"])
        self.assertEqual(
            set(report["local_vortex_primitive_command_coverage"]),
            {
                "vortex-run",
                "vortex-count-where",
                "vortex-filter",
                "vortex-project",
                "vortex-filter-project",
            },
        )

        by_id = {row["route_id"]: row for row in report["rows"]}
        prepare_first = by_id["local_file_prepare_once_first_query"]
        self.assertIn("SourceState", prepare_first["vortex_normalization_point"])
        self.assertIn("VortexPreparedState", prepare_first["vortex_normalization_point"])
        self.assertEqual(prepare_first["execution_mode"], "prepared_vortex")
        self.assertIn("prepared query", prepare_first["output_route"])

        native = by_id["native_vortex_query"]
        self.assertEqual(native["vortex_normalization_point"], "native_vortex_boundary")
        self.assertEqual(native["route_runtime_status"], "scoped_runtime_supported")
        self.assertNotIn("write_vortex", native["recommended_user_surface"])

        materialized = by_id["materialized_python_snapshot_reentry"]
        self.assertIn("materialized snapshot", materialized["vortex_normalization_point"])
        self.assertIn("Vortex-preparable", materialized["vortex_normalization_point"])
        self.assertFalse(materialized["external_engine_invoked"])

        primitive_rows = {
            row["route_id"]: row for row in report["local_vortex_primitive_rows"]
        }
        self.assertIn("vortex_count_all", primitive_rows)
        self.assertIn("vortex_filter_project_limit_collect", primitive_rows)
        self.assertEqual(
            primitive_rows["vortex_count_all"]["vortex_normalization_point"],
            "native_vortex_boundary",
        )
        self.assertTrue(
            primitive_rows["vortex_filter_project_limit_collect"][
                "supports_source_order_limit"
            ]
        )

    def test_context_route_selector_filters_by_input_and_output(self) -> None:
        src = REPO_ROOT / "python" / "src"
        if str(src) not in sys.path:
            sys.path.insert(0, str(src))
        from shardloom import ShardLoomContext

        routes = ShardLoomContext(client=None).user_route_capability_report()
        matches = routes.routes_for(
            input_family="local_compat_file",
            desired_output="prepared_query_result",
        )

        self.assertIn(
            "local_file_prepare_once_first_query",
            {row.route_id for row in matches},
        )
        self.assertIn(
            "local_file_prepare_once_batch",
            {row.route_id for row in routes.routes_for(input_family="local_compat_file")},
        )
        self.assertEqual(
            routes.route("local_vortex_primitive_report").vortex_normalization_point,
            "native_vortex_boundary",
        )
        primitives = ShardLoomContext(client=None).local_vortex_primitive_route_report()
        self.assertEqual(
            primitives.route("vortex_filter_project_limit_collect").cli_command,
            "vortex-filter-project",
        )
        self.assertIn(
            "vortex_select_star_limit_collect",
            primitives.source_order_limit_route_ids,
        )

    def test_validator_rejects_unsupported_or_overclaimed_route_rows(self) -> None:
        module = load_route_module()
        route_report = module.load_report(REPO_ROOT)
        rows = [module.row_payload(row) for row in route_report.rows]
        rows[0]["route_runtime_status"] = "unsupported"
        rows[0]["performance_claim_allowed"] = True
        rows[0]["fallback_attempted"] = True
        for row in rows:
            if row["route_id"] == "local_vortex_primitive_report":
                row["recommended_user_surface"] = (
                    "ctx.read_vortex(path).write_vortex('out.vortex')"
                )
                break
        fake_report = SimpleNamespace(
            all_no_fallback_no_external_engine=False,
            flexible_anything_claim_allowed=True,
            performance_equivalence_claim_allowed=True,
            production_claim_allowed=False,
            spark_replacement_claim_allowed=False,
            claim_gate_status="not_claim_grade",
            unsupported_local_benchmark_route_ids=(rows[0]["route_id"],),
        )

        blockers = module.validate_rows(fake_report, rows)

        self.assertTrue(any("invalid route_runtime_status" in blocker for blocker in blockers))
        self.assertTrue(any("fallback_attempted must be false" in blocker for blocker in blockers))
        self.assertTrue(any("performance_claim_allowed must be false" in blocker for blocker in blockers))
        self.assertTrue(any("must not be generically unsupported" in blocker for blocker in blockers))
        self.assertTrue(any("must not advertise write_vortex" in blocker for blocker in blockers))

    def test_validator_rejects_incomplete_local_vortex_primitive_routes(self) -> None:
        module = load_route_module()
        src = REPO_ROOT / "python" / "src"
        if str(src) not in sys.path:
            sys.path.insert(0, str(src))
        from shardloom import ShardLoomContext

        report = ShardLoomContext(client=None).local_vortex_primitive_route_report()
        rows = [module.primitive_row_payload(row) for row in report.rows]
        rows[0]["route_runtime_status"] = "unsupported"
        rows[0]["cli_command"] = "vortex-made-up"
        rows[1]["vortex_normalization_point"] = "decoded_arrow_boundary"
        fake_report = SimpleNamespace(
            command_coverage=("vortex-run",),
            source_order_limit_route_ids=(),
            all_runtime_supported=False,
            all_no_fallback_no_external_engine=False,
        )

        blockers = module.validate_local_vortex_primitives(fake_report, rows)

        self.assertTrue(
            any("route_runtime_status must be scoped_runtime_supported" in b for b in blockers)
        )
        self.assertTrue(any("unrecognized cli_command" in b for b in blockers))
        self.assertTrue(
            any("vortex_normalization_point must be native_vortex_boundary" in b for b in blockers)
        )
        self.assertTrue(any("missing command coverage" in b for b in blockers))
        self.assertTrue(any("missing source-order limit routes" in b for b in blockers))


if __name__ == "__main__":
    unittest.main()
