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
        self.assertEqual(
            report["v1_scope_document"],
            "docs/architecture/v1-front-door-runtime-scope.md",
        )
        self.assertIn("selective_filter", report["v1_example_scenario_ids"])
        self.assertEqual(report["v1_expected_error_scenario_ids"], [])
        self.assertEqual(
            set(report["v1_public_front_door_ids"]),
            {
                "local_source_auto_prepare_vortex_front_door",
                "generated_source_prepare_vortex_front_door",
            },
        )
        self.assertEqual(report["unsupported_local_benchmark_route_ids"], [])
        self.assertTrue(report["all_no_fallback_no_external_engine"])
        self.assertFalse(report["flexible_anything_claim_allowed"])
        self.assertFalse(report["performance_equivalence_claim_allowed"])
        self.assertFalse(report["production_claim_allowed"])
        self.assertFalse(report["spark_replacement_claim_allowed"])
        self.assertTrue(report["local_vortex_primitive_all_runtime_supported"])
        self.assertTrue(report["local_vortex_primitive_all_no_fallback_no_external_engine"])
        self.assertEqual(
            report["local_file_benchmark_schema_version"],
            "shardloom.local_file_benchmark_route_report.v1",
        )
        self.assertEqual(report["local_file_benchmark_unsupported_scenario_ids"], [])
        self.assertTrue(report["local_file_benchmark_all_no_fallback_no_external_engine"])
        self.assertTrue(report["local_file_benchmark_all_mapped_without_generic_unsupported"])
        self.assertTrue(
            report["acceptance_summary"][
                "all_required_local_file_benchmark_scenarios_mapped"
            ]
        )
        self.assertTrue(
            report["acceptance_summary"][
                "all_admitted_benchmark_routes_have_clear_output_options"
            ]
        )
        self.assertTrue(
            report["acceptance_summary"][
                "all_admitted_local_file_benchmark_routes_have_clear_output_options"
            ]
        )
        self.assertTrue(
            report["acceptance_summary"][
                "all_prepared_routes_expose_workspace_manifest_reuse_contract"
            ]
        )
        self.assertTrue(
            report["acceptance_summary"][
                "generated_source_route_exposes_artifact_adjacent_manifest_reuse_contract"
            ]
        )
        self.assertTrue(
            report["acceptance_summary"][
                "public_front_door_routes_expose_auto_and_generated_prepared_surfaces"
            ]
        )
        self.assertTrue(
            report["acceptance_summary"][
                "public_front_door_routes_expose_prepared_state_reuse_contracts"
            ]
        )
        self.assertTrue(
            report["acceptance_summary"]["public_front_door_routes_preserve_no_fallback"]
        )
        self.assertTrue(
            report["acceptance_summary"][
                "all_prepared_local_file_benchmark_routes_expose_workspace_manifest_reuse_contract"
            ]
        )
        self.assertEqual(
            report["public_front_door_route_schema_version"],
            "shardloom.public_front_door_route_rows.v1",
        )
        self.assertEqual(report["public_front_door_route_count"], 2)
        self.assertEqual(
            set(report["public_front_door_route_ids"]),
            {
                "local_source_auto_prepare_vortex_front_door",
                "generated_source_prepare_vortex_front_door",
            },
        )
        self.assertIn(
            "native_vortex_output",
            report["admitted_route_output_options"]["native_vortex_query"],
        )
        self.assertIn(
            "result_sink_replay",
            report["admitted_local_file_benchmark_output_options"]["join_aggregate"],
        )
        self.assertEqual(
            set(report["local_vortex_primitive_command_coverage"]),
            {
                "vortex-run",
                "vortex-count-where",
                "vortex-filter",
                "vortex-project",
                "vortex-filter-project",
                "public-workflow run",
            },
        )

        by_id = {row["route_id"]: row for row in report["rows"]}
        prepare_first = by_id["local_file_prepare_once_first_query"]
        self.assertIn("SourceState", prepare_first["vortex_normalization_point"])
        self.assertIn("VortexPreparedState", prepare_first["vortex_normalization_point"])
        self.assertEqual(prepare_first["execution_mode"], "prepared_vortex")
        self.assertIn("prepared query", prepare_first["output_route"])
        self.assertEqual(
            prepare_first["prepared_state_fingerprint"],
            "runtime_prepared_state_fingerprint_pending",
        )
        self.assertEqual(
            prepare_first["prepared_state_reuse_scope"],
            "workspace_manifest_local_vortex_artifacts",
        )
        self.assertEqual(
            prepare_first["prepared_state_reuse_manifest_path"],
            "<workspace>/.shardloom/prepared-vortex-reuse-manifest.json",
        )
        self.assertEqual(
            prepare_first["prepared_state_reuse_policy"],
            "shardloom.python.prepared_vortex_reuse_manifest.v1",
        )
        self.assertEqual(prepare_first["prepared_state_reuse_hit"], "runtime_evaluated")
        self.assertEqual(
            prepare_first["prepared_state_reuse_reason"],
            "runtime_evaluated_workspace_manifest_lookup",
        )
        self.assertEqual(
            prepare_first["prepared_state_invalidation_reason"],
            "runtime_evaluated_on_reuse_miss_or_block",
        )
        self.assertEqual(
            prepare_first["nearest_runnable_route"],
            "local_file_prepare_once_first_query",
        )
        self.assertEqual(prepare_first["runtime_blocker_code"], "none")

        direct = by_id["local_file_direct_transient_route"]
        self.assertEqual(
            direct["prepared_state_reuse_scope"],
            "not_applicable_no_prepared_state",
        )

        generated = by_id["generated_rows_local_output"]
        self.assertIn("GeneratedSourceState", generated["vortex_normalization_point"])
        self.assertIn("VortexPreparedState", generated["vortex_normalization_point"])
        self.assertIn(
            "feature_gated_local_vortex_output",
            generated["desired_outputs"],
        )
        self.assertEqual(
            generated["prepared_state_fingerprint"],
            "runtime_prepared_state_fingerprint_pending",
        )
        self.assertEqual(
            generated["prepared_state_reuse_scope"],
            "artifact_adjacent_manifest_local_vortex_artifacts",
        )
        self.assertEqual(
            generated["prepared_state_reuse_manifest_path"],
            "<target-dir>/.shardloom/<target-name>.prepared-state-reuse.manifest",
        )
        self.assertEqual(
            generated["prepared_state_reuse_policy"],
            "artifact_adjacent_local_prepared_state_reuse.v1",
        )
        self.assertEqual(generated["prepared_state_reuse_hit"], "runtime_evaluated")
        self.assertEqual(
            generated["prepared_state_reuse_reason"],
            "runtime_evaluated_artifact_adjacent_manifest_lookup",
        )
        self.assertEqual(
            generated["prepared_state_invalidation_reason"],
            "runtime_evaluated_on_source_schema_plan_policy_or_artifact_drift",
        )
        self.assertEqual(
            generated["source_split_manifest_id"],
            "not_applicable_generated_source_no_source_splits",
        )
        self.assertIn(
            "prepared_state_reuse_manifest_for_feature_gated_local_vortex_output",
            generated["required_evidence"],
        )

        native = by_id["native_vortex_query"]
        self.assertEqual(native["vortex_normalization_point"], "native_vortex_boundary")
        self.assertEqual(native["route_runtime_status"], "scoped_runtime_supported")
        self.assertIn("native_vortex_route", native["recommended_user_surface"])
        self.assertIn("memory_gb", native["recommended_user_surface"])
        self.assertIn("write_vortex", native["recommended_user_surface"])

        materialized = by_id["materialized_python_snapshot_reentry"]
        self.assertIn("materialized snapshot", materialized["vortex_normalization_point"])
        self.assertIn("Vortex-preparable", materialized["vortex_normalization_point"])
        self.assertFalse(materialized["external_engine_invoked"])

        broad = by_id["broad_sql_python_dataframe_runtime"]
        self.assertEqual(
            broad["route_runtime_status"],
            "scoped_runtime_supported",
        )
        self.assertEqual(
            broad["runtime_blocker_code"],
            "none",
        )
        self.assertIn(
            "Vortex preparation/native Vortex unified plan",
            broad["vortex_normalization_point"],
        )
        self.assertIn(
            "documented local SQL/Python/DataFrame-style subset",
            broad["recommended_user_surface"],
        )
        performance_evidence = by_id["performance_equivalence_evidence"]
        self.assertEqual(
            performance_evidence["route_runtime_status"],
            "benchmark_publication_pending",
        )
        self.assertEqual(
            performance_evidence["runtime_blocker_code"],
            "cg6.front_door_performance_equivalence_benchmark_missing",
        )

        primitive_rows = {
            row["route_id"]: row for row in report["local_vortex_primitive_rows"]
        }
        self.assertIn("vortex_count_all", primitive_rows)
        self.assertIn("vortex_filter_project_limit_collect", primitive_rows)
        self.assertIn("vortex_tail_collect", primitive_rows)
        self.assertIn("vortex_sample_collect", primitive_rows)
        self.assertEqual(
            primitive_rows["vortex_count_all"]["vortex_normalization_point"],
            "native_vortex_boundary",
        )
        self.assertTrue(
            primitive_rows["vortex_filter_project_limit_collect"][
                "supports_source_order_limit"
            ]
        )
        for row in primitive_rows.values():
            self.assertNotIn("col('value')", row["dataframe_surface"])
        self.assertIn(
            "filter('gte:value:3')",
            primitive_rows["vortex_filter_project_limit_collect"]["dataframe_surface"],
        )

        scenarios = {
            row["scenario_id"]: row for row in report["local_file_benchmark_rows"]
        }
        self.assertEqual(len(scenarios), 15)
        self.assertEqual(
            scenarios["selective_filter"]["route_runtime_status"],
            "internal_smoke_only",
        )
        self.assertEqual(
            scenarios["selective_filter"]["selected_execution_mode"],
            "direct_compatibility_transient",
        )
        self.assertIn(
            "local_file_prepare_once_first_query",
            scenarios["selective_filter"]["alternate_route_ids"],
        )
        self.assertEqual(
            scenarios["join_aggregate"]["route_runtime_status"],
            "prepared_route_supported",
        )
        self.assertIn(
            "VortexPreparedState",
            scenarios["join_aggregate"]["vortex_normalization_point"],
        )
        self.assertEqual(
            scenarios["join_aggregate"]["prepared_state_fingerprint"],
            "runtime_prepared_state_fingerprint_pending",
        )
        self.assertEqual(
            scenarios["join_aggregate"]["prepared_state_reuse_scope"],
            "workspace_manifest_local_vortex_artifacts",
        )
        self.assertEqual(
            scenarios["join_aggregate"]["prepared_state_reuse_manifest_digest"],
            "runtime_prepared_state_reuse_manifest_digest_pending",
        )
        self.assertEqual(
            scenarios["join_aggregate"]["nearest_runnable_route"],
            "local_file_prepare_once_first_query",
        )
        self.assertEqual(scenarios["join_aggregate"]["runtime_blocker_code"], "none")
        self.assertIn(
            "not claim native nested field pruning",
            scenarios["nested_json_field_scan"]["claim_boundary"],
        )
        self.assertIn(
            "not general deletes",
            scenarios["small_change_over_large_base"]["claim_boundary"],
        )

        public_front_doors = {
            row["front_door_id"]: row for row in report["public_front_door_route_rows"]
        }
        local_auto = public_front_doors["local_source_auto_prepare_vortex_front_door"]
        self.assertEqual(local_auto["owning_route_id"], "local_file_prepare_once_first_query")
        self.assertEqual(local_auto["route_lane_id"], "prepare_once_first_query")
        self.assertIn("ctx.prepare_vortex", local_auto["public_user_surface"])
        self.assertIn(".query", local_auto["public_user_surface"])
        self.assertIn(".collect", local_auto["public_user_surface"])
        self.assertIn("SourceState", local_auto["vortex_normalization_point"])
        self.assertIn("VortexPreparedState", local_auto["vortex_normalization_point"])
        self.assertEqual(
            local_auto["prepared_state_reuse_scope"],
            "workspace_manifest_local_vortex_artifacts",
        )
        self.assertEqual(local_auto["front_door_end_state"], "result_sink")
        self.assertTrue(local_auto["includes_query"])
        self.assertTrue(local_auto["includes_output"])
        self.assertFalse(local_auto["fallback_attempted"])
        self.assertFalse(local_auto["external_engine_invoked"])

        generated_front_door = public_front_doors[
            "generated_source_prepare_vortex_front_door"
        ]
        self.assertEqual(generated_front_door["owning_route_id"], "generated_rows_local_output")
        self.assertIn("ctx.from_rows", generated_front_door["public_user_surface"])
        self.assertIn(".prepare_vortex", generated_front_door["public_user_surface"])
        self.assertIn(
            "GeneratedSourceState",
            generated_front_door["vortex_normalization_point"],
        )
        self.assertIn(
            "VortexPreparedState",
            generated_front_door["vortex_normalization_point"],
        )
        self.assertEqual(
            generated_front_door["prepared_state_reuse_scope"],
            "artifact_adjacent_manifest_local_vortex_artifacts",
        )
        self.assertIn(
            "prepared_state_reuse_manifest_for_feature_gated_local_vortex_output",
            generated_front_door["required_evidence"],
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
            routes.route("local_file_prepare_once_first_query").prepared_state_reuse_scope,
            "workspace_manifest_local_vortex_artifacts",
        )
        self.assertEqual(
            routes.route("generated_rows_local_output").prepared_state_reuse_scope,
            "artifact_adjacent_manifest_local_vortex_artifacts",
        )
        self.assertEqual(
            routes.route("local_vortex_primitive_report").vortex_normalization_point,
            "native_vortex_boundary",
        )
        self.assertEqual(
            set(routes.public_front_door_route_ids),
            {
                "local_source_auto_prepare_vortex_front_door",
                "generated_source_prepare_vortex_front_door",
            },
        )
        self.assertTrue(
            all(
                row.no_fallback_no_external_engine
                for row in routes.public_front_door_route_rows
            )
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
        self.assertIn(
            "vortex_tail_collect",
            primitives.source_order_limit_route_ids,
        )
        self.assertIn(
            "vortex_sample_collect",
            primitives.source_order_limit_route_ids,
        )
        local_file_routes = ShardLoomContext(
            client=None
        ).local_file_benchmark_route_report()
        self.assertEqual(
            local_file_routes.scenario("many_small_files_scan").route_runtime_status,
            "prepared_route_supported",
        )
        self.assertEqual(local_file_routes.unsupported_scenario_ids, ())

    def test_validator_rejects_unsupported_or_overclaimed_route_rows(self) -> None:
        module = load_route_module()
        route_report = module.load_report(REPO_ROOT)
        rows = [module.row_payload(row) for row in route_report.rows]
        rows[0]["route_runtime_status"] = "unsupported"
        rows[0]["performance_claim_allowed"] = True
        rows[0]["fallback_attempted"] = True
        rows[1]["desired_outputs"] = ["opaque_internal"]
        rows[1]["output_route"] = "internal only"
        rows[1]["evidence_route"] = "internal evidence only"
        rows[1]["recommended_user_surface"] = "ctx.internal_only()"
        rows[2]["prepared_state_reuse_manifest_path"] = "missing"
        for row in rows:
            if row["route_id"] == "generated_rows_local_output":
                row["prepared_state_reuse_manifest_path"] = "missing"
                row["required_evidence"] = [
                    item
                    for item in row["required_evidence"]
                    if item
                    != "prepared_state_reuse_manifest_for_feature_gated_local_vortex_output"
                ]
                break
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
        self.assertTrue(any("clear output option" in blocker for blocker in blockers))
        self.assertTrue(any("must not advertise write_vortex" in blocker for blocker in blockers))
        self.assertTrue(any("workspace manifest path" in blocker for blocker in blockers))
        self.assertTrue(
            any("artifact-adjacent reuse manifest path" in blocker for blocker in blockers)
        )
        self.assertTrue(
            any(
                "feature-gated local Vortex output prepared-state reuse manifest evidence"
                in blocker
                for blocker in blockers
            )
        )

    def test_validator_rejects_incomplete_local_file_benchmark_routes(self) -> None:
        module = load_route_module()
        src = REPO_ROOT / "python" / "src"
        if str(src) not in sys.path:
            sys.path.insert(0, str(src))
        from shardloom import ShardLoomContext

        report = ShardLoomContext(client=None).local_file_benchmark_route_report()
        rows = [module.local_file_benchmark_row_payload(row) for row in report.rows]
        rows[0]["route_runtime_status"] = "unsupported"
        rows[0]["fallback_attempted"] = True
        rows[1]["vortex_normalization_point"] = "decoded_arrow_boundary"
        rows[2]["output_route"] = "internal only"
        rows[2]["evidence_route"] = "internal evidence only"
        rows[2]["prepared_state_reuse_policy"] = "missing"
        rows = [
            row
            for row in rows
            if row["scenario_id"] != "small_change_over_large_base"
        ]
        fake_report = SimpleNamespace(
            route_runtime_status_counts={"unsupported": 1},
            unsupported_scenario_ids=("selective_filter",),
            all_no_fallback_no_external_engine=False,
            all_mapped_without_generic_unsupported=False,
            claim_gate_status="not_claim_grade",
            performance_claim_allowed=False,
            production_claim_allowed=False,
            spark_replacement_claim_allowed=False,
        )

        blockers = module.validate_local_file_benchmark_routes(
            fake_report,
            rows,
            module.load_scenario_catalog(REPO_ROOT),
            {"local_file_direct_transient_route", "local_file_prepare_once_first_query"},
        )

        self.assertTrue(any("missing scenarios" in blocker for blocker in blockers))
        self.assertTrue(any("invalid route_runtime_status" in blocker for blocker in blockers))
        self.assertTrue(any("must not be generically unsupported" in blocker for blocker in blockers))
        self.assertTrue(any("fallback_attempted must be false" in blocker for blocker in blockers))
        self.assertTrue(any("must name SourceState" in blocker for blocker in blockers))
        self.assertTrue(any("clear output option" in blocker for blocker in blockers))
        self.assertTrue(any("reuse manifest policy" in blocker for blocker in blockers))

    def test_validator_rejects_incomplete_public_front_door_routes(self) -> None:
        module = load_route_module()
        route_report = module.load_report(REPO_ROOT)
        route_rows = [module.row_payload(row) for row in route_report.rows]
        public_rows = [
            module.public_front_door_row_payload(row)
            for row in route_report.public_front_door_route_rows
        ]
        public_rows[0]["public_user_surface"] = "ctx.read('fact.csv')"
        public_rows[0]["includes_query"] = False
        public_rows[0]["front_door_end_state"] = "VortexPreparedState"
        public_rows[0]["prepared_state_reuse_manifest_path"] = "missing"
        public_rows[1]["public_user_surface"] = "ctx.generated_internal_only()"
        public_rows[1]["required_evidence"] = []
        public_rows[1]["fallback_attempted"] = True

        blockers = module.validate_public_front_door_routes(public_rows, route_rows)

        self.assertTrue(
            any(
                "public_user_surface must include ctx.prepare_vortex" in blocker
                for blocker in blockers
            )
        )
        self.assertTrue(any("must set includes_query=true" in blocker for blocker in blockers))
        self.assertTrue(any("front_door_end_state must be result_sink" in blocker for blocker in blockers))
        self.assertTrue(
            any("public_user_surface must include ctx.from_rows" in blocker for blocker in blockers)
        )
        self.assertTrue(any("workspace manifest path" in blocker for blocker in blockers))
        self.assertTrue(any("missing required_evidence" in blocker for blocker in blockers))
        self.assertTrue(any("fallback_attempted must be false" in blocker for blocker in blockers))

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
