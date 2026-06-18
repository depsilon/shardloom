from __future__ import annotations

import importlib.util
import sys
import unittest
from pathlib import Path
from types import SimpleNamespace


REPO_ROOT = Path(__file__).resolve().parents[2]


def load_scope_module():
    module_path = REPO_ROOT / "scripts" / "check_v1_vortex_runtime_scope.py"
    spec = importlib.util.spec_from_file_location(
        "check_v1_vortex_runtime_scope_for_test",
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


class V1VortexRuntimeScopeTests(unittest.TestCase):
    def test_scope_validator_passes_current_repo_contract(self) -> None:
        module = load_scope_module()

        report = module.build_report(REPO_ROOT)

        self.assertEqual(report["status"], "passed", report["blockers"])
        self.assertEqual(
            report["schema_version"],
            "shardloom.v1_vortex_runtime_scope_report.v1",
        )
        self.assertEqual(report["local_vortex_primitive_route_count"], 11)
        self.assertEqual(report["native_vortex_provider_route_count"], 8)
        self.assertEqual(report["local_file_benchmark_route_count"], 15)
        self.assertTrue(report["local_vortex_primitive_v1_scope_ready"])
        self.assertTrue(report["native_vortex_provider_route_v1_scope_ready"])
        self.assertFalse(
            report["native_vortex_provider_general_multi_input_join_claim_allowed"]
        )
        self.assertTrue(report["user_route_v1_vortex_scope_ready"])
        self.assertTrue(report["all_no_fallback_no_external_engine"])
        self.assertIn("object_store_vortex_io", report["unsupported_boundary_ids"])
        self.assertIn(
            "feature_gated_local_vortex_runtime",
            report["feature_profile_decision"],
        )
        self.assertFalse(report["performance_claim_allowed"])
        self.assertFalse(report["production_claim_allowed"])
        self.assertFalse(report["spark_replacement_claim_allowed"])

    def test_context_reports_expose_v1_vortex_scope(self) -> None:
        source_path = str(REPO_ROOT / "python" / "src")
        if source_path not in sys.path:
            sys.path.insert(0, source_path)
        from shardloom import ShardLoomContext

        ctx = ShardLoomContext(client=None)
        primitive_report = ctx.local_vortex_primitive_route_report()
        provider_report = ctx.native_vortex_provider_route_certificate_report()
        user_report = ctx.user_route_capability_report()

        self.assertEqual(
            primitive_report.v1_scope_document,
            "docs/architecture/v1-vortex-runtime-scope.md",
        )
        self.assertEqual(
            user_report.v1_vortex_scope_document,
            "docs/architecture/v1-vortex-runtime-scope.md",
        )
        self.assertTrue(primitive_report.v1_scope_ready)
        self.assertTrue(provider_report.v1_scope_ready)
        self.assertTrue(user_report.v1_vortex_scope_ready)
        self.assertEqual(len(primitive_report.v1_supported_route_ids), 11)
        self.assertEqual(len(provider_report.rows), 8)
        self.assertEqual(
            provider_report.v1_provider_route_ids,
            (
                "native_vortex_user_aggregate",
                "native_vortex_user_join",
                "native_vortex_user_top_n",
                "native_vortex_user_cast",
                "native_vortex_user_contains",
                "native_vortex_user_sink",
            ),
        )
        self.assertEqual(
            provider_report.route("hash-join").right_input_contract,
            "declared_native_vortex_right_input_required",
        )
        self.assertFalse(provider_report.general_multi_input_join_claim_allowed)
        self.assertEqual(len(user_report.v1_vortex_supported_benchmark_scenario_ids), 15)

    def test_validator_rejects_primitive_rows_missing_native_io_evidence(self) -> None:
        module = load_scope_module()
        row = SimpleNamespace(
            route_id="vortex_count_all",
            start_state="native_vortex_file",
            vortex_normalization_point="native_vortex_boundary",
            execution_mode="native_vortex",
            route_runtime_status="scoped_runtime_supported",
            fallback_attempted=False,
            external_engine_invoked=False,
            claim_gate_status="not_claim_grade",
            required_evidence=("execution_certificate",),
            output_route="report",
            evidence_route="execution evidence",
            materialization_decode_boundary="report boundary",
            claim_boundary="scoped",
        )
        report = SimpleNamespace(
            rows=(row,),
            route_order=("vortex_count_all",),
            schema_version="shardloom.local_vortex_primitive_route_report.v1",
            v1_scope_document="docs/architecture/v1-vortex-runtime-scope.md",
            v1_supported_route_ids=("vortex_count_all",),
            v1_supported_starting_states=(
                "native_local_vortex_file",
                "prepared_local_vortex_state",
                "prepared_compatibility_artifact",
                "generated_local_vortex_artifact",
            ),
            v1_unsupported_boundary_ids=(
                "object_store_vortex_io",
                "table_catalog_vortex_io",
                "generalized_source_sink_api",
                "broad_vortex_sql_dataframe_parity",
                "nested_complex_dtype_general_vortex",
                "vector_device_gpu_vortex_runtime",
            ),
            v1_feature_profile_decision="feature_gated_local_vortex_runtime",
            v1_scope_ready=False,
            all_runtime_supported=True,
            all_no_fallback_no_external_engine=True,
        )

        blockers = module.validate_primitive_report(
            report,
            {
                "supported_primitive_route_ids": ("vortex_count_all",),
                "supported_starting_states": report.v1_supported_starting_states,
                "unsupported_boundary_ids": report.v1_unsupported_boundary_ids,
            },
        )

        self.assertIn(
            "vortex_count_all: required_evidence must include native_io_certificate",
            blockers,
        )

    def test_validator_rejects_provider_rows_that_overclaim(self) -> None:
        module = load_scope_module()
        source_path = str(REPO_ROOT / "python" / "src")
        if source_path not in sys.path:
            sys.path.insert(0, source_path)
        from shardloom import ShardLoomContext

        report = ShardLoomContext(client=None).native_vortex_provider_route_certificate_report()
        first = report.rows[0]
        bad_row = SimpleNamespace(
            **{
                name: getattr(first, name)
                for name in (
                    "route_id",
                    "operation_family",
                    "provider_scenario",
                    "benchmark_scenario_id",
                    "python_surface",
                    "sql_surface",
                    "required_right_input",
                    "right_input_contract",
                    "resolved_internal_command",
                    "feature_gate",
                    "start_state",
                    "vortex_normalization_point",
                    "execution_policy",
                    "typed_result_contract",
                    "typed_sink_contract",
                    "decode_materialization_boundary",
                    "output_route",
                    "evidence_route",
                    "route_certificate_status",
                    "route_certificate_source",
                    "benchmark_route_equivalence",
                    "route_runtime_status",
                    "external_engine_invoked",
                    "required_evidence",
                    "claim_gate_status",
                    "production_claim_allowed",
                    "claim_boundary",
                )
            },
            fallback_attempted=True,
            performance_claim_allowed=True,
        )
        fake_report = SimpleNamespace(
            rows=(bad_row,),
            route_order=(bad_row.route_id,),
            scenario_order=(bad_row.provider_scenario,),
            schema_version=report.schema_version,
            v1_scope_document=report.v1_scope_document,
            v1_provider_route_ids=(bad_row.route_id,),
            v1_provider_scenario_ids=(bad_row.provider_scenario,),
            feature_gate=report.feature_gate,
            v1_scope_ready=False,
            all_runtime_supported=True,
            all_route_certificates_current=True,
            all_no_fallback_no_external_engine=False,
            general_multi_input_join_claim_allowed=True,
            performance_claim_allowed=True,
            production_claim_allowed=False,
        )

        blockers = module.validate_provider_route_report(
            fake_report,
            {
                "provider_route_ids": (bad_row.route_id,),
                "provider_scenario_ids": (bad_row.provider_scenario,),
            },
        )

        self.assertIn("native Vortex provider v1_scope_ready must be true", blockers)
        self.assertIn("native Vortex provider routes must preserve no fallback", blockers)
        self.assertIn(
            "native Vortex provider report must not claim arbitrary joins",
            blockers,
        )
        self.assertTrue(
            any("fallback_attempted must be false" in blocker for blocker in blockers)
        )
        self.assertTrue(
            any("performance_claim_allowed must be false" in blocker for blocker in blockers)
        )


if __name__ == "__main__":
    unittest.main()
