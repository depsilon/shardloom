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
        self.assertTrue(report["local_vortex_primitive_v1_scope_ready"])
        self.assertTrue(report["user_route_v1_vortex_scope_ready"])
        self.assertTrue(report["all_no_fallback_no_external_engine"])
        self.assertEqual(report["evidence_class"], "declarative_specification")
        self.assertFalse(report["runtime_execution_performed"])
        self.assertFalse(report["performance_evidence_produced"])
        self.assertEqual(
            set(report["user_route_ids"]),
            {"native_vortex_query", "object_store_lakehouse_runtime"},
        )
        self.assertEqual(len(report["public_input_examples"]), 4)
        self.assertIn("object_store_vortex_io", report["unsupported_boundary_ids"])
        self.assertIn(
            "feature_gated_local_vortex_runtime",
            report["feature_profile_decision"],
        )
        self.assertFalse(report["performance_claim_allowed"])
        self.assertFalse(report["production_claim_allowed"])
        self.assertFalse(report["spark_replacement_claim_allowed"])
        self.assertFalse(any("benchmark" in key or "scenario" in key for key in report))

    def test_context_reports_expose_v1_vortex_scope(self) -> None:
        source_path = str(REPO_ROOT / "python" / "src")
        if source_path not in sys.path:
            sys.path.insert(0, source_path)
        from shardloom import ShardLoomContext

        ctx = ShardLoomContext(client=None)
        primitive_report = ctx.local_vortex_primitive_route_report()
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
        self.assertTrue(user_report.v1_vortex_scope_ready)
        self.assertEqual(len(primitive_report.v1_supported_route_ids), 11)
        self.assertEqual(
            set(user_report.route_order),
            {"native_vortex_query", "object_store_lakehouse_runtime"},
        )
        self.assertIn(
            "source_free_sql_front_door",
            {row.front_door_id for row in user_report.public_front_door_route_rows},
        )

    def test_validator_accepts_and_rejects_hand_declared_user_route_reports(self) -> None:
        module = load_scope_module()
        constants = {
            "supported_primitive_route_ids": ("vortex_count_all",),
            "supported_starting_states": (
                "native_local_vortex_file",
                "prepared_local_vortex_state",
                "prepared_compatibility_artifact",
                "generated_local_vortex_artifact",
            ),
            "unsupported_boundary_ids": (
                "object_store_vortex_io",
                "table_catalog_vortex_io",
                "generalized_source_sink_api",
                "broad_vortex_sql_dataframe_parity",
                "nested_complex_dtype_general_vortex",
                "vector_device_gpu_vortex_runtime",
            ),
        }
        native = SimpleNamespace(
            route_id="native_vortex_query",
            owner="shared_native_workflow",
            input_family="declared_input_or_source_free",
            input_examples=("data.vortex", "from_rows([...])", "SELECT 1 AS id"),
            start_state="declared_input_or_source_free",
            route_runtime_status="global_runtime_supported",
            execution_mode="native_vortex",
            vortex_normalization_point="declared input or source-free expression -> native Vortex admission -> native_vortex_unified_plan -> typed result or declared sink",
            materialization_decode_boundary="native encoded until bounded result or sink",
            output_route="complete typed result or committed requested output",
            fallback_attempted=False,
            external_engine_invoked=False,
            claim_gate_status="not_claim_grade",
            blocker_id=None,
            performance_claim_allowed=False,
            production_claim_allowed=False,
            spark_replacement_claim_allowed=False,
        )
        external = SimpleNamespace(
            route_id="object_store_lakehouse_runtime",
            owner="external_environment_gate",
            input_family="object_store_lakehouse_catalog",
            input_examples=("s3://bucket/table",),
            start_state="remote_or_table_source",
            route_runtime_status="external_environment_gate_pending",
            execution_mode="external_environment_gate_pending",
            vortex_normalization_point="external environment proof required",
            materialization_decode_boundary="remote output transfer explicit",
            output_route="external production gate",
            fallback_attempted=False,
            external_engine_invoked=False,
            claim_gate_status="not_claim_grade",
            blocker_id="cg9.cg10.cg21.production_io_front_door_missing",
            performance_claim_allowed=False,
            production_claim_allowed=False,
            spark_replacement_claim_allowed=False,
        )
        door_specs = (
            ("local_source_vortex_middle_front_door", "local_compat_file", "ctx.read_csv('x.csv')"),
            ("native_vortex_front_door", "native_vortex_file", "ctx.read_vortex('x.vortex')"),
            ("declared_memory_front_door", "declared_memory", "ctx.from_rows([])"),
            ("source_free_sql_front_door", "source_free", "ctx.sql('SELECT 1 AS id')"),
        )
        doors = tuple(
            SimpleNamespace(
                front_door_id=front_id,
                owning_route_id="native_vortex_query",
                input_family=family,
                public_user_surface=surface,
                vortex_normalization_point=native.vortex_normalization_point,
                execution_mode="native_vortex",
                output_route=native.output_route,
                fallback_attempted=False,
                external_engine_invoked=False,
            )
            for front_id, family, surface in door_specs
        )
        report = SimpleNamespace(
            v1_vortex_scope_document="docs/architecture/v1-vortex-runtime-scope.md",
            v1_vortex_supported_starting_states=constants["supported_starting_states"],
            v1_vortex_supported_primitive_route_ids=constants["supported_primitive_route_ids"],
            v1_vortex_unsupported_boundary_ids=constants["unsupported_boundary_ids"],
            v1_vortex_feature_profile_decision="feature_gated_local_vortex_runtime",
            v1_vortex_scope_ready=True,
            all_no_fallback_no_external_engine=True,
            route_order=(native.route_id, external.route_id),
            rows=(native, external),
            public_front_door_route_rows=doors,
        )

        self.assertEqual(module.validate_user_routes(report, constants), [])
        native.owner = "missing_owner"
        external.fallback_attempted = True
        report.rows = (native, external, SimpleNamespace(route_id="unexpected_compatibility_route"))
        report.route_order = (native.route_id, external.route_id, "unexpected_compatibility_route")
        blockers = module.validate_user_routes(report, constants)
        self.assertTrue(any("owner must be shared_native_workflow" in blocker for blocker in blockers))
        self.assertTrue(any("external environment route: fallback_attempted" in blocker for blocker in blockers))
        self.assertTrue(any("route ids must contain" in blocker for blocker in blockers))

    def test_validator_rejects_primitive_rows_missing_native_io_evidence(self) -> None:
        module = load_scope_module()
        row = SimpleNamespace(
            route_id="vortex_count_all",
            start_state="native_vortex_file",
            vortex_normalization_point="native_vortex_boundary",
            execution_mode="native_vortex",
            route_runtime_status="global_runtime_supported",
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

if __name__ == "__main__":
    unittest.main()
