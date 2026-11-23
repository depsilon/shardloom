import importlib.util
import sys
import unittest
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
SCRIPTS = REPO_ROOT / "scripts"
if str(SCRIPTS) not in sys.path:
    sys.path.insert(0, str(SCRIPTS))

from release_report_utils import fail_closed_fields


def _load_validator():
    script = SCRIPTS / "check_v1_local_resource_safety.py"
    spec = importlib.util.spec_from_file_location("check_v1_local_resource_safety_for_scope_test", script)
    if spec is None or spec.loader is None:
        raise RuntimeError("cannot load v1 local resource safety validator")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def source_prepared_payload():
    return {
        "schema_version": "shardloom.v1_source_prepared_state_scope_report.v1",
        "status": "passed",
        "evidence_class": "declarative_specification",
        "runtime_execution_performed": False,
        "performance_evidence_produced": False,
        "claim_gate_status": "not_claim_grade",
        "canonical_route": (
            "declared input or source-free expression -> native Vortex admission -> "
            "native_vortex_unified_plan -> typed result or declared sink"
        ),
        "route_ids": ["native_vortex_query"],
        "state_owner": "ResidentVortexSession",
        "reuse_scope": "native_session_or_explicit_vortex_artifact",
        "reuse_policy": "validate_source_generation_and_declaration_before_each_execution",
        "query_answers_cached": False,
        "v1_scope_ready": True,
        "all_no_fallback_no_external_engine": True,
        "unsupported_boundary_ids": ["global_hidden_cache"],
        **fail_closed_fields(),
    }


def local_output_payload():
    return {
        "schema_version": "shardloom.v1_local_output_sink_scope_report.v1",
        "status": "passed",
        "evidence_class": "declarative_contract",
        "declarative_contract_ready": True,
        "runtime_evidence_verified": False,
        "claim_gate_status": "not_claim_grade",
        "output_route_ids": ["native_vortex_query"],
        "output_user_route_rows": [
            {
                "route_id": "native_vortex_query",
                "owner": "shared_native_workflow",
                "execution_mode": "native_vortex",
            }
        ],
        "user_write_methods": ["write_json", "write_vortex"],
        "write_policy_ids": [
            "error_if_exists_by_default",
            "explicit_allow_overwrite",
            "append_mode_unsupported",
            "atomic_rename_same_directory",
            "partial_write_cleanup_reported",
        ],
        "unsupported_boundary_ids": ["append_mode"],
        "v1_scope_ready": True,
        "all_no_fallback_no_external_engine": True,
        "all_output_routes_emit_sink_evidence": True,
        "all_output_routes_no_fallback_no_external_engine": True,
        "all_write_methods_no_fallback_no_external_engine": True,
        "write_policy_contract_ready": True,
        **fail_closed_fields(),
    }


class V1LocalResourceScopeContractTests(unittest.TestCase):
    def test_actual_source_report_satisfies_resource_gate_contract(self) -> None:
        from check_v1_source_prepared_state_scope import build_report

        payload = build_report(REPO_ROOT)
        self.assertEqual(payload["status"], "passed", payload["blockers"])
        summary, blockers = _load_validator().validate_source_prepared(payload)

        self.assertEqual(blockers, [])
        self.assertEqual(summary["status"], "passed")
        self.assertFalse(summary["runtime_evidence_verified"])

    def test_actual_source_report_requires_explicit_false_claim_and_effect_fields(self) -> None:
        from check_v1_source_prepared_state_scope import build_report

        payload = build_report(REPO_ROOT)
        validator = _load_validator()
        for field in fail_closed_fields():
            for value in (None, True, 0, "false"):
                with self.subTest(field=field, value=value):
                    changed = dict(payload)
                    if value is None:
                        changed.pop(field, None)
                    else:
                        changed[field] = value
                    summary, blockers = validator.validate_source_prepared(changed)
                    self.assertEqual(summary["status"], "failed")
                    self.assertIn(f"source_prepared_state: {field} must be false", blockers)

    def test_current_source_prepared_declaration_passes_without_smoke_proof(self) -> None:
        validator = _load_validator()
        summary, blockers = validator.validate_source_prepared(source_prepared_payload())

        self.assertEqual(blockers, [])
        self.assertEqual(summary["status"], "passed")
        self.assertEqual(summary["evidence_class"], "declarative_specification")
        self.assertFalse(summary["runtime_evidence_verified"])
        self.assertEqual(summary["route_ids"], ["native_vortex_query"])
        self.assertEqual(summary["state_owner"], "ResidentVortexSession")
        self.assertFalse(summary["query_answers_cached"])
        self.assertNotIn("internal_source_smoke_non_persistent", summary)

    def test_source_prepared_rejects_wrong_owner_route_cache_or_evidence_class(self) -> None:
        validator = _load_validator()
        cases = (
            ("state_owner", "HiddenGlobalCache", "state_owner=HiddenGlobalCache"),
            ("route_ids", ["local_file_internal_source_smoke_route"], "route_ids="),
            ("query_answers_cached", True, "query_answers_cached=True"),
            ("query_answers_cached", 0, "query_answers_cached=0"),
            ("evidence_class", "runtime_evidence", "evidence_class=runtime_evidence"),
            ("runtime_execution_performed", True, "runtime_execution_performed=True"),
            (
                "runtime_evidence_verified",
                True,
                "runtime_evidence_verified must be false when present",
            ),
        )
        for key, value, expected in cases:
            with self.subTest(key=key):
                payload = source_prepared_payload()
                payload[key] = value
                summary, blockers = validator.validate_source_prepared(payload)
                self.assertEqual(summary["status"], "failed")
                self.assertTrue(any(expected in blocker for blocker in blockers), blockers)

    def test_current_output_declaration_passes_and_summary_disclaims_runtime(self) -> None:
        validator = _load_validator()
        summary, blockers = validator.validate_local_output(local_output_payload())

        self.assertEqual(blockers, [])
        self.assertEqual(summary["status"], "passed")
        self.assertEqual(summary["evidence_class"], "declarative_contract")
        self.assertFalse(summary["runtime_evidence_verified"])
        self.assertEqual(summary["output_route_count"], 1)
        self.assertNotIn("sink_replay_ready", summary)

    def test_output_declaration_rejects_scope_status_runtime_claim_and_route_drift(self) -> None:
        validator = _load_validator()
        cases = (
            ("status", "failed", "status=failed"),
            ("evidence_class", "runtime_evidence", "evidence_class=runtime_evidence"),
            ("declarative_contract_ready", False, "declarative_contract_ready=False"),
            ("runtime_evidence_verified", True, "runtime_evidence_verified=True"),
            ("runtime_evidence_verified", 0, "runtime_evidence_verified=0"),
            ("output_route_ids", ["native_vortex_primitive_row_export"], "output_route_ids="),
            ("write_policy_contract_ready", False, "write_policy_contract_ready must be true"),
        )
        for key, value, expected in cases:
            with self.subTest(key=key):
                payload = local_output_payload()
                payload[key] = value
                summary, blockers = validator.validate_local_output(payload)
                self.assertEqual(summary["status"], "failed")
                self.assertTrue(any(expected in blocker for blocker in blockers), blockers)

    def test_output_declaration_rejects_misowned_or_non_native_route_row(self) -> None:
        validator = _load_validator()
        for field, value, expected in (
            ("owner", "another_engine", "route owner must be shared_native_workflow"),
            ("execution_mode", "external_engine", "route execution_mode must be native_vortex"),
        ):
            with self.subTest(field=field):
                payload = local_output_payload()
                payload["output_user_route_rows"][0][field] = value
                summary, blockers = validator.validate_local_output(payload)
                self.assertEqual(summary["status"], "failed")
                self.assertTrue(any(expected in blocker for blocker in blockers), blockers)


def public_native_vortex_resource_payload(*, footer_completed: bool):
    execution_activity = "false" if footer_completed else "true"
    field_map = {
        "public_workflow_route_id": "native_vortex_aggregate",
        "public_workflow_resolved_internal_command": "vortex-run",
        "public_workflow_start_state": "native_vortex_file",
        "public_workflow_vortex_normalization_point": "native_vortex_boundary",
        "public_workflow_memory_gb": "1",
        "public_workflow_max_parallelism": "2",
        "public_workflow_dynamic_parallelism_floor_applied": "false",
        "local_primitive_resource_envelope_schema_version": "shardloom.local_vortex_resource_envelope.v1",
        "local_primitive_resource_memory_gb": "1",
        "local_primitive_resource_max_parallelism": "2",
        "local_primitive_state_budget_schema_version": "shardloom.local_vortex_state_budget.v2",
        "local_primitive_state_budget_status": "bounded_in_memory_low_pressure_spill_not_required",
        "local_primitive_state_family": (
            "scalar_aggregate_state"
            if footer_completed
            else "scalar_aggregate_state+direct_dictionary_or_typed"
        ),
        "local_primitive_budget_scope": "local_vortex_scalar_aggregate",
        "local_primitive_spill_policy": "fail_closed_before_uncertified_spill",
        "local_primitive_state_budget_diagnostic_code": "none",
        "local_primitive_memory_admission_schema_version": "shardloom.memory_admission.v1",
        "local_primitive_memory_admission_scope": "public_local_vortex_primitive_state_budget",
        "local_primitive_memory_reservation_owner_class": "aggregate",
        "local_primitive_memory_reservation_status": "granted",
        "local_primitive_memory_admission_decision": "granted",
        "local_primitive_memory_pressure_before": "normal",
        "local_primitive_memory_pressure_after": "normal",
        "local_primitive_memory_reserved_before_bytes": "0",
        "local_primitive_memory_reserved_after_release_bytes": "0",
        "local_primitive_native_io_certificate_status": "certified",
        "public_workflow_native_vortex_plan_route_family": "native_vortex_unified_plan",
        "public_workflow_fallback_attempted": "false",
        "public_workflow_external_engine_invoked": "false",
        "public_workflow_native_vortex_plan_fallback_attempted": "false",
        "public_workflow_native_vortex_plan_external_engine_invoked": "false",
        "object_store_io": "false",
        "write_io": "false",
        "spill_io_performed": "false",
        "local_primitive_spill_required": "false",
        "local_primitive_spill_supported": "false",
        "local_primitive_spill_io_performed": "false",
        "local_primitive_memory_fail_before_oom": "false",
        "local_primitive_memory_fallback_attempted": "false",
        "local_primitive_native_io_object_store_io": "false",
        "local_primitive_native_io_write_io": "false",
        "local_primitive_native_io_spill_io_performed": "false",
        "local_primitive_native_io_fallback_attempted": "false",
        "local_primitive_native_io_fallback_execution_allowed": "false",
        "fallback_attempted": "false",
        "external_engine_invoked": "false",
        "public_workflow_route_attached": "true",
        "public_workflow_bounded_request": "true",
        "local_primitive_report_present": "true",
        "local_primitive_state_budget_required": "true",
        "local_primitive_fail_closed_if_spill_required": "true",
        "local_primitive_memory_reservation_required": "true",
        "local_primitive_memory_reservation_release_performed": "true",
        "local_primitive_native_io_certified": "true",
        "local_primitive_native_io_encoded_representation_preserved": "true",
        "local_primitive_native_io_streaming_capability": execution_activity,
        "upstream_vortex_scan_called": execution_activity,
        "data_read": execution_activity,
        "data_decoded": execution_activity,
        "data_materialized": execution_activity,
    }
    if footer_completed:
        field_map["local_primitive_scan_metadata_completed"] = "true"
        field_map["local_primitive_aggregate_update_strategy"] = (
            "exact_file_footer_scalar_completion"
        )
    return {
        "schema_version": "shardloom.output.v2",
        "command": "run",
        "status": "success",
        "fallback": {"attempted": False},
        "fields": [{"key": k, "value": v} for k, v in field_map.items()],
    }


def _set_output_field(payload, key: str, value: str) -> None:
    for item in payload["fields"]:
        if item["key"] == key:
            item["value"] = value
            return
    payload["fields"].append({"key": key, "value": value})


class V1LocalResourceRoutePayloadTests(unittest.TestCase):
    def test_public_native_vortex_route_accepts_footer_completed_and_direct_scan_payloads(
        self,
    ) -> None:
        validator = _load_validator()
        for footer_completed in (True, False):
            with self.subTest(footer_completed=footer_completed):
                summary, blockers = validator.validate_command_payload(
                    "public_native_vortex_resource_route",
                    public_native_vortex_resource_payload(footer_completed=footer_completed),
                )
                self.assertEqual(blockers, [])
                self.assertEqual(summary["status"], "passed")

    def test_public_native_vortex_route_rejects_inconsistent_activity_or_strategy(self) -> None:
        validator = _load_validator()
        cases = (
            (True, "data_read", "true", "data_read must be false"),
            (True, "upstream_vortex_scan_called", "true", "upstream_vortex_scan_called must be false"),
            (
                True,
                "local_primitive_aggregate_update_strategy",
                "direct_scan",
                "local_primitive_aggregate_update_strategy=direct_scan",
            ),
            (False, "data_read", "false", "data_read must be true"),
            (False, "upstream_vortex_scan_called", "false", "upstream_vortex_scan_called must be true"),
        )
        for footer_completed, key, value, expected in cases:
            with self.subTest(footer_completed=footer_completed, key=key, value=value):
                payload = public_native_vortex_resource_payload(footer_completed=footer_completed)
                _set_output_field(payload, key, value)
                summary, blockers = validator.validate_command_payload(
                    "public_native_vortex_resource_route", payload
                )
                self.assertEqual(summary["status"], "failed")
                self.assertTrue(any(expected in blocker for blocker in blockers), blockers)


if __name__ == "__main__":
    unittest.main()
