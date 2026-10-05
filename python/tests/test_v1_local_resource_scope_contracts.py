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


if __name__ == "__main__":
    unittest.main()
