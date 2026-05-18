from __future__ import annotations

import json
import os
import sys
import tempfile
import textwrap
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from shardloom import (
    __version__,
    context as shardloom_context,
    ClaimGateCloseoutReport,
    ComputeCapabilityMatrix,
    CompatibilitySourceSmokeReport,
    CapabilityPosture,
    ContextCapabilities,
    CapabilityView,
    DataFrameMethodCapabilityMatrix,
    GeneratedSourceApiAdmissionMatrix,
    EngineCapabilityMatrix,
    ExecutionResultEnvelopeView,
    GeneratedSourceCertificateContract,
    LocalVortexPrimitiveSmokeReport,
    ShardLoomBinaryNotFoundError,
    ShardLoomClient,
    ShardLoomCommandError,
    ShardLoomContext,
    ShardLoomProtocolError,
    OutputEnvelope,
    PreparedVortexArtifacts,
    PredicateDtypeCoverageRow,
    RestApiContractPlan,
    RestApiDataPlane,
    RestApiDiscoveryContract,
    RestApiEventStream,
    RestApiLocalLifecycle,
    RestApiPlanPreview,
    RestApiSecurityGovernance,
    SemanticConformanceSuite,
    WorkloadCertificationDossier,
    WorkflowReadinessSmokeReport,
)

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


class ShardLoomClientTests(unittest.TestCase):
    def fake_cli(self, body: str) -> list[str]:
        tempdir = tempfile.TemporaryDirectory()
        self.addCleanup(tempdir.cleanup)
        path = Path(tempdir.name) / "fake_shardloom.py"
        path.write_text(_FAKE_CLI_ENVELOPE_PRELUDE + "\n" + body, encoding="utf-8")
        return [sys.executable, str(path)]

    def test_package_exports_non_placeholder_version(self) -> None:
        self.assertRegex(__version__, r"^\d+\.\d+\.\d+")
        self.assertNotEqual(__version__, "0.0.0")

    def test_status_appends_json_format_and_parses_fields(self) -> None:
        binary = self.fake_cli(
            textwrap.dedent(
                """
                import json, sys
                assert sys.argv[1:] == ["status", "--format", "json"], sys.argv
                print(json.dumps({
                    "schema_version": "shardloom.output.v2",
                    "command": "status",
                    "status": "success",
                    "summary": "ok",
                    "human_text": "ok",
                    "fallback": {"attempted": False, "allowed": False, "engine": None, "reason": "disabled"},
                    "diagnostics": [],
                    "fields": [{"key": "engine", "value": "shardloom"}],
                }))
                """
            )
        )

        result = ShardLoomClient(binary=binary).status()

        self.assertEqual(result.command, "status")
        self.assertEqual(result.status, "success")
        self.assertFalse(result.fallback.attempted)
        self.assertEqual(result.field_map["engine"], "shardloom")

    def test_typed_envelope_payloads_are_preserved(self) -> None:
        binary = self.fake_cli(
            textwrap.dedent(
                """
                import json, sys
                assert sys.argv[1:] == ["status", "--format", "json"], sys.argv
                print(json.dumps({
                    "schema_version": "shardloom.output.v2",
                    "command": "status",
                    "status": "success",
                    "summary": "ok",
                    "human_text": "ok",
                    "fallback": {"attempted": False, "allowed": False, "engine": None, "reason": "disabled"},
                    "diagnostics": [],
                    "result": {"fields": [{"key": "engine", "value": "shardloom"}]},
                    "result_refs": [{"id": "result.local", "kind": "json", "status": "available", "uri": None}],
                    "artifacts": [{"artifact_id": "artifact.evidence", "artifact_kind": "evidence", "status": "available", "payload": {"fields": []}}],
                    "artifact_refs": [{"id": "artifact.ref", "kind": "file", "status": "available", "uri": "artifact.json"}],
                    "certificates": [{"id": "certificate.execution", "kind": "execution_certificate", "status": "available", "uri": None}],
                    "policy": {"fields": [{"key": "fallback_execution_allowed", "value": "false"}]},
                    "lifecycle": {"fields": [{"key": "phase", "value": "report_only"}]},
                    "capability_snapshot": {"fields": [{"key": "scope", "value": "status"}]},
                    "fields": [{"key": "engine", "value": "shardloom"}],
                }))
                """
            )
        )

        result = ShardLoomClient(binary=binary).status()

        self.assertEqual(result.result["fields"][0]["key"], "engine")
        self.assertEqual(result.result_refs[0]["id"], "result.local")
        self.assertEqual(result.artifacts[0]["artifact_id"], "artifact.evidence")
        self.assertEqual(result.artifact_refs[0]["uri"], "artifact.json")
        self.assertEqual(result.certificates[0]["kind"], "execution_certificate")
        self.assertEqual(result.policy["fields"][0]["key"], "fallback_execution_allowed")
        self.assertEqual(result.lifecycle["fields"][0]["value"], "report_only")
        self.assertEqual(result.capability_snapshot["fields"][0]["value"], "status")

    def test_capability_view_no_runtime_and_no_fallback_require_explicit_fields(self) -> None:
        def capability_envelope(fields: list[dict[str, str]]) -> OutputEnvelope:
            return OutputEnvelope.from_json({
                "schema_version": "shardloom.output.v2",
                "command": "capabilities",
                "status": "success",
                "summary": "ok",
                "human_text": "ok",
                "fallback": {
                    "attempted": False,
                    "allowed": False,
                    "engine": None,
                    "reason": "disabled",
                },
                "diagnostics": [],
                "result": {"fields": []},
                "result_refs": [],
                "artifacts": [],
                "artifact_refs": [],
                "certificates": [],
                "policy": {"fields": []},
                "lifecycle": {"fields": []},
                "capability_snapshot": {"fields": []},
                "fields": fields,
            })

        envelope = capability_envelope(
            [
                {"key": "scope", "value": "python"},
                {"key": "runtime_execution", "value": "false"},
                {"key": "fallback_attempted", "value": "false"},
                {"key": "fallback_execution_allowed", "value": "false"},
            ]
        )
        view = CapabilityView(scope="python", envelope=envelope)

        self.assertFalse(view.runtime_execution)
        self.assertFalse(view.fallback_attempted)
        self.assertFalse(view.fallback_allowed)
        self.assertFalse(view.no_runtime)
        self.assertFalse(view.no_fallback)

        explicit = capability_envelope(
            [
                {"key": "scope", "value": "workflow"},
                {"key": "runtime_execution", "value": "false"},
                {"key": "fallback_attempted", "value": "false"},
                {"key": "fallback_execution_allowed", "value": "false"},
                {"key": "no_runtime", "value": "true"},
                {"key": "no_fallback", "value": "true"},
            ]
        )
        explicit_view = CapabilityView(scope="workflow", envelope=explicit)

        self.assertTrue(explicit_view.no_runtime)
        self.assertTrue(explicit_view.no_fallback)

    def test_execution_result_view_preserves_artifact_rich_slots(self) -> None:
        envelope = OutputEnvelope.from_json(
            {
                "schema_version": "shardloom.output.v2",
                "command": "top-level-exec",
                "status": "success",
                "summary": "executed",
                "human_text": "executed",
                "fallback": {
                    "attempted": False,
                    "allowed": False,
                    "engine": None,
                    "reason": "disabled",
                },
                "diagnostics": [],
                "result": {
                    "fields": [
                        {"key": "plan_id", "value": "plan.count"},
                        {"key": "plan_kind", "value": "vortex_primitive"},
                        {"key": "execution_status", "value": "executed"},
                        {"key": "provider_api_surface", "value": "vortex_local_primitive"},
                        {"key": "provider_version", "value": "0.70"},
                        {"key": "evidence_completeness_status", "value": "evidence_incomplete"},
                        {"key": "result_refs", "value": "result.rows"},
                        {"key": "artifact_refs", "value": "vortex_local_engine_report"},
                        {"key": "inline_artifact_ids", "value": "vortex_local_engine_report,plan.count.execution_evidence_slots"},
                        {"key": "execution_certificate_refs", "value": "cert.execution"},
                        {"key": "native_io_certificate_refs", "value": "cert.native_io"},
                        {"key": "representation_transitions", "value": "vortex_encoded->vortex_encoded"},
                        {"key": "fallback_attempted", "value": "false"},
                        {"key": "external_engine_invoked", "value": "false"},
                    ]
                },
                "result_refs": [{"id": "result.rows", "kind": "execution_result", "status": "available", "uri": None}],
                "artifacts": [
                    {
                        "artifact_id": "plan.count.execution_evidence_slots",
                        "artifact_kind": "execution_evidence_slots",
                        "status": "evidence_incomplete",
                        "payload": {
                            "fields": [
                                {"key": "evidence_slot_order", "value": "result_refs,provider_version,native_io_certificate_refs"},
                                {"key": "evidence_slot_result_refs_status", "value": "present"},
                                {"key": "evidence_slot_result_refs_refs", "value": "result.rows"},
                                {"key": "evidence_slot_result_refs_detail", "value": "result refs are present"},
                                {"key": "evidence_slot_provider_version_status", "value": "present"},
                                {"key": "evidence_slot_provider_version_refs", "value": "0.70"},
                                {"key": "evidence_slot_provider_version_detail", "value": "provider version is present"},
                                {"key": "evidence_slot_native_io_certificate_refs_status", "value": "evidence_incomplete"},
                                {"key": "evidence_slot_native_io_certificate_refs_refs", "value": "none"},
                                {"key": "evidence_slot_native_io_certificate_refs_detail", "value": "Native I/O certificate missing"},
                            ]
                        },
                    },
                    {
                        "artifact_id": "top-level-exec.execution_mode_selection_report",
                        "artifact_kind": "execution_mode_selection_report",
                        "status": "available",
                        "payload": {
                            "fields": [
                                {"key": "execution_mode_selection_schema_version", "value": "shardloom.execution_mode_selection_report.v1"},
                                {"key": "requested_execution_mode", "value": "auto"},
                                {"key": "selected_execution_mode", "value": "prepared_vortex"},
                                {"key": "execution_mode", "value": "prepared_vortex"},
                                {"key": "mode_selection_reason", "value": "typed report selected prepared artifact reuse"},
                                {"key": "execution_mode_family", "value": "native_vortex"},
                                {"key": "source_format", "value": "vortex"},
                                {"key": "workload_constitution_id", "value": "local_vortex_analytics_v1"},
                                {"key": "compatibility_import_included", "value": "false"},
                                {"key": "vortex_prepare_included", "value": "false"},
                                {"key": "vortex_write_reopen_included", "value": "false"},
                                {"key": "direct_transient_execution", "value": "false"},
                                {"key": "vortex_native_claim_allowed", "value": "true"},
                                {"key": "certification_requested", "value": "false"},
                                {"key": "result_sink_requested", "value": "false"},
                                {"key": "prepared_artifact_available", "value": "true"},
                                {"key": "native_vortex_provider_available", "value": "true"},
                                {"key": "mode_supported", "value": "true"},
                                {"key": "support_status", "value": "supported"},
                                {"key": "unsupported_diagnostic_code", "value": "none"},
                                {"key": "blocker_id", "value": "none"},
                                {"key": "required_future_evidence", "value": "none"},
                                {"key": "claim_gate_status", "value": "fixture_smoke_only"},
                                {"key": "claim_gate_reason", "value": "operator evidence required"},
                                {"key": "fallback_attempted", "value": "false"},
                                {"key": "external_engine_invoked", "value": "false"},
                            ]
                        },
                    },
                    {
                        "artifact_id": "top-level-exec.compute_flow_evidence",
                        "artifact_kind": "compute_flow_evidence",
                        "status": "fixture_smoke_only",
                        "payload": {
                            "fields": [
                                {"key": "selected_execution_mode", "value": "prepared_vortex"},
                                {"key": "source_format", "value": "vortex"},
                                {"key": "prepared_artifact_ref", "value": "artifact.fact.vortex"},
                                {"key": "fact_vortex_digest", "value": "sha256:abc"},
                                {"key": "native_io_certificate_status", "value": "certified"},
                                {"key": "computed_result_sink_replay_verified", "value": "true"},
                                {"key": "computed_result_sink_native_io_certificate_status", "value": "certified"},
                                {"key": "result_sink_claim_gate_status", "value": "result_sink_replay_certified"},
                                {"key": "result_sink_claim_gate_reason", "value": "result sink replay present"},
                                {"key": "operator_execution_class", "value": "materialized_temporary"},
                                {"key": "operator_admission_status", "value": "materialized_temporary_supported"},
                                {"key": "operator_blocker_id", "value": "gar-flow-2b.materialized_temporary_operator_not_encoded_native"},
                                {"key": "operator_blocker_reason", "value": "temporary operator materializes Vortex-derived arrays"},
                                {"key": "operator_encoded_native_claim_allowed", "value": "false"},
                                {"key": "operator_temporary_materialization_used", "value": "true"},
                                {"key": "materialization_boundary_report_emitted", "value": "true"},
                                {"key": "fallback_attempted", "value": "false"},
                                {"key": "external_engine_invoked", "value": "false"},
                            ]
                        },
                    },
                    {
                        "artifact_id": "gar0038.facade_compatibility_matrix",
                        "artifact_kind": "facade_compatibility_matrix",
                        "status": "mixed_report_only_matrix",
                        "payload": {
                            "fields": [
                                {
                                    "key": "schema_version",
                                    "value": "shardloom.facade_compatibility_matrix.v1",
                                },
                                {
                                    "key": "report_id",
                                    "value": "gar0038.facade_compatibility_matrix",
                                },
                                {"key": "gar_id", "value": "GAR-0038-A"},
                                {
                                    "key": "support_status",
                                    "value": "mixed_report_only_matrix",
                                },
                                {"key": "claim_gate_status", "value": "not_claim_grade"},
                                {
                                    "key": "row_order",
                                    "value": (
                                        "vortex_primitive,prepared_encoded,"
                                        "source_backed_encoded,reader_backed_encoded,"
                                        "report_only,sql_dataframe_runtime,"
                                        "object_store_runtime,write_runtime,"
                                        "legacy_native_vortex_scan_placeholder,"
                                        "external_engine_fallback"
                                    ),
                                },
                                {"key": "unsupported_surface_count", "value": "3"},
                                {
                                    "key": "legacy_boundary_status",
                                    "value": "legacy_placeholder_removed_or_unsupported",
                                },
                                {
                                    "key": "all_rows_no_fallback_no_external_engine",
                                    "value": "true",
                                },
                                {
                                    "key": "surface_sql_dataframe_runtime_support_status",
                                    "value": "unsupported",
                                },
                                {
                                    "key": "surface_external_engine_fallback_support_status",
                                    "value": "prohibited",
                                },
                            ]
                        },
                    }
                ],
                "artifact_refs": [{"id": "vortex_local_engine_report", "kind": "execution_artifact", "status": "available", "uri": None}],
                "certificates": [{"id": "cert.execution", "kind": "execution_certificate", "status": "available", "uri": None}],
                "policy": {"fields": [{"key": "fallback_attempted", "value": "false"}]},
                "lifecycle": {"fields": [{"key": "execution_status", "value": "executed"}]},
                "capability_snapshot": {"fields": [{"key": "provider_version", "value": "0.70"}]},
                "fields": [
                    {"key": "plan_id", "value": "plan.count"},
                    {"key": "plan_kind", "value": "vortex_primitive"},
                    {"key": "execution_status", "value": "executed"},
                    {"key": "provider_api_surface", "value": "vortex_local_primitive"},
                    {"key": "provider_version", "value": "0.70"},
                    {"key": "evidence_completeness_status", "value": "evidence_incomplete"},
                    {"key": "result_refs", "value": "result.rows"},
                    {"key": "artifact_refs", "value": "vortex_local_engine_report"},
                    {"key": "inline_artifact_ids", "value": "vortex_local_engine_report,plan.count.execution_evidence_slots"},
                    {"key": "execution_certificate_refs", "value": "cert.execution"},
                    {"key": "native_io_certificate_refs", "value": "cert.native_io"},
                    {"key": "representation_transitions", "value": "vortex_encoded->vortex_encoded"},
                    {"key": "requested_execution_mode", "value": "auto"},
                    {"key": "selected_execution_mode", "value": "prepared_vortex"},
                    {"key": "mode_selection_reason", "value": "input was already prepared"},
                    {"key": "execution_mode_family", "value": "native_vortex"},
                    {"key": "vortex_native_claim_allowed", "value": "true"},
                    {"key": "compatibility_import_included", "value": "false"},
                    {"key": "direct_transient_execution", "value": "false"},
                    {"key": "fallback_attempted", "value": "false"},
                    {"key": "external_engine_invoked", "value": "false"},
                ],
            }
        )

        result = ExecutionResultEnvelopeView(envelope)

        self.assertEqual(result.plan_id, "plan.count")
        self.assertEqual(result.provider_version, "0.70")
        self.assertEqual(result.result_refs, ("result.rows",))
        self.assertIn("vortex_local_engine_report", result.inline_artifact_ids)
        self.assertEqual(result.execution_certificate_refs, ("cert.execution",))
        self.assertEqual(result.native_io_certificate_refs, ("cert.native_io",))
        self.assertEqual(result.representation_transitions, ("vortex_encoded->vortex_encoded",))
        self.assertEqual(result.requested_execution_mode, "auto")
        self.assertEqual(result.selected_execution_mode, "prepared_vortex")
        self.assertEqual(
            result.mode_selection_reason,
            "typed report selected prepared artifact reuse",
        )
        self.assertEqual(result.execution_mode_family, "native_vortex")
        self.assertTrue(result.mode_supported)
        self.assertEqual(result.support_status, "supported")
        self.assertEqual(result.claim_gate_status, "fixture_smoke_only")
        self.assertEqual(result.claim_gate_reason, "operator evidence required")
        self.assertEqual(result.unsupported_diagnostic_code, "none")
        self.assertEqual(result.blocker_id, "none")
        self.assertEqual(result.required_future_evidence, "none")
        self.assertEqual(
            result.execution_mode_selection_fields["source_format"],
            "vortex",
        )
        self.assertEqual(
            result.compute_flow_evidence_fields["prepared_artifact_ref"],
            "artifact.fact.vortex",
        )
        self.assertTrue(result.vortex_native_claim_allowed)
        self.assertFalse(result.compatibility_import_included)
        self.assertFalse(result.vortex_prepare_included)
        self.assertFalse(result.vortex_write_reopen_included)
        self.assertFalse(result.direct_transient_execution)
        self.assertTrue(result.computed_result_sink_replay_verified)
        self.assertEqual(
            result.computed_result_sink_native_io_certificate_status,
            "certified",
        )
        self.assertEqual(
            result.result_sink_claim_gate_status,
            "result_sink_replay_certified",
        )
        self.assertEqual(result.operator_execution_class, "materialized_temporary")
        self.assertEqual(
            result.operator_admission_status,
            "materialized_temporary_supported",
        )
        self.assertEqual(
            result.operator_blocker_id,
            "gar-flow-2b.materialized_temporary_operator_not_encoded_native",
        )
        self.assertEqual(
            result.operator_blocker_reason,
            "temporary operator materializes Vortex-derived arrays",
        )
        self.assertFalse(result.operator_encoded_native_claim_allowed)
        self.assertTrue(result.operator_temporary_materialization_used)
        self.assertEqual(
            result.facade_compatibility_matrix_report_id,
            "gar0038.facade_compatibility_matrix",
        )
        self.assertEqual(result.facade_compatibility_matrix_gar_id, "GAR-0038-A")
        self.assertEqual(
            result.facade_compatibility_matrix_support_status,
            "mixed_report_only_matrix",
        )
        self.assertEqual(
            result.facade_compatibility_matrix_claim_gate_status,
            "not_claim_grade",
        )
        self.assertIn(
            "sql_dataframe_runtime",
            result.facade_compatibility_matrix_row_order,
        )
        self.assertEqual(result.facade_unsupported_surface_count, 3)
        self.assertEqual(
            result.facade_legacy_boundary_status,
            "legacy_placeholder_removed_or_unsupported",
        )
        self.assertTrue(result.facade_all_rows_no_fallback_no_external_engine)
        self.assertEqual(
            result.facade_compatibility_matrix_fields[
                "surface_sql_dataframe_runtime_support_status"
            ],
            "unsupported",
        )
        self.assertFalse(result.fallback_attempted)
        self.assertFalse(result.external_engine_invoked)
        self.assertEqual(len(result.evidence_slots), 3)
        self.assertEqual(
            result.incomplete_evidence_slots[0].kind,
            "native_io_certificate_refs",
        )

    def test_execution_mode_incomplete_bool_does_not_fall_back_to_flat_field(
        self,
    ) -> None:
        envelope = OutputEnvelope.from_json(
            {
                "schema_version": "shardloom.output.v2",
                "command": "top-level-exec",
                "status": "success",
                "summary": "executed",
                "human_text": "executed",
                "fallback": {
                    "attempted": False,
                    "allowed": False,
                    "engine": None,
                    "reason": "not requested",
                },
                "diagnostics": [],
                "result": {"fields": []},
                "result_refs": [],
                "artifacts": [
                    {
                        "artifact_id": "mode.selection",
                        "artifact_kind": "execution_mode_selection_report",
                        "status": "evidence_incomplete",
                        "payload": {
                            "fields": [
                                {
                                    "key": "vortex_native_claim_allowed",
                                    "value": "evidence_incomplete",
                                }
                            ]
                        },
                    }
                ],
                "artifact_refs": [],
                "certificates": [],
                "policy": {"fields": []},
                "lifecycle": {"fields": []},
                "capability_snapshot": {"fields": []},
                "fields": [
                    {"key": "vortex_native_claim_allowed", "value": "true"},
                ],
            }
        )

        result = ExecutionResultEnvelopeView(envelope)

        self.assertFalse(result.vortex_native_claim_allowed)

    def test_facade_matrix_prefers_prefixed_fields_over_generic_envelope_keys(
        self,
    ) -> None:
        envelope = OutputEnvelope.from_json(
            {
                "schema_version": "shardloom.output.v2",
                "command": "top-level-exec",
                "status": "success",
                "summary": "executed",
                "human_text": "executed",
                "fallback": {
                    "attempted": False,
                    "allowed": False,
                    "engine": None,
                    "reason": "not requested",
                },
                "diagnostics": [],
                "result": {"fields": []},
                "result_refs": [],
                "artifacts": [],
                "artifact_refs": [],
                "certificates": [],
                "policy": {"fields": []},
                "lifecycle": {"fields": []},
                "capability_snapshot": {"fields": []},
                "fields": [
                    {"key": "support_status", "value": "generic_status"},
                    {
                        "key": "facade_compatibility_matrix_support_status",
                        "value": "facade_matrix_status",
                    },
                    {"key": "unsupported_surface_count", "value": "99"},
                    {"key": "facade_unsupported_surface_count", "value": "3"},
                ],
            }
        )

        result = ExecutionResultEnvelopeView(envelope)

        self.assertEqual(
            result.facade_compatibility_matrix_support_status,
            "facade_matrix_status",
        )
        self.assertEqual(result.facade_unsupported_surface_count, 3)

    def test_execution_result_view_reads_external_engine_from_typed_policy(self) -> None:
        envelope = OutputEnvelope.from_json(
            {
                "schema_version": "shardloom.output.v2",
                "command": "top-level-exec",
                "status": "unsupported",
                "summary": "blocked",
                "human_text": "blocked",
                "fallback": {
                    "attempted": False,
                    "allowed": False,
                    "engine": None,
                    "reason": "disabled",
                },
                "diagnostics": [],
                "result": {
                    "fields": [
                        {"key": "plan_id", "value": "plan.blocked"},
                        {"key": "execution_status", "value": "blocked_unsupported"},
                    ]
                },
                "result_refs": [],
                "artifacts": [],
                "artifact_refs": [],
                "certificates": [],
                "policy": {
                    "fields": [
                        {"key": "fallback_attempted", "value": "false"},
                        {"key": "external_engine_invoked", "value": "true"},
                    ]
                },
                "lifecycle": {"fields": []},
                "capability_snapshot": {"fields": []},
                "fields": [
                    {"key": "plan_id", "value": "plan.blocked"},
                    {"key": "execution_status", "value": "blocked_unsupported"},
                ],
            }
        )

        result = ExecutionResultEnvelopeView(envelope)

        self.assertTrue(result.external_engine_invoked)

    def test_v2_envelopes_require_typed_payload_slots(self) -> None:
        payload = {
            "schema_version": "shardloom.output.v2",
            "command": "status",
            "status": "success",
            "summary": "ok",
            "human_text": "ok",
            "fallback": {
                "attempted": False,
                "allowed": False,
                "engine": None,
                "reason": "disabled",
            },
            "diagnostics": [],
            "fields": [],
        }

        with self.assertRaisesRegex(ValueError, "capability_snapshot"):
            OutputEnvelope.from_json(payload)

    def test_capabilities_scope_uses_explicit_scope(self) -> None:
        binary = self.fake_cli(
            textwrap.dedent(
                """
                import json, sys
                assert sys.argv[1:] == ["capabilities", "python", "--format", "json"], sys.argv
                print(json.dumps({
                    "schema_version": "shardloom.output.v2",
                    "command": "capabilities",
                    "status": "success",
                    "summary": "ok",
                    "human_text": "ok",
                    "fallback": {"attempted": False, "allowed": False, "engine": None, "reason": "disabled"},
                    "diagnostics": [],
                    "fields": [{"key": "scope", "value": "python"}],
                }))
                """
            )
        )

        result = ShardLoomClient(binary=binary).capabilities("python")

        self.assertEqual(result.field_map["scope"], "python")

    def test_live_fixture_client_methods_dispatch_expected_commands(self) -> None:
        binary = self.fake_cli(
            textwrap.dedent(
                """
                import json, sys
                args = sys.argv[1:]
                if args == ["live-change-contract-plan", "--format", "json"]:
                    command = "live-change-contract-plan"
                    fields = [
                        {"key": "change_record_field_order", "value": "key,operation,sequence,event_time_ms,processing_time_ms,source_offset,schema_digest,payload_ref"},
                        {"key": "change_operation_vocabulary", "value": "append,upsert,delete,retract,tombstone"},
                        {"key": "fixture_operator_vocabulary", "value": "filter,project,count,count_where,group_count"},
                        {"key": "runtime_execution", "value": "false"},
                    ]
                elif args == ["live-fixture-run", "project", "key,metric", "--format", "json"]:
                    command = "live-fixture-run"
                    fields = [
                        {"key": "fixture_operator", "value": "project"},
                        {"key": "input_change_record_count", "value": "10"},
                        {"key": "active_state_key_count", "value": "3"},
                        {"key": "output_row_count", "value": "3"},
                        {"key": "output_rows", "value": "key=a,metric=east|key=b,metric=west|key=e,metric=east"},
                        {"key": "freshness_certificate_status", "value": "certified"},
                        {"key": "state_certificate_status", "value": "certified"},
                        {"key": "continuous_view_certificate_status", "value": "certified"},
                        {"key": "execution_certificate_status", "value": "certified"},
                        {"key": "native_io_certificate_status", "value": "certified"},
                        {"key": "runtime_execution", "value": "true"},
                        {"key": "fallback_attempted", "value": "false"},
                        {"key": "external_engine_invoked", "value": "false"},
                    ]
                elif args == ["hybrid-overlay-run", "group-count", "metric", "--format", "json"]:
                    command = "hybrid-overlay-run"
                    fields = [
                        {"key": "fixture_operator", "value": "group_count"},
                        {"key": "base_row_count", "value": "4"},
                        {"key": "hot_change_record_count", "value": "6"},
                        {"key": "merged_row_count", "value": "3"},
                        {"key": "output_rows", "value": "east:group_count:2|west:group_count:1"},
                        {"key": "delta_overlay_certificate_status", "value": "certified"},
                        {"key": "micro_segment_flush_evidence_status", "value": "certified"},
                        {"key": "layout_health_bundle_status", "value": "compaction_recommended"},
                        {"key": "freshness_certificate_status", "value": "certified"},
                        {"key": "execution_certificate_status", "value": "certified"},
                        {"key": "native_io_certificate_status", "value": "certified"},
                        {"key": "runtime_execution", "value": "true"},
                        {"key": "data_read", "value": "false"},
                        {"key": "write_io", "value": "false"},
                        {"key": "fallback_attempted", "value": "false"},
                        {"key": "external_engine_invoked", "value": "false"},
                    ]
                else:
                    raise AssertionError(args)
                print(json.dumps({
                    "schema_version": "shardloom.output.v2",
                    "command": command,
                    "status": "success",
                    "summary": "ok",
                    "human_text": "ok",
                    "fallback": {"attempted": False, "allowed": False, "engine": None, "reason": "disabled"},
                    "diagnostics": [],
                    "fields": fields,
                }))
                """
            )
        )
        client = ShardLoomClient(binary=binary)

        contract = client.live_change_contract_plan()
        fixture = client.live_fixture_run("project", ("key", "metric"))
        hybrid = client.hybrid_overlay_run("group-count", "metric")

        self.assertEqual(contract.operations, ("append", "upsert", "delete", "retract", "tombstone"))
        self.assertFalse(contract.runtime_execution)
        self.assertEqual(fixture.operator, "project")
        self.assertEqual(fixture.output_row_count, 3)
        self.assertTrue(fixture.all_certified)
        self.assertFalse(fixture.fallback_attempted)
        self.assertFalse(fixture.external_engine_invoked)
        self.assertEqual(hybrid.operator, "group_count")
        self.assertEqual(hybrid.base_row_count, 4)
        self.assertEqual(hybrid.hot_change_record_count, 6)
        self.assertEqual(hybrid.merged_row_count, 3)
        self.assertEqual(hybrid.output_rows, ("east:group_count:2", "west:group_count:1"))
        self.assertEqual(hybrid.layout_health_status, "compaction_recommended")
        self.assertTrue(hybrid.all_certified)
        self.assertTrue(hybrid.runtime_execution)
        self.assertFalse(hybrid.data_read)
        self.assertFalse(hybrid.write_io)
        self.assertFalse(hybrid.fallback_attempted)
        self.assertFalse(hybrid.external_engine_invoked)

    def test_from_env_reads_client_configuration_without_running_commands(self) -> None:
        binary = self.fake_cli(
            textwrap.dedent(
                """
                import json, sys
                assert sys.argv[1:] == ["status", "--format", "json"], sys.argv
                print(json.dumps({
                    "schema_version": "shardloom.output.v2",
                    "command": "status",
                    "status": "success",
                    "summary": "ok",
                    "human_text": "ok",
                    "fallback": {"attempted": False, "allowed": False, "engine": None, "reason": "disabled"},
                    "diagnostics": [],
                    "fields": [{"key": "engine", "value": "shardloom"}],
                }))
                """
            )
        )

        client = ShardLoomClient.from_env(
            {
                "SHARDLOOM_REPO_ROOT": "repo",
                "SHARDLOOM_PROFILE_ORDER": "debug,release",
                "SHARDLOOM_TIMEOUT_SECONDS": "5",
            },
            binary=binary,
        )

        self.assertEqual(client.status().field("engine"), "shardloom")

    def test_from_env_rejects_invalid_timeout(self) -> None:
        with self.assertRaises(ValueError):
            ShardLoomClient.from_env({"SHARDLOOM_TIMEOUT_SECONDS": "soon"})

    def test_context_constructor_is_side_effect_free(self) -> None:
        ctx = shardloom_context(binary=["definitely-missing-shardloom"])

        self.assertIsInstance(ctx, ShardLoomContext)

    def test_smoke_check_runs_no_dataset_commands(self) -> None:
        binary = self.fake_cli(
            textwrap.dedent(
                """
                import json, sys
                args = sys.argv[1:]
                if args == ["status", "--format", "json"]:
                    command = "status"
                    fields = [
                        {"key": "engine", "value": "shardloom"},
                        {"key": "cli_binary_version", "value": "0.1.0-test"},
                    ]
                elif args == ["capabilities", "python", "--format", "json"]:
                    command = "capabilities"
                    fields = [
                        {"key": "scope", "value": "python"},
                        {"key": "surface_components", "value": "thin_cli_json_wrapper,python_api"},
                    ]
                elif args == ["capabilities", "deployment", "--format", "json"]:
                    command = "capabilities"
                    fields = [{"key": "scope", "value": "deployment"}]
                elif args == ["input-adapters", "--format", "json"]:
                    command = "input-adapters"
                    fields = [{"key": "plan_only", "value": "true"}]
                else:
                    raise AssertionError(args)
                print(json.dumps({
                    "schema_version": "shardloom.output.v2",
                    "command": command,
                    "status": "success",
                    "summary": "ok",
                    "human_text": "ok",
                    "fallback": {"attempted": False, "allowed": False, "engine": None, "reason": "disabled"},
                    "diagnostics": [],
                    "fields": fields,
                }))
                """
            )
        )

        report = ShardLoomClient(binary=binary).smoke_check()

        self.assertEqual(
            report.commands,
            ("status", "capabilities", "capabilities", "input-adapters"),
        )
        self.assertFalse(report.fallback_attempted)
        self.assertEqual(report.python_capabilities.field("scope"), "python")
        self.assertEqual(report.deployment_capabilities.field("scope"), "deployment")
        self.assertTrue(report.input_adapters.field_bool("plan_only"))
        self.assertEqual(report.python_package_version, __version__)
        self.assertEqual(report.protocol_version, "shardloom.output.v2")
        self.assertEqual(report.cli_version, "0.1.0-test")
        self.assertEqual(report.resolved_cli_path, sys.executable)
        self.assertIn("python_api", report.feature_gates)

    def test_context_capabilities_collects_typed_views_without_dataset_commands(self) -> None:
        binary = self.fake_cli(
            textwrap.dedent(
                """
                import json, sys
                args = sys.argv[1:]
                if args == ["status", "--format", "json"]:
                    command = "status"
                    fields = [{"key": "fallback_execution_allowed", "value": "false"}]
                elif args == ["input-adapters", "--format", "json"]:
                    command = "input-adapters"
                    fields = [{"key": "plan_only", "value": "true"}]
                elif len(args) == 4 and args[0] == "capabilities" and args[2:] == ["--format", "json"]:
                    command = "capabilities"
                    scope = args[1]
                    fields = [
                        {"key": "scope", "value": scope},
                        {"key": "capability_status", "value": "planned"},
                    ]
                    if scope == "python":
                        fields.extend([
                            {"key": "support_status", "value": "report_only"},
                            {"key": "claim_gate_status", "value": "not_claim_grade"},
                            {"key": "runtime_execution", "value": "false"},
                            {"key": "data_read", "value": "false"},
                            {"key": "write_io", "value": "false"},
                            {"key": "object_store_io", "value": "false"},
                            {"key": "catalog_io", "value": "false"},
                            {"key": "fallback_attempted", "value": "false"},
                            {"key": "external_engine_invoked", "value": "false"},
                            {"key": "no_fallback", "value": "true"},
                        ])
                    if scope == "adapters":
                        fields.append({"key": "adapter_certification_required", "value": "true"})
                    if scope == "operators":
                        fields.append({"key": "materialization_boundary_reported", "value": "true"})
                    if scope in {"sql", "dataframe"}:
                        fields.extend([
                            {"key": "planner_readiness_claim_gate_status", "value": "not_claim_grade"},
                            {"key": "planner_readiness_row_order", "value": "sql_text_admission,sql_parse,sql_bind,sql_plan,sql_execute,dataframe_lazy_plan,dataframe_expression_builder,dataframe_join,dataframe_aggregate,dataframe_window,plan_diagnostics,unsupported_execution_state"},
                            {"key": "planner_readiness_sql_row_order", "value": "sql_text_admission,sql_parse,sql_bind,sql_plan,sql_execute"},
                            {"key": "planner_readiness_dataframe_row_order", "value": "dataframe_lazy_plan,dataframe_expression_builder,dataframe_join,dataframe_aggregate,dataframe_window"},
                            {"key": "planner_readiness_parser_executed", "value": "false"},
                            {"key": "planner_readiness_binder_executed", "value": "false"},
                            {"key": "planner_readiness_planner_executed", "value": "false"},
                            {"key": "planner_readiness_runtime_execution", "value": "false"},
                            {"key": "planner_readiness_dataframe_runtime", "value": "false"},
                            {"key": "planner_readiness_external_engine_invoked", "value": "false"},
                            {"key": "planner_readiness_fallback_attempted", "value": "false"},
                        ])
                    if scope in {"python", "sql", "dataframe", "api-surfaces"}:
                        fields.extend([
                            {"key": "generated_source_contract_schema_version", "value": "shardloom.generated_source_certificate_contract.v1"},
                            {"key": "generated_source_contract_report_id", "value": "gar-gen-1.generated_source_certificate_contract"},
                            {"key": "generated_source_certificate_schema_version", "value": "shardloom.generated_source_certificate.v1"},
                            {"key": "generated_source_support_status_vocabulary", "value": "smoke_only,fixture_smoke_supported,report_only,planned_runtime"},
                            {"key": "generated_source_case_count", "value": "3"},
                            {"key": "generated_source_case_order", "value": "no_dataset_smoke,user_generated_source,engine_native_generated_source"},
                            {"key": "generated_source_required_field_order", "value": "input_dataset_count,source_io_performed,generated_source_created,generated_source_kind,generated_source_schema_digest,generated_source_row_count,generated_source_plan_digest,generated_source_seed,generation_deterministic,output_io_performed,output_native_io_certificate_status,generated_source_certificate_status,fallback_attempted,external_engine_invoked,claim_gate_status"},
                            {"key": "generated_source_contract_claim_gate_status", "value": "not_claim_grade"},
                            {"key": "generated_source_contract_fallback_attempted", "value": "false"},
                            {"key": "generated_source_contract_external_engine_invoked", "value": "false"},
                            {"key": "generated_source_contract_object_store_io_performed", "value": "false"},
                            {"key": "generated_source_contract_foundry_runtime_invoked", "value": "false"},
                            {"key": "generated_source_contract_broad_sql_dataframe_claim_allowed", "value": "false"},
                            {"key": "no_dataset_smoke_support_status", "value": "smoke_only"},
                            {"key": "no_dataset_smoke_generated_source_certificate_status", "value": "not_applicable_no_generated_rows"},
                            {"key": "no_dataset_smoke_generated_source_created", "value": "false"},
                            {"key": "no_dataset_smoke_output_io_performed", "value": "false"},
                            {"key": "no_dataset_smoke_claim_gate_status", "value": "smoke_only"},
                            {"key": "user_generated_source_support_status", "value": "fixture_smoke_supported"},
                            {"key": "user_generated_source_blocker_id", "value": "none_scoped_local_jsonl_smoke_only"},
                            {"key": "user_generated_source_claim_gate_status", "value": "fixture_smoke_only"},
                            {"key": "engine_native_generated_source_support_status", "value": "fixture_smoke_supported"},
                            {"key": "engine_native_generated_source_blocker_id", "value": "none_scoped_local_range_jsonl_smoke_only"},
                            {"key": "input_dataset_count", "value": "0"},
                            {"key": "source_io_performed", "value": "false"},
                            {"key": "generated_source_created", "value": "false"},
                            {"key": "output_io_performed", "value": "false"},
                            {"key": "generated_source_certificate_status", "value": "not_applicable_no_generated_rows"},
                        ])
                        api_rows = [
                            ("python_ctx_from_rows", "fixture_smoke_supported", "true", "false", "true", "false", "true", "none_scoped_local_jsonl_smoke_only", "generated_source_certificate,output_native_io_certificate,execution_certificate,no_fallback_evidence", "fixture_smoke_only"),
                            ("python_ctx_range", "fixture_smoke_supported", "true", "false", "true", "false", "true", "none_scoped_local_range_jsonl_smoke_only", "generated_source_certificate,output_native_io_certificate,execution_certificate,no_fallback_evidence", "fixture_smoke_only"),
                            ("python_ctx_literal_table", "report_only", "false", "false", "false", "false", "false", "gar-gen-1.literal_table_runtime_not_implemented", "literal_table_generator_contract,generated_source_certificate,output_native_io_certificate,execution_certificate,no_fallback_evidence", "not_claim_grade"),
                            ("python_ctx_calendar", "report_only", "false", "false", "false", "false", "false", "gar-gen-1.calendar_runtime_not_implemented", "calendar_generator_contract,generated_source_certificate,output_native_io_certificate,execution_certificate,no_fallback_evidence", "not_claim_grade"),
                            ("python_generated_source_write", "fixture_smoke_supported", "true", "false", "true", "false", "true", "none_supported_generated_source_write_smokes_only", "generated_source_kind,generated_source_schema_digest,generated_source_row_count,generated_source_plan_digest,output_native_io_certificate,execution_certificate,no_fallback_evidence", "fixture_smoke_only"),
                            ("sql_literal_select", "report_only", "false", "false", "false", "false", "false", "gar-gen-1.sql_literal_select_runtime_not_implemented", "sql_parser,sql_binder,sql_planner,literal_projection_semantics,generated_source_certificate,output_native_io_certificate", "not_claim_grade"),
                            ("sql_values", "report_only", "false", "false", "false", "false", "false", "gar-gen-1.sql_values_runtime_not_implemented", "sql_parser,sql_binder,values_table_semantics,generated_source_certificate,output_native_io_certificate", "not_claim_grade"),
                            ("sql_source_free_projection", "report_only", "false", "false", "false", "false", "false", "gar-gen-1.sql_source_free_projection_runtime_not_implemented", "sql_expression_semantics,projection_plan_digest,generated_source_certificate,execution_certificate", "not_claim_grade"),
                            ("sql_generate_series_range", "report_only", "false", "false", "false", "false", "false", "gar-gen-1.sql_generate_series_range_runtime_not_implemented", "sql_table_function_contract,range_generator_semantics,generated_source_certificate,output_native_io_certificate", "not_claim_grade"),
                            ("dataframe_source_free_projection", "report_only", "false", "false", "false", "false", "false", "gar-gen-1.dataframe_source_free_projection_runtime_not_implemented", "typed_expression_contract,projection_plan_digest,generated_source_certificate,execution_certificate", "not_claim_grade"),
                            ("dataframe_generated_with_column", "report_only", "false", "false", "false", "false", "false", "gar-gen-1.dataframe_generated_with_column_runtime_not_implemented", "expression_engine,type_coercion,determinism_policy,generated_source_certificate,execution_certificate", "not_claim_grade"),
                        ]
                        fields.extend([
                            {"key": "generated_source_api_admission_schema_version", "value": "shardloom.generated_source_api_admission.v1"},
                            {"key": "generated_source_api_admission_matrix_id", "value": "gar-gen-1e.source_free_api_admission"},
                            {"key": "generated_source_api_admission_support_status_vocabulary", "value": "smoke_only,fixture_smoke_supported,report_only,planned_runtime"},
                            {"key": "generated_source_api_admission_claim_gate_status", "value": "not_claim_grade"},
                            {"key": "generated_source_api_admission_row_count", "value": str(len(api_rows))},
                            {"key": "generated_source_api_admission_row_order", "value": ",".join(row[0] for row in api_rows)},
                            {"key": "generated_source_api_admission_python_row_order", "value": ",".join(row[0] for row in api_rows if row[0].startswith("python_"))},
                            {"key": "generated_source_api_admission_sql_row_order", "value": ",".join(row[0] for row in api_rows if row[0].startswith("sql_"))},
                            {"key": "generated_source_api_admission_dataframe_row_order", "value": ",".join(row[0] for row in api_rows if row[0].startswith("dataframe_"))},
                            {"key": "generated_source_api_admission_blocker_ids", "value": ",".join(row[7] for row in api_rows)},
                            {"key": "generated_source_api_admission_required_evidence", "value": ",".join(row[8] for row in api_rows)},
                            {"key": "generated_source_api_admission_runtime_execution", "value": "true"},
                            {"key": "generated_source_api_admission_data_read", "value": "false"},
                            {"key": "generated_source_api_admission_write_io", "value": "true"},
                            {"key": "generated_source_api_admission_source_io_performed", "value": "false"},
                            {"key": "generated_source_api_admission_generated_source_created", "value": "true"},
                            {"key": "generated_source_api_admission_fallback_attempted", "value": "false"},
                            {"key": "generated_source_api_admission_external_engine_invoked", "value": "false"},
                            {"key": "generated_source_api_admission_fallback_execution_allowed", "value": "false"},
                            {"key": "generated_source_api_admission_broad_sql_dataframe_claim_allowed", "value": "false"},
                        ])
                        for row in api_rows:
                            row_id, support, runtime, data_read, write_io, source_io, generated, blocker, evidence, claim = row
                            fields.extend([
                                {"key": f"{row_id}_support_status", "value": support},
                                {"key": f"{row_id}_runtime_execution", "value": runtime},
                                {"key": f"{row_id}_data_read", "value": data_read},
                                {"key": f"{row_id}_write_io", "value": write_io},
                                {"key": f"{row_id}_source_io_performed", "value": source_io},
                                {"key": f"{row_id}_generated_source_created", "value": generated},
                                {"key": f"{row_id}_blocker_id", "value": blocker},
                                {"key": f"{row_id}_required_evidence", "value": evidence},
                                {"key": f"{row_id}_claim_gate_status", "value": claim},
                                {"key": f"{row_id}_fallback_attempted", "value": "false"},
                                {"key": f"{row_id}_external_engine_invoked", "value": "false"},
                                {"key": f"{row_id}_fallback_execution_allowed", "value": "false"},
                            ])
                    if scope in {"workflow", "remote-api", "cross-cg"}:
                        fields.extend([
                            {"key": "severity", "value": "error"},
                            {"key": "blocker_ids", "value": f"cg.{scope}.blocked"},
                            {"key": "required_evidence", "value": "execution_certificate,native_io_certificate"},
                            {"key": "suggested_next_action", "value": "inspect parity report"},
                            {"key": "no_runtime", "value": "true"},
                            {"key": "no_fallback", "value": "true"},
                            {"key": "no_effects", "value": "true"},
                            {"key": "fallback_attempted", "value": "false"},
                            {"key": "external_engine_invoked", "value": "false"},
                        ])
                else:
                    raise AssertionError(args)
                print(json.dumps({
                    "schema_version": "shardloom.output.v2",
                    "command": command,
                    "status": "success",
                    "summary": "ok",
                    "human_text": "ok",
                    "fallback": {"attempted": False, "allowed": False, "engine": None, "reason": "disabled"},
                    "diagnostics": [],
                    "fields": fields,
                }))
                """
            )
        )

        ctx = shardloom_context(binary=binary)
        capabilities = ctx.capabilities()

        self.assertIsInstance(capabilities, ContextCapabilities)
        self.assertIsInstance(capabilities.python, CapabilityView)
        self.assertIsInstance(capabilities.python.posture, CapabilityPosture)
        self.assertEqual(capabilities.python.field("scope"), "python")
        self.assertEqual(capabilities.python.support_status, "report_only")
        self.assertEqual(capabilities.python.claim_gate_status, "not_claim_grade")
        self.assertEqual(
            capabilities.python.claim_gate_statuses,
            ("not_claim_grade", "smoke_only", "fixture_smoke_only"),
        )
        self.assertFalse(capabilities.python.runtime_execution)
        self.assertFalse(capabilities.python.data_read)
        self.assertFalse(capabilities.python.write_io)
        self.assertFalse(capabilities.python.object_store_io)
        self.assertFalse(capabilities.python.catalog_io)
        self.assertFalse(capabilities.python.external_engine_invoked)
        self.assertTrue(capabilities.python.no_fallback)
        self.assertTrue(capabilities.python.no_effects)
        self.assertEqual(capabilities.python.posture.support_status, "report_only")
        self.assertTrue(capabilities.python.posture.report_only)
        self.assertFalse(capabilities.python.posture.supported)
        self.assertFalse(capabilities.python.posture.unsupported)
        self.assertFalse(capabilities.python.posture.claim_grade)
        self.assertEqual(capabilities.deployment.field("scope"), "deployment")
        self.assertEqual(capabilities.engines.field("scope"), "engines")
        self.assertEqual(capabilities.workflow.field("scope"), "workflow")
        self.assertEqual(capabilities.remote_api.field("scope"), "remote-api")
        self.assertEqual(capabilities.cross_cg.field("scope"), "cross-cg")
        self.assertEqual(capabilities.functions.capability_state, "planned")
        self.assertEqual(capabilities.sql_support.scope, "sql")
        self.assertEqual(
            capabilities.sql_support.planner_readiness_claim_gate_status,
            "not_claim_grade",
        )
        self.assertEqual(capabilities.sql_support.claim_gate_status, "not_claim_grade")
        self.assertEqual(
            capabilities.sql_support.claim_gate_statuses,
            ("not_claim_grade", "smoke_only", "fixture_smoke_only"),
        )
        self.assertEqual(
            capabilities.sql_support.sql_planner_readiness_rows,
            (
                "sql_text_admission",
                "sql_parse",
                "sql_bind",
                "sql_plan",
                "sql_execute",
            ),
        )
        self.assertIn(
            "dataframe_join",
            capabilities.dataframe.dataframe_planner_readiness_rows,
        )
        self.assertTrue(capabilities.sql_support.planner_readiness_non_executing)
        self.assertFalse(capabilities.sql_support.runtime_execution)
        self.assertFalse(capabilities.sql_support.external_engine_invoked)
        self.assertTrue(capabilities.sql_support.posture.report_only)
        self.assertFalse(capabilities.sql_support.posture.claim_grade)
        generated_source = capabilities.python.generated_source_contract
        self.assertIsInstance(generated_source, GeneratedSourceCertificateContract)
        self.assertTrue(generated_source.present)
        self.assertEqual(
            generated_source.case_order,
            (
                "no_dataset_smoke",
                "user_generated_source",
                "engine_native_generated_source",
            ),
        )
        self.assertEqual(generated_source.claim_gate_status, "not_claim_grade")
        self.assertTrue(generated_source.all_no_fallback_no_external_engine)
        self.assertTrue(generated_source.no_object_store_or_foundry_runtime)
        self.assertFalse(generated_source.broad_sql_dataframe_claim_allowed)
        self.assertTrue(generated_source.no_dataset_smoke_separate_from_generated_output)
        self.assertEqual(generated_source.no_dataset_smoke.support_status, "smoke_only")
        self.assertEqual(
            generated_source.no_dataset_smoke.generated_source_certificate_status,
            "not_applicable_no_generated_rows",
        )
        self.assertFalse(generated_source.no_dataset_smoke.generated_source_created)
        self.assertFalse(generated_source.no_dataset_smoke.output_io_performed)
        self.assertEqual(
            generated_source.user_generated_source.support_status,
            "fixture_smoke_supported",
        )
        self.assertEqual(
            generated_source.user_generated_source.blocker_id,
            "none_scoped_local_jsonl_smoke_only",
        )
        self.assertEqual(
            generated_source.user_generated_source.claim_gate_status,
            "fixture_smoke_only",
        )
        self.assertEqual(
            generated_source.engine_native_generated_source.support_status,
            "fixture_smoke_supported",
        )
        self.assertEqual(
            generated_source.engine_native_generated_source.blocker_id,
            "none_scoped_local_range_jsonl_smoke_only",
        )
        self.assertEqual(
            capabilities.sql_support.generated_source_contract.schema_version,
            "shardloom.generated_source_certificate_contract.v1",
        )
        self.assertEqual(
            capabilities.dataframe.generated_source_contract.user_generated_source.support_status,
            "fixture_smoke_supported",
        )
        self.assertEqual(
            capabilities.api_surfaces.generated_source_contract.engine_native_generated_source.blocker_id,
            "none_scoped_local_range_jsonl_smoke_only",
        )
        api_admission = capabilities.python.generated_source_api_admission
        self.assertIsInstance(api_admission, GeneratedSourceApiAdmissionMatrix)
        self.assertTrue(api_admission.present)
        self.assertEqual(
            api_admission.python_row_order,
            (
                "python_ctx_from_rows",
                "python_ctx_range",
                "python_ctx_literal_table",
                "python_ctx_calendar",
                "python_generated_source_write",
            ),
        )
        self.assertEqual(
            api_admission.sql_row_order,
            (
                "sql_literal_select",
                "sql_values",
                "sql_source_free_projection",
                "sql_generate_series_range",
            ),
        )
        self.assertEqual(
            api_admission.dataframe_row_order,
            (
                "dataframe_source_free_projection",
                "dataframe_generated_with_column",
            ),
        )
        self.assertEqual(api_admission.claim_gate_status, "not_claim_grade")
        self.assertTrue(api_admission.all_no_fallback_no_external_engine)
        self.assertFalse(api_admission.broad_sql_dataframe_claim_allowed)
        self.assertTrue(
            api_admission.row("python_ctx_from_rows").fixture_smoke_supported
        )
        self.assertTrue(api_admission.row("python_ctx_range").runtime_execution)
        self.assertTrue(api_admission.row("python_generated_source_write").write_io)
        self.assertTrue(api_admission.row("sql_values").report_only)
        self.assertFalse(api_admission.row("sql_values").runtime_execution)
        self.assertEqual(
            api_admission.row("sql_values").blocker_id,
            "gar-gen-1.sql_values_runtime_not_implemented",
        )
        self.assertEqual(
            capabilities.sql_support.generated_source_api_admission.row(
                "sql_literal_select"
            ).support_status,
            "report_only",
        )
        self.assertEqual(
            capabilities.dataframe.generated_source_api_admission.row(
                "dataframe_generated_with_column"
            ).claim_gate_status,
            "not_claim_grade",
        )
        self.assertTrue(
            capabilities.api_surfaces.generated_source_api_admission.row(
                "python_ctx_calendar"
            ).report_only
        )
        self.assertTrue(capabilities.dataframe.planner_readiness_non_executing)
        dataframe_methods = capabilities.dataframe_method_matrix
        self.assertIsInstance(dataframe_methods, DataFrameMethodCapabilityMatrix)
        self.assertEqual(dataframe_methods.scope, "dataframe")
        self.assertIn("filter", dataframe_methods.plan_only_methods)
        self.assertIn("select", dataframe_methods.plan_only_methods)
        self.assertIn("join", dataframe_methods.unsupported_methods)
        self.assertIn("agg", dataframe_methods.unsupported_methods)
        self.assertIn("window", dataframe_methods.unsupported_methods)
        self.assertIn("data_quality", dataframe_methods.unsupported_methods)
        self.assertIn("write_vortex", dataframe_methods.unsupported_methods)
        self.assertIn("from_pandas", dataframe_methods.unsupported_methods)
        self.assertEqual(
            dataframe_methods.row("read_vortex").support_status,
            "source_declaration_supported",
        )
        self.assertEqual(
            dataframe_methods.row("from_rows").support_status,
            "fixture_smoke_supported",
        )
        self.assertTrue(dataframe_methods.row("from_rows").runtime_execution)
        self.assertTrue(dataframe_methods.row("from_rows").write_io)
        self.assertEqual(
            dataframe_methods.row("range").support_status,
            "fixture_smoke_supported",
        )
        self.assertTrue(dataframe_methods.row("range").runtime_execution)
        self.assertTrue(dataframe_methods.row("range").write_io)
        self.assertEqual(
            dataframe_methods.row("join").blocker_id,
            "cg21.workflow.join.operator_unsupported",
        )
        self.assertEqual(
            dataframe_methods.row("write_vortex").required_evidence,
            ("sink_write_evidence", "native_io_certificate", "commit_evidence"),
        )
        self.assertTrue(dataframe_methods.row("to_pandas").materialization_required)
        self.assertEqual(
            dataframe_methods.row("display").blocker_id,
            "cg21.workflow.display.rich_display_unsupported",
        )
        self.assertEqual(dataframe_methods.claim_gate_statuses, ("not_claim_grade",))
        self.assertTrue(dataframe_methods.all_no_fallback_no_external_engine)
        self.assertTrue(dataframe_methods.any_runtime_execution)
        self.assertFalse(dataframe_methods.any_data_read)
        self.assertTrue(dataframe_methods.any_write_io)
        self.assertEqual(
            ctx.dataframe_method_matrix().row("agg").diagnostic_operation,
            "agg",
        )
        self.assertIn("adapter_certification_required", capabilities.adapters.required_gates)
        self.assertIn(
            "materialization_boundary_reported",
            capabilities.operators.materialization_boundaries,
        )
        self.assertTrue(capabilities.input_adapters.field_bool("plan_only"))
        self.assertFalse(capabilities.fallback_attempted)
        self.assertEqual(capabilities.cross_cg.severity, "error")
        self.assertEqual(capabilities.cross_cg.blocker_ids, ("cg.cross-cg.blocked",))
        self.assertEqual(
            capabilities.cross_cg.required_evidence,
            ("execution_certificate", "native_io_certificate"),
        )
        self.assertEqual(capabilities.cross_cg.suggested_next_action, "inspect parity report")
        self.assertTrue(capabilities.cross_cg.no_runtime)
        self.assertTrue(capabilities.cross_cg.no_fallback)
        self.assertTrue(capabilities.cross_cg.no_effects)
        self.assertFalse(capabilities.cross_cg.fallback_attempted)
        self.assertFalse(capabilities.cross_cg.external_engine_invoked)
        self.assertTrue(capabilities.cross_cg.posture.unsupported)
        self.assertTrue(capabilities.cross_cg.posture.report_only)
        self.assertEqual(
            capabilities.cross_cg.posture.required_evidence,
            ("execution_certificate", "native_io_certificate"),
        )
        self.assertEqual(ctx.functions().field("scope"), "functions")
        self.assertEqual(ctx.workflow_capabilities().field("scope"), "workflow")
        self.assertEqual(ctx.remote_api_capabilities().field("scope"), "remote-api")
        self.assertEqual(ctx.cross_cg_capability_parity().field("scope"), "cross-cg")

    def test_engine_capability_matrix_streaming_capability_view(self) -> None:
        binary = self.fake_cli(
            textwrap.dedent(
                """
                import json, sys
                assert sys.argv[1:] == ["engine-capability-matrix", "--format", "json"], sys.argv
                print(json.dumps({
                    "schema_version": "shardloom.output.v2",
                    "command": "engine-capability-matrix",
                    "status": "success",
                    "summary": "ok",
                    "human_text": "ok",
                    "fallback": {"attempted": False, "allowed": False, "engine": None, "reason": "disabled"},
                    "diagnostics": [],
                    "fields": [
                        {"key": "engine_modes", "value": "batch,live,hybrid"},
                        {"key": "live_hybrid_claim_blocked_count", "value": "2"},
                        {"key": "streaming_capability_matrix_report_id", "value": "gar0013.streaming_runtime_capability_matrix"},
                        {"key": "streaming_capability_matrix_row_order", "value": "local_vortex_streaming_plan,object_store_byte_range_streaming_read,bounded_backpressure_plan"},
                        {"key": "streaming_capability_matrix_blocked_row_count", "value": "2"},
                        {"key": "streaming_capability_matrix_diagnostic_code_order", "value": "SL_OBJECT_STORE_UNSUPPORTED,SL_MATERIALIZATION_REQUIRED,SL_NOT_IMPLEMENTED"},
                        {"key": "streaming_capability_matrix_all_rows_no_fallback_no_external_engine", "value": "true"},
                        {"key": "fallback_attempted", "value": "false"},
                        {"key": "external_engine_invoked", "value": "false"}
                    ],
                }))
                """
            )
        )

        result = ShardLoomClient(binary=binary).engine_capability_matrix()

        self.assertIsInstance(result, EngineCapabilityMatrix)
        self.assertEqual(result.engine_modes, ("batch", "live", "hybrid"))
        self.assertEqual(result.live_hybrid_claim_blocked_count, 2)
        self.assertEqual(
            result.streaming_capability_matrix_report_id,
            "gar0013.streaming_runtime_capability_matrix",
        )
        self.assertIn("object_store_byte_range_streaming_read", result.streaming_capability_rows)
        self.assertEqual(result.streaming_capability_blocked_row_count, 2)
        self.assertIn(
            "SL_OBJECT_STORE_UNSUPPORTED",
            result.streaming_capability_diagnostic_codes,
        )
        self.assertTrue(result.streaming_capability_no_fallback_no_external_engine)
        self.assertFalse(result.fallback_attempted)
        self.assertFalse(result.external_engine_invoked)

    def test_context_capabilities_empty_scope_list_is_explicit(self) -> None:
        binary = self.fake_cli(
            textwrap.dedent(
                """
                import json, sys
                assert sys.argv[1:] == ["status", "--format", "json"], sys.argv
                print(json.dumps({
                    "schema_version": "shardloom.output.v2",
                    "command": "status",
                    "status": "success",
                    "summary": "ok",
                    "human_text": "ok",
                    "fallback": {"attempted": False, "allowed": False, "engine": None, "reason": "disabled"},
                    "diagnostics": [],
                    "fields": [{"key": "fallback_execution_allowed", "value": "false"}],
                }))
                """
            )
        )

        capabilities = shardloom_context(binary=binary).capabilities(
            scopes=[],
            include_input_adapters=False,
        )

        self.assertEqual(capabilities.views, {})
        self.assertIsNone(capabilities.input_adapters)

    def test_vortex_run_passes_explicit_runtime_command(self) -> None:
        binary = self.fake_cli(
            textwrap.dedent(
                """
                import json, sys
                assert sys.argv[1:] == ["vortex-run", "file.vortex", "count", "8", "2", "--format", "json"], sys.argv
                print(json.dumps({
                    "schema_version": "shardloom.output.v2",
                    "command": "vortex-run",
                    "status": "success",
                    "summary": "ok",
                    "human_text": "ok",
                    "fallback": {"attempted": False, "allowed": False, "engine": None, "reason": "disabled"},
                    "diagnostics": [],
                    "fields": [{"key": "fallback_execution_allowed", "value": "false"}],
                }))
                """
            )
        )

        result = ShardLoomClient(binary=binary).vortex_run(
            "file.vortex", "count", memory_gb=8, max_parallelism=2
        )

        self.assertEqual(result.command, "vortex-run")
        self.assertEqual(result.field_map["fallback_execution_allowed"], "false")
        self.assertEqual(result.field("fallback_execution_allowed"), "false")
        self.assertTrue(result.field_bool("fallback_execution_allowed") is False)

    def test_vortex_count_helper_dispatches_default_and_local_execution(self) -> None:
        binary = self.fake_cli(
            textwrap.dedent(
                """
                import json, sys
                args = sys.argv[1:]
                if args == ["vortex-count", "file.vortex", "--format", "json"]:
                    fields = [{"key": "local_execution", "value": "false"}]
                elif args == [
                    "vortex-count",
                    "file.vortex",
                    "--execute-local-encoded-count",
                    "8",
                    "2",
                    "--format",
                    "json",
                ]:
                    fields = [{"key": "local_execution", "value": "true"}]
                else:
                    raise AssertionError(args)
                print(json.dumps({
                    "schema_version": "shardloom.output.v2",
                    "command": "vortex-count",
                    "status": "success",
                    "summary": "ok",
                    "human_text": "ok",
                    "fallback": {"attempted": False, "allowed": False, "engine": None, "reason": "disabled"},
                    "diagnostics": [],
                    "fields": fields,
                }))
                """
            )
        )
        client = ShardLoomClient(binary=binary)

        default = client.vortex_count("file.vortex")
        executed = client.vortex_count(
            "file.vortex",
            execute_local_encoded_count=True,
            memory_gb=8,
            max_parallelism=2,
        )

        self.assertFalse(default.field_bool("local_execution"))
        self.assertTrue(executed.field_bool("local_execution"))

    def test_local_vortex_primitive_helpers_dispatch_cli_commands(self) -> None:
        binary = self.fake_cli(
            textwrap.dedent(
                """
                import json, sys
                args = sys.argv[1:]
                expected = {
                    ("vortex-count-where", "file.vortex", "gte:value:3", "--execute-local-primitive", "4", "2", "--format", "json"): "vortex-count-where",
                    ("vortex-filter", "file.vortex", "gte:value:3", "--execute-local-primitive", "4", "2", "--format", "json"): "vortex-filter",
                    ("vortex-project", "file.vortex", "metric,value", "--execute-local-primitive", "4", "2", "--format", "json"): "vortex-project",
                    ("vortex-filter-project", "file.vortex", "gte:value:3", "metric,value", "--execute-local-primitive", "4", "2", "--format", "json"): "vortex-filter-project",
                }
                command = expected.get(tuple(args))
                if command is None:
                    raise AssertionError(args)
                print(json.dumps({
                    "schema_version": "shardloom.output.v2",
                    "command": command,
                    "status": "success",
                    "summary": "ok",
                    "human_text": "ok",
                    "fallback": {"attempted": False, "allowed": False, "engine": None, "reason": "disabled"},
                    "diagnostics": [],
                    "fields": [{"key": "local_execution", "value": "true"}],
                }))
                """
            )
        )
        client = ShardLoomClient(binary=binary)

        count_where = client.vortex_count_where(
            "file.vortex",
            "gte:value:3",
            execute_local_primitive=True,
            memory_gb=4,
            max_parallelism=2,
        )
        filtered = client.vortex_filter(
            "file.vortex",
            "gte:value:3",
            execute_local_primitive=True,
            memory_gb=4,
            max_parallelism=2,
        )
        projected = client.vortex_project(
            "file.vortex",
            ["metric", "value"],
            execute_local_primitive=True,
            memory_gb=4,
            max_parallelism=2,
        )
        filter_project = client.vortex_filter_project(
            "file.vortex",
            "gte:value:3",
            ("metric", "value"),
            execute_local_primitive=True,
            memory_gb=4,
            max_parallelism=2,
        )

        self.assertEqual(count_where.command, "vortex-count-where")
        self.assertEqual(filtered.command, "vortex-filter")
        self.assertEqual(projected.command, "vortex-project")
        self.assertEqual(filter_project.command, "vortex-filter-project")

    def test_local_vortex_primitive_smoke_dispatches_certified_fixture_workflow(self) -> None:
        binary = self.fake_cli(
            textwrap.dedent(
                """
                import json, sys
                args = sys.argv[1:]
                expected = {
                    ("vortex-run", "file.vortex", "count", "3", "4", "--format", "json"): (
                        "vortex-run",
                        [{"key": "local_primitive_rows_scanned", "value": "5"}],
                    ),
                    ("vortex-count-where", "file.vortex", "gte:value:3", "--execute-local-primitive", "3", "4", "--format", "json"): (
                        "vortex-count-where",
                        [{"key": "filtered_count_local_execution_rows_selected", "value": "3"}],
                    ),
                    ("vortex-filter", "file.vortex", "gte:value:3", "--execute-local-primitive", "3", "4", "--format", "json"): (
                        "vortex-filter",
                        [{"key": "filter_local_execution_rows_selected", "value": "3"}],
                    ),
                    ("vortex-project", "file.vortex", "metric,value", "--execute-local-primitive", "3", "4", "--format", "json"): (
                        "vortex-project",
                        [{"key": "project_local_execution_rows_projected", "value": "5"}],
                    ),
                    ("vortex-filter-project", "file.vortex", "gte:value:3", "metric,value", "--execute-local-primitive", "3", "4", "--format", "json"): (
                        "vortex-filter-project",
                        [{"key": "filter_project_local_execution_rows_projected", "value": "3"}],
                    ),
                }
                matched = expected.get(tuple(args))
                if matched is None:
                    raise AssertionError(args)
                command, command_fields = matched
                fields = [
                    {"key": "fallback_execution_allowed", "value": "false"},
                    {"key": "local_primitive_native_io_certificate_emitted", "value": "true"},
                    {"key": "local_primitive_native_io_certificate_status", "value": "certified"},
                    {"key": "local_primitive_native_io_certified", "value": "true"},
                    {"key": "local_primitive_native_io_data_materialized", "value": "false"},
                    {"key": "local_primitive_native_io_fallback_attempted", "value": "false"},
                    {"key": "local_primitive_execution_certificate_emitted", "value": "true"},
                    {"key": "local_primitive_execution_certificate_status", "value": "certified"},
                    {"key": "local_primitive_execution_certificate_correctness_passed", "value": "true"},
                    {"key": "local_primitive_execution_certificate_fallback_attempted", "value": "false"},
                ]
                fields.extend(command_fields)
                print(json.dumps({
                    "schema_version": "shardloom.output.v2",
                    "command": command,
                    "status": "success",
                    "summary": "ok",
                    "human_text": "ok",
                    "fallback": {"attempted": False, "allowed": False, "engine": None, "reason": "disabled"},
                    "diagnostics": [],
                    "fields": fields,
                }))
                """
            )
        )

        report = ShardLoomClient(binary=binary).local_vortex_primitive_smoke(
            "file.vortex",
            columns=("metric", "value"),
            memory_gb=3,
            max_parallelism=4,
        )

        self.assertIsInstance(report, LocalVortexPrimitiveSmokeReport)
        self.assertEqual(
            report.commands,
            (
                "vortex-run",
                "vortex-count-where",
                "vortex-filter",
                "vortex-project",
                "vortex-filter-project",
            ),
        )
        self.assertTrue(report.all_certified)
        self.assertEqual(report.uncertified_commands, ())
        self.assertFalse(report.fallback_attempted)
        self.assertEqual(report.count.field_int("local_primitive_rows_scanned"), 5)
        self.assertEqual(
            report.filter_project.field_int("filter_project_local_execution_rows_projected"),
            3,
        )

    def test_vortex_project_helper_dispatches_default_plan_command(self) -> None:
        binary = self.fake_cli(
            textwrap.dedent(
                """
                import json, sys
                assert sys.argv[1:] == [
                    "vortex-project",
                    "file.vortex",
                    "metric",
                    "--format",
                    "json",
                ], sys.argv
                print(json.dumps({
                    "schema_version": "shardloom.output.v2",
                    "command": "vortex-project",
                    "status": "success",
                    "summary": "ok",
                    "human_text": "ok",
                    "fallback": {"attempted": False, "allowed": False, "engine": None, "reason": "disabled"},
                    "diagnostics": [],
                    "fields": [{"key": "local_execution", "value": "false"}],
                }))
                """
            )
        )

        result = ShardLoomClient(binary=binary).vortex_project("file.vortex", "metric")

        self.assertEqual(result.command, "vortex-project")
        self.assertFalse(result.field_bool("local_execution"))

    def test_vortex_filter_helpers_dispatch_default_plan_commands(self) -> None:
        binary = self.fake_cli(
            textwrap.dedent(
                """
                import json, sys
                args = sys.argv[1:]
                expected = {
                    ("vortex-count-where", "file.vortex", "gte:value:3", "--format", "json"): "vortex-count-where",
                    ("vortex-filter", "file.vortex", "gte:value:3", "--format", "json"): "vortex-filter",
                    ("vortex-filter-project", "file.vortex", "gte:value:3", "metric", "--format", "json"): "vortex-filter-project",
                }
                command = expected.get(tuple(args))
                if command is None:
                    raise AssertionError(args)
                print(json.dumps({
                    "schema_version": "shardloom.output.v2",
                    "command": command,
                    "status": "success",
                    "summary": "ok",
                    "human_text": "ok",
                    "fallback": {"attempted": False, "allowed": False, "engine": None, "reason": "disabled"},
                    "diagnostics": [],
                    "fields": [{"key": "local_execution", "value": "false"}],
                }))
                """
            )
        )
        client = ShardLoomClient(binary=binary)

        count_where = client.vortex_count_where("file.vortex", "gte:value:3")
        filtered = client.vortex_filter("file.vortex", "gte:value:3")
        filter_project = client.vortex_filter_project(
            "file.vortex",
            "gte:value:3",
            "metric",
        )

        self.assertEqual(count_where.command, "vortex-count-where")
        self.assertEqual(filtered.command, "vortex-filter")
        self.assertEqual(filter_project.command, "vortex-filter-project")
        self.assertFalse(filter_project.field_bool("local_execution"))

    def test_vortex_local_execution_helpers_validate_resource_arguments(self) -> None:
        client = ShardLoomClient(binary=["shardloom"])

        with self.assertRaises(ValueError):
            client.vortex_count("file.vortex", execute_local_encoded_count=True)
        with self.assertRaises(ValueError):
            client.vortex_filter(
                "file.vortex",
                "gte:value:3",
                memory_gb=1,
            )
        with self.assertRaises(ValueError):
            client.vortex_project("file.vortex", [])
        with self.assertRaises(ValueError):
            client.vortex_filter_project(
                "file.vortex",
                "gte:value:3",
                "metric",
                execute_local_primitive=True,
                memory_gb=0,
                max_parallelism=2,
            )
        with self.assertRaises(ValueError):
            client.vortex_write_intent_plan("file.vortex", [])

    def test_unsupported_envelope_raises_with_diagnostics_and_fallback(self) -> None:
        binary = self.fake_cli(
            textwrap.dedent(
                """
                import json, sys
                print(json.dumps({
                    "schema_version": "shardloom.output.v2",
                    "command": "capabilities",
                    "status": "unsupported",
                    "summary": "unsupported",
                    "human_text": "unsupported",
                    "fallback": {"attempted": False, "allowed": False, "engine": None, "reason": "disabled"},
                    "diagnostics": [{
                        "code": "UnsupportedSql",
                        "severity": "error",
                        "category": "unsupported_feature",
                        "message": "unsupported",
                        "feature": "sql",
                        "reason": "not implemented",
                        "suggested_next_step": "inspect capabilities",
                        "fallback": {"attempted": False, "allowed": False, "engine": None, "reason": "disabled"}
                    }],
                    "fields": [],
                }))
                sys.exit(1)
                """
            )
        )

        with self.assertRaises(ShardLoomCommandError) as raised:
            ShardLoomClient(binary=binary).capabilities("sql")

        error = raised.exception
        self.assertEqual(error.returncode, 1)
        self.assertFalse(error.envelope.fallback.attempted)
        self.assertEqual(error.envelope.diagnostics[0].code, "UnsupportedSql")

    def test_workflow_error_view_preserves_normalized_diagnostic_categories(self) -> None:
        binary = self.fake_cli(
            textwrap.dedent(
                """
                import json, sys
                print(json.dumps({
                    "schema_version": "shardloom.output.v2",
                    "command": "workflow-unsupported-plan",
                    "status": "unsupported",
                    "summary": "unsupported",
                    "human_text": "unsupported",
                    "fallback": {"attempted": False, "allowed": False, "engine": None, "reason": "disabled"},
                    "diagnostics": [
                        {
                            "code": "SL_MATERIALIZATION_REQUIRED",
                            "severity": "error",
                            "category": "materialization",
                            "message": "materialization blocked",
                            "feature": "cg21.workflow.collect",
                            "reason": "collect is blocked",
                            "suggested_next_step": "request artifact",
                            "fallback": {"attempted": False, "allowed": False, "engine": None, "reason": "disabled"}
                        },
                        {
                            "code": "SL_OBJECT_STORE_UNSUPPORTED",
                            "severity": "error",
                            "category": "object_store",
                            "message": "object store blocked",
                            "feature": "cg21.workflow.object_store_read",
                            "reason": "remote read is blocked",
                            "suggested_next_step": "use object-store plan",
                            "fallback": {"attempted": False, "allowed": False, "engine": None, "reason": "disabled"}
                        },
                        {
                            "code": "SL_NO_FALLBACK_EXECUTION",
                            "severity": "error",
                            "category": "no_fallback_policy",
                            "message": "fallback blocked",
                            "feature": "cg21.workflow.fallback_engine",
                            "reason": "fallback is prohibited",
                            "suggested_next_step": "use native evidence",
                            "fallback": {"attempted": False, "allowed": False, "engine": None, "reason": "disabled"}
                        }
                    ],
                    "fields": [{"key": "fallback_attempted", "value": "false"}],
                }))
                sys.exit(1)
                """
            )
        )

        envelope = ShardLoomClient(binary=binary).workflow_unsupported_plan(
            "collect",
            "read_csv(events.csv)",
            check=False,
        )

        self.assertEqual(
            [diagnostic.category for diagnostic in envelope.diagnostics],
            ["materialization", "object_store", "no_fallback_policy"],
        )
        self.assertTrue(envelope.has_error_diagnostics)
        self.assertFalse(envelope.fallback.attempted)

    def test_object_store_runtime_gate_preserves_blocker_diagnostics(self) -> None:
        binary = self.fake_cli(
            textwrap.dedent(
                """
                import json, sys
                assert sys.argv[1:] == ["cg10-object-store-runtime-gate", "--format", "json"], sys.argv
                print(json.dumps({
                    "schema_version": "shardloom.output.v2",
                    "command": "cg10-object-store-runtime-gate",
                    "status": "success",
                    "summary": "CG-10 object-store runtime promotion gate",
                    "human_text": "runtime blocker diagnostics propagated: true",
                    "fallback": {"attempted": False, "allowed": False, "engine": None, "reason": "disabled"},
                    "diagnostics": [
                        {
                            "code": "SL_OBJECT_STORE_UNSUPPORTED",
                            "severity": "info",
                            "category": "object_store",
                            "message": "object-store runtime action coordinator_start is blocked",
                            "feature": "coordinator_start",
                            "reason": "gar0008b.coordinator_start_blocked requires scheduler_policy before runtime promotion.",
                            "suggested_next_step": "Keep the path report-only until all required evidence is attached.",
                            "fallback": {"attempted": False, "allowed": False, "engine": None, "reason": "disabled"}
                        },
                        {
                            "code": "SL_OBJECT_STORE_UNSUPPORTED",
                            "severity": "info",
                            "category": "object_store",
                            "message": "object-store runtime action commit_record_write is blocked",
                            "feature": "commit_record_write",
                            "reason": "gar0008b.commit_record_write_blocked requires commit_record_schema before runtime promotion.",
                            "suggested_next_step": "Keep the path report-only until all required evidence is attached.",
                            "fallback": {"attempted": False, "allowed": False, "engine": None, "reason": "disabled"}
                        }
                    ],
                    "fields": [
                        {"key": "runtime_blocker_matrix_diagnostics_propagated", "value": "true"},
                        {"key": "runtime_blocker_matrix_diagnostic_count", "value": "2"},
                        {"key": "runtime_blocker_matrix_envelope_status", "value": "success"},
                        {"key": "fallback_attempted", "value": "false"}
                    ],
                }))
                """
            )
        )

        envelope = ShardLoomClient(binary=binary).object_store_runtime_gate()

        self.assertEqual(envelope.status, "success")
        self.assertFalse(envelope.has_error_diagnostics)
        self.assertEqual(
            [diagnostic.category for diagnostic in envelope.diagnostics],
            ["object_store", "object_store"],
        )
        self.assertEqual(
            [diagnostic.feature for diagnostic in envelope.diagnostics],
            ["coordinator_start", "commit_record_write"],
        )
        self.assertTrue(
            envelope.field_bool("runtime_blocker_matrix_diagnostics_propagated")
        )
        self.assertEqual(envelope.field_int("runtime_blocker_matrix_diagnostic_count"), 2)
        self.assertFalse(envelope.diagnostics[0].fallback.attempted)
        self.assertFalse(envelope.fallback.attempted)

    def test_invalid_json_raises_protocol_error(self) -> None:
        binary = self.fake_cli("print('not-json')")

        with self.assertRaises(ShardLoomProtocolError):
            ShardLoomClient(binary=binary).status()

    def test_missing_envelope_fields_raise_protocol_error(self) -> None:
        binary = self.fake_cli("print('{\"schema_version\":\"shardloom.output.v2\"}')")

        with self.assertRaises(ShardLoomProtocolError):
            ShardLoomClient(binary=binary).status()

    def test_successful_plan_with_error_diagnostic_remains_inspectable(self) -> None:
        binary = self.fake_cli(
            textwrap.dedent(
                """
                import json
                print(json.dumps({
                    "schema_version": "shardloom.output.v2",
                    "command": "status",
                    "status": "success",
                    "summary": "ok",
                    "human_text": "ok",
                    "fallback": {"attempted": False, "allowed": False, "engine": None, "reason": "disabled"},
                    "diagnostics": [{
                        "code": "Example",
                        "severity": "error",
                        "category": "invalid_input",
                        "message": "bad",
                        "feature": None,
                        "reason": None,
                        "suggested_next_step": None,
                        "fallback": {"attempted": False, "allowed": False, "engine": None, "reason": "disabled"}
                    }],
                    "fields": [],
                }))
                """
            )
        )

        result = ShardLoomClient(binary=binary).status()

        self.assertFalse(result.is_error)
        self.assertTrue(result.has_error_diagnostics)

    def test_non_json_format_is_rejected(self) -> None:
        client = ShardLoomClient(binary=["shardloom"])

        with self.assertRaises(ValueError):
            client.run(["status", "--format", "text"])

    def test_explicit_binary_overrides_env_default(self) -> None:
        binary = self.fake_cli(
            textwrap.dedent(
                """
                import json
                print(json.dumps({
                    "schema_version": "shardloom.output.v2",
                    "command": "api-compat-plan",
                    "status": "success",
                    "summary": "ok",
                    "human_text": "ok",
                    "fallback": {"attempted": False, "allowed": False, "engine": None, "reason": "disabled"},
                    "diagnostics": [],
                    "fields": [],
                }))
                """
            )
        )
        old = os.environ.get("SHARDLOOM_BIN")
        os.environ["SHARDLOOM_BIN"] = "ignored-test-value"
        try:
            result = ShardLoomClient(binary=binary).api_compat_plan()
        finally:
            if old is None:
                os.environ.pop("SHARDLOOM_BIN", None)
            else:
                os.environ["SHARDLOOM_BIN"] = old

        self.assertEqual(result.command, "api-compat-plan")

    def test_rest_api_contract_plan_view(self) -> None:
        binary = self.fake_cli(
            textwrap.dedent(
                """
                import json, sys
                assert sys.argv[1:] == ["rest-api-contract-plan", "--format", "json"], sys.argv
                print(json.dumps({
                    "schema_version": "shardloom.output.v2",
                    "command": "rest-api-contract-plan",
                    "status": "success",
                    "summary": "ok",
                    "human_text": "ok",
                    "fallback": {"attempted": False, "allowed": False, "engine": None, "reason": "disabled"},
                    "diagnostics": [],
                    "fields": [
                        {"key": "api_version", "value": "v1"},
                        {"key": "openapi_version", "value": "3.2.0"},
                        {"key": "openapi_contract_path", "value": "docs/api/shardloom-openapi-v1.yaml"},
                        {"key": "represented_resources", "value": "health,version,capabilities,governance"},
                        {"key": "discovery_endpoint_paths", "value": "/v1/health,/v1/capabilities"},
                        {"key": "execution_mode_vocabulary", "value": "auto,compatibility_import_certified,prepared_vortex,native_vortex,direct_compatibility_transient"},
                        {"key": "execution_mode_selection_schema_version", "value": "shardloom.execution_mode_selection_report.v1"},
                        {"key": "execution_mode_selection_fields", "value": "requested_execution_mode,selected_execution_mode,mode_selection_reason,support_status,fallback_attempted,external_engine_invoked"},
                        {"key": "rest_execution_mode_support_status", "value": "report_only"},
                        {"key": "unsupported_execution_mode_diagnostic_code", "value": "SL_UNSUPPORTED_EXECUTION_MODE"},
                        {"key": "openapi_contract_artifact_checked_in", "value": "true"},
                        {"key": "server_started", "value": "false"},
                        {"key": "network_listener_opened", "value": "false"},
                        {"key": "fallback_attempted", "value": "false"}
                    ],
                }))
                """
            )
        )

        result = ShardLoomClient(binary=binary).rest_api_contract_plan()

        self.assertIsInstance(result, RestApiContractPlan)
        self.assertEqual(result.api_version, "v1")
        self.assertEqual(result.openapi_version, "3.2.0")
        self.assertEqual(result.openapi_contract_path, "docs/api/shardloom-openapi-v1.yaml")
        self.assertEqual(result.represented_resources[-1], "governance")
        self.assertEqual(result.discovery_endpoint_paths, ("/v1/health", "/v1/capabilities"))
        self.assertIn("native_vortex", result.execution_mode_vocabulary)
        self.assertEqual(
            result.execution_mode_selection_schema_version,
            "shardloom.execution_mode_selection_report.v1",
        )
        self.assertIn("fallback_attempted", result.execution_mode_selection_fields)
        self.assertEqual(result.rest_execution_mode_support_status, "report_only")
        self.assertEqual(
            result.unsupported_execution_mode_diagnostic_code,
            "SL_UNSUPPORTED_EXECUTION_MODE",
        )
        self.assertTrue(result.contract_artifact_checked_in)
        self.assertFalse(result.server_started)
        self.assertFalse(result.network_listener_opened)
        self.assertFalse(result.fallback_attempted)

    def test_serve_discovery_contract_view(self) -> None:
        binary = self.fake_cli(
            textwrap.dedent(
                """
                import json, sys
                assert sys.argv[1:] == [
                    "serve", "--mode", "discovery", "--bind", "127.0.0.1:8787", "--format", "json"
                ], sys.argv
                print(json.dumps({
                    "schema_version": "shardloom.output.v2",
                    "command": "serve",
                    "status": "success",
                    "summary": "ok",
                    "human_text": "ok",
                    "fallback": {"attempted": False, "allowed": False, "engine": None, "reason": "disabled"},
                    "diagnostics": [],
                    "fields": [
                        {"key": "api_version", "value": "v1"},
                        {"key": "openapi_version", "value": "3.2.0"},
                        {"key": "openapi_contract_path", "value": "docs/api/shardloom-openapi-v1.yaml"},
                        {"key": "represented_resources", "value": "health,version,capabilities"},
                        {"key": "discovery_endpoint_paths", "value": "/v1/health,/v1/capabilities"},
                        {"key": "server_mode", "value": "discovery"},
                        {"key": "bind", "value": "127.0.0.1:8787"},
                        {"key": "serve_command_contract_only", "value": "true"},
                        {"key": "server_started", "value": "false"},
                        {"key": "network_listener_opened", "value": "false"},
                        {"key": "fallback_attempted", "value": "false"}
                    ],
                }))
                """
            )
        )

        result = ShardLoomClient(binary=binary).serve_discovery_contract()

        self.assertIsInstance(result, RestApiDiscoveryContract)
        self.assertEqual(result.server_mode, "discovery")
        self.assertEqual(result.bind, "127.0.0.1:8787")
        self.assertTrue(result.contract_only)
        self.assertFalse(result.server_started)
        self.assertFalse(result.network_listener_opened)
        self.assertFalse(result.fallback_attempted)

    def test_rest_api_plan_preview_view(self) -> None:
        binary = self.fake_cli(
            textwrap.dedent(
                """
                import json, sys
                assert sys.argv[1:] == [
                    "rest-api-plan-preview", "unsupported-operator", "--format", "json"
                ], sys.argv
                print(json.dumps({
                    "schema_version": "shardloom.output.v2",
                    "command": "rest-api-plan-preview",
                    "status": "unsupported",
                    "summary": "ok",
                    "human_text": "ok",
                    "fallback": {"attempted": False, "allowed": False, "engine": None, "reason": "disabled"},
                    "diagnostics": [{
                        "code": "SL_UNSUPPORTED_SQL",
                        "severity": "error",
                        "category": "unsupported_feature",
                        "message": "unsupported",
                        "feature": "rest_api_plan_operator",
                        "reason": "unsupported operator rejected without fallback execution",
                        "suggested_next_step": "rewrite",
                        "fallback": {"attempted": False, "allowed": False, "engine": None, "reason": "disabled"}
                    }],
                    "fields": [
                        {"key": "scenario", "value": "unsupported-operator"},
                        {"key": "preview_status", "value": "unsupported"},
                        {"key": "plan_handle", "value": "plan://cg23/unsupported-operator"},
                        {"key": "preview_operations", "value": "plan_handle,validate,explain,estimate,unsupported_report,certification_preview"},
                        {"key": "stage_order", "value": "parser,binder,native_logical,native_physical,execution_readiness,evidence_readiness,certification"},
                        {"key": "parser_stage_status", "value": "ready"},
                        {"key": "binder_stage_status", "value": "ready"},
                        {"key": "native_logical_stage_status", "value": "unsupported"},
                        {"key": "native_physical_stage_status", "value": "not_evaluated"},
                        {"key": "execution_readiness_stage_status", "value": "not_evaluated"},
                        {"key": "evidence_readiness_stage_status", "value": "not_evaluated"},
                        {"key": "certification_stage_status", "value": "not_evaluated"},
                        {"key": "problem_details_emitted", "value": "true"},
                        {"key": "problem_details_type", "value": "https://shardloom.dev/problems/unsupported-plan-operator"},
                        {"key": "problem_details_status", "value": "422"},
                        {"key": "problem_details_diagnostic_code", "value": "SL_UNSUPPORTED_SQL"},
                        {"key": "unsupported_reason", "value": "unsupported operator rejected without fallback execution"},
                        {"key": "server_started", "value": "false"},
                        {"key": "network_listener_opened", "value": "false"},
                        {"key": "runtime_execution", "value": "false"},
                        {"key": "fallback_attempted", "value": "false"},
                        {"key": "execution_delegated", "value": "false"}
                    ],
                }))
                """
            )
        )

        result = ShardLoomClient(binary=binary).rest_api_plan_preview(
            "unsupported-operator",
            check=False,
        )

        self.assertIsInstance(result, RestApiPlanPreview)
        self.assertEqual(result.scenario, "unsupported-operator")
        self.assertEqual(result.preview_status, "unsupported")
        self.assertEqual(result.plan_handle, "plan://cg23/unsupported-operator")
        self.assertIn("certification_preview", result.operations)
        self.assertEqual(result.stage_order[2], "native_logical")
        self.assertEqual(result.stage_statuses["native_logical"], "unsupported")
        self.assertTrue(result.problem_details_emitted)
        self.assertEqual(
            result.problem_details_type,
            "https://shardloom.dev/problems/unsupported-plan-operator",
        )
        self.assertEqual(result.problem_details_status, 422)
        self.assertEqual(result.problem_details_diagnostic_code, "SL_UNSUPPORTED_SQL")
        self.assertIn("without fallback", result.unsupported_reason or "")
        self.assertFalse(result.server_started)
        self.assertFalse(result.network_listener_opened)
        self.assertFalse(result.runtime_execution)
        self.assertFalse(result.fallback_attempted)
        self.assertFalse(result.execution_delegated)

    def test_workflow_unsupported_plan_direct_view_defaults_to_non_raising(self) -> None:
        binary = self.fake_cli(
            textwrap.dedent(
                """
                import json, sys
                assert sys.argv[1:] == [
                    "workflow-unsupported-plan", "collect", "read_csv(events.csv)", "--format", "json"
                ], sys.argv
                print(json.dumps({
                    "schema_version": "shardloom.output.v2",
                    "command": "workflow-unsupported-plan",
                    "status": "unsupported",
                    "summary": "unsupported",
                    "human_text": "unsupported",
                    "fallback": {"attempted": False, "allowed": False, "engine": None, "reason": "disabled"},
                    "diagnostics": [{
                        "code": "SL_UNSUPPORTED_SQL",
                        "severity": "error",
                        "category": "unsupported_feature",
                        "message": "unsupported",
                        "feature": "workflow_unsupported_plan",
                        "reason": "unsupported report",
                        "suggested_next_step": "inspect capability report",
                        "fallback": {"attempted": False, "allowed": False, "engine": None, "reason": "disabled"}
                    }],
                    "fields": [
                        {"key": "workflow_operation", "value": "collect"},
                        {"key": "blocker_id", "value": "cg21.workflow.collect.materialization_unsupported"},
                        {"key": "fallback_attempted", "value": "false"}
                    ],
                }))
                sys.exit(1)
                """
            )
        )

        result = ShardLoomClient(binary=binary).workflow_unsupported_plan(
            "collect",
            "read_csv(events.csv)",
        )

        self.assertEqual(result.command, "workflow-unsupported-plan")
        self.assertEqual(result.status, "unsupported")
        self.assertEqual(
            result.field("blocker_id"),
            "cg21.workflow.collect.materialization_unsupported",
        )
        self.assertFalse(result.fallback.attempted)

    def test_workload_certification_dossier_view(self) -> None:
        binary = self.fake_cli(
            textwrap.dedent(
                """
                import json, sys
                assert sys.argv[1:] == [
                    "workload-certification-dossier", "local-vortex-count", "--format", "json"
                ], sys.argv
                print(json.dumps({
                    "schema_version": "shardloom.output.v2",
                    "command": "workload-certification-dossier",
                    "status": "success",
                    "summary": "ok",
                    "human_text": "ok",
                    "fallback": {"attempted": False, "allowed": False, "engine": None, "reason": "disabled"},
                    "diagnostics": [],
                    "fields": [
                        {"key": "scenario", "value": "local-vortex-count"},
                        {"key": "workload_id", "value": "workload://cg7/local-vortex-count"},
                        {"key": "overall_status", "value": "partial"},
                        {"key": "certificate_refs", "value": "certificates/cg16/local-vortex-count/execution.json,certificates/cg19/local-vortex-count/native-io.json"},
                        {"key": "missing_evidence", "value": "claim_grade_benchmark_results,api_contract_workload_mapping"},
                        {"key": "blocked_evidence", "value": "cg6.benchmark.claim_grade_results_missing"},
                        {"key": "unsupported_evidence", "value": "none"},
                        {"key": "blocker_ids", "value": "cg6.benchmark.claim_grade_results_missing,cg23.api.workload_mapping_planned"},
                        {"key": "suggested_next_action", "value": "Run benchmark-claim-evidence-plan and rest-api-contract-plan before publishing this workload as certified."},
                        {"key": "no_runtime", "value": "true"},
                        {"key": "no_fallback", "value": "true"},
                        {"key": "no_effects", "value": "true"}
                    ],
                }))
                """
            )
        )

        result = ShardLoomClient(binary=binary).workload_certification_dossier()

        self.assertIsInstance(result, WorkloadCertificationDossier)
        self.assertEqual(result.scenario, "local-vortex-count")
        self.assertEqual(result.workload_id, "workload://cg7/local-vortex-count")
        self.assertEqual(result.overall_status, "partial")
        self.assertIn(
            "certificates/cg16/local-vortex-count/execution.json",
            result.certificate_refs,
        )
        self.assertIn("claim_grade_benchmark_results", result.missing_evidence)
        self.assertEqual(
            result.blocked_evidence,
            ("cg6.benchmark.claim_grade_results_missing",),
        )
        self.assertEqual(result.unsupported_evidence, ())
        self.assertIn("cg23.api.workload_mapping_planned", result.blocker_ids)
        self.assertIn("benchmark-claim-evidence-plan", result.suggested_next_action or "")
        self.assertTrue(result.no_runtime)
        self.assertTrue(result.no_fallback)
        self.assertTrue(result.no_effects)

    def test_claim_gate_closeout_view(self) -> None:
        binary = self.fake_cli(
            textwrap.dedent(
                """
                import json, sys
                assert sys.argv[1:] == ["claim-gate-closeout", "--format", "json"], sys.argv
                print(json.dumps({
                    "schema_version": "shardloom.output.v2",
                    "command": "claim-gate-closeout",
                    "status": "success",
                    "summary": "ok",
                    "human_text": "ok",
                    "fallback": {"attempted": False, "allowed": False, "engine": None, "reason": "disabled"},
                    "diagnostics": [],
                    "fields": [
                        {"key": "p7_closeout_status", "value": "complete_report_only"},
                        {"key": "claim_gate_status", "value": "blocked_for_broad_claims"},
                        {"key": "release_readiness_status", "value": "blocked_until_priority_8"},
                        {"key": "allowed_claims", "value": "report_only_workflow_diagnostics,local_vortex_count_fixture_evidence"},
                        {"key": "blocked_claims", "value": "production_workflow_certification,public_package_release,comparative_performance_claims"},
                        {"key": "out_of_scope_claims", "value": "external_engine_fallback,foundry_platform_execution"},
                        {"key": "blocker_ids", "value": "p7.claim_gate.broad_claims_blocked,p8.release.package_artifacts_missing"},
                        {"key": "no_runtime", "value": "true"},
                        {"key": "no_fallback", "value": "true"},
                        {"key": "no_effects", "value": "true"}
                    ],
                }))
                """
            )
        )

        result = ShardLoomClient(binary=binary).claim_gate_closeout()

        self.assertIsInstance(result, ClaimGateCloseoutReport)
        self.assertEqual(result.claim_gate_status, "blocked_for_broad_claims")
        self.assertEqual(result.release_readiness_status, "blocked_until_priority_8")
        self.assertEqual(result.p7_closeout_status, "complete_report_only")
        self.assertIn("local_vortex_count_fixture_evidence", result.allowed_claims)
        self.assertIn("public_package_release", result.blocked_claims)
        self.assertIn("external_engine_fallback", result.out_of_scope_claims)
        self.assertIn("p8.release.package_artifacts_missing", result.blocker_ids)
        self.assertTrue(result.no_runtime)
        self.assertTrue(result.no_fallback)
        self.assertTrue(result.no_effects)

    def test_compute_capability_matrix_view(self) -> None:
        binary = self.fake_cli(
            textwrap.dedent(
                """
                import json, sys
                assert sys.argv[1:] == ["compute-capability-matrix", "--format", "json"], sys.argv
                print(json.dumps({
                    "schema_version": "shardloom.output.v2",
                    "command": "compute-capability-matrix",
                    "status": "success",
                    "summary": "ok",
                    "human_text": "ok",
                    "fallback": {"attempted": False, "allowed": False, "engine": None, "reason": "disabled"},
                    "diagnostics": [],
                    "fields": [
                        {"key": "matrix_status", "value": "report_only"},
                        {"key": "claim_grade_status", "value": "evidence_incomplete"},
                        {"key": "native_vortex_admission_status", "value": "scoped_fixture_lane_admitted"},
                        {"key": "native_vortex_admission_lane_order", "value": "local_vortex_count_scalar"},
                        {"key": "native_vortex_admission_lane_local_vortex_count_scalar_source_surface", "value": "local_vortex_file_scan"},
                        {"key": "native_vortex_admission_lane_local_vortex_count_scalar_operator_surface", "value": "count_all"},
                        {"key": "native_vortex_admission_lane_local_vortex_count_scalar_sink_surface", "value": "typed_scalar_result"},
                        {"key": "native_vortex_admission_lane_local_vortex_count_scalar_admission_status", "value": "admitted_fixture_certified"},
                        {"key": "native_vortex_admission_lane_local_vortex_count_scalar_support_status", "value": "fixture_certified"},
                        {"key": "native_vortex_admission_lane_local_vortex_count_scalar_execution_mode", "value": "native_vortex"},
                        {"key": "native_vortex_admission_lane_local_vortex_count_scalar_provider_kind", "value": "vortex_scan"},
                        {"key": "native_vortex_admission_lane_local_vortex_count_scalar_provider_api_surface", "value": "VortexFile::scan,ScanBuilder::into_array_iter"},
                        {"key": "native_vortex_admission_lane_local_vortex_count_scalar_provider_crate", "value": "vortex"},
                        {"key": "native_vortex_admission_lane_local_vortex_count_scalar_provider_version", "value": "0.70"},
                        {"key": "native_vortex_admission_lane_local_vortex_count_scalar_feature_gate", "value": "vortex-encoded-read-spike"},
                        {"key": "native_vortex_admission_lane_local_vortex_count_scalar_shardloom_admission_policy", "value": "local_fixture_scan_count_only"},
                        {"key": "native_vortex_admission_lane_local_vortex_count_scalar_compute_row_ref", "value": "compute_row.local_vortex_count"},
                        {"key": "native_vortex_admission_lane_local_vortex_count_scalar_benchmark_ref", "value": "vortex-count-benchmark.local_fixture_smoke"},
                        {"key": "native_vortex_admission_lane_local_vortex_count_scalar_correctness_refs", "value": "cg5.local_vortex_count,query_primitive_correctness"},
                        {"key": "native_vortex_admission_lane_local_vortex_count_scalar_execution_certificate_refs", "value": "certificates/cg16/local-vortex-count/execution.json"},
                        {"key": "native_vortex_admission_lane_local_vortex_count_scalar_native_io_refs", "value": "certificates/cg19/local-vortex-count/native-io.json"},
                        {"key": "native_vortex_admission_lane_local_vortex_count_scalar_materialization_decode_refs", "value": "native_vortex_source_to_scalar_count_result"},
                        {"key": "native_vortex_admission_lane_local_vortex_count_scalar_policy_refs", "value": "fallback_attempted=false,external_engine_invoked=false"},
                        {"key": "native_vortex_admission_lane_local_vortex_count_scalar_required_future_evidence", "value": "claim_grade_benchmark_rows,broad_source_sink_operator_coverage"},
                        {"key": "native_vortex_admission_lane_local_vortex_count_scalar_claim_gate_status", "value": "fixture_smoke_only"},
                        {"key": "native_vortex_admission_lane_local_vortex_count_scalar_claim_boundary", "value": "local_count_all_fixture_smoke_only_not_universal_native_vortex"},
                        {"key": "native_vortex_admission_lane_local_vortex_count_scalar_residual_executor", "value": "none"},
                        {"key": "native_vortex_admission_lane_local_vortex_count_scalar_vortex_native_claim_allowed", "value": "true"},
                        {"key": "native_vortex_admission_lane_local_vortex_count_scalar_fallback_attempted", "value": "false"},
                        {"key": "native_vortex_admission_lane_local_vortex_count_scalar_external_engine_invoked", "value": "false"},
                        {"key": "native_vortex_admission_lane_local_vortex_count_scalar_object_store_io", "value": "false"},
                        {"key": "native_vortex_admission_lane_local_vortex_count_scalar_write_io", "value": "false"},
                        {"key": "native_unsupported_coverage_status", "value": "complete_for_current_matrix"},
                        {"key": "native_unsupported_coverage_current_matrix_complete", "value": "true"},
                        {"key": "native_unsupported_coverage_row_order", "value": "native_source_object_store_range,native_operator_joins,native_workload_sql_dataframe"},
                        {"key": "native_unsupported_coverage_row_native_source_object_store_range_category", "value": "source"},
                        {"key": "native_unsupported_coverage_row_native_source_object_store_range_surface", "value": "object_store_range_read"},
                        {"key": "native_unsupported_coverage_row_native_source_object_store_range_support_status", "value": "unsupported"},
                        {"key": "native_unsupported_coverage_row_native_source_object_store_range_unsupported_diagnostic_code", "value": "SL_UNSUPPORTED_NATIVE_OBJECT_STORE_SOURCE"},
                        {"key": "native_unsupported_coverage_row_native_source_object_store_range_blocker_id", "value": "gar0002.native.source.object_store_range"},
                        {"key": "native_unsupported_coverage_row_native_source_object_store_range_required_future_evidence", "value": "object_store_request_planner,native_io_certificate"},
                        {"key": "native_unsupported_coverage_row_native_source_object_store_range_source_refs", "value": "docs/architecture/object-store-request-planner.md"},
                        {"key": "native_unsupported_coverage_row_native_source_object_store_range_claim_gate_status", "value": "not_claim_grade"},
                        {"key": "native_unsupported_coverage_row_native_source_object_store_range_execution_attempted", "value": "false"},
                        {"key": "native_unsupported_coverage_row_native_source_object_store_range_fallback_attempted", "value": "false"},
                        {"key": "native_unsupported_coverage_row_native_source_object_store_range_external_engine_invoked", "value": "false"},
                        {"key": "native_unsupported_coverage_row_native_operator_joins_category", "value": "operator"},
                        {"key": "native_unsupported_coverage_row_native_operator_joins_surface", "value": "join"},
                        {"key": "native_unsupported_coverage_row_native_operator_joins_support_status", "value": "unsupported"},
                        {"key": "native_unsupported_coverage_row_native_operator_joins_unsupported_diagnostic_code", "value": "SL_UNSUPPORTED_NATIVE_JOIN"},
                        {"key": "native_unsupported_coverage_row_native_operator_joins_blocker_id", "value": "cg21.workflow.join.operator_unsupported"},
                        {"key": "native_unsupported_coverage_row_native_operator_joins_required_future_evidence", "value": "join_null_semantics,build_probe_memory_policy,benchmark_row"},
                        {"key": "native_unsupported_coverage_row_native_operator_joins_source_refs", "value": "docs/architecture/compute-engine-flow-reference.md"},
                        {"key": "native_unsupported_coverage_row_native_operator_joins_claim_gate_status", "value": "not_claim_grade"},
                        {"key": "native_unsupported_coverage_row_native_operator_joins_execution_attempted", "value": "false"},
                        {"key": "native_unsupported_coverage_row_native_operator_joins_fallback_attempted", "value": "false"},
                        {"key": "native_unsupported_coverage_row_native_operator_joins_external_engine_invoked", "value": "false"},
                        {"key": "native_unsupported_coverage_row_native_workload_sql_dataframe_category", "value": "workload"},
                        {"key": "native_unsupported_coverage_row_native_workload_sql_dataframe_surface", "value": "sql_dataframe_frontend"},
                        {"key": "native_unsupported_coverage_row_native_workload_sql_dataframe_support_status", "value": "unsupported"},
                        {"key": "native_unsupported_coverage_row_native_workload_sql_dataframe_unsupported_diagnostic_code", "value": "SL_UNSUPPORTED_SQL_DATAFRAME_RUNTIME"},
                        {"key": "native_unsupported_coverage_row_native_workload_sql_dataframe_blocker_id", "value": "cg21.workflow.sql.frontend_unsupported"},
                        {"key": "native_unsupported_coverage_row_native_workload_sql_dataframe_required_future_evidence", "value": "sql_parser,binder,planner,dataframe_api_semantics"},
                        {"key": "native_unsupported_coverage_row_native_workload_sql_dataframe_source_refs", "value": "docs/architecture/global-architecture-review.md"},
                        {"key": "native_unsupported_coverage_row_native_workload_sql_dataframe_claim_gate_status", "value": "not_claim_grade"},
                        {"key": "native_unsupported_coverage_row_native_workload_sql_dataframe_execution_attempted", "value": "false"},
                        {"key": "native_unsupported_coverage_row_native_workload_sql_dataframe_fallback_attempted", "value": "false"},
                        {"key": "native_unsupported_coverage_row_native_workload_sql_dataframe_external_engine_invoked", "value": "false"},
                        {"key": "predicate_dtype_coverage_current_matrix_complete", "value": "true"},
                        {"key": "predicate_dtype_coverage_row_order", "value": "predicate_i64_range,nested_field_pruning"},
                        {"key": "predicate_dtype_coverage_row_predicate_i64_range_category", "value": "predicate"},
                        {"key": "predicate_dtype_coverage_row_predicate_i64_range_family", "value": "range_comparison"},
                        {"key": "predicate_dtype_coverage_row_predicate_i64_range_surface", "value": "i64_min_max_pruning_and_native_filter"},
                        {"key": "predicate_dtype_coverage_row_predicate_i64_range_support_status", "value": "fixture_certified"},
                        {"key": "predicate_dtype_coverage_row_predicate_i64_range_runtime_surface", "value": "metadata_pruning,prepared_vortex,native_vortex"},
                        {"key": "predicate_dtype_coverage_row_predicate_i64_range_statistics_required", "value": "row_count,min_value,max_value,null_count"},
                        {"key": "predicate_dtype_coverage_row_predicate_i64_range_fixture_status", "value": "local_fixture_present"},
                        {"key": "predicate_dtype_coverage_row_predicate_i64_range_correctness_refs", "value": "query_primitive_correctness.filtered_count,traditional_analytics.partition_pruning"},
                        {"key": "predicate_dtype_coverage_row_predicate_i64_range_benchmark_refs", "value": "traditional_analytics.partition_pruning"},
                        {"key": "predicate_dtype_coverage_row_predicate_i64_range_execution_certificate_refs", "value": "fixture_execution_certificate_required_for_claim_grade"},
                        {"key": "predicate_dtype_coverage_row_predicate_i64_range_native_io_refs", "value": "native_io_certificate_required_for_source_bound_data"},
                        {"key": "predicate_dtype_coverage_row_predicate_i64_range_materialization_decode_refs", "value": "metadata_pruning_or_encoded_filter_no_full_materialization"},
                        {"key": "predicate_dtype_coverage_row_predicate_i64_range_unsupported_diagnostic_code", "value": "none"},
                        {"key": "predicate_dtype_coverage_row_predicate_i64_range_blocker_id", "value": "gar0006a.range_claim_grade_evidence_missing"},
                        {"key": "predicate_dtype_coverage_row_predicate_i64_range_required_future_evidence", "value": "claim_grade_range_fixture_matrix,benchmark_rows,native_io_certificate"},
                        {"key": "predicate_dtype_coverage_row_predicate_i64_range_claim_gate_status", "value": "fixture_smoke_only"},
                        {"key": "predicate_dtype_coverage_row_predicate_i64_range_claim_boundary", "value": "scoped i64 range/equality fixture coverage, not broad predicate coverage"},
                        {"key": "predicate_dtype_coverage_row_predicate_i64_range_execution_attempted", "value": "false"},
                        {"key": "predicate_dtype_coverage_row_predicate_i64_range_fallback_attempted", "value": "false"},
                        {"key": "predicate_dtype_coverage_row_predicate_i64_range_external_engine_invoked", "value": "false"},
                        {"key": "predicate_dtype_coverage_row_nested_field_pruning_category", "value": "nested_shape"},
                        {"key": "predicate_dtype_coverage_row_nested_field_pruning_family", "value": "nested_struct_list_map"},
                        {"key": "predicate_dtype_coverage_row_nested_field_pruning_surface", "value": "nested_json_or_struct_field_predicate"},
                        {"key": "predicate_dtype_coverage_row_nested_field_pruning_support_status", "value": "unsupported"},
                        {"key": "predicate_dtype_coverage_row_nested_field_pruning_runtime_surface", "value": "unsupported"},
                        {"key": "predicate_dtype_coverage_row_nested_field_pruning_statistics_required", "value": "nested_field_path_stats,parent_child_presence,definition_repetition_policy"},
                        {"key": "predicate_dtype_coverage_row_nested_field_pruning_fixture_status", "value": "blocked"},
                        {"key": "predicate_dtype_coverage_row_nested_field_pruning_correctness_refs", "value": "nested_json_fixture_required"},
                        {"key": "predicate_dtype_coverage_row_nested_field_pruning_benchmark_refs", "value": "nested_json_field_scan_coverage_only"},
                        {"key": "predicate_dtype_coverage_row_nested_field_pruning_execution_certificate_refs", "value": "none"},
                        {"key": "predicate_dtype_coverage_row_nested_field_pruning_native_io_refs", "value": "none"},
                        {"key": "predicate_dtype_coverage_row_nested_field_pruning_materialization_decode_refs", "value": "unsupported_no_nested_decode_or_materialization"},
                        {"key": "predicate_dtype_coverage_row_nested_field_pruning_unsupported_diagnostic_code", "value": "SL_UNSUPPORTED_NESTED_FIELD_PRUNING"},
                        {"key": "predicate_dtype_coverage_row_nested_field_pruning_blocker_id", "value": "gar0006a.nested_field_pruning_unsupported"},
                        {"key": "predicate_dtype_coverage_row_nested_field_pruning_required_future_evidence", "value": "nested_path_stats,struct_list_map_semantics,deterministic_diagnostic"},
                        {"key": "predicate_dtype_coverage_row_nested_field_pruning_claim_gate_status", "value": "not_claim_grade"},
                        {"key": "predicate_dtype_coverage_row_nested_field_pruning_claim_boundary", "value": "nested benchmark fixture coverage is not native nested pruning support"},
                        {"key": "predicate_dtype_coverage_row_nested_field_pruning_execution_attempted", "value": "false"},
                        {"key": "predicate_dtype_coverage_row_nested_field_pruning_fallback_attempted", "value": "false"},
                        {"key": "predicate_dtype_coverage_row_nested_field_pruning_external_engine_invoked", "value": "false"},
                        {"key": "materialization_policy_schema_version", "value": "shardloom.materialization_policy.v1"},
                        {"key": "materialization_policy_report_ref", "value": "compute-capability-matrix://materialization_policy.v1"},
                        {"key": "materialization_policy_row_order", "value": "encoded_native_operator_path,materialized_temporary_operator_path,unsupported_operator_path"},
                        {"key": "materialization_policy_all_rows_classified", "value": "true"},
                        {"key": "materialization_policy_row_encoded_native_operator_path_operator_execution_class", "value": "encoded_native"},
                        {"key": "materialization_policy_row_encoded_native_operator_path_support_status", "value": "report_only_contract"},
                        {"key": "materialization_policy_row_encoded_native_operator_path_data_decoded", "value": "false"},
                        {"key": "materialization_policy_row_encoded_native_operator_path_data_materialized", "value": "false"},
                        {"key": "materialization_policy_row_encoded_native_operator_path_stayed_encoded", "value": "true"},
                        {"key": "materialization_policy_row_encoded_native_operator_path_materialization_boundary_required", "value": "true"},
                        {"key": "materialization_policy_row_encoded_native_operator_path_materialization_boundary_emitted", "value": "true"},
                        {"key": "materialization_policy_row_encoded_native_operator_path_materialized_temporary_path", "value": "false"},
                        {"key": "materialization_policy_row_encoded_native_operator_path_encoded_native_claim_allowed", "value": "true"},
                        {"key": "materialization_policy_row_encoded_native_operator_path_materialization_decode_refs", "value": "metadata_or_encoded_values_no_row_materialization"},
                        {"key": "materialization_policy_row_encoded_native_operator_path_policy_refs", "value": "fallback_attempted=false,external_engine_invoked=false"},
                        {"key": "materialization_policy_row_encoded_native_operator_path_unsupported_diagnostic_code", "value": "none"},
                        {"key": "materialization_policy_row_encoded_native_operator_path_blocker_id", "value": "none"},
                        {"key": "materialization_policy_row_encoded_native_operator_path_required_future_evidence", "value": "execution_certificate,native_io_certificate"},
                        {"key": "materialization_policy_row_encoded_native_operator_path_claim_gate_status", "value": "fixture_or_claim_gate_dependent"},
                        {"key": "materialization_policy_row_encoded_native_operator_path_claim_boundary", "value": "encoded evidence required"},
                        {"key": "materialization_policy_row_encoded_native_operator_path_runtime_execution", "value": "false"},
                        {"key": "materialization_policy_row_encoded_native_operator_path_fallback_attempted", "value": "false"},
                        {"key": "materialization_policy_row_encoded_native_operator_path_external_engine_invoked", "value": "false"},
                        {"key": "materialization_policy_row_materialized_temporary_operator_path_operator_execution_class", "value": "materialized_temporary"},
                        {"key": "materialization_policy_row_materialized_temporary_operator_path_support_status", "value": "supported_with_boundary"},
                        {"key": "materialization_policy_row_materialized_temporary_operator_path_data_decoded", "value": "true"},
                        {"key": "materialization_policy_row_materialized_temporary_operator_path_data_materialized", "value": "true"},
                        {"key": "materialization_policy_row_materialized_temporary_operator_path_stayed_encoded", "value": "false"},
                        {"key": "materialization_policy_row_materialized_temporary_operator_path_materialization_boundary_required", "value": "true"},
                        {"key": "materialization_policy_row_materialized_temporary_operator_path_materialization_boundary_emitted", "value": "true"},
                        {"key": "materialization_policy_row_materialized_temporary_operator_path_materialized_temporary_path", "value": "true"},
                        {"key": "materialization_policy_row_materialized_temporary_operator_path_encoded_native_claim_allowed", "value": "false"},
                        {"key": "materialization_policy_row_materialized_temporary_operator_path_materialization_decode_refs", "value": "materialization_boundary_report_required"},
                        {"key": "materialization_policy_row_materialized_temporary_operator_path_policy_refs", "value": "fallback_attempted=false,external_engine_invoked=false"},
                        {"key": "materialization_policy_row_materialized_temporary_operator_path_unsupported_diagnostic_code", "value": "none"},
                        {"key": "materialization_policy_row_materialized_temporary_operator_path_blocker_id", "value": "gar-flow-2b.materialized_temporary_operator_not_encoded_native"},
                        {"key": "materialization_policy_row_materialized_temporary_operator_path_required_future_evidence", "value": "encoded_native_operator_evidence_before_encoded_claim"},
                        {"key": "materialization_policy_row_materialized_temporary_operator_path_claim_gate_status", "value": "not_claim_grade"},
                        {"key": "materialization_policy_row_materialized_temporary_operator_path_claim_boundary", "value": "materialized temporary is not encoded native"},
                        {"key": "materialization_policy_row_materialized_temporary_operator_path_runtime_execution", "value": "false"},
                        {"key": "materialization_policy_row_materialized_temporary_operator_path_fallback_attempted", "value": "false"},
                        {"key": "materialization_policy_row_materialized_temporary_operator_path_external_engine_invoked", "value": "false"},
                        {"key": "materialization_policy_row_unsupported_operator_path_operator_execution_class", "value": "unsupported"},
                        {"key": "materialization_policy_row_unsupported_operator_path_support_status", "value": "unsupported"},
                        {"key": "materialization_policy_row_unsupported_operator_path_data_decoded", "value": "false"},
                        {"key": "materialization_policy_row_unsupported_operator_path_data_materialized", "value": "false"},
                        {"key": "materialization_policy_row_unsupported_operator_path_stayed_encoded", "value": "false"},
                        {"key": "materialization_policy_row_unsupported_operator_path_materialization_boundary_required", "value": "false"},
                        {"key": "materialization_policy_row_unsupported_operator_path_materialization_boundary_emitted", "value": "false"},
                        {"key": "materialization_policy_row_unsupported_operator_path_materialized_temporary_path", "value": "false"},
                        {"key": "materialization_policy_row_unsupported_operator_path_encoded_native_claim_allowed", "value": "false"},
                        {"key": "materialization_policy_row_unsupported_operator_path_materialization_decode_refs", "value": "unsupported_no_decode_or_materialization"},
                        {"key": "materialization_policy_row_unsupported_operator_path_policy_refs", "value": "fallback_attempted=false,external_engine_invoked=false"},
                        {"key": "materialization_policy_row_unsupported_operator_path_unsupported_diagnostic_code", "value": "SL_UNSUPPORTED_OPERATOR_MATERIALIZATION_POLICY"},
                        {"key": "materialization_policy_row_unsupported_operator_path_blocker_id", "value": "gar0003b.unsupported_operator_materialization_policy"},
                        {"key": "materialization_policy_row_unsupported_operator_path_required_future_evidence", "value": "operator_capability_row,deterministic_diagnostic"},
                        {"key": "materialization_policy_row_unsupported_operator_path_claim_gate_status", "value": "not_claim_grade"},
                        {"key": "materialization_policy_row_unsupported_operator_path_claim_boundary", "value": "unsupported does not execute"},
                        {"key": "materialization_policy_row_unsupported_operator_path_runtime_execution", "value": "false"},
                        {"key": "materialization_policy_row_unsupported_operator_path_fallback_attempted", "value": "false"},
                        {"key": "materialization_policy_row_unsupported_operator_path_external_engine_invoked", "value": "false"},
                        {"key": "compute_row_order", "value": "local_vortex_count,direct_compatibility_transient,sql_frontend"},
                        {"key": "compute_row_local_vortex_count_surface", "value": "vortex_file_scan_count"},
                        {"key": "compute_row_local_vortex_count_family", "value": "scan"},
                        {"key": "compute_row_local_vortex_count_support_status", "value": "fixture_certified"},
                        {"key": "compute_row_local_vortex_count_engine_mode", "value": "batch"},
                        {"key": "compute_row_local_vortex_count_execution_mode", "value": "native_vortex"},
                        {"key": "compute_row_local_vortex_count_provider_kind", "value": "vortex_scan"},
                        {"key": "compute_row_local_vortex_count_semantic_profile", "value": "ShardLoomNative"},
                        {"key": "compute_row_local_vortex_count_materialization_decode_requirement", "value": "metadata_or_scan_count_no_row_materialization"},
                        {"key": "compute_row_local_vortex_count_memory_spill_requirement", "value": "streaming_bounded_memory"},
                        {"key": "compute_row_local_vortex_count_correctness_refs", "value": "tests/local_vortex_count"},
                        {"key": "compute_row_local_vortex_count_benchmark_refs", "value": "benchmarks/local_vortex_count_smoke"},
                        {"key": "compute_row_local_vortex_count_execution_certificate_refs", "value": "certificates/cg16/local-vortex-count/execution.json"},
                        {"key": "compute_row_local_vortex_count_native_io_refs", "value": "certificates/cg19/local-vortex-count/native-io.json"},
                        {"key": "compute_row_local_vortex_count_unsupported_diagnostic_code", "value": "none"},
                        {"key": "compute_row_local_vortex_count_blocker_id", "value": "none"},
                        {"key": "compute_row_local_vortex_count_required_future_evidence", "value": "none"},
                        {"key": "compute_row_local_vortex_count_claim_gate_status", "value": "fixture_smoke_only"},
                        {"key": "compute_row_local_vortex_count_vortex_native_claim_allowed", "value": "true"},
                        {"key": "compute_row_local_vortex_count_fallback_attempted", "value": "false"},
                        {"key": "compute_row_local_vortex_count_external_engine_invoked", "value": "false"},
                        {"key": "compute_row_direct_compatibility_transient_surface", "value": "direct_compatibility_transient_query"},
                        {"key": "compute_row_direct_compatibility_transient_family", "value": "compatibility_transient"},
                        {"key": "compute_row_direct_compatibility_transient_support_status", "value": "unsupported"},
                        {"key": "compute_row_direct_compatibility_transient_engine_mode", "value": "batch"},
                        {"key": "compute_row_direct_compatibility_transient_execution_mode", "value": "direct_compatibility_transient"},
                        {"key": "compute_row_direct_compatibility_transient_provider_kind", "value": "shardloom_kernel"},
                        {"key": "compute_row_direct_compatibility_transient_semantic_profile", "value": "ShardLoomNative"},
                        {"key": "compute_row_direct_compatibility_transient_materialization_decode_requirement", "value": "direct_transient_executor_missing"},
                        {"key": "compute_row_direct_compatibility_transient_memory_spill_requirement", "value": "unsupported_until_transient_executor_exists"},
                        {"key": "compute_row_direct_compatibility_transient_correctness_refs", "value": "none"},
                        {"key": "compute_row_direct_compatibility_transient_benchmark_refs", "value": "none"},
                        {"key": "compute_row_direct_compatibility_transient_execution_certificate_refs", "value": "none"},
                        {"key": "compute_row_direct_compatibility_transient_native_io_refs", "value": "not_vortex_native"},
                        {"key": "compute_row_direct_compatibility_transient_unsupported_diagnostic_code", "value": "SL_UNSUPPORTED_DIRECT_COMPATIBILITY_TRANSIENT"},
                        {"key": "compute_row_direct_compatibility_transient_blocker_id", "value": "p75.direct_transient.executor_missing"},
                        {"key": "compute_row_direct_compatibility_transient_required_future_evidence", "value": "shardloom_native_transient_executor,direct_mode_certificate"},
                        {"key": "compute_row_direct_compatibility_transient_claim_gate_status", "value": "not_claim_grade"},
                        {"key": "compute_row_direct_compatibility_transient_vortex_native_claim_allowed", "value": "false"},
                        {"key": "compute_row_direct_compatibility_transient_fallback_attempted", "value": "false"},
                        {"key": "compute_row_direct_compatibility_transient_external_engine_invoked", "value": "false"},
                        {"key": "compute_row_sql_frontend_surface", "value": "sql_parse_bind_plan_execute"},
                        {"key": "compute_row_sql_frontend_family", "value": "sql"},
                        {"key": "compute_row_sql_frontend_support_status", "value": "unsupported"},
                        {"key": "compute_row_sql_frontend_engine_mode", "value": "batch"},
                        {"key": "compute_row_sql_frontend_execution_mode", "value": "auto"},
                        {"key": "compute_row_sql_frontend_provider_kind", "value": "shardloom_kernel"},
                        {"key": "compute_row_sql_frontend_semantic_profile", "value": "ShardLoomNative"},
                        {"key": "compute_row_sql_frontend_materialization_decode_requirement", "value": "unsupported_no_materialization"},
                        {"key": "compute_row_sql_frontend_memory_spill_requirement", "value": "unsupported"},
                        {"key": "compute_row_sql_frontend_correctness_refs", "value": "none"},
                        {"key": "compute_row_sql_frontend_benchmark_refs", "value": "none"},
                        {"key": "compute_row_sql_frontend_execution_certificate_refs", "value": "none"},
                        {"key": "compute_row_sql_frontend_native_io_refs", "value": "none"},
                        {"key": "compute_row_sql_frontend_unsupported_diagnostic_code", "value": "SL_UNSUPPORTED_SQL"},
                        {"key": "compute_row_sql_frontend_blocker_id", "value": "cg21.workflow.sql.frontend_unsupported"},
                        {"key": "compute_row_sql_frontend_required_future_evidence", "value": "parser,binder,planner,semantic_fixtures"},
                        {"key": "compute_row_sql_frontend_claim_gate_status", "value": "not_claim_grade"},
                        {"key": "compute_row_sql_frontend_vortex_native_claim_allowed", "value": "false"},
                        {"key": "compute_row_sql_frontend_fallback_attempted", "value": "false"},
                        {"key": "compute_row_sql_frontend_external_engine_invoked", "value": "false"},
                        {"key": "operator_family_order", "value": "predicates,joins"},
                        {"key": "operator_family_predicates_support_status", "value": "fixture_certified"},
                        {"key": "operator_family_predicates_next_evidence", "value": "semantic_fixture_expansion,benchmark_rows"},
                        {"key": "operator_family_joins_support_status", "value": "planned"},
                        {"key": "operator_family_joins_next_evidence", "value": "join_null_semantics,build_probe_memory,benchmarks"},
                        {"key": "all_rows_fallback_attempted_false", "value": "true"},
                        {"key": "no_runtime", "value": "true"},
                        {"key": "no_fallback", "value": "true"},
                        {"key": "no_effects", "value": "true"}
                    ],
                }))
                """
            )
        )

        result = ShardLoomClient(binary=binary).compute_capability_matrix()

        self.assertIsInstance(result, ComputeCapabilityMatrix)
        self.assertEqual(result.matrix_status, "report_only")
        self.assertEqual(result.claim_grade_status, "evidence_incomplete")
        self.assertEqual(
            result.native_unsupported_coverage_status,
            "complete_for_current_matrix",
        )
        self.assertTrue(result.native_unsupported_coverage_complete)
        self.assertEqual(
            result.native_vortex_admission_status,
            "scoped_fixture_lane_admitted",
        )
        admission_lanes = {lane.lane_id: lane for lane in result.native_vortex_admission_lanes}
        count_lane = admission_lanes["local_vortex_count_scalar"]
        self.assertEqual(count_lane.admission_status, "admitted_fixture_certified")
        self.assertEqual(count_lane.provider_kind, "vortex_scan")
        self.assertIn("ScanBuilder::into_array_iter", count_lane.provider_api_surface)
        self.assertEqual(
            count_lane.claim_boundary,
            "local_count_all_fixture_smoke_only_not_universal_native_vortex",
        )
        self.assertTrue(count_lane.vortex_native_claim_allowed)
        self.assertFalse(count_lane.fallback_attempted)
        self.assertFalse(count_lane.external_engine_invoked)
        self.assertFalse(count_lane.object_store_io)
        self.assertFalse(count_lane.write_io)
        rows = {row.row_id: row for row in result.rows}
        self.assertEqual(rows["local_vortex_count"].support_status, "fixture_certified")
        self.assertEqual(rows["local_vortex_count"].execution_mode, "native_vortex")
        self.assertEqual(rows["local_vortex_count"].provider_kind, "vortex_scan")
        self.assertEqual(rows["local_vortex_count"].claim_gate_status, "fixture_smoke_only")
        self.assertTrue(rows["local_vortex_count"].vortex_native_claim_allowed)
        self.assertEqual(
            rows["local_vortex_count"].native_io_refs,
            ("certificates/cg19/local-vortex-count/native-io.json",),
        )
        self.assertFalse(rows["local_vortex_count"].fallback_attempted)
        self.assertEqual(
            rows["direct_compatibility_transient"].execution_mode,
            "direct_compatibility_transient",
        )
        self.assertEqual(
            rows["direct_compatibility_transient"].unsupported_diagnostic_code,
            "SL_UNSUPPORTED_DIRECT_COMPATIBILITY_TRANSIENT",
        )
        self.assertFalse(rows["direct_compatibility_transient"].vortex_native_claim_allowed)
        self.assertEqual(rows["sql_frontend"].unsupported_diagnostic_code, "SL_UNSUPPORTED_SQL")
        self.assertIn("parser", rows["sql_frontend"].required_future_evidence)
        native_unsupported = {
            row.row_id: row for row in result.native_unsupported_coverage_rows
        }
        self.assertEqual(
            native_unsupported["native_source_object_store_range"].category,
            "source",
        )
        self.assertEqual(
            native_unsupported["native_operator_joins"].unsupported_diagnostic_code,
            "SL_UNSUPPORTED_NATIVE_JOIN",
        )
        self.assertEqual(
            native_unsupported["native_workload_sql_dataframe"].claim_gate_status,
            "not_claim_grade",
        )
        self.assertFalse(native_unsupported["native_workload_sql_dataframe"].fallback_attempted)
        self.assertFalse(
            native_unsupported["native_workload_sql_dataframe"].external_engine_invoked
        )
        self.assertTrue(result.predicate_dtype_coverage_complete)
        predicate_rows = {
            row.row_id: row for row in result.predicate_dtype_coverage_rows
        }
        range_row = predicate_rows["predicate_i64_range"]
        self.assertIsInstance(range_row, PredicateDtypeCoverageRow)
        self.assertEqual(range_row.support_status, "fixture_certified")
        self.assertIn("metadata_pruning", range_row.runtime_surface)
        self.assertIn("null_count", range_row.statistics_required)
        self.assertFalse(range_row.fallback_attempted)
        nested_row = predicate_rows["nested_field_pruning"]
        self.assertEqual(
            nested_row.unsupported_diagnostic_code,
            "SL_UNSUPPORTED_NESTED_FIELD_PRUNING",
        )
        self.assertEqual(nested_row.claim_gate_status, "not_claim_grade")
        self.assertFalse(nested_row.external_engine_invoked)
        self.assertEqual(
            result.materialization_policy_report_ref,
            "compute-capability-matrix://materialization_policy.v1",
        )
        self.assertTrue(result.materialization_policy_all_rows_classified)
        materialization_rows = {
            row.row_id: row for row in result.materialization_policy_rows
        }
        encoded_policy = materialization_rows["encoded_native_operator_path"]
        self.assertEqual(encoded_policy.operator_execution_class, "encoded_native")
        self.assertTrue(encoded_policy.stayed_encoded)
        self.assertFalse(encoded_policy.data_decoded)
        self.assertFalse(encoded_policy.data_materialized)
        self.assertTrue(encoded_policy.encoded_native_claim_allowed)
        temporary_policy = materialization_rows["materialized_temporary_operator_path"]
        self.assertTrue(temporary_policy.data_decoded)
        self.assertTrue(temporary_policy.data_materialized)
        self.assertTrue(temporary_policy.materialized_temporary_path)
        self.assertFalse(temporary_policy.encoded_native_claim_allowed)
        self.assertEqual(
            temporary_policy.blocker_id,
            "gar-flow-2b.materialized_temporary_operator_not_encoded_native",
        )
        unsupported_policy = materialization_rows["unsupported_operator_path"]
        self.assertEqual(
            unsupported_policy.unsupported_diagnostic_code,
            "SL_UNSUPPORTED_OPERATOR_MATERIALIZATION_POLICY",
        )
        self.assertFalse(unsupported_policy.runtime_execution)
        self.assertFalse(unsupported_policy.fallback_attempted)
        families = {row.family_id: row for row in result.operator_families}
        self.assertEqual(families["joins"].support_status, "planned")
        self.assertIn("build_probe_memory", families["joins"].next_evidence)
        self.assertTrue(result.no_runtime)
        self.assertTrue(result.no_fallback)
        self.assertTrue(result.no_effects)

    def test_semantic_conformance_suite_view(self) -> None:
        binary = self.fake_cli(
            textwrap.dedent(
                """
                import json, sys
                assert sys.argv[1:] == ["semantic-conformance-suite", "--format", "json"], sys.argv
                print(json.dumps({
                    "schema_version": "shardloom.output.v2",
                    "command": "semantic-conformance-suite",
                    "status": "success",
                    "summary": "ok",
                    "human_text": "ok",
                    "fallback": {"attempted": False, "allowed": False, "engine": None, "reason": "disabled"},
                    "diagnostics": [],
                    "fields": [
                        {"key": "semantic_profile", "value": "ShardLoomNative"},
                        {"key": "suite_status", "value": "partial_fixture_passed_planned_remaining"},
                        {"key": "executed_fixture_count", "value": "2"},
                        {"key": "passed_fixture_count", "value": "2"},
                        {"key": "failed_fixture_count", "value": "0"},
                        {"key": "row_order", "value": "null_comparison,join_null_semantics"},
                        {"key": "semantic_row_null_comparison_dimension", "value": "null comparison"},
                        {"key": "semantic_row_null_comparison_operator_family", "value": "predicates"},
                        {"key": "semantic_row_null_comparison_fixture_status", "value": "passed"},
                        {"key": "semantic_row_null_comparison_current_support", "value": "fixture_certified"},
                        {"key": "semantic_row_null_comparison_assertion", "value": "IS NULL preserves null identity"},
                        {"key": "semantic_row_null_comparison_blocker_id", "value": "none"},
                        {"key": "semantic_row_null_comparison_required_future_evidence", "value": "none"},
                        {"key": "semantic_row_null_comparison_fixture_executed", "value": "true"},
                        {"key": "semantic_row_null_comparison_passed", "value": "true"},
                        {"key": "semantic_row_null_comparison_fallback_attempted", "value": "false"},
                        {"key": "semantic_row_null_comparison_external_oracle_used", "value": "false"},
                        {"key": "semantic_row_join_null_semantics_dimension", "value": "join null semantics"},
                        {"key": "semantic_row_join_null_semantics_operator_family", "value": "joins"},
                        {"key": "semantic_row_join_null_semantics_fixture_status", "value": "blocked"},
                        {"key": "semantic_row_join_null_semantics_current_support", "value": "blocked_pending_operator"},
                        {"key": "semantic_row_join_null_semantics_assertion", "value": "operator family unsupported for semantic certification"},
                        {"key": "semantic_row_join_null_semantics_blocker_id", "value": "cg21.workflow.join.operator_unsupported"},
                        {"key": "semantic_row_join_null_semantics_required_future_evidence", "value": "join_operator,join_null_semantics_fixture"},
                        {"key": "semantic_row_join_null_semantics_fixture_executed", "value": "false"},
                        {"key": "semantic_row_join_null_semantics_passed", "value": "false"},
                        {"key": "semantic_row_join_null_semantics_fallback_attempted", "value": "false"},
                        {"key": "semantic_row_join_null_semantics_external_oracle_used", "value": "false"},
                        {"key": "no_runtime", "value": "true"},
                        {"key": "no_fallback", "value": "true"},
                        {"key": "no_effects", "value": "true"}
                    ],
                }))
                """
            )
        )

        result = ShardLoomClient(binary=binary).semantic_conformance_suite()

        self.assertIsInstance(result, SemanticConformanceSuite)
        self.assertEqual(result.semantic_profile, "ShardLoomNative")
        self.assertEqual(result.executed_fixture_count, 2)
        self.assertEqual(result.passed_fixture_count, 2)
        self.assertEqual(result.failed_fixture_count, 0)
        rows = {row.row_id: row for row in result.rows}
        self.assertEqual(rows["null_comparison"].fixture_status, "passed")
        self.assertTrue(rows["null_comparison"].fixture_executed)
        self.assertTrue(rows["null_comparison"].passed)
        self.assertFalse(rows["null_comparison"].fallback_attempted)
        self.assertEqual(
            rows["join_null_semantics"].blocker_id,
            "cg21.workflow.join.operator_unsupported",
        )
        self.assertIn("join_operator", rows["join_null_semantics"].required_future_evidence)
        self.assertFalse(rows["join_null_semantics"].external_oracle_used)
        self.assertTrue(result.no_runtime)
        self.assertTrue(result.no_fallback)
        self.assertTrue(result.no_effects)

    def test_rest_api_local_lifecycle_view(self) -> None:
        binary = self.fake_cli(
            textwrap.dedent(
                """
                import json, sys
                assert sys.argv[1:] == [
                    "rest-api-local-lifecycle", "certified-local-batch", "--format", "json"
                ], sys.argv
                print(json.dumps({
                    "schema_version": "shardloom.output.v2",
                    "command": "rest-api-local-lifecycle",
                    "status": "success",
                    "summary": "ok",
                    "human_text": "ok",
                    "fallback": {"attempted": False, "allowed": False, "engine": None, "reason": "disabled"},
                    "diagnostics": [],
                    "fields": [
                        {"key": "scenario", "value": "certified-local-batch"},
                        {"key": "lifecycle_status", "value": "succeeded"},
                        {"key": "query_id", "value": "query://cg23/certified-local-batch/0001"},
                        {"key": "result_ref", "value": "result://cg23/certified-local-batch/0001"},
                        {"key": "lifecycle_operations", "value": "execute,status,cancel,retry,profile,certificates,lineage,results,artifacts,cleanup"},
                        {"key": "result_policies", "value": "inline_json:decoded_rows,vortex_artifact:native_vortex_artifact,arrow_ipc_decoded_boundary:decoded_columnar_boundary"},
                        {"key": "inline_json_available", "value": "true"},
                        {"key": "vortex_artifact_available", "value": "true"},
                        {"key": "arrow_ipc_materialization", "value": "decoded_columnar_boundary"},
                        {"key": "arrow_ipc_certified_native", "value": "false"},
                        {"key": "result_ttl_seconds", "value": "3600"},
                        {"key": "cleanup_required", "value": "true"},
                        {"key": "non_certified_path_blocked", "value": "false"},
                        {"key": "cancellation_status", "value": "not_requested"},
                        {"key": "retry_status", "value": "not_requested"},
                        {"key": "query_execution", "value": "true"},
                        {"key": "runtime_execution", "value": "true"},
                        {"key": "local_execution_performed", "value": "true"},
                        {"key": "fallback_attempted", "value": "false"},
                        {"key": "execution_delegated", "value": "false"}
                    ],
                }))
                """
            )
        )

        result = ShardLoomClient(binary=binary).rest_api_local_lifecycle()

        self.assertIsInstance(result, RestApiLocalLifecycle)
        self.assertEqual(result.scenario, "certified-local-batch")
        self.assertEqual(result.lifecycle_status, "succeeded")
        self.assertEqual(result.query_id, "query://cg23/certified-local-batch/0001")
        self.assertEqual(result.result_ref, "result://cg23/certified-local-batch/0001")
        self.assertIn("cleanup", result.lifecycle_operations)
        self.assertIn("vortex_artifact:native_vortex_artifact", result.result_policies)
        self.assertTrue(result.inline_json_available)
        self.assertTrue(result.vortex_artifact_available)
        self.assertEqual(result.arrow_ipc_materialization, "decoded_columnar_boundary")
        self.assertFalse(result.arrow_ipc_certified_native)
        self.assertEqual(result.result_ttl_seconds, 3600)
        self.assertTrue(result.cleanup_required)
        self.assertFalse(result.non_certified_path_blocked)
        self.assertEqual(result.cancellation_status, "not_requested")
        self.assertEqual(result.retry_status, "not_requested")
        self.assertTrue(result.query_execution)
        self.assertTrue(result.runtime_execution)
        self.assertTrue(result.local_execution_performed)
        self.assertFalse(result.fallback_attempted)
        self.assertFalse(result.execution_delegated)

    def test_rest_api_event_stream_view(self) -> None:
        binary = self.fake_cli(
            textwrap.dedent(
                """
                import json, sys
                assert sys.argv[1:] == [
                    "rest-api-event-stream", "certified-live-fixture", "--format", "json"
                ], sys.argv
                print(json.dumps({
                    "schema_version": "shardloom.output.v2",
                    "command": "rest-api-event-stream",
                    "status": "success",
                    "summary": "ok",
                    "human_text": "ok",
                    "fallback": {"attempted": False, "allowed": False, "engine": None, "reason": "disabled"},
                    "diagnostics": [],
                    "fields": [
                        {"key": "scenario", "value": "certified-live-fixture"},
                        {"key": "event_stream_status", "value": "certified_fixture"},
                        {"key": "stream_id", "value": "event-stream://cg23/live-fixture/group-count"},
                        {"key": "stream_ref", "value": "event-stream://cg23/live-fixture/group-count"},
                        {"key": "engine_mode", "value": "live"},
                        {"key": "delivery_protocols", "value": "server_sent_events,websocket_optional"},
                        {"key": "event_types", "value": "progress,state,checkpoint,watermark,certificate,lineage,benchmark,hybrid_hot_cold_contribution"},
                        {"key": "certificate_ref_summary", "value": "certificates/cg22/live/fixture/freshness.json,certificates/cg22/live/fixture/group-count/execution.json"},
                        {"key": "asyncapi_contract_path", "value": "docs/api/shardloom-asyncapi-events-v1.yaml"},
                        {"key": "sse_first", "value": "true"},
                        {"key": "websocket_required", "value": "false"},
                        {"key": "event_count", "value": "7"},
                        {"key": "workload_certified", "value": "true"},
                        {"key": "production_claim_allowed", "value": "false"},
                        {"key": "broker_required", "value": "false"},
                        {"key": "broker_io", "value": "false"},
                        {"key": "object_store_io", "value": "false"},
                        {"key": "fallback_attempted", "value": "false"},
                        {"key": "execution_delegated", "value": "false"}
                    ],
                }))
                """
            )
        )

        result = ShardLoomClient(binary=binary).rest_api_event_stream()

        self.assertIsInstance(result, RestApiEventStream)
        self.assertEqual(result.scenario, "certified-live-fixture")
        self.assertEqual(result.event_stream_status, "certified_fixture")
        self.assertEqual(result.stream_id, "event-stream://cg23/live-fixture/group-count")
        self.assertEqual(result.stream_ref, "event-stream://cg23/live-fixture/group-count")
        self.assertEqual(result.engine_mode, "live")
        self.assertIn("server_sent_events", result.delivery_protocols)
        self.assertIn("watermark", result.event_types)
        self.assertIn("certificates/cg22/live/fixture/freshness.json", result.certificate_refs)
        self.assertEqual(
            result.asyncapi_contract_path,
            "docs/api/shardloom-asyncapi-events-v1.yaml",
        )
        self.assertTrue(result.sse_first)
        self.assertFalse(result.websocket_required)
        self.assertEqual(result.event_count, 7)
        self.assertTrue(result.workload_certified)
        self.assertFalse(result.production_claim_allowed)
        self.assertFalse(result.broker_required)
        self.assertFalse(result.broker_io)
        self.assertFalse(result.object_store_io)
        self.assertFalse(result.fallback_attempted)
        self.assertFalse(result.execution_delegated)

    def test_rest_api_security_governance_view(self) -> None:
        binary = self.fake_cli(
            textwrap.dedent(
                """
                import json, sys
                assert sys.argv[1:] == [
                    "rest-api-security-governance", "safe-local-default", "--format", "json"
                ], sys.argv
                print(json.dumps({
                    "schema_version": "shardloom.output.v2",
                    "command": "rest-api-security-governance",
                    "status": "success",
                    "summary": "ok",
                    "human_text": "ok",
                    "fallback": {"attempted": False, "allowed": False, "engine": None, "reason": "disabled"},
                    "diagnostics": [],
                    "fields": [
                        {"key": "scenario", "value": "safe-local-default"},
                        {"key": "governance_status", "value": "available_contract"},
                        {"key": "auth_postures", "value": "local_only:available_default,token:reference_only_contract"},
                        {"key": "api_scopes", "value": "read:allowed_local_metadata,write:policy_required,agent:dry_run_explain_estimate_certify_only"},
                        {"key": "mcp_tools", "value": "dry_run:allowed,explain:allowed,estimate:allowed,certify_preview:allowed,execute:blocked_policy_required"},
                        {"key": "evidence_model_signals", "value": "opentelemetry_traces,openlineage_facets,problem_details_errors,cloudevents,certificate_refs"},
                        {"key": "credential_references_only", "value": "true"},
                        {"key": "secrets_redacted", "value": "true"},
                        {"key": "raw_secret_values_present", "value": "false"},
                        {"key": "destructive_policy_required", "value": "true"},
                        {"key": "destructive_policy_present", "value": "false"},
                        {"key": "destructive_operations_allowed", "value": "false"},
                        {"key": "mcp_dry_run_default", "value": "true"},
                        {"key": "mcp_effectful_tools_allowed", "value": "false"},
                        {"key": "mcp_discovery_side_effect_free", "value": "true"},
                        {"key": "opentelemetry_exporter_enabled", "value": "false"},
                        {"key": "openlineage_facets_mapped", "value": "true"},
                        {"key": "problem_details_mapped", "value": "true"},
                        {"key": "cloudevents_mapped", "value": "true"},
                        {"key": "certificate_refs_mapped", "value": "true"},
                        {"key": "credential_resolution", "value": "false"},
                        {"key": "secret_resolution", "value": "false"},
                        {"key": "fallback_attempted", "value": "false"},
                        {"key": "execution_delegated", "value": "false"}
                    ],
                }))
                """
            )
        )

        result = ShardLoomClient(binary=binary).rest_api_security_governance()

        self.assertIsInstance(result, RestApiSecurityGovernance)
        self.assertEqual(result.scenario, "safe-local-default")
        self.assertEqual(result.governance_status, "available_contract")
        self.assertIn("token:reference_only_contract", result.auth_postures)
        self.assertIn("write:policy_required", result.api_scopes)
        self.assertIn("certify_preview:allowed", result.mcp_tools)
        self.assertIn("opentelemetry_traces", result.evidence_model_signals)
        self.assertTrue(result.credential_references_only)
        self.assertTrue(result.secrets_redacted)
        self.assertFalse(result.raw_secret_values_present)
        self.assertTrue(result.destructive_policy_required)
        self.assertFalse(result.destructive_policy_present)
        self.assertFalse(result.destructive_operations_allowed)
        self.assertTrue(result.mcp_dry_run_default)
        self.assertFalse(result.mcp_effectful_tools_allowed)
        self.assertTrue(result.mcp_discovery_side_effect_free)
        self.assertFalse(result.opentelemetry_exporter_enabled)
        self.assertTrue(result.openlineage_facets_mapped)
        self.assertTrue(result.problem_details_mapped)
        self.assertTrue(result.cloudevents_mapped)
        self.assertTrue(result.certificate_refs_mapped)
        self.assertFalse(result.credential_resolution)
        self.assertFalse(result.secret_resolution)
        self.assertFalse(result.fallback_attempted)
        self.assertFalse(result.execution_delegated)

    def test_rest_api_data_plane_view(self) -> None:
        binary = self.fake_cli(
            textwrap.dedent(
                """
                import json, sys
                assert sys.argv[1:] == [
                    "rest-api-data-plane", "standards-matrix", "--format", "json"
                ], sys.argv
                print(json.dumps({
                    "schema_version": "shardloom.output.v2",
                    "command": "rest-api-data-plane",
                    "status": "success",
                    "summary": "ok",
                    "human_text": "ok",
                    "fallback": {"attempted": False, "allowed": False, "engine": None, "reason": "disabled"},
                    "diagnostics": [],
                    "fields": [
                        {"key": "scenario", "value": "standards-matrix"},
                        {"key": "data_plane_status", "value": "standards_matrix_available"},
                        {"key": "transfer_modes", "value": "vortex_artifact:native_vortex_artifact,arrow_ipc_decoded_boundary:decoded_columnar_boundary,flight_ticket_future:decoded_columnar_boundary"},
                        {"key": "standards_names", "value": "iceberg_rest_catalog,polaris,gravitino,delta_sharing,substrait,wasi_webassembly_components,nats_jetstream,redpanda,kafka_compatible,paimon,fluss"},
                        {"key": "preferred_large_payload_modes", "value": "vortex_artifact,object_reference,paged_json"},
                        {"key": "large_payload_threshold_bytes", "value": "1048576"},
                        {"key": "rest_control_plane_sufficient_for_local_use", "value": "true"},
                        {"key": "flight_adbc_required_for_basic_local_use", "value": "false"},
                        {"key": "flight_ticket_requested", "value": "false"},
                        {"key": "flight_ticket_supported", "value": "false"},
                        {"key": "adbc_endpoint_requested", "value": "false"},
                        {"key": "adbc_endpoint_supported", "value": "false"},
                        {"key": "decoded_columnar_boundary_declared", "value": "true"},
                        {"key": "materialization_declared", "value": "true"},
                        {"key": "result_policy_declared", "value": "true"},
                        {"key": "standards_matrix_count", "value": "11"},
                        {"key": "flight_server_started", "value": "false"},
                        {"key": "adbc_endpoint_opened", "value": "false"},
                        {"key": "broker_io", "value": "false"},
                        {"key": "object_store_io", "value": "false"},
                        {"key": "catalog_probe", "value": "false"},
                        {"key": "fallback_attempted", "value": "false"},
                        {"key": "execution_delegated", "value": "false"}
                    ],
                }))
                """
            )
        )

        result = ShardLoomClient(binary=binary).rest_api_data_plane(
            "standards-matrix"
        )

        self.assertIsInstance(result, RestApiDataPlane)
        self.assertEqual(result.scenario, "standards-matrix")
        self.assertEqual(result.data_plane_status, "standards_matrix_available")
        self.assertIn("vortex_artifact:native_vortex_artifact", result.transfer_modes)
        self.assertIn("iceberg_rest_catalog", result.standards_names)
        self.assertIn("wasi_webassembly_components", result.standards_names)
        self.assertIn("vortex_artifact", result.preferred_large_payload_modes)
        self.assertEqual(result.large_payload_threshold_bytes, 1048576)
        self.assertTrue(result.rest_control_plane_sufficient_for_local_use)
        self.assertFalse(result.flight_adbc_required_for_basic_local_use)
        self.assertFalse(result.flight_ticket_requested)
        self.assertFalse(result.flight_ticket_supported)
        self.assertFalse(result.adbc_endpoint_requested)
        self.assertFalse(result.adbc_endpoint_supported)
        self.assertTrue(result.decoded_columnar_boundary_declared)
        self.assertTrue(result.materialization_declared)
        self.assertTrue(result.result_policy_declared)
        self.assertEqual(result.standards_matrix_count, 11)
        self.assertFalse(result.flight_server_started)
        self.assertFalse(result.adbc_endpoint_opened)
        self.assertFalse(result.broker_io)
        self.assertFalse(result.object_store_io)
        self.assertFalse(result.catalog_probe)
        self.assertFalse(result.fallback_attempted)
        self.assertFalse(result.execution_delegated)

    def test_env_binary_is_resolved_from_client_environment(self) -> None:
        client = ShardLoomClient(env={"SHARDLOOM_BIN": sys.executable, "PATH": ""})

        command = client._command(["status"])

        self.assertEqual(command[0], sys.executable)

    def test_subprocess_env_merges_overrides_with_inherited_environment(self) -> None:
        binary = self.fake_cli(
            textwrap.dedent(
                """
                import json, os
                assert os.environ.get("SHARDLOOM_TEST_INHERITED") == "base"
                assert os.environ.get("SHARDLOOM_TEST_OVERRIDE") == "override"
                print(json.dumps({
                    "schema_version": "shardloom.output.v2",
                    "command": "status",
                    "status": "success",
                    "summary": "ok",
                    "human_text": "ok",
                    "fallback": {"attempted": False, "allowed": False, "engine": None, "reason": "disabled"},
                    "diagnostics": [],
                    "fields": [],
                }))
                """
            )
        )
        old = os.environ.get("SHARDLOOM_TEST_INHERITED")
        os.environ["SHARDLOOM_TEST_INHERITED"] = "base"
        try:
            result = ShardLoomClient(
                binary=binary,
                env={"SHARDLOOM_TEST_OVERRIDE": "override"},
            ).status()
        finally:
            if old is None:
                os.environ.pop("SHARDLOOM_TEST_INHERITED", None)
            else:
                os.environ["SHARDLOOM_TEST_INHERITED"] = old

        self.assertEqual(result.command, "status")

    def test_relative_env_binary_resolves_from_client_cwd(self) -> None:
        tempdir = tempfile.TemporaryDirectory()
        self.addCleanup(tempdir.cleanup)
        root = Path(tempdir.name)
        bin_dir = root / "bin"
        bin_dir.mkdir()
        binary = bin_dir / "shardloom"
        binary.write_text("", encoding="utf-8")

        client = ShardLoomClient(
            env={"SHARDLOOM_BIN": str(Path("bin") / "shardloom"), "PATH": ""},
            cwd=root,
        )
        command = client._command(["status"])

        self.assertEqual(Path(command[0]), binary)

    def test_missing_binary_raises_deterministic_error(self) -> None:
        client = ShardLoomClient(env={"PATH": ""})

        with self.assertRaises(ShardLoomBinaryNotFoundError) as raised:
            client.status()

        error = raised.exception
        message = str(error)
        self.assertIn("ShardLoom CLI binary could not be resolved", message)
        self.assertIn("SHARDLOOM_BIN", message)
        self.assertFalse(error.fallback.attempted)
        self.assertFalse(error.fallback.allowed)
        self.assertEqual(error.diagnostics[0].code, "SL_BINARY_NOT_FOUND")
        self.assertEqual(error.diagnostics[0].category, "configuration")
        self.assertEqual(error.diagnostics[0].feature, "python_cli_binary_resolution")
        self.assertFalse(error.diagnostics[0].fallback.attempted)

        payload = error.to_error_payload("status")
        self.assertEqual(payload["schema_version"], "shardloom.output.v2")
        self.assertEqual(payload["command"], "status")
        self.assertEqual(payload["status"], "error")
        self.assertEqual(payload["fallback"]["attempted"], False)
        self.assertEqual(payload["fallback"]["allowed"], False)
        self.assertEqual(payload["diagnostics"][0]["code"], "SL_BINARY_NOT_FOUND")
        self.assertEqual(payload["result"], {"fields": []})
        self.assertEqual(payload["artifacts"], [])
        self.assertEqual(payload["certificates"], [])
        self.assertIn(
            {"key": "command_family", "value": "python_binary_resolution"},
            payload["lifecycle"]["fields"],
        )
        self.assertIn(
            {"key": "fallback_execution_allowed", "value": "false"},
            payload["policy"]["fields"],
        )
        self.assertEqual(OutputEnvelope.from_json(payload).command, "status")
        json.dumps(payload, sort_keys=True)

    def test_invalid_env_binary_raises_deterministic_error(self) -> None:
        tempdir = tempfile.TemporaryDirectory()
        self.addCleanup(tempdir.cleanup)
        missing = Path(tempdir.name) / "missing-shardloom"
        client = ShardLoomClient.from_env(
            {"SHARDLOOM_BIN": str(missing), "PATH": ""}
        )

        with self.assertRaises(ShardLoomBinaryNotFoundError) as raised:
            client.status()

        error = raised.exception
        self.assertIn("SHARDLOOM_BIN points to", str(error))
        self.assertIn("missing-shardloom", error.diagnostics[0].reason or "")
        self.assertFalse(error.to_error_payload("status")["fallback"]["attempted"])

    def test_from_repo_resolves_target_binary_lazily(self) -> None:
        tempdir = tempfile.TemporaryDirectory()
        self.addCleanup(tempdir.cleanup)
        root = Path(tempdir.name)
        target = root / "target" / "debug"
        target.mkdir(parents=True)
        (target / "shardloom").write_text("", encoding="utf-8")
        (target / "shardloom.exe").write_text("", encoding="utf-8")

        client = ShardLoomClient.from_repo(root, profile_order=("debug",))
        command = client._command(["status"])

        self.assertTrue(command[0].endswith(("shardloom", "shardloom.exe")))
        self.assertIn(str(target), command[0])

    def test_traditional_analytics_vortex_run_passes_explicit_inputs(self) -> None:
        binary = self.fake_cli(
            textwrap.dedent(
                """
                import json, sys
                assert sys.argv[1:] == [
                    "traditional-analytics-vortex-run",
                    "selective filter",
                    "fact.vortex",
                    "dim.vortex",
                    "--format",
                    "json",
                ], sys.argv
                print(json.dumps({
                    "schema_version": "shardloom.output.v2",
                    "command": "traditional-analytics-vortex-run",
                    "status": "success",
                    "summary": "ok",
                    "human_text": "ok",
                    "fallback": {"attempted": False, "allowed": False, "engine": None, "reason": "disabled"},
                    "diagnostics": [],
                    "fields": [{"key": "rows_scanned", "value": "42"}],
                }))
                """
            )
        )

        result = ShardLoomClient(binary=binary).traditional_analytics_vortex_run(
            "selective filter", "fact.vortex", "dim.vortex"
        )

        self.assertEqual(result.field_int("rows_scanned"), 42)

    def test_traditional_analytics_vortex_run_can_request_result_sink(self) -> None:
        binary = self.fake_cli(
            textwrap.dedent(
                """
                import json, sys
                assert sys.argv[1:] == [
                    "traditional-analytics-vortex-run",
                    "selective filter",
                    "fact.vortex",
                    "dim.vortex",
                    "--workspace",
                    "sink-work",
                    "--write-result-vortex",
                    "--execution-mode",
                    "prepared_vortex",
                    "--format",
                    "json",
                ], sys.argv
                print(json.dumps({
                    "schema_version": "shardloom.output.v2",
                    "command": "traditional-analytics-vortex-run",
                    "status": "success",
                    "summary": "ok",
                    "human_text": "ok",
                    "fallback": {"attempted": False, "allowed": False, "engine": None, "reason": "disabled"},
                    "diagnostics": [],
                    "fields": [
                        {"key": "computed_result_sink_requested", "value": "true"},
                        {"key": "computed_result_sink_replay_verified", "value": "true"},
                        {"key": "computed_result_sink_native_io_certificate_status", "value": "certified"}
                    ],
                }))
                """
            )
        )

        result = ShardLoomClient(binary=binary).traditional_analytics_vortex_run(
            "selective filter",
            "fact.vortex",
            "dim.vortex",
            workspace="sink-work",
            write_result_vortex=True,
            execution_mode="prepared_vortex",
        )

        self.assertEqual(result.field("computed_result_sink_requested"), "true")
        self.assertEqual(
            result.field("computed_result_sink_native_io_certificate_status"),
            "certified",
        )

    def test_traditional_analytics_vortex_run_can_pass_cdc_delta_vortex(self) -> None:
        binary = self.fake_cli(
            textwrap.dedent(
                """
                import json, sys
                assert sys.argv[1:] == [
                    "traditional-analytics-vortex-run",
                    "small change over large base",
                    "fact.vortex",
                    "dim.vortex",
                    "--cdc-delta-vortex",
                    "cdc_delta.vortex",
                    "--execution-mode",
                    "prepared_vortex",
                    "--format",
                    "json",
                ], sys.argv
                print(json.dumps({
                    "schema_version": "shardloom.output.v2",
                    "command": "traditional-analytics-vortex-run",
                    "status": "success",
                    "summary": "ok",
                    "human_text": "ok",
                    "fallback": {"attempted": False, "allowed": False, "engine": None, "reason": "disabled"},
                    "diagnostics": [],
                    "fields": [
                        {"key": "cdc_delta_vortex_path", "value": "cdc_delta.vortex"},
                        {"key": "streaming_projected_columns", "value": "base.id,base.metric,cdc_delta.id,cdc_delta.op,cdc_delta.value,cdc_delta.metric,cdc_delta.effective_ts"}
                    ],
                }))
                """
            )
        )

        result = ShardLoomClient(binary=binary).traditional_analytics_vortex_run(
            "small change over large base",
            "fact.vortex",
            "dim.vortex",
            cdc_delta_vortex="cdc_delta.vortex",
            execution_mode="prepared_vortex",
        )

        self.assertEqual(result.field("cdc_delta_vortex_path"), "cdc_delta.vortex")

    def test_traditional_analytics_methods_can_request_auto_mode(self) -> None:
        binary = self.fake_cli(
            textwrap.dedent(
                """
                import json, sys
                args = sys.argv[1:]
                if args == [
                    "traditional-analytics-run",
                    "selective filter",
                    "fact.csv",
                    "dim.csv",
                    "--workspace",
                    "work",
                    "--input-format",
                    "csv",
                    "--execution-mode",
                    "auto",
                    "--format",
                    "json",
                ]:
                    command = "traditional-analytics-run"
                elif args == [
                    "traditional-analytics-vortex-run",
                    "selective filter",
                    "fact.vortex",
                    "dim.vortex",
                    "--execution-mode",
                    "auto",
                    "--format",
                    "json",
                ]:
                    command = "traditional-analytics-vortex-run"
                else:
                    raise AssertionError(args)
                print(json.dumps({
                    "schema_version": "shardloom.output.v2",
                    "command": command,
                    "status": "success",
                    "summary": "ok",
                    "human_text": "ok",
                    "fallback": {"attempted": False, "allowed": False, "engine": None, "reason": "disabled"},
                    "diagnostics": [],
                    "fields": [
                        {"key": "requested_execution_mode", "value": "auto"},
                        {"key": "selected_execution_mode", "value": "native_vortex"},
                        {"key": "mode_selection_reason", "value": "auto selected explicit test path"}
                    ],
                }))
                """
            )
        )
        client = ShardLoomClient(binary=binary)

        compatibility = client.traditional_analytics_run(
            "selective filter",
            "fact.csv",
            "dim.csv",
            workspace="work",
            input_format="csv",
            execution_mode="auto",
        )
        native = client.traditional_analytics_vortex_run(
            "selective filter",
            "fact.vortex",
            "dim.vortex",
            execution_mode="auto",
        )

        self.assertEqual(
            ExecutionResultEnvelopeView(compatibility).requested_execution_mode,
            "auto",
        )
        self.assertEqual(
            ExecutionResultEnvelopeView(native).requested_execution_mode,
            "auto",
        )

    def test_prepare_traditional_analytics_vortex_artifacts_reports_lifecycle(self) -> None:
        binary = self.fake_cli(
            textwrap.dedent(
                """
                import json, sys
                assert sys.argv[1:] == [
                    "traditional-analytics-run",
                    "csv/file ingest",
                    "fact.csv",
                    "dim.csv",
                    "--workspace",
                    "work",
                    "--input-format",
                    "csv",
                    "--execution-mode",
                    "compatibility_import_certified",
                    "--format",
                    "json",
                ], sys.argv
                print(json.dumps({
                    "schema_version": "shardloom.output.v2",
                    "command": "traditional-analytics-run",
                    "status": "success",
                    "summary": "ok",
                    "human_text": "ok",
                    "fallback": {"attempted": False, "allowed": False, "engine": None, "reason": "disabled"},
                    "diagnostics": [],
                    "fields": [
                        {"key": "prepared_artifact_ref", "value": "fact=fact.vortex,dim=dim.vortex"},
                        {"key": "prepared_artifact_fact_ref", "value": "fact.vortex"},
                        {"key": "prepared_artifact_dim_ref", "value": "dim.vortex"},
                        {"key": "prepared_artifact_digest", "value": "fact=sha256:f,dim=sha256:d"},
                        {"key": "prepared_artifact_cleanup_policy", "value": "caller_owned_workspace_cleanup"},
                        {"key": "prepared_artifact_reuse_eligible", "value": "true"}
                    ],
                }))
                """
            )
        )

        artifacts = ShardLoomClient(binary=binary).prepare_traditional_analytics_vortex_artifacts(
            "fact.csv",
            "dim.csv",
            workspace="work",
            input_format="csv",
        )

        self.assertIsInstance(artifacts, PreparedVortexArtifacts)
        self.assertEqual(artifacts.fact_vortex_path, "fact.vortex")
        self.assertEqual(artifacts.dim_vortex_path, "dim.vortex")
        self.assertEqual(artifacts.artifact_digest, "fact=sha256:f,dim=sha256:d")
        self.assertEqual(artifacts.cleanup_policy, "caller_owned_workspace_cleanup")
        self.assertTrue(artifacts.reuse_eligible)

    def test_live_etl_smoke_dispatches_csv_and_vortex_modes(self) -> None:
        csv_binary = self.fake_cli(
            textwrap.dedent(
                """
                import json, sys
                assert sys.argv[1:] == [
                    "traditional-analytics-run",
                    "csv/file ingest",
                    "fact.csv",
                    "dim.csv",
                    "--workspace",
                    "work",
                    "--input-format",
                    "csv",
                    "--format",
                    "json",
                ], sys.argv
                print(json.dumps({
                    "schema_version": "shardloom.output.v2",
                    "command": "traditional-analytics-run",
                    "status": "success",
                    "summary": "ok",
                    "human_text": "ok",
                    "fallback": {"attempted": False, "allowed": False, "engine": None, "reason": "disabled"},
                    "diagnostics": [],
                    "fields": [{"key": "input_format", "value": "csv"}],
                }))
                """
            )
        )
        csv_result = ShardLoomClient(binary=csv_binary).live_etl_smoke(
            "csv/file ingest",
            "fact.csv",
            "dim.csv",
            input_format="csv",
            workspace="work",
        )
        self.assertEqual(csv_result.command, "traditional-analytics-run")

        vortex_binary = self.fake_cli(
            textwrap.dedent(
                """
                import json, sys
                assert sys.argv[1:] == [
                    "traditional-analytics-vortex-run",
                    "wide projection",
                    "fact.vortex",
                    "dim.vortex",
                    "--execution-mode",
                    "native_vortex",
                    "--format",
                    "json",
                ], sys.argv
                print(json.dumps({
                    "schema_version": "shardloom.output.v2",
                    "command": "traditional-analytics-vortex-run",
                    "status": "success",
                    "summary": "ok",
                    "human_text": "ok",
                    "fallback": {"attempted": False, "allowed": False, "engine": None, "reason": "disabled"},
                    "diagnostics": [],
                    "fields": [{"key": "input_format", "value": "vortex"}],
                }))
                """
            )
        )
        vortex_result = ShardLoomClient(binary=vortex_binary).live_etl_smoke(
            "wide projection",
            "fact.vortex",
            "dim.vortex",
            input_format="vortex",
        )
        self.assertEqual(vortex_result.command, "traditional-analytics-vortex-run")

    def test_live_etl_smoke_accepts_common_compatibility_formats(self) -> None:
        binary = self.fake_cli(
            textwrap.dedent(
                """
                import json, sys
                assert sys.argv[1:] == [
                    "traditional-analytics-run",
                    "hash join",
                    "fact.parquet",
                    "dim.parquet",
                    "--workspace",
                    "work",
                    "--input-format",
                    "parquet",
                    "--compat-output-format",
                    "orc",
                    "--verify-native-replay",
                    "--write-result-vortex",
                    "--memory-gb",
                    "8",
                    "--max-parallelism",
                    "4",
                    "--format",
                    "json",
                ], sys.argv
                print(json.dumps({
                    "schema_version": "shardloom.output.v2",
                    "command": "traditional-analytics-run",
                    "status": "success",
                    "summary": "ok",
                    "human_text": "ok",
                    "fallback": {"attempted": False, "allowed": False, "engine": None, "reason": "disabled"},
                    "diagnostics": [],
                    "fields": [{"key": "source_format", "value": "parquet"}],
                }))
                """
            )
        )

        result = ShardLoomClient(binary=binary).live_etl_smoke(
            "hash join",
            "fact.parquet",
            "dim.parquet",
            input_format="parquet",
            workspace="work",
            compatibility_output_format="orc",
            verify_native_replay=True,
            write_result_vortex=True,
            memory_gb=8,
            max_parallelism=4,
        )

        self.assertEqual(result.field("source_format"), "parquet")

    def test_live_etl_smoke_rejects_unknown_format(self) -> None:
        with self.assertRaises(ValueError):
            ShardLoomClient(binary=["shardloom"]).live_etl_smoke(
                "csv/file ingest", "fact.unknown", "dim.unknown", input_format="unknown"
            )

    def test_live_etl_smoke_rejects_native_replay_for_existing_vortex_inputs(self) -> None:
        with self.assertRaises(ValueError):
            ShardLoomClient(binary=["shardloom"]).live_etl_smoke(
                "wide projection",
                "fact.vortex",
                "dim.vortex",
                input_format="vortex",
                verify_native_replay=True,
            )

    def test_live_etl_smoke_rejects_result_sink_for_existing_vortex_inputs(self) -> None:
        with self.assertRaises(ValueError):
            ShardLoomClient(binary=["shardloom"]).live_etl_smoke(
                "wide projection",
                "fact.vortex",
                "dim.vortex",
                input_format="vortex",
                write_result_vortex=True,
            )

    def test_live_etl_smoke_dispatches_native_result_sink_with_workspace(self) -> None:
        binary = self.fake_cli(
            textwrap.dedent(
                """
                import json, sys
                assert sys.argv[1:] == [
                    "traditional-analytics-vortex-run",
                    "wide projection",
                    "fact.vortex",
                    "dim.vortex",
                    "--workspace",
                    "sink-work",
                    "--write-result-vortex",
                    "--execution-mode",
                    "native_vortex",
                    "--format",
                    "json",
                ], sys.argv
                print(json.dumps({
                    "schema_version": "shardloom.output.v2",
                    "command": "traditional-analytics-vortex-run",
                    "status": "success",
                    "summary": "ok",
                    "human_text": "ok",
                    "fallback": {"attempted": False, "allowed": False, "engine": None, "reason": "disabled"},
                    "diagnostics": [],
                    "fields": [
                        {"key": "computed_result_sink_requested", "value": "true"},
                        {"key": "computed_result_sink_replay_verified", "value": "true"}
                    ],
                }))
                """
            )
        )

        result = ShardLoomClient(binary=binary).live_etl_smoke(
            "wide projection",
            "fact.vortex",
            "dim.vortex",
            input_format="vortex",
            workspace="sink-work",
            write_result_vortex=True,
        )

        self.assertEqual(result.command, "traditional-analytics-vortex-run")
        self.assertEqual(result.field("computed_result_sink_requested"), "true")

    def test_live_etl_csv_to_vortex_replay_runs_import_then_native_replay(self) -> None:
        binary = self.fake_cli(
            textwrap.dedent(
                """
                import json, sys
                args = sys.argv[1:]
                if args == [
                    "traditional-analytics-run",
                    "selective filter",
                    "fact.csv",
                    "dim.csv",
                    "--workspace",
                    "work",
                    "--input-format",
                    "csv",
                    "--format",
                    "json",
                ]:
                    print(json.dumps({
                        "schema_version": "shardloom.output.v2",
                        "command": "traditional-analytics-run",
                        "status": "success",
                        "summary": "csv ok",
                        "human_text": "csv ok",
                        "fallback": {"attempted": False, "allowed": False, "engine": None, "reason": "disabled"},
                        "diagnostics": [],
                        "fields": [
                            {"key": "fact_vortex_path", "value": "work/fact.vortex"},
                            {"key": "dim_vortex_path", "value": "work/dim.vortex"},
                        ],
                    }))
                elif args == [
                    "traditional-analytics-vortex-run",
                    "selective filter",
                    "work/fact.vortex",
                    "work/dim.vortex",
                    "--execution-mode",
                    "native_vortex",
                    "--format",
                    "json",
                ]:
                    print(json.dumps({
                        "schema_version": "shardloom.output.v2",
                        "command": "traditional-analytics-vortex-run",
                        "status": "success",
                        "summary": "native ok",
                        "human_text": "native ok",
                        "fallback": {"attempted": False, "allowed": False, "engine": None, "reason": "disabled"},
                        "diagnostics": [],
                        "fields": [{"key": "source_format", "value": "vortex"}],
                    }))
                else:
                    raise AssertionError(args)
                """
            )
        )

        result = ShardLoomClient(binary=binary).live_etl_csv_to_vortex_replay(
            "selective filter",
            "fact.csv",
            "dim.csv",
            workspace="work",
        )

        self.assertEqual(result.csv_import.command, "traditional-analytics-run")
        self.assertEqual(
            result.native_vortex.command if result.native_vortex else None,
            "traditional-analytics-vortex-run",
        )
        self.assertEqual(result.fact_vortex_path, "work/fact.vortex")
        self.assertEqual(result.dim_vortex_path, "work/dim.vortex")
        self.assertTrue(result.native_replay_ran)
        self.assertFalse(result.fallback_attempted)

    def test_live_etl_csv_to_vortex_replay_can_skip_native_replay(self) -> None:
        binary = self.fake_cli(
            textwrap.dedent(
                """
                import json, sys
                assert sys.argv[1:] == [
                    "traditional-analytics-run",
                    "selective filter",
                    "fact.csv",
                    "dim.csv",
                    "--workspace",
                    "work",
                    "--input-format",
                    "csv",
                    "--format",
                    "json",
                ], sys.argv
                print(json.dumps({
                    "schema_version": "shardloom.output.v2",
                    "command": "traditional-analytics-run",
                    "status": "success",
                    "summary": "csv ok",
                    "human_text": "csv ok",
                    "fallback": {"attempted": False, "allowed": False, "engine": None, "reason": "disabled"},
                    "diagnostics": [],
                    "fields": [
                        {"key": "fact_vortex_path", "value": "work/fact.vortex"},
                        {"key": "dim_vortex_path", "value": "work/dim.vortex"},
                    ],
                }))
                """
            )
        )

        result = ShardLoomClient(binary=binary).live_etl_csv_to_vortex_replay(
            "selective filter",
            "fact.csv",
            "dim.csv",
            workspace="work",
            replay_native=False,
        )

        self.assertIsNone(result.native_vortex)
        self.assertFalse(result.native_replay_ran)

    def test_live_etl_csv_to_vortex_replay_requires_emitted_vortex_paths(self) -> None:
        binary = self.fake_cli(
            textwrap.dedent(
                """
                import json
                print(json.dumps({
                    "schema_version": "shardloom.output.v2",
                    "command": "traditional-analytics-run",
                    "status": "success",
                    "summary": "csv ok",
                    "human_text": "csv ok",
                    "fallback": {"attempted": False, "allowed": False, "engine": None, "reason": "disabled"},
                    "diagnostics": [],
                    "fields": [],
                }))
                """
            )
        )

        with self.assertRaises(ShardLoomProtocolError):
            ShardLoomClient(binary=binary).live_etl_csv_to_vortex_replay(
                "selective filter",
                "fact.csv",
                "dim.csv",
                workspace="work",
            )

    def test_dynamic_work_shaping_and_sizing_feedback_commands(self) -> None:
        binary = self.fake_cli(
            textwrap.dedent(
                """
                import json, sys
                assert sys.argv[1:] == [
                    "dynamic-work-shaping-plan",
                    "memory-pressure",
                    "--format",
                    "json",
                ], sys.argv
                print(json.dumps({
                    "schema_version": "shardloom.output.v2",
                    "command": "dynamic-work-shaping-plan",
                    "status": "success",
                    "summary": "ok",
                    "human_text": "ok",
                    "fallback": {"attempted": False, "allowed": False, "engine": None, "reason": "disabled"},
                    "diagnostics": [],
                    "fields": [{"key": "profile", "value": "memory-pressure"}],
                }))
                """
            )
        )

        result = ShardLoomClient(binary=binary).dynamic_work_shaping_plan(
            "memory-pressure"
        )

        self.assertEqual(result.field("profile"), "memory-pressure")

        feedback_binary = self.fake_cli(
            textwrap.dedent(
                """
                import json, sys
                assert sys.argv[1:] == [
                    "sizing-feedback-plan",
                    "8",
                    "task-too-large,memory-pressure-high",
                    "--format",
                    "json",
                ], sys.argv
                print(json.dumps({
                    "schema_version": "shardloom.output.v2",
                    "command": "sizing-feedback-plan",
                    "status": "success",
                    "summary": "ok",
                    "human_text": "ok",
                    "fallback": {"attempted": False, "allowed": False, "engine": None, "reason": "disabled"},
                    "diagnostics": [],
                    "fields": [{"key": "target_task_bytes_changed", "value": "true"}],
                }))
                """
            )
        )
        feedback = ShardLoomClient(binary=feedback_binary).sizing_feedback_plan(
            8, ["task-too-large", "memory-pressure-high"]
        )
        self.assertTrue(feedback.field_bool("target_task_bytes_changed"))

    def test_benchmark_and_world_class_plan_helpers(self) -> None:
        binary = self.fake_cli(
            textwrap.dedent(
                """
                import json, sys
                assert sys.argv[1:] == [
                    "benchmark-claim-evidence-plan",
                    "traditional-analytics",
                    "--format",
                    "json",
                ], sys.argv
                print(json.dumps({
                    "schema_version": "shardloom.output.v2",
                    "command": "benchmark-claim-evidence-plan",
                    "status": "success",
                    "summary": "ok",
                    "human_text": "ok",
                    "fallback": {"attempted": False, "allowed": False, "engine": None, "reason": "disabled"},
                    "diagnostics": [],
                    "fields": [{"key": "scope", "value": "traditional-analytics"}],
                }))
                """
            )
        )

        result = ShardLoomClient(binary=binary).benchmark_claim_evidence_plan(
            "traditional-analytics"
        )

        self.assertEqual(result.field("scope"), "traditional-analytics")

    def test_workflow_readiness_smoke_dispatches_no_write_planning_bundle(self) -> None:
        binary = self.fake_cli(
            textwrap.dedent(
                """
                import json, sys

                target = "file://tmp/out.vortex"
                compat = "file://tmp/out.parquet"
                workspace = "target/stage"
                args = sys.argv[1:]

                def emit(command, fields, diagnostics=None):
                    print(json.dumps({
                        "schema_version": "shardloom.output.v2",
                        "command": command,
                        "status": "success",
                        "summary": "ok",
                        "human_text": "ok",
                        "fallback": {"attempted": False, "allowed": False, "engine": None, "reason": "disabled"},
                        "diagnostics": diagnostics or [],
                        "fields": [
                            {"key": "fallback_execution_allowed", "value": "false"},
                            {"key": "execution", "value": "not_performed"},
                            *fields,
                        ],
                    }))

                if args == ["vortex-output-plan", target, "--format", "json"]:
                    emit("vortex-output-plan", [{"key": "target_format", "value": "vortex"}])
                elif args == ["translation-plan", compat, "--format", "json"]:
                    emit("translation-plan", [])
                elif args == ["plan-export", "native", "--format", "json"]:
                    emit("plan-export", [
                        {"key": "plan_only", "value": "true"},
                        {"key": "write_io", "value": "false"},
                        {"key": "read_io", "value": "false"},
                        {"key": "runtime_execution", "value": "false"},
                        {"key": "external_engine_execution", "value": "false"},
                    ])
                elif args[:2] == ["vortex-write-intent-plan", target] and args[-2:] == ["--format", "json"]:
                    assert args[2] == "native-vortex-target,schema-known,schema-compatible,delete-semantics-known,tombstone-semantics-known,commit-protocol-available,staged-output-required", args
                    emit("vortex-write-intent-plan", [
                        {"key": "target_uri", "value": target},
                        {"key": "target_is_native_vortex", "value": "true"},
                        {"key": "write_execution_allowed", "value": "false"},
                        {"key": "output_data_written", "value": "false"},
                        {"key": "manifest_written", "value": "false"},
                        {"key": "object_store_io", "value": "false"},
                        {"key": "upstream_vortex_write_called", "value": "false"},
                    ])
                elif args[:3] == ["vortex-output-payload-plan", target, workspace] and args[-2:] == ["--format", "json"]:
                    emit("vortex-output-payload-plan", [
                        {"key": "payload_write_allowed", "value": "false"},
                        {"key": "output_data_written", "value": "false"},
                        {"key": "object_store_io", "value": "false"},
                        {"key": "upstream_vortex_write_called", "value": "false"},
                    ])
                elif args[:2] == ["vortex-staged-manifest-file-plan", workspace] and args[-2:] == ["--format", "json"]:
                    emit("vortex-staged-manifest-file-plan", [
                        {"key": "manifest_file_written", "value": "false"},
                        {"key": "output_data_written", "value": "false"},
                        {"key": "object_store_io", "value": "false"},
                        {"key": "commit_performed", "value": "false"},
                    ])
                elif args[:2] == ["vortex-commit-marker-plan", workspace] and args[-2:] == ["--format", "json"]:
                    emit("vortex-commit-marker-plan", [
                        {"key": "commit_marker_written", "value": "false"},
                        {"key": "marker_write_allowed", "value": "false"},
                        {"key": "manifest_committed", "value": "false"},
                        {"key": "object_store_io", "value": "false"},
                    ])
                elif args[:2] == ["vortex-commit-intent-plan", target] and args[-2:] == ["--format", "json"]:
                    emit("vortex-commit-intent-plan", [
                        {"key": "commit_execution_allowed", "value": "false"},
                        {"key": "manifest_committed", "value": "false"},
                        {"key": "output_data_written", "value": "false"},
                        {"key": "object_store_io", "value": "false"},
                        {"key": "recovery_action_executed", "value": "false"},
                    ])
                elif args[:4] == ["vortex-commit-protocol-plan", target, "not-started", "validate-intent"] and args[-2:] == ["--format", "json"]:
                    emit("vortex-commit-protocol-plan", [
                        {"key": "commit_execution_allowed", "value": "false"},
                        {"key": "commit_marker_written", "value": "false"},
                        {"key": "manifest_committed", "value": "false"},
                        {"key": "object_store_io", "value": "false"},
                    ])
                elif args[:3] == ["vortex-local-commit-recovery-plan", target, workspace] and args[-2:] == ["--format", "json"]:
                    emit("vortex-local-commit-recovery-plan", [
                        {"key": "rollback_executed", "value": "false"},
                        {"key": "cleanup_performed", "value": "false"},
                        {"key": "object_store_io", "value": "false"},
                    ])
                elif args == ["table-intelligence-plan", "--format", "json"]:
                    emit("table-intelligence-plan", [{"key": "plan_only", "value": "true"}])
                elif args in (["table-compat-plan", "iceberg", "--format", "json"], ["table-compat-plan", "delta", "--format", "json"]):
                    emit("table-compat-plan", [
                        {"key": "plan_only", "value": "true"},
                        {"key": "write_io", "value": "false"},
                    ])
                elif args == ["layout-health-plan", "healthy", "--format", "json"]:
                    emit("layout-health-plan", [
                        {"key": "plan_only", "value": "true"},
                        {"key": "data_read", "value": "false"},
                        {"key": "write_io", "value": "false"},
                    ])
                elif args == ["compaction-plan", "small-files", "--format", "json"]:
                    emit("compaction-plan", [
                        {"key": "plan_only", "value": "true"},
                        {"key": "data_read", "value": "false"},
                        {"key": "write_io", "value": "false"},
                    ])
                elif args == ["cg9-catalog-metadata-gate", "--format", "json"]:
                    emit("cg9-catalog-metadata-gate", [
                        {"key": "plan_only", "value": "true"},
                        {"key": "claim_blocked", "value": "true"},
                        {"key": "catalog_io_allowed", "value": "false"},
                        {"key": "object_store_io_allowed", "value": "false"},
                        {"key": "write_io_allowed", "value": "false"},
                    ])
                elif args[0] in {
                    "object-store-request-plan",
                    "object-store-range-plan",
                    "object-store-coalesce-plan",
                    "object-store-schedule-plan",
                    "object-store-checkpoint-retry-plan",
                    "object-store-commit-plan",
                } and args[-2:] == ["--format", "json"]:
                    emit(args[0], [
                        {"key": "plan_only", "value": "true"},
                        {"key": "data_read", "value": "false"},
                        {"key": "object_store_io", "value": "false"},
                        {"key": "write_io", "value": "false"},
                    ])
                elif args[0] == "input-plan" and args[-2:] == ["--format", "json"]:
                    emit("input-plan", [
                        {"key": "plan_only", "value": "true"},
                        {"key": "source_kind", "value": "parquet"},
                        {"key": "capability_status", "value": "planned"},
                        {"key": "data_read", "value": "false"},
                        {"key": "data_materialized", "value": "false"},
                        {"key": "object_store_io", "value": "false"},
                        {"key": "write_io", "value": "false"},
                    ])
                elif args == ["capabilities", "migration", "--format", "json"]:
                    emit("capabilities", [
                        {"key": "scope", "value": "migration"},
                        {"key": "side_effect_free", "value": "true"},
                        {"key": "migration_report_count", "value": "5"},
                    ])
                elif args == ["correctness-plan", "--format", "json"]:
                    emit("correctness-plan", [
                        {"key": "status", "value": "planned"},
                        {"key": "fixture_count", "value": "36"},
                    ])
                elif args == ["benchmark-claim-evidence-plan", "foundation", "--format", "json"]:
                    emit("benchmark-claim-evidence-plan", [
                        {"key": "claim_evidence_status", "value": "needs_evidence"},
                        {"key": "performance_claim_allowed", "value": "false"},
                        {"key": "write_io", "value": "false"},
                        {"key": "fallback_attempted", "value": "false"},
                    ], diagnostics=[{
                        "code": "SL_NOT_IMPLEMENTED",
                        "severity": "error",
                        "category": "unsupported_feature",
                        "message": "needs evidence",
                        "feature": "benchmark",
                        "reason": "missing measurements",
                        "suggested_next_step": "collect evidence",
                        "fallback": {"attempted": False, "allowed": False, "engine": None, "reason": "disabled"},
                    }])
                elif args == ["world-class-sufficiency-plan", "--format", "json"]:
                    emit("world-class-sufficiency-plan", [
                        {"key": "fallback_attempted", "value": "false"},
                    ])
                else:
                    raise AssertionError(args)
                """
            )
        )

        report = ShardLoomClient(binary=binary).workflow_readiness_smoke(
            target_uri="file://tmp/out.vortex",
            compatibility_target_uri="file://tmp/out.parquet",
            workspace_path="target/stage",
        )

        self.assertIsInstance(report, WorkflowReadinessSmokeReport)
        self.assertEqual(len(report.plans), 30)
        self.assertEqual(report.output_commit[0].name, "vortex_output_target")
        self.assertIn("remote_input_s3_parquet", report.plan_names)
        self.assertIn("benchmark_claim_evidence", report.blocked_plan_names)
        self.assertIn("catalog_metadata_gate", report.blocked_plan_names)
        self.assertFalse(report.fallback_attempted)
        self.assertTrue(report.all_no_write)
        self.assertTrue(report.all_report_only_or_planned)
        self.assertEqual(
            report.output_commit[3].envelope.field("target_uri"),
            "file://tmp/out.vortex",
        )

    def test_input_adapter_and_input_plan_helpers(self) -> None:
        adapters_binary = self.fake_cli(
            textwrap.dedent(
                """
                import json, sys
                assert sys.argv[1:] == ["input-adapters", "--format", "json"], sys.argv
                print(json.dumps({
                    "schema_version": "shardloom.output.v2",
                    "command": "input-adapters",
                    "status": "success",
                    "summary": "ok",
                    "human_text": "ok",
                    "fallback": {"attempted": False, "allowed": False, "engine": None, "reason": "disabled"},
                    "diagnostics": [],
                    "fields": [
                        {"key": "critical_structured_adapter_order", "value": "native_vortex,parquet,arrow_ipc,csv,jsonl"},
                        {"key": "parquet_status", "value": "planned"}
                    ],
                }))
                """
            )
        )

        adapters = ShardLoomClient(binary=adapters_binary).input_adapters()

        self.assertEqual(adapters.command, "input-adapters")
        self.assertEqual(adapters.field("parquet_status"), "planned")

        plan_binary = self.fake_cli(
            textwrap.dedent(
                """
                import json, sys
                assert sys.argv[1:] == ["input-plan", "file://tmp/data.parquet", "--format", "json"], sys.argv
                print(json.dumps({
                    "schema_version": "shardloom.output.v2",
                    "command": "input-plan",
                    "status": "success",
                    "summary": "ok",
                    "human_text": "ok",
                    "fallback": {"attempted": False, "allowed": False, "engine": None, "reason": "disabled"},
                    "diagnostics": [],
                    "fields": [{"key": "plan_only", "value": "true"}],
                }))
                """
            )
        )

        input_plan = ShardLoomClient(binary=plan_binary).input_plan(
            "file://tmp/data.parquet"
        )

        self.assertEqual(input_plan.command, "input-plan")
        self.assertTrue(input_plan.field_bool("plan_only"))

    def test_compatibility_source_smoke_dispatches_report_only_input_plans(self) -> None:
        binary = self.fake_cli(
            textwrap.dedent(
                """
                import json, sys

                args = sys.argv[1:]
                fields = []
                command = args[0] if args else ""
                if args == ["input-adapters", "--format", "json"]:
                    fields = [
                        {"key": "critical_structured_adapter_order", "value": "native_vortex,parquet,arrow_ipc,csv,jsonl"},
                        {"key": "csv_status", "value": "planned"},
                        {"key": "jsonl_status", "value": "planned"},
                        {"key": "parquet_status", "value": "planned"},
                        {"key": "plan_only", "value": "true"},
                        {"key": "write_io", "value": "false"},
                    ]
                elif args == ["native-io-envelope-plan", "--format", "json"]:
                    fields = [
                        {"key": "per_path_certificate_required", "value": "true"},
                        {"key": "adapter_fidelity_report_required", "value": "true"},
                        {"key": "materialization_boundary_required_for_rows", "value": "true"},
                        {"key": "fallback_attempted", "value": "false"},
                    ]
                elif args[:1] == ["input-plan"] and args[-2:] == ["--format", "json"]:
                    uri = args[1]
                    source_kind = uri.rsplit(".", 1)[-1].replace("ndjson", "jsonl")
                    fields = [
                        {"key": "source_kind", "value": source_kind},
                        {"key": "adapter_kind", "value": "compatibility_file_adapter"},
                        {"key": "dataset_format", "value": source_kind},
                        {"key": "capability_status", "value": "planned"},
                        {"key": "metadata_availability", "value": "deferred"},
                        {"key": "fidelity", "value": "compatibility_logical"},
                        {"key": "materialization_risk", "value": "medium"},
                        {"key": "native_vortex", "value": "false"},
                        {"key": "compatibility_structured", "value": "true"},
                        {"key": "plan_only", "value": "true"},
                        {"key": "data_read", "value": "false"},
                        {"key": "data_materialized", "value": "false"},
                        {"key": "write_io", "value": "false"},
                        {"key": "fallback_execution_allowed", "value": "false"},
                    ]
                else:
                    raise AssertionError(args)

                print(json.dumps({
                    "schema_version": "shardloom.output.v2",
                    "command": command,
                    "status": "success",
                    "summary": "ok",
                    "human_text": "ok",
                    "fallback": {"attempted": False, "allowed": False, "engine": None, "reason": "disabled"},
                    "diagnostics": [],
                    "fields": fields,
                }))
                """
            )
        )

        report = ShardLoomClient(binary=binary).compatibility_source_smoke(
            {
                "csv": "fact.csv",
                "jsonl": "events.jsonl",
                "parquet": "fact.parquet",
            }
        )

        self.assertIsInstance(report, CompatibilitySourceSmokeReport)
        self.assertEqual(
            report.commands,
            (
                "input-adapters",
                "native-io-envelope-plan",
                "input-plan",
                "input-plan",
                "input-plan",
            ),
        )
        self.assertEqual(report.compatibility_source_names, ("csv", "jsonl", "parquet"))
        self.assertEqual(report.planned_source_names, ("csv", "jsonl", "parquet"))
        self.assertTrue(report.all_plan_only)
        self.assertFalse(report.fallback_attempted)
        self.assertEqual(report.sources[1].plan.field("source_kind"), "jsonl")


if __name__ == "__main__":
    unittest.main()
