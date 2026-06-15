from __future__ import annotations

import json
import importlib
import os
import re
import sys
import tempfile
import textwrap
import unittest
from unittest import mock
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
SCRIPTS_DIR = REPO_ROOT / "scripts"
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))

from release_report_utils import upstream_vortex_provider_version

UPSTREAM_VORTEX_PROVIDER_VERSION = upstream_vortex_provider_version(REPO_ROOT)

sys.path.insert(0, str(REPO_ROOT / "python" / "src"))

shardloom_session_module = importlib.import_module("shardloom.session")

from shardloom import (
    __version__,
    context as shardloom_context,
    session as shardloom_session,
    ClaimGateCloseoutReport,
    CommandMetadataReport,
    ComputeCapabilityMatrix,
    CompatibilitySourceSmokeReport,
    CapabilityPosture,
    CompatibilityPreparedVortexRoute,
    ContextCapabilities,
    CapabilityView,
    DataFrameMethodCapabilityMatrix,
    DataFrameNotebookPackageReadinessReport,
    ETLWorkflowCapabilityMatrix,
    GeneratedSourceApiAdmissionMatrix,
    EngineCapabilityMatrix,
    EvidenceSchemaRegistryReport,
    EvidenceAwareOptimizerTraceReport,
    ExecutionResultEnvelopeView,
    FoundryGeneratedOutputReport,
    FrontDoorParityMatrix,
    GeneratedObjectStoreOutputReport,
    GeneratedPartitionedObjectStoreOutputReport,
    GeneratedSourceCertificateContract,
    GeneratedSourceEvidenceAlignmentReport,
    GeneratedSourceWriteReport,
    LocalVortexPrimitiveSmokeReport,
    NativeVortexQuery,
    NativeVortexRoute,
    OpenLineageFacetMappingReport,
    OpenTelemetryTraceExportContractReport,
    ShardLoomBinaryNotFoundError,
    ShardLoomClient,
    ShardLoomCommandError,
    ShardLoomContext,
    ShardLoomSession,
    ShardLoomProtocolError,
    SqlLocalSourceSmokeReport,
    SessionPreparedState,
    SessionLazyFrame,
    SessionSqlResult,
    SessionSqlWorkflow,
    OutputEnvelope,
    PreparedVortexArtifacts,
    PreparedVortexBatchResult,
    PreparedVortexQuery,
    PreparedVortexScanPushdownRow,
    PredicateDtypeCoverageRow,
    ProductionUnsupportedDiagnosticRow,
    RestApiContractPlan,
    RestApiDataPlane,
    RestApiDiscoveryContract,
    RestApiEventStream,
    RestApiLocalLifecycle,
    RestApiPlanPreview,
    RestApiSecurityGovernance,
    RunsTodaySupportMatrix,
    RunsTodaySupportRow,
    SemanticConformanceSuite,
    SessionCacheSmokeReport,
    validate_runtime_execution_fields,
    VortexIngestSmokeReport,
    WorkloadCertificationDossier,
    WorkflowReadinessSmokeReport,
)

_FAKE_CLI_ENVELOPE_PRELUDE = textwrap.dedent(
    """
    import json as _shardloom_json
    import sys as _shardloom_sys

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

    def _shardloom_public_request_output_format(requested_output):
        return {
            "collect": "inline-jsonl",
            "write_jsonl": "jsonl",
            "write_csv": "csv",
            "write_parquet": "parquet",
            "write_arrow_ipc": "arrow-ipc",
            "write_avro": "avro",
            "write_orc": "orc",
            "write_vortex": "vortex",
        }[requested_output]

    def _shardloom_take_flag(args, flag):
        if flag not in args:
            return None
        index = args.index(flag)
        if index + 1 >= len(args):
            return None
        return args[index + 1]

    def _shardloom_has_flag(args, flag):
        return flag in args

    def _shardloom_values_after_flag(args, flag):
        values = []
        index = 0
        while index < len(args):
            if args[index] == flag and index + 1 < len(args):
                values.append(args[index + 1])
                index += 2
            else:
                index += 1
        return values

    def _shardloom_append_fanout_outputs(rewritten, args):
        for fanout_output in _shardloom_values_after_flag(args, "--fanout-output"):
            rewritten.extend(["--fanout-output", fanout_output])

    def _shardloom_without_format(args):
        if "--format" not in args:
            return args, []
        index = args.index("--format")
        return args[:index] + args[index + 2 :], args[index : index + 2]

    def _shardloom_rewrite_public_run_argv():
        args = _shardloom_sys.argv[1:]
        if len(args) < 2 or args[0] != "run":
            return
        args, format_tail = _shardloom_without_format(args)
        surface = args[1]
        requested_output = _shardloom_take_flag(args, "--request") or "collect"
        output_format = _shardloom_public_request_output_format(requested_output)
        output_ref = _shardloom_take_flag(args, "--output")
        sql = _shardloom_take_flag(args, "--sql")
        generated_kind = _shardloom_take_flag(args, "--generated-source-kind")
        allow_overwrite = _shardloom_has_flag(args, "--allow-overwrite")

        if generated_kind is not None:
            if generated_kind in {
                "user_rows",
                "literal_table",
                "calendar",
                "dataframe_source_free_projection",
                "dataframe_generated_with_column",
            }:
                rewritten = [
                    "generated-source-user-rows-smoke",
                    output_ref,
                    _shardloom_take_flag(args, "--generated-schema"),
                    _shardloom_take_flag(args, "--generated-rows"),
                    "--source-kind",
                    generated_kind,
                    "--output-format",
                    output_format,
                ]
            else:
                command = (
                    "generated-source-sequence-smoke"
                    if generated_kind == "sequence"
                    else "generated-source-range-smoke"
                )
                rewritten = [
                    command,
                    output_ref,
                    _shardloom_take_flag(args, "--generated-range-start"),
                    _shardloom_take_flag(args, "--generated-range-end"),
                    "--step",
                    _shardloom_take_flag(args, "--generated-range-step") or "1",
                    "--column",
                    _shardloom_take_flag(args, "--generated-range-column") or "value",
                    "--output-format",
                    output_format,
                ]
            _shardloom_append_fanout_outputs(rewritten, args)
            if allow_overwrite:
                rewritten.append("--allow-overwrite")
            _shardloom_sys.argv = [_shardloom_sys.argv[0], *rewritten, *format_tail]
            return

        if sql is not None and surface == "sql" and output_ref is not None and " FROM '" not in sql:
            rewritten = [
                "generated-source-sql-smoke",
                output_ref,
                sql,
                "--output-format",
                output_format,
            ]
            _shardloom_append_fanout_outputs(rewritten, args)
            if allow_overwrite:
                rewritten.append("--allow-overwrite")
            _shardloom_sys.argv = [_shardloom_sys.argv[0], *rewritten, *format_tail]
            return

        if sql is not None:
            rewritten = [
                "sql-local-source-smoke",
                sql,
                "--output-format",
                output_format,
            ]
            if output_ref is not None:
                rewritten.extend(["--output", output_ref])
            _shardloom_append_fanout_outputs(rewritten, args)
            if allow_overwrite:
                rewritten.append("--allow-overwrite")
            _shardloom_sys.argv = [_shardloom_sys.argv[0], *rewritten, *format_tail]

    if not globals().get("_SHARDLOOM_DISABLE_PUBLIC_RUN_REWRITE", False):
        _shardloom_rewrite_public_run_argv()
    """
)


def _complete_pulseweave_runtime_fields() -> dict[str, object]:
    return {
        "prepared_state_id": "prepared-state://pulseweave",
        "prepared_state_digest": "fnv1a64:prepared",
        "data_decoded": False,
        "runtime_execution_certificate_id": "execution.pulseweave",
        "runtime_execution_certificate_status": "certified",
        "native_io_certificate_status": "certified",
        "fallback_attempted": False,
        "external_engine_invoked": False,
        "claim_gate_status": "not_claim_grade",
        "prepared_vortex_scale_correctness_digest": "fnv1a64:correct",
        "pulseweave_schema_version": "shardloom.pulseweave.runtime_control.v1",
        "pulseweave_status": "applied",
        "pulseweave_application_scope": "prepared_vortex_local_batch",
        "pulseweave_runtime_decision_applied": True,
        "pulseweave_policy_mutated": True,
        "pulseweave_decision_digest": "fnv1a64:pulse",
        "pulseweave_blocker": "none",
        "pulseweave_claim_gate_status": "pulseweave_runtime_certified",
        "pulseweave_fallback_attempted": False,
        "pulseweave_external_engine_invoked": False,
        "flow_inventory_schema_version": "shardloom.pulseweave.flow_inventory.v1",
        "flow_inventory_wip_limit": 2,
        "flow_inventory_peak_in_flight": 2,
        "flow_inventory_ready_task_count": 5,
        "flow_inventory_held_for_memory_count": 0,
        "flow_inventory_held_for_downstream_count": 3,
        "flow_inventory_completed_task_count": 5,
        "flow_inventory_failed_task_count": 0,
        "flow_inventory_backpressure_event_count": 1,
        "flow_inventory_existing_scheduler_preserved": False,
        "scarcity_ledger_schema_version": "shardloom.pulseweave.scarcity_ledger.v1",
        "scarcity_ledger_memory_price_bps": 0,
        "scarcity_ledger_queue_price_bps": 10000,
        "scarcity_ledger_decode_price_bps": 0,
        "scarcity_ledger_sink_price_bps": 2500,
        "scarcity_ledger_spill_price_bps": 0,
        "scarcity_ledger_total_price_bps": 10000,
        "scarcity_ledger_selected_action": "hold_for_downstream",
        "scarcity_ledger_decision_reason": "downstream result-sink pressure limits in-flight work",
        "scarcity_ledger_decision_digest": "fnv1a64:ledger",
        "endopulse_schema_version": "shardloom.pulseweave.endopulse.v1",
        "endopulse_signal_set": "sink_pressure",
        "endopulse_previous_target_task_bytes": 67108864,
        "endopulse_next_target_task_bytes": 67108864,
        "endopulse_previous_wip_limit": 2,
        "endopulse_next_wip_limit": 1,
        "endopulse_adjustment_applied": True,
        "endopulse_hysteresis_state": "one_window_local_only",
        "endopulse_persistent_state_used": False,
        "proofbound_schema_version": "shardloom.pulseweave.proofbound.v1",
        "proofbound_pre_application_status": "admitted",
        "proofbound_post_application_status": "certified",
        "proofbound_required_evidence": "prepared_local_route,memory_budget,max_parallelism,task_estimates,materialization_decode_boundary,correctness_digest,output_digest,execution_certificate,native_io_certificate,no_fallback",
        "proofbound_missing_evidence": "none",
        "proofbound_certificate_status": "certified",
        "proofbound_no_fallback_status": "verified",
        "proofbound_claim_allowed": True,
    }


class ShardLoomClientTests(unittest.TestCase):
    def fake_cli(self, body: str, *, rewrite_public_run: bool = True) -> list[str]:
        tempdir = tempfile.TemporaryDirectory()
        self.addCleanup(tempdir.cleanup)
        path = Path(tempdir.name) / "fake_shardloom.py"
        prefix = "" if rewrite_public_run else "_SHARDLOOM_DISABLE_PUBLIC_RUN_REWRITE = True\n"
        path.write_text(prefix + _FAKE_CLI_ENVELOPE_PRELUDE + "\n" + body, encoding="utf-8")
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

    def test_doctor_returns_v1_stable_no_probe_fields(self) -> None:
        binary = self.fake_cli(
            textwrap.dedent(
                """
                import json, sys
                assert sys.argv[1:] == ["doctor", "--format", "json"], sys.argv
                fields = [
                    ["doctor_schema_version", "shardloom.doctor.v1"],
                    ["doctor_report_id", "doctor.local_v1"],
                    ["doctor_check_count", "8"],
                    ["doctor_check_order", "cli_version,python_package_version,package_channel,feature_support,vortex_support,local_write_support,no_fallback_invariant,environment_details"],
                    ["cli_version", "0.1.0"],
                    ["doctor_check_cli_version_status", "available"],
                    ["python_package_version", "not_probed"],
                    ["doctor_check_python_package_version_status", "not_probed_no_python_import"],
                    ["package_channel", "source_tree_local"],
                    ["doctor_check_package_channel_status", "local_source_tree_no_publication"],
                    ["doctor_check_feature_support_status", "contract_only"],
                    ["doctor_check_vortex_support_status", "deferred"],
                    ["doctor_check_local_write_support_status", "local_workspace_feature_gated"],
                    ["doctor_check_no_fallback_invariant_status", "verified"],
                    ["doctor_check_environment_details_status", "static_no_probe"],
                    ["environment_details", "static_no_probe"],
                    ["environment_probe_performed", "false"],
                    ["filesystem_probe_performed", "false"],
                    ["network_probe_performed", "false"],
                    ["runtime_execution", "false"],
                    ["fallback_attempted", "false"],
                    ["external_engine_invoked", "false"],
                    ["support_bundle_available", "true"],
                    ["support_bundle_command", "support-bundle --format json"],
                ]
                print(json.dumps({
                    "schema_version": "shardloom.output.v2",
                    "command": "doctor",
                    "status": "success",
                    "summary": "doctor checks",
                    "human_text": "ShardLoom doctor",
                    "fallback": {"attempted": False, "allowed": False, "engine": None, "reason": "disabled"},
                    "diagnostics": [],
                    "fields": [{"key": key, "value": value} for key, value in fields],
                }))
                """
            )
        )

        result = ShardLoomClient(binary=binary).doctor()

        self.assertEqual(result.command, "doctor")
        self.assertEqual(result.field("doctor_schema_version"), "shardloom.doctor.v1")
        self.assertEqual(result.field_int("doctor_check_count"), 8)
        self.assertIn("vortex_support", result.field("doctor_check_order") or "")
        self.assertEqual(
            result.field("doctor_check_python_package_version_status"),
            "not_probed_no_python_import",
        )
        self.assertEqual(result.field("package_channel"), "source_tree_local")
        self.assertFalse(result.field_bool("environment_probe_performed", True))
        self.assertFalse(result.field_bool("filesystem_probe_performed", True))
        self.assertFalse(result.field_bool("network_probe_performed", True))
        self.assertFalse(result.field_bool("fallback_attempted", True))
        self.assertFalse(result.field_bool("external_engine_invoked", True))
        self.assertTrue(result.field_bool("support_bundle_available", False))

    def test_support_bundle_redacts_note_and_keeps_effects_disabled(self) -> None:
        binary = self.fake_cli(
            textwrap.dedent(
                """
                import json, sys
                assert sys.argv[1:] == [
                    "support-bundle",
                    "--note",
                    "token=abc123 Authorization: Bearer secret-value",
                    "--include-defaults",
                    "--format",
                    "json",
                ], sys.argv
                fields = [
                    ["schema_version", "shardloom.support_bundle.v1"],
                    ["bundle_id", "support.bundle.local.v1"],
                    ["generated_by", "shardloom"],
                    ["support_bundle_status", "generated_in_envelope"],
                    ["support_bundle_generated", "true"],
                    ["support_bundle_written", "false"],
                    ["redaction_status", "redacted"],
                    ["redaction_policy", "redaction(kind=strict, redact_prompts=true, redact_payloads=true, redact_paths=true)"],
                    ["raw_secret_values_present", "false"],
                    ["input_contains_redacted_tokens", "true"],
                    ["redacted_note_preview", "token=<redacted> Authorization: Bearer <redacted>"],
                    ["included_reports", "doctor,feature_footprint,command_metadata,v1_api_schema_stability"],
                    ["included_report_refs", "doctor,feature_footprint,command_metadata,v1_api_schema_stability"],
                    ["included_report_count", "4"],
                    ["secret_values_included", "false"],
                    ["filesystem_write_performed", "false"],
                    ["filesystem_probe_performed", "false"],
                    ["network_probe_performed", "false"],
                    ["external_effects_executed", "false"],
                    ["runtime_execution", "false"],
                    ["fallback_attempted", "false"],
                    ["external_engine_invoked", "false"],
                    ["doctor_schema_version", "shardloom.doctor.v1"],
                    ["doctor_check_count", "8"],
                ]
                print(json.dumps({
                    "schema_version": "shardloom.output.v2",
                    "command": "support-bundle",
                    "status": "success",
                    "summary": "support bundle",
                    "human_text": "ShardLoom support bundle",
                    "fallback": {"attempted": False, "allowed": False, "engine": None, "reason": "disabled"},
                    "diagnostics": [],
                    "fields": [{"key": key, "value": value} for key, value in fields],
                }))
                """
            )
        )

        result = ShardLoomClient(binary=binary).support_bundle(
            note="token=abc123 Authorization: Bearer secret-value"
        )

        self.assertEqual(result.command, "support-bundle")
        self.assertEqual(result.field("schema_version"), "shardloom.support_bundle.v1")
        self.assertEqual(result.field("generated_by"), "shardloom")
        self.assertTrue(result.field_bool("support_bundle_generated", False))
        self.assertFalse(result.field_bool("support_bundle_written", True))
        self.assertEqual(result.field("redaction_status"), "redacted")
        self.assertFalse(result.field_bool("secret_values_included", True))
        self.assertIn("doctor", result.field("included_report_refs") or "")
        self.assertTrue(result.field_bool("input_contains_redacted_tokens", False))
        preview = result.field("redacted_note_preview") or ""
        self.assertIn("token=<redacted>", preview)
        self.assertIn("Bearer <redacted>", preview)
        self.assertNotIn("abc123", preview)
        self.assertNotIn("secret-value", preview)
        self.assertFalse(result.field_bool("raw_secret_values_present", True))
        self.assertFalse(result.field_bool("filesystem_write_performed", True))
        self.assertFalse(result.field_bool("network_probe_performed", True))
        self.assertFalse(result.field_bool("fallback_attempted", True))
        self.assertFalse(result.field_bool("external_engine_invoked", True))

    def test_runs_today_returns_typed_current_support_matrix(self) -> None:
        binary = self.fake_cli(
            textwrap.dedent(
                """
                import json, sys
                assert sys.argv[1:] == ["runs-today", "--format", "json"], sys.argv
                fields = [
                    ["runs_today_schema_version", "shardloom.runs_today_support_matrix.v1"],
                    ["runs_today_matrix_id", "review-p0-1.current-support"],
                    ["runs_today_support_state_vocabulary", "executable,feature_gated,diagnostic_only,report_only,blocked,future"],
                    ["runs_today_family_order", "cli_command,claim_state"],
                    ["runs_today_row_order", "cli_sql_local_source_smoke,claim_performance_superiority"],
                    ["runs_today_row_count", "2"],
                    ["runs_today_executable_row_count", "1"],
                    ["runs_today_feature_gated_row_count", "0"],
                    ["runs_today_diagnostic_only_row_count", "0"],
                    ["runs_today_report_only_row_count", "0"],
                    ["runs_today_blocked_row_count", "1"],
                    ["runs_today_future_row_count", "0"],
                    ["runs_today_all_rows_no_fallback_no_external_engine", "true"],
                    ["runs_today_performance_claim_allowed", "false"],
                    ["runs_today_package_publication_allowed", "false"],
                    ["production_unsupported_diagnostic_schema_version", "shardloom.production_unsupported_diagnostics.v1"],
                    ["production_unsupported_diagnostic_row_count", "2"],
                    ["production_unsupported_diagnostic_row_order", "broad_sql_dataframe_runtime,object_store_runtime"],
                    ["production_unsupported_diagnostic_all_rows_fallback_attempted_false", "true"],
                    ["production_unsupported_diagnostic_all_rows_external_engine_invoked_false", "true"],
                    ["production_unsupported_diagnostic_all_rows_side_effects_performed_false", "true"],
                    ["production_unsupported_diagnostic_row_broad_sql_dataframe_runtime_production_family", "sql_dataframe"],
                    ["production_unsupported_diagnostic_row_broad_sql_dataframe_runtime_user_surface", "sql,LazyFrame.collect,workflow-unsupported-plan"],
                    ["production_unsupported_diagnostic_row_broad_sql_dataframe_runtime_entrypoint_kind", "report_only_or_preview"],
                    ["production_unsupported_diagnostic_row_broad_sql_dataframe_runtime_support_status", "unsupported_boundary"],
                    ["production_unsupported_diagnostic_row_broad_sql_dataframe_runtime_diagnostic_code", "SL_UNSUPPORTED_PRODUCTION_SQL_DATAFRAME"],
                    ["production_unsupported_diagnostic_row_broad_sql_dataframe_runtime_blocker_id", "cg21.workflow.sql.frontend_unsupported"],
                    ["production_unsupported_diagnostic_row_broad_sql_dataframe_runtime_message", "Broad production SQL/DataFrame execution is not admitted."],
                    ["production_unsupported_diagnostic_row_broad_sql_dataframe_runtime_next_action", "Use capabilities sql,dataframe before requesting execution."],
                    ["production_unsupported_diagnostic_row_broad_sql_dataframe_runtime_required_evidence", "sql_parser,binder,planner"],
                    ["production_unsupported_diagnostic_row_broad_sql_dataframe_runtime_claim_gate_status", "not_claim_grade"],
                    ["production_unsupported_diagnostic_row_broad_sql_dataframe_runtime_route_scope", "preview_or_report_only"],
                    ["production_unsupported_diagnostic_row_broad_sql_dataframe_runtime_fallback_attempted", "false"],
                    ["production_unsupported_diagnostic_row_broad_sql_dataframe_runtime_external_engine_invoked", "false"],
                    ["production_unsupported_diagnostic_row_broad_sql_dataframe_runtime_side_effects_performed", "false"],
                    ["production_unsupported_diagnostic_row_object_store_runtime_production_family", "object_store"],
                    ["production_unsupported_diagnostic_row_object_store_runtime_user_surface", "object_store_read,s3://"],
                    ["production_unsupported_diagnostic_row_object_store_runtime_entrypoint_kind", "stub_or_fixture"],
                    ["production_unsupported_diagnostic_row_object_store_runtime_support_status", "unsupported_boundary"],
                    ["production_unsupported_diagnostic_row_object_store_runtime_diagnostic_code", "SL_UNSUPPORTED_PRODUCTION_OBJECT_STORE"],
                    ["production_unsupported_diagnostic_row_object_store_runtime_blocker_id", "review-p0-3.object_store_runtime_and_path_safety_required"],
                    ["production_unsupported_diagnostic_row_object_store_runtime_message", "Production object-store runtime is not admitted."],
                    ["production_unsupported_diagnostic_row_object_store_runtime_next_action", "Use object-store capability reports."],
                    ["production_unsupported_diagnostic_row_object_store_runtime_required_evidence", "credential_policy,native_io_certificate"],
                    ["production_unsupported_diagnostic_row_object_store_runtime_claim_gate_status", "not_claim_grade"],
                    ["production_unsupported_diagnostic_row_object_store_runtime_route_scope", "fixture_only_or_blocked"],
                    ["production_unsupported_diagnostic_row_object_store_runtime_fallback_attempted", "false"],
                    ["production_unsupported_diagnostic_row_object_store_runtime_external_engine_invoked", "false"],
                    ["production_unsupported_diagnostic_row_object_store_runtime_side_effects_performed", "false"],
                    ["runs_today_row_cli_sql_local_source_smoke_family", "cli_command"],
                    ["runs_today_row_cli_sql_local_source_smoke_surface", "sql-local-source-smoke"],
                    ["runs_today_row_cli_sql_local_source_smoke_support_state", "executable"],
                    ["runs_today_row_cli_sql_local_source_smoke_feature_gate", "default"],
                    ["runs_today_row_cli_sql_local_source_smoke_evidence_refs", "sql_local_source_runtime_smoke,sql_frontend_runtime_ladder_fields"],
                    ["runs_today_row_cli_sql_local_source_smoke_blocker_id", "none"],
                    ["runs_today_row_cli_sql_local_source_smoke_claim_gate_status", "fixture_smoke_only"],
                    ["runs_today_row_cli_sql_local_source_smoke_claim_boundary", "scoped local SQL only"],
                    ["runs_today_row_cli_sql_local_source_smoke_runtime_execution", "true"],
                    ["runs_today_row_cli_sql_local_source_smoke_data_read", "true"],
                    ["runs_today_row_cli_sql_local_source_smoke_write_io", "false"],
                    ["runs_today_row_cli_sql_local_source_smoke_fallback_attempted", "false"],
                    ["runs_today_row_cli_sql_local_source_smoke_external_engine_invoked", "false"],
                    ["runs_today_row_claim_performance_superiority_family", "claim_state"],
                    ["runs_today_row_claim_performance_superiority_surface", "performance_superiority,spark_replacement"],
                    ["runs_today_row_claim_performance_superiority_support_state", "blocked"],
                    ["runs_today_row_claim_performance_superiority_feature_gate", "not_enabled"],
                    ["runs_today_row_claim_performance_superiority_evidence_refs", "benchmark_claim_evidence_plan"],
                    ["runs_today_row_claim_performance_superiority_blocker_id", "cg5.cg6.required"],
                    ["runs_today_row_claim_performance_superiority_claim_gate_status", "not_claim_grade"],
                    ["runs_today_row_claim_performance_superiority_claim_boundary", "no performance claim"],
                    ["runs_today_row_claim_performance_superiority_runtime_execution", "false"],
                    ["runs_today_row_claim_performance_superiority_data_read", "false"],
                    ["runs_today_row_claim_performance_superiority_write_io", "false"],
                    ["runs_today_row_claim_performance_superiority_fallback_attempted", "false"],
                    ["runs_today_row_claim_performance_superiority_external_engine_invoked", "false"],
                ]
                print(json.dumps({
                    "schema_version": "shardloom.output.v2",
                    "command": "runs-today",
                    "status": "success",
                    "summary": "ok",
                    "human_text": "ok",
                    "fallback": {"attempted": False, "allowed": False, "engine": None, "reason": "disabled"},
                    "diagnostics": [],
                    "fields": [{"key": key, "value": value} for key, value in fields],
                }))
                """
            )
        )

        matrix = ShardLoomClient(binary=binary).runs_today()

        self.assertIsInstance(matrix, RunsTodaySupportMatrix)
        self.assertEqual(matrix.schema_version, "shardloom.runs_today_support_matrix.v1")
        self.assertEqual(matrix.matrix_id, "review-p0-1.current-support")
        self.assertEqual(
            matrix.support_state_vocabulary,
            (
                "executable",
                "feature_gated",
                "diagnostic_only",
                "report_only",
                "blocked",
                "future",
            ),
        )
        self.assertEqual(matrix.executable_row_count, 1)
        self.assertEqual(matrix.blocked_row_count, 1)
        self.assertTrue(matrix.all_rows_no_fallback_no_external_engine)
        self.assertFalse(matrix.performance_claim_allowed)
        self.assertFalse(matrix.package_publication_allowed)
        self.assertEqual(
            matrix.production_unsupported_diagnostic_schema_version,
            "shardloom.production_unsupported_diagnostics.v1",
        )
        self.assertTrue(matrix.production_unsupported_diagnostic_all_rows_safe)
        production_sql = matrix.production_unsupported_diagnostic_row(
            "broad-sql-dataframe-runtime"
        )
        self.assertIsInstance(production_sql, ProductionUnsupportedDiagnosticRow)
        self.assertEqual(
            production_sql.diagnostic_code,
            "SL_UNSUPPORTED_PRODUCTION_SQL_DATAFRAME",
        )
        self.assertIn("workflow-unsupported-plan", production_sql.user_surface)
        self.assertFalse(production_sql.fallback_attempted)
        self.assertFalse(production_sql.external_engine_invoked)
        self.assertFalse(production_sql.side_effects_performed)
        row = matrix.row("cli_sql_local_source_smoke")
        self.assertIsInstance(row, RunsTodaySupportRow)
        self.assertEqual(row.support_state, "executable")
        self.assertEqual(row.surface, ("sql-local-source-smoke",))
        self.assertIn("sql_frontend_runtime_ladder_fields", row.evidence_refs)
        self.assertTrue(row.runtime_execution)
        self.assertTrue(row.data_read)
        self.assertFalse(row.fallback_attempted)
        blocked = matrix.rows_by_support_state("blocked")
        self.assertEqual(blocked[0].row_id, "claim_performance_superiority")
        self.assertEqual(blocked[0].surface, ("performance_superiority", "spark_replacement"))
        self.assertEqual(matrix.rows_by_family("claim_state"), blocked)

    def test_command_metadata_returns_typed_registry_report(self) -> None:
        binary = self.fake_cli(
            textwrap.dedent(
                """
                import json, sys
                assert sys.argv[1:] == ["command-metadata", "vortex-ingest-smoke", "--format", "json"], sys.argv
                fields = [
                    ["command_registry_schema_version", "shardloom.command_registry.v1"],
                    ["registered_command_count", "4"],
                    ["command_registry_support_state_vocabulary", "executable,feature_gated,diagnostic_only,report_only,blocked,future"],
                    ["command_registry_user_surface_graduation_posture_vocabulary", "high_level_context,client_only,diagnostic_only,feature_gated,not_user_facing"],
                    ["registered_commands", "help,command-metadata,status,vortex-ingest-smoke"],
                    ["registered_command_families", "help=status_capabilities,command-metadata=status_capabilities,status=status_capabilities,vortex-ingest-smoke=prepared_source_backed_execution"],
                    ["registered_command_support_states", "help=diagnostic_only,command-metadata=diagnostic_only,status=diagnostic_only,vortex-ingest-smoke=executable"],
                    ["registered_command_user_surface_graduation_postures", "help=diagnostic_only,command-metadata=diagnostic_only,status=diagnostic_only,vortex-ingest-smoke=client_only"],
                    ["registered_command_side_effect_levels", "help=side_effect_free_metadata_or_report,command-metadata=side_effect_free_metadata_or_report,status=side_effect_free_metadata_or_report,vortex-ingest-smoke=local_runtime_or_local_artifact_effect_possible"],
                    ["registered_command_feature_gate_statuses", "help=not_required_for_metadata,command-metadata=not_required_for_metadata,status=not_required_for_metadata,vortex-ingest-smoke=not_required_for_metadata"],
                    ["registered_command_input_contracts", "help=registry_or_capability_scope_args,command-metadata=registry_or_capability_scope_args,status=registry_or_capability_scope_args,vortex-ingest-smoke=local_source_or_vortex_artifact_args"],
                    ["registered_command_output_contracts", "help=typed_envelope_metadata_report_only,command-metadata=typed_envelope_metadata_report_only,status=typed_envelope_metadata_report_only,vortex-ingest-smoke=typed_envelope_plus_local_runtime_or_artifact_evidence"],
                    ["registered_command_owning_phase_items", "help=REVIEW-P1-1,command-metadata=REVIEW-P1-1,status=REVIEW-P1-1,vortex-ingest-smoke=GAR-RUNTIME-IMPL-4"],
                    ["selected_command", "vortex-ingest-smoke"],
                    ["selected_command_family", "prepared_source_backed_execution"],
                    ["selected_command_support_state", "executable"],
                    ["selected_command_user_surface_graduation_posture", "client_only"],
                    ["selected_command_side_effect_level", "local_runtime_or_local_artifact_effect_possible"],
                    ["selected_command_usage_fragment", "vortex-ingest-smoke <local-source-path> <target.vortex>"],
                    ["selected_command_feature_gate_status", "not_required_for_metadata"],
                    ["selected_command_input_contract", "local_source_or_vortex_artifact_args"],
                    ["selected_command_output_contract", "typed_envelope_plus_local_runtime_or_artifact_evidence"],
                    ["selected_command_evidence_fields", "command|family|support_state|user_surface_graduation_posture|side_effect_level|usage_fragment|feature_gate_status|input_contract|output_contract|owning_phase_item|claim_boundary|fallback_boundary|fallback_attempted|external_engine_invoked"],
                    ["selected_command_owning_phase_item", "GAR-RUNTIME-IMPL-4"],
                    ["fallback_attempted", "false"],
                    ["external_engine_invoked", "false"],
                ]
                print(json.dumps({
                    "schema_version": "shardloom.output.v2",
                    "command": "command-metadata",
                    "status": "success",
                    "summary": "ok",
                    "human_text": "ok",
                    "fallback": {"attempted": False, "allowed": False, "engine": None, "reason": "disabled"},
                    "diagnostics": [],
                    "fields": [{"key": key, "value": value} for key, value in fields],
                }))
                """
            )
        )

        report = ShardLoomClient(binary=binary).command_metadata("vortex-ingest-smoke")

        self.assertIsInstance(report, CommandMetadataReport)
        self.assertEqual(report.schema_version, "shardloom.command_registry.v1")
        self.assertEqual(report.registered_command_count, 4)
        self.assertEqual(
            report.support_state_vocabulary,
            (
                "executable",
                "feature_gated",
                "diagnostic_only",
                "report_only",
                "blocked",
                "future",
            ),
        )
        self.assertEqual(
            report.user_surface_graduation_posture_vocabulary,
            (
                "high_level_context",
                "client_only",
                "diagnostic_only",
                "feature_gated",
                "not_user_facing",
            ),
        )
        self.assertEqual(
            report.registered_commands,
            ("help", "command-metadata", "status", "vortex-ingest-smoke"),
        )
        self.assertEqual(report.selected_command, "vortex-ingest-smoke")
        self.assertEqual(
            report.selected_command_family,
            "prepared_source_backed_execution",
        )
        self.assertEqual(report.family_for("command-metadata"), "status_capabilities")
        self.assertEqual(report.support_state_for("help"), "diagnostic_only")
        self.assertEqual(
            report.user_surface_graduation_posture_for("command-metadata"),
            "diagnostic_only",
        )
        self.assertEqual(report.support_state_for("vortex-ingest-smoke"), "executable")
        self.assertEqual(
            report.user_surface_graduation_posture_for("vortex-ingest-smoke"),
            "client_only",
        )
        self.assertEqual(
            report.side_effect_level_for("vortex-ingest-smoke"),
            "local_runtime_or_local_artifact_effect_possible",
        )
        self.assertEqual(
            report.feature_gate_status_for("vortex-ingest-smoke"),
            "not_required_for_metadata",
        )
        self.assertEqual(
            report.input_contract_for("vortex-ingest-smoke"),
            "local_source_or_vortex_artifact_args",
        )
        self.assertEqual(
            report.output_contract_for("vortex-ingest-smoke"),
            "typed_envelope_plus_local_runtime_or_artifact_evidence",
        )
        self.assertEqual(
            report.owning_phase_item_for("vortex-ingest-smoke"),
            "GAR-RUNTIME-IMPL-4",
        )
        self.assertEqual(
            report.selected_command_feature_gate_status,
            "not_required_for_metadata",
        )
        self.assertEqual(
            report.selected_command_user_surface_graduation_posture,
            "client_only",
        )
        self.assertEqual(
            report.selected_command_input_contract,
            "local_source_or_vortex_artifact_args",
        )
        self.assertEqual(
            report.selected_command_output_contract,
            "typed_envelope_plus_local_runtime_or_artifact_evidence",
        )
        self.assertEqual(
            report.selected_command_owning_phase_item,
            "GAR-RUNTIME-IMPL-4",
        )
        self.assertIn("fallback_attempted", report.selected_command_evidence_fields)
        self.assertFalse(report.fallback_attempted)
        self.assertFalse(report.external_engine_invoked)

    def test_evidence_schema_returns_typed_registry_report(self) -> None:
        binary = self.fake_cli(
            textwrap.dedent(
                """
                import json, sys
                assert sys.argv[1:] == ["evidence-schema", "execution_mode_selection_report", "--format", "json"], sys.argv
                fields = [
                    ["evidence_schema_registry_schema_version", "shardloom.evidence_field_schema_registry.v1"],
                    ["evidence_schema_registry_surface_count", "1"],
                    ["evidence_schema_registry_field_count", "3"],
                    ["evidence_schema_registry_surface_order", "execution_mode_selection_report"],
                    ["evidence_schema_registry_dtype_vocabulary", "string,boolean,integer"],
                    ["evidence_schema_registry_cardinality_vocabulary", "scalar,list_or_csv"],
                    ["evidence_schema_registry_fallback_attempted", "false"],
                    ["evidence_schema_registry_external_engine_invoked", "false"],
                    ["selected_surface", "execution_mode_selection_report"],
                    ["selected_surface_field_order", "selected_execution_mode,fallback_attempted,required_future_evidence"],
                    ["evidence_schema_surface_execution_mode_selection_report_field_order", "selected_execution_mode,fallback_attempted,required_future_evidence"],
                    ["evidence_schema_surface_execution_mode_selection_report_python_accessor_mapping", "TraditionalAnalyticsRun.execution_mode_selection_fields"],
                    ["evidence_schema_surface_execution_mode_selection_report_required_no_fallback_fields", "fallback_attempted,external_engine_invoked"],
                    ["evidence_schema_field_execution_mode_selection_report_selected_execution_mode_dtype", "string"],
                    ["evidence_schema_field_execution_mode_selection_report_selected_execution_mode_cardinality", "scalar"],
                    ["evidence_schema_field_execution_mode_selection_report_selected_execution_mode_support_state", "schema_declared"],
                    ["evidence_schema_field_execution_mode_selection_report_selected_execution_mode_no_fallback_semantics", "inherits_surface_no_fallback_boundary"],
                    ["evidence_schema_field_execution_mode_selection_report_selected_execution_mode_python_accessor_mapping", "TraditionalAnalyticsRun.execution_mode_selection_fields"],
                    ["evidence_schema_field_execution_mode_selection_report_fallback_attempted_dtype", "boolean"],
                    ["evidence_schema_field_execution_mode_selection_report_fallback_attempted_cardinality", "scalar"],
                    ["evidence_schema_field_execution_mode_selection_report_fallback_attempted_no_fallback_semantics", "must_remain_false"],
                    ["evidence_schema_field_execution_mode_selection_report_fallback_attempted_support_state", "schema_declared"],
                    ["evidence_schema_field_execution_mode_selection_report_fallback_attempted_python_accessor_mapping", "TraditionalAnalyticsRun.execution_mode_selection_fields"],
                    ["evidence_schema_field_execution_mode_selection_report_required_future_evidence_dtype", "string"],
                    ["evidence_schema_field_execution_mode_selection_report_required_future_evidence_cardinality", "list_or_csv"],
                ]
                print(json.dumps({
                    "schema_version": "shardloom.output.v2",
                    "command": "evidence-schema",
                    "status": "success",
                    "summary": "ok",
                    "human_text": "ok",
                    "fallback": {"attempted": False, "allowed": False, "engine": None, "reason": "disabled"},
                    "diagnostics": [],
                    "fields": [{"key": key, "value": value} for key, value in fields],
                }))
                """
            )
        )

        report = ShardLoomClient(binary=binary).evidence_schema(
            "execution_mode_selection_report"
        )

        self.assertIsInstance(report, EvidenceSchemaRegistryReport)
        self.assertEqual(
            report.schema_version, "shardloom.evidence_field_schema_registry.v1"
        )
        self.assertEqual(report.surface_count, 1)
        self.assertEqual(report.field_count, 3)
        self.assertEqual(report.surface_order, ("execution_mode_selection_report",))
        self.assertEqual(report.selected_surface, "execution_mode_selection_report")
        self.assertEqual(
            report.selected_surface_field_order,
            (
                "selected_execution_mode",
                "fallback_attempted",
                "required_future_evidence",
            ),
        )
        self.assertEqual(
            report.field_order_for("execution_mode_selection_report"),
            (
                "selected_execution_mode",
                "fallback_attempted",
                "required_future_evidence",
            ),
        )
        self.assertEqual(
            report.python_accessor_mapping_for("execution_mode_selection_report"),
            "TraditionalAnalyticsRun.execution_mode_selection_fields",
        )
        self.assertEqual(
            report.required_no_fallback_fields_for(
                "execution_mode_selection_report"
            ),
            ("fallback_attempted", "external_engine_invoked"),
        )
        self.assertEqual(
            report.dtype_for(
                "execution_mode_selection_report", "fallback_attempted"
            ),
            "boolean",
        )
        self.assertEqual(
            report.cardinality_for(
                "execution_mode_selection_report", "required_future_evidence"
            ),
            "list_or_csv",
        )
        self.assertEqual(
            report.no_fallback_semantics_for(
                "execution_mode_selection_report", "fallback_attempted"
            ),
            "must_remain_false",
        )
        self.assertEqual(
            report.support_state_for(
                "execution_mode_selection_report", "selected_execution_mode"
            ),
            "schema_declared",
        )
        self.assertEqual(
            report.python_accessor_for(
                "execution_mode_selection_report", "selected_execution_mode"
            ),
            "TraditionalAnalyticsRun.execution_mode_selection_fields",
        )
        self.assertFalse(report.fallback_attempted)
        self.assertFalse(report.external_engine_invoked)

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

    def test_typed_payload_fields_are_primary_field_map(self) -> None:
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
                    "result": {"fields": [{"key": "engine", "value": "typed-result"}]},
                    "result_refs": [],
                    "artifacts": [],
                    "artifact_refs": [],
                    "certificates": [],
                    "policy": {"fields": [{"key": "fallback_execution_allowed", "value": "false"}]},
                    "lifecycle": {"fields": [{"key": "phase", "value": "typed-lifecycle"}]},
                    "capability_snapshot": {"fields": [{"key": "scope", "value": "typed-capability"}]},
                    "fields": [{"key": "engine", "value": "legacy-result"}],
                }))
                """
            )
        )

        result = ShardLoomClient(binary=binary).status()

        self.assertEqual(result.legacy_field_map["engine"], "legacy-result")
        self.assertEqual(result.field_map["engine"], "typed-result")
        self.assertEqual(result.field_map["fallback_execution_allowed"], "false")
        self.assertEqual(result.field_map["phase"], "typed-lifecycle")
        self.assertEqual(result.field_map["scope"], "typed-capability")
        self.assertIs(result.field_map, result.field_map)

    def test_optimizer_plan_typed_view_preserves_report_only_boundaries(self) -> None:
        binary = self.fake_cli(
            textwrap.dedent(
                """
                import json, sys
                assert sys.argv[1:] == ["optimizer-plan", "--format", "json"], sys.argv
                fields = [
                    {"key": "optimizer_trace_id", "value": "optimizer_trace.gar_perf_2b.report_only_registry"},
                    {"key": "optimizer_registry_version", "value": "gar-perf-2b.optimizer_registry.v1"},
                    {"key": "optimizer_phase", "value": "logical"},
                    {"key": "optimizer_rule_order", "value": "predicate_pushdown,common_subplan_source_state_reuse"},
                    {"key": "optimizer_rule_status_vocabulary", "value": "admitted,applied,blocked,unsupported,not_applicable,report_only"},
                    {"key": "benchmark_optimizer_trace_ref", "value": "optimizer-trace://gar-perf-2b.report-only-registry"},
                    {"key": "claim_gate_status", "value": "not_claim_grade"},
                    {"key": "runtime_execution", "value": "false"},
                    {"key": "optimizer_execution", "value": "false"},
                    {"key": "plan_rewritten", "value": "false"},
                    {"key": "optimizer_rule_applied_count", "value": "0"},
                    {"key": "fallback_attempted", "value": "false"},
                    {"key": "fallback_execution_allowed", "value": "false"},
                    {"key": "external_engine_invoked", "value": "false"},
                    {"key": "all_no_fallback_no_external_engine", "value": "true"},
                    {"key": "optimizer_rule_predicate_pushdown_status", "value": "report_only"},
                    {"key": "optimizer_rule_predicate_pushdown_applied", "value": "false"},
                    {"key": "optimizer_rule_common_subplan_source_state_reuse_status", "value": "admitted"},
                    {"key": "optimizer_rule_common_subplan_source_state_reuse_applied", "value": "false"},
                ]
                print(json.dumps({
                    "schema_version": "shardloom.output.v2",
                    "command": "optimizer-plan",
                    "status": "success",
                    "summary": "optimizer",
                    "human_text": "optimizer",
                    "fallback": {"attempted": False, "allowed": False, "engine": None, "reason": "disabled"},
                    "diagnostics": [],
                    "fields": fields,
                }))
                """
            )
        )

        report = ShardLoomClient(binary=binary).optimizer_plan()

        self.assertIsInstance(report, EvidenceAwareOptimizerTraceReport)
        self.assertEqual(report.optimizer_phase, "logical")
        self.assertEqual(report.rule_status("predicate-pushdown"), "report_only")
        self.assertEqual(
            report.rule_status("common_subplan_source_state_reuse"), "admitted"
        )
        self.assertFalse(report.rule_applied("common_subplan_source_state_reuse"))
        self.assertTrue(report.no_runtime)
        self.assertTrue(report.no_rewrite_applied)
        self.assertTrue(report.no_fallback_no_external_engine)

    def test_session_cache_smoke_typed_view_preserves_scoped_runtime_boundaries(
        self,
    ) -> None:
        binary = self.fake_cli(
            textwrap.dedent(
                """
                import json, sys
                assert sys.argv[1:] == ["session-cache-smoke", "--format", "json"], sys.argv
                fields = [
                    {"key": "session_id", "value": "session-cache-smoke-gar-4l-5i"},
                    {"key": "session_runtime_status", "value": "scoped_session_cache_runtime_certified"},
                    {"key": "cache_artifact_order", "value": "source_state,vortex_prepared_state,output_plan,schema_cache,dictionary_cache"},
                    {"key": "invalidation_reason_order", "value": "source_fingerprint_changed,schema_digest_changed,output_artifact_fingerprint_changed"},
                    {"key": "cache_hit_count", "value": "5"},
                    {"key": "cache_miss_count", "value": "8"},
                    {"key": "invalidation_count", "value": "3"},
                    {"key": "buffer_reuse_count", "value": "1"},
                    {"key": "source_state_id", "value": "source-state://session-cache-smoke/local-orders"},
                    {"key": "prepared_state_id", "value": "vortex-prepared-state://session-cache-smoke/local-orders"},
                    {"key": "output_plan_id", "value": "output-plan://session-cache-smoke/local-orders-jsonl"},
                    {"key": "lifecycle_closed_and_cleaned", "value": "true"},
                    {"key": "fallback_attempted", "value": "false"},
                    {"key": "fallback_execution_allowed", "value": "false"},
                    {"key": "external_engine_invoked", "value": "false"},
                    {"key": "no_fallback_no_external_engine", "value": "true"},
                    {"key": "optimizer_trace_id", "value": "optimizer_trace.gar_perf_2b.report_only_registry"},
                    {"key": "optimizer_rule_common_subplan_source_state_reuse_status", "value": "admitted"},
                ]
                print(json.dumps({
                    "schema_version": "shardloom.output.v2",
                    "command": "session-cache-smoke",
                    "status": "success",
                    "summary": "session cache",
                    "human_text": "session cache",
                    "fallback": {"attempted": False, "allowed": False, "engine": None, "reason": "disabled"},
                    "diagnostics": [],
                    "fields": fields,
                }))
                """
            )
        )

        report = ShardLoomClient(binary=binary).session_cache_smoke()

        self.assertIsInstance(report, SessionCacheSmokeReport)
        self.assertEqual(report.session_runtime_status, "scoped_session_cache_runtime_certified")
        self.assertEqual(report.cache_hit_count, 5)
        self.assertEqual(report.cache_miss_count, 8)
        self.assertEqual(report.invalidation_count, 3)
        self.assertEqual(report.buffer_reuse_count, 1)
        self.assertIn("source_state", report.cache_artifact_order)
        self.assertIn("output_artifact_fingerprint_changed", report.invalidation_reason_order)
        self.assertTrue(report.lifecycle_closed_and_cleaned)
        self.assertTrue(report.no_fallback_no_external_engine)
        self.assertEqual(
            report.optimizer_rule_status("common-subplan-source-state-reuse"),
            "admitted",
        )

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
                        {
                            "key": "provider_version",
                            "value": UPSTREAM_VORTEX_PROVIDER_VERSION,
                        },
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
                                {
                                    "key": "evidence_slot_provider_version_refs",
                                    "value": UPSTREAM_VORTEX_PROVIDER_VERSION,
                                },
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
                                {"key": "prepared_native_vortex_lifecycle_status", "value": "prepared_native_vortex_lifecycle_complete_with_output_replay"},
                                {"key": "prepared_native_vortex_lifecycle_output_status", "value": "vortex_result_sink_written_and_replay_verified"},
                                {"key": "prepared_native_vortex_lifecycle_no_standalone_lane", "value": "true"},
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
                "capability_snapshot": {
                    "fields": [
                        {
                            "key": "provider_version",
                            "value": UPSTREAM_VORTEX_PROVIDER_VERSION,
                        }
                    ]
                },
                "fields": [
                    {"key": "plan_id", "value": "plan.count"},
                    {"key": "plan_kind", "value": "vortex_primitive"},
                    {"key": "execution_status", "value": "executed"},
                    {"key": "provider_api_surface", "value": "vortex_local_primitive"},
                    {
                        "key": "provider_version",
                        "value": UPSTREAM_VORTEX_PROVIDER_VERSION,
                    },
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
        self.assertEqual(result.provider_version, UPSTREAM_VORTEX_PROVIDER_VERSION)
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
            result.prepared_native_vortex_lifecycle_status,
            "prepared_native_vortex_lifecycle_complete_with_output_replay",
        )
        self.assertEqual(
            result.prepared_native_vortex_lifecycle_output_status,
            "vortex_result_sink_written_and_replay_verified",
        )
        self.assertTrue(result.prepared_native_vortex_lifecycle_no_standalone_lane)
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

    def test_runtime_execution_envelope_validation_accepts_complete_local_runtime(
        self,
    ) -> None:
        envelope = OutputEnvelope.from_json(
            {
                "schema_version": "shardloom.output.v2",
                "command": "sql-local-source-smoke",
                "status": "success",
                "summary": "sql local source",
                "human_text": "sql local source",
                "fallback": {
                    "attempted": False,
                    "allowed": False,
                    "engine": None,
                    "reason": "disabled",
                },
                "diagnostics": [],
                "result": {
                    "fields": [
                        {"key": "source_state_id", "value": "source-state-1"},
                        {"key": "source_state_digest", "value": "fnv64:source"},
                        {"key": "source_state_materialization_layout", "value": "scalar_row_map"},
                        {"key": "execution_certificate_ref", "value": "sql-local-source.execution.v1"},
                    ]
                },
                "result_refs": [],
                "artifacts": [],
                "artifact_refs": [],
                "certificates": [],
                "policy": {
                    "fields": [
                        {"key": "fallback_attempted", "value": "false"},
                        {"key": "external_engine_invoked", "value": "false"},
                        {"key": "claim_gate_status", "value": "fixture_smoke_only"},
                    ]
                },
                "lifecycle": {"fields": []},
                "capability_snapshot": {"fields": []},
                "fields": [],
            }
        )

        validation = envelope.runtime_execution_validation(
            surface_id="sql_local_source_smoke"
        )

        self.assertTrue(validation.passed)
        self.assertEqual(
            validation.schema_version,
            "shardloom.runtime_execution_envelope_validation.v1",
        )
        self.assertEqual(validation.surface_id, "sql_local_source_smoke")
        self.assertFalse(validation.runtime_claim_allowed)
        self.assertEqual(validation.blockers, ())

    def test_runtime_execution_envelope_validation_blocks_missing_runtime_evidence(
        self,
    ) -> None:
        envelope = OutputEnvelope.from_json(
            {
                "schema_version": "shardloom.output.v2",
                "command": "prepared-run",
                "status": "success",
                "summary": "prepared run",
                "human_text": "prepared run",
                "fallback": {
                    "attempted": False,
                    "allowed": False,
                    "engine": None,
                    "reason": "disabled",
                },
                "diagnostics": [],
                "result": {"fields": [{"key": "execution_mode", "value": "prepared_vortex"}]},
                "result_refs": [],
                "artifacts": [],
                "artifact_refs": [],
                "certificates": [],
                "policy": {
                    "fields": [
                        {"key": "fallback_attempted", "value": "false"},
                        {"key": "external_engine_invoked", "value": "false"},
                        {"key": "claim_gate_status", "value": "fixture_smoke_only"},
                    ]
                },
                "lifecycle": {"fields": []},
                "capability_snapshot": {"fields": []},
                "fields": [],
            }
        )

        validation = envelope.runtime_execution_validation(surface_id="prepared_run")

        self.assertFalse(validation.passed)
        self.assertIn("execution_certificate", validation.missing_fields)
        self.assertIn("route_state_ref", validation.missing_fields)
        self.assertIn("materialization_or_decode_evidence", validation.missing_fields)
        self.assertIn("prepared_state_id", validation.missing_fields)
        self.assertIn("prepared_state_digest", validation.missing_fields)

    def test_runtime_execution_envelope_validation_blocks_certified_timing_drift(
        self,
    ) -> None:
        envelope = OutputEnvelope.from_json(
            {
                "schema_version": "shardloom.output.v2",
                "command": "traditional-analytics-run",
                "status": "success",
                "summary": "compat certified",
                "human_text": "compat certified",
                "fallback": {
                    "attempted": False,
                    "allowed": False,
                    "engine": None,
                    "reason": "disabled",
                },
                "diagnostics": [],
                "result": {
                    "fields": [
                        {"key": "execution_mode", "value": "compatibility_import_certified"},
                        {"key": "source_state_id", "value": "source-state-1"},
                        {"key": "source_state_materialization_layout", "value": "columnar_source_state"},
                        {"key": "execution_certificate_ref", "value": "compat.execution.v1"},
                        {"key": "timing_scope", "value": "warm_query_only"},
                        {"key": "preparation_included", "value": "false"},
                    ]
                },
                "result_refs": [],
                "artifacts": [],
                "artifact_refs": [],
                "certificates": [],
                "policy": {
                    "fields": [
                        {"key": "fallback_attempted", "value": "false"},
                        {"key": "external_engine_invoked", "value": "false"},
                        {"key": "claim_gate_status", "value": "fixture_smoke_only"},
                    ]
                },
                "lifecycle": {"fields": []},
                "capability_snapshot": {"fields": []},
                "fields": [],
            }
        )

        validation = envelope.runtime_execution_validation()

        self.assertFalse(validation.passed)
        self.assertIn("timing_scope", validation.invalid_fields)
        self.assertIn("preparation_included", validation.invalid_fields)

    def test_runtime_execution_envelope_validation_blocks_invalid_no_fallback_flags(
        self,
    ) -> None:
        envelope = OutputEnvelope.from_json(
            {
                "schema_version": "shardloom.output.v2",
                "command": "sql-local-source-smoke",
                "status": "success",
                "summary": "sql local source",
                "human_text": "sql local source",
                "fallback": {
                    "attempted": False,
                    "allowed": False,
                    "engine": None,
                    "reason": "disabled",
                },
                "diagnostics": [],
                "result": {
                    "fields": [
                        {"key": "source_state_id", "value": "source-state-1"},
                        {
                            "key": "source_state_materialization_layout",
                            "value": "scalar_row_map",
                        },
                        {
                            "key": "execution_certificate_ref",
                            "value": "sql-local-source.execution.v1",
                        },
                    ]
                },
                "result_refs": [],
                "artifacts": [],
                "artifact_refs": [],
                "certificates": [],
                "policy": {
                    "fields": [
                        {"key": "fallback_attempted", "value": "maybe"},
                        {"key": "external_engine_invoked", "value": "false"},
                        {"key": "claim_gate_status", "value": "fixture_smoke_only"},
                    ]
                },
                "lifecycle": {"fields": []},
                "capability_snapshot": {"fields": []},
                "fields": [],
            }
        )

        validation = envelope.runtime_execution_validation()

        self.assertFalse(validation.passed)
        self.assertIn("fallback_attempted", validation.invalid_fields)

    def test_runtime_execution_field_validation_accepts_benchmark_aliases(
        self,
    ) -> None:
        validation = validate_runtime_execution_fields(
            {
                "selected_execution_mode": "prepared_vortex",
                "prepared_artifact_ref": "target/fact.vortex|target/dim.vortex",
                "prepared_artifact_digest": "fnv64:prepared",
                "data_decoded": "false",
                "runtime_execution_certificate_id": "execution.prepared-alias-row",
                "runtime_execution_certificate_status": "certified",
                "fallback_attempted": False,
                "external_engine_invoked": False,
                "claim_gate_status": "fixture_smoke_only",
            },
            command="traditional-analytics-benchmark-row",
            surface_id="prepared_alias_row",
            execution_mode="prepared_vortex",
        )

        self.assertTrue(validation.passed)
        self.assertEqual(validation.missing_fields, ())

    def test_runtime_execution_field_validation_accepts_prepared_lifecycle_aliases(
        self,
    ) -> None:
        validation = validate_runtime_execution_fields(
            {
                "selected_execution_mode": "prepared_vortex",
                "prepared_state_id": "prepared-state://lifecycle-row",
                "prepared_state_digest": "fnv1a64:prepared",
                "prepared_native_vortex_lifecycle_materialization_decode_status": (
                    "materialization_decode_boundary_reported"
                ),
                "runtime_execution_certificate_id": "execution.prepared-lifecycle-row",
                "prepared_native_vortex_lifecycle_fallback_attempted": False,
                "prepared_native_vortex_lifecycle_external_engine_invoked": False,
                "prepared_native_vortex_lifecycle_claim_gate_status": "not_claim_grade",
            },
            command="traditional-analytics-benchmark-row",
            surface_id="prepared_lifecycle_row",
            execution_mode="prepared_vortex",
        )

        self.assertTrue(validation.passed)
        self.assertEqual(validation.missing_fields, ())
        self.assertEqual(validation.invalid_fields, ())

    def test_runtime_execution_field_validation_accepts_certified_import_aliases(
        self,
    ) -> None:
        validation = validate_runtime_execution_fields(
            {
                "selected_execution_mode": "compatibility_import_certified",
                "source_state_id": "source-state-fixture",
                "source_state_digest": "fnv64:source",
                "source_state_materialization_layout": "columnar_source_state",
                "runtime_execution_certificate_id": "execution.compat-alias-row",
                "runtime_execution_certificate_status": "certified",
                "timing_scope": "cold_certified_end_to_end",
                "compatibility_import_included": True,
                "fallback_attempted": False,
                "external_engine_invoked": False,
                "claim_gate_status": "not_claim_grade",
            },
            command="traditional-analytics-benchmark-row",
            surface_id="compat_alias_row",
            execution_mode="compatibility_import_certified",
        )

        self.assertTrue(validation.passed)
        self.assertEqual(validation.invalid_fields, ())

    def test_runtime_execution_field_validation_blocks_report_only_runtime_row(
        self,
    ) -> None:
        validation = validate_runtime_execution_fields(
            {
                "runtime_execution": True,
                "support_state": "report_only",
                "source_state_id": "source-state://report-only",
                "data_decoded": False,
                "runtime_execution_certificate_id": "execution.report-only",
                "fallback_attempted": False,
                "external_engine_invoked": False,
                "claim_gate_status": "not_claim_grade",
            },
            command="runs-today-status-row",
            surface_id="report_only_runtime",
        )

        self.assertFalse(validation.passed)
        self.assertIn("runtime_execution", validation.invalid_fields)

    def test_runtime_execution_field_validation_blocks_minimal_runtime_claim_grade(
        self,
    ) -> None:
        validation = validate_runtime_execution_fields(
            {
                "source_state_id": "source-state://minimal-runtime",
                "data_decoded": False,
                "runtime_execution_certificate_id": "execution.minimal-runtime",
                "fallback_attempted": False,
                "external_engine_invoked": False,
                "claim_gate_status": "claim_grade",
                "selected_evidence_level": "minimal_runtime",
            },
            command="traditional-analytics-benchmark-row",
            surface_id="minimal_runtime_claim_grade",
        )

        self.assertFalse(validation.passed)
        self.assertIn("evidence_level", validation.invalid_fields)

    def test_runtime_execution_field_validation_requires_execution_cert_ref(
        self,
    ) -> None:
        validation = validate_runtime_execution_fields(
            {
                "source_state_id": "source-state://refs-only",
                "data_decoded": False,
                "evidence_level_certificate_refs": "execution_certificate_status",
                "fallback_attempted": False,
                "external_engine_invoked": False,
                "claim_gate_status": "fixture_smoke_only",
            },
            command="traditional-analytics-benchmark-row",
            surface_id="evidence_level_refs_only",
        )

        self.assertFalse(validation.passed)
        self.assertIn("execution_certificate", validation.missing_fields)

    def test_runtime_execution_field_validation_blocks_claim_grade_without_requirements(
        self,
    ) -> None:
        validation = validate_runtime_execution_fields(
            {
                "source_state_id": "source-state://claim-grade",
                "data_decoded": False,
                "runtime_execution_certificate_id": "execution.claim-grade",
                "runtime_execution_certificate_status": "certified",
                "fallback_attempted": False,
                "external_engine_invoked": False,
                "claim_gate_status": "claim_grade",
                "claim_grade_requirements_met": False,
            },
            command="traditional-analytics-benchmark-row",
            surface_id="claim_grade_without_requirements",
        )

        self.assertFalse(validation.passed)
        self.assertIn("claim_grade_requirements_met", validation.invalid_fields)

    def test_runtime_execution_field_validation_accepts_claim_grade_with_requirements(
        self,
    ) -> None:
        validation = validate_runtime_execution_fields(
            {
                "source_state_id": "source-state://claim-grade",
                "data_decoded": False,
                "runtime_execution_certificate_id": "execution.claim-grade",
                "runtime_execution_certificate_status": "certified",
                "fallback_attempted": False,
                "external_engine_invoked": False,
                "claim_gate_status": "claim_grade",
                "claim_grade_requirements_met": True,
            },
            command="traditional-analytics-benchmark-row",
            surface_id="claim_grade_with_requirements",
        )

        self.assertTrue(validation.passed)
        self.assertTrue(validation.runtime_claim_allowed)

    def test_runtime_execution_field_validation_blocks_certified_level_without_cert_status(
        self,
    ) -> None:
        validation = validate_runtime_execution_fields(
            {
                "source_state_id": "source-state://certified-level",
                "data_decoded": False,
                "runtime_execution_certificate_id": "execution.certified-level",
                "fallback_attempted": False,
                "external_engine_invoked": False,
                "claim_gate_status": "not_claim_grade",
                "evidence_level": "certified",
            },
            command="traditional-analytics-benchmark-row",
            surface_id="certified_level_missing_status",
        )

        self.assertFalse(validation.passed)
        self.assertIn("execution_certificate_status", validation.invalid_fields)

    def test_runtime_execution_validation_ignores_certified_non_execution_certificate(
        self,
    ) -> None:
        envelope = OutputEnvelope.from_json(
            {
                "schema_version": "shardloom.output.v2",
                "command": "runtime-field-mapping",
                "status": "success",
                "summary": "runtime field mapping",
                "human_text": "runtime field mapping",
                "fallback": {
                    "attempted": False,
                    "allowed": False,
                    "engine": None,
                    "reason": "disabled",
                },
                "diagnostics": [],
                "result": {
                    "fields": [
                        {"key": "source_state_id", "value": "source-state://certified-level"},
                        {"key": "data_decoded", "value": "false"},
                        {"key": "fallback_attempted", "value": "false"},
                        {"key": "external_engine_invoked", "value": "false"},
                        {"key": "claim_gate_status", "value": "not_claim_grade"},
                        {"key": "evidence_level", "value": "certified"},
                    ]
                },
                "result_refs": [],
                "artifacts": [],
                "artifact_refs": [],
                "certificates": [
                    {
                        "id": "native_io.fixture",
                        "kind": "native_io_certificate",
                        "status": "certified",
                        "uri": None,
                    }
                ],
                "policy": {"fields": []},
                "lifecycle": {"fields": []},
                "capability_snapshot": {"fields": []},
                "fields": [],
            }
        )

        validation = envelope.runtime_execution_validation(
            surface_id="certified_non_execution_certificate"
        )

        self.assertFalse(validation.passed)
        self.assertIn("execution_certificate_status", validation.invalid_fields)
        self.assertIn("execution_certificate", validation.missing_fields)

    def test_runtime_execution_field_validation_treats_empty_cert_refs_as_missing(
        self,
    ) -> None:
        validation = validate_runtime_execution_fields(
            {
                "source_state_id": "source-state://empty-cert-refs",
                "data_decoded": False,
                "execution_certificate_refs": "[]",
                "fallback_attempted": False,
                "external_engine_invoked": False,
                "claim_gate_status": "fixture_smoke_only",
            },
            command="traditional-analytics-benchmark-row",
            surface_id="empty_cert_refs",
        )

        self.assertFalse(validation.passed)
        self.assertIn("execution_certificate", validation.missing_fields)

    def test_runtime_execution_field_validation_blocks_non_success_runtime_expected(
        self,
    ) -> None:
        validation = validate_runtime_execution_fields(
            {
                "source_state_id": "source-state://unsupported",
                "data_decoded": False,
                "runtime_execution_certificate_id": "execution.unsupported",
                "runtime_execution_certificate_status": "certified",
                "fallback_attempted": False,
                "external_engine_invoked": False,
                "claim_gate_status": "not_claim_grade",
            },
            command="traditional-analytics-benchmark-row",
            status="unsupported",
            surface_id="unsupported_runtime_expected",
            runtime_expected=True,
        )

        self.assertFalse(validation.passed)
        self.assertIn("status", validation.invalid_fields)

    def test_runtime_execution_field_validation_blocks_full_replay_without_replay_proof(
        self,
    ) -> None:
        validation = validate_runtime_execution_fields(
            {
                "source_state_id": "source-state://full-replay",
                "data_decoded": False,
                "runtime_execution_certificate_id": "execution.full-replay",
                "runtime_execution_certificate_status": "certified",
                "fallback_attempted": False,
                "external_engine_invoked": False,
                "claim_gate_status": "not_claim_grade",
                "evidence_level": "full_replay",
            },
            command="traditional-analytics-benchmark-row",
            surface_id="full_replay_missing_replay",
        )

        self.assertFalse(validation.passed)
        self.assertIn("result_sink_replay_verified", validation.invalid_fields)
        self.assertIn("result_sink_replay_ref", validation.missing_fields)

    def test_runtime_execution_field_validation_accepts_full_replay_with_replay_proof(
        self,
    ) -> None:
        validation = validate_runtime_execution_fields(
            {
                "source_state_id": "source-state://full-replay",
                "data_decoded": False,
                "runtime_execution_certificate_id": "execution.full-replay",
                "runtime_execution_certificate_status": "certified",
                "fallback_attempted": False,
                "external_engine_invoked": False,
                "claim_gate_status": "not_claim_grade",
                "evidence_level": "full_replay",
                "evidence_level_result_sink_replay_verified": True,
                "evidence_level_result_sink_replay_refs": "result-sink://fixture",
            },
            command="traditional-analytics-benchmark-row",
            surface_id="full_replay_with_replay",
        )

        self.assertTrue(validation.passed)
        self.assertEqual(validation.missing_fields, ())

    def test_runtime_execution_field_validation_blocks_incomplete_split_operator_proof(
        self,
    ) -> None:
        validation = validate_runtime_execution_fields(
            {
                "prepared_state_id": "prepared-state://split-operator",
                "prepared_state_digest": "fnv1a64:prepared",
                "data_decoded": False,
                "runtime_execution_certificate_id": "execution.split-operator",
                "runtime_execution_certificate_status": "certified",
                "fallback_attempted": False,
                "external_engine_invoked": False,
                "claim_gate_status": "not_claim_grade",
                "prepared_vortex_scale_split_operator_runtime_status": (
                    "local_split_operator_runtime_certified"
                ),
            },
            command="traditional-analytics-benchmark-row",
            surface_id="split_operator_incomplete",
            execution_mode="prepared_vortex",
        )

        self.assertFalse(validation.passed)
        self.assertIn(
            "prepared_vortex_scale_split_operator_family", validation.missing_fields
        )

    def test_runtime_execution_field_validation_accepts_complete_split_operator_proof(
        self,
    ) -> None:
        validation = validate_runtime_execution_fields(
            {
                "prepared_state_id": "prepared-state://split-operator",
                "prepared_state_digest": "fnv1a64:prepared",
                "data_decoded": False,
                "runtime_execution_certificate_id": "execution.split-operator",
                "runtime_execution_certificate_status": "certified",
                "fallback_attempted": False,
                "external_engine_invoked": False,
                "claim_gate_status": "not_claim_grade",
                "prepared_vortex_scale_split_operator_runtime_status": (
                    "local_split_operator_runtime_certified"
                ),
                "prepared_vortex_scale_split_operator_family": "stateful_hash_aggregate",
                "prepared_vortex_scale_split_operator_stateful": True,
                "prepared_vortex_scale_split_operator_shuffle_required": True,
                "prepared_vortex_scale_split_operator_local_combine_used": True,
                "prepared_vortex_scale_split_operator_global_merge_used": True,
                "prepared_vortex_scale_split_operator_retry_replay_status": (
                    "verified_idempotent_stateful_shuffle_split_operator_replay"
                ),
                "prepared_vortex_scale_split_operator_source_replay_status": (
                    "prepared_vortex_source_replay_verified"
                ),
                "prepared_vortex_scale_split_operator_memory_envelope_status": (
                    "declared_local_memory_envelope_admitted"
                ),
                "prepared_vortex_scale_split_operator_backpressure_status": (
                    "bounded_by_reader_chunk_scheduler_and_declared_parallelism"
                ),
                "prepared_vortex_scale_split_operator_spill_policy_status": (
                    "larger_than_memory_spill_io_not_required_for_local_runtime_envelope"
                ),
                "prepared_vortex_scale_split_operator_output_commit_proof_status": (
                    "result_sink_replay_verified_for_split_operator"
                ),
                "prepared_vortex_scale_split_operator_execution_certificate_status": (
                    "certified"
                ),
                "prepared_vortex_scale_split_operator_execution_certificate_id": (
                    "p746.prepared_vortex_local_split_operator.group-by-aggregation."
                    "stateful_hash_aggregate"
                ),
                "prepared_vortex_scale_split_operator_claim_gate_status": (
                    "local_split_operator_runtime_certified"
                ),
                "prepared_vortex_scale_split_operator_fallback_attempted": False,
                "prepared_vortex_scale_split_operator_external_engine_invoked": False,
            },
            command="traditional-analytics-benchmark-row",
            surface_id="split_operator_complete",
            execution_mode="prepared_vortex",
        )

        self.assertTrue(validation.passed)
        self.assertEqual(validation.missing_fields, ())

    def test_runtime_execution_field_validation_blocks_incomplete_pulseweave_proof(
        self,
    ) -> None:
        fields = _complete_pulseweave_runtime_fields()
        del fields["flow_inventory_wip_limit"]

        validation = validate_runtime_execution_fields(
            fields,
            command="traditional-analytics-benchmark-row",
            surface_id="pulseweave_incomplete",
            execution_mode="prepared_vortex",
        )

        self.assertFalse(validation.passed)
        self.assertIn("flow_inventory_wip_limit", validation.missing_fields)

    def test_runtime_execution_field_validation_blocks_pulseweave_without_certified_proof(
        self,
    ) -> None:
        fields = _complete_pulseweave_runtime_fields()
        fields["runtime_execution_certificate_status"] = "evidence_incomplete"
        fields["proofbound_certificate_status"] = "evidence_incomplete"

        validation = validate_runtime_execution_fields(
            fields,
            command="traditional-analytics-benchmark-row",
            surface_id="pulseweave_missing_certificate",
            execution_mode="prepared_vortex",
        )

        self.assertFalse(validation.passed)
        self.assertIn("execution_certificate_status", validation.invalid_fields)
        self.assertIn("proofbound_certificate_status", validation.invalid_fields)

    def test_runtime_execution_field_validation_accepts_complete_pulseweave_proof(
        self,
    ) -> None:
        validation = validate_runtime_execution_fields(
            _complete_pulseweave_runtime_fields(),
            command="traditional-analytics-benchmark-row",
            surface_id="pulseweave_complete",
            execution_mode="prepared_vortex",
        )

        self.assertTrue(validation.passed)
        self.assertEqual(validation.missing_fields, ())
        self.assertEqual(validation.invalid_fields, ())

    def test_runtime_execution_field_validation_accepts_route_correctness_digest_for_pulseweave(
        self,
    ) -> None:
        fields = _complete_pulseweave_runtime_fields()
        del fields["prepared_vortex_scale_correctness_digest"]
        fields["correctness_digest"] = "fnv1a64:route-correct"

        validation = validate_runtime_execution_fields(
            fields,
            command="traditional-analytics-benchmark-row",
            surface_id="pulseweave_route_correctness_digest",
            execution_mode="prepared_vortex",
        )

        self.assertTrue(validation.passed)
        self.assertNotIn(
            "pulseweave_correctness_output_digest", validation.missing_fields
        )

    def test_sql_local_source_report_result_rows_validate_jsonl_objects(self) -> None:
        def report_for(result_jsonl: str) -> SqlLocalSourceSmokeReport:
            envelope = OutputEnvelope.from_json(
                {
                    "schema_version": "shardloom.output.v2",
                    "command": "sql-local-source-smoke",
                    "status": "success",
                    "summary": "sql local source",
                    "human_text": "sql local source",
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
                    "fields": [{"key": "result_jsonl", "value": result_jsonl}],
                }
            )
            return SqlLocalSourceSmokeReport(envelope)

        valid = report_for('{"id":1,"label":"alpha"}\n\n{"id":2,"label":"beta"}\n')
        self.assertEqual(
            valid.result_rows,
            ({"id": 1, "label": "alpha"}, {"id": 2, "label": "beta"}),
        )
        self.assertEqual(valid.first_result_row, {"id": 1, "label": "alpha"})

        with self.assertRaisesRegex(ShardLoomProtocolError, "invalid JSONL at line 1"):
            _ = report_for("not-json\n").result_rows

        with self.assertRaisesRegex(ShardLoomProtocolError, "line 1 is not a JSON object"):
            _ = report_for("1\n").result_rows

    def test_sql_local_source_report_preserves_csv_escaped_like_escape_character(
        self,
    ) -> None:
        envelope = OutputEnvelope.from_json(
            {
                "schema_version": "shardloom.output.v2",
                "command": "sql-local-source-smoke",
                "status": "success",
                "summary": "sql local source",
                "human_text": "sql local source",
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
                "fields": [
                    {
                        "key": "string_predicate_like_escape_character",
                        "value": "\",\"",
                    },
                ],
            }
        )
        report = SqlLocalSourceSmokeReport(envelope)

        self.assertEqual(report.string_predicate_like_escape_character, (",",))

    def test_sql_local_source_report_hides_absent_like_escape_character(
        self,
    ) -> None:
        envelope = OutputEnvelope.from_json(
            {
                "schema_version": "shardloom.output.v2",
                "command": "sql-local-source-smoke",
                "status": "success",
                "summary": "sql local source",
                "human_text": "sql local source",
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
                "fields": [
                    {
                        "key": "string_predicate_like_escape_character",
                        "value": "not_applicable",
                    },
                ],
            }
        )
        report = SqlLocalSourceSmokeReport(envelope)

        self.assertEqual(report.string_predicate_like_escape_character, ())

    def test_sql_local_source_report_hides_absent_sink_artifact_refs(self) -> None:
        envelope = OutputEnvelope.from_json(
            {
                "schema_version": "shardloom.output.v2",
                "command": "sql-local-source-smoke",
                "status": "success",
                "summary": "sql local source",
                "human_text": "sql local source",
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
                "fields": [
                    {"key": "sink_artifact_ref", "value": "not_applicable"},
                    {"key": "sink_artifact_refs", "value": "not_applicable,not_requested,none"},
                    {"key": "sink_artifact_digest", "value": "not_requested"},
                    {"key": "sink_artifact_digests", "value": "not_applicable,not_requested"},
                    {
                        "key": "output_plan_required_columns",
                        "value": "not_applicable_inline_result,not_applicable_id",
                    },
                ],
            }
        )
        report = SqlLocalSourceSmokeReport(envelope)

        self.assertIsNone(report.sink_artifact_ref)
        self.assertEqual(report.sink_artifact_refs, ())
        self.assertIsNone(report.sink_artifact_digest)
        self.assertEqual(report.sink_artifact_digests, ())
        self.assertEqual(report.output_plan_required_columns, ("not_applicable_id",))

    def test_sql_local_source_report_preserves_sentinel_like_sink_artifact_refs(self) -> None:
        envelope = OutputEnvelope.from_json(
            {
                "schema_version": "shardloom.output.v2",
                "command": "sql-local-source-smoke",
                "status": "success",
                "summary": "sql local source",
                "human_text": "sql local source",
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
                "fields": [
                    {
                        "key": "sink_artifact_refs",
                        "value": (
                            "jsonl:not_applicable_output.jsonl,"
                            "csv:target/not_applicable_output.csv,"
                            "not_applicable_inline_result,not_requested"
                        ),
                    },
                    {
                        "key": "sink_artifact_digests",
                        "value": (
                            "jsonl:not_applicable_digest,"
                            "csv:not_applicable_digest_csv,"
                            "not_applicable_inline_result,none"
                        ),
                    },
                ],
            }
        )
        report = SqlLocalSourceSmokeReport(envelope)

        self.assertEqual(
            report.sink_artifact_refs,
            (
                "jsonl:not_applicable_output.jsonl",
                "csv:target/not_applicable_output.csv",
            ),
        )
        self.assertEqual(
            report.sink_artifact_digests,
            ("jsonl:not_applicable_digest", "csv:not_applicable_digest_csv"),
        )

    def test_sql_local_source_report_window_evidence_accessors(self) -> None:
        envelope = OutputEnvelope.from_json(
            {
                "schema_version": "shardloom.output.v2",
                "command": "sql-local-source-smoke",
                "status": "success",
                "summary": "sql local source",
                "human_text": "sql local source",
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
                "fields": [
                    {"key": "result_jsonl", "value": "{}\n"},
                    {"key": "window_partition_columns", "value": "none,category"},
                    {"key": "window_value_columns", "value": "label,label"},
                    {"key": "window_offset_rows", "value": "1,not_applicable,2"},
                    {"key": "window_bucket_counts", "value": "4,none,not_applicable"},
                    {"key": "window_lag_runtime_execution", "value": "true"},
                    {"key": "window_lead_runtime_execution", "value": "true"},
                    {"key": "window_ntile_runtime_execution", "value": "true"},
                    {"key": "window_percent_rank_runtime_execution", "value": "true"},
                    {"key": "window_cume_dist_runtime_execution", "value": "true"},
                ],
            }
        )
        report = SqlLocalSourceSmokeReport(envelope)

        self.assertEqual(report.window_value_columns, ("label", "label"))
        self.assertEqual(report.window_partition_columns, ("none", "category"))
        self.assertEqual(report.window_offset_rows, (1, 2))
        self.assertEqual(report.window_bucket_counts, (4,))
        self.assertTrue(report.window_lag_runtime_execution)
        self.assertTrue(report.window_lead_runtime_execution)
        self.assertTrue(report.window_ntile_runtime_execution)
        self.assertTrue(report.window_percent_rank_runtime_execution)
        self.assertTrue(report.window_cume_dist_runtime_execution)

    def test_vortex_ingest_smoke_helper_dispatches_prepare_once_route(self) -> None:
        binary = self.fake_cli(
            textwrap.dedent(
                """
                import json, sys
                assert sys.argv[1:] == [
                    "vortex-ingest-smoke",
                    "target/source.csv",
                    "target/source.vortex",
                    "--input-format",
                    "csv",
                    "--allow-overwrite",
                    "--format",
                    "json",
                ], sys.argv
                print(json.dumps({
                    "schema_version": "shardloom.output.v2",
                    "command": "vortex-ingest-smoke",
                    "status": "success",
                    "summary": "ok",
                    "human_text": "ok",
                    "fallback": {"attempted": False, "allowed": False, "engine": None, "reason": "disabled"},
                    "diagnostics": [],
                    "fields": [
                        {"key": "source_path", "value": "target/source.csv"},
                        {"key": "target_vortex_path", "value": "target/source.vortex"},
                        {"key": "vortex_ingest_output_workspace_path_safety_status", "value": "enforced"},
                        {"key": "vortex_ingest_output_commit_status", "value": "committed"},
                        {"key": "source_format", "value": "csv"},
                        {"key": "source_adapter_id", "value": "local_csv_input_adapter"},
                        {"key": "source_adapter_registry_entry_id", "value": "shardloom.local_input_adapter.csv.v1"},
                        {"key": "source_adapter_feature_gate", "value": "default"},
                        {"key": "source_adapter_boundary", "value": "local_text_source_state_adapter"},
                        {"key": "vortex_ingest_status", "value": "prepared_state_created"},
                        {"key": "prepared_state_id", "value": "vortex-prepared-state-fnv64-abc"},
                        {"key": "prepared_state_digest", "value": "fnv64:abc"},
                        {"key": "vortex_artifact_digest", "value": "fnv64:def"},
                        {"key": "input_row_count", "value": "2"},
                        {"key": "writer_row_count", "value": "2"},
                        {"key": "reopen_row_count", "value": "2"},
                        {"key": "reopen_verification_status", "value": "reopen_row_count_verified"},
                        {"key": "certification_level", "value": "ingest_certified"},
                        {"key": "certification_status", "value": "fixture_smoke_certified"},
                        {"key": "source_state_materialization_layout", "value": "arrow_record_batch_columnar_source_state"},
                        {"key": "source_state_parse_normalization", "value": "structured_reader_to_arrow_record_batches"},
                        {"key": "source_state_columnar_preserved", "value": "true"},
                        {"key": "source_state_record_batch_count", "value": "1"},
                        {"key": "source_to_columnar_millis", "value": "4"},
                        {"key": "vortex_array_build_millis", "value": "3"},
                        {"key": "vortex_array_build_provider_kind", "value": "vortex_array_kernel"},
                        {"key": "vortex_array_build_provider_surface", "value": "ArrayRef::from_arrow(RecordBatch)"},
                        {"key": "vortex_array_build_strategy", "value": "vortex_from_arrow_record_batch"},
                        {"key": "vortex_array_build_input_layout", "value": "arrow_record_batch_columnar_source_state"},
                        {"key": "vortex_array_build_record_batch_count", "value": "1"},
                        {"key": "vortex_array_build_manual_scalar_copy_avoided", "value": "true"},
                        {"key": "vortex_preparation_spine_status", "value": "admitted_local_preparation_spine"},
                        {"key": "vortex_preparation_spine_vortex_first_decision", "value": "use_vortex_native_provider"},
                        {"key": "vortex_preparation_spine_provider_kind", "value": "vortex_array_kernel"},
                        {"key": "vortex_preparation_spine_provider_api_surface", "value": "ArrayRef::from_arrow(RecordBatch);VortexSession::write_options().write(ArrayStream);VortexSession::open_options().open_buffer(...).scan().into_array_stream().read_all()"},
                        {"key": "vortex_preparation_spine_source_split_count", "value": "1"},
                        {"key": "vortex_preparation_spine_source_split_refs", "value": "source-state://abc:split=1:bytes=0..64:rows=0..2"},
                        {"key": "vortex_preparation_spine_source_byte_range_refs", "value": "source-state://abc:split=1:bytes=0..64"},
                        {"key": "vortex_preparation_spine_source_row_range_refs", "value": "source-state://abc:split=1:rows=0..2"},
                        {"key": "vortex_preparation_spine_native_io_certificate_status", "value": "certified_local_vortex_preparation_spine"},
                        {"key": "vortex_scout_ingress_status", "value": "admitted_scout_ingress_clean"},
                        {"key": "vortex_scout_ingress_anomaly_count", "value": "0"},
                        {"key": "vortex_scout_ingress_anomaly_families", "value": "none"},
                        {"key": "vortex_scout_ingress_schema_drift_status", "value": "not_detected_no_prior_schema_baseline"},
                        {"key": "vortex_scout_ingress_unsupported_shape_status", "value": "not_detected"},
                        {"key": "vortex_scout_ingress_quarantine_required", "value": "false"},
                        {"key": "vortex_scout_ingress_quarantine_output_plan_status", "value": "not_required"},
                        {"key": "vortex_scout_ingress_unsupported_diagnostic_code", "value": "none"},
                        {"key": "vortex_scout_ingress_no_standalone_lane_status", "value": "funnelled_through_vortex_ingest_source_state_to_vortex_prepared_state"},
                        {"key": "vortex_layout_write_advisor_status", "value": "admitted_local_layout_write_strategy"},
                        {"key": "vortex_layout_write_advisor_strategy_admitted", "value": "true"},
                        {"key": "vortex_layout_write_advisor_runtime_decision_applied", "value": "true"},
                        {"key": "vortex_layout_write_advisor_selected_strategy", "value": "single_local_vortex_artifact"},
                        {"key": "vortex_layout_write_advisor_strategy_decision_digest", "value": "fnv64:layout"},
                        {"key": "vortex_layout_write_advisor_provider_admitted", "value": "true"},
                        {"key": "vortex_layout_write_advisor_blocker", "value": "none"},
                        {"key": "vortex_layout_write_advisor_layout_strategy", "value": "single_local_vortex_artifact"},
                        {"key": "vortex_layout_write_advisor_no_standalone_lane_status", "value": "funnelled_through_vortex_ingest_source_state_to_vortex_prepared_state"},
                        {"key": "vortex_capillary_preparation_status", "value": "applied_capillary_pulseweave_control"},
                        {"key": "vortex_capillary_preparation_activation_result", "value": "activated"},
                        {"key": "vortex_capillary_preparation_activation_reason", "value": "capillary_claim_evidence_requested"},
                        {"key": "vortex_capillary_preparation_activation_observed_split_count", "value": "1"},
                        {"key": "vortex_capillary_preparation_task_count", "value": "6"},
                        {"key": "vortex_capillary_preparation_task_roles", "value": "source_split_discovery,read_chunk,columnarize_encode,vortex_segment_write,reopen_verify,sink_evidence"},
                        {"key": "vortex_capillary_preparation_execution_window_count", "value": "3"},
                        {"key": "vortex_capillary_preparation_execution_window_ids", "value": "vortex-capillary-window-0000;vortex-capillary-window-0001;vortex-capillary-window-0002"},
                        {"key": "vortex_capillary_preparation_scheduler_applied", "value": "true"},
                        {"key": "vortex_capillary_preparation_scheduler_application_reason", "value": "pulseweave_batch_window_applied_to_capillary_manifest"},
                        {"key": "vortex_capillary_preparation_prewrite_status", "value": "applied_before_array_build"},
                        {"key": "vortex_capillary_preparation_prewrite_scheduler_applied", "value": "true"},
                        {"key": "vortex_capillary_preparation_prewrite_execution_window_count", "value": "3"},
                        {"key": "vortex_capillary_preparation_prewrite_execution_window_ids", "value": "vortex-capillary-window-0000;vortex-capillary-window-0001;vortex-capillary-window-0002"},
                        {"key": "vortex_capillary_preparation_prewrite_array_build_gate_status", "value": "applied_prewrite_window"},
                        {"key": "vortex_capillary_preparation_prewrite_write_gate_status", "value": "applied_prewrite_window"},
                        {"key": "vortex_capillary_preparation_prewrite_reopen_gate_status", "value": "applied_prewrite_window"},
                        {"key": "vortex_capillary_preparation_prewrite_sink_evidence_gate_status", "value": "applied_prewrite_window"},
                        {"key": "vortex_capillary_preparation_prewrite_fallback_attempted", "value": "false"},
                        {"key": "vortex_capillary_preparation_prewrite_external_engine_invoked", "value": "false"},
                        {"key": "vortex_capillary_preparation_native_io_certificate_status", "value": "certified"},
                        {"key": "vortex_capillary_preparation_pulseweave_status", "value": "applied"},
                        {"key": "vortex_capillary_preparation_pulseweave_runtime_decision_applied", "value": "true"},
                        {"key": "vortex_capillary_preparation_pulseweave_decision_digest", "value": "fnv64:pulse"},
                        {"key": "vortex_capillary_preparation_proofbound_claim_allowed", "value": "true"},
                        {"key": "vortex_capillary_preparation_no_standalone_lane_status", "value": "funnelled_through_vortex_ingest_source_state_to_vortex_prepared_state"},
                        {"key": "vortex_copy_budget_status", "value": "reported_copy_budget_with_unmeasured_segments"},
                        {"key": "vortex_copy_budget_measurement_status", "value": "reported_with_not_measured_segments"},
                        {"key": "vortex_copy_budget_buffer_reuse_status", "value": "blocked_until_correctness_parity"},
                        {"key": "vortex_copy_budget_unsafe_lifetime_shortcut_status", "value": "blocked_no_unsafe_lifetime_shortcuts"},
                        {"key": "vortex_copy_budget_no_standalone_lane_status", "value": "funnelled_through_vortex_ingest_source_state_to_vortex_prepared_state"},
                        {"key": "source_io_performed", "value": "true"},
                        {"key": "prepared_state_created", "value": "true"},
                        {"key": "claim_gate_status", "value": "fixture_smoke_only"},
                        {"key": "fallback_attempted", "value": "false"},
                        {"key": "external_engine_invoked", "value": "false"}
                    ],
                }))
                """
            )
        )

        result = ShardLoomClient(binary=binary).vortex_ingest_smoke(
            "target/source.csv",
            "target/source.vortex",
            input_format="csv",
            allow_overwrite=True,
        )

        self.assertIsInstance(result, VortexIngestSmokeReport)
        self.assertEqual(result.source_path, "target/source.csv")
        self.assertEqual(result.target_vortex_path, "target/source.vortex")
        self.assertEqual(result.workspace_path_safety_status, "enforced")
        self.assertEqual(result.output_commit_status, "committed")
        self.assertEqual(result.source_format, "csv")
        self.assertEqual(result.source_adapter_id, "local_csv_input_adapter")
        self.assertEqual(
            result.source_adapter_registry_entry_id,
            "shardloom.local_input_adapter.csv.v1",
        )
        self.assertEqual(result.source_adapter_feature_gate, "default")
        self.assertEqual(
            result.source_adapter_boundary,
            "local_text_source_state_adapter",
        )
        self.assertEqual(result.vortex_ingest_status, "prepared_state_created")
        self.assertEqual(result.prepared_state_id, "vortex-prepared-state-fnv64-abc")
        self.assertEqual(result.prepared_state_digest, "fnv64:abc")
        self.assertEqual(result.vortex_artifact_digest, "fnv64:def")
        self.assertEqual(result.input_row_count, 2)
        self.assertEqual(result.writer_row_count, 2)
        self.assertEqual(result.reopen_row_count, 2)
        self.assertEqual(result.reopen_verification_status, "reopen_row_count_verified")
        self.assertEqual(result.certification_level, "ingest_certified")
        self.assertEqual(result.certification_status, "fixture_smoke_certified")
        self.assertEqual(
            result.source_state_materialization_layout,
            "arrow_record_batch_columnar_source_state",
        )
        self.assertEqual(
            result.source_state_parse_normalization,
            "structured_reader_to_arrow_record_batches",
        )
        self.assertTrue(result.source_state_columnar_preserved)
        self.assertEqual(result.source_state_record_batch_count, 1)
        self.assertEqual(result.source_to_columnar_millis, 4)
        self.assertEqual(result.vortex_array_build_millis, 3)
        self.assertEqual(result.vortex_array_build_provider_kind, "vortex_array_kernel")
        self.assertEqual(
            result.vortex_array_build_provider_surface,
            "ArrayRef::from_arrow(RecordBatch)",
        )
        self.assertEqual(
            result.vortex_array_build_strategy,
            "vortex_from_arrow_record_batch",
        )
        self.assertEqual(
            result.vortex_array_build_input_layout,
            "arrow_record_batch_columnar_source_state",
        )
        self.assertEqual(result.vortex_array_build_record_batch_count, 1)
        self.assertTrue(result.vortex_array_build_manual_scalar_copy_avoided)
        self.assertEqual(
            result.vortex_preparation_spine_status,
            "admitted_local_preparation_spine",
        )
        self.assertEqual(
            result.vortex_preparation_spine_vortex_first_decision,
            "use_vortex_native_provider",
        )
        self.assertEqual(
            result.vortex_preparation_spine_provider_kind,
            "vortex_array_kernel",
        )
        self.assertIn(
            "ArrayRef::from_arrow(RecordBatch)",
            result.vortex_preparation_spine_provider_api_surface or "",
        )
        self.assertEqual(result.vortex_preparation_spine_source_split_count, 1)
        self.assertEqual(
            result.vortex_preparation_spine_source_split_refs,
            ("source-state://abc:split=1:bytes=0..64:rows=0..2",),
        )
        self.assertEqual(
            result.vortex_preparation_spine_source_byte_range_refs,
            ("source-state://abc:split=1:bytes=0..64",),
        )
        self.assertEqual(
            result.vortex_preparation_spine_source_row_range_refs,
            ("source-state://abc:split=1:rows=0..2",),
        )
        self.assertEqual(
            result.vortex_preparation_spine_native_io_certificate_status,
            "certified_local_vortex_preparation_spine",
        )
        self.assertEqual(
            result.vortex_scout_ingress_status,
            "admitted_scout_ingress_clean",
        )
        self.assertEqual(result.vortex_scout_ingress_anomaly_count, 0)
        self.assertEqual(result.vortex_scout_ingress_anomaly_families, ())
        self.assertEqual(
            result.vortex_scout_ingress_schema_drift_status,
            "not_detected_no_prior_schema_baseline",
        )
        self.assertEqual(
            result.vortex_scout_ingress_unsupported_shape_status,
            "not_detected",
        )
        self.assertFalse(result.vortex_scout_ingress_quarantine_required)
        self.assertEqual(
            result.vortex_scout_ingress_quarantine_output_plan_status,
            "not_required",
        )
        self.assertEqual(
            result.vortex_scout_ingress_unsupported_diagnostic_code,
            "none",
        )
        self.assertEqual(
            result.vortex_scout_ingress_no_standalone_lane_status,
            "funnelled_through_vortex_ingest_source_state_to_vortex_prepared_state",
        )
        self.assertEqual(
            result.vortex_layout_write_advisor_status,
            "admitted_local_layout_write_strategy",
        )
        self.assertTrue(result.vortex_layout_write_advisor_strategy_admitted)
        self.assertTrue(result.vortex_layout_write_advisor_runtime_decision_applied)
        self.assertEqual(
            result.vortex_layout_write_advisor_selected_strategy,
            "single_local_vortex_artifact",
        )
        self.assertEqual(
            result.vortex_layout_write_advisor_strategy_decision_digest,
            "fnv64:layout",
        )
        self.assertTrue(result.vortex_layout_write_advisor_provider_admitted)
        self.assertEqual(result.vortex_layout_write_advisor_blocker, "none")
        self.assertEqual(
            result.vortex_layout_write_advisor_layout_strategy,
            "single_local_vortex_artifact",
        )
        self.assertEqual(
            result.vortex_layout_write_advisor_no_standalone_lane_status,
            "funnelled_through_vortex_ingest_source_state_to_vortex_prepared_state",
        )
        self.assertEqual(
            result.vortex_capillary_preparation_status,
            "applied_capillary_pulseweave_control",
        )
        self.assertEqual(
            result.vortex_capillary_preparation_activation_result,
            "activated",
        )
        self.assertEqual(
            result.vortex_capillary_preparation_activation_reason,
            "capillary_claim_evidence_requested",
        )
        self.assertEqual(
            result.vortex_capillary_preparation_activation_observed_split_count,
            1,
        )
        self.assertEqual(result.vortex_capillary_preparation_task_count, 6)
        self.assertEqual(
            result.vortex_capillary_preparation_task_roles,
            (
                "source_split_discovery",
                "read_chunk",
                "columnarize_encode",
                "vortex_segment_write",
                "reopen_verify",
                "sink_evidence",
            ),
        )
        self.assertEqual(result.vortex_capillary_preparation_execution_window_count, 3)
        self.assertEqual(
            result.vortex_capillary_preparation_execution_window_ids,
            (
                "vortex-capillary-window-0000",
                "vortex-capillary-window-0001",
                "vortex-capillary-window-0002",
            ),
        )
        self.assertTrue(result.vortex_capillary_preparation_scheduler_applied)
        self.assertEqual(
            result.vortex_capillary_preparation_scheduler_application_reason,
            "pulseweave_batch_window_applied_to_capillary_manifest",
        )
        self.assertEqual(
            result.vortex_capillary_preparation_prewrite_status,
            "applied_before_array_build",
        )
        self.assertTrue(result.vortex_capillary_preparation_prewrite_scheduler_applied)
        self.assertEqual(
            result.vortex_capillary_preparation_prewrite_execution_window_count,
            3,
        )
        self.assertEqual(
            result.vortex_capillary_preparation_prewrite_execution_window_ids,
            (
                "vortex-capillary-window-0000",
                "vortex-capillary-window-0001",
                "vortex-capillary-window-0002",
            ),
        )
        self.assertEqual(
            result.vortex_capillary_preparation_prewrite_array_build_gate_status,
            "applied_prewrite_window",
        )
        self.assertEqual(
            result.vortex_capillary_preparation_prewrite_write_gate_status,
            "applied_prewrite_window",
        )
        self.assertEqual(
            result.vortex_capillary_preparation_prewrite_reopen_gate_status,
            "applied_prewrite_window",
        )
        self.assertEqual(
            result.vortex_capillary_preparation_prewrite_sink_evidence_gate_status,
            "applied_prewrite_window",
        )
        self.assertFalse(result.vortex_capillary_preparation_prewrite_fallback_attempted)
        self.assertFalse(
            result.vortex_capillary_preparation_prewrite_external_engine_invoked
        )
        self.assertEqual(
            result.vortex_capillary_preparation_native_io_certificate_status,
            "certified",
        )
        self.assertEqual(
            result.vortex_capillary_preparation_pulseweave_status,
            "applied",
        )
        self.assertTrue(
            result.vortex_capillary_preparation_pulseweave_runtime_decision_applied
        )
        self.assertEqual(
            result.vortex_capillary_preparation_pulseweave_decision_digest,
            "fnv64:pulse",
        )
        self.assertTrue(result.vortex_capillary_preparation_proofbound_claim_allowed)
        self.assertEqual(
            result.vortex_capillary_preparation_no_standalone_lane_status,
            "funnelled_through_vortex_ingest_source_state_to_vortex_prepared_state",
        )
        self.assertEqual(
            result.vortex_copy_budget_status,
            "reported_copy_budget_with_unmeasured_segments",
        )
        self.assertEqual(
            result.vortex_copy_budget_measurement_status,
            "reported_with_not_measured_segments",
        )
        self.assertEqual(
            result.vortex_copy_budget_buffer_reuse_status,
            "blocked_until_correctness_parity",
        )
        self.assertEqual(
            result.vortex_copy_budget_unsafe_lifetime_shortcut_status,
            "blocked_no_unsafe_lifetime_shortcuts",
        )
        self.assertEqual(
            result.vortex_copy_budget_no_standalone_lane_status,
            "funnelled_through_vortex_ingest_source_state_to_vortex_prepared_state",
        )
        self.assertTrue(result.source_io_performed)
        self.assertTrue(result.prepared_state_created)
        self.assertFalse(result.fallback_attempted)
        self.assertFalse(result.external_engine_invoked)
        self.assertEqual(result.claim_gate_status, "fixture_smoke_only")

    def test_vortex_ingest_smoke_helper_dispatches_delta_overlay_route(self) -> None:
        binary = self.fake_cli(
            textwrap.dedent(
                """
                import json, sys
                assert sys.argv[1:] == [
                    "vortex-ingest-smoke",
                    "target/base.csv",
                    "target/base.vortex",
                    "--delta-source",
                    "target/delta.csv",
                    "--delta-target",
                    "target/delta.vortex",
                    "--format",
                    "json",
                ], sys.argv
                print(json.dumps({
                    "schema_version": "shardloom.output.v2",
                    "command": "vortex-ingest-smoke",
                    "status": "success",
                    "summary": "ok",
                    "human_text": "ok",
                    "fallback": {"attempted": False, "allowed": False, "engine": None, "reason": "disabled"},
                    "diagnostics": [],
                    "fields": [
                        {"key": "source_path", "value": "target/base.csv"},
                        {"key": "target_vortex_path", "value": "target/base.vortex"},
                        {"key": "source_format", "value": "csv"},
                        {"key": "vortex_ingest_status", "value": "prepared_state_created"},
                        {"key": "prepared_state_id", "value": "vortex-prepared-state-base"},
                        {"key": "prepared_state_digest", "value": "fnv64:base"},
                        {"key": "vortex_artifact_digest", "value": "fnv64:artifact"},
                        {"key": "input_row_count", "value": "2"},
                        {"key": "writer_row_count", "value": "2"},
                        {"key": "reopen_row_count", "value": "2"},
                        {"key": "claim_gate_status", "value": "fixture_smoke_only"},
                        {"key": "vortex_differential_preparation_status", "value": "admitted_append_only_delta_overlay"},
                        {"key": "vortex_differential_preparation_update_mode", "value": "append_only"},
                        {"key": "vortex_differential_preparation_delta_row_count", "value": "1"},
                        {"key": "vortex_differential_preparation_overlay_applied", "value": "true"},
                        {"key": "vortex_differential_preparation_base_reprepare_performed", "value": "false"},
                        {"key": "vortex_differential_preparation_delta_artifact_written", "value": "true"},
                        {"key": "vortex_differential_preparation_native_io_certificate_status", "value": "certified_local_vortex_differential_preparation_overlay"},
                        {"key": "vortex_differential_preparation_no_standalone_lane_status", "value": "funnelled_through_vortex_ingest_source_state_to_prepared_state_delta_overlay"},
                        {"key": "vortex_differential_preparation_refinement_status", "value": "admitted_append_only_refinement"},
                        {"key": "vortex_differential_preparation_refinement_mode", "value": "automatic_append_only_delta"},
                        {"key": "vortex_differential_preparation_automatic_detection_status", "value": "append_only_delta_detected"},
                        {"key": "vortex_differential_preparation_blocker_id", "value": "none"},
                        {"key": "vortex_differential_preparation_refinement_manifest_path", "value": "target/.shardloom/base.vortex.differential-refinement.manifest"},
                        {"key": "vortex_differential_preparation_refinement_manifest_digest", "value": "fnv64:manifest"},
                        {"key": "vortex_differential_preparation_refinement_manifest_written", "value": "true"},
                        {"key": "vortex_differential_preparation_refined_prepared_state_id", "value": "vortex-prepared-state-refinement"},
                        {"key": "vortex_differential_preparation_overlay_consumer_family", "value": "count"},
                        {"key": "vortex_differential_preparation_overlay_consumer_status", "value": "admitted_base_manifest_plus_delta_reopen_row_count"},
                        {"key": "vortex_differential_preparation_overlay_consumer_correctness_digest", "value": "fnv64:consumer"},
                        {"key": "fallback_attempted", "value": "false"},
                        {"key": "external_engine_invoked", "value": "false"}
                    ],
                }))
                """
            )
        )

        result = ShardLoomClient(binary=binary).vortex_ingest_smoke(
            "target/base.csv",
            "target/base.vortex",
            delta_source_path="target/delta.csv",
            delta_target_vortex_path="target/delta.vortex",
        )

        self.assertEqual(
            result.vortex_differential_preparation_status,
            "admitted_append_only_delta_overlay",
        )
        self.assertEqual(
            result.vortex_differential_preparation_update_mode,
            "append_only",
        )
        self.assertEqual(result.vortex_differential_preparation_delta_row_count, 1)
        self.assertTrue(result.vortex_differential_preparation_overlay_applied)
        self.assertFalse(
            result.vortex_differential_preparation_base_reprepare_performed
        )
        self.assertTrue(
            result.vortex_differential_preparation_delta_artifact_written
        )
        self.assertEqual(
            result.vortex_differential_preparation_native_io_certificate_status,
            "certified_local_vortex_differential_preparation_overlay",
        )
        self.assertEqual(
            result.vortex_differential_preparation_no_standalone_lane_status,
            "funnelled_through_vortex_ingest_source_state_to_prepared_state_delta_overlay",
        )
        self.assertEqual(
            result.vortex_differential_preparation_refinement_status,
            "admitted_append_only_refinement",
        )
        self.assertEqual(
            result.vortex_differential_preparation_refinement_mode,
            "automatic_append_only_delta",
        )
        self.assertEqual(
            result.vortex_differential_preparation_automatic_detection_status,
            "append_only_delta_detected",
        )
        self.assertEqual(result.vortex_differential_preparation_blocker_id, "none")
        self.assertEqual(
            result.vortex_differential_preparation_refinement_manifest_path,
            "target/.shardloom/base.vortex.differential-refinement.manifest",
        )
        self.assertEqual(
            result.vortex_differential_preparation_refinement_manifest_digest,
            "fnv64:manifest",
        )
        self.assertTrue(
            result.vortex_differential_preparation_refinement_manifest_written
        )
        self.assertEqual(
            result.vortex_differential_preparation_refined_prepared_state_id,
            "vortex-prepared-state-refinement",
        )
        self.assertEqual(
            result.vortex_differential_preparation_overlay_consumer_family,
            "count",
        )
        self.assertEqual(
            result.vortex_differential_preparation_overlay_consumer_status,
            "admitted_base_manifest_plus_delta_reopen_row_count",
        )
        self.assertEqual(
            result.vortex_differential_preparation_overlay_consumer_correctness_digest,
            "fnv64:consumer",
        )

    def test_context_prepare_vortex_dispatches_vortex_ingest_smoke(self) -> None:
        binary = self.fake_cli(
            textwrap.dedent(
                """
                import json, sys
                assert sys.argv[1:] == [
                    "vortex-ingest-smoke",
                    "target/source.csv",
                    "target/source.vortex",
                    "--certification-level",
                    "ingest_minimal",
                    "--format",
                    "json",
                ], sys.argv
                print(json.dumps({
                    "schema_version": "shardloom.output.v2",
                    "command": "vortex-ingest-smoke",
                    "status": "success",
                    "summary": "ok",
                    "human_text": "ok",
                    "fallback": {"attempted": False, "allowed": False, "engine": None, "reason": "disabled"},
                    "diagnostics": [],
                    "fields": [
                        {"key": "source_path", "value": "target/source.csv"},
                        {"key": "target_vortex_path", "value": "target/source.vortex"},
                        {"key": "source_format", "value": "csv"},
                        {"key": "vortex_ingest_status", "value": "prepared_state_created"},
                        {"key": "prepared_state_id", "value": "vortex-prepared-state-fnv64-abc"},
                        {"key": "prepared_state_digest", "value": "fnv64:abc"},
                        {"key": "vortex_artifact_digest", "value": "fnv64:def"},
                        {"key": "input_row_count", "value": "2"},
                        {"key": "writer_row_count", "value": "2"},
                        {"key": "reopen_row_count", "value": "0"},
                        {"key": "reopen_verification_status", "value": "not_performed_ingest_minimal"},
                        {"key": "certification_level", "value": "ingest_minimal"},
                        {"key": "certification_status", "value": "minimal_ingest_evidence_reported"},
                        {"key": "source_io_performed", "value": "true"},
                        {"key": "prepared_state_created", "value": "true"},
                        {"key": "claim_gate_status", "value": "not_claim_grade"},
                        {"key": "fallback_attempted", "value": "false"},
                        {"key": "external_engine_invoked", "value": "false"}
                    ],
                }))
                """
            )
        )
        ctx = ShardLoomContext(client=ShardLoomClient(binary=binary))

        result = ctx.prepare_vortex(
            "target/source.csv",
            "target/source.vortex",
            certification_level="ingest_minimal",
        )

        self.assertEqual(result.envelope.command, "vortex-ingest-smoke")
        self.assertEqual(result.vortex_ingest_status, "prepared_state_created")
        self.assertEqual(result.reopen_verification_status, "not_performed_ingest_minimal")
        self.assertEqual(result.certification_level, "ingest_minimal")
        self.assertEqual(result.claim_gate_status, "not_claim_grade")

    def test_context_prepare_vortex_returns_compatibility_prepared_route(self) -> None:
        route = ShardLoomContext(client=ShardLoomClient(binary=("unused",))).prepare_vortex(
            "fact.arrow",
            "dim.arrow",
            workspace="target/prepared",
            evidence_level="certified",
        )

        self.assertIsInstance(route, CompatibilityPreparedVortexRoute)
        self.assertEqual(route.input_format, "arrow-ipc")
        self.assertEqual(route.route_id, "local_file_prepare_once_first_query")
        self.assertEqual(route.batch_route_id, "local_file_prepare_once_batch")
        self.assertEqual(route.source_route, "compatibility_import_certified")
        self.assertEqual(route.execution_mode, "prepared_vortex")
        self.assertEqual(route.batch_execution_mode, "shardloom-prepare-batch")
        self.assertIn("VortexPreparedState", route.vortex_normalization_point)
        self.assertFalse(route.fallback_attempted)
        self.assertFalse(route.external_engine_invoked)
        fields = route.route_fields()
        self.assertTrue(fields["preparation_included_in_route"])
        self.assertTrue(fields["query_timing_starts_after_preparation"])
        self.assertEqual(fields["input_format"], "arrow-ipc")

    def test_context_prepare_vortex_rejects_mixed_inferred_input_formats(self) -> None:
        with self.assertRaisesRegex(ValueError, "infer the same input_format"):
            ShardLoomContext(client=ShardLoomClient(binary=("unused",))).prepare_vortex(
                "fact.parquet",
                "dim.csv",
                workspace="target/prepared",
            )

    def test_lazy_frame_prepare_vortex_auto_uses_artifact_manifest_reuse(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            source = root / "source.csv"
            workspace = root / "prepared"
            target = workspace / "source.vortex"
            count_path = root / "counts.json"
            source.write_text("id,label\n1,alpha\n", encoding="utf-8")
            binary = self.fake_cli(
                textwrap.dedent(
                    f"""
                    import json, sys
                    from pathlib import Path

                    args = sys.argv[1:]
                    assert args[0:3] == [
                        "vortex-ingest-smoke",
                        {str(source)!r},
                        {str(target)!r},
                    ], sys.argv
                    assert args[-2:] == ["--format", "json"], sys.argv
                    allow_overwrite = "--allow-overwrite" in args
                    count_path = Path({str(count_path)!r})
                    counts = json.loads(count_path.read_text(encoding="utf-8")) if count_path.exists() else {{"cli": 0, "writes": 0}}
                    counts["cli"] += 1
                    source_path = Path(args[1])
                    target_path = Path(args[2])
                    target_path.parent.mkdir(parents=True, exist_ok=True)
                    source_text = source_path.read_text(encoding="utf-8")
                    if "beta" in source_text:
                        assert allow_overwrite, sys.argv
                        counts["writes"] += 1
                        target_path.write_text(f"prepared drift {{counts['writes']}}", encoding="utf-8")
                        status = "prepared_state_created"
                        created = "true"
                        reused = "false"
                        reuse_hit = "false"
                        reuse_reason = "prepared_state_created_after_source_content_digest_changed"
                        invalidation = "source_content_digest_changed"
                    elif counts["writes"] == 0:
                        counts["writes"] += 1
                        target_path.write_text(f"prepared {{counts['writes']}}", encoding="utf-8")
                        status = "prepared_state_created"
                        created = "true"
                        reused = "false"
                        reuse_hit = "false"
                        reuse_reason = "no_reuse_manifest"
                        invalidation = "no_reuse_manifest"
                    else:
                        status = "prepared_state_reused_from_artifact_adjacent_manifest"
                        created = "false"
                        reused = "true"
                        reuse_hit = "true"
                        reuse_reason = "manifest_fingerprints_match"
                        invalidation = "none"
                    count_path.write_text(json.dumps(counts), encoding="utf-8")
                    print(json.dumps({{
                        "schema_version": "shardloom.output.v2",
                        "command": "vortex-ingest-smoke",
                        "status": "success",
                        "summary": "ok",
                        "human_text": "ok",
                        "fallback": {{"attempted": False, "allowed": False, "engine": None, "reason": "disabled"}},
                        "diagnostics": [],
                        "fields": [
                            {{"key": "source_path", "value": str(source_path)}},
                            {{"key": "target_vortex_path", "value": str(target_path)}},
                            {{"key": "source_format", "value": "csv"}},
                            {{"key": "vortex_ingest_status", "value": status}},
                            {{"key": "prepared_state_id", "value": "vortex-prepared-state-auto"}},
                            {{"key": "prepared_state_digest", "value": "sha256:prepared-auto"}},
                            {{"key": "vortex_artifact_digest", "value": "sha256:vortex-auto"}},
                            {{"key": "input_row_count", "value": "1"}},
                            {{"key": "writer_row_count", "value": "1"}},
                            {{"key": "reopen_row_count", "value": "1"}},
                            {{"key": "reopen_verification_status", "value": "reopen_row_count_verified"}},
                            {{"key": "certification_level", "value": "ingest_certified"}},
                            {{"key": "certification_status", "value": "fixture_smoke_certified"}},
                            {{"key": "source_io_performed", "value": "true"}},
                            {{"key": "prepared_state_created", "value": created}},
                            {{"key": "prepared_state_reused", "value": reused}},
                            {{"key": "prepared_state_reuse_hit", "value": reuse_hit}},
                            {{"key": "prepared_state_reuse_reason", "value": reuse_reason}},
                            {{"key": "prepared_state_reuse_manifest_digest", "value": "sha256:manifest-auto"}},
                            {{"key": "prepared_state_invalidation_reason", "value": invalidation}},
                            {{"key": "claim_gate_status", "value": "fixture_smoke_only"}},
                            {{"key": "fallback_attempted", "value": "false"}},
                            {{"key": "external_engine_invoked", "value": "false"}}
                        ],
                    }}))
                    """
                )
            )
            frame = ShardLoomContext(client=ShardLoomClient(binary=binary)).read_csv(source)

            first = frame.prepare_vortex(workspace=workspace)
            second = frame.prepare_vortex(workspace=workspace)

            self.assertIsInstance(first, VortexIngestSmokeReport)
            self.assertEqual(first.target_vortex_path, str(target))
            self.assertTrue(first.prepared_state_created)
            self.assertFalse(first.prepared_state_reuse_hit)
            self.assertEqual(first.prepared_state_reuse_reason, "no_reuse_manifest")
            self.assertEqual(
                second.vortex_ingest_status,
                "prepared_state_reused_from_artifact_adjacent_manifest",
            )
            self.assertFalse(second.prepared_state_created)
            self.assertTrue(second.prepared_state_reused)
            self.assertTrue(second.prepared_state_reuse_hit)
            self.assertEqual(second.prepared_state_reuse_reason, "manifest_fingerprints_match")
            self.assertEqual(second.prepared_state_invalidation_reason, "none")
            self.assertFalse(second.fallback_attempted)
            self.assertFalse(second.external_engine_invoked)

            source.write_text("id,label\n1,alpha\n2,beta\n", encoding="utf-8")
            third = frame.prepare_vortex(workspace=workspace, allow_overwrite=True)
            self.assertFalse(third.prepared_state_reuse_hit)
            self.assertEqual(
                third.prepared_state_reuse_reason,
                "prepared_state_created_after_source_content_digest_changed",
            )
            self.assertEqual(
                third.prepared_state_invalidation_reason,
                "source_content_digest_changed",
            )
            self.assertEqual(
                json.loads(count_path.read_text(encoding="utf-8")),
                {"cli": 3, "writes": 2},
            )

    def test_lazy_frame_prepare_vortex_route_is_queryable_when_dim_is_supplied(
        self,
    ) -> None:
        binary = self.fake_cli(
            textwrap.dedent(
                """
                import json, sys
                assert sys.argv[1:] == [
                    "traditional-analytics-prepare-batch-run",
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
                    "command": "traditional-analytics-prepare-batch-run",
                    "status": "success",
                    "summary": "prepare/batch",
                    "human_text": "prepare/batch",
                    "fallback": {"attempted": False, "allowed": False, "engine": None, "reason": "disabled"},
                    "diagnostics": [],
                    "fields": [
                        {"key": "prepare_batch_schema_version", "value": "shardloom.traditional_analytics.prepare_and_batch.v1"},
                        {"key": "prepare_batch_lifecycle_status", "value": "prepared_vortex_lifecycle_scan_complete_output_not_requested"},
                        {"key": "prepare_batch_lifecycle_output_status", "value": "vortex_result_sink_not_requested"},
                        {"key": "prepare_batch_lifecycle_no_standalone_lane", "value": "true"},
                        {"key": "prepare_batch_fact_vortex_path", "value": "fact.vortex"},
                        {"key": "prepare_batch_dim_vortex_path", "value": "dim.vortex"},
                        {"key": "prepare_batch_fact_vortex_digest", "value": "sha256:f"},
                        {"key": "prepare_batch_dim_vortex_digest", "value": "sha256:d"},
                        {"key": "prepare_batch_prepared_artifact_cleanup_policy", "value": "caller_owned_workspace_cleanup"},
                        {"key": "prepare_batch_prepared_artifact_reuse_eligible", "value": "true"},
                        {"key": "scenario_order", "value": "selective filter"},
                        {"key": "source_state_reused", "value": "true"},
                        {"key": "fallback_attempted", "value": "false"},
                        {"key": "external_engine_invoked", "value": "false"}
                    ],
                }))
                """
            )
        )
        frame = ShardLoomContext(client=ShardLoomClient(binary=binary)).read_csv("fact.csv")

        route = frame.prepare_vortex(dim="dim.csv", workspace="work")
        query = route.query("selective filter")
        result = query.collect()

        self.assertIsInstance(route, CompatibilityPreparedVortexRoute)
        self.assertIsInstance(query, PreparedVortexQuery)
        self.assertIsInstance(result, PreparedVortexBatchResult)
        self.assertEqual(query.execution_mode, "prepared_vortex")
        self.assertEqual(result.scenario_order, ("selective filter",))
        self.assertFalse(result.fallback_attempted)
        self.assertFalse(result.external_engine_invoked)

    def test_lazy_frame_prepare_vortex_rejects_ambiguous_or_non_raw_inputs(
        self,
    ) -> None:
        ctx = ShardLoomContext(client=ShardLoomClient(binary=("unused",)))

        with self.assertRaisesRegex(ValueError, "requires target_vortex_path or workspace"):
            ctx.read_csv("source.csv").prepare_vortex()

        with self.assertRaisesRegex(ValueError, "either target_vortex_path or workspace, not both"):
            ctx.read_csv("source.csv").prepare_vortex(
                "target/source.vortex",
                workspace="target/prepared",
            )

        with self.assertRaisesRegex(ValueError, "already Vortex-native"):
            ctx.read_vortex("source.vortex").prepare_vortex(workspace="target/prepared")

        with self.assertRaisesRegex(ValueError, "before query operators"):
            ctx.read_csv("source.csv").filter("id > 0").prepare_vortex(
                workspace="target/prepared"
            )

        live_ctx = ShardLoomContext(client=ShardLoomClient(binary=("unused",)), engine="live")
        with self.assertRaisesRegex(ValueError, "live/hybrid preparation remains gated"):
            live_ctx.read_csv("source.csv").prepare_vortex(workspace="target/prepared")

        with self.assertRaisesRegex(ValueError, "query routes require dim"):
            ctx.read_csv("source.csv").prepare_vortex(
                workspace="target/prepared",
                input_format="csv",
            )

    def test_generated_source_prepare_vortex_uses_generated_vortex_sink_evidence(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            workspace = Path(tempdir) / "prepared"
            binary = self.fake_cli(
                textwrap.dedent(
                    f"""
                    import json, sys
                    args = sys.argv[1:]
                    assert args[0] == "generated-source-user-rows-smoke", sys.argv
                    assert args[1].startswith({str(workspace)!r}), sys.argv
                    assert args[1].endswith(".vortex"), sys.argv
                    assert args[2:] == [
                        "id:int64,label:utf8", "id=1,label=alpha",
                        "--source-kind", "user_rows", "--output-format", "vortex",
                        "--format", "json"
                    ], sys.argv
                    print(json.dumps({{
                        "schema_version": "shardloom.output.v2",
                        "command": "generated-source-user-rows-smoke",
                        "status": "success",
                        "summary": "ok",
                        "human_text": "ok",
                        "fallback": {{"attempted": False, "allowed": False, "engine": None, "reason": "disabled"}},
                        "diagnostics": [],
                        "fields": [
                            {{"key": "generated_source_kind", "value": "user_rows"}},
                            {{"key": "generated_source_row_count", "value": "1"}},
                            {{"key": "generated_source_certificate_status", "value": "present"}},
                            {{"key": "output_native_io_certificate_status", "value": "certified_local_vortex_sink"}},
                            {{"key": "output_path", "value": args[1]}},
                            {{"key": "output_format", "value": "vortex"}},
                            {{"key": "vortex_output_runtime_execution", "value": "true"}},
                            {{"key": "vortex_output_reopen_verified", "value": "true"}},
                            {{"key": "vortex_output_row_count", "value": "1"}},
                            {{"key": "vortex_artifact_digest", "value": "fnv64:vortex-generated"}},
                            {{"key": "prepared_state_created", "value": "true"}},
                            {{"key": "prepared_state_reused", "value": "false"}},
                            {{"key": "prepared_state_reuse_hit", "value": "false"}},
                            {{"key": "prepared_state_reuse_scope", "value": "artifact_adjacent_manifest_local_vortex_artifacts"}},
                            {{"key": "prepared_state_reuse_reason", "value": "prepared_state_created_after_no_reuse_manifest"}},
                            {{"key": "prepared_state_reuse_manifest_digest", "value": "fnv64:generated-manifest"}},
                            {{"key": "prepared_state_invalidation_reason", "value": "no_reuse_manifest"}},
                            {{"key": "upstream_vortex_write_called", "value": "true"}},
                            {{"key": "upstream_vortex_scan_called", "value": "true"}},
                            {{"key": "fallback_attempted", "value": "false"}},
                            {{"key": "external_engine_invoked", "value": "false"}},
                            {{"key": "claim_gate_status", "value": "fixture_smoke_only"}}
                        ],
                    }}))
                    """
                )
            )
            ctx = ShardLoomContext(client=ShardLoomClient(binary=binary))

            report = ctx.from_rows([{"id": 1, "label": "alpha"}]).prepare_vortex(
                workspace=workspace
            )

            self.assertIsInstance(report, GeneratedSourceWriteReport)
            self.assertTrue(report.output_path.startswith(str(workspace)))
            self.assertTrue(report.output_path.endswith(".vortex"))
            self.assertEqual(report.output_format, "vortex")
            self.assertTrue(report.vortex_output_runtime_execution)
            self.assertTrue(report.vortex_output_reopen_verified)
            self.assertTrue(report.prepared_state_created)
            self.assertFalse(report.prepared_state_reused)
            self.assertFalse(report.prepared_state_reuse_hit)
            self.assertEqual(
                report.prepared_state_reuse_scope,
                "artifact_adjacent_manifest_local_vortex_artifacts",
            )
            self.assertEqual(
                report.prepared_state_reuse_reason,
                "prepared_state_created_after_no_reuse_manifest",
            )
            self.assertEqual(
                report.prepared_state_reuse_manifest_digest,
                "fnv64:generated-manifest",
            )
            self.assertFalse(report.fallback_attempted)
            self.assertFalse(report.external_engine_invoked)

    def test_generated_source_prepare_vortex_rejects_ambiguous_target_ownership(
        self,
    ) -> None:
        source = ShardLoomContext(client=ShardLoomClient(binary=("unused",))).from_rows(
            [{"id": 1}]
        )

        with self.assertRaisesRegex(ValueError, "requires target_vortex_path or workspace"):
            source.prepare_vortex()

        with self.assertRaisesRegex(ValueError, "either target_vortex_path or workspace"):
            source.prepare_vortex("target/generated.vortex", workspace="target/prepared")

    def test_generated_range_and_sql_prepare_vortex_use_vortex_output_format(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            workspace = Path(tempdir) / "prepared"
            range_target = workspace / "generated-range-0-2-1-value.vortex"
            sql_target = Path(tempdir) / "values.vortex"
            binary = self.fake_cli(
                textwrap.dedent(
                    f"""
                    import json, sys
                    args = sys.argv[1:]
                    if args[0] == "generated-source-range-smoke":
                        assert args == [
                            "generated-source-range-smoke",
                            {str(range_target)!r},
                            "0",
                            "2",
                            "--step",
                            "1",
                            "--column",
                            "value",
                            "--output-format",
                            "vortex",
                            "--format",
                            "json",
                        ], sys.argv
                        kind = "range"
                        output_path = {str(range_target)!r}
                    elif args[0] == "generated-source-sql-smoke":
                        assert args == [
                            "generated-source-sql-smoke",
                            {str(sql_target)!r},
                            "VALUES (1)",
                            "--output-format",
                            "vortex",
                            "--format",
                            "json",
                        ], sys.argv
                        kind = "sql_values"
                        output_path = {str(sql_target)!r}
                    else:
                        raise AssertionError(sys.argv)
                    print(json.dumps({{
                        "schema_version": "shardloom.output.v2",
                        "command": args[0],
                        "status": "success",
                        "summary": "ok",
                        "human_text": "ok",
                        "fallback": {{"attempted": False, "allowed": False, "engine": None, "reason": "disabled"}},
                        "diagnostics": [],
                        "fields": [
                            {{"key": "generated_source_kind", "value": kind}},
                            {{"key": "generated_source_row_count", "value": "1"}},
                            {{"key": "generated_source_certificate_status", "value": "present"}},
                            {{"key": "output_native_io_certificate_status", "value": "certified_local_vortex_sink"}},
                            {{"key": "output_path", "value": output_path}},
                            {{"key": "output_format", "value": "vortex"}},
                            {{"key": "prepared_state_created", "value": "true"}},
                            {{"key": "prepared_state_reuse_hit", "value": "false"}},
                            {{"key": "vortex_output_runtime_execution", "value": "true"}},
                            {{"key": "upstream_vortex_write_called", "value": "true"}},
                            {{"key": "upstream_vortex_scan_called", "value": "true"}},
                            {{"key": "fallback_attempted", "value": "false"}},
                            {{"key": "external_engine_invoked", "value": "false"}},
                            {{"key": "claim_gate_status", "value": "fixture_smoke_only"}}
                        ],
                    }}))
                    """
                )
            )
            ctx = ShardLoomContext(client=ShardLoomClient(binary=binary))

            range_report = ctx.range(0, 2).prepare_vortex(workspace=workspace)
            sql_report = ctx.sql_values("VALUES (1)").prepare_vortex(sql_target)

            self.assertEqual(range_report.output_path, str(range_target))
            self.assertEqual(sql_report.output_path, str(sql_target))
            self.assertEqual(range_report.output_format, "vortex")
            self.assertEqual(sql_report.output_format, "vortex")
            self.assertTrue(range_report.prepared_state_created)
            self.assertTrue(sql_report.prepared_state_created)
            self.assertFalse(range_report.prepared_state_reuse_hit)
            self.assertFalse(sql_report.prepared_state_reuse_hit)

    def test_session_prepare_vortex_returns_compatibility_prepared_route(self) -> None:
        session = ShardLoomSession(client=ShardLoomClient(binary=("unused",)))

        route = session.prepare_vortex(
            "fact.jsonl",
            dim="dim.jsonl",
            workspace="target/prepared",
        )

        self.assertIsInstance(route, CompatibilityPreparedVortexRoute)
        self.assertEqual(route.input_format, "jsonl")
        self.assertEqual(route.route_id, "local_file_prepare_once_first_query")
        self.assertEqual(route.batch_route_id, "local_file_prepare_once_batch")
        self.assertFalse(route.fallback_attempted)
        self.assertFalse(route.external_engine_invoked)

    def test_context_prepared_route_query_collect_dispatches_prepare_batch(self) -> None:
        binary = self.fake_cli(
            textwrap.dedent(
                """
                import json, sys
                assert sys.argv[1:] == [
                    "traditional-analytics-prepare-batch-run",
                    "selective filter",
                    "fact.parquet",
                    "dim.parquet",
                    "--workspace",
                    "prepare-work",
                    "--input-format",
                    "parquet",
                    "--cdc-delta",
                    "cdc.parquet",
                    "--result-workspace",
                    "result-work",
                    "--write-result-vortex",
                    "--evidence-level",
                    "certified",
                    "--memory-gb",
                    "2",
                    "--max-parallelism",
                    "4",
                    "--format",
                    "json",
                ], sys.argv
                print(json.dumps({
                    "schema_version": "shardloom.output.v2",
                    "command": "traditional-analytics-prepare-batch-run",
                    "status": "success",
                    "summary": "ok",
                    "human_text": "ok",
                    "fallback": {"attempted": False, "allowed": False, "engine": None, "reason": "disabled"},
                    "diagnostics": [],
                    "fields": [
                        {"key": "prepare_batch_schema_version", "value": "shardloom.traditional_analytics.prepare_and_batch.v1"},
                        {"key": "prepare_batch_lifecycle_status", "value": "prepared_vortex_lifecycle_complete_with_output_replay"},
                        {"key": "prepare_batch_lifecycle_output_status", "value": "vortex_result_sink_written_and_replay_verified"},
                        {"key": "prepare_batch_lifecycle_no_standalone_lane", "value": "true"},
                        {"key": "prepare_batch_preparation_input_format", "value": "parquet"},
                        {"key": "prepare_batch_preparation_included_in_batch_timing", "value": "false"},
                        {"key": "prepare_batch_fact_vortex_path", "value": "fact.vortex"},
                        {"key": "prepare_batch_dim_vortex_path", "value": "dim.vortex"},
                        {"key": "prepare_batch_cdc_delta_vortex_path", "value": "cdc.vortex"},
                        {"key": "prepare_batch_prepared_artifact_cleanup_policy", "value": "caller_owned_workspace_cleanup"},
                        {"key": "prepare_batch_prepared_artifact_reuse_eligible", "value": "true"},
                        {"key": "scenario_order", "value": "selective filter"},
                        {"key": "source_state_digest", "value": "sha256:source"},
                        {"key": "source_state_reuse_status", "value": "per_batch_selective_filter_state_reused"},
                        {"key": "source_state_reused", "value": "true"},
                        {"key": "selected_evidence_level", "value": "certified"},
                        {"key": "fallback_attempted", "value": "false"},
                        {"key": "external_engine_invoked", "value": "false"}
                    ],
                }))
                """
            )
        )
        ctx = ShardLoomContext(client=ShardLoomClient(binary=binary))

        query = ctx.prepare_vortex(
            "fact.parquet",
            dim="dim.parquet",
            workspace="prepare-work",
            cdc_delta="cdc.parquet",
            evidence_level="certified",
            memory_gb=2,
            max_parallelism=4,
        ).query("selective filter", result_workspace="result-work")

        self.assertIsInstance(query, PreparedVortexQuery)
        self.assertEqual(query.execution_mode, "prepared_vortex")
        result = query.write_vortex()

        self.assertIsInstance(result, PreparedVortexBatchResult)
        self.assertEqual(result.batch.command, "traditional-analytics-prepare-batch-run")
        self.assertEqual(result.artifacts.fact_vortex_path, "fact.vortex")
        self.assertEqual(result.artifacts.cdc_delta_vortex_path, "cdc.vortex")
        self.assertEqual(result.scenario_order, ("selective filter",))
        self.assertTrue(result.source_state_reused)
        self.assertTrue(result.lifecycle_complete_with_output_replay)
        self.assertFalse(result.fallback_attempted)
        self.assertFalse(result.external_engine_invoked)

    def test_context_prepared_route_reuses_workspace_manifest_without_reprepare(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            fact = root / "fact.csv"
            dim = root / "dim.csv"
            workspace = root / "prepared"
            count_path = root / "counts.json"
            fact.write_text(
                "id,group_key,dim_key,value,metric,flag,category\n"
                "1,1,10,7,3.5,1,A\n",
                encoding="utf-8",
            )
            dim.write_text("dim_key,dim_label,weight\n10,alpha,1.0\n", encoding="utf-8")
            binary = self.fake_cli(
                textwrap.dedent(
                    f"""
                    import json, sys
                    from pathlib import Path

                    count_path = Path({str(count_path)!r})
                    counts = json.loads(count_path.read_text(encoding="utf-8")) if count_path.exists() else {{"prepare": 0, "batch": 0, "repair": 0}}

                    def emit(command, fields):
                        print(json.dumps({{
                            "schema_version": "shardloom.output.v2",
                            "command": command,
                            "status": "success",
                            "summary": "ok",
                            "human_text": "ok",
                            "fallback": {{"attempted": False, "allowed": False, "engine": None, "reason": "disabled"}},
                            "diagnostics": [],
                            "fields": [{{"key": key, "value": str(value)}} for key, value in fields.items()],
                        }}))

                    command = sys.argv[1]
                    if command == "traditional-analytics-prepare-batch-run":
                        counts["prepare"] += 1
                        count_path.write_text(json.dumps(counts), encoding="utf-8")
                        workspace = Path(sys.argv[sys.argv.index("--workspace") + 1])
                        workspace.mkdir(parents=True, exist_ok=True)
                        fact_vortex = workspace / "fact.vortex"
                        dim_vortex = workspace / "dim.vortex"
                        fact_vortex.write_text(f"fact artifact {{counts['prepare']}}", encoding="utf-8")
                        dim_vortex.write_text(f"dim artifact {{counts['prepare']}}", encoding="utf-8")
                        emit(command, {{
                            "prepare_batch_schema_version": "shardloom.traditional_analytics.prepare_and_batch.v1",
                            "prepare_batch_lifecycle_status": "prepared_vortex_lifecycle_scan_complete_output_not_requested",
                            "prepare_batch_lifecycle_output_status": "vortex_result_sink_not_requested",
                            "prepare_batch_lifecycle_no_standalone_lane": "true",
                            "prepare_batch_preparation_input_format": "csv",
                            "prepare_batch_preparation_included_in_batch_timing": "false",
                            "prepare_batch_query_timing_starts_after_preparation": "true",
                            "prepare_batch_fact_vortex_path": str(fact_vortex),
                            "prepare_batch_dim_vortex_path": str(dim_vortex),
                            "prepare_batch_fact_vortex_digest": f"sha256:fact-{{counts['prepare']}}",
                            "prepare_batch_dim_vortex_digest": f"sha256:dim-{{counts['prepare']}}",
                            "prepare_batch_prepared_state_id": f"prepared-state://{{counts['prepare']}}",
                            "prepare_batch_prepared_state_digest": f"sha256:prepared-{{counts['prepare']}}",
                            "prepare_batch_source_state_id": f"source-state://{{counts['prepare']}}",
                            "prepare_batch_source_state_digest": f"sha256:source-{{counts['prepare']}}",
                            "prepare_batch_prepared_artifact_cleanup_policy": "caller_owned_workspace_cleanup",
                            "prepare_batch_prepared_artifact_reuse_eligible": "true",
                            "scenario_order": "selective filter",
                            "source_state_digest": "sha256:batch-source",
                            "source_state_reuse_status": "not_prepared_single_consumer_uses_scenario_scan",
                            "source_state_reused": "false",
                            "selected_evidence_level": "certified",
                            "fallback_attempted": "false",
                            "external_engine_invoked": "false",
                        }})
                    elif command == "traditional-analytics-vortex-batch-run":
                        counts["batch"] += 1
                        count_path.write_text(json.dumps(counts), encoding="utf-8")
                        assert sys.argv[3] == str(Path({str(workspace / "fact.vortex")!r}).resolve(strict=False)), sys.argv
                        assert sys.argv[4] == str(Path({str(workspace / "dim.vortex")!r}).resolve(strict=False)), sys.argv
                        emit(command, {{
                            "schema_version": "shardloom.traditional_analytics.vortex_batch.v1",
                            "runner_kind": "single_process_prepared_native_batch",
                            "support_status": "runtime_supported",
                            "claim_gate_status": "fixture_smoke_only",
                            "requested_execution_mode": "prepared_vortex",
                            "selected_execution_modes": "prepared_vortex",
                            "scenario_order": "selective filter",
                            "source_state_digest": "sha256:batch-source",
                            "source_state_reuse_status": "not_prepared_single_consumer_uses_scenario_scan",
                            "source_state_reused": "false",
                            "selected_evidence_level": "certified",
                            "all_native_io_certificates_certified": "true",
                            "result_sink_requested": "false",
                            "all_result_sink_replays_verified": "false",
                            "fallback_attempted": "false",
                            "external_engine_invoked": "false",
                        }})
                    elif command == "vortex-ingest-smoke":
                        counts["repair"] += 1
                        count_path.write_text(json.dumps(counts), encoding="utf-8")
                        assert sys.argv[2] == {str(fact)!r}, sys.argv
                        assert sys.argv[3] == str(Path({str(workspace / "fact.vortex")!r}).resolve(strict=False)), sys.argv
                        assert sys.argv[4:] == [
                            "--input-format",
                            "csv",
                            "--allow-overwrite",
                            "--format",
                            "json",
                        ], sys.argv
                        Path(sys.argv[3]).write_text(f"fact artifact repaired {{counts['repair']}}", encoding="utf-8")
                        emit(command, {{
                            "source_path": sys.argv[2],
                            "target_vortex_path": sys.argv[3],
                            "source_format": "csv",
                            "source_state_id": f"source-state://repair-{{counts['repair']}}",
                            "source_state_digest": f"sha256:repair-source-{{counts['repair']}}",
                            "vortex_ingest_status": "prepared_state_created",
                            "prepared_state_id": f"prepared-state://repair-{{counts['repair']}}",
                            "prepared_state_digest": f"sha256:repair-prepared-{{counts['repair']}}",
                            "vortex_artifact_digest": f"sha256:repair-artifact-{{counts['repair']}}",
                            "source_to_columnar_millis": "2",
                            "vortex_array_build_millis": "3",
                            "vortex_write_millis": "4",
                            "vortex_reopen_verify_millis": "5",
                            "input_row_count": "1",
                            "writer_row_count": "1",
                            "reopen_row_count": "1",
                            "reopen_verification_status": "reopen_row_count_verified",
                            "certification_level": "ingest_certified",
                            "certification_status": "fixture_smoke_certified",
                            "source_io_performed": "true",
                            "prepared_state_created": "true",
                            "claim_gate_status": "fixture_smoke_only",
                            "fallback_attempted": "false",
                            "external_engine_invoked": "false",
                        }})
                    else:
                        raise AssertionError(sys.argv)
                    """
                )
            )
            ctx = ShardLoomContext(client=ShardLoomClient(binary=binary))
            route = ctx.prepare_vortex(
                fact,
                dim=dim,
                workspace=workspace,
                input_format="csv",
                evidence_level="certified",
            )

            first = route.run_batch("selective filter")
            second = route.run_batch("selective filter")

            self.assertIsInstance(first, PreparedVortexBatchResult)
            self.assertIsInstance(second, PreparedVortexBatchResult)
            self.assertEqual(first.batch.command, "traditional-analytics-prepare-batch-run")
            self.assertFalse(first.prepared_state_reuse_hit)
            self.assertEqual(first.prepared_state_reuse_reason, "no_reuse_manifest")
            self.assertEqual(
                second.batch.command,
                "traditional-analytics-prepared-state-reuse-batch-run",
            )
            self.assertEqual(
                second.batch.field("prepare_batch_preparation_timing_scope"),
                "workspace_manifest_reuse_skips_compatibility_prepare",
            )
            self.assertEqual(
                second.batch.field(
                    "prepare_batch_source_admission_digest_policy_schema_version"
                ),
                "shardloom.traditional_analytics.source_admission_digest_policy.v1",
            )
            self.assertEqual(
                second.batch.field(
                    "prepare_batch_source_admission_digest_policy_status"
                ),
                "content_digest_fingerprint_reuse_hit",
            )
            self.assertEqual(
                second.batch.field("prepare_batch_prepared_state_index_schema_version"),
                "shardloom.traditional_analytics.prepared_state_index.v1",
            )
            self.assertEqual(
                second.batch.field("prepare_batch_prepared_state_index_lookup_status"),
                "workspace_index_manifest_hit",
            )
            self.assertTrue(
                str(second.batch.field("prepare_batch_prepared_state_index_digest")).startswith(
                    "sha256:"
                )
            )
            self.assertEqual(
                second.batch.field(
                    "prepare_batch_prepared_state_read_through_cache_schema_version"
                ),
                "shardloom.traditional_analytics.prepared_state_read_through_cache.v1",
            )
            self.assertEqual(
                second.batch.field(
                    "prepare_batch_prepared_state_read_through_cache_status"
                ),
                "python_route_manifest_payload_reuse_index_not_read_through",
            )
            self.assertEqual(
                second.batch.field(
                    "prepare_batch_prepared_state_read_through_cache_fallback_attempted"
                ),
                "false",
            )
            self.assertEqual(
                second.batch.field(
                    "prepare_batch_prepared_state_read_through_cache_external_engine_invoked"
                ),
                "false",
            )
            self.assertEqual(
                second.batch.field("prepare_batch_prepared_state_dependency_status"),
                "manifest_dependencies_matched",
            )
            self.assertEqual(
                second.batch.field("prepare_batch_prepared_state_partial_repair_status"),
                "not_needed_manifest_hit",
            )
            self.assertEqual(
                second.batch.field(
                    "prepare_batch_prepared_state_partial_repair_regeneration_performed"
                ),
                "false",
            )
            self.assertEqual(
                second.batch.field(
                    "prepare_batch_source_admission_full_content_digest_requested"
                ),
                "true",
            )
            self.assertEqual(second.batch.field("prepare_batch_preparation_micros"), "0")
            self.assertEqual(second.batch.field("prepare_batch_prepared_state_created"), "false")
            self.assertEqual(second.batch.field("prepare_batch_prepared_state_reused"), "true")
            self.assertEqual(second.batch.field("prepared_state_reuse_hit"), "true")
            self.assertTrue(second.prepared_state_reuse_hit)
            self.assertEqual(
                second.prepared_state_reuse_reason,
                "manifest_fingerprints_match",
            )
            self.assertEqual(
                second.batch.field("prepared_state_reuse_reason"),
                "manifest_fingerprints_match",
            )
            self.assertTrue(
                str(second.batch.field("prepared_state_reuse_manifest_digest")).startswith(
                    "sha256:"
                )
            )
            self.assertEqual(
                second.artifacts.fact_vortex_path,
                str((workspace / "fact.vortex").resolve(strict=False)),
            )
            self.assertFalse(second.fallback_attempted)
            self.assertFalse(second.external_engine_invoked)
            self.assertEqual(
                json.loads(count_path.read_text(encoding="utf-8")),
                {"prepare": 1, "batch": 1, "repair": 0},
            )
            self.assertTrue((workspace / ".shardloom" / "prepared-state-index.json").exists())

            original_stat = fact.stat()
            fact.write_text(
                "id,group_key,dim_key,value,metric,flag,category\n"
                "1,1,10,9,4.5,1,A\n",
                encoding="utf-8",
            )
            os.utime(
                fact,
                ns=(original_stat.st_atime_ns, original_stat.st_mtime_ns),
            )
            third = route.run_batch("selective filter")
            self.assertFalse(third.prepared_state_reuse_hit)
            self.assertEqual(
                third.prepared_state_reuse_reason,
                "role_scoped_repair_completed",
            )
            self.assertEqual(
                third.batch.field("prepare_batch_prepared_state_dependency_status"),
                "manifest_dependencies_repaired",
            )
            self.assertEqual(
                third.batch.field("prepare_batch_prepared_state_dependency_changed_roles"),
                "fact_input",
            )
            self.assertEqual(
                third.batch.field("prepare_batch_prepared_state_partial_repair_status"),
                "admitted_role_repair_completed",
            )
            self.assertEqual(
                third.batch.field("prepare_batch_prepared_state_partial_repair_reused_roles"),
                "dim_input",
            )
            self.assertEqual(
                third.batch.field("prepare_batch_prepared_state_partial_repair_repaired_roles"),
                "fact_input",
            )
            self.assertEqual(
                third.batch.field("prepare_batch_source_to_columnar_micros"),
                "2000",
            )
            self.assertEqual(
                third.batch.field("prepare_batch_vortex_array_build_micros"),
                "3000",
            )
            self.assertEqual(
                third.batch.field("prepare_batch_vortex_write_micros"),
                "4000",
            )
            self.assertEqual(
                third.batch.field("prepare_batch_vortex_reopen_verify_micros"),
                "5000",
            )
            self.assertEqual(
                third.batch.field(
                    "prepare_batch_prepared_state_partial_repair_vortex_write_micros"
                ),
                "4000",
            )
            self.assertEqual(
                third.batch.field(
                    "prepare_batch_prepared_state_partial_repair_regeneration_performed"
                ),
                "true",
            )
            self.assertEqual(
                third.batch.field(
                    "prepare_batch_prepared_state_partial_repair_stale_segment_reuse_allowed"
                ),
                "false",
            )
            self.assertEqual(
                json.loads(count_path.read_text(encoding="utf-8")),
                {"prepare": 1, "batch": 2, "repair": 1},
            )

    def test_context_session_reuses_prepared_vortex_state_when_fingerprints_match(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            source_path = root / "source.csv"
            target_path = root / "source.vortex"
            count_path = root / "count.txt"
            source_path.write_text("id,label\n1,alpha\n2,beta\n", encoding="utf-8")
            binary = self.fake_cli(
                textwrap.dedent(
                    f"""
                    import json, sys
                    from pathlib import Path
                    count_path = Path({str(count_path)!r})
                    count = int(count_path.read_text(encoding="utf-8")) if count_path.exists() else 0
                    count += 1
                    count_path.write_text(str(count), encoding="utf-8")
                    assert sys.argv[1] == "vortex-ingest-smoke", sys.argv
                    assert sys.argv[2] == {str(source_path)!r}, sys.argv
                    assert sys.argv[3] == {str(target_path)!r}, sys.argv
                    Path(sys.argv[3]).write_text(f"vortex artifact {{count}}", encoding="utf-8")
                    print(json.dumps({{
                        "schema_version": "shardloom.output.v2",
                        "command": "vortex-ingest-smoke",
                        "status": "success",
                        "summary": "ok",
                        "human_text": "ok",
                        "fallback": {{"attempted": False, "allowed": False, "engine": None, "reason": "disabled"}},
                        "diagnostics": [],
                        "fields": [
                            {{"key": "source_path", "value": sys.argv[2]}},
                            {{"key": "target_vortex_path", "value": sys.argv[3]}},
                            {{"key": "source_format", "value": "csv"}},
                            {{"key": "source_state_id", "value": f"source-state-{{count}}"}},
                            {{"key": "source_state_digest", "value": f"sha256:source-{{count}}"}},
                            {{"key": "vortex_ingest_status", "value": "prepared_state_created"}},
                            {{"key": "prepared_state_id", "value": f"vortex-prepared-state-{{count}}"}},
                            {{"key": "prepared_state_digest", "value": f"sha256:prepared-{{count}}"}},
                            {{"key": "vortex_artifact_digest", "value": f"sha256:vortex-{{count}}"}},
                            {{"key": "input_row_count", "value": "2"}},
                            {{"key": "writer_row_count", "value": "2"}},
                            {{"key": "reopen_row_count", "value": "2"}},
                            {{"key": "reopen_verification_status", "value": "reopen_row_count_verified"}},
                            {{"key": "certification_level", "value": "ingest_certified"}},
                            {{"key": "certification_status", "value": "fixture_smoke_certified"}},
                            {{"key": "source_io_performed", "value": "true"}},
                            {{"key": "prepared_state_created", "value": "true"}},
                            {{"key": "claim_gate_status", "value": "fixture_smoke_only"}},
                            {{"key": "fallback_attempted", "value": "false"}},
                            {{"key": "external_engine_invoked", "value": "false"}}
                        ],
                    }}))
                    """
                )
            )
            ctx = ShardLoomContext(client=ShardLoomClient(binary=binary))
            session = ctx.session(session_id="test-session")

            with mock.patch.object(
                shardloom_session_module,
                "_file_content_digest",
                wraps=shardloom_session_module._file_content_digest,
            ) as digest_mock:
                first = session.prepare_vortex(
                    source_path,
                    target_path,
                    allow_overwrite=True,
                )
                self.assertEqual(digest_mock.call_count, 2)
                second = session.prepare_vortex(source_path, target_path)
                self.assertEqual(digest_mock.call_count, 2)

                self.assertIsInstance(session, ShardLoomSession)
                self.assertIsInstance(first, SessionPreparedState)
                self.assertFalse(first.reuse_hit)
                self.assertEqual(first.reuse_reason, "no_cached_prepared_state")
                self.assertTrue(second.reuse_hit)
                self.assertEqual(
                    second.reuse_reason,
                    "source_and_prepared_artifact_fingerprints_match",
                )
                self.assertEqual(second.prepared_state_id, first.prepared_state_id)
                self.assertEqual(second.source_state_id, "source-state-1")
                self.assertFalse(second.fallback_attempted)
                self.assertFalse(second.external_engine_invoked)
                self.assertEqual(count_path.read_text(encoding="utf-8"), "1")

                source_path.write_text("id,label\n1,alpha\n3,gamma\n", encoding="utf-8")
                third = session.prepare_vortex(
                    source_path,
                    target_path,
                    allow_overwrite=True,
                )
                self.assertEqual(digest_mock.call_count, 4)
            self.assertFalse(third.reuse_hit)
            self.assertEqual(third.reuse_reason, "source_fingerprint_changed")
            self.assertEqual(third.prepared_state_id, "vortex-prepared-state-2")
            self.assertEqual(count_path.read_text(encoding="utf-8"), "2")

            evidence = session.evidence()
            self.assertEqual(evidence["session_id"], "test-session")
            self.assertEqual(evidence["session_state_scope"], "in_process_python_local")
            self.assertEqual(evidence["cache_hit_count"], 1)
            self.assertEqual(evidence["cache_miss_count"], 2)
            self.assertEqual(evidence["source_state_reuse_count"], 1)
            self.assertEqual(evidence["prepared_artifact_reuse_count"], 1)
            self.assertEqual(evidence["output_plan_reuse_count"], 0)
            self.assertFalse(evidence["fallback_attempted"])
            self.assertFalse(evidence["external_engine_invoked"])

            closed_evidence = session.close()
            self.assertTrue(closed_evidence["session_closed"])
            with self.assertRaisesRegex(RuntimeError, "ShardLoomSession is closed"):
                session.prepare_vortex(source_path, target_path)

    def test_top_level_session_helper_constructs_caller_owned_session(self) -> None:
        sess = shardloom_session(
            client=ShardLoomClient(binary=[sys.executable, "-c", "raise SystemExit(0)"]),
            engine="batch",
            session_id="top-level-session",
        )

        self.assertIsInstance(sess, ShardLoomSession)
        evidence = sess.evidence()
        self.assertEqual(evidence["session_id"], "top-level-session")
        self.assertEqual(evidence["engine_mode"], "batch")
        self.assertEqual(evidence["session_state_scope"], "in_process_python_local")
        self.assertFalse(evidence["fallback_attempted"])
        self.assertFalse(evidence["external_engine_invoked"])
        self.assertFalse(evidence["session_closed"])
        self.assertTrue(sess.close()["session_closed"])

    def test_session_read_csv_workflow_collect_reuses_source_state_when_fingerprints_match(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            source_path = root / "source.csv"
            count_path = root / "session-read-count.txt"
            source_path.write_text("id,label\n1,alpha\n2,beta\n", encoding="utf-8")
            binary = self.fake_cli(
                textwrap.dedent(
                    f"""
                    import json, sys
                    from pathlib import Path
                    count_path = Path({str(count_path)!r})
                    count = int(count_path.read_text(encoding="utf-8")) if count_path.exists() else 0
                    count += 1
                    count_path.write_text(str(count), encoding="utf-8")
                    assert sys.argv[1] == "sql-local-source-smoke", sys.argv
                    assert "--output" not in sys.argv, sys.argv
                    assert sys.argv[sys.argv.index("--output-format") + 1] == "inline-jsonl", sys.argv
                    statement = sys.argv[2]
                    assert "source.csv" in statement, statement
                    print(json.dumps({{
                        "schema_version": "shardloom.output.v2",
                        "command": "sql-local-source-smoke",
                        "status": "success",
                        "summary": "ok",
                        "human_text": "ok",
                        "fallback": {{"attempted": False, "allowed": False, "engine": None, "reason": "disabled"}},
                        "diagnostics": [],
                        "fields": [
                            {{"key": "output_format", "value": "inline-jsonl"}},
                            {{"key": "result_jsonl", "value": "{{\\"id\\":1,\\"count\\":" + str(count) + "}}\\n{{\\"id\\":2,\\"count\\":" + str(count) + "}}\\n"}},
                            {{"key": "source_state_id", "value": f"sql-source-state-{{count}}"}},
                            {{"key": "source_state_digest", "value": f"fnv64:sql-source-{{count}}"}},
                            {{"key": "source_schema_digest", "value": f"fnv64:sql-schema-{{count}}"}},
                            {{"key": "plan_digest", "value": f"fnv64:sql-plan-{{count}}"}},
                            {{"key": "execution_certificate_ref", "value": "sql-local-source.csv.projection-limit.execution.v1"}},
                            {{"key": "source_state_contract_schema_version", "value": "shardloom.local_source_state.v1"}},
                            {{"key": "source_state_read_plan", "value": "projected_source_state"}},
                            {{"key": "source_state_projection_pushdown_status", "value": "reader_projection_applied"}},
                            {{"key": "user_surface_runtime_scope", "value": "format_neutral_sql_python_runtime"}},
                            {{"key": "format_specific_boundary_scope", "value": "read_ingest_and_write_only"}},
                            {{"key": "format_specific_compute_path", "value": "false"}},
                            {{"key": "source_state_materialization_layout", "value": "arrow_record_batch_columnar_source_state_then_scalar_row_map"}},
                            {{"key": "source_state_parse_normalization", "value": "structured_reader_to_arrow_record_batches_then_scalar_rows"}},
                            {{"key": "source_state_columnar_preserved", "value": "true"}},
                            {{"key": "source_state_record_batch_count", "value": "1"}},
                            {{"key": "source_to_columnar_millis", "value": "3"}},
                            {{"key": "source_state_runtime_consumption_layout", "value": "scalar_row_map_expression_runtime"}},
                            {{"key": "source_state_scalar_runtime_materialization_required", "value": "true"}},
                            {{"key": "source_state_materialized_columns", "value": "id"}},
                            {{"key": "source_state_reader_projection_columns", "value": "id"}},
                            {{"key": "claim_gate_status", "value": "fixture_smoke_only"}},
                            {{"key": "fallback_attempted", "value": "false"}},
                            {{"key": "external_engine_invoked", "value": "false"}}
                        ],
                    }}))
                    """
                )
            )
            ctx = ShardLoomContext(client=ShardLoomClient(binary=binary))
            sess = ctx.session(session_id="ordinary-workflow-session")
            frame = sess.read_csv(source_path).select("id").limit(2)

            first = frame.collect()
            second = frame.collect()

            self.assertIsInstance(frame, SessionLazyFrame)
            self.assertIsInstance(first, SessionSqlResult)
            self.assertFalse(first.reuse_hit)
            self.assertEqual(first.reuse_reason, "no_cached_result")
            self.assertEqual(first.report.result_rows, ({"id": 1, "count": 1}, {"id": 2, "count": 1}))
            self.assertTrue(second.reuse_hit)
            self.assertEqual(
                second.reuse_reason,
                "source_and_output_fingerprints_match",
            )
            self.assertTrue(second.source_state_reuse_hit)
            self.assertFalse(second.output_plan_reuse_hit)
            self.assertEqual(second.source_state_id, "sql-source-state-1")
            self.assertEqual(second.report.result_rows, ({"id": 1, "count": 1}, {"id": 2, "count": 1}))
            self.assertEqual(count_path.read_text(encoding="utf-8"), "1")

            source_path.write_text("id,label\n1,alpha\n3,gamma\n", encoding="utf-8")
            third = frame.collect()
            self.assertFalse(third.reuse_hit)
            self.assertEqual(third.reuse_reason, "source_fingerprint_changed")
            self.assertEqual(third.source_state_id, "sql-source-state-2")
            self.assertEqual(count_path.read_text(encoding="utf-8"), "2")

            evidence = sess.evidence()
            self.assertEqual(evidence["session_id"], "ordinary-workflow-session")
            self.assertEqual(evidence["cache_hit_count"], 1)
            self.assertEqual(evidence["cache_miss_count"], 2)
            self.assertEqual(evidence["source_state_reuse_count"], 1)
            self.assertEqual(evidence["output_plan_reuse_count"], 0)
            self.assertFalse(evidence["fallback_attempted"])
            self.assertFalse(evidence["external_engine_invoked"])

            sess.close()
            with self.assertRaisesRegex(RuntimeError, "ShardLoomSession is closed"):
                frame.collect()

    def test_session_sql_workflow_write_reuses_output_when_fingerprints_match(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            source_path = root / "source.csv"
            output_path = root / "sql-session-out.jsonl"
            count_path = root / "session-sql-write-count.txt"
            source_path.write_text("id,label\n1,alpha\n2,beta\n", encoding="utf-8")
            binary = self.fake_cli(
                textwrap.dedent(
                    f"""
                    import json, sys
                    from pathlib import Path
                    count_path = Path({str(count_path)!r})
                    count = int(count_path.read_text(encoding="utf-8")) if count_path.exists() else 0
                    count += 1
                    count_path.write_text(str(count), encoding="utf-8")
                    assert sys.argv[1] == "sql-local-source-smoke", sys.argv
                    assert sys.argv[sys.argv.index("--output-format") + 1] == "jsonl", sys.argv
                    assert "--output" in sys.argv, sys.argv
                    output_path = Path(sys.argv[sys.argv.index("--output") + 1])
                    output_path.write_text(json.dumps({{"id": 1, "count": count}}) + "\\n", encoding="utf-8")
                    print(json.dumps({{
                        "schema_version": "shardloom.output.v2",
                        "command": "sql-local-source-smoke",
                        "status": "success",
                        "summary": "ok",
                        "human_text": "ok",
                        "fallback": {{"attempted": False, "allowed": False, "engine": None, "reason": "disabled"}},
                        "diagnostics": [],
                        "fields": [
                            {{"key": "output_format", "value": "jsonl"}},
                            {{"key": "output_path", "value": str(output_path)}},
                            {{"key": "output_plan_digest", "value": f"sha256:session-sql-output-plan-{{count}}"}},
                            {{"key": "source_state_id", "value": f"sql-source-state-{{count}}"}},
                            {{"key": "source_state_digest", "value": f"fnv64:sql-source-{{count}}"}},
                            {{"key": "source_schema_digest", "value": f"fnv64:sql-schema-{{count}}"}},
                            {{"key": "plan_digest", "value": f"fnv64:sql-plan-{{count}}"}},
                            {{"key": "execution_certificate_ref", "value": "sql-local-source.csv.projection-limit.execution.v1"}},
                            {{"key": "source_state_contract_schema_version", "value": "shardloom.local_source_state.v1"}},
                            {{"key": "source_state_read_plan", "value": "projected_source_state"}},
                            {{"key": "source_state_projection_pushdown_status", "value": "reader_projection_applied"}},
                            {{"key": "user_surface_runtime_scope", "value": "format_neutral_sql_python_runtime"}},
                            {{"key": "format_specific_boundary_scope", "value": "read_ingest_and_write_only"}},
                            {{"key": "format_specific_compute_path", "value": "false"}},
                            {{"key": "source_state_materialization_layout", "value": "arrow_record_batch_columnar_source_state_then_scalar_row_map"}},
                            {{"key": "source_state_parse_normalization", "value": "structured_reader_to_arrow_record_batches_then_scalar_rows"}},
                            {{"key": "source_state_columnar_preserved", "value": "true"}},
                            {{"key": "source_state_record_batch_count", "value": "1"}},
                            {{"key": "source_to_columnar_millis", "value": "3"}},
                            {{"key": "source_state_runtime_consumption_layout", "value": "scalar_row_map_expression_runtime"}},
                            {{"key": "source_state_scalar_runtime_materialization_required", "value": "true"}},
                            {{"key": "source_state_materialized_columns", "value": "id"}},
                            {{"key": "source_state_reader_projection_columns", "value": "id"}},
                            {{"key": "result_replay_verified", "value": "true"}},
                            {{"key": "output_replay_status", "value": "verified_local_file_digest"}},
                            {{"key": "claim_gate_status", "value": "fixture_smoke_only"}},
                            {{"key": "fallback_attempted", "value": "false"}},
                            {{"key": "external_engine_invoked", "value": "false"}}
                        ],
                    }}))
                    """
                )
            )
            ctx = ShardLoomContext(client=ShardLoomClient(binary=binary))
            sess = ctx.session(session_id="ordinary-sql-session")
            workflow = sess.sql(f"SELECT id FROM '{source_path}' LIMIT 2")

            first = workflow.write_jsonl(output_path, allow_overwrite=True)
            second = workflow.write_jsonl(output_path)

            self.assertIsInstance(workflow, SessionSqlWorkflow)
            self.assertIsInstance(first, SessionSqlResult)
            self.assertFalse(first.reuse_hit)
            self.assertEqual(first.reuse_reason, "no_cached_result")
            self.assertTrue(second.reuse_hit)
            self.assertEqual(
                second.reuse_reason,
                "source_and_output_fingerprints_match",
            )
            self.assertTrue(second.output_plan_reuse_hit)
            self.assertTrue(second.result_replay_reuse_hit)
            self.assertEqual(second.output_plan_digest, "sha256:session-sql-output-plan-1")
            self.assertEqual(second.source_state_id, "sql-source-state-1")
            self.assertEqual(count_path.read_text(encoding="utf-8"), "1")

            output_path.write_text('{"id":99,"count":99}\n', encoding="utf-8")
            third = workflow.write_jsonl(output_path, allow_overwrite=True)
            self.assertFalse(third.reuse_hit)
            self.assertEqual(third.reuse_reason, "output_artifact_fingerprint_changed")
            self.assertEqual(third.output_plan_digest, "sha256:session-sql-output-plan-2")
            self.assertEqual(count_path.read_text(encoding="utf-8"), "2")

            evidence = sess.evidence()
            self.assertEqual(evidence["session_id"], "ordinary-sql-session")
            self.assertEqual(evidence["cache_hit_count"], 1)
            self.assertEqual(evidence["cache_miss_count"], 2)
            self.assertEqual(evidence["source_state_reuse_count"], 1)
            self.assertEqual(evidence["output_plan_reuse_count"], 1)
            self.assertEqual(evidence["result_replay_reuse_count"], 1)
            self.assertEqual(
                evidence["last_invalidation_reason"],
                "output_artifact_fingerprint_changed",
            )
            self.assertFalse(evidence["fallback_attempted"])
            self.assertFalse(evidence["external_engine_invoked"])

    def test_context_session_reuses_local_query_output_when_fingerprints_match(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            source_path = root / "source.csv"
            output_path = root / "out.jsonl"
            count_path = root / "sql-count.txt"
            source_path.write_text("id,label\n1,alpha\n2,beta\n", encoding="utf-8")
            binary = self.fake_cli(
                textwrap.dedent(
                    f"""
                    import json, sys
                    from pathlib import Path
                    count_path = Path({str(count_path)!r})
                    count = int(count_path.read_text(encoding="utf-8")) if count_path.exists() else 0
                    count += 1
                    count_path.write_text(str(count), encoding="utf-8")
                    assert sys.argv[1] == "sql-local-source-smoke", sys.argv
                    assert "--output" in sys.argv, sys.argv
                    output_path = Path(sys.argv[sys.argv.index("--output") + 1])
                    output_path.write_text(json.dumps({{"id": 1, "count": count}}) + "\\n", encoding="utf-8")
                    print(json.dumps({{
                        "schema_version": "shardloom.output.v2",
                        "command": "sql-local-source-smoke",
                        "status": "success",
                        "summary": "ok",
                        "human_text": "ok",
                        "fallback": {{"attempted": False, "allowed": False, "engine": None, "reason": "disabled"}},
                        "diagnostics": [],
                        "fields": [
                            {{"key": "output_format", "value": "jsonl"}},
                            {{"key": "output_path", "value": str(output_path)}},
                            {{"key": "output_plan_digest", "value": f"sha256:output-plan-{{count}}"}},
                            {{"key": "source_state_id", "value": f"sql-source-state-{{count}}"}},
                            {{"key": "source_state_digest", "value": f"fnv64:sql-source-{{count}}"}},
                            {{"key": "source_schema_digest", "value": f"fnv64:sql-schema-{{count}}"}},
                            {{"key": "plan_digest", "value": f"fnv64:sql-plan-{{count}}"}},
                            {{"key": "execution_certificate_ref", "value": "sql-local-source.csv.projection-limit.execution.v1"}},
                            {{"key": "source_state_contract_schema_version", "value": "shardloom.local_source_state.v1"}},
                            {{"key": "source_state_read_plan", "value": "projected_source_state"}},
                            {{"key": "source_state_projection_pushdown_status", "value": "reader_projection_applied"}},
                            {{"key": "user_surface_runtime_scope", "value": "format_neutral_sql_python_runtime"}},
                            {{"key": "format_specific_boundary_scope", "value": "read_ingest_and_write_only"}},
                            {{"key": "format_specific_compute_path", "value": "false"}},
                            {{"key": "source_state_materialization_layout", "value": "arrow_record_batch_columnar_source_state_then_scalar_row_map"}},
                            {{"key": "source_state_parse_normalization", "value": "structured_reader_to_arrow_record_batches_then_scalar_rows"}},
                            {{"key": "source_state_columnar_preserved", "value": "true"}},
                            {{"key": "source_state_record_batch_count", "value": "1"}},
                            {{"key": "source_to_columnar_millis", "value": "3"}},
                            {{"key": "source_state_runtime_consumption_layout", "value": "scalar_row_map_expression_runtime"}},
                            {{"key": "source_state_scalar_runtime_materialization_required", "value": "true"}},
                            {{"key": "source_state_materialized_columns", "value": "id"}},
                            {{"key": "source_state_reader_projection_columns", "value": "id"}},
                            {{"key": "result_replay_verified", "value": "true"}},
                            {{"key": "output_replay_status", "value": "verified_local_file_digest"}},
                            {{"key": "claim_gate_status", "value": "fixture_smoke_only"}},
                            {{"key": "fallback_attempted", "value": "false"}},
                            {{"key": "external_engine_invoked", "value": "false"}}
                        ],
                    }}))
                    """
                )
            )
            ctx = ShardLoomContext(client=ShardLoomClient(binary=binary))
            frame = ctx.read_csv(source_path).select("id").limit(2)
            sess = ctx.session(session_id="sql-session")

            first = sess.write(frame, output_path, allow_overwrite=True)
            second = sess.write(frame, output_path)

            self.assertIsInstance(first, SessionSqlResult)
            self.assertFalse(first.reuse_hit)
            self.assertEqual(first.reuse_reason, "no_cached_result")
            self.assertTrue(second.reuse_hit)
            self.assertEqual(second.output_plan_digest, "sha256:output-plan-1")
            self.assertTrue(second.output_plan_reuse_hit)
            self.assertTrue(second.result_replay_reuse_hit)
            self.assertEqual(second.source_state_id, "sql-source-state-1")
            self.assertEqual(second.source_state_digest, "fnv64:sql-source-1")
            self.assertEqual(second.source_schema_digest, "fnv64:sql-schema-1")
            self.assertEqual(second.plan_digest, "fnv64:sql-plan-1")
            self.assertEqual(
                second.execution_certificate_ref,
                "sql-local-source.csv.projection-limit.execution.v1",
            )
            self.assertTrue(second.runtime_validation.passed)
            self.assertFalse(second.runtime_validation.runtime_claim_allowed)
            self.assertEqual(
                second.source_state_contract_schema_version,
                "shardloom.local_source_state.v1",
            )
            self.assertEqual(second.source_state_read_plan, "projected_source_state")
            self.assertEqual(
                second.source_state_projection_pushdown_status,
                "reader_projection_applied",
            )
            self.assertEqual(
                second.user_surface_runtime_scope,
                "format_neutral_sql_python_runtime",
            )
            self.assertEqual(
                second.format_specific_boundary_scope, "read_ingest_and_write_only"
            )
            self.assertFalse(second.format_specific_compute_path)
            self.assertEqual(
                second.source_state_materialization_layout,
                "arrow_record_batch_columnar_source_state_then_scalar_row_map",
            )
            self.assertEqual(
                second.source_state_parse_normalization,
                "structured_reader_to_arrow_record_batches_then_scalar_rows",
            )
            self.assertTrue(second.source_state_columnar_preserved)
            self.assertEqual(second.source_state_record_batch_count, 1)
            self.assertEqual(second.source_to_columnar_millis, 3)
            self.assertEqual(
                second.source_state_runtime_consumption_layout,
                "scalar_row_map_expression_runtime",
            )
            self.assertTrue(second.source_state_scalar_runtime_materialization_required)
            self.assertEqual(second.source_state_materialized_columns, ("id",))
            self.assertEqual(second.source_state_reader_projection_columns, ("id",))
            self.assertEqual(
                second.evidence()["source_state_id"], "sql-source-state-1"
            )
            self.assertEqual(
                second.evidence()["source_state_digest"], "fnv64:sql-source-1"
            )
            self.assertEqual(second.evidence()["source_schema_digest"], "fnv64:sql-schema-1")
            self.assertEqual(second.evidence()["plan_digest"], "fnv64:sql-plan-1")
            self.assertEqual(
                second.evidence()["execution_certificate_ref"],
                "sql-local-source.csv.projection-limit.execution.v1",
            )
            self.assertEqual(
                second.evidence()["runtime_envelope_validation_status"],
                "passed",
            )
            self.assertEqual(
                second.evidence()["source_state_read_plan"], "projected_source_state"
            )
            self.assertEqual(
                second.evidence()["source_state_projection_pushdown_status"],
                "reader_projection_applied",
            )
            self.assertEqual(
                second.evidence()["user_surface_runtime_scope"],
                "format_neutral_sql_python_runtime",
            )
            self.assertEqual(
                second.evidence()["format_specific_boundary_scope"],
                "read_ingest_and_write_only",
            )
            self.assertEqual(second.evidence()["format_specific_compute_path"], False)
            self.assertEqual(
                second.evidence()["source_state_materialization_layout"],
                "arrow_record_batch_columnar_source_state_then_scalar_row_map",
            )
            self.assertEqual(second.evidence()["source_state_columnar_preserved"], True)
            self.assertFalse(second.fallback_attempted)
            self.assertFalse(second.external_engine_invoked)
            self.assertEqual(count_path.read_text(encoding="utf-8"), "1")

            output_path.write_text('{"id":99,"count":99}\n', encoding="utf-8")
            third = sess.write(frame, output_path, allow_overwrite=True)
            self.assertFalse(third.reuse_hit)
            self.assertEqual(third.reuse_reason, "output_artifact_fingerprint_changed")
            self.assertEqual(third.output_plan_digest, "sha256:output-plan-2")
            self.assertEqual(count_path.read_text(encoding="utf-8"), "2")

            evidence = sess.evidence()
            self.assertEqual(evidence["cache_hit_count"], 1)
            self.assertEqual(evidence["cache_miss_count"], 2)
            self.assertEqual(evidence["source_state_reuse_count"], 1)
            self.assertEqual(evidence["output_plan_reuse_count"], 1)
            self.assertEqual(evidence["result_replay_reuse_count"], 1)
            self.assertEqual(
                evidence["last_invalidation_reason"],
                "output_artifact_fingerprint_changed",
            )
            self.assertFalse(evidence["fallback_attempted"])
            self.assertFalse(evidence["external_engine_invoked"])

    def test_context_session_reuses_local_fanout_outputs_when_fingerprints_match(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            source_path = root / "source.csv"
            jsonl_output_path = root / "out.jsonl"
            csv_output_path = root / "out.csv"
            count_path = root / "fanout-count.txt"
            source_path.write_text("id,label\n1,alpha\n2,beta\n", encoding="utf-8")
            binary = self.fake_cli(
                textwrap.dedent(
                    f"""
                    import json, sys
                    from pathlib import Path
                    count_path = Path({str(count_path)!r})
                    count = int(count_path.read_text(encoding="utf-8")) if count_path.exists() else 0
                    count += 1
                    count_path.write_text(str(count), encoding="utf-8")
                    outputs = {{}}
                    if sys.argv[1] == "run":
                        output_index = sys.argv.index("--output")
                        outputs["jsonl"] = Path(sys.argv[output_index + 1])
                        fanout_index = sys.argv.index("--fanout-output")
                        fmt, path = sys.argv[fanout_index + 1].split("=", 1)
                        outputs[fmt] = Path(path)
                    else:
                        assert sys.argv[1] == "sql-local-source-smoke", sys.argv
                        fanout_args = [arg for arg in sys.argv if arg.startswith(("jsonl=", "csv="))]
                        if "--output" in sys.argv:
                            output_index = sys.argv.index("--output")
                            outputs["jsonl"] = Path(sys.argv[output_index + 1])
                            assert len(fanout_args) == 1, sys.argv
                        else:
                            assert len(fanout_args) == 2, sys.argv
                        for item in fanout_args:
                            fmt, path = item.split("=", 1)
                            outputs[fmt] = Path(path)
                    outputs["jsonl"].write_text(json.dumps({{"id": 1, "count": count}}) + "\\n", encoding="utf-8")
                    outputs["csv"].write_text("id,count\\n1," + str(count) + "\\n", encoding="utf-8")
                    print(json.dumps({{
                        "schema_version": "shardloom.output.v2",
                        "command": "sql-local-source-smoke",
                        "status": "success",
                        "summary": "ok",
                        "human_text": "ok",
                        "fallback": {{"attempted": False, "allowed": False, "engine": None, "reason": "disabled"}},
                        "diagnostics": [],
                        "fields": [
                            {{"key": "output_route", "value": "local_fanout"}},
                            {{"key": "output_fanout_performed", "value": "true"}},
                            {{"key": "fanout_output_count", "value": "2"}},
                            {{"key": "fanout_output_formats", "value": "jsonl,csv"}},
                            {{"key": "fanout_output_paths", "value": str(outputs["jsonl"]) + "," + str(outputs["csv"])}},
                            {{"key": "fanout_output_digests", "value": f"jsonl:sha256:jsonl-{{count}},csv:sha256:csv-{{count}}"}},
                            {{"key": "fanout_output_workspace_path_safety_statuses", "value": "jsonl:true,csv:true"}},
                            {{"key": "fanout_output_commit_modes", "value": "jsonl:atomic_rename_same_directory,csv:atomic_rename_same_directory"}},
                            {{"key": "fanout_output_native_io_certificate_statuses", "value": "jsonl:certified_local_file_sink,csv:certified_local_file_sink"}},
                            {{"key": "fanout_output_replay_statuses", "value": "jsonl:verified_local_file_digest,csv:verified_local_file_digest"}},
                            {{"key": "fanout_output_fidelity_statuses", "value": "jsonl:logical_rows_replay_verified,csv:logical_rows_replay_verified_type_metadata_not_preserved"}},
                            {{"key": "fanout_output_fidelity_loss", "value": "jsonl:jsonl_text_roundtrip_not_full_type_metadata_fidelity,csv:csv_text_roundtrip_loses_static_type_metadata"}},
                            {{"key": "output_plan_digest", "value": f"sha256:fanout-output-plan-{{count}}"}},
                            {{"key": "source_state_id", "value": f"sql-source-state-{{count}}"}},
                            {{"key": "source_state_digest", "value": f"fnv64:sql-source-{{count}}"}},
                            {{"key": "source_schema_digest", "value": f"fnv64:sql-schema-{{count}}"}},
                            {{"key": "plan_digest", "value": f"fnv64:sql-plan-{{count}}"}},
                            {{"key": "execution_certificate_ref", "value": "sql-local-source.csv.fanout.execution.v1"}},
                            {{"key": "source_state_contract_schema_version", "value": "shardloom.local_source_state.v1"}},
                            {{"key": "source_state_read_plan", "value": "projected_source_state"}},
                            {{"key": "source_state_projection_pushdown_status", "value": "reader_projection_applied"}},
                            {{"key": "user_surface_runtime_scope", "value": "format_neutral_sql_python_runtime"}},
                            {{"key": "format_specific_boundary_scope", "value": "read_ingest_and_write_only"}},
                            {{"key": "format_specific_compute_path", "value": "false"}},
                            {{"key": "source_state_materialization_layout", "value": "arrow_record_batch_columnar_source_state_then_scalar_row_map"}},
                            {{"key": "source_state_parse_normalization", "value": "structured_reader_to_arrow_record_batches_then_scalar_rows"}},
                            {{"key": "source_state_columnar_preserved", "value": "true"}},
                            {{"key": "source_state_record_batch_count", "value": "1"}},
                            {{"key": "source_to_columnar_millis", "value": "3"}},
                            {{"key": "source_state_runtime_consumption_layout", "value": "scalar_row_map_expression_runtime"}},
                            {{"key": "source_state_scalar_runtime_materialization_required", "value": "true"}},
                            {{"key": "source_state_materialized_columns", "value": "id"}},
                            {{"key": "source_state_reader_projection_columns", "value": "id"}},
                            {{"key": "result_batch_state_status", "value": "shared_flat_scalar_columnar_boundary_available"}},
                            {{"key": "result_batch_state_digest", "value": f"fnv64:result-batch-{{count}}"}},
                            {{"key": "result_batch_state_layout", "value": "flat_scalar_column_vectors_v1"}},
                            {{"key": "result_batch_state_row_count", "value": "1"}},
                            {{"key": "result_batch_state_column_count", "value": "2"}},
                            {{"key": "result_batch_state_materialization_required", "value": "terminal_text_materialization_required"}},
                            {{"key": "result_batch_state_decode_required", "value": "false"}},
                            {{"key": "result_batch_state_build_millis", "value": "1"}},
                            {{"key": "output_plan_materialization_required", "value": "jsonl:terminal_text_materialization_required,csv:terminal_text_materialization_required"}},
                            {{"key": "output_plan_required_columns", "value": "id,count"}},
                            {{"key": "output_plan_ordering_required", "value": "jsonl:false,csv:false"}},
                            {{"key": "output_plan_statistics_required", "value": "jsonl:not_required_for_text_sink,csv:not_required_for_text_sink"}},
                            {{"key": "output_plan_text_materialization_boundary", "value": "jsonl:jsonl_terminal_encoder,csv:csv_terminal_encoder"}},
                            {{"key": "output_plan_conversion_blocker", "value": "jsonl:none,csv:none"}},
                            {{"key": "output_plan_type_nullability_support", "value": "jsonl:logical_values_including_nested_json_boundary,csv:flat_scalar_text_values_null_as_empty_boundary"}},
                            {{"key": "output_plan_dictionary_required", "value": "jsonl:not_applicable_text_sink,csv:not_applicable_text_sink"}},
                            {{"key": "output_plan_compression_encoding_posture", "value": "jsonl:jsonl_uncompressed_text_terminal_encoder,csv:csv_uncompressed_text_terminal_encoder"}},
                            {{"key": "output_plan_replay_depth", "value": "jsonl:write_digest_replay,csv:write_digest_replay"}},
                            {{"key": "output_layout_write_advisor_status", "value": "advisory_only_compatibility_targets"}},
                            {{"key": "output_layout_write_advisor_selected_strategy", "value": "jsonl:advisory_only_no_runtime_write_knob_applied,csv:advisory_only_no_runtime_write_knob_applied"}},
                            {{"key": "output_layout_write_advisor_runtime_decision_applied", "value": "false"}},
                            {{"key": "output_metadata_preservation_map", "value": "jsonl:field_names=preserved,row_order=preserved,row_count=digest_replay_verified,static_types=logical_json_boundary,csv:column_names=preserved,row_order=preserved,row_count=digest_replay_verified,static_types=dropped"}},
                            {{"key": "output_metadata_loss", "value": "jsonl:static_types_and_vortex_layout_metadata_not_fully_preserved,csv:static_types_nullability_and_vortex_layout_metadata_lost"}},
                            {{"key": "fanout_conversion_dag_status", "value": "shared_fanout_conversion_dag_applied"}},
                            {{"key": "fanout_shared_stage_count", "value": "3"}},
                            {{"key": "fanout_terminal_sink_count", "value": "2"}},
                            {{"key": "fanout_shared_conversion_millis", "value": "1"}},
                            {{"key": "fanout_terminal_conversion_millis", "value": "4"}},
                            {{"key": "fanout_duplicate_conversion_avoided", "value": "true"}},
                            {{"key": "output_capillary_status", "value": "applied_output_pulseweave_control"}},
                            {{"key": "output_capillary_task_roles", "value": "schema_map,columnar_export,terminal_encode,compression,local_write,digest,replay,evidence_render"}},
                            {{"key": "output_capillary_window_count", "value": "13"}},
                            {{"key": "output_sink_pressure_status", "value": "bounded_by_output_sink_pressure"}},
                            {{"key": "output_memory_pressure_status", "value": "within_declared_output_memory_budget"}},
                            {{"key": "pulseweave_output_policy_applied", "value": "true"}},
                            {{"key": "output_conversion_millis", "value": "5"}},
                            {{"key": "sink_artifact_conversion_millis", "value": "jsonl:2,csv:2"}},
                            {{"key": "fanout_output_conversion_millis", "value": "4"}},
                            {{"key": "result_reuse_for_fanout", "value": "true"}},
                            {{"key": "fanout_result_reuse_hit", "value": "true"}},
                            {{"key": "result_replay_verified", "value": "true"}},
                            {{"key": "output_replay_status", "value": "verified_local_sink_artifacts"}},
                            {{"key": "claim_gate_status", "value": "fixture_smoke_only"}},
                            {{"key": "fallback_attempted", "value": "false"}},
                            {{"key": "external_engine_invoked", "value": "false"}}
                        ],
                    }}))
                    """
                )
            )
            ctx = ShardLoomContext(client=ShardLoomClient(binary=binary))
            frame = ctx.read_csv(source_path).select("id").limit(2)
            sess = ctx.session(session_id="fanout-session")
            outputs = {
                "jsonl": str(jsonl_output_path),
                "csv": str(csv_output_path),
            }

            first = sess.fanout(frame, outputs, allow_overwrite=True)
            second = sess.fanout(frame, outputs)

            self.assertIsInstance(first, SessionSqlResult)
            self.assertFalse(first.reuse_hit)
            self.assertEqual(first.reuse_reason, "no_cached_result")
            self.assertTrue(second.reuse_hit)
            self.assertTrue(second.output_plan_reuse_hit)
            self.assertTrue(second.result_replay_reuse_hit)
            self.assertEqual(
                second.reuse_reason,
                "source_and_output_fingerprints_match",
            )
            self.assertEqual(second.output_plan_digest, "sha256:fanout-output-plan-1")
            self.assertEqual(
                second.result_batch_state_status,
                "shared_flat_scalar_columnar_boundary_available",
            )
            self.assertEqual(second.result_batch_state_digest, "fnv64:result-batch-1")
            self.assertEqual(
                second.result_batch_state_layout,
                "flat_scalar_column_vectors_v1",
            )
            self.assertEqual(second.result_batch_state_row_count, 1)
            self.assertEqual(second.result_batch_state_column_count, 2)
            self.assertEqual(
                second.result_batch_state_materialization_required,
                "terminal_text_materialization_required",
            )
            self.assertFalse(second.result_batch_state_decode_required)
            self.assertEqual(
                second.output_plan_materialization_required,
                "jsonl:terminal_text_materialization_required,csv:terminal_text_materialization_required",
            )
            self.assertEqual(second.output_plan_required_columns, ("id", "count"))
            self.assertEqual(
                second.output_plan_ordering_required,
                "jsonl:false,csv:false",
            )
            self.assertEqual(
                second.output_plan_statistics_required,
                "jsonl:not_required_for_text_sink,csv:not_required_for_text_sink",
            )
            self.assertEqual(
                second.output_plan_text_materialization_boundary,
                "jsonl:jsonl_terminal_encoder,csv:csv_terminal_encoder",
            )
            self.assertEqual(second.output_plan_conversion_blocker, "jsonl:none,csv:none")
            self.assertEqual(
                second.output_plan_type_nullability_support,
                "jsonl:logical_values_including_nested_json_boundary,csv:flat_scalar_text_values_null_as_empty_boundary",
            )
            self.assertEqual(
                second.output_plan_dictionary_required,
                "jsonl:not_applicable_text_sink,csv:not_applicable_text_sink",
            )
            self.assertEqual(
                second.output_plan_compression_encoding_posture,
                "jsonl:jsonl_uncompressed_text_terminal_encoder,csv:csv_uncompressed_text_terminal_encoder",
            )
            self.assertEqual(
                second.output_plan_replay_depth,
                "jsonl:write_digest_replay,csv:write_digest_replay",
            )
            self.assertEqual(
                second.output_layout_write_advisor_status,
                "advisory_only_compatibility_targets",
            )
            self.assertEqual(
                second.output_layout_write_advisor_selected_strategy,
                "jsonl:advisory_only_no_runtime_write_knob_applied,csv:advisory_only_no_runtime_write_knob_applied",
            )
            self.assertFalse(second.output_layout_write_advisor_runtime_decision_applied)
            self.assertIn(
                "jsonl:field_names=preserved",
                second.output_metadata_preservation_map,
            )
            self.assertIn(
                "csv:static_types_nullability_and_vortex_layout_metadata_lost",
                second.output_metadata_loss,
            )
            self.assertEqual(
                second.fanout_conversion_dag_status,
                "shared_fanout_conversion_dag_applied",
            )
            self.assertEqual(second.fanout_shared_stage_count, 3)
            self.assertEqual(second.fanout_terminal_sink_count, 2)
            self.assertEqual(second.fanout_shared_conversion_millis, 1)
            self.assertEqual(second.fanout_terminal_conversion_millis, 4)
            self.assertTrue(second.fanout_duplicate_conversion_avoided)
            self.assertEqual(
                second.output_capillary_status,
                "applied_output_pulseweave_control",
            )
            self.assertEqual(
                second.output_capillary_task_roles,
                "schema_map,columnar_export,terminal_encode,compression,local_write,digest,replay,evidence_render",
            )
            self.assertEqual(second.output_capillary_window_count, 13)
            self.assertEqual(
                second.output_sink_pressure_status,
                "bounded_by_output_sink_pressure",
            )
            self.assertEqual(
                second.output_memory_pressure_status,
                "within_declared_output_memory_budget",
            )
            self.assertTrue(second.pulseweave_output_policy_applied)
            self.assertEqual(second.output_conversion_millis, 5)
            self.assertEqual(second.sink_artifact_conversion_millis, "jsonl:2,csv:2")
            self.assertEqual(second.fanout_output_conversion_millis, 4)
            self.assertEqual(second.source_state_id, "sql-source-state-1")
            self.assertEqual(
                second.execution_certificate_ref,
                "sql-local-source.csv.fanout.execution.v1",
            )
            self.assertFalse(second.fallback_attempted)
            self.assertFalse(second.external_engine_invoked)
            self.assertEqual(count_path.read_text(encoding="utf-8"), "1")

            csv_output_path.write_text("id,count\n99,99\n", encoding="utf-8")
            third = sess.fanout(frame, outputs, allow_overwrite=True)
            self.assertFalse(third.reuse_hit)
            self.assertEqual(third.reuse_reason, "output_artifact_fingerprint_changed")
            self.assertEqual(third.output_plan_digest, "sha256:fanout-output-plan-2")
            self.assertEqual(count_path.read_text(encoding="utf-8"), "2")
            third_evidence = third.evidence()
            self.assertEqual(
                third_evidence["result_batch_state_status"],
                "shared_flat_scalar_columnar_boundary_available",
            )
            self.assertEqual(
                third_evidence["result_batch_state_digest"], "fnv64:result-batch-2"
            )
            self.assertEqual(
                third_evidence["output_plan_text_materialization_boundary"],
                "jsonl:jsonl_terminal_encoder,csv:csv_terminal_encoder",
            )
            self.assertEqual(
                third_evidence["output_plan_conversion_blocker"],
                "jsonl:none,csv:none",
            )
            self.assertEqual(
                third_evidence["output_layout_write_advisor_status"],
                "advisory_only_compatibility_targets",
            )
            self.assertFalse(
                third_evidence["output_layout_write_advisor_runtime_decision_applied"]
            )
            self.assertIn(
                "csv:column_names=preserved",
                third_evidence["output_metadata_preservation_map"],
            )
            self.assertIn(
                "jsonl:static_types_and_vortex_layout_metadata_not_fully_preserved",
                third_evidence["output_metadata_loss"],
            )
            self.assertEqual(
                third_evidence["fanout_conversion_dag_status"],
                "shared_fanout_conversion_dag_applied",
            )
            self.assertEqual(third_evidence["fanout_shared_stage_count"], 3)
            self.assertEqual(third_evidence["fanout_terminal_sink_count"], 2)
            self.assertEqual(third_evidence["fanout_shared_conversion_millis"], 1)
            self.assertEqual(third_evidence["fanout_terminal_conversion_millis"], 4)
            self.assertTrue(third_evidence["fanout_duplicate_conversion_avoided"])
            self.assertEqual(
                third_evidence["output_capillary_status"],
                "applied_output_pulseweave_control",
            )
            self.assertEqual(
                third_evidence["output_capillary_task_roles"],
                "schema_map,columnar_export,terminal_encode,compression,local_write,digest,replay,evidence_render",
            )
            self.assertEqual(third_evidence["output_capillary_window_count"], 13)
            self.assertEqual(
                third_evidence["output_sink_pressure_status"],
                "bounded_by_output_sink_pressure",
            )
            self.assertEqual(
                third_evidence["output_memory_pressure_status"],
                "within_declared_output_memory_budget",
            )
            self.assertTrue(third_evidence["pulseweave_output_policy_applied"])
            self.assertEqual(third_evidence["output_conversion_millis"], 5)

            evidence = sess.evidence()
            self.assertEqual(evidence["session_id"], "fanout-session")
            self.assertEqual(evidence["cache_hit_count"], 1)
            self.assertEqual(evidence["cache_miss_count"], 2)
            self.assertEqual(evidence["source_state_reuse_count"], 1)
            self.assertEqual(evidence["output_plan_reuse_count"], 1)
            self.assertEqual(evidence["result_replay_reuse_count"], 1)
            self.assertEqual(
                evidence["last_invalidation_reason"],
                "output_artifact_fingerprint_changed",
            )
            self.assertFalse(evidence["fallback_attempted"])
            self.assertFalse(evidence["external_engine_invoked"])

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
                elif args == ["live-hybrid-state-transition-smoke", "--format", "json"]:
                    command = "live-hybrid-state-transition-smoke"
                    fields = [
                        {"key": "schema_version", "value": "shardloom.live_hybrid_state_transition_fixture.v1"},
                        {"key": "selected_engine_mode", "value": "hybrid"},
                        {"key": "transition_kind", "value": "bounded_snapshot_retry_cleanup_fixture"},
                        {"key": "snapshot_epoch", "value": "11"},
                        {"key": "freshness_certificate_status", "value": "certified"},
                        {"key": "state_certificate_status", "value": "certified"},
                        {"key": "state_transition_certificate_status", "value": "certified"},
                        {"key": "attempt_count", "value": "2"},
                        {"key": "attempt_outcome_order", "value": "attempt-1:cancelled_cleanup_completed,attempt-2:certified"},
                        {"key": "cancellation_cleanup_completed", "value": "true"},
                        {"key": "partial_output_committed", "value": "false"},
                        {"key": "durable_checkpoint_store_used", "value": "false"},
                        {"key": "exactly_once_claim_allowed", "value": "false"},
                        {"key": "runtime_execution", "value": "true"},
                        {"key": "fallback_attempted", "value": "false"},
                        {"key": "external_engine_invoked", "value": "false"},
                    ]
                elif args == ["live-hybrid-durable-checkpoint-smoke", "target/live-checkpoint", "--format", "json"]:
                    command = "live-hybrid-durable-checkpoint-smoke"
                    fields = [
                        {"key": "schema_version", "value": "shardloom.live_hybrid_durable_checkpoint_fixture.v1"},
                        {"key": "selected_engine_mode", "value": "hybrid"},
                        {"key": "checkpoint_store_kind", "value": "local_filesystem_fixture_store"},
                        {"key": "checkpoint_dir", "value": "target/live-checkpoint"},
                        {"key": "checkpoint_path", "value": "target/live-checkpoint/cg22-live-hybrid-checkpoint.json"},
                        {"key": "changelog_path", "value": "target/live-checkpoint/cg22-live-hybrid-changelog.jsonl"},
                        {"key": "state_store_path", "value": "target/live-checkpoint/cg22-live-hybrid-state-store.json"},
                        {"key": "micro_segment_path", "value": "target/live-checkpoint/cg22-live-hybrid-vortex-micro-segment.json"},
                        {"key": "cold_vortex_segment_manifest_path", "value": "target/live-checkpoint/cg22-live-hybrid-cold-vortex-segment-manifest.json"},
                        {"key": "partial_checkpoint_path", "value": "target/live-checkpoint/cg22-live-hybrid-checkpoint.partial.json"},
                        {"key": "input_change_record_count", "value": "10"},
                        {"key": "active_state_key_count", "value": "3"},
                        {"key": "checkpoint_record_count", "value": "3"},
                        {"key": "micro_segment_record_count", "value": "10"},
                        {"key": "micro_segment_delete_vector_entry_count", "value": "3"},
                        {"key": "micro_segment_tombstone_count", "value": "1"},
                        {"key": "restored_active_state_key_count", "value": "3"},
                        {"key": "durable_checkpoint_store_used", "value": "true"},
                        {"key": "durable_checkpoint_write_performed", "value": "true"},
                        {"key": "durable_checkpoint_restore_performed", "value": "true"},
                        {"key": "durable_changelog_write_performed", "value": "true"},
                        {"key": "durable_state_store_used", "value": "true"},
                        {"key": "durable_state_store_write_performed", "value": "true"},
                        {"key": "durable_state_store_restore_performed", "value": "true"},
                        {"key": "micro_segment_persistence_performed", "value": "true"},
                        {"key": "micro_segment_restore_performed", "value": "true"},
                        {"key": "cold_vortex_segment_promotion_performed", "value": "true"},
                        {"key": "cold_vortex_segment_manifest_restore_performed", "value": "true"},
                        {"key": "restart_restore_performed", "value": "true"},
                        {"key": "partial_checkpoint_detected", "value": "true"},
                        {"key": "partial_checkpoint_committed", "value": "false"},
                        {"key": "partial_checkpoint_cleanup_completed", "value": "true"},
                        {"key": "duplicate_replay_protection_performed", "value": "true"},
                        {"key": "state_restore_status", "value": "restored_local_checkpoint_state_store_microsegment_and_cold_manifest_match"},
                        {"key": "restart_restore_status", "value": "local_restart_restore_replayed_checkpoint_state_store_and_microsegment_manifest"},
                        {"key": "duplicate_replay_protection_status", "value": "duplicate_change_sequence_replayed_once_by_sequence_key"},
                        {"key": "retry_idempotency_key", "value": "cg22-live-hybrid-local-seq-1-10-attempt-2"},
                        {"key": "state_match", "value": "true"},
                        {"key": "vortex_micro_segment_persistence_status", "value": "certified_local_vortex_micro_segment_manifest_fixture"},
                        {"key": "cold_vortex_segment_promotion_status", "value": "certified_local_cold_vortex_segment_manifest_fixture"},
                        {"key": "upstream_vortex_file_write_performed", "value": "false"},
                        {"key": "vortex_micro_segment_manifest_only", "value": "true"},
                        {"key": "cold_vortex_promotion_manifest_only", "value": "true"},
                        {"key": "freshness_certificate_status", "value": "certified"},
                        {"key": "state_certificate_status", "value": "certified"},
                        {"key": "execution_certificate_status", "value": "certified"},
                        {"key": "native_io_certificate_status", "value": "certified"},
                        {"key": "runtime_execution", "value": "true"},
                        {"key": "write_io", "value": "true"},
                        {"key": "object_store_io", "value": "false"},
                        {"key": "exactly_once_claim_allowed", "value": "false"},
                        {"key": "broker_replay_supported", "value": "false"},
                        {"key": "production_claim_allowed", "value": "false"},
                        {"key": "fallback_attempted", "value": "false"},
                        {"key": "external_engine_invoked", "value": "false"},
                    ]
                elif args == ["distributed-local-fixture-run", "2", "fault-injection", "--format", "json"]:
                    command = "distributed-local-fixture-run"
                    fields = [
                        {"key": "schema_version", "value": "shardloom.local_distributed_fixture_run.v1"},
                        {"key": "distributed_runtime_status", "value": "scoped_local_fixture_supported"},
                        {"key": "distributed_claim_gate_status", "value": "not_distributed_runtime_grade"},
                        {"key": "worker_count", "value": "2"},
                        {"key": "split_unit_count", "value": "3"},
                        {"key": "task_attempt_count", "value": "6"},
                        {"key": "task_attempt_outcome_order", "value": "split-000:success,split-001:retry_scheduled,split-001:success,split-002:success,split-002:duplicate_rejected,split-000:stale_lease_rejected"},
                        {"key": "result_fragment_count", "value": "3"},
                        {"key": "merged_row_count", "value": "3"},
                        {"key": "merged_rows", "value": "east:3:13|north:2:10|west:2:9"},
                        {"key": "merge_digest", "value": "fnv64:fixture-merge"},
                        {"key": "retry_performed", "value": "true"},
                        {"key": "duplicate_attempt_rejected", "value": "true"},
                        {"key": "stale_lease_rejected", "value": "true"},
                        {"key": "cancellation_cleanup_completed", "value": "true"},
                        {"key": "partial_output_committed", "value": "false"},
                        {"key": "repartition_performed", "value": "true"},
                        {"key": "remote_shuffle_performed", "value": "false"},
                        {"key": "skew_detected", "value": "true"},
                        {"key": "memory_budget_exceeded", "value": "false"},
                        {"key": "spill_required", "value": "false"},
                        {"key": "execution_certificate_status", "value": "certified"},
                        {"key": "native_io_certificate_status", "value": "certified"},
                        {"key": "runtime_execution", "value": "true"},
                        {"key": "production_claim_allowed", "value": "false"},
                        {"key": "distributed_performance_claim_allowed", "value": "false"},
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
        transition = client.live_hybrid_state_transition_smoke()
        self.assertEqual(transition.selected_engine_mode, "hybrid")
        self.assertEqual(transition.transition_kind, "bounded_snapshot_retry_cleanup_fixture")
        self.assertEqual(transition.snapshot_epoch, 11)
        self.assertEqual(transition.attempt_count, 2)
        self.assertEqual(
            transition.attempt_outcomes,
            ("attempt-1:cancelled_cleanup_completed", "attempt-2:certified"),
        )
        self.assertTrue(transition.all_certified)
        self.assertTrue(transition.cleanup_completed)
        self.assertFalse(transition.partial_output_committed)
        self.assertFalse(transition.durable_checkpoint_store_used)
        self.assertFalse(transition.exactly_once_claim_allowed)
        self.assertTrue(transition.runtime_execution)
        self.assertFalse(transition.fallback_attempted)
        self.assertFalse(transition.external_engine_invoked)
        checkpoint = client.live_hybrid_durable_checkpoint_smoke("target/live-checkpoint")
        self.assertEqual(checkpoint.selected_engine_mode, "hybrid")
        self.assertEqual(checkpoint.checkpoint_store_kind, "local_filesystem_fixture_store")
        self.assertEqual(checkpoint.checkpoint_dir, "target/live-checkpoint")
        self.assertEqual(
            checkpoint.checkpoint_path,
            "target/live-checkpoint/cg22-live-hybrid-checkpoint.json",
        )
        self.assertEqual(
            checkpoint.changelog_path,
            "target/live-checkpoint/cg22-live-hybrid-changelog.jsonl",
        )
        self.assertEqual(
            checkpoint.state_store_path,
            "target/live-checkpoint/cg22-live-hybrid-state-store.json",
        )
        self.assertEqual(
            checkpoint.micro_segment_path,
            "target/live-checkpoint/cg22-live-hybrid-vortex-micro-segment.json",
        )
        self.assertEqual(
            checkpoint.cold_vortex_segment_manifest_path,
            "target/live-checkpoint/cg22-live-hybrid-cold-vortex-segment-manifest.json",
        )
        self.assertEqual(
            checkpoint.partial_checkpoint_path,
            "target/live-checkpoint/cg22-live-hybrid-checkpoint.partial.json",
        )
        self.assertEqual(checkpoint.input_change_record_count, 10)
        self.assertEqual(checkpoint.active_state_key_count, 3)
        self.assertEqual(checkpoint.checkpoint_record_count, 3)
        self.assertEqual(checkpoint.restored_active_state_key_count, 3)
        self.assertEqual(checkpoint.micro_segment_record_count, 10)
        self.assertEqual(checkpoint.micro_segment_delete_vector_entry_count, 3)
        self.assertEqual(checkpoint.micro_segment_tombstone_count, 1)
        self.assertTrue(checkpoint.durable_checkpoint_store_used)
        self.assertTrue(checkpoint.durable_checkpoint_write_performed)
        self.assertTrue(checkpoint.durable_checkpoint_restore_performed)
        self.assertTrue(checkpoint.durable_changelog_write_performed)
        self.assertTrue(checkpoint.durable_state_store_used)
        self.assertTrue(checkpoint.durable_state_store_write_performed)
        self.assertTrue(checkpoint.durable_state_store_restore_performed)
        self.assertTrue(checkpoint.micro_segment_persistence_performed)
        self.assertTrue(checkpoint.micro_segment_restore_performed)
        self.assertTrue(checkpoint.cold_vortex_segment_promotion_performed)
        self.assertTrue(checkpoint.cold_vortex_segment_manifest_restore_performed)
        self.assertTrue(checkpoint.restart_restore_performed)
        self.assertTrue(checkpoint.partial_checkpoint_detected)
        self.assertFalse(checkpoint.partial_checkpoint_committed)
        self.assertTrue(checkpoint.partial_checkpoint_cleanup_completed)
        self.assertTrue(checkpoint.duplicate_replay_protection_performed)
        self.assertEqual(
            checkpoint.state_restore_status,
            "restored_local_checkpoint_state_store_microsegment_and_cold_manifest_match",
        )
        self.assertEqual(
            checkpoint.restart_restore_status,
            "local_restart_restore_replayed_checkpoint_state_store_and_microsegment_manifest",
        )
        self.assertEqual(
            checkpoint.duplicate_replay_protection_status,
            "duplicate_change_sequence_replayed_once_by_sequence_key",
        )
        self.assertEqual(
            checkpoint.retry_idempotency_key,
            "cg22-live-hybrid-local-seq-1-10-attempt-2",
        )
        self.assertTrue(checkpoint.state_match)
        self.assertEqual(
            checkpoint.vortex_micro_segment_persistence_status,
            "certified_local_vortex_micro_segment_manifest_fixture",
        )
        self.assertEqual(
            checkpoint.cold_vortex_segment_promotion_status,
            "certified_local_cold_vortex_segment_manifest_fixture",
        )
        self.assertFalse(checkpoint.upstream_vortex_file_write_performed)
        self.assertTrue(checkpoint.vortex_micro_segment_manifest_only)
        self.assertTrue(checkpoint.cold_vortex_promotion_manifest_only)
        self.assertTrue(checkpoint.all_certified)
        self.assertTrue(checkpoint.runtime_execution)
        self.assertTrue(checkpoint.write_io)
        self.assertFalse(checkpoint.object_store_io)
        self.assertFalse(checkpoint.exactly_once_claim_allowed)
        self.assertFalse(checkpoint.broker_replay_supported)
        self.assertFalse(checkpoint.production_claim_allowed)
        self.assertFalse(checkpoint.fallback_attempted)
        self.assertFalse(checkpoint.external_engine_invoked)
        distributed = client.distributed_local_fixture_run(2, "fault-injection")
        self.assertEqual(distributed.distributed_runtime_status, "scoped_local_fixture_supported")
        self.assertEqual(distributed.distributed_claim_gate_status, "not_distributed_runtime_grade")
        self.assertEqual(distributed.worker_count, 2)
        self.assertEqual(distributed.split_unit_count, 3)
        self.assertEqual(distributed.task_attempt_count, 6)
        self.assertEqual(distributed.result_fragment_count, 3)
        self.assertEqual(distributed.merged_row_count, 3)
        self.assertEqual(
            distributed.merged_rows,
            ("east:3:13", "north:2:10", "west:2:9"),
        )
        self.assertTrue(distributed.retry_performed)
        self.assertTrue(distributed.duplicate_attempt_rejected)
        self.assertTrue(distributed.stale_lease_rejected)
        self.assertTrue(distributed.cancellation_cleanup_completed)
        self.assertFalse(distributed.partial_output_committed)
        self.assertTrue(distributed.repartition_performed)
        self.assertFalse(distributed.remote_shuffle_performed)
        self.assertTrue(distributed.skew_detected)
        self.assertFalse(distributed.memory_budget_exceeded)
        self.assertFalse(distributed.spill_required)
        self.assertTrue(distributed.all_certified)
        self.assertTrue(distributed.runtime_execution)
        self.assertFalse(distributed.production_claim_allowed)
        self.assertFalse(distributed.distributed_performance_claim_allowed)
        self.assertFalse(distributed.fallback_attempted)
        self.assertFalse(distributed.external_engine_invoked)

        ctx = ShardLoomContext(client=client)
        ctx_transition = ctx.live_hybrid_state_transition_smoke()
        self.assertEqual(ctx_transition.snapshot_epoch, 11)
        self.assertTrue(ctx_transition.cleanup_completed)
        ctx_checkpoint = ctx.live_hybrid_durable_checkpoint_smoke("target/live-checkpoint")
        self.assertTrue(ctx_checkpoint.state_match)
        self.assertTrue(ctx_checkpoint.write_io)
        ctx_distributed = ctx.distributed_local_fixture_run(2, "fault-injection")
        self.assertEqual(ctx_distributed.merged_rows, distributed.merged_rows)

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
                    if scope == "certification":
                        fields.extend([
                            {"key": "best_default_certification_gate_schema_version", "value": "shardloom.best_default_certification_gate.v1"},
                            {"key": "best_default_certification_gate_report_id", "value": "gar-0032-e.best_default_certification_gate"},
                            {"key": "best_default_certification_gate_claim_gate_status", "value": "not_claim_grade"},
                            {"key": "best_default_certification_gate_correctness_evidence_required", "value": "true"},
                            {"key": "best_default_language_allowed", "value": "false"},
                            {"key": "best_default_certification_gate_best_default_claim_allowed", "value": "false"},
                            {"key": "best_default_certification_gate_runtime_execution", "value": "false"},
                            {"key": "best_default_certification_gate_fallback_attempted", "value": "false"},
                            {"key": "best_default_certification_gate_external_engine_invoked", "value": "false"},
                        ])
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
                            {"key": "user_generated_source_blocker_id", "value": "none_scoped_local_jsonl_csv_smoke_only"},
                            {"key": "user_generated_source_claim_gate_status", "value": "fixture_smoke_only"},
                            {"key": "engine_native_generated_source_support_status", "value": "fixture_smoke_supported"},
                            {"key": "engine_native_generated_source_blocker_id", "value": "none_scoped_local_range_sequence_jsonl_csv_smoke_only"},
                            {"key": "input_dataset_count", "value": "0"},
                            {"key": "source_io_performed", "value": "false"},
                            {"key": "generated_source_created", "value": "false"},
                            {"key": "output_io_performed", "value": "false"},
                            {"key": "generated_source_certificate_status", "value": "not_applicable_no_generated_rows"},
                        ])
                        api_rows = [
                            ("python_ctx_from_rows", "fixture_smoke_supported", "true", "false", "true", "false", "true", "none_scoped_local_jsonl_csv_smoke_only", "generated_source_certificate,output_native_io_certificate,execution_certificate,no_fallback_evidence", "fixture_smoke_only"),
                            ("python_ctx_range", "fixture_smoke_supported", "true", "false", "true", "false", "true", "none_scoped_local_range_jsonl_csv_smoke_only", "generated_source_certificate,output_native_io_certificate,execution_certificate,no_fallback_evidence", "fixture_smoke_only"),
                            ("python_ctx_sequence", "fixture_smoke_supported", "true", "false", "true", "false", "true", "none_scoped_local_sequence_jsonl_csv_smoke_only", "generated_source_certificate,output_native_io_certificate,execution_certificate,no_fallback_evidence", "fixture_smoke_only"),
                            ("python_ctx_literal_table", "fixture_smoke_supported", "true", "false", "true", "false", "true", "none_scoped_local_literal_table_jsonl_csv_smoke_only", "literal_table_generator_contract,generated_source_certificate,output_native_io_certificate,execution_certificate,no_fallback_evidence", "fixture_smoke_only"),
                            ("python_ctx_calendar", "fixture_smoke_supported", "true", "false", "true", "false", "true", "none_scoped_local_calendar_jsonl_csv_smoke_only", "calendar_generator_contract,generated_source_certificate,output_native_io_certificate,execution_certificate,no_fallback_evidence", "fixture_smoke_only"),
                            ("python_generated_source_write", "fixture_smoke_supported", "true", "false", "true", "false", "true", "none_supported_generated_source_write_smokes_only", "generated_source_kind,generated_source_schema_digest,generated_source_row_count,generated_source_plan_digest,output_native_io_certificate,execution_certificate,no_fallback_evidence", "fixture_smoke_only"),
                            ("sql_literal_select", "fixture_smoke_supported", "true", "false", "true", "false", "true", "none_scoped_local_sql_literal_select_jsonl_csv_smoke_only", "sql_parser,sql_binder,sql_planner,literal_projection_semantics,generated_source_certificate,output_native_io_certificate,execution_certificate,no_fallback_evidence", "fixture_smoke_only"),
                            ("sql_values", "fixture_smoke_supported", "true", "false", "true", "false", "true", "none_scoped_local_sql_values_jsonl_csv_smoke_only", "sql_parser,sql_binder,values_table_semantics,generated_source_certificate,output_native_io_certificate,execution_certificate,no_fallback_evidence", "fixture_smoke_only"),
                            ("sql_source_free_projection", "fixture_smoke_supported", "true", "false", "true", "false", "true", "none_scoped_local_sql_range_projection_jsonl_csv_smoke_only", "sql_parser,sql_binder,sql_planner,range_projection_expression_semantics,generated_source_certificate,output_native_io_certificate,execution_certificate,no_fallback_evidence", "fixture_smoke_only"),
                            ("sql_generate_series_range", "fixture_smoke_supported", "true", "false", "true", "false", "true", "none_scoped_local_sql_generate_series_range_jsonl_csv_smoke_only", "sql_parser,sql_binder,sql_table_function_contract,range_generator_semantics,scoped_projection_expression_semantics,generated_source_certificate,output_native_io_certificate,execution_certificate,no_fallback_evidence", "fixture_smoke_only"),
                            ("dataframe_source_free_projection", "fixture_smoke_supported", "true", "false", "true", "false", "true", "none_scoped_local_dataframe_literal_projection_jsonl_csv_structured_smoke_only", "dataframe_literal_projection_contract,generated_source_certificate,output_native_io_certificate,execution_certificate,no_fallback_evidence", "fixture_smoke_only"),
                            ("dataframe_generated_with_column", "fixture_smoke_supported", "true", "false", "true", "false", "true", "none_scoped_local_generated_with_column_jsonl_csv_structured_smoke_only", "generated_row_literal_projection,range_projection_expression_semantics,generated_source_certificate,output_native_io_certificate,execution_certificate,no_fallback_evidence", "fixture_smoke_only"),
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
                        alignment_rows = [
                            ("no_dataset_smoke", "smoke_only", "no_dataset_smoke", "false", "not_applicable_no_generated_rows", "not_emitted_no_output_data", "not_emitted_no_generated_rows", "not_emitted_smoke_only", "not_applicable_smoke_only", "not_applicable", "gar-novel-1a.no_dataset_smoke_not_generated_output", "no_dataset_smoke_status,capability_envelope,no_fallback_evidence", "smoke_only"),
                            ("python_generated_source_write", "fixture_smoke_supported", "user_generated_source_or_engine_native_generated_source", "true", "required_for_runtime", "required_for_runtime_output", "report_only_generated_source_facet_ref", "report_only_result_sink_span_ref", "advisory_ref_only", "not_applicable_local_output", "none_scoped_local_jsonl_csv_smoke_only", "generated_source_certificate,output_native_io_certificate,execution_certificate,no_fallback_evidence", "fixture_smoke_only"),
                            ("sql_dataframe_source_free", "report_only", "sql_dataframe_report_only", "false", "not_emitted_report_only", "not_emitted_report_only", "mapped_report_only_no_event", "mapped_report_only_no_export", "advisory_schema_only", "not_applicable", "gar-novel-1a.sql_dataframe_runtime_not_implemented", "parser_binder_or_dataframe_plan,generated_source_certificate,output_native_io_certificate,execution_certificate,no_fallback_evidence", "not_claim_grade"),
                            ("foundry_generated_output", "report_only", "foundry_report_only", "false", "not_emitted_report_only", "not_emitted_report_only", "mapped_report_only_no_event", "mapped_report_only_no_export", "not_applicable_until_runtime_proof", "shardloom.foundry_generated_output_boundary.v1", "gar-gen-1f.foundry_output_api_not_invoked", "foundry_output_api_evidence,result_dataset_written,evidence_dataset_written,generated_source_certificate,output_native_io_certificate,no_fallback_evidence", "not_claim_grade"),
                        ]
                        fields.extend([
                            {"key": "generated_source_evidence_alignment_schema_version", "value": "shardloom.generated_source_evidence_alignment.v1"},
                            {"key": "generated_source_evidence_alignment_report_id", "value": "gar-novel-1a.generated_source_cross_surface_alignment"},
                            {"key": "generated_source_evidence_alignment_docs_ref", "value": "docs/architecture/evidence-native-generated-execution-observability-confidence.md"},
                            {"key": "generated_source_evidence_alignment_contract_ref", "value": "shardloom.generated_source_certificate_contract.v1"},
                            {"key": "generated_source_evidence_alignment_api_admission_ref", "value": "shardloom.generated_source_api_admission.v1"},
                            {"key": "generated_source_evidence_alignment_openlineage_ref", "value": "GAR-NOVEL-1B.report_only_facets"},
                            {"key": "generated_source_evidence_alignment_opentelemetry_ref", "value": "GAR-NOVEL-1C.report_only_spans"},
                            {"key": "generated_source_evidence_alignment_bayesian_confidence_ref", "value": "GAR-NOVEL-1D.report_only_confidence"},
                            {"key": "generated_source_evidence_alignment_row_count", "value": str(len(alignment_rows))},
                            {"key": "generated_source_evidence_alignment_row_order", "value": ",".join(row[0] for row in alignment_rows)},
                            {"key": "generated_source_evidence_alignment_openlineage_export_enabled", "value": "false"},
                            {"key": "generated_source_evidence_alignment_opentelemetry_export_enabled", "value": "false"},
                            {"key": "generated_source_evidence_alignment_opentelemetry_network_exporter_enabled", "value": "false"},
                            {"key": "generated_source_evidence_alignment_bayesian_confidence_enabled", "value": "false"},
                            {"key": "generated_source_evidence_alignment_foundry_runtime_invoked", "value": "false"},
                            {"key": "generated_source_evidence_alignment_object_store_io_performed", "value": "false"},
                            {"key": "generated_source_evidence_alignment_fallback_attempted", "value": "false"},
                            {"key": "generated_source_evidence_alignment_external_engine_invoked", "value": "false"},
                            {"key": "generated_source_evidence_alignment_all_rows_no_fallback_no_external_engine", "value": "true"},
                            {"key": "generated_source_evidence_alignment_claim_gate_status", "value": "not_claim_grade"},
                        ])
                        for row in alignment_rows:
                            row_id, support, case, runtime, generated_cert, output_cert, lineage, telemetry, confidence, foundry_ref, blocker, evidence, claim = row
                            prefix = f"generated_source_evidence_alignment_row_{row_id}"
                            fields.extend([
                                {"key": f"{prefix}_support_status", "value": support},
                                {"key": f"{prefix}_source_free_case", "value": case},
                                {"key": f"{prefix}_runtime_execution", "value": runtime},
                                {"key": f"{prefix}_generated_source_certificate_status", "value": generated_cert},
                                {"key": f"{prefix}_output_native_io_certificate_status", "value": output_cert},
                                {"key": f"{prefix}_openlineage_facet_status", "value": lineage},
                                {"key": f"{prefix}_opentelemetry_span_status", "value": telemetry},
                                {"key": f"{prefix}_bayesian_confidence_status", "value": confidence},
                                {"key": f"{prefix}_foundry_boundary_ref", "value": foundry_ref},
                                {"key": f"{prefix}_blocker_id", "value": blocker},
                                {"key": f"{prefix}_required_evidence", "value": evidence},
                                {"key": f"{prefix}_claim_gate_status", "value": claim},
                                {"key": f"{prefix}_fallback_attempted", "value": "false"},
                                {"key": f"{prefix}_external_engine_invoked", "value": "false"},
                            ])
                    if scope == "observability":
                        lineage_rows = [
                            ("execution_mode", "ExecutionModeFacet", "shardloom_execution_mode", "run", "execution_mode,engine_mode,provider_kind,selected_mode_reason"),
                            ("no_fallback", "NoFallbackFacet", "shardloom_no_fallback", "run", "fallback_attempted,fallback_execution_allowed,external_engine_invoked"),
                            ("native_io_certificate", "NativeIoCertificateFacet", "shardloom_native_io_certificate", "input_dataset,output_dataset,run_refs", "native_io_certificate_status,native_io_certificate_ref,source_io_performed,output_io_performed,representation_transition"),
                            ("materialization_boundary", "MaterializationBoundaryFacet", "shardloom_materialization_boundary", "run,input_dataset,output_dataset", "data_decoded,data_materialized,stayed_encoded,materialization_boundary,representation_state"),
                            ("claim_gate", "ClaimGateFacet", "shardloom_claim_gate", "run", "claim_gate_status,claim_boundary,claim_blockers,workload_constitution_refs"),
                            ("generated_source", "GeneratedSourceFacet", "shardloom_generated_source", "run,output_dataset", "generated_source_kind,generated_source_schema_digest,generated_source_row_count,generated_source_plan_digest,generated_source_seed,generation_deterministic,generated_source_certificate_status"),
                            ("vortex_artifact", "VortexArtifactFacet", "shardloom_vortex_artifact", "input_dataset,output_dataset", "vortex_artifact_ref,vortex_artifact_digest,layout_summary,encoding_summary,statistics_summary,prepared_state_digest"),
                        ]
                        fields.extend([
                            {"key": "openlineage_facet_mapping_schema_version", "value": "shardloom.openlineage_facet_mapping.v1"},
                            {"key": "openlineage_facet_mapping_report_id", "value": "gar-novel-1b.openlineage_facet_mapping"},
                            {"key": "openlineage_facet_mapping_gar_id", "value": "GAR-NOVEL-1B"},
                            {"key": "openlineage_facet_mapping_docs_ref", "value": "docs/architecture/evidence-native-generated-execution-observability-confidence.md"},
                            {"key": "openlineage_facet_mapping_object_model_ref", "value": "https://openlineage.io/docs/spec/object-model/"},
                            {"key": "openlineage_facet_mapping_facets_ref", "value": "https://openlineage.io/docs/spec/facets/"},
                            {"key": "openlineage_facet_mapping_custom_facets_ref", "value": "https://openlineage.io/docs/spec/facets/custom-facets/"},
                            {"key": "openlineage_facet_mapping_producer_placeholder", "value": "https://github.com/depsilon/shardloom"},
                            {"key": "openlineage_facet_mapping_schema_url_base_placeholder", "value": "https://shardloom.io/schemas/openlineage/"},
                            {"key": "openlineage_facet_mapping_row_count", "value": str(len(lineage_rows))},
                            {"key": "openlineage_facet_mapping_row_order", "value": ",".join(row[0] for row in lineage_rows)},
                            {"key": "openlineage_facet_mapping_export_enabled", "value": "false"},
                            {"key": "openlineage_facet_mapping_event_emitted", "value": "false"},
                            {"key": "openlineage_facet_mapping_network_call_performed", "value": "false"},
                            {"key": "openlineage_facet_mapping_backend_configured", "value": "false"},
                            {"key": "openlineage_facet_mapping_client_dependency_added", "value": "false"},
                            {"key": "openlineage_facet_mapping_schema_published", "value": "false"},
                            {"key": "openlineage_facet_mapping_redaction_policy_required", "value": "true"},
                            {"key": "openlineage_facet_mapping_retention_policy_required", "value": "true"},
                            {"key": "openlineage_facet_mapping_opt_in_required", "value": "true"},
                            {"key": "openlineage_facet_mapping_all_rows_report_only", "value": "true"},
                            {"key": "openlineage_facet_mapping_all_rows_no_fallback_no_external_engine", "value": "true"},
                            {"key": "openlineage_facet_mapping_claim_gate_status", "value": "not_claim_grade"},
                        ])
                        for row_id, facet, key, entity, evidence in lineage_rows:
                            prefix = f"openlineage_facet_mapping_row_{row_id}"
                            fields.extend([
                                {"key": f"{prefix}_facet_name", "value": facet},
                                {"key": f"{prefix}_facet_key", "value": key},
                                {"key": f"{prefix}_openlineage_entity", "value": entity},
                                {"key": f"{prefix}_shardloom_evidence_fields", "value": evidence},
                                {"key": f"{prefix}_schema_url_placeholder", "value": f"https://shardloom.io/schemas/openlineage/{row_id.replace('_', '-')}-facet-v1.json"},
                                {"key": f"{prefix}_schema_version", "value": "v1"},
                                {"key": f"{prefix}_producer", "value": "https://github.com/depsilon/shardloom"},
                                {"key": f"{prefix}_facet_status", "value": "report_only_schema_placeholder"},
                                {"key": f"{prefix}_export_enabled", "value": "false"},
                                {"key": f"{prefix}_event_emitted", "value": "false"},
                                {"key": f"{prefix}_network_call_performed", "value": "false"},
                                {"key": f"{prefix}_redaction_required", "value": "true"},
                                {"key": f"{prefix}_retention_policy_required", "value": "true"},
                                {"key": f"{prefix}_claim_gate_status", "value": "not_claim_grade"},
                                {"key": f"{prefix}_claim_boundary", "value": "report-only OpenLineage facet mapping; no export, backend, network call, fallback, or production lineage claim"},
                                {"key": f"{prefix}_fallback_attempted", "value": "false"},
                                {"key": f"{prefix}_external_engine_invoked", "value": "false"},
                            ])
                        otel_rows = [
                            ("request_admission", "shardloom.request_admission", "request_admission_millis,total_runtime_millis", "execution_mode,engine_mode,capability_admission_status,selected_mode_reason,claim_gate_status,fallback_attempted,external_engine_invoked"),
                            ("source_read", "shardloom.source_read", "source_read_millis,source_discovery_millis,schema_inference_millis,source_parse_millis", "source_format,source_io_performed,source_state_digest,row_count_estimate,file_count,byte_size"),
                            ("compatibility_parse", "shardloom.compatibility_parse", "compatibility_parse_millis,compatibility_to_vortex_import_millis", "source_format,compatibility_parse_status,generated_source_created,source_io_performed,source_state_digest"),
                            ("vortex_import", "shardloom.vortex_import", "compatibility_to_vortex_import_millis,vortex_prepare_millis,vortex_write_millis,vortex_reopen_millis", "prepared_state_digest,vortex_artifact_digest,layout_summary,encoding_summary,statistics_summary"),
                            ("vortex_scan", "shardloom.vortex_scan", "vortex_scan_millis,source_backed_scan_millis", "scan_filter_pushed_down,scan_projection_pushed_down,scan_limit_pushed_down,data_decoded,data_materialized,source_backed_scan_used"),
                            ("operator_compute", "shardloom.operator_compute", "operator_compute_millis", "operator_execution_class,fused_pipeline_used,rows_scanned,rows_selected,rows_output,encoded_native_claim_allowed"),
                            ("result_sink", "shardloom.result_sink", "result_sink_write_millis,output_write_millis,output_replay_millis", "output_io_performed,output_format,output_native_io_certificate_status,result_replay_verified,output_digest"),
                            ("evidence_render", "shardloom.evidence_render", "evidence_render_millis", "execution_certificate_status,native_io_certificate_status,materialization_boundary,generated_source_certificate_status,claim_gate_status"),
                            ("claim_gate", "shardloom.claim_gate", "claim_gate_millis,evidence_render_millis,total_runtime_millis", "claim_gate_status,claim_boundary,performance_claim_allowed,production_claim_allowed,scale_claim_status"),
                        ]
                        fields.extend([
                            {"key": "opentelemetry_trace_export_schema_version", "value": "shardloom.opentelemetry_trace_export_contract.v1"},
                            {"key": "opentelemetry_trace_export_report_id", "value": "gar-novel-1c.opentelemetry_trace_export_contract"},
                            {"key": "opentelemetry_trace_export_gar_id", "value": "GAR-NOVEL-1C"},
                            {"key": "opentelemetry_trace_export_docs_ref", "value": "docs/architecture/evidence-native-generated-execution-observability-confidence.md"},
                            {"key": "opentelemetry_trace_export_traces_ref", "value": "https://opentelemetry.io/docs/concepts/signals/traces/"},
                            {"key": "opentelemetry_trace_export_common_ref", "value": "https://opentelemetry.io/docs/specs/otel/common/"},
                            {"key": "opentelemetry_trace_export_otlp_spec_ref", "value": "https://opentelemetry.io/docs/specs/otlp/"},
                            {"key": "opentelemetry_trace_export_otlp_exporter_ref", "value": "https://opentelemetry.io/docs/specs/otel/protocol/exporter/"},
                            {"key": "opentelemetry_trace_export_schema_url_base_placeholder", "value": "https://shardloom.io/schemas/opentelemetry/"},
                            {"key": "opentelemetry_trace_export_row_count", "value": str(len(otel_rows))},
                            {"key": "opentelemetry_trace_export_row_order", "value": ",".join(row[0] for row in otel_rows)},
                            {"key": "opentelemetry_trace_export_trace_export_enabled", "value": "false"},
                            {"key": "opentelemetry_trace_export_metric_export_enabled", "value": "false"},
                            {"key": "opentelemetry_trace_export_log_export_enabled", "value": "false"},
                            {"key": "opentelemetry_trace_export_otlp_exporter_configured", "value": "false"},
                            {"key": "opentelemetry_trace_export_network_exporter_enabled", "value": "false"},
                            {"key": "opentelemetry_trace_export_collector_configured", "value": "false"},
                            {"key": "opentelemetry_trace_export_sdk_dependency_added", "value": "false"},
                            {"key": "opentelemetry_trace_export_runtime_collection_enabled", "value": "false"},
                            {"key": "opentelemetry_trace_export_trace_emitted", "value": "false"},
                            {"key": "opentelemetry_trace_export_metric_emitted", "value": "false"},
                            {"key": "opentelemetry_trace_export_log_emitted", "value": "false"},
                            {"key": "opentelemetry_trace_export_network_call_performed", "value": "false"},
                            {"key": "opentelemetry_trace_export_attribute_allowlist_required", "value": "true"},
                            {"key": "opentelemetry_trace_export_redaction_policy_required", "value": "true"},
                            {"key": "opentelemetry_trace_export_retention_policy_required", "value": "true"},
                            {"key": "opentelemetry_trace_export_opt_in_required", "value": "true"},
                            {"key": "opentelemetry_trace_export_all_rows_report_only", "value": "true"},
                            {"key": "opentelemetry_trace_export_all_rows_no_fallback_no_external_engine", "value": "true"},
                            {"key": "opentelemetry_trace_export_no_export_side_effects", "value": "true"},
                            {"key": "opentelemetry_trace_export_claim_gate_status", "value": "not_claim_grade"},
                        ])
                        for row_id, span_name, timing, attrs in otel_rows:
                            prefix = f"opentelemetry_trace_export_span_{row_id}"
                            fields.extend([
                                {"key": f"{prefix}_span_name", "value": span_name},
                                {"key": f"{prefix}_span_kind", "value": "internal"},
                                {"key": f"{prefix}_timing_fields", "value": timing},
                                {"key": f"{prefix}_shardloom_attribute_allowlist", "value": attrs},
                                {"key": f"{prefix}_redaction_policy", "value": "allowlist_only_redact_paths_query_text_credentials_headers"},
                                {"key": f"{prefix}_sensitive_fields", "value": "query_text,source_location,output_location,credential,headers"},
                                {"key": f"{prefix}_metric_refs", "value": f"{row_id}_millis"},
                                {"key": f"{prefix}_span_status", "value": "report_only_not_emitted"},
                                {"key": f"{prefix}_export_enabled", "value": "false"},
                                {"key": f"{prefix}_span_emitted", "value": "false"},
                                {"key": f"{prefix}_metric_emitted", "value": "false"},
                                {"key": f"{prefix}_log_emitted", "value": "false"},
                                {"key": f"{prefix}_network_exporter_enabled", "value": "false"},
                                {"key": f"{prefix}_redaction_required", "value": "true"},
                                {"key": f"{prefix}_retention_policy_required", "value": "true"},
                                {"key": f"{prefix}_claim_gate_status", "value": "not_claim_grade"},
                                {"key": f"{prefix}_claim_boundary", "value": "report-only OpenTelemetry span mapping; no SDK, exporter, collector, network call, fallback, or production tracing claim"},
                                {"key": f"{prefix}_fallback_attempted", "value": "false"},
                                {"key": f"{prefix}_external_engine_invoked", "value": "false"},
                            ])
                    if scope in {"python", "dataframe", "notebook", "deployment", "api-surfaces"}:
                        readiness_rows = [
                            (
                                "python_package_metadata",
                                "package",
                                "Python package metadata and source-tree import",
                                "ready_local",
                                "true",
                                "SL_PACKAGE_METADATA_LOCAL_READY",
                                "none_local_metadata_only",
                            ),
                            (
                                "editable_install_smoke",
                                "package",
                                "Editable/source-tree local install smoke",
                                "smoke_supported",
                                "true",
                                "SL_EDITABLE_INSTALL_SMOKE_ONLY",
                                "gar-0024.public_package_publication_gate_required",
                            ),
                            (
                                "dataframe_method_matrix",
                                "dataframe",
                                "DataFrame/query-builder method capability matrix",
                                "report_only",
                                "false",
                                "SL_DATAFRAME_METHOD_MATRIX_REPORT_ONLY",
                                "gar-0010-b.broad_dataframe_runtime_evidence_missing",
                            ),
                            (
                                "notebook_display_surface",
                                "notebook",
                                "Notebook rich display and materialization boundary",
                                "blocked",
                                "false",
                                "SL_NOTEBOOK_DISPLAY_UNSUPPORTED",
                                "cg21.workflow.display.rich_display_unsupported",
                            ),
                            (
                                "public_package_publication",
                                "package",
                                "TestPyPI, PyPI, Conda, Homebrew, and installer channels",
                                "blocked",
                                "false",
                                "SL_PUBLIC_PACKAGE_PUBLICATION_BLOCKED",
                                "gar-0024.package_publication_gate_required",
                            ),
                            (
                                "unsupported_diagnostics",
                                "diagnostics",
                                "Deterministic unsupported diagnostics for DataFrame/notebook/package requests",
                                "ready_local",
                                "false",
                                "SL_UNSUPPORTED_WORKFLOW_DIAGNOSTIC_READY",
                                "none_diagnostics_ready",
                            ),
                        ]
                        fields.extend([
                            {"key": "dataframe_notebook_package_readiness_schema_version", "value": "shardloom.dataframe_notebook_package_readiness.v1"},
                            {"key": "dataframe_notebook_package_readiness_report_id", "value": "gar-0010-b.dataframe_notebook_package_readiness"},
                            {"key": "dataframe_notebook_package_readiness_docs_ref", "value": "docs/architecture/dataframe-notebook-package-readiness.md"},
                            {"key": "dataframe_notebook_package_readiness_source_refs", "value": "RFC 0010,RFC 0024,RFC 0032,python/README.md"},
                            {"key": "dataframe_notebook_package_readiness_support_status_vocabulary", "value": "ready_local,smoke_supported,report_only,blocked"},
                            {"key": "dataframe_notebook_package_readiness_row_count", "value": str(len(readiness_rows))},
                            {"key": "dataframe_notebook_package_readiness_row_order", "value": ",".join(row[0] for row in readiness_rows)},
                            {"key": "dataframe_notebook_package_readiness_ready_local_count", "value": "2"},
                            {"key": "dataframe_notebook_package_readiness_smoke_supported_count", "value": "1"},
                            {"key": "dataframe_notebook_package_readiness_report_only_count", "value": "1"},
                            {"key": "dataframe_notebook_package_readiness_blocked_count", "value": "2"},
                            {"key": "dataframe_notebook_package_readiness_local_install_smoke_supported", "value": "true"},
                            {"key": "dataframe_notebook_package_readiness_installed_package_smoke_distinct_from_runtime_support", "value": "true"},
                            {"key": "dataframe_notebook_package_readiness_dataframe_runtime_supported", "value": "false"},
                            {"key": "dataframe_notebook_package_readiness_notebook_runtime_supported", "value": "false"},
                            {"key": "dataframe_notebook_package_readiness_package_publication_ready", "value": "false"},
                            {"key": "dataframe_notebook_package_readiness_package_publication_claim_allowed", "value": "false"},
                            {"key": "dataframe_notebook_package_readiness_dataframe_runtime_claim_allowed", "value": "false"},
                            {"key": "dataframe_notebook_package_readiness_notebook_runtime_claim_allowed", "value": "false"},
                            {"key": "dataframe_notebook_package_readiness_fallback_attempted", "value": "false"},
                            {"key": "dataframe_notebook_package_readiness_external_engine_invoked", "value": "false"},
                            {"key": "dataframe_notebook_package_readiness_all_rows_no_runtime_claims", "value": "true"},
                            {"key": "dataframe_notebook_package_readiness_claim_gate_status", "value": "not_claim_grade"},
                            {"key": "dataframe_notebook_package_readiness_claim_boundary", "value": "Local package/import smoke and report-only DataFrame/notebook readiness only"},
                        ])
                        for row in readiness_rows:
                            row_id, family, surface, support_status, local_smoke, diagnostic, blocker = row
                            prefix = f"dataframe_notebook_package_readiness_row_{row_id}"
                            fields.extend([
                                {"key": f"{prefix}_family", "value": family},
                                {"key": f"{prefix}_surface", "value": surface},
                                {"key": f"{prefix}_support_status", "value": support_status},
                                {"key": f"{prefix}_local_install_smoke", "value": local_smoke},
                                {"key": f"{prefix}_package_publication_allowed", "value": "false"},
                                {"key": f"{prefix}_dataframe_runtime_supported", "value": "false"},
                                {"key": f"{prefix}_notebook_runtime_supported", "value": "false"},
                                {"key": f"{prefix}_deterministic_diagnostic_code", "value": diagnostic},
                                {"key": f"{prefix}_blocker_id", "value": blocker},
                                {"key": f"{prefix}_required_evidence", "value": "package_metadata,capability_view,no_fallback_evidence"},
                                {"key": f"{prefix}_claim_gate_status", "value": "not_claim_grade"},
                                {"key": f"{prefix}_fallback_attempted", "value": "false"},
                                {"key": f"{prefix}_external_engine_invoked", "value": "false"},
                                {"key": f"{prefix}_claim_boundary", "value": "readiness only; no runtime or publication claim"},
                            ])
                    if scope == "api-surfaces":
                        wrapper_rows = [
                            (
                                "python_cli_json_client",
                                "language_sdk",
                                "shardloom-python",
                                "w5_execute_certified_local_paths",
                                "cli_subprocess",
                                "ready_local",
                                "python.src.shardloom.client",
                                "source_tree_python_client",
                                "none_supported_local_cli_json_wrapper",
                                "output_envelope,no_fallback_policy",
                                "true",
                                "Ready local Python wrapper only",
                            ),
                            (
                                "sqlalchemy",
                                "python_ecosystem",
                                "sqlalchemy-shardloom",
                                "w0_declared_only",
                                "rest_http",
                                "blocked",
                                "SQLAlchemy dialect",
                                "not_implemented",
                                "SL_SQLALCHEMY_CONNECTOR_UNSUPPORTED",
                                "dialect_contract,no_fallback_policy",
                                "false",
                                "SQLAlchemy remains blocked",
                            ),
                        ]
                        fields.extend([
                            {"key": "wrapper_connector_registry_schema_version", "value": "shardloom.wrapper_connector_implementation_registry.v1"},
                            {"key": "wrapper_connector_registry_report_id", "value": "gar-0037-a.wrapper_connector_implementation_registry"},
                            {"key": "wrapper_connector_registry_docs_ref", "value": "docs/architecture/wrapper-connector-implementation-registry.md"},
                            {"key": "wrapper_connector_registry_support_status_vocabulary", "value": "ready_local,report_only,blocked"},
                            {"key": "wrapper_connector_registry_row_count", "value": str(len(wrapper_rows))},
                            {"key": "wrapper_connector_registry_row_order", "value": ",".join(row[0] for row in wrapper_rows)},
                            {"key": "wrapper_connector_registry_ready_local_count", "value": "1"},
                            {"key": "wrapper_connector_registry_report_only_count", "value": "0"},
                            {"key": "wrapper_connector_registry_blocked_count", "value": "1"},
                            {"key": "wrapper_connector_registry_diagnostic_codes", "value": ",".join(row[8] for row in wrapper_rows)},
                            {"key": "wrapper_connector_registry_required_evidence", "value": ",".join(row[9] for row in wrapper_rows)},
                            {"key": "wrapper_connector_registry_dependency_expansion_allowed", "value": "false"},
                            {"key": "wrapper_connector_registry_wrapper_ecosystem_claim_allowed", "value": "false"},
                            {"key": "wrapper_connector_registry_fallback_attempted", "value": "false"},
                            {"key": "wrapper_connector_registry_external_engine_invoked", "value": "false"},
                            {"key": "wrapper_connector_registry_all_rows_no_fallback_no_external_engine", "value": "true"},
                            {"key": "wrapper_connector_registry_claim_gate_status", "value": "not_claim_grade"},
                        ])
                        for row in wrapper_rows:
                            (
                                row_id,
                                family,
                                planned_package,
                                maturity,
                                transport,
                                support_status,
                                surface,
                                evidence,
                                diagnostic,
                                required,
                                explicit_execution,
                                claim_boundary,
                            ) = row
                            prefix = f"wrapper_connector_registry_row_{row_id}"
                            fields.extend([
                                {"key": f"{prefix}_family", "value": family},
                                {"key": f"{prefix}_planned_package", "value": planned_package},
                                {"key": f"{prefix}_maturity", "value": maturity},
                                {"key": f"{prefix}_primary_transport", "value": transport},
                                {"key": f"{prefix}_support_status", "value": support_status},
                                {"key": f"{prefix}_user_visible_surface", "value": surface},
                                {"key": f"{prefix}_implementation_evidence", "value": evidence},
                                {"key": f"{prefix}_deterministic_diagnostic_code", "value": diagnostic},
                                {"key": f"{prefix}_required_evidence", "value": required},
                                {"key": f"{prefix}_explicit_execution_available", "value": explicit_execution},
                                {"key": f"{prefix}_dependency_added", "value": "false"},
                                {"key": f"{prefix}_network_listener_started", "value": "false"},
                                {"key": f"{prefix}_data_plane_bridge_supported", "value": "false"},
                                {"key": f"{prefix}_external_engine_invoked", "value": "false"},
                                {"key": f"{prefix}_fallback_attempted", "value": "false"},
                                {"key": f"{prefix}_claim_gate_status", "value": "not_claim_grade"},
                                {"key": f"{prefix}_claim_boundary", "value": claim_boundary},
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
                    if scope == "workflow":
                        fields.extend([
                            {"key": "etl_workflow_matrix_schema_version", "value": "shardloom.etl_workflow_capability_matrix.v1"},
                            {"key": "etl_workflow_matrix_id", "value": "gar-0033-a.etl_workflow_capability_matrix"},
                            {"key": "etl_workflow_row_order", "value": "first_10_minutes_local_smoke,local_csv_parquet_certified_workload,prepared_native_vortex_batch_smoke,source_free_user_rows_jsonl_csv,source_free_range_jsonl_csv,source_free_literal_table_jsonl_csv,source_free_calendar_jsonl_csv,dirty_csv_fixture,nested_json_fixture,cdc_overlay_fixture,sql_dataframe_capability_posture,data_quality_api,object_store_runtime,table_lakehouse_runtime,production_etl_certification"},
                            {"key": "etl_workflow_row_count", "value": "15"},
                            {"key": "etl_workflow_supported_local_rows", "value": "first_10_minutes_local_smoke,local_csv_parquet_certified_workload,prepared_native_vortex_batch_smoke,source_free_user_rows_jsonl_csv,source_free_range_jsonl_csv,source_free_literal_table_jsonl_csv,source_free_calendar_jsonl_csv,dirty_csv_fixture,nested_json_fixture,cdc_overlay_fixture"},
                            {"key": "etl_workflow_supported_local_count", "value": "10"},
                            {"key": "etl_workflow_report_only_rows", "value": "sql_dataframe_capability_posture,data_quality_api"},
                            {"key": "etl_workflow_report_only_count", "value": "2"},
                            {"key": "etl_workflow_blocked_rows", "value": "object_store_runtime,table_lakehouse_runtime,production_etl_certification"},
                            {"key": "etl_workflow_blocked_count", "value": "3"},
                            {"key": "etl_workflow_required_evidence", "value": "correctness_digest,execution_certificate,native_io_certificate,materialization_boundary,result_sink_evidence,source_state_evidence,generated_source_certificate,output_native_io_certificate,claim_gate_status,no_fallback_evidence"},
                            {"key": "etl_workflow_claim_boundary", "value": "local workflow claims only for already certified or smoke-supported technical-preview paths; production ETL, broad SQL/DataFrame, object-store/lakehouse, Foundry, package, performance, and Spark-displacement claims remain blocked"},
                            {"key": "etl_workflow_claim_gate_status", "value": "not_claim_grade"},
                            {"key": "etl_workflow_fallback_attempted", "value": "false"},
                            {"key": "etl_workflow_external_engine_invoked", "value": "false"},
                            {"key": "etl_workflow_production_etl_claim_allowed", "value": "false"},
                            {"key": "etl_workflow_object_store_runtime_supported", "value": "false"},
                            {"key": "etl_workflow_table_lakehouse_runtime_supported", "value": "false"},
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
        readiness = capabilities.dataframe_notebook_package_readiness
        self.assertIsInstance(readiness, DataFrameNotebookPackageReadinessReport)
        self.assertEqual(
            readiness.schema_version,
            "shardloom.dataframe_notebook_package_readiness.v1",
        )
        self.assertEqual(
            readiness.report_id,
            "gar-0010-b.dataframe_notebook_package_readiness",
        )
        self.assertEqual(
            readiness.row_order,
            (
                "python_package_metadata",
                "editable_install_smoke",
                "dataframe_method_matrix",
                "notebook_display_surface",
                "public_package_publication",
                "unsupported_diagnostics",
            ),
        )
        self.assertTrue(readiness.local_install_smoke_supported)
        self.assertTrue(readiness.installed_package_smoke_distinct_from_runtime_support)
        self.assertFalse(readiness.dataframe_runtime_supported)
        self.assertFalse(readiness.notebook_runtime_supported)
        self.assertFalse(readiness.package_publication_ready)
        self.assertFalse(readiness.package_publication_claim_allowed)
        self.assertFalse(readiness.dataframe_runtime_claim_allowed)
        self.assertFalse(readiness.notebook_runtime_claim_allowed)
        self.assertTrue(readiness.all_rows_no_fallback_no_external_engine)
        self.assertTrue(readiness.all_rows_no_runtime_claims)
        self.assertTrue(readiness.row("python-package-metadata").ready_local)
        self.assertTrue(readiness.row("editable-install-smoke").smoke_supported)
        self.assertTrue(readiness.row("dataframe-method-matrix").report_only)
        self.assertTrue(readiness.row("notebook-display-surface").blocked)
        self.assertEqual(
            readiness.row("public-package-publication").blocker_id,
            "gar-0024.package_publication_gate_required",
        )
        self.assertEqual(
            capabilities.python.dataframe_notebook_package_readiness.schema_version,
            "shardloom.dataframe_notebook_package_readiness.v1",
        )
        self.assertTrue(
            capabilities.deployment.dataframe_notebook_package_readiness
            .row("public_package_publication")
            .blocked
        )
        self.assertEqual(
            ctx.dataframe_notebook_package_readiness().row(
                "notebook_display_surface"
            ).deterministic_diagnostic_code,
            "SL_NOTEBOOK_DISPLAY_UNSUPPORTED",
        )
        wrapper_registry = capabilities.wrapper_connector_registry
        self.assertEqual(
            wrapper_registry.schema_version,
            "shardloom.wrapper_connector_implementation_registry.v1",
        )
        self.assertEqual(wrapper_registry.ready_local_count, 1)
        self.assertEqual(wrapper_registry.blocked_count, 1)
        self.assertFalse(wrapper_registry.wrapper_ecosystem_claim_allowed)
        self.assertTrue(wrapper_registry.all_rows_no_fallback_no_external_engine)
        self.assertTrue(wrapper_registry.row("python-cli-json-client").ready_local)
        self.assertTrue(wrapper_registry.row("sqlalchemy").blocked)
        self.assertEqual(
            wrapper_registry.row("sqlalchemy").deterministic_diagnostic_code,
            "SL_SQLALCHEMY_CONNECTOR_UNSUPPORTED",
        )
        etl_workflows = capabilities.etl_workflow_matrix
        self.assertIsInstance(etl_workflows, ETLWorkflowCapabilityMatrix)
        self.assertEqual(
            etl_workflows.schema_version,
            "shardloom.etl_workflow_capability_matrix.v1",
        )
        self.assertEqual(
            etl_workflows.matrix_id,
            "gar-0033-a.etl_workflow_capability_matrix",
        )
        self.assertEqual(len(etl_workflows.rows), 15)
        self.assertIn(
            "local_csv_parquet_certified_workload",
            etl_workflows.supported_local_rows,
        )
        self.assertIn("source_free_user_rows_jsonl_csv", etl_workflows.supported_local_rows)
        self.assertIn("source_free_literal_table_jsonl_csv", etl_workflows.supported_local_rows)
        self.assertIn("source_free_calendar_jsonl_csv", etl_workflows.supported_local_rows)
        self.assertIn("dirty_csv_fixture", etl_workflows.supported_local_rows)
        self.assertEqual(
            etl_workflows.report_only_rows,
            ("sql_dataframe_capability_posture", "data_quality_api"),
        )
        self.assertIn("object_store_runtime", etl_workflows.blocked_rows)
        self.assertIn("table_lakehouse_runtime", etl_workflows.blocked_rows)
        self.assertIn("production_etl_certification", etl_workflows.blocked_rows)
        self.assertTrue(etl_workflows.row("object-store-runtime").blocked)
        self.assertEqual(
            etl_workflows.row("data_quality_api").blocker_id,
            "cg21.workflow.data_quality.checks_unsupported",
        )
        self.assertTrue(etl_workflows.all_no_fallback_no_external_engine)
        self.assertFalse(etl_workflows.production_etl_claim_allowed)
        self.assertFalse(etl_workflows.object_store_or_table_runtime_supported)
        self.assertEqual(capabilities.functions.capability_state, "planned")
        self.assertEqual(
            capabilities.certification.field("best_default_certification_gate_schema_version"),
            "shardloom.best_default_certification_gate.v1",
        )
        self.assertEqual(
            capabilities.certification.field("best_default_certification_gate_report_id"),
            "gar-0032-e.best_default_certification_gate",
        )
        self.assertEqual(
            capabilities.certification.field("best_default_certification_gate_claim_gate_status"),
            "not_claim_grade",
        )
        self.assertIn("not_claim_grade", capabilities.certification.claim_gate_statuses)
        self.assertFalse(
            capabilities.certification.envelope.field_bool(
                "best_default_language_allowed", True
            )
        )
        self.assertFalse(
            capabilities.certification.envelope.field_bool(
                "best_default_certification_gate_best_default_claim_allowed", True
            )
        )
        self.assertFalse(
            capabilities.certification.envelope.field_bool(
                "best_default_certification_gate_runtime_execution", True
            )
        )
        self.assertFalse(
            capabilities.certification.envelope.field_bool(
                "best_default_certification_gate_fallback_attempted", True
            )
        )
        self.assertFalse(
            capabilities.certification.envelope.field_bool(
                "best_default_certification_gate_external_engine_invoked", True
            )
        )
        self.assertIn(
            "best_default_certification_gate_correctness_evidence_required",
            capabilities.certification.required_gates,
        )
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
            "none_scoped_local_jsonl_csv_smoke_only",
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
            "none_scoped_local_range_sequence_jsonl_csv_smoke_only",
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
            "none_scoped_local_range_sequence_jsonl_csv_smoke_only",
        )
        api_admission = capabilities.python.generated_source_api_admission
        self.assertIsInstance(api_admission, GeneratedSourceApiAdmissionMatrix)
        self.assertTrue(api_admission.present)
        self.assertEqual(
            api_admission.python_row_order,
            (
                "python_ctx_from_rows",
                "python_ctx_range",
                "python_ctx_sequence",
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
        self.assertTrue(api_admission.row("sql_values").fixture_smoke_supported)
        self.assertTrue(api_admission.row("sql_values").runtime_execution)
        self.assertEqual(
            api_admission.row("sql_values").blocker_id,
            "none_scoped_local_sql_values_jsonl_csv_smoke_only",
        )
        self.assertEqual(
            capabilities.sql_support.generated_source_api_admission.row(
                "sql_literal_select"
            ).support_status,
            "fixture_smoke_supported",
        )
        self.assertEqual(
            capabilities.dataframe.generated_source_api_admission.row(
                "dataframe_generated_with_column"
            ).claim_gate_status,
            "fixture_smoke_only",
        )
        self.assertTrue(
            capabilities.dataframe.generated_source_api_admission.row(
                "dataframe_generated_with_column"
            ).fixture_smoke_supported
        )
        self.assertTrue(
            capabilities.dataframe.generated_source_api_admission.row(
                "dataframe_source_free_projection"
            ).fixture_smoke_supported
        )
        self.assertTrue(
            capabilities.dataframe.generated_source_api_admission.row(
                "dataframe_source_free_projection"
            ).runtime_execution
        )
        self.assertEqual(
            capabilities.dataframe.generated_source_api_admission.row(
                "dataframe_source_free_projection"
            ).blocker_id,
            "none_scoped_local_dataframe_literal_projection_jsonl_csv_structured_smoke_only",
        )
        self.assertTrue(
            capabilities.dataframe.generated_source_api_admission.row(
                "dataframe_generated_with_column"
            ).runtime_execution
        )
        self.assertEqual(
            capabilities.dataframe.generated_source_api_admission.row(
                "dataframe_generated_with_column"
            ).blocker_id,
            "none_scoped_local_generated_with_column_jsonl_csv_structured_smoke_only",
        )
        self.assertTrue(
            capabilities.api_surfaces.generated_source_api_admission.row(
                "python_ctx_calendar"
            ).fixture_smoke_supported
        )
        self.assertTrue(
            capabilities.api_surfaces.generated_source_api_admission.row(
                "python_ctx_literal_table"
            ).runtime_execution
        )
        evidence_alignment = capabilities.python.generated_source_evidence_alignment
        self.assertIsInstance(
            evidence_alignment, GeneratedSourceEvidenceAlignmentReport
        )
        self.assertTrue(evidence_alignment.present)
        self.assertEqual(
            evidence_alignment.schema_version,
            "shardloom.generated_source_evidence_alignment.v1",
        )
        self.assertEqual(
            evidence_alignment.row_order,
            (
                "no_dataset_smoke",
                "python_generated_source_write",
                "sql_dataframe_source_free",
                "foundry_generated_output",
            ),
        )
        self.assertEqual(evidence_alignment.claim_gate_status, "not_claim_grade")
        self.assertTrue(evidence_alignment.all_no_fallback_no_external_engine)
        self.assertFalse(evidence_alignment.openlineage_export_enabled)
        self.assertFalse(evidence_alignment.opentelemetry_export_enabled)
        self.assertFalse(evidence_alignment.opentelemetry_network_exporter_enabled)
        self.assertFalse(evidence_alignment.bayesian_confidence_enabled)
        self.assertEqual(
            evidence_alignment.row("python_generated_source_write").support_status,
            "fixture_smoke_supported",
        )
        self.assertTrue(
            evidence_alignment.row("python_generated_source_write").runtime_execution
        )
        self.assertEqual(
            evidence_alignment.row("sql_dataframe_source_free").support_status,
            "report_only",
        )
        self.assertFalse(
            evidence_alignment.row("sql_dataframe_source_free").runtime_execution
        )
        self.assertEqual(
            evidence_alignment.row("foundry_generated_output").foundry_boundary_ref,
            "shardloom.foundry_generated_output_boundary.v1",
        )
        self.assertEqual(
            capabilities.sql_support.generated_source_evidence_alignment.row(
                "sql_dataframe_source_free"
            ).claim_gate_status,
            "not_claim_grade",
        )
        self.assertTrue(capabilities.dataframe.planner_readiness_non_executing)
        dataframe_methods = capabilities.dataframe_method_matrix
        self.assertIsInstance(dataframe_methods, DataFrameMethodCapabilityMatrix)
        self.assertEqual(dataframe_methods.scope, "dataframe")
        self.assertIn("filter", dataframe_methods.plan_only_methods)
        self.assertIn("where", dataframe_methods.plan_only_methods)
        self.assertIn("query", dataframe_methods.plan_only_methods)
        self.assertIn("select", dataframe_methods.plan_only_methods)
        self.assertIn("project", dataframe_methods.plan_only_methods)
        self.assertNotIn("join", dataframe_methods.unsupported_methods)
        self.assertNotIn("agg", dataframe_methods.unsupported_methods)
        self.assertNotIn("groupby", dataframe_methods.unsupported_methods)
        self.assertNotIn("with_columns", dataframe_methods.unsupported_methods)
        self.assertNotIn("assign", dataframe_methods.unsupported_methods)
        self.assertNotIn("order_by", dataframe_methods.unsupported_methods)
        self.assertNotIn("sort_values", dataframe_methods.unsupported_methods)
        self.assertNotIn("distinct", dataframe_methods.unsupported_methods)
        self.assertNotIn("drop_duplicates", dataframe_methods.unsupported_methods)
        self.assertNotIn("unique", dataframe_methods.unsupported_methods)
        self.assertNotIn("window", dataframe_methods.unsupported_methods)
        self.assertNotIn("data_quality", dataframe_methods.unsupported_methods)
        self.assertNotIn("schema_contract", dataframe_methods.unsupported_methods)
        self.assertNotIn("profile", dataframe_methods.unsupported_methods)
        self.assertNotIn("quarantine", dataframe_methods.unsupported_methods)
        self.assertNotIn("object_store_generated_output", dataframe_methods.unsupported_methods)
        self.assertNotIn("foundry_generated_output", dataframe_methods.unsupported_methods)
        self.assertNotIn("sql", dataframe_methods.unsupported_methods)
        self.assertNotIn("from_pandas", dataframe_methods.unsupported_methods)
        self.assertNotIn("rename", dataframe_methods.unsupported_methods)
        self.assertNotIn("rename_columns", dataframe_methods.unsupported_methods)
        self.assertNotIn("drop", dataframe_methods.unsupported_methods)
        self.assertNotIn("drop_columns", dataframe_methods.unsupported_methods)
        self.assertNotIn("astype", dataframe_methods.unsupported_methods)
        self.assertNotIn("dropna", dataframe_methods.unsupported_methods)
        self.assertIn("sample", dataframe_methods.unsupported_methods)
        self.assertIn("explode", dataframe_methods.unsupported_methods)
        self.assertNotIn("merge", dataframe_methods.unsupported_methods)
        self.assertNotIn("concat", dataframe_methods.unsupported_methods)
        self.assertIn("pivot", dataframe_methods.unsupported_methods)
        self.assertIn("pivot_table", dataframe_methods.unsupported_methods)
        self.assertIn("melt", dataframe_methods.unsupported_methods)
        self.assertIn("rolling", dataframe_methods.unsupported_methods)
        self.assertNotIn("nunique", dataframe_methods.unsupported_methods)
        self.assertNotIn("value_counts", dataframe_methods.unsupported_methods)
        self.assertNotIn("nlargest", dataframe_methods.unsupported_methods)
        self.assertNotIn("nsmallest", dataframe_methods.unsupported_methods)
        self.assertNotIn("fillna", dataframe_methods.unsupported_methods)
        self.assertNotIn("fill_null", dataframe_methods.unsupported_methods)
        self.assertNotIn("isna", dataframe_methods.unsupported_methods)
        self.assertNotIn("isnull", dataframe_methods.unsupported_methods)
        self.assertNotIn("notna", dataframe_methods.unsupported_methods)
        self.assertNotIn("notnull", dataframe_methods.unsupported_methods)
        self.assertIn("duplicated", dataframe_methods.unsupported_methods)
        self.assertIn("mask", dataframe_methods.unsupported_methods)
        self.assertIn("replace", dataframe_methods.unsupported_methods)
        self.assertIn("set_index", dataframe_methods.unsupported_methods)
        self.assertIn("reset_index", dataframe_methods.unsupported_methods)
        self.assertIn("sort_index", dataframe_methods.unsupported_methods)
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
            dataframe_methods.row("head").support_status,
            "fixture_smoke_supported",
        )
        self.assertTrue(dataframe_methods.row("head").runtime_execution)
        self.assertTrue(dataframe_methods.row("take").data_read)
        self.assertEqual(
            dataframe_methods.row("with_column").support_status,
            "fixture_smoke_supported",
        )
        self.assertTrue(dataframe_methods.row("with_column").runtime_execution)
        self.assertIn(
            "computed_projection_evidence",
            dataframe_methods.row("with_column").required_evidence,
        )
        self.assertEqual(
            dataframe_methods.row("with_columns").support_status,
            "fixture_smoke_supported",
        )
        self.assertTrue(dataframe_methods.row("with_columns").runtime_execution)
        self.assertEqual(
            dataframe_methods.row("assign").support_status,
            "fixture_smoke_supported",
        )
        self.assertIn(
            "computed_projection_evidence",
            dataframe_methods.row("assign").required_evidence,
        )
        self.assertEqual(
            dataframe_methods.row("sql").support_status,
            "fixture_smoke_supported",
        )
        self.assertTrue(dataframe_methods.row("sql").runtime_execution)
        self.assertTrue(dataframe_methods.row("sql").data_read)
        self.assertTrue(dataframe_methods.row("sql").write_io)
        self.assertIsNone(dataframe_methods.row("sql").blocker_id)
        self.assertIn(
            "sql_frontend_runtime_ladder",
            dataframe_methods.row("sql").required_evidence,
        )
        self.assertEqual(
            dataframe_methods.row("dataframe_generated_with_column").support_status,
            "fixture_smoke_supported",
        )
        self.assertTrue(dataframe_methods.row("dataframe_generated_with_column").runtime_execution)
        self.assertTrue(dataframe_methods.row("dataframe_generated_with_column").write_io)
        self.assertIsNone(dataframe_methods.row("dataframe_generated_with_column").blocker_id)
        self.assertIn(
            "generated_row_literal_projection",
            dataframe_methods.row("dataframe_generated_with_column").required_evidence,
        )
        self.assertEqual(
            dataframe_methods.row("join").support_status,
            "fixture_smoke_supported",
        )
        self.assertTrue(dataframe_methods.row("join").runtime_execution)
        self.assertIn("join_operator", dataframe_methods.row("join").required_evidence)
        self.assertEqual(
            dataframe_methods.row("groupby").support_status,
            "lazy_group_handle_supported",
        )
        self.assertEqual(
            dataframe_methods.row("agg").support_status,
            "fixture_smoke_supported",
        )
        self.assertEqual(
            dataframe_methods.row("aggregate").support_status,
            "fixture_smoke_supported",
        )
        self.assertEqual(
            dataframe_methods.row("sort").support_status,
            "fixture_smoke_supported",
        )
        self.assertIn("sort_operator", dataframe_methods.row("sort").required_evidence)
        self.assertEqual(
            dataframe_methods.row("order_by").support_status,
            "fixture_smoke_supported",
        )
        self.assertEqual(
            dataframe_methods.row("sort_by").support_status,
            "fixture_smoke_supported",
        )
        self.assertEqual(
            dataframe_methods.row("sort_values").support_status,
            "fixture_smoke_supported",
        )
        self.assertIn(
            "sort_operator",
            dataframe_methods.row("sort_values").required_evidence,
        )
        self.assertEqual(
            dataframe_methods.row("distinct").support_status,
            "fixture_smoke_supported",
        )
        self.assertTrue(dataframe_methods.row("distinct").runtime_execution)
        self.assertIn(
            "distinct_projection_operator",
            dataframe_methods.row("distinct").required_evidence,
        )
        self.assertEqual(
            dataframe_methods.row("drop_duplicates").support_status,
            "fixture_smoke_supported",
        )
        self.assertEqual(
            dataframe_methods.row("unique").support_status,
            "fixture_smoke_supported",
        )
        self.assertEqual(
            dataframe_methods.row("rename").support_status,
            "fixture_smoke_supported",
        )
        self.assertTrue(dataframe_methods.row("rename").runtime_execution)
        self.assertTrue(dataframe_methods.row("rename").data_read)
        self.assertIsNone(dataframe_methods.row("rename").diagnostic_operation)
        self.assertIsNone(dataframe_methods.row("rename").blocker_id)
        self.assertIn(
            "declared_schema_projection_rewrite",
            dataframe_methods.row("rename").required_evidence,
        )
        self.assertEqual(
            dataframe_methods.row("drop").support_status,
            "fixture_smoke_supported",
        )
        self.assertTrue(dataframe_methods.row("drop").runtime_execution)
        self.assertTrue(dataframe_methods.row("drop").data_read)
        self.assertIsNone(dataframe_methods.row("drop").diagnostic_operation)
        self.assertIsNone(dataframe_methods.row("drop").blocker_id)
        self.assertIn("projection_rewrite_semantics", dataframe_methods.row("drop").required_evidence)
        self.assertEqual(
            dataframe_methods.row("rename_columns").support_status,
            "fixture_smoke_supported",
        )
        self.assertEqual(
            dataframe_methods.row("drop_columns").support_status,
            "fixture_smoke_supported",
        )
        self.assertEqual(
            dataframe_methods.row("astype").support_status,
            "fixture_smoke_supported",
        )
        self.assertIn(
            "cast_projection_contract",
            dataframe_methods.row("astype").required_evidence,
        )
        self.assertEqual(
            dataframe_methods.row("dropna").support_status,
            "fixture_smoke_supported",
        )
        self.assertIn(
            "null_filter_semantics",
            dataframe_methods.row("dropna").required_evidence,
        )
        self.assertEqual(
            dataframe_methods.row("value_counts").support_status,
            "fixture_smoke_supported",
        )
        self.assertTrue(dataframe_methods.row("value_counts").runtime_execution)
        self.assertTrue(dataframe_methods.row("value_counts").data_read)
        self.assertIsNone(dataframe_methods.row("value_counts").diagnostic_operation)
        self.assertIsNone(dataframe_methods.row("value_counts").blocker_id)
        self.assertIn(
            "grouped_count_semantics",
            dataframe_methods.row("value_counts").required_evidence,
        )
        self.assertIn(
            "ordering_contract",
            dataframe_methods.row("value_counts").required_evidence,
        )
        self.assertEqual(
            dataframe_methods.row("nunique").support_status,
            "fixture_smoke_supported",
        )
        self.assertTrue(dataframe_methods.row("nunique").runtime_execution)
        self.assertTrue(dataframe_methods.row("nunique").data_read)
        self.assertIsNone(dataframe_methods.row("nunique").diagnostic_operation)
        self.assertIsNone(dataframe_methods.row("nunique").blocker_id)
        self.assertEqual(
            dataframe_methods.row("nlargest").support_status,
            "fixture_smoke_supported",
        )
        self.assertIn(
            "top_n_contract",
            dataframe_methods.row("nlargest").required_evidence,
        )
        self.assertEqual(
            dataframe_methods.row("nsmallest").support_status,
            "fixture_smoke_supported",
        )
        self.assertIn(
            "top_n_contract",
            dataframe_methods.row("nsmallest").required_evidence,
        )
        self.assertIn(
            "distinct_count_semantics",
            dataframe_methods.row("nunique").required_evidence,
        )
        self.assertIn(
            "dropna_policy",
            dataframe_methods.row("nunique").required_evidence,
        )
        self.assertEqual(
            dataframe_methods.row("concat").support_status,
            "fixture_smoke_supported",
        )
        self.assertTrue(dataframe_methods.row("concat").runtime_execution)
        self.assertTrue(dataframe_methods.row("concat").data_read)
        self.assertIsNone(dataframe_methods.row("concat").diagnostic_operation)
        self.assertIsNone(dataframe_methods.row("concat").blocker_id)
        self.assertIn(
            "schema_alignment_contract",
            dataframe_methods.row("concat").required_evidence,
        )
        self.assertIn(
            "set_operation_semantics",
            dataframe_methods.row("concat").required_evidence,
        )
        self.assertEqual(
            dataframe_methods.row("merge").support_status,
            "fixture_smoke_supported",
        )
        self.assertTrue(dataframe_methods.row("merge").runtime_execution)
        self.assertTrue(dataframe_methods.row("merge").data_read)
        self.assertIsNone(dataframe_methods.row("merge").diagnostic_operation)
        self.assertIsNone(dataframe_methods.row("merge").blocker_id)
        self.assertIn(
            "join_alias_semantics",
            dataframe_methods.row("merge").required_evidence,
        )
        self.assertIn(
            "join_operator_capability",
            dataframe_methods.row("merge").required_evidence,
        )
        self.assertEqual(
            dataframe_methods.row("fillna").support_status,
            "fixture_smoke_supported",
        )
        self.assertTrue(dataframe_methods.row("fillna").runtime_execution)
        self.assertTrue(dataframe_methods.row("fillna").data_read)
        self.assertIsNone(dataframe_methods.row("fillna").diagnostic_operation)
        self.assertIsNone(dataframe_methods.row("fillna").blocker_id)
        self.assertIn(
            "null_fill_semantics",
            dataframe_methods.row("fillna").required_evidence,
        )
        self.assertIn(
            "projection_rewrite_semantics",
            dataframe_methods.row("fillna").required_evidence,
        )
        self.assertEqual(
            dataframe_methods.row("fill_null").support_status,
            "fixture_smoke_supported",
        )
        self.assertEqual(
            dataframe_methods.row("isna").support_status,
            "fixture_smoke_supported",
        )
        self.assertTrue(dataframe_methods.row("isna").runtime_execution)
        self.assertTrue(dataframe_methods.row("isna").data_read)
        self.assertIsNone(dataframe_methods.row("isna").diagnostic_operation)
        self.assertIsNone(dataframe_methods.row("isna").blocker_id)
        self.assertIn(
            "null_mask_semantics",
            dataframe_methods.row("isna").required_evidence,
        )
        self.assertEqual(
            dataframe_methods.row("isnull").support_status,
            "fixture_smoke_supported",
        )
        self.assertEqual(
            dataframe_methods.row("notna").support_status,
            "fixture_smoke_supported",
        )
        self.assertTrue(dataframe_methods.row("notna").runtime_execution)
        self.assertTrue(dataframe_methods.row("notna").data_read)
        self.assertIsNone(dataframe_methods.row("notna").diagnostic_operation)
        self.assertIsNone(dataframe_methods.row("notna").blocker_id)
        self.assertIn(
            "not_null_mask_semantics",
            dataframe_methods.row("notna").required_evidence,
        )
        self.assertEqual(
            dataframe_methods.row("notnull").support_status,
            "fixture_smoke_supported",
        )
        self.assertEqual(
            dataframe_methods.row("sample").blocker_id,
            "cg21.workflow.sample.sampling_semantics_unsupported",
        )
        self.assertFalse(dataframe_methods.row("sample").write_io)
        self.assertIn(
            "deterministic_seed_policy",
            dataframe_methods.row("sample").required_evidence,
        )
        self.assertEqual(
            dataframe_methods.row("explode").blocker_id,
            "cg21.workflow.explode.nested_expansion_unsupported",
        )
        self.assertIn(
            "list_expansion_operator",
            dataframe_methods.row("explode").required_evidence,
        )
        self.assertEqual(
            dataframe_methods.row("pivot").blocker_id,
            "cg21.workflow.pivot.reshape_semantics_unsupported",
        )
        self.assertFalse(dataframe_methods.row("pivot").write_io)
        self.assertIn("reshape_semantics", dataframe_methods.row("pivot").required_evidence)
        self.assertEqual(
            dataframe_methods.row("pivot_table").diagnostic_operation,
            "pivot_table",
        )
        self.assertEqual(
            dataframe_methods.row("pivot_table").blocker_id,
            "cg21.workflow.pivot_table.aggregate_reshape_unsupported",
        )
        self.assertIn(
            "aggregate_reshape_semantics",
            dataframe_methods.row("pivot_table").required_evidence,
        )
        self.assertEqual(
            dataframe_methods.row("melt").blocker_id,
            "cg21.workflow.melt.reshape_semantics_unsupported",
        )
        self.assertIn("unpivot_semantics", dataframe_methods.row("melt").required_evidence)
        self.assertEqual(
            dataframe_methods.row("rolling").blocker_id,
            "cg21.workflow.rolling.window_semantics_unsupported",
        )
        self.assertIn(
            "window_frame_semantics",
            dataframe_methods.row("rolling").required_evidence,
        )
        self.assertEqual(
            dataframe_methods.row("duplicated").blocker_id,
            "cg21.workflow.duplicated.row_mask_unsupported",
        )
        self.assertIn(
            "duplicate_mask_semantics",
            dataframe_methods.row("duplicated").required_evidence,
        )
        self.assertEqual(
            dataframe_methods.row("mask").blocker_id,
            "cg21.workflow.mask.conditional_replace_unsupported",
        )
        self.assertEqual(
            dataframe_methods.row("replace").blocker_id,
            "cg21.workflow.replace.value_rewrite_unsupported",
        )
        self.assertEqual(
            dataframe_methods.row("set_index").blocker_id,
            "cg21.workflow.set_index.index_state_unsupported",
        )
        self.assertEqual(
            dataframe_methods.row("reset_index").blocker_id,
            "cg21.workflow.reset_index.index_state_unsupported",
        )
        self.assertEqual(
            dataframe_methods.row("sort_index").blocker_id,
            "cg21.workflow.sort_index.index_order_unsupported",
        )
        self.assertEqual(
            dataframe_methods.row("window").support_status,
            "fixture_smoke_supported",
        )
        self.assertTrue(dataframe_methods.row("window").runtime_execution)
        self.assertIn("window_operator", dataframe_methods.row("window").required_evidence)
        self.assertEqual(
            dataframe_methods.row("to_python_objects").support_status,
            "fixture_smoke_supported",
        )
        self.assertTrue(dataframe_methods.row("to_python_objects").runtime_execution)
        self.assertTrue(dataframe_methods.row("to_python_objects").materialization_required)
        self.assertEqual(
            dataframe_methods.row("schema").support_status,
            "fixture_smoke_supported",
        )
        self.assertEqual(
            dataframe_methods.row("describe_schema").support_status,
            "fixture_smoke_supported",
        )
        self.assertEqual(
            dataframe_methods.row("validate_schema").support_status,
            "fixture_smoke_supported",
        )
        self.assertEqual(
            dataframe_methods.row("schema_contract").support_status,
            "fixture_smoke_supported",
        )
        self.assertTrue(dataframe_methods.row("schema").runtime_execution)
        self.assertTrue(dataframe_methods.row("validate_schema").materialization_required)
        self.assertTrue(dataframe_methods.row("schema_contract").materialization_required)
        self.assertEqual(
            dataframe_methods.row("data_quality_summary").support_status,
            "fixture_smoke_supported",
        )
        self.assertTrue(dataframe_methods.row("data_quality_check").runtime_execution)
        self.assertEqual(
            dataframe_methods.row("profile").support_status,
            "fixture_smoke_supported",
        )
        self.assertTrue(dataframe_methods.row("profile").runtime_execution)
        self.assertTrue(dataframe_methods.row("profile").data_read)
        self.assertTrue(dataframe_methods.row("profile").materialization_required)
        self.assertIn("profile_runtime", dataframe_methods.row("profile").required_evidence)
        self.assertEqual(
            dataframe_methods.row("quarantine").support_status,
            "fixture_smoke_supported",
        )
        self.assertTrue(dataframe_methods.row("quarantine").runtime_execution)
        self.assertTrue(dataframe_methods.row("quarantine").data_read)
        self.assertTrue(dataframe_methods.row("quarantine").write_io)
        self.assertEqual(
            dataframe_methods.row("object_store_generated_output").support_status,
            "fixture_smoke_supported",
        )
        self.assertTrue(
            dataframe_methods.row("object_store_generated_output").runtime_execution
        )
        self.assertTrue(dataframe_methods.row("object_store_generated_output").write_io)
        self.assertTrue(
            dataframe_methods.row(
                "object_store_generated_output"
            ).materialization_required
        )
        self.assertIsNone(dataframe_methods.row("object_store_generated_output").blocker_id)
        self.assertIn(
            "object_store_write_smoke",
            dataframe_methods.row("object_store_generated_output").required_evidence,
        )
        self.assertEqual(
            dataframe_methods.row("foundry_generated_output").support_status,
            "fixture_smoke_supported",
        )
        self.assertTrue(dataframe_methods.row("foundry_generated_output").runtime_execution)
        self.assertTrue(dataframe_methods.row("foundry_generated_output").write_io)
        self.assertTrue(dataframe_methods.row("foundry_generated_output").materialization_required)
        self.assertIsNone(dataframe_methods.row("foundry_generated_output").blocker_id)
        self.assertIn(
            "foundry_style_result_dataset",
            dataframe_methods.row("foundry_generated_output").required_evidence,
        )
        self.assertTrue(dataframe_methods.row("quarantine").materialization_required)
        self.assertIn(
            "local_quarantine_sink_write_evidence",
            dataframe_methods.row("quarantine").required_evidence,
        )
        self.assertEqual(
            dataframe_methods.row("write").required_evidence,
            (
                "sql_local_source_smoke",
                "local_jsonl_csv_or_feature_gated_structured_output",
                "output_native_io_certificate",
                "result_replay_verified",
                "output_fidelity_report_status",
            ),
        )
        self.assertEqual(
            dataframe_methods.row("write_jsonl").required_evidence,
            (
                "sql_local_source_smoke",
                "local_jsonl_output",
                "output_native_io_certificate",
                "result_replay_verified",
                "output_fidelity_report_status",
            ),
        )
        self.assertEqual(
            dataframe_methods.row("write_csv").required_evidence,
            (
                "sql_local_source_smoke",
                "local_csv_output",
                "output_native_io_certificate",
                "result_replay_verified",
                "output_fidelity_report_status",
            ),
        )
        self.assertTrue(dataframe_methods.row("write_csv").runtime_execution)
        self.assertTrue(dataframe_methods.row("write_jsonl").write_io)
        self.assertEqual(
            dataframe_methods.row("fanout").required_evidence,
            (
                "sql_local_source_smoke",
                "local_output_fanout",
                "output_native_io_certificate",
                "result_replay_verified",
                "output_fidelity_report_status",
                "no_fallback_evidence",
            ),
        )
        self.assertTrue(dataframe_methods.row("fanout").runtime_execution)
        self.assertTrue(dataframe_methods.row("fanout").write_io)
        self.assertEqual(
            dataframe_methods.row("write_vortex").required_evidence,
            (
                "sql_local_source_smoke",
                "feature_gated_local_vortex_output",
                "output_native_io_certificate",
                "result_replay_verified",
                "output_fidelity_report_status",
                "upstream_vortex_write_called",
            ),
        )
        self.assertTrue(dataframe_methods.row("write_vortex").runtime_execution)
        self.assertTrue(dataframe_methods.row("write_vortex").write_io)
        self.assertIsNone(dataframe_methods.row("join").blocker_id)
        self.assertTrue(dataframe_methods.row("to_pandas").materialization_required)
        self.assertEqual(
            dataframe_methods.row("to_pandas").support_status,
            "optional_dependency_runtime_supported",
        )
        self.assertTrue(dataframe_methods.row("to_pandas").runtime_execution)
        self.assertTrue(dataframe_methods.row("to_arrow_ipc").data_read)
        self.assertEqual(
            dataframe_methods.row("from_pandas").support_status,
            "materialized_input_boundary_supported",
        )
        self.assertIsNone(dataframe_methods.row("from_pandas").blocker_id)
        self.assertEqual(
            dataframe_methods.row("display").support_status,
            "fixture_smoke_supported",
        )
        self.assertTrue(dataframe_methods.row("display").runtime_execution)
        self.assertEqual(
            dataframe_methods.row("display").required_evidence,
            (
                "sql_local_source_smoke",
                "bounded_inline_jsonl_result",
                "notebook_display_contract",
                "no_fallback_evidence",
            ),
        )
        self.assertEqual(dataframe_methods.claim_gate_statuses, ("not_claim_grade",))
        self.assertTrue(dataframe_methods.all_no_fallback_no_external_engine)
        self.assertTrue(dataframe_methods.any_runtime_execution)
        self.assertTrue(dataframe_methods.any_data_read)
        self.assertTrue(dataframe_methods.any_write_io)
        self.assertIsNone(ctx.dataframe_method_matrix().row("agg").diagnostic_operation)
        self.assertTrue(ctx.etl_workflow_matrix().row("production_etl_certification").blocked)
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
        observability = ctx.observability()
        lineage = observability.openlineage_facet_mapping
        self.assertIsInstance(lineage, OpenLineageFacetMappingReport)
        self.assertTrue(lineage.present)
        self.assertEqual(lineage.schema_version, "shardloom.openlineage_facet_mapping.v1")
        self.assertEqual(lineage.gar_id, "GAR-NOVEL-1B")
        self.assertEqual(
            lineage.row_order,
            (
                "execution_mode",
                "no_fallback",
                "native_io_certificate",
                "materialization_boundary",
                "claim_gate",
                "generated_source",
                "vortex_artifact",
            ),
        )
        self.assertFalse(lineage.export_enabled)
        self.assertFalse(lineage.event_emitted)
        self.assertFalse(lineage.network_call_performed)
        self.assertFalse(lineage.schema_published)
        self.assertTrue(lineage.all_rows_report_only)
        self.assertTrue(lineage.all_no_fallback_no_external_engine)
        self.assertEqual(lineage.claim_gate_status, "not_claim_grade")
        self.assertEqual(
            lineage.row("generated_source").facet_name,
            "GeneratedSourceFacet",
        )
        self.assertIn(
            "generated_source_certificate_status",
            lineage.row("generated_source").shardloom_evidence_fields,
        )
        self.assertTrue(lineage.row("no_fallback").report_only_no_export)
        self.assertTrue(lineage.row("vortex_artifact").no_fallback_no_external_engine)
        telemetry = observability.opentelemetry_trace_export_contract
        self.assertIsInstance(telemetry, OpenTelemetryTraceExportContractReport)
        self.assertTrue(telemetry.present)
        self.assertEqual(
            telemetry.schema_version,
            "shardloom.opentelemetry_trace_export_contract.v1",
        )
        self.assertEqual(telemetry.gar_id, "GAR-NOVEL-1C")
        self.assertEqual(
            telemetry.row_order,
            (
                "request_admission",
                "source_read",
                "compatibility_parse",
                "vortex_import",
                "vortex_scan",
                "operator_compute",
                "result_sink",
                "evidence_render",
                "claim_gate",
            ),
        )
        self.assertFalse(telemetry.trace_export_enabled)
        self.assertFalse(telemetry.metric_export_enabled)
        self.assertFalse(telemetry.log_export_enabled)
        self.assertFalse(telemetry.network_exporter_enabled)
        self.assertFalse(telemetry.otlp_exporter_configured)
        self.assertFalse(telemetry.trace_emitted)
        self.assertFalse(telemetry.network_call_performed)
        self.assertTrue(telemetry.all_rows_report_only)
        self.assertTrue(telemetry.all_no_fallback_no_external_engine)
        self.assertTrue(telemetry.no_export_side_effects)
        self.assertEqual(telemetry.claim_gate_status, "not_claim_grade")
        self.assertEqual(
            telemetry.row("operator_compute").span_name,
            "shardloom.operator_compute",
        )
        self.assertIn(
            "operator_compute_millis",
            telemetry.row("operator_compute").timing_fields,
        )
        self.assertIn(
            "fallback_attempted",
            telemetry.row("request_admission").shardloom_attribute_allowlist,
        )
        self.assertTrue(telemetry.row("result_sink").report_only_no_export)
        self.assertTrue(telemetry.row("claim_gate").no_fallback_no_external_engine)

    def test_context_front_door_parity_matrix_exposes_broad_gaps(self) -> None:
        binary = self.fake_cli(
            textwrap.dedent(
                """
                raise AssertionError("front-door parity matrix must not invoke CLI")
                """
            )
        )
        ctx = ShardLoomContext(client=ShardLoomClient(binary=binary))

        matrix = ctx.front_door_parity_matrix()

        self.assertIsInstance(matrix, FrontDoorParityMatrix)
        self.assertEqual(matrix.schema_version, "shardloom.front_door_parity_matrix.v1")
        self.assertTrue(matrix.scoped_local_front_door_parity_supported)
        self.assertFalse(matrix.flexible_anything_claim_allowed)
        self.assertFalse(matrix.performance_equivalence_claim_allowed)
        self.assertTrue(matrix.all_no_fallback_no_external_engine)
        self.assertTrue(matrix.all_broad_gaps_have_precise_runtime_status)
        self.assertEqual(
            matrix.runtime_gap_status_counts["front_door_connection_pending"],
            2,
        )
        self.assertIn("local_file_filter_project_limit", matrix.row_order)
        self.assertIn("arbitrary_sql_python_dataframe_breadth", matrix.row_order)
        local = matrix.row("local_file_filter_project_limit")
        self.assertTrue(local.equivalent_admitted_scope)
        self.assertEqual(local.runtime_gap_status, "admitted_scope")
        self.assertEqual(local.shared_runtime_path, "sql-local-source-smoke")
        self.assertIsNone(local.blocker_id)
        self.assertIn("no_benchmark_claim", local.performance_equivalence_status)
        generated = matrix.row("generated_source_output")
        self.assertTrue(generated.equivalent_admitted_scope)
        self.assertTrue(generated.write_io)
        schema_quality = matrix.row("schema_quality_preview")
        self.assertTrue(schema_quality.equivalent_admitted_scope)
        self.assertIn("ctx.sql", schema_quality.sql_surface)
        self.assertIsNone(schema_quality.blocker_id)
        materialization = matrix.row("decoded_materialization_interop")
        self.assertTrue(materialization.equivalent_admitted_scope)
        self.assertTrue(materialization.materialization_required)
        self.assertIsNone(materialization.blocker_id)
        self.assertIn("to_pandas", materialization.sql_surface)
        vortex = matrix.row("local_vortex_primitive_runtime")
        self.assertTrue(vortex.equivalent_admitted_scope)
        self.assertEqual(
            vortex.shared_runtime_path,
            "vortex-run/vortex-count-where/vortex-filter/vortex-project/vortex-filter-project",
        )
        self.assertIn("local.vortex", vortex.sql_surface)
        self.assertFalse(vortex.materialization_required)
        self.assertIsNone(vortex.blocker_id)
        self.assertIn("Vortex-normalized", vortex.claim_boundary)
        typed_nested = matrix.row("typed_nested_compatibility_sink")
        self.assertTrue(typed_nested.equivalent_admitted_scope)
        self.assertTrue(typed_nested.write_io)
        self.assertTrue(typed_nested.materialization_required)
        self.assertIsNone(typed_nested.blocker_id)
        self.assertIn("Parquet, Arrow IPC, Avro, and local Vortex", typed_nested.claim_boundary)
        broad = matrix.row("arbitrary_sql_python_dataframe_breadth")
        self.assertTrue(broad.broad_gap)
        self.assertEqual(broad.parity_status, "front_door_gap")
        self.assertEqual(broad.runtime_gap_status, "front_door_connection_pending")
        self.assertEqual(
            broad.blocker_id,
            "cg20.cg21.broad_language_surface_missing",
        )
        performance = matrix.row("performance_equivalence")
        self.assertEqual(performance.support_status, "benchmark_publication_pending")
        self.assertEqual(performance.runtime_gap_status, "benchmark_publication_pending")
        self.assertEqual(performance.performance_equivalence_status, "not_claim_grade")
        self.assertEqual(len(matrix.admitted_rows), 7)
        self.assertGreaterEqual(len(matrix.broad_gap_rows), 4)

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
                        {"key": "live_hybrid_fabric_gate_schema_version", "value": "shardloom.live_hybrid_fabric_freshness_gate.v1"},
                        {"key": "live_hybrid_fabric_gate_report_id", "value": "gar-0034-a.live_hybrid_fabric_freshness_gate"},
                        {"key": "live_hybrid_fabric_gate_row_count", "value": "10"},
                        {"key": "live_hybrid_fabric_gate_row_order", "value": "live_broker_adapter,live_durable_checkpoint_store,live_unbounded_scheduler,live_freshness_certificate,live_exactly_once_claim,live_hybrid_state_transition_fixture,hybrid_micro_segment_flush,hybrid_object_store_commit,hybrid_catalog_snapshot,baseline_oracle_boundary"},
                        {"key": "live_hybrid_fabric_gate_blocked_row_count", "value": "7"},
                        {"key": "live_hybrid_fabric_gate_report_only_row_count", "value": "1"},
                        {"key": "live_hybrid_fabric_gate_fixture_smoke_row_count", "value": "2"},
                        {"key": "live_hybrid_fabric_gate_claim_gate_status", "value": "not_claim_grade"},
                        {"key": "live_hybrid_fabric_gate_freshness_claim_allowed", "value": "false"},
                        {"key": "live_hybrid_fabric_gate_exactly_once_claim_allowed", "value": "false"},
                        {"key": "live_hybrid_fabric_gate_production_live_claim_allowed", "value": "false"},
                        {"key": "live_hybrid_fabric_gate_production_hybrid_claim_allowed", "value": "false"},
                        {"key": "live_hybrid_fabric_gate_object_store_runtime_supported", "value": "false"},
                        {"key": "live_hybrid_fabric_gate_broker_runtime_supported", "value": "false"},
                        {"key": "live_hybrid_fabric_gate_state_store_runtime_supported", "value": "false"},
                        {"key": "live_hybrid_fabric_gate_baseline_oracle_only", "value": "true"},
                        {"key": "live_hybrid_fabric_gate_fallback_attempted", "value": "false"},
                        {"key": "live_hybrid_fabric_gate_external_engine_invoked", "value": "false"},
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
        self.assertEqual(
            result.live_hybrid_fabric_gate_schema_version,
            "shardloom.live_hybrid_fabric_freshness_gate.v1",
        )
        self.assertEqual(
            result.live_hybrid_fabric_gate_report_id,
            "gar-0034-a.live_hybrid_fabric_freshness_gate",
        )
        self.assertIn("live_freshness_certificate", result.live_hybrid_fabric_gate_rows)
        self.assertIn(
            "live_hybrid_state_transition_fixture",
            result.live_hybrid_fabric_gate_rows,
        )
        self.assertIn("baseline_oracle_boundary", result.live_hybrid_fabric_gate_rows)
        self.assertEqual(result.live_hybrid_fabric_gate_blocked_row_count, 7)
        self.assertEqual(result.live_hybrid_fabric_gate_report_only_row_count, 1)
        self.assertEqual(result.live_hybrid_fabric_gate_fixture_smoke_row_count, 2)
        self.assertEqual(result.live_hybrid_fabric_gate_claim_gate_status, "not_claim_grade")
        self.assertFalse(result.live_hybrid_freshness_claim_allowed)
        self.assertFalse(result.live_hybrid_exactly_once_claim_allowed)
        self.assertFalse(result.live_hybrid_production_live_claim_allowed)
        self.assertFalse(result.live_hybrid_production_hybrid_claim_allowed)
        self.assertFalse(result.live_hybrid_object_store_runtime_supported)
        self.assertFalse(result.live_hybrid_broker_runtime_supported)
        self.assertFalse(result.live_hybrid_state_store_runtime_supported)
        self.assertTrue(result.live_hybrid_baseline_oracle_only)
        self.assertFalse(result.live_hybrid_fabric_gate_fallback_attempted)
        self.assertFalse(result.live_hybrid_fabric_gate_external_engine_invoked)
        self.assertTrue(result.live_hybrid_fabric_gate_no_fallback_no_external_engine)
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

    def test_vortex_run_uses_public_run_facade_payload(self) -> None:
        binary = self.fake_cli(
            textwrap.dedent(
                """
                import json, sys
                assert sys.argv[1:] == [
                    "run",
                    "cli",
                    "--input",
                    "file.vortex",
                    "--input-format",
                    "vortex",
                    "--request",
                    "collect",
                    "--execution-policy",
                    "native_vortex",
                    "--materialization-policy",
                    "zero_decode",
                    "--evidence-level",
                    "runtime_smoke",
                    "--bounded",
                    "true",
                    "--vortex-primitive",
                    "count",
                    "--memory-gb",
                    "8",
                    "--max-parallelism",
                    "2",
                    "--format",
                    "json",
                ], sys.argv
                print(json.dumps({
                    "schema_version": "shardloom.output.v2",
                    "command": "run",
                    "status": "success",
                    "summary": "ok",
                    "human_text": "ok",
                    "fallback": {"attempted": False, "allowed": False, "engine": None, "reason": "disabled"},
                    "diagnostics": [],
                    "fields": [
                        {"key": "public_workflow_route_attached", "value": "true"},
                        {"key": "public_workflow_route_id", "value": "native_vortex_count_all"},
                        {"key": "public_workflow_resolved_internal_command", "value": "vortex-run"},
                        {"key": "public_workflow_vortex_primitive", "value": "count"},
                        {"key": "public_workflow_memory_gb", "value": "8"},
                        {"key": "public_workflow_max_parallelism", "value": "2"},
                        {"key": "fallback_execution_allowed", "value": "false"},
                    ],
                }))
                """
            )
        )

        result = ShardLoomClient(binary=binary).vortex_run(
            "file.vortex", "count", memory_gb=8, max_parallelism=2
        )

        self.assertEqual(result.command, "run")
        self.assertEqual(result.field("public_workflow_route_id"), "native_vortex_count_all")
        self.assertEqual(result.field("public_workflow_resolved_internal_command"), "vortex-run")
        self.assertEqual(result.field("public_workflow_vortex_primitive"), "count")
        self.assertEqual(result.field_map["fallback_execution_allowed"], "false")
        self.assertEqual(result.field("fallback_execution_allowed"), "false")
        self.assertTrue(result.field_bool("fallback_execution_allowed") is False)

    def test_vortex_run_preserves_non_count_runtime_command(self) -> None:
        binary = self.fake_cli(
            textwrap.dedent(
                """
                import json, sys
                assert sys.argv[1:] == [
                    "vortex-run",
                    "file.vortex",
                    "project:metric",
                    "8",
                    "2",
                    "--format",
                    "json",
                ], sys.argv
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
            "file.vortex", "project:metric", memory_gb=8, max_parallelism=2
        )

        self.assertEqual(result.command, "vortex-run")
        self.assertEqual(result.field("fallback_execution_allowed"), "false")

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
                assert args[:16] == [
                    "run",
                    "cli",
                    "--input",
                    "file.vortex",
                    "--input-format",
                    "vortex",
                    "--request",
                    "collect",
                    "--execution-policy",
                    "native_vortex",
                    "--materialization-policy",
                    "zero_decode",
                    "--evidence-level",
                    "runtime_smoke",
                    "--bounded",
                    "true",
                ], args
                primitive = args[args.index("--vortex-primitive") + 1]
                expected_command = {
                    "count_where": "vortex-count-where",
                    "filter": "vortex-filter",
                    "project": "vortex-project",
                    "filter_project": "vortex-filter-project",
                }.get(primitive)
                if expected_command is None:
                    raise AssertionError(args)
                assert args[args.index("--memory-gb") + 1] == "4", args
                assert args[args.index("--max-parallelism") + 1] == "2", args
                if primitive in {"count_where", "filter", "filter_project"}:
                    assert args[args.index("--vortex-predicate") + 1] == "gte:value:3", args
                if primitive in {"project", "filter_project"}:
                    assert args[args.index("--vortex-columns") + 1] == "metric,value", args
                if "--vortex-source-order-limit" in args:
                    assert primitive == "filter_project", args
                    assert args[args.index("--vortex-source-order-limit") + 1] == "5", args
                print(json.dumps({
                    "schema_version": "shardloom.output.v2",
                    "command": "run",
                    "status": "success",
                    "summary": "ok",
                    "human_text": "ok",
                    "fallback": {"attempted": False, "allowed": False, "engine": None, "reason": "disabled"},
                    "diagnostics": [],
                    "fields": [
                        {"key": "public_workflow_route_attached", "value": "true"},
                        {"key": "public_workflow_resolved_internal_command", "value": expected_command},
                        {"key": "public_workflow_vortex_primitive", "value": primitive},
                        {"key": "local_execution", "value": "true"},
                    ],
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
        filter_project_limited = client.vortex_filter_project(
            "file.vortex",
            "gte:value:3",
            ("metric", "value"),
            source_order_limit=5,
            execute_local_primitive=True,
            memory_gb=4,
            max_parallelism=2,
        )

        self.assertEqual(count_where.command, "run")
        self.assertEqual(
            count_where.field("public_workflow_resolved_internal_command"),
            "vortex-count-where",
        )
        self.assertEqual(filtered.command, "run")
        self.assertEqual(
            filtered.field("public_workflow_resolved_internal_command"),
            "vortex-filter",
        )
        self.assertEqual(projected.command, "run")
        self.assertEqual(
            projected.field("public_workflow_resolved_internal_command"),
            "vortex-project",
        )
        self.assertEqual(filter_project.command, "run")
        self.assertEqual(
            filter_project.field("public_workflow_resolved_internal_command"),
            "vortex-filter-project",
        )
        self.assertEqual(filter_project_limited.command, "run")
        self.assertEqual(
            filter_project_limited.field("public_workflow_resolved_internal_command"),
            "vortex-filter-project",
        )

    def test_local_vortex_primitive_smoke_dispatches_certified_fixture_workflow(self) -> None:
        binary = self.fake_cli(
            textwrap.dedent(
                """
                import json, sys
                args = sys.argv[1:]
                assert args[:16] == [
                    "run",
                    "cli",
                    "--input",
                    "file.vortex",
                    "--input-format",
                    "vortex",
                    "--request",
                    "collect",
                    "--execution-policy",
                    "native_vortex",
                    "--materialization-policy",
                    "zero_decode",
                    "--evidence-level",
                    "runtime_smoke",
                    "--bounded",
                    "true",
                ], args
                primitive = args[args.index("--vortex-primitive") + 1]
                expected = {
                    "count": (
                        "vortex-run",
                        [{"key": "local_primitive_rows_scanned", "value": "5"}],
                    ),
                    "count_where": (
                        "vortex-count-where",
                        [{"key": "filtered_count_local_execution_rows_selected", "value": "3"}],
                    ),
                    "filter": (
                        "vortex-filter",
                        [{"key": "filter_local_execution_rows_selected", "value": "3"}],
                    ),
                    "project": (
                        "vortex-project",
                        [{"key": "project_local_execution_rows_projected", "value": "5"}],
                    ),
                    "filter_project": (
                        "vortex-filter-project",
                        [{"key": "filter_project_local_execution_rows_projected", "value": "3"}],
                    ),
                }
                matched = expected.get(primitive)
                if matched is None:
                    raise AssertionError(args)
                command, command_fields = matched
                assert args[args.index("--memory-gb") + 1] == "3", args
                assert args[args.index("--max-parallelism") + 1] == "4", args
                if primitive in {"count_where", "filter", "filter_project"}:
                    assert args[args.index("--vortex-predicate") + 1] == "gte:value:3", args
                if primitive in {"project", "filter_project"}:
                    assert args[args.index("--vortex-columns") + 1] == "metric,value", args
                fields = [
                    {"key": "public_workflow_route_attached", "value": "true"},
                    {"key": "public_workflow_resolved_internal_command", "value": command},
                    {"key": "public_workflow_vortex_primitive", "value": primitive},
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
                    "command": "run",
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
            client.vortex_filter_project(
                "file.vortex",
                "gte:value:3",
                "metric",
                source_order_limit=0,
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

    def test_command_error_redacts_credential_bearing_uri_args(self) -> None:
        binary = self.fake_cli(
            textwrap.dedent(
                """
                import json, sys
                print(json.dumps({
                    "schema_version": "shardloom.output.v2",
                    "command": "input-plan",
                    "status": "unsupported",
                    "summary": "unsupported",
                    "human_text": "unsupported",
                    "fallback": {"attempted": False, "allowed": False, "engine": None, "reason": "disabled"},
                    "diagnostics": [{
                        "code": "UnsupportedInput",
                        "severity": "error",
                        "category": "unsupported_feature",
                        "message": "unsupported",
                        "feature": "input",
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
            ShardLoomClient(binary=binary).run(
                [
                    "input-plan",
                    "s3://user@bucket/path.vortex?token=secret",
                    "--sql",
                    "SELECT * FROM 'gs://writer@bucket/data.csv?sig=secret'",
                ]
            )

        command = " ".join(raised.exception.command)
        self.assertIn("s3://<redacted>@bucket/path.vortex", command)
        self.assertIn("gs://<redacted>@bucket/data.csv", command)
        self.assertNotIn("token=secret", command)
        self.assertNotIn("sig=secret", command)
        self.assertNotIn("user@bucket", command)
        self.assertNotIn("writer@bucket", command)

    def test_protocol_error_redacts_credential_bearing_uri_args(self) -> None:
        binary = self.fake_cli("import sys; sys.exit(1)")

        with self.assertRaises(ShardLoomProtocolError) as raised:
            ShardLoomClient(binary=binary).run(
                ["input-plan", "s3://user@bucket/path.vortex?token=secret"]
            )

        message = str(raised.exception)
        self.assertIn("s3://<redacted>@bucket/path.vortex", message)
        self.assertNotIn("token=secret", message)
        self.assertNotIn("user@bucket", message)

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

    def test_object_store_read_smoke_wrapper_calls_local_emulator_profile(self) -> None:
        binary = self.fake_cli(
            textwrap.dedent(
                """
                import json, sys
                assert sys.argv[1:] == [
                    "object-store-read-smoke",
                    "target/object.bin",
                    "--profile",
                    "local-emulator",
                    "--range",
                    "4:8",
                    "--format",
                    "json",
                ], sys.argv
                print(json.dumps({
                    "schema_version": "shardloom.output.v2",
                    "command": "object-store-read-smoke",
                    "status": "success",
                    "summary": "object-store local-emulator read smoke",
                    "human_text": "local-emulator object-store read smoke",
                    "fallback": {"attempted": False, "allowed": False, "engine": None, "reason": "disabled"},
                    "diagnostics": [],
                    "fields": [
                        {"key": "object_store_read_status", "value": "succeeded"},
                        {"key": "provider_profile", "value": "local-emulator"},
                        {"key": "byte_range_read_status", "value": "performed_local_emulator"},
                        {"key": "source_state_id", "value": "object-store-local-emulator-fnv64-demo"},
                        {"key": "native_io_certificate_status", "value": "fixture_smoke_only"},
                        {"key": "claim_gate_status", "value": "fixture_smoke_only"},
                        {"key": "object_store_io", "value": "true"},
                        {"key": "fallback_attempted", "value": "false"},
                        {"key": "external_engine_invoked", "value": "false"}
                    ],
                }))
                """
            )
        )

        envelope = ShardLoomClient(binary=binary).object_store_read_smoke(
            "target/object.bin",
            byte_range=(4, 8),
        )

        self.assertEqual(envelope.status, "success")
        self.assertEqual(envelope.field("object_store_read_status"), "succeeded")
        self.assertTrue(envelope.field_bool("object_store_io"))
        self.assertFalse(envelope.field_bool("fallback_attempted"))
        self.assertFalse(envelope.field_bool("external_engine_invoked"))

    def test_local_table_metadata_read_smoke_wrapper_calls_scoped_runtime(self) -> None:
        binary = self.fake_cli(
            textwrap.dedent(
                """
                import json, sys
                assert sys.argv[1:] == [
                    "local-table-metadata-read-smoke",
                    "--format",
                    "json",
                ], sys.argv
                print(json.dumps({
                    "schema_version": "shardloom.output.v2",
                    "command": "local-table-metadata-read-smoke",
                    "status": "success",
                    "summary": "local table metadata read smoke",
                    "human_text": "local table metadata read smoke",
                    "fallback": {"attempted": False, "allowed": False, "engine": None, "reason": "disabled"},
                    "diagnostics": [],
                    "fields": [
                        {"key": "mode", "value": "local_table_metadata_read_smoke"},
                        {"key": "support_status", "value": "runtime_supported"},
                        {"key": "claim_gate_status", "value": "scoped_local_metadata_smoke_only"},
                        {"key": "table_metadata_read_performed", "value": "true"},
                        {"key": "object_store_io_performed", "value": "false"},
                        {"key": "fallback_attempted", "value": "false"},
                        {"key": "external_engine_invoked", "value": "false"}
                    ],
                }))
                """
            )
        )

        envelope = ShardLoomClient(binary=binary).local_table_metadata_read_smoke()

        self.assertEqual(envelope.command, "local-table-metadata-read-smoke")
        self.assertEqual(envelope.field("support_status"), "runtime_supported")
        self.assertEqual(
            envelope.field("claim_gate_status"),
            "scoped_local_metadata_smoke_only",
        )
        self.assertTrue(envelope.field_bool("table_metadata_read_performed"))
        self.assertFalse(envelope.field_bool("object_store_io_performed"))
        self.assertFalse(envelope.field_bool("fallback_attempted"))
        self.assertFalse(envelope.field_bool("external_engine_invoked"))

    def test_object_store_read_smoke_wrapper_calls_public_fixture_profile(self) -> None:
        binary = self.fake_cli(
            textwrap.dedent(
                """
                import json, sys
                assert sys.argv[1:] == [
                    "object-store-read-smoke",
                    "s3://shardloom-public-fixtures/orders.vortex",
                    "--profile",
                    "public-no-credential-fixture",
                    "--public-fixture-path",
                    "target/public-fixture.vortex",
                    "--fixture-listing",
                    "--range",
                    "4:8",
                    "--format",
                    "json",
                ], sys.argv
                print(json.dumps({
                    "schema_version": "shardloom.output.v2",
                    "command": "object-store-read-smoke",
                    "status": "success",
                    "summary": "object-store public fixture read smoke",
                    "human_text": "public no-credential fixture object-store read smoke",
                    "fallback": {"attempted": False, "allowed": False, "engine": None, "reason": "disabled"},
                    "diagnostics": [],
                    "fields": [
                        {"key": "object_store_read_status", "value": "succeeded"},
                        {"key": "provider_profile", "value": "public-no-credential-fixture"},
                        {"key": "object_store_provider", "value": "s3"},
                        {"key": "object_store_uri_parse_status", "value": "parsed_public_no_credential_fixture_uri"},
                        {"key": "byte_range_read_status", "value": "performed_public_no_credential_fixture"},
                        {"key": "listing_status", "value": "performed_public_fixture_single_object"},
                        {"key": "credential_resolution_performed", "value": "false"},
                        {"key": "network_probe_performed", "value": "false"},
                        {"key": "native_io_certificate_status", "value": "public_fixture_smoke_only"},
                        {"key": "claim_gate_status", "value": "public_fixture_smoke_only"},
                        {"key": "object_store_io", "value": "true"},
                        {"key": "public_no_credential_fixture_claim_allowed", "value": "true"},
                        {"key": "fallback_attempted", "value": "false"},
                        {"key": "external_engine_invoked", "value": "false"}
                    ],
                }))
                """
            )
        )

        envelope = ShardLoomClient(binary=binary).object_store_read_smoke(
            "s3://shardloom-public-fixtures/orders.vortex",
            profile="public-no-credential-fixture",
            public_fixture_path="target/public-fixture.vortex",
            fixture_listing=True,
            byte_range=(4, 8),
        )

        self.assertEqual(envelope.status, "success")
        self.assertEqual(envelope.field("provider_profile"), "public-no-credential-fixture")
        self.assertEqual(envelope.field("object_store_provider"), "s3")
        self.assertTrue(envelope.field_bool("object_store_io"))
        self.assertTrue(envelope.field_bool("public_no_credential_fixture_claim_allowed"))
        self.assertFalse(envelope.field_bool("fallback_attempted"))
        self.assertFalse(envelope.field_bool("external_engine_invoked"))

    def test_object_store_partition_discovery_smoke_wrapper_calls_local_emulator_profile(
        self,
    ) -> None:
        binary = self.fake_cli(
            textwrap.dedent(
                """
                import json, sys
                assert sys.argv[1:] == [
                    "object-store-partition-discovery-smoke",
                    "target/table",
                    "--profile",
                    "local-emulator",
                    "--partition-columns",
                    "region,date",
                    "--format",
                    "json",
                ], sys.argv
                print(json.dumps({
                    "schema_version": "shardloom.output.v2",
                    "command": "object-store-partition-discovery-smoke",
                    "status": "success",
                    "summary": "object-store local-emulator partition discovery smoke",
                    "human_text": "local-emulator partition discovery smoke",
                    "fallback": {"attempted": False, "allowed": False, "engine": None, "reason": "disabled"},
                    "diagnostics": [],
                    "fields": [
                        {"key": "partition_discovery_status", "value": "succeeded"},
                        {"key": "provider_profile", "value": "local-emulator"},
                        {"key": "partition_listing_status", "value": "performed_local_emulator"},
                        {"key": "requested_partition_columns", "value": "region,date"},
                        {"key": "discovered_partition_columns", "value": "date,region"},
                        {"key": "native_io_certificate_status", "value": "fixture_smoke_only"},
                        {"key": "claim_gate_status", "value": "fixture_smoke_only"},
                        {"key": "object_store_io", "value": "true"},
                        {"key": "object_store_listing_io", "value": "true"},
                        {"key": "credential_resolution_performed", "value": "false"},
                        {"key": "network_probe_performed", "value": "false"},
                        {"key": "provider_probe_performed", "value": "false"},
                        {"key": "fallback_attempted", "value": "false"},
                        {"key": "external_engine_invoked", "value": "false"}
                    ],
                }))
                """
            )
        )

        envelope = ShardLoomClient(
            binary=binary
        ).object_store_partition_discovery_smoke(
            "target/table",
            partition_columns=("region", "date"),
        )

        self.assertEqual(envelope.command, "object-store-partition-discovery-smoke")
        self.assertEqual(envelope.field("partition_discovery_status"), "succeeded")
        self.assertEqual(envelope.field("requested_partition_columns"), "region,date")
        self.assertTrue(envelope.field_bool("object_store_io"))
        self.assertTrue(envelope.field_bool("object_store_listing_io"))
        self.assertFalse(envelope.field_bool("credential_resolution_performed"))
        self.assertFalse(envelope.field_bool("network_probe_performed"))
        self.assertFalse(envelope.field_bool("provider_probe_performed"))
        self.assertFalse(envelope.field_bool("fallback_attempted"))
        self.assertFalse(envelope.field_bool("external_engine_invoked"))

    def test_context_object_store_partition_discovery_smoke_routes_to_client(self) -> None:
        binary = self.fake_cli(
            textwrap.dedent(
                """
                import json, sys
                assert sys.argv[1:] == [
                    "object-store-partition-discovery-smoke",
                    "target/table",
                    "--profile",
                    "local-emulator",
                    "--partition-columns",
                    "region,date",
                    "--format",
                    "json",
                ], sys.argv
                print(json.dumps({
                    "schema_version": "shardloom.output.v2",
                    "command": "object-store-partition-discovery-smoke",
                    "status": "success",
                    "summary": "ok",
                    "human_text": "ok",
                    "fallback": {"attempted": False, "allowed": False, "engine": None, "reason": "disabled"},
                    "diagnostics": [],
                    "fields": [
                        {"key": "partition_discovery_status", "value": "succeeded"},
                        {"key": "object_store_listing_io", "value": "true"},
                        {"key": "fallback_attempted", "value": "false"},
                        {"key": "external_engine_invoked", "value": "false"}
                    ],
                }))
                """
            )
        )
        ctx = ShardLoomContext(client=ShardLoomClient(binary=binary))

        envelope = ctx.object_store_partition_discovery_smoke(
            "target/table",
            partition_columns=("region", "date"),
        )

        self.assertEqual(envelope.command, "object-store-partition-discovery-smoke")
        self.assertEqual(envelope.field("partition_discovery_status"), "succeeded")
        self.assertTrue(envelope.field_bool("object_store_listing_io"))
        self.assertFalse(envelope.field_bool("fallback_attempted"))
        self.assertFalse(envelope.field_bool("external_engine_invoked"))

    def test_object_store_write_smoke_wrapper_calls_local_emulator_profile(self) -> None:
        binary = self.fake_cli(
            textwrap.dedent(
                """
                import json, sys
                assert sys.argv[1:] == [
                    "object-store-write-smoke",
                    "source/object.bin",
                    "target/object.bin",
                    "--profile",
                    "local-emulator",
                    "--idempotency-key",
                    "orders-batch-001",
                    "--allow-overwrite",
                    "--rollback-after-commit",
                    "--format",
                    "json",
                ], sys.argv
                print(json.dumps({
                    "schema_version": "shardloom.output.v2",
                    "command": "object-store-write-smoke",
                    "status": "success",
                    "summary": "object-store local-emulator write smoke",
                    "human_text": "local-emulator object-store write smoke",
                    "fallback": {"attempted": False, "allowed": False, "engine": None, "reason": "disabled"},
                    "diagnostics": [],
                    "fields": [
                        {"key": "object_store_write_status", "value": "rolled_back"},
                        {"key": "provider_profile", "value": "local-emulator"},
                        {"key": "commit_protocol_status", "value": "rolled_back"},
                        {"key": "rollback_status", "value": "performed_local_emulator_cleanup"},
                        {"key": "idempotency_key", "value": "orders-batch-001"},
                        {"key": "idempotency_status", "value": "caller_supplied"},
                        {"key": "native_io_certificate_status", "value": "fixture_smoke_only"},
                        {"key": "claim_gate_status", "value": "fixture_smoke_only"},
                        {"key": "object_store_io", "value": "true"},
                        {"key": "object_store_write_io", "value": "true"},
                        {"key": "fallback_attempted", "value": "false"},
                        {"key": "external_engine_invoked", "value": "false"}
                    ],
                }))
                """
            )
        )

        envelope = ShardLoomClient(binary=binary).object_store_write_smoke(
            "source/object.bin",
            "target/object.bin",
            idempotency_key="orders-batch-001",
            allow_overwrite=True,
            rollback_after_commit=True,
        )

        self.assertEqual(envelope.status, "success")
        self.assertEqual(envelope.field("object_store_write_status"), "rolled_back")
        self.assertTrue(envelope.field_bool("object_store_io"))
        self.assertTrue(envelope.field_bool("object_store_write_io"))
        self.assertFalse(envelope.field_bool("fallback_attempted"))
        self.assertFalse(envelope.field_bool("external_engine_invoked"))

    def test_object_store_write_recovery_smoke_wrapper_calls_local_emulator_profile(self) -> None:
        binary = self.fake_cli(
            textwrap.dedent(
                """
                import json, sys
                assert sys.argv[1:] == [
                    "object-store-write-recovery-smoke",
                    "target/object.bin",
                    "--profile",
                    "local-emulator",
                    "--idempotency-key",
                    "orders-batch-001",
                    "--format",
                    "json",
                ], sys.argv
                print(json.dumps({
                    "schema_version": "shardloom.output.v2",
                    "command": "object-store-write-recovery-smoke",
                    "status": "success",
                    "summary": "object-store local-emulator write recovery smoke",
                    "human_text": "local-emulator object-store write recovery smoke",
                    "fallback": {"attempted": False, "allowed": False, "engine": None, "reason": "disabled"},
                    "diagnostics": [],
                    "fields": [
                        {"key": "object_store_write_recovery_status", "value": "recovered"},
                        {"key": "provider_profile", "value": "local-emulator"},
                        {"key": "recovery_replay_status", "value": "recovered_local_emulator_sidecar"},
                        {"key": "target_digest_matched", "value": "true"},
                        {"key": "payload_digest_matched", "value": "true"},
                        {"key": "expected_idempotency_key", "value": "orders-batch-001"},
                        {"key": "recovered_idempotency_key", "value": "orders-batch-001"},
                        {"key": "idempotency_status", "value": "recovered_from_commit_manifest"},
                        {"key": "native_io_certificate_status", "value": "fixture_smoke_only"},
                        {"key": "claim_gate_status", "value": "fixture_smoke_only"},
                        {"key": "object_store_io", "value": "true"},
                        {"key": "object_store_read_io", "value": "true"},
                        {"key": "object_store_write_io", "value": "false"},
                        {"key": "write_io", "value": "false"},
                        {"key": "fallback_attempted", "value": "false"},
                        {"key": "external_engine_invoked", "value": "false"}
                    ],
                }))
                """
            )
        )

        envelope = ShardLoomClient(binary=binary).object_store_write_recovery_smoke(
            "target/object.bin",
            idempotency_key="orders-batch-001",
        )

        self.assertEqual(envelope.status, "success")
        self.assertEqual(envelope.field("object_store_write_recovery_status"), "recovered")
        self.assertTrue(envelope.field_bool("target_digest_matched"))
        self.assertTrue(envelope.field_bool("payload_digest_matched"))
        self.assertTrue(envelope.field_bool("object_store_read_io"))
        self.assertFalse(envelope.field_bool("object_store_write_io"))
        self.assertFalse(envelope.field_bool("write_io"))
        self.assertFalse(envelope.field_bool("fallback_attempted"))
        self.assertFalse(envelope.field_bool("external_engine_invoked"))

    def test_context_generated_output_to_object_store_uses_local_emulator_route(self) -> None:
        binary = self.fake_cli(
            textwrap.dedent(
                """
                import json, sys

                args = sys.argv[1:]
                if args == [
                    "generated-source-user-rows-smoke",
                    "target/staging/generated.jsonl",
                    "id:int64,label:utf8",
                    "id=1,label=alpha",
                    "--source-kind",
                    "user_rows",
                    "--output-format",
                    "jsonl",
                    "--allow-overwrite",
                    "--format",
                    "json",
                ]:
                    print(json.dumps({
                        "schema_version": "shardloom.output.v2",
                        "command": "generated-source-user-rows-smoke",
                        "status": "success",
                        "summary": "generated rows staged",
                        "human_text": "generated rows staged",
                        "fallback": {"attempted": False, "allowed": False, "engine": None, "reason": "disabled"},
                        "diagnostics": [],
                        "fields": [
                            {"key": "output_path", "value": "target/staging/generated.jsonl"},
                            {"key": "output_format", "value": "jsonl"},
                            {"key": "generated_source_kind", "value": "user_rows"},
                            {"key": "generated_source_row_count", "value": "1"},
                            {"key": "generated_source_certificate_status", "value": "present"},
                            {"key": "output_native_io_certificate_status", "value": "certified_local_jsonl_sink"},
                            {"key": "claim_gate_status", "value": "fixture_smoke_only"},
                            {"key": "fallback_attempted", "value": "false"},
                            {"key": "external_engine_invoked", "value": "false"}
                        ],
                    }))
                elif args == [
                    "object-store-write-smoke",
                    "target/staging/generated.jsonl",
                    "target/object-store/generated.jsonl",
                    "--profile",
                    "local-emulator",
                    "--idempotency-key",
                    "generated-output-001",
                    "--allow-overwrite",
                    "--rollback-after-commit",
                    "--format",
                    "json",
                ]:
                    print(json.dumps({
                        "schema_version": "shardloom.output.v2",
                        "command": "object-store-write-smoke",
                        "status": "success",
                        "summary": "object-store generated output",
                        "human_text": "object-store generated output",
                        "fallback": {"attempted": False, "allowed": False, "engine": None, "reason": "disabled"},
                        "diagnostics": [],
                        "fields": [
                            {"key": "object_store_write_status", "value": "rolled_back"},
                            {"key": "provider_profile", "value": "local-emulator"},
                            {"key": "commit_protocol_status", "value": "rolled_back"},
                            {"key": "rollback_status", "value": "performed_local_emulator_cleanup"},
                            {"key": "idempotency_key", "value": "generated-output-001"},
                            {"key": "native_io_certificate_status", "value": "fixture_smoke_only"},
                            {"key": "claim_gate_status", "value": "fixture_smoke_only"},
                            {"key": "object_store_io", "value": "true"},
                            {"key": "object_store_write_io", "value": "true"},
                            {"key": "fallback_attempted", "value": "false"},
                            {"key": "external_engine_invoked", "value": "false"}
                        ],
                    }))
                else:
                    raise AssertionError(sys.argv)
                """
            )
        )

        report = ShardLoomContext(
            ShardLoomClient(binary=binary)
        ).generated_output_to_object_store(
            "target/object-store/generated.jsonl",
            rows=[{"id": 1, "label": "alpha"}],
            staging_path="target/staging/generated.jsonl",
            idempotency_key="generated-output-001",
            allow_overwrite=True,
            rollback_after_commit=True,
        )

        self.assertIsInstance(report, GeneratedObjectStoreOutputReport)
        self.assertEqual(report.command, "object-store-write-smoke")
        self.assertEqual(report.status, "success")
        self.assertEqual(report.target_uri, "target/object-store/generated.jsonl")
        self.assertEqual(report.staging_path, "target/staging/generated.jsonl")
        self.assertEqual(report.output_format, "jsonl")
        self.assertTrue(report.generated_source_created)
        self.assertTrue(report.runtime_execution)
        self.assertTrue(report.write_io)
        self.assertTrue(report.object_store_io)
        self.assertEqual(report.object_store_write_status, "rolled_back")
        self.assertEqual(report.commit_protocol_status, "rolled_back")
        self.assertEqual(report.rollback_status, "performed_local_emulator_cleanup")
        self.assertIsNone(report.object_store_write_recovery_status)
        self.assertFalse(report.output_replay_verified)
        self.assertEqual(report.claim_gate_status, "fixture_smoke_only")
        self.assertFalse(report.fallback_attempted)
        self.assertFalse(report.external_engine_invoked)

    def test_context_generated_output_to_partitioned_object_store_verifies_discovery(
        self,
    ) -> None:
        binary = self.fake_cli(
            textwrap.dedent(
                """
                import json, sys

                args = sys.argv[1:]
                if args == [
                    "generated-source-user-rows-smoke",
                    "target/staging/partitioned.jsonl",
                    "id:int64,label:utf8",
                    "id=1,label=alpha",
                    "--source-kind",
                    "user_rows",
                    "--output-format",
                    "jsonl",
                    "--allow-overwrite",
                    "--format",
                    "json",
                ]:
                    print(json.dumps({
                        "schema_version": "shardloom.output.v2",
                        "command": "generated-source-user-rows-smoke",
                        "status": "success",
                        "summary": "generated rows staged",
                        "human_text": "generated rows staged",
                        "fallback": {"attempted": False, "allowed": False, "engine": None, "reason": "disabled"},
                        "diagnostics": [],
                        "fields": [
                            {"key": "output_path", "value": "target/staging/partitioned.jsonl"},
                            {"key": "output_format", "value": "jsonl"},
                            {"key": "generated_source_kind", "value": "user_rows"},
                            {"key": "generated_source_row_count", "value": "1"},
                            {"key": "generated_source_certificate_status", "value": "present"},
                            {"key": "output_native_io_certificate_status", "value": "certified_local_jsonl_sink"},
                            {"key": "claim_gate_status", "value": "fixture_smoke_only"},
                            {"key": "fallback_attempted", "value": "false"},
                            {"key": "external_engine_invoked", "value": "false"}
                        ],
                    }))
                elif args == [
                    "object-store-write-smoke",
                    "target/staging/partitioned.jsonl",
                    "target/object-store/partitioned/region=us/date=2026-06-06/part-00000.jsonl",
                    "--profile",
                    "local-emulator",
                    "--idempotency-key",
                    "partitioned-generated-output-001",
                    "--allow-overwrite",
                    "--format",
                    "json",
                ]:
                    print(json.dumps({
                        "schema_version": "shardloom.output.v2",
                        "command": "object-store-write-smoke",
                        "status": "success",
                        "summary": "object-store partitioned generated output",
                        "human_text": "object-store partitioned generated output",
                        "fallback": {"attempted": False, "allowed": False, "engine": None, "reason": "disabled"},
                        "diagnostics": [],
                        "fields": [
                            {"key": "object_store_write_status", "value": "committed"},
                            {"key": "provider_profile", "value": "local-emulator"},
                            {"key": "commit_protocol_status", "value": "committed"},
                            {"key": "idempotency_key", "value": "partitioned-generated-output-001"},
                            {"key": "native_io_certificate_status", "value": "fixture_smoke_only"},
                            {"key": "claim_gate_status", "value": "fixture_smoke_only"},
                            {"key": "object_store_io", "value": "true"},
                            {"key": "object_store_write_io", "value": "true"},
                            {"key": "fallback_attempted", "value": "false"},
                            {"key": "external_engine_invoked", "value": "false"}
                        ],
                    }))
                elif args == [
                    "object-store-write-recovery-smoke",
                    "target/object-store/partitioned/region=us/date=2026-06-06/part-00000.jsonl",
                    "--profile",
                    "local-emulator",
                    "--idempotency-key",
                    "partitioned-generated-output-001",
                    "--format",
                    "json",
                ]:
                    print(json.dumps({
                        "schema_version": "shardloom.output.v2",
                        "command": "object-store-write-recovery-smoke",
                        "status": "success",
                        "summary": "object-store partitioned generated output recovery",
                        "human_text": "object-store partitioned generated output recovery",
                        "fallback": {"attempted": False, "allowed": False, "engine": None, "reason": "disabled"},
                        "diagnostics": [],
                        "fields": [
                            {"key": "object_store_write_recovery_status", "value": "recovered"},
                            {"key": "provider_profile", "value": "local-emulator"},
                            {"key": "recovery_replay_status", "value": "recovered_local_emulator_sidecar"},
                            {"key": "target_digest_matched", "value": "true"},
                            {"key": "payload_digest_matched", "value": "true"},
                            {"key": "idempotency_status", "value": "recovered_from_commit_manifest"},
                            {"key": "native_io_certificate_status", "value": "fixture_smoke_only"},
                            {"key": "claim_gate_status", "value": "fixture_smoke_only"},
                            {"key": "object_store_io", "value": "true"},
                            {"key": "object_store_read_io", "value": "true"},
                            {"key": "object_store_write_io", "value": "false"},
                            {"key": "write_io", "value": "false"},
                            {"key": "fallback_attempted", "value": "false"},
                            {"key": "external_engine_invoked", "value": "false"}
                        ],
                    }))
                elif args == [
                    "object-store-partition-discovery-smoke",
                    "target/object-store/partitioned",
                    "--profile",
                    "local-emulator",
                    "--partition-columns",
                    "region,date",
                    "--format",
                    "json",
                ]:
                    print(json.dumps({
                        "schema_version": "shardloom.output.v2",
                        "command": "object-store-partition-discovery-smoke",
                        "status": "success",
                        "summary": "partition discovery",
                        "human_text": "partition discovery",
                        "fallback": {"attempted": False, "allowed": False, "engine": None, "reason": "disabled"},
                        "diagnostics": [],
                        "fields": [
                            {"key": "partition_discovery_status", "value": "succeeded"},
                            {"key": "partition_listing_status", "value": "performed_local_emulator"},
                            {"key": "requested_partition_columns", "value": "region,date"},
                            {"key": "discovered_partition_columns", "value": "date,region"},
                            {"key": "native_io_certificate_status", "value": "fixture_smoke_only"},
                            {"key": "claim_gate_status", "value": "fixture_smoke_only"},
                            {"key": "object_store_io", "value": "true"},
                            {"key": "object_store_listing_io", "value": "true"},
                            {"key": "credential_resolution_performed", "value": "false"},
                            {"key": "network_probe_performed", "value": "false"},
                            {"key": "provider_probe_performed", "value": "false"},
                            {"key": "fallback_attempted", "value": "false"},
                            {"key": "external_engine_invoked", "value": "false"}
                        ],
                    }))
                else:
                    raise AssertionError(sys.argv)
                """
            )
        )

        report = ShardLoomContext(
            ShardLoomClient(binary=binary)
        ).generated_output_to_partitioned_object_store(
            "target/object-store/partitioned",
            partition_values={"region": "us", "date": "2026-06-06"},
            rows=[{"id": 1, "label": "alpha"}],
            staging_path="target/staging/partitioned.jsonl",
            idempotency_key="partitioned-generated-output-001",
            allow_overwrite=True,
        )

        self.assertIsInstance(report, GeneratedPartitionedObjectStoreOutputReport)
        self.assertEqual(report.command, "object-store-partition-discovery-smoke")
        self.assertEqual(report.status, "success")
        self.assertEqual(
            report.partitioned_target_uri,
            "target/object-store/partitioned/region=us/date=2026-06-06/part-00000.jsonl",
        )
        self.assertEqual(report.object_store_write_status, "committed")
        self.assertEqual(report.object_store_write_recovery_status, "recovered")
        self.assertEqual(report.partition_discovery_status, "succeeded")
        self.assertEqual(report.discovered_partition_columns, ("date", "region"))
        self.assertTrue(report.generated_source_created)
        self.assertTrue(report.runtime_execution)
        self.assertTrue(report.output_replay_verified)
        self.assertTrue(report.write_io)
        self.assertTrue(report.object_store_io)
        self.assertTrue(report.object_store_listing_io)
        self.assertEqual(report.claim_gate_status, "fixture_smoke_only")
        self.assertFalse(report.fallback_attempted)
        self.assertFalse(report.external_engine_invoked)

    def test_context_foundry_generated_output_uses_local_style_dataset_route(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            result_dataset = Path(tempdir) / "result-dataset"
            evidence_dataset = Path(tempdir) / "evidence-dataset"
            result_part = result_dataset / "part-00000.jsonl"
            script = textwrap.dedent(
                """
                import json, sys

                assert sys.argv[1:] == [
                    "generated-source-user-rows-smoke",
                    __RESULT_PART__,
                    "id:int64,label:utf8",
                    "id=1,label=alpha",
                    "--source-kind",
                    "user_rows",
                    "--output-format",
                    "jsonl",
                    "--allow-overwrite",
                    "--format",
                    "json",
                ], sys.argv
                print(json.dumps({
                    "schema_version": "shardloom.output.v2",
                    "command": "generated-source-user-rows-smoke",
                    "status": "success",
                    "summary": "local Foundry-style generated output",
                    "human_text": "local Foundry-style generated output",
                    "fallback": {"attempted": False, "allowed": False, "engine": None, "reason": "disabled"},
                    "diagnostics": [],
                    "fields": [
                        {"key": "output_path", "value": __RESULT_PART__},
                        {"key": "output_format", "value": "jsonl"},
                        {"key": "generated_source_kind", "value": "user_rows"},
                        {"key": "generated_source_row_count", "value": "1"},
                        {"key": "generated_source_certificate_status", "value": "present"},
                        {"key": "output_native_io_certificate_status", "value": "certified_local_jsonl_sink"},
                        {"key": "sink_artifact_digest", "value": "sha256:generated"},
                        {"key": "claim_gate_status", "value": "fixture_smoke_only"},
                        {"key": "fallback_attempted", "value": "false"},
                        {"key": "external_engine_invoked", "value": "false"}
                    ],
                }))
                """
            ).replace("__RESULT_PART__", json.dumps(str(result_part)))
            binary = self.fake_cli(script)

            report = ShardLoomContext(
                ShardLoomClient(binary=binary)
            ).foundry_generated_output(
                result_dataset,
                rows=[{"id": 1, "label": "alpha"}],
                evidence_ref=evidence_dataset,
                allow_overwrite=True,
            )

            self.assertIsInstance(report, FoundryGeneratedOutputReport)
            self.assertEqual(report.command, "generated-source-user-rows-smoke")
            self.assertEqual(report.status, "success")
            self.assertEqual(report.result_dataset_path, str(result_dataset))
            self.assertEqual(report.evidence_dataset_path, str(evidence_dataset))
            self.assertTrue(report.runtime_execution)
            self.assertTrue(report.generated_source_created)
            self.assertTrue(report.foundry_style_output_api_invoked)
            self.assertTrue(report.write_io)
            self.assertFalse(report.foundry_runtime_invoked)
            self.assertFalse(report.foundry_output_api_invoked)
            self.assertEqual(report.claim_gate_status, "fixture_smoke_only")
            self.assertFalse(report.fallback_attempted)
            self.assertFalse(report.external_engine_invoked)
            self.assertTrue((result_dataset / "_dataset_metadata.json").exists())
            self.assertTrue((evidence_dataset / "part-00000.jsonl").exists())
            evidence_metadata = json.loads(
                (evidence_dataset / "_dataset_metadata.json").read_text(encoding="utf-8")
            )
            self.assertTrue(evidence_metadata["foundry_style_output_api_invoked"])
            self.assertFalse(evidence_metadata["foundry_runtime_invoked"])

    def test_context_foundry_generated_output_rejects_result_metadata_symlink(self) -> None:
        if not hasattr(os, "symlink"):
            self.skipTest("symlink is unavailable on this platform")
        with tempfile.TemporaryDirectory() as tempdir:
            result_dataset = Path(tempdir) / "result-dataset"
            result_dataset.mkdir()
            result_part = result_dataset / "part-00000.jsonl"
            outside = Path(tempdir) / "outside-metadata.json"
            outside.write_text("outside sentinel\n", encoding="utf-8")
            os.symlink(outside, result_dataset / "_dataset_metadata.json")
            binary = self._foundry_generated_output_fake_cli(result_part)

            with self.assertRaisesRegex(ValueError, "symlink"):
                ShardLoomContext(
                    ShardLoomClient(binary=binary)
                ).foundry_generated_output(
                    result_dataset,
                    rows=[{"id": 1, "label": "alpha"}],
                    allow_overwrite=True,
                )

            self.assertEqual(outside.read_text(encoding="utf-8"), "outside sentinel\n")
            self.assertTrue((result_dataset / "_dataset_metadata.json").is_symlink())

    def test_context_foundry_generated_output_rejects_evidence_part_symlink(self) -> None:
        if not hasattr(os, "symlink"):
            self.skipTest("symlink is unavailable on this platform")
        with tempfile.TemporaryDirectory() as tempdir:
            result_dataset = Path(tempdir) / "result-dataset"
            evidence_dataset = Path(tempdir) / "evidence-dataset"
            evidence_dataset.mkdir()
            result_part = result_dataset / "part-00000.jsonl"
            outside = Path(tempdir) / "outside-evidence.jsonl"
            outside.write_text("outside evidence sentinel\n", encoding="utf-8")
            os.symlink(outside, evidence_dataset / "part-00000.jsonl")
            binary = self._foundry_generated_output_fake_cli(result_part)

            with self.assertRaisesRegex(ValueError, "symlink"):
                ShardLoomContext(
                    ShardLoomClient(binary=binary)
                ).foundry_generated_output(
                    result_dataset,
                    rows=[{"id": 1, "label": "alpha"}],
                    evidence_ref=evidence_dataset,
                    allow_overwrite=True,
                )

            self.assertEqual(
                outside.read_text(encoding="utf-8"),
                "outside evidence sentinel\n",
            )
            self.assertTrue((evidence_dataset / "part-00000.jsonl").is_symlink())

    def _foundry_generated_output_fake_cli(self, result_part: Path) -> Path:
        script = textwrap.dedent(
            """
            import json, sys

            assert sys.argv[1:] == [
                "generated-source-user-rows-smoke",
                __RESULT_PART__,
                "id:int64,label:utf8",
                "id=1,label=alpha",
                "--source-kind",
                "user_rows",
                "--output-format",
                "jsonl",
                "--allow-overwrite",
                "--format",
                "json",
            ], sys.argv
            print(json.dumps({
                "schema_version": "shardloom.output.v2",
                "command": "generated-source-user-rows-smoke",
                "status": "success",
                "summary": "local Foundry-style generated output",
                "human_text": "local Foundry-style generated output",
                "fallback": {"attempted": False, "allowed": False, "engine": None, "reason": "disabled"},
                "diagnostics": [],
                "fields": [
                    {"key": "output_path", "value": __RESULT_PART__},
                    {"key": "output_format", "value": "jsonl"},
                    {"key": "generated_source_kind", "value": "user_rows"},
                    {"key": "generated_source_row_count", "value": "1"},
                    {"key": "generated_source_certificate_status", "value": "present"},
                    {"key": "output_native_io_certificate_status", "value": "certified_local_jsonl_sink"},
                    {"key": "sink_artifact_digest", "value": "sha256:generated"},
                    {"key": "claim_gate_status", "value": "fixture_smoke_only"},
                    {"key": "fallback_attempted", "value": "false"},
                    {"key": "external_engine_invoked", "value": "false"}
                ],
            }))
            """
        ).replace("__RESULT_PART__", json.dumps(str(result_part)))
        return self.fake_cli(script)

    def test_local_table_append_commit_rehearsal_wrapper_calls_local_manifest_profile(self) -> None:
        binary = self.fake_cli(
            textwrap.dedent(
                """
                import json, sys
                assert sys.argv[1:] == [
                    "local-table-append-commit-rehearsal-smoke",
                    "target/table/metadata/v2.json",
                    "--profile",
                    "local-manifest",
                    "--idempotency-key",
                    "orders-table-commit-001",
                    "--allow-overwrite",
                    "--rollback-after-commit",
                    "--format",
                    "json",
                ], sys.argv
                print(json.dumps({
                    "schema_version": "shardloom.output.v2",
                    "command": "local-table-append-commit-rehearsal-smoke",
                    "status": "success",
                    "summary": "local table append commit rehearsal smoke",
                    "human_text": "local table append commit rehearsal smoke",
                    "fallback": {"attempted": False, "allowed": False, "engine": None, "reason": "disabled"},
                    "diagnostics": [],
                    "fields": [
                        {"key": "table_append_commit_status", "value": "rolled_back"},
                        {"key": "provider_profile", "value": "local-manifest"},
                        {"key": "commit_protocol_status", "value": "rolled_back"},
                        {"key": "rollback_status", "value": "performed_local_manifest_cleanup"},
                        {"key": "idempotency_key", "value": "orders-table-commit-001"},
                        {"key": "idempotency_status", "value": "caller_supplied"},
                        {"key": "native_io_certificate_status", "value": "fixture_smoke_only"},
                        {"key": "claim_gate_status", "value": "scoped_local_table_append_commit_rehearsal_only"},
                        {"key": "table_metadata_write_performed", "value": "true"},
                        {"key": "manifest_write_performed", "value": "true"},
                        {"key": "object_store_io", "value": "false"},
                        {"key": "fallback_attempted", "value": "false"},
                        {"key": "external_engine_invoked", "value": "false"}
                    ],
                }))
                """
            )
        )

        envelope = ShardLoomClient(
            binary=binary
        ).local_table_append_commit_rehearsal_smoke(
            "target/table/metadata/v2.json",
            idempotency_key="orders-table-commit-001",
            allow_overwrite=True,
            rollback_after_commit=True,
        )

        self.assertEqual(envelope.status, "success")
        self.assertEqual(envelope.field("table_append_commit_status"), "rolled_back")
        self.assertTrue(envelope.field_bool("table_metadata_write_performed"))
        self.assertTrue(envelope.field_bool("manifest_write_performed"))
        self.assertFalse(envelope.field_bool("object_store_io"))
        self.assertFalse(envelope.field_bool("fallback_attempted"))
        self.assertFalse(envelope.field_bool("external_engine_invoked"))

    def test_local_table_commit_recovery_wrapper_calls_local_manifest_profile(self) -> None:
        binary = self.fake_cli(
            textwrap.dedent(
                """
                import json, sys
                assert sys.argv[1:] == [
                    "local-table-commit-recovery-smoke",
                    "target/table/metadata/v2.json",
                    "--profile",
                    "local-manifest",
                    "--idempotency-key",
                    "orders-table-commit-001",
                    "--format",
                    "json",
                ], sys.argv
                print(json.dumps({
                    "schema_version": "shardloom.output.v2",
                    "command": "local-table-commit-recovery-smoke",
                    "status": "success",
                    "summary": "local table commit recovery smoke",
                    "human_text": "local table commit recovery smoke",
                    "fallback": {"attempted": False, "allowed": False, "engine": None, "reason": "disabled"},
                    "diagnostics": [],
                    "fields": [
                        {"key": "table_commit_recovery_status", "value": "recovered"},
                        {"key": "provider_profile", "value": "local-manifest"},
                        {"key": "manifest_replay_status", "value": "verified_local_manifest_sidecar"},
                        {"key": "commit_record_replay_status", "value": "verified_local_manifest_sidecar"},
                        {"key": "manifest_digest_matched", "value": "true"},
                        {"key": "correctness_digest_matched", "value": "true"},
                        {"key": "recovered_idempotency_key", "value": "orders-table-commit-001"},
                        {"key": "claim_gate_status", "value": "scoped_local_table_commit_recovery_only"},
                        {"key": "table_metadata_read_performed", "value": "true"},
                        {"key": "manifest_write_performed", "value": "false"},
                        {"key": "object_store_io", "value": "false"},
                        {"key": "fallback_attempted", "value": "false"},
                        {"key": "external_engine_invoked", "value": "false"}
                    ],
                }))
                """
            )
        )

        envelope = ShardLoomClient(binary=binary).local_table_commit_recovery_smoke(
            "target/table/metadata/v2.json",
            idempotency_key="orders-table-commit-001",
        )

        self.assertEqual(envelope.status, "success")
        self.assertEqual(envelope.field("table_commit_recovery_status"), "recovered")
        self.assertTrue(envelope.field_bool("manifest_digest_matched"))
        self.assertTrue(envelope.field_bool("correctness_digest_matched"))
        self.assertTrue(envelope.field_bool("table_metadata_read_performed"))
        self.assertFalse(envelope.field_bool("manifest_write_performed"))
        self.assertFalse(envelope.field_bool("object_store_io"))
        self.assertFalse(envelope.field_bool("fallback_attempted"))
        self.assertFalse(envelope.field_bool("external_engine_invoked"))

    def test_context_runtime_smoke_helpers_delegate_to_client_commands(self) -> None:
        binary = self.fake_cli(
            textwrap.dedent(
                """
                import json, sys

                args = sys.argv[1:]
                if args == [
                    "object-store-read-smoke",
                    "target/object.bin",
                    "--profile",
                    "local-emulator",
                    "--range",
                    "2:4",
                    "--format",
                    "json",
                ]:
                    command = "object-store-read-smoke"
                    fields = [
                        {"key": "object_store_read_status", "value": "succeeded"},
                        {"key": "fallback_attempted", "value": "false"},
                        {"key": "external_engine_invoked", "value": "false"},
                    ]
                elif args == [
                    "object-store-write-smoke",
                    "source/object.bin",
                    "target/object.bin",
                    "--profile",
                    "local-emulator",
                    "--idempotency-key",
                    "orders-batch-001",
                    "--rollback-after-commit",
                    "--format",
                    "json",
                ]:
                    command = "object-store-write-smoke"
                    fields = [
                        {"key": "object_store_write_status", "value": "rolled_back"},
                        {"key": "fallback_attempted", "value": "false"},
                        {"key": "external_engine_invoked", "value": "false"},
                    ]
                elif args == ["local-table-metadata-read-smoke", "--format", "json"]:
                    command = "local-table-metadata-read-smoke"
                    fields = [
                        {"key": "table_metadata_read_performed", "value": "true"},
                        {"key": "fallback_attempted", "value": "false"},
                        {"key": "external_engine_invoked", "value": "false"},
                    ]
                elif args == [
                    "local-table-append-commit-rehearsal-smoke",
                    "target/table/metadata/v2.json",
                    "--profile",
                    "local-manifest",
                    "--idempotency-key",
                    "orders-table-commit-001",
                    "--rollback-after-commit",
                    "--format",
                    "json",
                ]:
                    command = "local-table-append-commit-rehearsal-smoke"
                    fields = [
                        {"key": "table_append_commit_status", "value": "rolled_back"},
                        {"key": "fallback_attempted", "value": "false"},
                        {"key": "external_engine_invoked", "value": "false"},
                    ]
                elif args == [
                    "local-table-commit-recovery-smoke",
                    "target/table/metadata/v2.json",
                    "--profile",
                    "local-manifest",
                    "--idempotency-key",
                    "orders-table-commit-001",
                    "--format",
                    "json",
                ]:
                    command = "local-table-commit-recovery-smoke"
                    fields = [
                        {"key": "table_commit_recovery_status", "value": "recovered"},
                        {"key": "fallback_attempted", "value": "false"},
                        {"key": "external_engine_invoked", "value": "false"},
                    ]
                elif args == [
                    "sqlite-local-import-export-smoke",
                    "target/orders.sqlite",
                    "--table",
                    "orders",
                    "--export-jsonl",
                    "target/orders.jsonl",
                    "--roundtrip-db",
                    "target/orders-roundtrip.sqlite",
                    "--order-by",
                    "id",
                    "--allow-overwrite",
                    "--format",
                    "json",
                ]:
                    command = "sqlite-local-import-export-smoke"
                    fields = [
                        {"key": "roundtrip_replay_verified", "value": "true"},
                        {"key": "fallback_attempted", "value": "false"},
                        {"key": "external_engine_invoked", "value": "false"},
                    ]
                else:
                    raise AssertionError(sys.argv)

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
        ctx = ShardLoomContext(client=ShardLoomClient(binary=binary))

        read = ctx.object_store_read_smoke("target/object.bin", byte_range=(2, 4))
        write = ctx.object_store_write_smoke(
            "source/object.bin",
            "target/object.bin",
            idempotency_key="orders-batch-001",
            rollback_after_commit=True,
        )
        metadata = ctx.local_table_metadata_read_smoke()
        append = ctx.local_table_append_commit_rehearsal_smoke(
            "target/table/metadata/v2.json",
            idempotency_key="orders-table-commit-001",
            rollback_after_commit=True,
        )
        recovery = ctx.local_table_commit_recovery_smoke(
            "target/table/metadata/v2.json",
            idempotency_key="orders-table-commit-001",
        )
        sqlite = ctx.sqlite_local_import_export_smoke(
            "target/orders.sqlite",
            table="orders",
            export_jsonl="target/orders.jsonl",
            roundtrip_db="target/orders-roundtrip.sqlite",
            order_by="id",
            allow_overwrite=True,
        )

        self.assertEqual(read.command, "object-store-read-smoke")
        self.assertEqual(write.command, "object-store-write-smoke")
        self.assertEqual(metadata.command, "local-table-metadata-read-smoke")
        self.assertEqual(append.command, "local-table-append-commit-rehearsal-smoke")
        self.assertEqual(recovery.command, "local-table-commit-recovery-smoke")
        self.assertEqual(sqlite.command, "sqlite-local-import-export-smoke")
        self.assertFalse(read.field_bool("fallback_attempted"))
        self.assertFalse(write.field_bool("fallback_attempted"))
        self.assertFalse(metadata.field_bool("fallback_attempted"))
        self.assertFalse(append.field_bool("fallback_attempted"))
        self.assertFalse(recovery.field_bool("fallback_attempted"))
        self.assertFalse(sqlite.field_bool("fallback_attempted"))
        self.assertFalse(read.field_bool("external_engine_invoked"))
        self.assertFalse(write.field_bool("external_engine_invoked"))
        self.assertFalse(metadata.field_bool("external_engine_invoked"))
        self.assertFalse(append.field_bool("external_engine_invoked"))
        self.assertFalse(recovery.field_bool("external_engine_invoked"))
        self.assertFalse(sqlite.field_bool("external_engine_invoked"))

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
                        {"key": "rest_runtime_unsupported_schema_version", "value": "shardloom.rest_api_runtime_unsupported_contract.v1"},
                        {"key": "rest_runtime_unsupported_report_id", "value": "gar-0035-a.rest_api_runtime_unsupported_contract"},
                        {"key": "rest_runtime_unsupported_row_order", "value": "http_listener_runtime,remote_execution_runtime,flight_adbc_transport_runtime,external_broker_integration,dependency_expanded_server,openapi_discovery_contract,plan_preview_contract,result_delivery_contract"},
                        {"key": "rest_runtime_unsupported_blocked_row_count", "value": "5"},
                        {"key": "rest_runtime_unsupported_report_only_row_count", "value": "3"},
                        {"key": "rest_runtime_unsupported_diagnostic_codes", "value": "SL_REST_SERVER_UNSUPPORTED,SL_REMOTE_EXECUTION_UNSUPPORTED,SL_COLUMNAR_TRANSPORT_UNSUPPORTED,SL_EXTERNAL_BROKER_UNSUPPORTED,SL_SERVER_DEPENDENCY_UNSUPPORTED,SL_REPORT_ONLY_SURFACE,SL_REPORT_ONLY_SURFACE,SL_REPORT_ONLY_SURFACE"},
                        {"key": "rest_runtime_unsupported_claim_gate_status", "value": "not_claim_grade"},
                        {"key": "rest_runtime_http_listener_supported", "value": "false"},
                        {"key": "rest_runtime_remote_execution_supported", "value": "false"},
                        {"key": "rest_runtime_flight_adbc_transport_supported", "value": "false"},
                        {"key": "rest_runtime_external_broker_supported", "value": "false"},
                        {"key": "rest_runtime_dependency_expansion_allowed", "value": "false"},
                        {"key": "rest_runtime_server_started", "value": "false"},
                        {"key": "rest_runtime_network_listener_opened", "value": "false"},
                        {"key": "rest_runtime_external_engine_invoked", "value": "false"},
                        {"key": "rest_runtime_fallback_attempted", "value": "false"},
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
        self.assertEqual(
            result.rest_runtime_unsupported_schema_version,
            "shardloom.rest_api_runtime_unsupported_contract.v1",
        )
        self.assertEqual(
            result.rest_runtime_unsupported_report_id,
            "gar-0035-a.rest_api_runtime_unsupported_contract",
        )
        self.assertIn("http_listener_runtime", result.rest_runtime_unsupported_rows)
        self.assertIn("result_delivery_contract", result.rest_runtime_unsupported_rows)
        self.assertEqual(result.rest_runtime_unsupported_blocked_row_count, 5)
        self.assertEqual(result.rest_runtime_unsupported_report_only_row_count, 3)
        self.assertIn(
            "SL_REMOTE_EXECUTION_UNSUPPORTED",
            result.rest_runtime_unsupported_diagnostic_codes,
        )
        self.assertEqual(result.rest_runtime_unsupported_claim_gate_status, "not_claim_grade")
        self.assertFalse(result.rest_runtime_http_listener_supported)
        self.assertFalse(result.rest_runtime_remote_execution_supported)
        self.assertFalse(result.rest_runtime_flight_adbc_transport_supported)
        self.assertFalse(result.rest_runtime_external_broker_supported)
        self.assertFalse(result.rest_runtime_dependency_expansion_allowed)
        self.assertTrue(result.rest_runtime_no_server_no_fallback_no_external_engine)
        self.assertTrue(result.contract_artifact_checked_in)
        self.assertFalse(result.server_started)
        self.assertFalse(result.network_listener_opened)
        self.assertFalse(result.fallback_attempted)

    def test_rest_api_views_expose_common_surface_parity_fields(self) -> None:
        envelope = OutputEnvelope.from_field_mapping(
            {
                "rest_api_surface_parity_schema_version": "shardloom.rest_api_surface_parity.v1",
                "rest_api_surface_parity_surface_id": "rest_api_contract_plan",
                "rest_api_surface_parity_status": "available_contract",
                "rest_api_cli_python_field_parity": "true",
                "rest_api_runtime_execution": "false",
                "rest_api_runtime_equivalent_api_claim_allowed": "false",
                "rest_api_policy_fields": "requested_execution_mode,engine_mode,fallback_policy,materialization_policy,result_policy,evidence_policy,effect_policy,network_policy,security_governance_policy",
                "rest_api_mode_selection_fields": "requested_execution_mode,selected_execution_mode,mode_selection_reason,claim_gate_status,fallback_attempted,external_engine_invoked",
                "rest_api_evidence_fields": "execution_certificate_ref,native_io_certificate_ref,no_fallback_evidence_artifact_ref,problem_details_diagnostic_code",
                "rest_api_evidence_refs": "openapi_contract_path,rest_runtime_unsupported_report_id",
                "rest_api_claim_gate_status": "not_claim_grade",
                "rest_api_claim_gate_reason": "contract-only surface",
                "rest_api_no_fallback_fields": "fallback_attempted,fallback_execution_allowed,external_engine_invoked,execution_delegated,no_fallback",
                "rest_api_fallback_attempted": "false",
                "rest_api_external_engine_invoked": "false",
                "rest_api_execution_delegated": "false",
                "rest_api_no_fallback_no_external_engine": "true",
            }
        )

        views = (
            RestApiContractPlan(envelope),
            RestApiDiscoveryContract(envelope),
            RestApiPlanPreview(envelope),
            RestApiLocalLifecycle(envelope),
            RestApiEventStream(envelope),
            RestApiSecurityGovernance(envelope),
            RestApiDataPlane(envelope),
        )

        for view in views:
            self.assertEqual(
                view.rest_api_surface_parity_schema_version,
                "shardloom.rest_api_surface_parity.v1",
            )
            self.assertEqual(
                view.rest_api_surface_parity_surface_id,
                "rest_api_contract_plan",
            )
            self.assertEqual(view.rest_api_surface_parity_status, "available_contract")
            self.assertTrue(view.rest_api_cli_python_field_parity)
            self.assertFalse(view.rest_api_runtime_execution)
            self.assertFalse(view.rest_api_runtime_equivalent_api_claim_allowed)
            self.assertIn("fallback_policy", view.rest_api_policy_fields)
            self.assertIn("claim_gate_status", view.rest_api_mode_selection_fields)
            self.assertIn("execution_certificate_ref", view.rest_api_evidence_fields)
            self.assertIn("rest_runtime_unsupported_report_id", view.rest_api_evidence_refs)
            self.assertEqual(view.rest_api_claim_gate_status, "not_claim_grade")
            self.assertIn("external_engine_invoked", view.rest_api_no_fallback_fields)
            self.assertFalse(view.rest_api_fallback_attempted)
            self.assertFalse(view.rest_api_external_engine_invoked)
            self.assertFalse(view.rest_api_execution_delegated)
            self.assertTrue(view.rest_api_no_fallback_no_external_engine)

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
                        {"key": "native_vortex_admission_lane_local_vortex_count_scalar_provider_version", "value": "__UPSTREAM_VORTEX_PROVIDER_VERSION__"},
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
                        {"key": "prepared_vortex_scan_pushdown_schema_version", "value": "shardloom.prepared_vortex.scan_pushdown_matrix.v1"},
                        {"key": "prepared_vortex_scan_pushdown_status", "value": "complete_for_current_prepared_native_runtime"},
                        {"key": "prepared_vortex_scan_pushdown_row_order", "value": "selective_filter,filter_projection_limit"},
                        {"key": "prepared_vortex_scan_pushdown_row_count", "value": "2"},
                        {"key": "prepared_vortex_scan_pushdown_supported_count", "value": "1"},
                        {"key": "prepared_vortex_scan_pushdown_partially_supported_count", "value": "1"},
                        {"key": "prepared_vortex_scan_pushdown_unsupported_count", "value": "0"},
                        {"key": "prepared_vortex_scan_pushdown_claim_gate_status", "value": "not_claim_grade"},
                        {"key": "prepared_vortex_scan_pushdown_all_rows_no_fallback", "value": "true"},
                        {"key": "prepared_vortex_scan_pushdown_all_rows_external_engine_invoked_false", "value": "true"},
                        {"key": "prepared_vortex_scan_pushdown_row_selective_filter_scenario", "value": "selective filter"},
                        {"key": "prepared_vortex_scan_pushdown_row_selective_filter_pushdown_status", "value": "scan_pushdown_supported"},
                        {"key": "prepared_vortex_scan_pushdown_row_selective_filter_filter_required", "value": "true"},
                        {"key": "prepared_vortex_scan_pushdown_row_selective_filter_projection_required", "value": "true"},
                        {"key": "prepared_vortex_scan_pushdown_row_selective_filter_limit_required", "value": "false"},
                        {"key": "prepared_vortex_scan_pushdown_row_selective_filter_filter_pushed_down", "value": "true"},
                        {"key": "prepared_vortex_scan_pushdown_row_selective_filter_projection_pushed_down", "value": "true"},
                        {"key": "prepared_vortex_scan_pushdown_row_selective_filter_limit_pushed_down", "value": "false"},
                        {"key": "prepared_vortex_scan_pushdown_row_selective_filter_filter_status", "value": "pushed_down"},
                        {"key": "prepared_vortex_scan_pushdown_row_selective_filter_projection_status", "value": "pushed_down"},
                        {"key": "prepared_vortex_scan_pushdown_row_selective_filter_limit_status", "value": "not_needed"},
                        {"key": "prepared_vortex_scan_pushdown_row_selective_filter_residual_limit_status", "value": "not_needed"},
                        {"key": "prepared_vortex_scan_pushdown_row_selective_filter_residual_limit_executor", "value": "none"},
                        {"key": "prepared_vortex_scan_pushdown_row_selective_filter_filter_columns_read", "value": "flag,value"},
                        {"key": "prepared_vortex_scan_pushdown_row_selective_filter_output_columns_read", "value": "id,value,metric"},
                        {"key": "prepared_vortex_scan_pushdown_row_selective_filter_filter_only_columns_read", "value": "flag"},
                        {"key": "prepared_vortex_scan_pushdown_row_selective_filter_blocker_id", "value": "none"},
                        {"key": "prepared_vortex_scan_pushdown_row_selective_filter_blocker_reason", "value": "none"},
                        {"key": "prepared_vortex_scan_pushdown_row_selective_filter_benchmark_refs", "value": "traditional_analytics.prepared_native.selective_filter"},
                        {"key": "prepared_vortex_scan_pushdown_row_selective_filter_claim_gate_status", "value": "not_claim_grade"},
                        {"key": "prepared_vortex_scan_pushdown_row_selective_filter_claim_boundary", "value": "prepared/native Vortex Scan pushdown evidence only"},
                        {"key": "prepared_vortex_scan_pushdown_row_selective_filter_fallback_attempted", "value": "false"},
                        {"key": "prepared_vortex_scan_pushdown_row_selective_filter_external_engine_invoked", "value": "false"},
                        {"key": "prepared_vortex_scan_pushdown_row_filter_projection_limit_scenario", "value": "filter + projection + limit"},
                        {"key": "prepared_vortex_scan_pushdown_row_filter_projection_limit_pushdown_status", "value": "scan_pushdown_partially_supported"},
                        {"key": "prepared_vortex_scan_pushdown_row_filter_projection_limit_filter_required", "value": "true"},
                        {"key": "prepared_vortex_scan_pushdown_row_filter_projection_limit_projection_required", "value": "true"},
                        {"key": "prepared_vortex_scan_pushdown_row_filter_projection_limit_limit_required", "value": "true"},
                        {"key": "prepared_vortex_scan_pushdown_row_filter_projection_limit_filter_pushed_down", "value": "true"},
                        {"key": "prepared_vortex_scan_pushdown_row_filter_projection_limit_projection_pushed_down", "value": "true"},
                        {"key": "prepared_vortex_scan_pushdown_row_filter_projection_limit_limit_pushed_down", "value": "false"},
                        {"key": "prepared_vortex_scan_pushdown_row_filter_projection_limit_filter_status", "value": "pushed_down"},
                        {"key": "prepared_vortex_scan_pushdown_row_filter_projection_limit_projection_status", "value": "pushed_down"},
                        {"key": "prepared_vortex_scan_pushdown_row_filter_projection_limit_limit_status", "value": "blocked_no_scan_limit_admission"},
                        {"key": "prepared_vortex_scan_pushdown_row_filter_projection_limit_residual_limit_status", "value": "applied_by_shardloom_native_source_order_residual"},
                        {"key": "prepared_vortex_scan_pushdown_row_filter_projection_limit_residual_limit_executor", "value": "shardloom_native"},
                        {"key": "prepared_vortex_scan_pushdown_row_filter_projection_limit_filter_columns_read", "value": "flag,value"},
                        {"key": "prepared_vortex_scan_pushdown_row_filter_projection_limit_output_columns_read", "value": "id,value,metric"},
                        {"key": "prepared_vortex_scan_pushdown_row_filter_projection_limit_filter_only_columns_read", "value": "flag"},
                        {"key": "prepared_vortex_scan_pushdown_row_filter_projection_limit_blocker_id", "value": "gar-perf-2c.limit_pushdown_not_admitted"},
                        {"key": "prepared_vortex_scan_pushdown_row_filter_projection_limit_blocker_reason", "value": "filter/projection/limit currently keeps the limit in ShardLoom residual logic because the scan limit is order-sensitive"},
                        {"key": "prepared_vortex_scan_pushdown_row_filter_projection_limit_benchmark_refs", "value": "traditional_analytics.prepared_native.filter_projection_limit"},
                        {"key": "prepared_vortex_scan_pushdown_row_filter_projection_limit_claim_gate_status", "value": "not_claim_grade"},
                        {"key": "prepared_vortex_scan_pushdown_row_filter_projection_limit_claim_boundary", "value": "prepared/native Vortex Scan pushdown evidence only"},
                        {"key": "prepared_vortex_scan_pushdown_row_filter_projection_limit_fallback_attempted", "value": "false"},
                        {"key": "prepared_vortex_scan_pushdown_row_filter_projection_limit_external_engine_invoked", "value": "false"},
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
                        {"key": "compute_row_direct_compatibility_transient_support_status", "value": "fixture_certified"},
                        {"key": "compute_row_direct_compatibility_transient_engine_mode", "value": "batch"},
                        {"key": "compute_row_direct_compatibility_transient_execution_mode", "value": "direct_compatibility_transient"},
                        {"key": "compute_row_direct_compatibility_transient_provider_kind", "value": "shardloom_kernel"},
                        {"key": "compute_row_direct_compatibility_transient_semantic_profile", "value": "ShardLoomNative"},
                        {"key": "compute_row_direct_compatibility_transient_materialization_decode_requirement", "value": "direct_local_source_state_materialization_boundary_reported"},
                        {"key": "compute_row_direct_compatibility_transient_memory_spill_requirement", "value": "bounded_local_direct_transient_no_spill_claim"},
                        {"key": "compute_row_direct_compatibility_transient_correctness_refs", "value": "traditional_direct_transient_tests,benchmark_harness_direct_transient"},
                        {"key": "compute_row_direct_compatibility_transient_benchmark_refs", "value": "direct_transient_csv_jsonl_structured_smoke_rows"},
                        {"key": "compute_row_direct_compatibility_transient_execution_certificate_refs", "value": "traditional_analytics.direct_transient.runtime"},
                        {"key": "compute_row_direct_compatibility_transient_native_io_refs", "value": "not_vortex_native; local SourceState Native I/O evidence only"},
                        {"key": "compute_row_direct_compatibility_transient_unsupported_diagnostic_code", "value": "none"},
                        {"key": "compute_row_direct_compatibility_transient_blocker_id", "value": "p75.direct_transient.not_vortex_native_claim"},
                        {"key": "compute_row_direct_compatibility_transient_required_future_evidence", "value": "broader_operator_coverage,result_sink_replay,claim_grade_benchmark_rows"},
                        {"key": "compute_row_direct_compatibility_transient_claim_gate_status", "value": "fixture_smoke_only"},
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
            ).replace(
                "__UPSTREAM_VORTEX_PROVIDER_VERSION__",
                UPSTREAM_VORTEX_PROVIDER_VERSION,
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
        self.assertEqual(count_lane.provider_version, UPSTREAM_VORTEX_PROVIDER_VERSION)
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
        self.assertEqual(
            result.prepared_vortex_scan_pushdown_status,
            "complete_for_current_prepared_native_runtime",
        )
        self.assertTrue(result.prepared_vortex_scan_pushdown_all_rows_no_fallback)
        self.assertTrue(
            result.prepared_vortex_scan_pushdown_all_rows_external_engine_free
        )
        pushdown_rows = {
            row.row_id: row for row in result.prepared_vortex_scan_pushdown_rows
        }
        selective_pushdown = pushdown_rows["selective_filter"]
        self.assertIsInstance(selective_pushdown, PreparedVortexScanPushdownRow)
        self.assertEqual(selective_pushdown.pushdown_status, "scan_pushdown_supported")
        self.assertTrue(selective_pushdown.filter_pushed_down)
        self.assertTrue(selective_pushdown.projection_pushed_down)
        self.assertFalse(selective_pushdown.limit_required)
        self.assertEqual(selective_pushdown.filter_columns_read, ("flag", "value"))
        self.assertEqual(selective_pushdown.filter_only_columns_read, ("flag",))
        limit_pushdown = pushdown_rows["filter_projection_limit"]
        self.assertEqual(
            limit_pushdown.limit_status,
            "blocked_no_scan_limit_admission",
        )
        self.assertEqual(limit_pushdown.residual_limit_executor, "shardloom_native")
        self.assertFalse(limit_pushdown.limit_pushed_down)
        self.assertFalse(limit_pushdown.fallback_attempted)
        self.assertFalse(limit_pushdown.external_engine_invoked)
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
            rows["direct_compatibility_transient"].support_status,
            "fixture_certified",
        )
        self.assertEqual(
            rows["direct_compatibility_transient"].unsupported_diagnostic_code,
            "none",
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
                        {"key": "engine_mode", "value": "batch"},
                        {"key": "control_plane_invoked", "value": "true"},
                        {"key": "control_plane_scope", "value": "in_process_local_batch"},
                        {"key": "network_policy", "value": "loopback_only_no_listener"},
                        {"key": "checkpoint_state_posture", "value": "local_ephemeral_result_lifecycle"},
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
                        {"key": "live_fixture_invoked", "value": "false"},
                        {"key": "hybrid_fixture_invoked", "value": "false"},
                        {"key": "remote_worker_invoked", "value": "false"},
                        {"key": "distributed_runtime_status", "value": "blocked"},
                        {"key": "distributed_worker_blocker_id", "value": "gar-runtime-impl-4q.distributed_worker_runtime_blocked"},
                        {"key": "distributed_claim_gate_status", "value": "not_distributed_runtime_grade"},
                        {"key": "small_result_boundary", "value": "inline_json_paged_json_jsonl_vortex_artifact_arrow_ipc_boundary"},
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
        self.assertEqual(result.engine_mode, "batch")
        self.assertTrue(result.control_plane_invoked)
        self.assertEqual(result.control_plane_scope, "in_process_local_batch")
        self.assertEqual(result.network_policy, "loopback_only_no_listener")
        self.assertEqual(result.checkpoint_state_posture, "local_ephemeral_result_lifecycle")
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
        self.assertFalse(result.live_fixture_invoked)
        self.assertFalse(result.hybrid_fixture_invoked)
        self.assertFalse(result.remote_worker_invoked)
        self.assertEqual(result.distributed_runtime_status, "blocked")
        self.assertEqual(
            result.distributed_worker_blocker_id,
            "gar-runtime-impl-4q.distributed_worker_runtime_blocked",
        )
        self.assertEqual(result.distributed_claim_gate_status, "not_distributed_runtime_grade")
        self.assertEqual(
            result.small_result_boundary,
            "inline_json_paged_json_jsonl_vortex_artifact_arrow_ipc_boundary",
        )
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

    def test_context_native_vortex_route_dispatches_engine_and_resource_policy(self) -> None:
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
                    "native_vortex",
                    "--memory-gb",
                    "3",
                    "--max-parallelism",
                    "2",
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
                        {"key": "selected_execution_mode", "value": "native_vortex"},
                        {"key": "resource_policy_memory_budget_gb", "value": "3"},
                        {"key": "resource_policy_max_parallelism", "value": "2"},
                        {"key": "computed_result_sink_requested", "value": "true"},
                        {"key": "external_engine_invoked", "value": "false"}
                    ],
                }))
                """
            )
        )
        ctx = ShardLoomContext(client=ShardLoomClient(binary=binary))
        route = ctx.native_vortex_route(
            "fact.vortex",
            "dim.vortex",
            workspace="sink-work",
            execution_mode="native_vortex",
            memory_gb=3,
            max_parallelism=2,
        )

        self.assertIsInstance(route, NativeVortexRoute)
        self.assertEqual(route.route_fields()["vortex_normalization_point"], "native_vortex_boundary")
        self.assertFalse(route.route_fields()["preparation_included_in_route"])
        query = route.query("selective filter")
        self.assertIsInstance(query, NativeVortexQuery)
        result = query.write_vortex()

        self.assertEqual(result.command, "traditional-analytics-vortex-run")
        self.assertEqual(result.field("selected_execution_mode"), "native_vortex")
        self.assertEqual(result.field("resource_policy_memory_budget_gb"), "3")
        self.assertEqual(result.field("resource_policy_max_parallelism"), "2")
        self.assertFalse(result.fallback.attempted)

    def test_session_native_vortex_route_returns_route_handle(self) -> None:
        session = ShardLoomSession(client=ShardLoomClient(binary=("unused",)))

        route = session.native_vortex_route(
            "fact.vortex",
            "dim.vortex",
            execution_mode="prepared_vortex",
            memory_gb=4,
            max_parallelism=1,
        )

        self.assertIsInstance(route, NativeVortexRoute)
        self.assertEqual(route.execution_mode, "prepared_vortex")
        self.assertEqual(route.memory_gb, 4)
        self.assertEqual(route.max_parallelism, 1)
        self.assertFalse(route.fallback_attempted)
        self.assertFalse(route.external_engine_invoked)

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
                        {"key": "prepared_state_id", "value": "prepared-state://abc"},
                        {"key": "prepared_state_digest", "value": "fnv1a64:abc"},
                        {"key": "source_state_id", "value": "source-state://abc"},
                        {"key": "source_state_digest", "value": "fnv1a64:source"},
                        {"key": "source_state_columnar_preserved", "value": "false"},
                        {"key": "source_state_record_batch_count", "value": "2"},
                        {"key": "vortex_array_build_provider_kind", "value": "vortex_array_kernel"},
                        {"key": "vortex_array_build_provider_surface", "value": "ArrayRef::from_arrow(RecordBatch)"},
                        {"key": "vortex_array_build_strategy", "value": "vortex_from_text_adapter_record_batch_without_persistent_traditional_rows"},
                        {"key": "vortex_array_build_input_layout", "value": "traditional_text_adapter_record_batch"},
                        {"key": "vortex_array_build_record_batch_count", "value": "2"},
                        {"key": "vortex_array_build_manual_scalar_copy_avoided", "value": "true"},
                        {"key": "vortex_preparation_spine_status", "value": "admitted_local_preparation_spine"},
                        {"key": "vortex_preparation_spine_vortex_first_decision", "value": "use_vortex_native_provider"},
                        {"key": "vortex_preparation_spine_provider_kind", "value": "vortex_array_kernel"},
                        {"key": "vortex_preparation_spine_provider_api_surface", "value": "ArrayRef::from_arrow(RecordBatch);VortexSession::write_options().write(ArrayStream);VortexSession::open_options().open_buffer(...).scan().into_array_stream().read_all()"},
                        {"key": "vortex_preparation_spine_source_split_count", "value": "2"},
                        {"key": "vortex_preparation_spine_source_split_refs", "value": "source-state://abc:split=1:bytes=0..128:rows=0..2;source-state://abc:split=2:bytes=0..128:rows=2..4"},
                        {"key": "vortex_preparation_spine_native_io_certificate_status", "value": "certified_local_vortex_preparation_spine"},
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
        self.assertEqual(artifacts.prepared_state_id, "prepared-state://abc")
        self.assertEqual(artifacts.source_state_digest, "fnv1a64:source")
        self.assertFalse(artifacts.source_state_columnar_preserved)
        self.assertEqual(artifacts.source_state_record_batch_count, 2)
        self.assertEqual(artifacts.vortex_array_build_provider_kind, "vortex_array_kernel")
        self.assertEqual(
            artifacts.vortex_array_build_provider_surface,
            "ArrayRef::from_arrow(RecordBatch)",
        )
        self.assertEqual(
            artifacts.vortex_array_build_strategy,
            "vortex_from_text_adapter_record_batch_without_persistent_traditional_rows",
        )
        self.assertEqual(
            artifacts.vortex_array_build_input_layout,
            "traditional_text_adapter_record_batch",
        )
        self.assertEqual(artifacts.vortex_array_build_record_batch_count, 2)
        self.assertTrue(artifacts.vortex_array_build_manual_scalar_copy_avoided)
        self.assertEqual(
            artifacts.vortex_preparation_spine_status,
            "admitted_local_preparation_spine",
        )
        self.assertEqual(
            artifacts.vortex_preparation_spine_vortex_first_decision,
            "use_vortex_native_provider",
        )
        self.assertEqual(
            artifacts.vortex_preparation_spine_provider_kind,
            "vortex_array_kernel",
        )
        self.assertEqual(artifacts.vortex_preparation_spine_source_split_count, 2)
        self.assertEqual(
            artifacts.vortex_preparation_spine_source_split_refs,
            (
                "source-state://abc:split=1:bytes=0..128:rows=0..2",
                "source-state://abc:split=2:bytes=0..128:rows=2..4",
            ),
        )
        self.assertEqual(
            artifacts.vortex_preparation_spine_native_io_certificate_status,
            "certified_local_vortex_preparation_spine",
        )
        self.assertEqual(artifacts.cleanup_policy, "caller_owned_workspace_cleanup")
        self.assertTrue(artifacts.reuse_eligible)

    def test_traditional_analytics_vortex_batch_run_preserves_cli_default_mode(self) -> None:
        binary = self.fake_cli(
            textwrap.dedent(
                """
                import json, sys
                assert sys.argv[1:] == [
                    "traditional-analytics-vortex-batch-run",
                    "hash join,join + aggregate",
                    "fact.vortex",
                    "dim.vortex",
                    "--cdc-delta-vortex",
                    "cdc.vortex",
                    "--workspace",
                    "out",
                    "--write-result-vortex",
                    "--evidence-level",
                    "full_replay",
                    "--format",
                    "json",
                ], sys.argv
                print(json.dumps({
                    "schema_version": "shardloom.output.v2",
                    "command": "traditional-analytics-vortex-batch-run",
                    "status": "success",
                    "summary": "ok",
                    "human_text": "ok",
                    "fallback": {"attempted": False, "allowed": False, "engine": None, "reason": "disabled"},
                    "diagnostics": [],
                    "fields": [
                        {"key": "schema_version", "value": "shardloom.traditional_analytics.vortex_batch.v1"},
                        {"key": "scenario_order", "value": "hash join,join + aggregate"},
                        {"key": "source_state_digest", "value": "fnv1a64:batch"},
                        {"key": "source_state_reuse_status", "value": "per_batch_dimension_label_state_reused"},
                        {"key": "source_state_reused", "value": "true"},
                        {"key": "source_state_recompute_avoided_count", "value": "1"},
                        {"key": "selected_evidence_level", "value": "full_replay"}
                    ],
                }))
                """
            )
        )

        result = ShardLoomClient(binary=binary).traditional_analytics_vortex_batch_run(
            ["hash join", "join + aggregate"],
            "fact.vortex",
            "dim.vortex",
            cdc_delta_vortex="cdc.vortex",
            workspace="out",
            write_result_vortex=True,
            evidence_level="full_replay",
        )

        self.assertEqual(result.command, "traditional-analytics-vortex-batch-run")
        self.assertEqual(result.field("source_state_digest"), "fnv1a64:batch")

    def test_traditional_analytics_vortex_batch_run_accepts_explicit_mode(self) -> None:
        binary = self.fake_cli(
            textwrap.dedent(
                """
                import json, sys
                assert sys.argv[1:] == [
                    "traditional-analytics-vortex-batch-run",
                    "hash join",
                    "fact.vortex",
                    "dim.vortex",
                    "--execution-mode",
                    "prepared_vortex",
                    "--format",
                    "json",
                ], sys.argv
                print(json.dumps({
                    "schema_version": "shardloom.output.v2",
                    "command": "traditional-analytics-vortex-batch-run",
                    "status": "success",
                    "summary": "ok",
                    "human_text": "ok",
                    "fallback": {"attempted": False, "allowed": False, "engine": None, "reason": "disabled"},
                    "diagnostics": [],
                    "fields": [
                        {"key": "selected_execution_mode", "value": "prepared_vortex"}
                    ],
                }))
                """
            )
        )

        result = ShardLoomClient(binary=binary).traditional_analytics_vortex_batch_run(
            "hash join",
            "fact.vortex",
            "dim.vortex",
            execution_mode="prepared_vortex",
        )

        self.assertEqual(result.field("selected_execution_mode"), "prepared_vortex")

    def test_traditional_analytics_prepare_batch_run_dispatches_combined_route(self) -> None:
        binary = self.fake_cli(
            textwrap.dedent(
                """
                import json, sys
                assert sys.argv[1:] == [
                    "traditional-analytics-prepare-batch-run",
                    "selective filter,filter + projection + limit",
                    "fact.csv",
                    "dim.csv",
                    "--workspace",
                    "prepare-work",
                    "--input-format",
                    "csv",
                    "--cdc-delta",
                    "cdc.csv",
                    "--result-workspace",
                    "batch-work",
                    "--write-result-vortex",
                    "--evidence-level",
                    "full_replay",
                    "--memory-gb",
                    "2",
                    "--max-parallelism",
                    "4",
                    "--format",
                    "json",
                ], sys.argv
                print(json.dumps({
                    "schema_version": "shardloom.output.v2",
                    "command": "traditional-analytics-prepare-batch-run",
                    "status": "success",
                    "summary": "ok",
                    "human_text": "ok",
                    "fallback": {"attempted": False, "allowed": False, "engine": None, "reason": "disabled"},
                    "diagnostics": [],
                    "fields": [
                        {"key": "schema_version", "value": "shardloom.traditional_analytics.vortex_batch.v1"},
                        {"key": "prepare_batch_schema_version", "value": "shardloom.traditional_analytics.prepare_and_batch.v1"},
                        {"key": "prepare_batch_lifecycle_schema_version", "value": "shardloom.traditional_analytics.prepared_native_vortex_lifecycle.v1"},
                        {"key": "prepare_batch_lifecycle_status", "value": "prepared_vortex_lifecycle_complete_with_output_replay"},
                        {"key": "prepare_batch_lifecycle_output_status", "value": "vortex_result_sink_written_and_replay_verified"},
                        {"key": "prepare_batch_lifecycle_no_standalone_lane", "value": "true"},
                        {"key": "prepare_batch_scale_schema_version", "value": "shardloom.traditional_analytics.prepared_vortex_local_scale.v1"},
                        {"key": "prepare_batch_scale_route", "value": "compatibility_import_certified_to_prepared_vortex_batch"},
                        {"key": "prepare_batch_scale_runtime_status", "value": "prepared_vortex_in_between_processing_evidence"},
                        {"key": "prepare_batch_scale_no_standalone_lane", "value": "true"},
                        {"key": "prepare_batch_scale_real_bytes", "value": "true"},
                        {"key": "prepare_batch_scale_memory_budget_bytes", "value": "2147483648"},
                        {"key": "prepare_batch_scale_split_runtime_status", "value": "scheduled_reader_chunk_execution_completed"},
                        {"key": "prepare_batch_scale_split_execution_certificate_status", "value": "certified"},
                        {"key": "prepare_batch_scale_split_operator_runtime_status", "value": "local_split_operator_runtime_partially_certified"},
                        {"key": "prepare_batch_scale_split_operator_certified_count", "value": "1"},
                        {"key": "prepare_batch_scale_claim_gate_status", "value": "not_scale_grade"},
                        {"key": "prepare_batch_preparation_included_in_batch_timing", "value": "false"},
                        {"key": "prepare_batch_fact_vortex_path", "value": "fact.vortex"},
                        {"key": "prepare_batch_dim_vortex_path", "value": "dim.vortex"},
                        {"key": "prepare_batch_cdc_delta_vortex_path", "value": "cdc.vortex"},
                        {"key": "prepare_batch_fact_vortex_digest", "value": "sha256:f"},
                        {"key": "prepare_batch_dim_vortex_digest", "value": "sha256:d"},
                        {"key": "prepare_batch_cdc_delta_vortex_digest", "value": "sha256:c"},
                        {"key": "prepare_batch_prepared_artifact_cleanup_policy", "value": "caller_owned_workspace_cleanup"},
                        {"key": "prepare_batch_prepared_artifact_reuse_eligible", "value": "true"},
                        {"key": "prepare_batch_source_state_columnar_preserved", "value": "false"},
                        {"key": "prepare_batch_source_state_record_batch_count", "value": "2"},
                        {"key": "prepare_batch_vortex_array_build_provider_kind", "value": "vortex_array_kernel"},
                        {"key": "prepare_batch_vortex_array_build_provider_surface", "value": "ArrayRef::from_arrow(RecordBatch)"},
                        {"key": "prepare_batch_vortex_array_build_strategy", "value": "vortex_from_arrow_record_batch_mixed_traditional_text_and_direct_columnar"},
                        {"key": "prepare_batch_vortex_array_build_input_layout", "value": "mixed_traditional_arrow_record_batch_text_adapter_and_vortex_provider_record_batch"},
                        {"key": "prepare_batch_vortex_array_build_record_batch_count", "value": "3"},
                        {"key": "prepare_batch_vortex_array_build_manual_scalar_copy_avoided", "value": "true"},
                        {"key": "scenario_order", "value": "selective-filter,filter---projection---limit"},
                        {"key": "session_route_used", "value": "true"},
                        {"key": "process_spawn_count", "value": "1"},
                        {"key": "source_state_digest", "value": "fnv1a64:batch"},
                        {"key": "source_state_reuse_status", "value": "per_batch_selective_filter_state_reused"},
                        {"key": "source_state_reused", "value": "true"},
                        {"key": "selected_evidence_level", "value": "full_replay"},
                        {"key": "scenario_selective-filter_prepared_vortex_scale_no_standalone_lane", "value": "true"},
                        {"key": "scenario_selective-filter_prepared_vortex_scale_real_bytes", "value": "true"},
                        {"key": "scenario_selective-filter_prepared_vortex_scale_split_runtime_status", "value": "scheduled_reader_chunk_execution_completed"},
                        {"key": "scenario_selective-filter_prepared_vortex_scale_split_execution_certificate_status", "value": "certified"},
                        {"key": "scenario_selective-filter_prepared_vortex_scale_split_operator_runtime_status", "value": "local_split_operator_runtime_certified"},
                        {"key": "scenario_selective-filter_prepared_vortex_scale_split_operator_execution_certificate_status", "value": "certified"},
                        {"key": "scenario_selective-filter_prepared_vortex_scale_idempotency_key", "value": "prepared-vortex:fnv1a64-feedface"},
                        {"key": "scenario_selective-filter_prepared_native_vortex_lifecycle_status", "value": "prepared_native_vortex_lifecycle_complete_with_output_replay"},
                        {"key": "scenario_selective-filter_prepared_native_vortex_lifecycle_output_status", "value": "vortex_result_sink_written_and_replay_verified"},
                        {"key": "scenario_selective-filter_prepared_native_vortex_lifecycle_no_standalone_lane", "value": "true"},
                        {"key": "fallback_attempted", "value": "false"},
                        {"key": "external_engine_invoked", "value": "false"}
                    ],
                }))
                """
            )
        )

        result = ShardLoomClient(binary=binary).traditional_analytics_prepare_batch_run(
            ["selective filter", "filter + projection + limit"],
            "fact.csv",
            "dim.csv",
            workspace="prepare-work",
            input_format="csv",
            cdc_delta_input="cdc.csv",
            result_workspace="batch-work",
            write_result_vortex=True,
            evidence_level="full_replay",
            memory_gb=2,
            max_parallelism=4,
        )

        self.assertEqual(result.command, "traditional-analytics-prepare-batch-run")
        self.assertEqual(
            result.field("prepare_batch_schema_version"),
            "shardloom.traditional_analytics.prepare_and_batch.v1",
        )
        self.assertEqual(
            result.field("source_state_reuse_status"),
            "per_batch_selective_filter_state_reused",
        )
        self.assertEqual(result.field("session_route_used"), "true")
        self.assertEqual(result.field("process_spawn_count"), "1")
        self.assertEqual(result.field("prepare_batch_scale_no_standalone_lane"), "true")
        self.assertEqual(result.field("prepare_batch_scale_real_bytes"), "true")
        self.assertEqual(
            result.field("prepare_batch_scale_split_runtime_status"),
            "scheduled_reader_chunk_execution_completed",
        )
        self.assertEqual(
            result.field("prepare_batch_scale_split_execution_certificate_status"),
            "certified",
        )
        self.assertEqual(
            result.field("prepare_batch_scale_split_operator_runtime_status"),
            "local_split_operator_runtime_partially_certified",
        )
        self.assertEqual(result.field("prepare_batch_scale_claim_gate_status"), "not_scale_grade")
        self.assertEqual(
            result.field(
                "scenario_selective-filter_prepared_vortex_scale_split_operator_execution_certificate_status"
            ),
            "certified",
        )
        self.assertEqual(
            result.field("scenario_selective-filter_prepared_vortex_scale_idempotency_key"),
            "prepared-vortex:fnv1a64-feedface",
        )
        self.assertEqual(
            result.field("prepare_batch_lifecycle_status"),
            "prepared_vortex_lifecycle_complete_with_output_replay",
        )
        self.assertEqual(
            result.field("scenario_selective-filter_prepared_native_vortex_lifecycle_status"),
            "prepared_native_vortex_lifecycle_complete_with_output_replay",
        )
        self.assertFalse(result.fallback.attempted)

    def test_prepare_and_run_traditional_analytics_vortex_batch_reuses_artifacts(self) -> None:
        binary = self.fake_cli(
            textwrap.dedent(
                """
                import json, sys
                assert sys.argv[1:] == [
                    "traditional-analytics-prepare-batch-run",
                    "hash join,join + aggregate",
                    "fact.csv",
                    "dim.csv",
                    "--workspace",
                    "work",
                    "--input-format",
                    "csv",
                    "--cdc-delta",
                    "cdc.csv",
                    "--result-workspace",
                    "batch-work",
                    "--evidence-level",
                    "certified",
                    "--memory-gb",
                    "2",
                    "--max-parallelism",
                    "4",
                    "--format",
                    "json",
                ], sys.argv
                print(json.dumps({
                    "schema_version": "shardloom.output.v2",
                    "command": "traditional-analytics-prepare-batch-run",
                    "status": "success",
                    "summary": "prepare/batch",
                    "human_text": "prepare/batch",
                    "fallback": {"attempted": False, "allowed": False, "engine": None, "reason": "disabled"},
                    "diagnostics": [],
                    "fields": [
                        {"key": "prepare_batch_schema_version", "value": "shardloom.traditional_analytics.prepare_and_batch.v1"},
                        {"key": "prepare_batch_lifecycle_status", "value": "prepared_vortex_lifecycle_scan_complete_output_not_requested"},
                        {"key": "prepare_batch_lifecycle_output_status", "value": "vortex_result_sink_not_requested"},
                        {"key": "prepare_batch_lifecycle_no_standalone_lane", "value": "true"},
                        {"key": "prepare_batch_fact_vortex_path", "value": "fact.vortex"},
                        {"key": "prepare_batch_dim_vortex_path", "value": "dim.vortex"},
                        {"key": "prepare_batch_cdc_delta_vortex_path", "value": "cdc.vortex"},
                        {"key": "prepare_batch_fact_vortex_digest", "value": "sha256:f"},
                        {"key": "prepare_batch_dim_vortex_digest", "value": "sha256:d"},
                        {"key": "prepare_batch_cdc_delta_vortex_digest", "value": "sha256:c"},
                        {"key": "prepare_batch_prepared_artifact_cleanup_policy", "value": "caller_owned_workspace_cleanup"},
                        {"key": "prepare_batch_prepared_artifact_reuse_eligible", "value": "true"},
                        {"key": "scenario_order", "value": "hash join,join + aggregate"},
                        {"key": "session_route_used", "value": "true"},
                        {"key": "process_spawn_count", "value": "1"},
                        {"key": "source_state_digest", "value": "fnv1a64:batch"},
                        {"key": "source_state_reuse_status", "value": "per_batch_dimension_label_state_reused"},
                        {"key": "source_state_reused", "value": "true"},
                        {"key": "source_state_recompute_avoided_count", "value": "1"},
                        {"key": "selected_evidence_level", "value": "certified"},
                        {"key": "external_engine_invoked", "value": "false"}
                    ],
                }))
                """
            )
        )

        result = ShardLoomClient(
            binary=binary
        ).prepare_and_run_traditional_analytics_vortex_batch(
            ["hash join", "join + aggregate"],
            "fact.csv",
            "dim.csv",
            workspace="work",
            input_format="csv",
            cdc_delta_input="cdc.csv",
            result_workspace="batch-work",
            evidence_level="certified",
            memory_gb=2,
            max_parallelism=4,
        )

        self.assertIsInstance(result, PreparedVortexBatchResult)
        self.assertEqual(result.artifacts.cdc_delta_vortex_path, "cdc.vortex")
        self.assertEqual(result.scenario_order, ("hash join", "join + aggregate"))
        self.assertEqual(result.source_state_digest, "fnv1a64:batch")
        self.assertTrue(result.source_state_reused)
        self.assertEqual(result.source_state_recompute_avoided_count, 1)
        self.assertTrue(result.session_route_used)
        self.assertEqual(result.process_spawn_count, 1)
        self.assertTrue(result.prepared_artifacts_reuse_eligible)
        self.assertEqual(result.selected_evidence_level, "certified")
        self.assertEqual(
            result.lifecycle_status,
            "prepared_vortex_lifecycle_scan_complete_output_not_requested",
        )
        self.assertEqual(result.lifecycle_output_status, "vortex_result_sink_not_requested")
        self.assertFalse(result.lifecycle_complete_with_output_replay)
        self.assertTrue(result.lifecycle_no_standalone_lane)
        self.assertFalse(result.fallback_attempted)
        self.assertFalse(result.external_engine_invoked)

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

    def test_benchmark_constitution_helper(self) -> None:
        binary = self.fake_cli(
            textwrap.dedent(
                """
                import json, sys
                assert sys.argv[1:] == [
                    "benchmark-constitution",
                    "foundation",
                    "--format",
                    "json",
                ], sys.argv
                print(json.dumps({
                    "schema_version": "shardloom.output.v2",
                    "command": "benchmark-constitution",
                    "status": "success",
                    "summary": "ok",
                    "human_text": "ok",
                    "fallback": {"attempted": False, "allowed": False, "engine": None, "reason": "disabled"},
                    "diagnostics": [],
                    "fields": [
                        {"key": "scope", "value": "foundation"},
                        {"key": "benchmark_constitution_status", "value": "missing_evidence"},
                    ],
                }))
                """
            )
        )

        result = ShardLoomClient(binary=binary).benchmark_constitution("foundation")

        self.assertEqual(result.field("scope"), "foundation")
        self.assertEqual(result.field("benchmark_constitution_status"), "missing_evidence")

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

    def test_extension_udf_and_sqlite_effectful_operation_helpers(self) -> None:
        binary = self.fake_cli(
            textwrap.dedent(
                """
                import json, sys

                args = sys.argv[1:]
                if args == ["extension-registry", "--format", "json"]:
                    command = "extension-registry"
                    fields = [
                        {"key": "effectful_operation_admission_matrix_schema_version", "value": "shardloom.effectful_operation_admission_matrix.v1"},
                        {"key": "extension_code_executed", "value": "false"},
                        {"key": "fallback_attempted", "value": "false"},
                        {"key": "external_engine_invoked", "value": "false"},
                    ]
                elif args == ["udf-registry", "--format", "json"]:
                    command = "udf-registry"
                    fields = [
                        {"key": "typed_udf_registry_schema_version", "value": "shardloom.typed_udf_registry.v1"},
                        {"key": "typed_udf_registry_support_status", "value": "scoped_fixture_supported"},
                        {"key": "typed_udf_registry_claim_gate_status", "value": "fixture_smoke_only"},
                        {"key": "typed_udf_registry_admitted_local_fixture_count", "value": "1"},
                        {"key": "typed_udf_registry_arbitrary_runtime_bridge_available", "value": "false"},
                        {"key": "typed_udf_registry_network_access_allowed", "value": "false"},
                        {"key": "typed_udf_registry_fallback_attempted", "value": "false"},
                        {"key": "typed_udf_registry_external_engine_invoked", "value": "false"},
                    ]
                elif args == [
                    "extension-registry",
                    "--manifest-dir",
                    "target/extensions",
                    "--format",
                    "json",
                ]:
                    command = "extension-registry"
                    fields = [
                        {"key": "extension_registry_input_kind", "value": "approved_local_manifest_directory"},
                        {"key": "extension_registry_manifest_count", "value": "2"},
                        {"key": "extension_registry_contract_complete_count", "value": "2"},
                        {"key": "extension_registry_runtime_execution", "value": "false"},
                        {"key": "extension_registry_extension_code_executed", "value": "false"},
                        {"key": "extension_registry_external_effect_executed", "value": "false"},
                        {"key": "extension_registry_fallback_attempted", "value": "false"},
                        {"key": "extension_registry_external_engine_invoked", "value": "false"},
                    ]
                elif args == ["extension-inspect", "example.fixture", "--format", "json"]:
                    command = "extension-inspect"
                    fields = [
                        {"key": "extension_code_executed", "value": "false"},
                        {"key": "fallback_attempted", "value": "false"},
                        {"key": "external_engine_invoked", "value": "false"},
                    ]
                elif args == [
                    "extension-inspect",
                    "--manifest",
                    "target/extension.json",
                    "--format",
                    "json",
                ]:
                    command = "extension-inspect"
                    fields = [
                        {"key": "extension_manifest_input_kind", "value": "local_manifest_file"},
                        {"key": "extension_manifest_inspection_status", "value": "validated"},
                        {"key": "extension_manifest_extension_code_executed", "value": "false"},
                        {"key": "extension_manifest_external_effect_executed", "value": "false"},
                        {"key": "extension_manifest_fallback_attempted", "value": "false"},
                        {"key": "extension_manifest_external_engine_invoked", "value": "false"},
                    ]
                elif args == ["udf-runtime-plan", "fixture", "--format", "json"]:
                    command = "udf-runtime-plan"
                    fields = [
                        {"key": "udf_runtime_kind", "value": "builtin_deterministic_fixture"},
                        {"key": "udf_execution_performed", "value": "false"},
                        {"key": "fallback_attempted", "value": "false"},
                        {"key": "external_engine_invoked", "value": "false"},
                    ]
                elif args == ["udf-local-scalar-fixture-smoke", "3,null,-4", "--format", "json"]:
                    command = "udf-local-scalar-fixture-smoke"
                    fields = [
                        {"key": "output_values", "value": "6,null,-8"},
                        {"key": "udf_execution_performed", "value": "true"},
                        {"key": "fallback_attempted", "value": "false"},
                        {"key": "external_engine_invoked", "value": "false"},
                    ]
                elif args == [
                    "embedding-vector-local-fixture-smoke",
                    "alpha;beta;gamma",
                    "--query",
                    "beta",
                    "--format",
                    "json",
                ]:
                    command = "embedding-vector-local-fixture-smoke"
                    fields = [
                        {"key": "schema_version", "value": "shardloom.deterministic_embedding_vector_fixture.v1"},
                        {"key": "fixture_id", "value": "sl_fixture_hash_embedding_vector"},
                        {"key": "embedding_model_id", "value": "sl_fixture_hash_embedding_v1"},
                        {"key": "vector_index_kind", "value": "local_bruteforce_l2_fixture"},
                        {"key": "vector_metric", "value": "squared_l2"},
                        {"key": "vector_dimension", "value": "4"},
                        {"key": "nearest_index", "value": "1"},
                        {"key": "nearest_text", "value": "beta"},
                        {"key": "nearest_distance_squared", "value": "0"},
                        {"key": "model_call_performed", "value": "false"},
                        {"key": "credential_resolution_performed", "value": "false"},
                        {"key": "network_probe_performed", "value": "false"},
                        {"key": "external_effect_executed", "value": "false"},
                        {"key": "fallback_attempted", "value": "false"},
                        {"key": "external_engine_invoked", "value": "false"},
                    ]
                elif args == [
                    "sqlite-local-import-export-smoke",
                    "target/orders.sqlite",
                    "--table",
                    "orders",
                    "--export-jsonl",
                    "target/orders.jsonl",
                    "--roundtrip-db",
                    "target/orders-roundtrip.sqlite",
                    "--order-by",
                    "id",
                    "--allow-overwrite",
                    "--format",
                    "json",
                ]:
                    command = "sqlite-local-import-export-smoke"
                    fields = [{"key": "roundtrip_replay_verified", "value": "true"}]
                else:
                    raise AssertionError(sys.argv)

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

        self.assertEqual(client.extension_registry().command, "extension-registry")
        udf_registry = client.udf_registry()
        self.assertEqual(udf_registry.command, "udf-registry")
        self.assertEqual(
            udf_registry.field("typed_udf_registry_schema_version"),
            "shardloom.typed_udf_registry.v1",
        )
        self.assertEqual(
            udf_registry.field_int("typed_udf_registry_admitted_local_fixture_count"),
            1,
        )
        self.assertFalse(
            udf_registry.field_bool("typed_udf_registry_arbitrary_runtime_bridge_available")
        )
        self.assertFalse(udf_registry.field_bool("typed_udf_registry_network_access_allowed"))
        self.assertFalse(udf_registry.field_bool("typed_udf_registry_fallback_attempted"))
        self.assertFalse(udf_registry.field_bool("typed_udf_registry_external_engine_invoked"))
        registry_dir = client.extension_registry(manifest_dir="target/extensions")
        self.assertEqual(registry_dir.command, "extension-registry")
        self.assertEqual(
            registry_dir.field("extension_registry_input_kind"),
            "approved_local_manifest_directory",
        )
        self.assertEqual(registry_dir.field_int("extension_registry_manifest_count"), 2)
        self.assertFalse(registry_dir.field_bool("extension_registry_runtime_execution"))
        self.assertEqual(
            client.extension_inspect("example.fixture").field("extension_code_executed"),
            "false",
        )
        manifest_inspected = client.extension_inspect(
            manifest_path="target/extension.json"
        )
        self.assertEqual(manifest_inspected.command, "extension-inspect")
        self.assertEqual(
            manifest_inspected.field("extension_manifest_input_kind"),
            "local_manifest_file",
        )
        self.assertFalse(
            manifest_inspected.field_bool("extension_manifest_extension_code_executed")
        )
        self.assertEqual(
            client.udf_runtime_plan("fixture").field("udf_runtime_kind"),
            "builtin_deterministic_fixture",
        )
        self.assertEqual(
            client.udf_local_scalar_fixture_smoke([3, None, -4]).field("output_values"),
            "6,null,-8",
        )
        embedding = client.embedding_vector_local_fixture_smoke(
            ["alpha", "beta", "gamma"],
            query="beta",
        )
        self.assertEqual(embedding.command, "embedding-vector-local-fixture-smoke")
        self.assertEqual(
            embedding.field("schema_version"),
            "shardloom.deterministic_embedding_vector_fixture.v1",
        )
        self.assertEqual(embedding.field("nearest_text"), "beta")
        self.assertEqual(embedding.field_int("nearest_distance_squared"), 0)
        self.assertFalse(embedding.field_bool("model_call_performed"))
        self.assertFalse(embedding.field_bool("network_probe_performed"))
        sqlite = client.sqlite_local_import_export_smoke(
            "target/orders.sqlite",
            table="orders",
            export_jsonl="target/orders.jsonl",
            roundtrip_db="target/orders-roundtrip.sqlite",
            order_by="id",
            allow_overwrite=True,
        )
        self.assertEqual(sqlite.command, "sqlite-local-import-export-smoke")
        self.assertTrue(sqlite.field_bool("roundtrip_replay_verified"))

        ctx = ShardLoomContext(client=client)
        self.assertEqual(ctx.extension_registry().command, "extension-registry")
        ctx_udf_registry = ctx.udf_registry()
        self.assertEqual(ctx_udf_registry.command, "udf-registry")
        self.assertFalse(
            ctx_udf_registry.field_bool("typed_udf_registry_external_engine_invoked")
        )
        ctx_registry_dir = ctx.extension_registry(manifest_dir="target/extensions")
        self.assertEqual(ctx_registry_dir.command, "extension-registry")
        self.assertFalse(
            ctx_registry_dir.field_bool("extension_registry_extension_code_executed")
        )
        inspected = ctx.extension_inspect("example.fixture")
        self.assertEqual(inspected.command, "extension-inspect")
        self.assertFalse(inspected.field_bool("extension_code_executed"))
        ctx_manifest = ctx.extension_inspect(manifest_path="target/extension.json")
        self.assertEqual(ctx_manifest.command, "extension-inspect")
        self.assertEqual(
            ctx_manifest.field("extension_manifest_inspection_status"),
            "validated",
        )
        self.assertFalse(
            ctx_manifest.field_bool("extension_manifest_external_effect_executed")
        )
        udf_plan = ctx.udf_runtime_plan("fixture")
        self.assertEqual(udf_plan.field("udf_runtime_kind"), "builtin_deterministic_fixture")
        self.assertFalse(udf_plan.field_bool("udf_execution_performed"))
        udf_smoke = ctx.udf_local_scalar_fixture_smoke([3, None, -4])
        self.assertEqual(udf_smoke.field("output_values"), "6,null,-8")
        self.assertTrue(udf_smoke.field_bool("udf_execution_performed"))
        embedding_smoke = ctx.embedding_vector_local_fixture_smoke(
            ("alpha", "beta", "gamma"),
            query="beta",
        )
        self.assertEqual(embedding_smoke.field("nearest_text"), "beta")
        self.assertFalse(embedding_smoke.field_bool("external_effect_executed"))
        for envelope in (inspected, udf_plan, udf_smoke, embedding_smoke):
            self.assertFalse(envelope.field_bool("fallback_attempted"))
            self.assertFalse(envelope.field_bool("external_engine_invoked"))
        for envelope in (manifest_inspected, ctx_manifest):
            self.assertFalse(envelope.field_bool("extension_manifest_fallback_attempted"))
            self.assertFalse(envelope.field_bool("extension_manifest_external_engine_invoked"))
        for envelope in (registry_dir, ctx_registry_dir):
            self.assertFalse(envelope.field_bool("extension_registry_fallback_attempted"))
            self.assertFalse(envelope.field_bool("extension_registry_external_engine_invoked"))

    def test_plan_import_and_export_helpers_expose_substrait_contract(self) -> None:
        binary = self.fake_cli(
            textwrap.dedent(
                """
                import json, sys

                args = sys.argv[1:]
                if args == ["plan-import", "substrait", "fixture", "--format", "json"]:
                    command = "plan-import"
                    direction = "import"
                elif args == ["plan-export", "substrait", "--format", "json"]:
                    command = "plan-export"
                    direction = "export"
                else:
                    raise AssertionError(sys.argv)

                print(json.dumps({
                    "schema_version": "shardloom.output.v2",
                    "command": command,
                    "status": "unsupported",
                    "summary": "substrait report only",
                    "human_text": "substrait report only",
                    "fallback": {"attempted": False, "allowed": False, "engine": None, "reason": "disabled"},
                    "diagnostics": [],
                    "fields": [
                        {"key": "substrait_report_contract_schema_version", "value": "shardloom.substrait_report_only_contract.v1"},
                        {"key": "substrait_report_contract_direction", "value": direction},
                        {"key": "substrait_report_contract_support_status", "value": "report_only"},
                        {"key": "substrait_import_parser_status", "value": "not_implemented"},
                        {"key": "substrait_export_serializer_status", "value": "not_implemented"},
                        {"key": "substrait_dependency_status", "value": "not_added"},
                        {"key": "substrait_imported_plan_execution_allowed", "value": "false"},
                        {"key": "substrait_external_engine_invoked", "value": "false"},
                        {"key": "substrait_fallback_attempted", "value": "false"},
                        {"key": "substrait_claim_gate_status", "value": "not_claim_grade"}
                    ],
                }))
                """
            )
        )
        client = ShardLoomClient(binary=binary)

        imported = client.plan_import("substrait", "fixture", check=False)
        exported = client.plan_export("substrait", check=False)

        self.assertEqual(imported.field("substrait_report_contract_direction"), "import")
        self.assertEqual(exported.field("substrait_report_contract_direction"), "export")
        self.assertEqual(
            imported.field("substrait_report_contract_support_status"),
            "report_only",
        )
        self.assertFalse(imported.field_bool("substrait_imported_plan_execution_allowed"))
        self.assertFalse(exported.field_bool("substrait_external_engine_invoked"))
        self.assertFalse(exported.field_bool("substrait_fallback_attempted"))

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
