use std::process::Command;

mod support;

use support::{assert_contains, field};

struct EnvelopeCase<'a> {
    name: &'a str,
    args: &'a [&'a str],
    command: &'a str,
    status: &'a str,
    family: &'a str,
    success: bool,
    allow_stderr: bool,
    fields: &'a [(&'a str, &'a str)],
    fragments: &'a [&'a str],
}

fn assert_field(output: &str, key: &str, value: &str, case_name: &str) {
    assert_contains(output, &field(key, value), case_name);
}

fn assert_common_typed_slots(output: &str, case: &EnvelopeCase<'_>) {
    assert_contains(
        output,
        "\"schema_version\":\"shardloom.output.v2\"",
        case.name,
    );
    assert_contains(
        output,
        &format!("\"command\":\"{}\"", case.command),
        case.name,
    );
    assert_contains(
        output,
        &format!("\"status\":\"{}\"", case.status),
        case.name,
    );
    assert_contains(
        output,
        "\"fallback\":{\"attempted\":false,\"allowed\":false",
        case.name,
    );
    assert_contains(output, "\"diagnostics\":[", case.name);
    assert_contains(output, "\"result\":{\"fields\":[", case.name);
    assert_contains(output, "\"result_refs\":[", case.name);
    assert_contains(output, "\"artifacts\":[", case.name);
    assert_contains(output, "\"artifact_refs\":[", case.name);
    assert_contains(output, "\"certificates\":[", case.name);
    assert_contains(output, "\"policy\":{\"fields\":[", case.name);
    assert_contains(output, "\"lifecycle\":{\"fields\":[", case.name);
    assert_contains(output, "\"capability_snapshot\":{\"fields\":[", case.name);
    assert_contains(output, "\"fields\":[", case.name);
    assert_field(output, "command_family", case.family, case.name);
}

fn run_case(case: &EnvelopeCase<'_>) {
    let output = Command::new(env!("CARGO_BIN_EXE_shardloom"))
        .args(case.args)
        .output()
        .expect("shardloom command runs");
    let stdout = String::from_utf8(output.stdout).expect("stdout is utf8");
    let stderr = String::from_utf8(output.stderr).expect("stderr is utf8");

    assert_eq!(
        output.status.success(),
        case.success,
        "{}: stdout={stdout} stderr={stderr}",
        case.name
    );
    if !case.allow_stderr {
        assert!(stderr.is_empty(), "{}: stderr={stderr}", case.name);
    }

    assert_common_typed_slots(&stdout, case);
    for (key, value) in case.fields {
        assert_field(&stdout, key, value, case.name);
    }
    for fragment in case.fragments {
        assert_contains(&stdout, fragment, case.name);
    }
}

#[cfg(feature = "vortex-traditional-analytics-benchmark")]
fn extract_json_field_value(output: &str, key: &str) -> String {
    let needle = format!("{{\"key\":\"{key}\",\"value\":\"");
    let start = output
        .find(&needle)
        .unwrap_or_else(|| panic!("missing field {key} in output: {output}"))
        + needle.len();
    let rest = &output[start..];
    let end = rest
        .find("\"}")
        .unwrap_or_else(|| panic!("unterminated field {key} in output: {output}"));
    rest[..end].to_string()
}

#[test]
#[allow(clippy::too_many_lines)]
fn representative_cli_json_paths_keep_typed_envelope_contract() {
    let cases = [
        EnvelopeCase {
            name: "status success",
            args: &["status", "--format", "json"],
            command: "status",
            status: "success",
            family: "status_capabilities",
            success: true,
            allow_stderr: false,
            fields: &[
                ("protocol_version", "shardloom.output.v2"),
                ("fallback_execution_allowed", "false"),
            ],
            fragments: &[],
        },
        EnvelopeCase {
            name: "runs-today success",
            args: &["runs-today", "--format", "json"],
            command: "runs-today",
            status: "success",
            family: "status_capabilities",
            success: true,
            allow_stderr: false,
            fields: &[
                (
                    "runs_today_schema_version",
                    "shardloom.runs_today_support_matrix.v1",
                ),
                (
                    "runs_today_support_state_vocabulary",
                    "executable,feature_gated,diagnostic_only,report_only,blocked,future",
                ),
                ("runs_today_all_rows_no_fallback_no_external_engine", "true"),
            ],
            fragments: &[],
        },
        EnvelopeCase {
            name: "cross-CG capability parity",
            args: &["capabilities", "cross-cg", "--format", "json"],
            command: "capabilities",
            status: "success",
            family: "status_capabilities",
            success: true,
            allow_stderr: false,
            fields: &[
                ("schema_version", "shardloom.cross_cg_capability_parity.v1"),
                ("capability_status", "report_only"),
                ("represented_gates", "cg21,cg22,cg23"),
                ("cg21_workflow_no_runtime", "true"),
                ("cg22_engine_modes_no_fallback", "true"),
                ("cg23_remote_api_no_effects", "true"),
            ],
            fragments: &[],
        },
        EnvelopeCase {
            name: "workflow unsupported preview",
            args: &[
                "workflow-unsupported-plan",
                "preview",
                "read_vortex(orders.vortex)",
                "20",
                "--format",
                "json",
            ],
            command: "workflow-unsupported-plan",
            status: "unsupported",
            family: "workflow_planning",
            success: false,
            allow_stderr: false,
            fields: &[
                ("schema_version", "shardloom.workflow_unsupported.v1"),
                ("workflow_operation", "preview"),
                (
                    "blocker_id",
                    "cg21.workflow.preview.materialization_unsupported",
                ),
                ("target_ref", "20"),
                ("no_runtime", "true"),
                ("no_fallback", "true"),
                ("no_effects", "true"),
            ],
            fragments: &[
                "\"code\":\"SL_MATERIALIZATION_REQUIRED\"",
                "\"category\":\"materialization\"",
            ],
        },
        EnvelopeCase {
            name: "api compatibility lock",
            args: &["api-compat-plan", "--format", "json"],
            command: "api-compat-plan",
            status: "success",
            family: "rest_api_planning",
            success: true,
            allow_stderr: false,
            fields: &[
                ("compatibility_lock_status", "locked"),
                ("json_error_paths_enveloped", "true"),
                ("unknown_command_json_enveloped", "true"),
                ("missing_binary_error_payload_shaped", "true"),
            ],
            fragments: &[],
        },
        EnvelopeCase {
            name: "rest api contract plan",
            args: &["rest-api-contract-plan", "--format", "json"],
            command: "rest-api-contract-plan",
            status: "success",
            family: "rest_api_planning",
            success: true,
            allow_stderr: false,
            fields: &[
                ("mode", "rest_api_contract_plan"),
                ("openapi_version", "3.2.0"),
                (
                    "rest_api_surface_parity_surface_id",
                    "rest_api_contract_plan",
                ),
                ("rest_api_cli_python_field_parity", "true"),
                ("rest_api_runtime_equivalent_api_claim_allowed", "false"),
                ("rest_api_no_fallback_no_external_engine", "true"),
                ("fallback_attempted", "false"),
                ("network_listener_opened", "false"),
            ],
            fragments: &[],
        },
        EnvelopeCase {
            name: "serve discovery contract",
            args: &[
                "serve",
                "--mode",
                "discovery",
                "--bind",
                "127.0.0.1:8787",
                "--format",
                "json",
            ],
            command: "serve",
            status: "success",
            family: "rest_api_planning",
            success: true,
            allow_stderr: false,
            fields: &[
                ("mode", "rest_api_discovery_mode"),
                ("server_mode", "discovery"),
                (
                    "rest_api_surface_parity_surface_id",
                    "rest_api_discovery_mode",
                ),
                ("rest_api_cli_python_field_parity", "true"),
                ("rest_api_runtime_equivalent_api_claim_allowed", "false"),
                ("rest_api_no_fallback_no_external_engine", "true"),
                ("server_started", "false"),
                ("fallback_attempted", "false"),
            ],
            fragments: &[],
        },
        EnvelopeCase {
            name: "rest api plan preview unsupported",
            args: &[
                "rest-api-plan-preview",
                "unsupported-operator",
                "--format",
                "json",
            ],
            command: "rest-api-plan-preview",
            status: "unsupported",
            family: "rest_api_planning",
            success: true,
            allow_stderr: false,
            fields: &[
                ("mode", "rest_api_plan_preview"),
                ("preview_status", "unsupported"),
                (
                    "rest_api_surface_parity_surface_id",
                    "rest_api_plan_preview",
                ),
                ("rest_api_cli_python_field_parity", "true"),
                ("rest_api_runtime_equivalent_api_claim_allowed", "false"),
                ("rest_api_no_fallback_no_external_engine", "true"),
                ("native_logical_stage_status", "unsupported"),
                ("problem_details_diagnostic_code", "SL_UNSUPPORTED_SQL"),
                ("runtime_execution", "false"),
                ("execution_delegated", "false"),
                ("fallback_attempted", "false"),
            ],
            fragments: &["\"code\":\"SL_UNSUPPORTED_SQL\""],
        },
        EnvelopeCase {
            name: "rest api local lifecycle certified",
            args: &[
                "rest-api-local-lifecycle",
                "certified-local-batch",
                "--format",
                "json",
            ],
            command: "rest-api-local-lifecycle",
            status: "success",
            family: "rest_api_planning",
            success: true,
            allow_stderr: false,
            fields: &[
                ("mode", "rest_api_local_lifecycle"),
                ("lifecycle_status", "succeeded"),
                (
                    "rest_api_surface_parity_surface_id",
                    "rest_api_local_lifecycle",
                ),
                ("rest_api_cli_python_field_parity", "true"),
                ("rest_api_runtime_equivalent_api_claim_allowed", "false"),
                ("rest_api_no_fallback_no_external_engine", "true"),
                ("runtime_execution", "true"),
                ("local_execution_performed", "true"),
                ("data_read", "false"),
                ("fallback_attempted", "false"),
            ],
            fragments: &[
                "\"result_refs\":[{\"id\":\"result://cg23/certified-local-batch/0001\"",
                "\"kind\":\"execution_certificate\"",
                "\"kind\":\"native_io_certificate\"",
            ],
        },
        EnvelopeCase {
            name: "rest api event stream certified live",
            args: &[
                "rest-api-event-stream",
                "certified-live-fixture",
                "--format",
                "json",
            ],
            command: "rest-api-event-stream",
            status: "success",
            family: "rest_api_planning",
            success: true,
            allow_stderr: false,
            fields: &[
                ("mode", "rest_api_event_stream"),
                ("event_stream_status", "certified_fixture"),
                (
                    "rest_api_surface_parity_surface_id",
                    "rest_api_event_stream",
                ),
                ("rest_api_cli_python_field_parity", "true"),
                ("rest_api_runtime_equivalent_api_claim_allowed", "false"),
                ("rest_api_no_fallback_no_external_engine", "true"),
                ("sse_first", "true"),
                ("asyncapi_version", "3.0.0"),
                ("workload_certified", "true"),
                ("production_claim_allowed", "false"),
                ("broker_io", "false"),
                ("object_store_io", "false"),
                ("fallback_attempted", "false"),
            ],
            fragments: &[
                "\"kind\":\"execution_certificate\"",
                "\"kind\":\"native_io_certificate\"",
            ],
        },
        EnvelopeCase {
            name: "rest api security governance safe default",
            args: &[
                "rest-api-security-governance",
                "safe-local-default",
                "--format",
                "json",
            ],
            command: "rest-api-security-governance",
            status: "success",
            family: "rest_api_planning",
            success: true,
            allow_stderr: false,
            fields: &[
                ("mode", "rest_api_security_governance"),
                ("governance_status", "available_contract"),
                (
                    "rest_api_surface_parity_surface_id",
                    "rest_api_security_governance",
                ),
                ("rest_api_cli_python_field_parity", "true"),
                ("rest_api_runtime_equivalent_api_claim_allowed", "false"),
                ("rest_api_no_fallback_no_external_engine", "true"),
                ("credential_references_only", "true"),
                ("raw_secret_values_present", "false"),
                ("secrets_redacted", "true"),
                ("mcp_dry_run_default", "true"),
                ("mcp_effectful_tools_allowed", "false"),
                ("opentelemetry_exporter_enabled", "false"),
                ("runtime_execution", "false"),
                ("fallback_attempted", "false"),
            ],
            fragments: &[],
        },
        EnvelopeCase {
            name: "rest api data plane standards matrix",
            args: &[
                "rest-api-data-plane",
                "standards-matrix",
                "--format",
                "json",
            ],
            command: "rest-api-data-plane",
            status: "success",
            family: "rest_api_planning",
            success: true,
            allow_stderr: false,
            fields: &[
                ("mode", "rest_api_data_plane"),
                ("data_plane_status", "standards_matrix_available"),
                ("rest_api_surface_parity_surface_id", "rest_api_data_plane"),
                ("rest_api_cli_python_field_parity", "true"),
                ("rest_api_runtime_equivalent_api_claim_allowed", "false"),
                ("rest_api_no_fallback_no_external_engine", "true"),
                ("rest_control_plane_sufficient_for_local_use", "true"),
                ("flight_adbc_required_for_basic_local_use", "false"),
                ("standards_matrix_count", "11"),
                ("decoded_columnar_boundary_declared", "true"),
                ("flight_server_started", "false"),
                ("adbc_endpoint_opened", "false"),
                ("broker_io", "false"),
                ("fallback_attempted", "false"),
            ],
            fragments: &["iceberg_rest_catalog,polaris,gravitino"],
        },
        EnvelopeCase {
            name: "input planning success",
            args: &["input-adapters", "--format", "json"],
            command: "input-adapters",
            status: "success",
            family: "input_planning",
            success: true,
            allow_stderr: false,
            fields: &[("plan_only", "true")],
            fragments: &[],
        },
        EnvelopeCase {
            name: "workflow planning success",
            args: &["layout-health-plan", "healthy", "--format", "json"],
            command: "layout-health-plan",
            status: "success",
            family: "workflow_planning",
            success: true,
            allow_stderr: false,
            fields: &[("layout_health_status", "healthy")],
            fragments: &[],
        },
        EnvelopeCase {
            name: "object store planning success",
            args: &["object-store-request-plan", "ready", "--format", "json"],
            command: "object-store-request-plan",
            status: "success",
            family: "object_store_planning",
            success: true,
            allow_stderr: false,
            fields: &[("object_store_request_status", "planned")],
            fragments: &[],
        },
        EnvelopeCase {
            name: "object store runtime blocker diagnostics",
            args: &["cg10-object-store-runtime-gate", "--format", "json"],
            command: "cg10-object-store-runtime-gate",
            status: "success",
            family: "object_store_planning",
            success: true,
            allow_stderr: false,
            fields: &[
                ("runtime_blocker_matrix_diagnostics_propagated", "true"),
                ("runtime_blocker_matrix_diagnostic_count", "10"),
                ("runtime_blocker_matrix_envelope_status", "success"),
                ("fallback_attempted", "false"),
                ("external_engine_invoked", "false"),
            ],
            fragments: &[
                "\"code\":\"SL_OBJECT_STORE_UNSUPPORTED\"",
                "\"severity\":\"info\"",
                "\"category\":\"object_store\"",
                "\"feature\":\"coordinator_start\"",
                "\"feature\":\"commit_record_write\"",
                "\"feature\":\"partition_discovery\"",
                "\"feature\":\"catalog_integration\"",
                "\"feature\":\"remote_result_delivery\"",
            ],
        },
        EnvelopeCase {
            name: "optimizer report-only trace",
            args: &["optimizer-plan", "--format", "json"],
            command: "optimizer-plan",
            status: "success",
            family: "optimizer_planning",
            success: true,
            allow_stderr: false,
            fields: &[
                ("mode", "evidence_aware_optimizer_trace"),
                ("gar_id", "GAR-PERF-2B"),
                ("claim_gate_status", "not_claim_grade"),
                ("optimizer_rule_applied_count", "0"),
                ("fallback_attempted", "false"),
                ("external_engine_invoked", "false"),
                ("all_no_fallback_no_external_engine", "true"),
            ],
            fragments: &[
                "\"optimizer_trace_id\"",
                "\"optimizer_rule_common_subplan_source_state_reuse_status\"",
            ],
        },
        EnvelopeCase {
            name: "session cache runtime smoke",
            args: &["session-cache-smoke", "--format", "json"],
            command: "session-cache-smoke",
            status: "success",
            family: "engine_runtime_planning",
            success: true,
            allow_stderr: false,
            fields: &[
                ("mode", "session_cache_smoke"),
                (
                    "session_runtime_status",
                    "scoped_session_cache_runtime_certified",
                ),
                ("cache_hit_count", "5"),
                ("cache_miss_count", "8"),
                ("invalidation_count", "3"),
                ("buffer_reuse_count", "1"),
                ("explicit_close_performed", "true"),
                ("cleanup_performed", "true"),
                ("runtime_execution", "true"),
                ("fallback_attempted", "false"),
                ("external_engine_invoked", "false"),
                ("no_fallback_no_external_engine", "true"),
            ],
            fragments: &[
                "\"optimizer_trace_id\"",
                "\"source_state_id\"",
                "\"vortex_prepared_state_id\"",
                "\"output_plan_id\"",
            ],
        },
        EnvelopeCase {
            name: "engine runtime success",
            args: &[
                "streaming-plan",
                "file://tmp/data.vortex",
                "file://tmp/out.vortex",
                "--format",
                "json",
            ],
            command: "streaming-plan",
            status: "success",
            family: "engine_runtime_planning",
            success: true,
            allow_stderr: false,
            fields: &[("runtime_execution", "false")],
            fragments: &["\"artifact_kind\":\"materialization_boundary_report\""],
        },
        EnvelopeCase {
            name: "engine fabric selection success",
            args: &["engine-selection-plan", "--format", "json"],
            command: "engine-selection-plan",
            status: "success",
            family: "engine_runtime_planning",
            success: true,
            allow_stderr: false,
            fields: &[
                ("requested_engine_mode", "auto"),
                ("selected_engine_mode", "batch"),
                ("external_engine_invoked", "false"),
            ],
            fragments: &["\"report_id\",\"value\":\"cg22.engine_selection\""],
        },
        EnvelopeCase {
            name: "live fixture runtime success",
            args: &[
                "live-fixture-run",
                "group-count",
                "metric",
                "--format",
                "json",
            ],
            command: "live-fixture-run",
            status: "success",
            family: "engine_runtime_planning",
            success: true,
            allow_stderr: false,
            fields: &[
                ("fixture_operator", "group_count"),
                ("runtime_execution", "true"),
                ("data_read", "false"),
                ("write_io", "false"),
                ("fallback_attempted", "false"),
            ],
            fragments: &[
                "\"id\":\"cg22.live.fixture.group_count.execution\",\"kind\":\"execution_certificate\"",
                "\"id\":\"cg22.live.fixture.group_count.native_io\",\"kind\":\"native_io_certificate\"",
            ],
        },
        EnvelopeCase {
            name: "hybrid overlay runtime success",
            args: &[
                "hybrid-overlay-run",
                "group-count",
                "metric",
                "--format",
                "json",
            ],
            command: "hybrid-overlay-run",
            status: "success",
            family: "engine_runtime_planning",
            success: true,
            allow_stderr: false,
            fields: &[
                ("fixture_operator", "group_count"),
                ("delta_overlay_certificate_status", "certified"),
                ("micro_segment_flush_evidence_status", "certified"),
                ("layout_health_bundle_status", "compaction_recommended"),
                ("runtime_execution", "true"),
                ("data_read", "false"),
                ("write_io", "false"),
                ("fallback_attempted", "false"),
            ],
            fragments: &[
                "\"id\":\"cg22.hybrid.fixture.delta_overlay\",\"kind\":\"certificate\"",
                "\"id\":\"cg22.hybrid.fixture.group_count.execution\",\"kind\":\"execution_certificate\"",
                "\"id\":\"cg22.hybrid.fixture.group_count.native_io\",\"kind\":\"native_io_certificate\"",
            ],
        },
        EnvelopeCase {
            name: "distributed local fixture runtime success",
            args: &[
                "distributed-local-fixture-run",
                "2",
                "fault-injection",
                "--format",
                "json",
            ],
            command: "distributed-local-fixture-run",
            status: "success",
            family: "engine_runtime_planning",
            success: true,
            allow_stderr: false,
            fields: &[
                (
                    "distributed_runtime_status",
                    "scoped_local_fixture_supported",
                ),
                (
                    "distributed_claim_gate_status",
                    "not_distributed_runtime_grade",
                ),
                ("coordinator_invoked", "true"),
                ("remote_worker_invoked", "false"),
                ("split_execution_performed", "true"),
                ("deterministic_merge_performed", "true"),
                ("merged_rows", "east:3:13|north:2:10|west:2:9"),
                ("retry_performed", "true"),
                ("partial_output_committed", "false"),
                ("runtime_execution", "true"),
                ("fallback_attempted", "false"),
            ],
            fragments: &[
                "\"id\":\"prod-ready-1d.local_distributed_fixture.retry_duplicate_stale_lease.execution\",\"kind\":\"execution_certificate\"",
                "\"id\":\"prod-ready-1d.local_distributed_fixture.retry_duplicate_stale_lease.native_io\",\"kind\":\"native_io_certificate\"",
            ],
        },
        EnvelopeCase {
            name: "live hybrid durable checkpoint runtime success",
            args: &[
                "live-hybrid-durable-checkpoint-smoke",
                "target/typed-envelope-live-hybrid-checkpoint",
                "--format",
                "json",
            ],
            command: "live-hybrid-durable-checkpoint-smoke",
            status: "success",
            family: "engine_runtime_planning",
            success: true,
            allow_stderr: false,
            fields: &[
                (
                    "schema_version",
                    "shardloom.live_hybrid_durable_checkpoint_fixture.v1",
                ),
                ("checkpoint_store_kind", "local_filesystem_fixture_store"),
                ("durable_checkpoint_store_used", "true"),
                ("durable_checkpoint_write_performed", "true"),
                ("durable_checkpoint_restore_performed", "true"),
                ("durable_changelog_write_performed", "true"),
                ("state_match", "true"),
                ("write_io", "true"),
                ("object_store_io", "false"),
                ("exactly_once_claim_allowed", "false"),
                ("production_claim_allowed", "false"),
                ("runtime_execution", "true"),
                ("fallback_attempted", "false"),
            ],
            fragments: &[
                "\"id\":\"cg22.live_hybrid.fixture.durable_checkpoint.execution\",\"kind\":\"execution_certificate\"",
                "\"id\":\"cg22.live_hybrid.fixture.durable_checkpoint.native_io\",\"kind\":\"native_io_certificate\"",
            ],
        },
        EnvelopeCase {
            name: "vortex runtime success",
            args: &[
                "vortex-memory-plan",
                "file://tmp/data.vortex",
                "8",
                "--format",
                "json",
            ],
            command: "vortex-memory-plan",
            status: "success",
            family: "vortex_runtime_planning",
            success: true,
            allow_stderr: false,
            fields: &[("tasks_executed", "false")],
            fragments: &[],
        },
        EnvelopeCase {
            name: "vortex planning success",
            args: &[
                "vortex-count-readiness-plan",
                "metadata-footer",
                "file://tmp/in.vortex",
                "--feature-gate",
                "--query-primitive-ready",
                "--count-primitive",
                "--metadata-footer-ready",
                "--format",
                "json",
            ],
            command: "vortex-count-readiness-plan",
            status: "success",
            family: "vortex_planning",
            success: true,
            allow_stderr: false,
            fields: &[("count_ready", "true")],
            fragments: &[],
        },
        EnvelopeCase {
            name: "vortex output commit success",
            args: &[
                "vortex-output-payload-plan",
                "file://tmp/out.vortex",
                "/tmp/stage",
                "write-intent-ready,staged-output-ready,finalized-manifest-ready,payload-content-available,local-workspace,feature-gate-enabled",
                "--format",
                "json",
            ],
            command: "vortex-output-payload-plan",
            status: "success",
            family: "vortex_output_commit",
            success: true,
            allow_stderr: false,
            fields: &[("payload_content_available", "true")],
            fragments: &[],
        },
        EnvelopeCase {
            name: "extension planning success",
            args: &["extension-registry", "--format", "json"],
            command: "extension-registry",
            status: "success",
            family: "extension_planning",
            success: true,
            allow_stderr: false,
            fields: &[("extension_code_executed", "false")],
            fragments: &[],
        },
        EnvelopeCase {
            name: "invalid input error",
            args: &["capabilities", "unknown", "--format", "json"],
            command: "capabilities",
            status: "error",
            family: "status_capabilities",
            success: false,
            allow_stderr: false,
            fields: &[],
            fragments: &["\"code\":\"SL_INVALID_INPUT\""],
        },
        EnvelopeCase {
            name: "unknown command error",
            args: &["definitely-unknown", "--format", "json"],
            command: "cli",
            status: "error",
            family: "other",
            success: false,
            allow_stderr: true,
            fields: &[],
            fragments: &["\"code\":\"SL_INVALID_INPUT\""],
        },
        EnvelopeCase {
            name: "prepared source unsupported",
            args: &[
                "vortex-encoded-read-boundary",
                "file://fixture.vortex",
                "local-path-only",
                "--format",
                "json",
            ],
            command: "vortex-encoded-read-boundary",
            status: "unsupported",
            family: "prepared_source_backed_execution",
            success: false,
            allow_stderr: false,
            fields: &[("read_execution_allowed", "false")],
            fragments: &[],
        },
        EnvelopeCase {
            name: "vortex primitive unsupported",
            args: &["vortex-count", "file://fixture.vortex", "--format", "json"],
            command: "vortex-count",
            status: "unsupported",
            family: "vortex_primitive_execution",
            success: false,
            allow_stderr: false,
            fields: &[("execution", "metadata_only_or_not_performed")],
            fragments: &["\"code\":\"SL_NOT_IMPLEMENTED\""],
        },
        EnvelopeCase {
            name: "blocked evidence success",
            args: &["cg20-user-capability-gate", "--format", "json"],
            command: "cg20-user-capability-gate",
            status: "success",
            family: "evidence_certificates",
            success: true,
            allow_stderr: false,
            fields: &[("claim_blocked", "true")],
            fragments: &[],
        },
        EnvelopeCase {
            name: "workload certification dossier",
            args: &[
                "workload-certification-dossier",
                "local-vortex-count",
                "--format",
                "json",
            ],
            command: "workload-certification-dossier",
            status: "success",
            family: "evidence_certificates",
            success: true,
            allow_stderr: false,
            fields: &[
                (
                    "schema_version",
                    "shardloom.workload_certification_dossier.v1",
                ),
                ("overall_status", "partial"),
                ("cg16_execution_certificate_status", "certified"),
                ("no_fallback", "true"),
            ],
            fragments: &[],
        },
        EnvelopeCase {
            name: "claim gate closeout",
            args: &["claim-gate-closeout", "--format", "json"],
            command: "claim-gate-closeout",
            status: "success",
            family: "evidence_certificates",
            success: true,
            allow_stderr: false,
            fields: &[
                ("schema_version", "shardloom.claim_gate_closeout.v1"),
                ("claim_gate_status", "blocked_for_broad_claims"),
                ("release_readiness_status", "blocked_until_priority_8"),
                ("public_package_claim_allowed", "false"),
                ("no_fallback", "true"),
            ],
            fragments: &["\"artifact_kind\":\"claim_gate_closeout_report\""],
        },
        EnvelopeCase {
            name: "compute capability matrix",
            args: &["compute-capability-matrix", "--format", "json"],
            command: "compute-capability-matrix",
            status: "success",
            family: "status_capabilities",
            success: true,
            allow_stderr: false,
            fields: &[
                ("schema_version", "shardloom.compute_capability_matrix.v1"),
                ("matrix_status", "report_only"),
                ("compute_row_count", "15"),
                ("operator_family_count", "14"),
                (
                    "native_unsupported_coverage_status",
                    "complete_for_current_matrix",
                ),
                ("native_unsupported_coverage_row_count", "22"),
                ("unadmitted_compute_rows_missing_diagnostics_count", "0"),
                (
                    "operator_execution_class_vocabulary",
                    "encoded_native,residual_native,materialized_temporary,unsupported",
                ),
                (
                    "materialization_policy_schema_version",
                    "shardloom.materialization_policy.v1",
                ),
                (
                    "materialization_policy_row_materialized_temporary_operator_path_encoded_native_claim_allowed",
                    "false",
                ),
                (
                    "compute_row_prepared_encoded_filter_operator_execution_class",
                    "encoded_native",
                ),
                (
                    "compute_row_grouped_aggregate_operator_execution_class",
                    "residual_native",
                ),
                (
                    "compute_row_grouped_aggregate_operator_admission_status",
                    "residual_native_fixture_admitted",
                ),
                (
                    "compute_row_grouped_aggregate_operator_encoded_native_claim_allowed",
                    "false",
                ),
                ("production_certified_count", "0"),
                ("no_fallback", "true"),
            ],
            fragments: &[
                "\"artifact_kind\":\"compute_capability_matrix_report\"",
                "direct_compatibility_transient",
            ],
        },
        EnvelopeCase {
            name: "traditional analytics direct transient admission unsupported",
            args: &[
                "traditional-analytics-run",
                "hash join",
                "missing_fact.csv",
                "missing_dim.csv",
                "--input-format",
                "csv",
                "--execution-mode",
                "direct_compatibility_transient",
                "--format",
                "json",
            ],
            command: "traditional-analytics-run",
            status: "unsupported",
            family: "benchmarks",
            success: false,
            allow_stderr: false,
            fields: &[
                ("requested_execution_mode", "direct_compatibility_transient"),
                ("selected_execution_mode", "direct_compatibility_transient"),
                ("execution_mode_family", "compatibility"),
                ("mode_supported", "false"),
                ("support_status", "unsupported"),
                (
                    "unsupported_diagnostic_code",
                    "direct_compatibility_transient_not_implemented",
                ),
                ("blocker_id", "P7.5.4"),
                ("runtime_execution", "false"),
                ("query_execution", "false"),
                ("data_read", "false"),
                ("data_materialized", "false"),
                ("write_io", "false"),
                ("direct_transient_execution", "false"),
                ("vortex_native_claim_allowed", "false"),
                ("claim_gate_status", "not_claim_grade"),
                ("fallback_attempted", "false"),
                ("external_engine_invoked", "false"),
                ("no_runtime", "true"),
                ("no_fallback", "true"),
                ("no_effects", "true"),
                (
                    "unsupported_detail",
                    "direct transient smoke currently supports only selective filter or filter + projection + limit",
                ),
            ],
            fragments: &[
                "\"artifact_kind\":\"execution_mode_selection_report\"",
                "\"status\":\"unsupported\"",
                "SL_NOT_IMPLEMENTED",
            ],
        },
        EnvelopeCase {
            name: "semantic conformance suite",
            args: &["semantic-conformance-suite", "--format", "json"],
            command: "semantic-conformance-suite",
            status: "success",
            family: "status_capabilities",
            success: true,
            allow_stderr: false,
            fields: &[
                ("schema_version", "shardloom.semantic_conformance.v1"),
                ("semantic_profile", "ShardLoomNative"),
                ("suite_status", "partial_fixture_passed_planned_remaining"),
                ("executed_fixture_count", "23"),
                ("passed_fixture_count", "23"),
                ("failed_fixture_count", "0"),
                ("no_fallback", "true"),
            ],
            fragments: &["\"artifact_kind\":\"semantic_conformance_report\""],
        },
        EnvelopeCase {
            name: "evidence incomplete benchmark success",
            args: &["benchmark-plan", "foundation", "--format", "json"],
            command: "benchmark-plan",
            status: "success",
            family: "benchmarks",
            success: true,
            allow_stderr: false,
            fields: &[("claim_gate_status", "evidence_missing")],
            fragments: &["\"artifact_kind\":\"benchmark_plan_report\""],
        },
        EnvelopeCase {
            name: "benchmark constitution validator success",
            args: &["benchmark-constitution", "foundation", "--format", "json"],
            command: "benchmark-constitution",
            status: "success",
            family: "benchmarks",
            success: true,
            allow_stderr: false,
            fields: &[("benchmark_constitution_status", "missing_evidence")],
            fragments: &["\"artifact_kind\":\"benchmark_constitution_report\""],
        },
        EnvelopeCase {
            name: "foundry optional harness success",
            args: &["universal-harness-plan", "--format", "json"],
            command: "universal-harness-plan",
            status: "success",
            family: "evidence_certificates",
            success: true,
            allow_stderr: false,
            fields: &[
                ("universal_harness_status", "evidence_incomplete"),
                (
                    "universal_harness_execution_gate_status",
                    "blocked_missing_evidence",
                ),
                ("universal_harness_execution_allowed", "false"),
                ("universal_harness_execution_attempted", "false"),
                ("foundry_required", "false"),
                ("foundry_optional_example", "true"),
                ("foundry_optional_harness_required", "true"),
                ("external_baseline_execution", "false"),
                ("runtime_execution", "false"),
                ("fallback_attempted", "false"),
            ],
            fragments: &["\"artifact_kind\":\"universal_harness_report\""],
        },
    ];

    for case in cases {
        run_case(&case);
    }
}

#[test]
#[cfg(feature = "vortex-traditional-analytics-benchmark")]
#[allow(clippy::too_many_lines)]
fn traditional_analytics_cli_reuse_preserves_optional_text_columns_for_prepared_vortex() {
    let root = std::env::temp_dir().join(format!(
        "shardloom-cli-prepared-reuse-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("time after epoch")
            .as_nanos()
    ));
    std::fs::create_dir_all(&root).expect("temp root");
    let fact_csv = root.join("fact.csv");
    let dim_csv = root.join("dim.csv");
    std::fs::write(
        &fact_csv,
        "id,group_key,dim_key,value,metric,flag,category,event_date,nullable_metric_00,raw_event_time,dirty_numeric,dirty_flag\n1,10,1,6000,2.5,1,A,2024-03-01,1.25,2024-01-01T00:00:00Z,6000,Y\n2,11,2,1000,3.5,0,B,2024-07-01,,not-a-timestamp,bad-number,N\n3,10,1,8000,4.0,1,A,2024-05-01,3.75,2024-01-03T00:00:00Z,8000,Y\n",
    )
    .expect("fact csv");
    std::fs::write(&dim_csv, "dim_key,dim_label,weight\n1,one,1.5\n2,two,2.0\n").expect("dim csv");
    let fact_arg = fact_csv.display().to_string();
    let dim_arg = dim_csv.display().to_string();
    let workspace_arg = root.join("workspace").display().to_string();

    let import_output = Command::new(env!("CARGO_BIN_EXE_shardloom"))
        .args([
            "traditional-analytics-run",
            "csv/file ingest",
            &fact_arg,
            &dim_arg,
            "--workspace",
            &workspace_arg,
            "--input-format",
            "csv",
            "--execution-mode",
            "compatibility_import_certified",
            "--preserve-all-text-columns-for-reuse",
            "--format",
            "json",
        ])
        .output()
        .expect("shardloom import command runs");
    let import_stdout = String::from_utf8(import_output.stdout).expect("stdout is utf8");
    let import_stderr = String::from_utf8(import_output.stderr).expect("stderr is utf8");
    let import_case = EnvelopeCase {
        name: "traditional analytics CLI prepared reuse import",
        args: &[],
        command: "traditional-analytics-run",
        status: "success",
        family: "benchmarks",
        success: true,
        allow_stderr: false,
        fields: &[],
        fragments: &[],
    };
    assert!(
        import_output.status.success(),
        "{}: stdout={import_stdout} stderr={import_stderr}",
        import_case.name
    );
    assert!(
        import_stderr.is_empty(),
        "{}: stderr={import_stderr}",
        import_case.name
    );
    assert_common_typed_slots(&import_stdout, &import_case);
    assert_field(
        &import_stdout,
        "fallback_attempted",
        "false",
        import_case.name,
    );
    assert_field(
        &import_stdout,
        "external_engine_invoked",
        "false",
        import_case.name,
    );

    let fact_vortex = extract_json_field_value(&import_stdout, "fact_vortex_path");
    let dim_vortex = extract_json_field_value(&import_stdout, "dim_vortex_path");
    for (scenario, expected_result) in [
        (
            "partition pruning",
            "{\\\"row_count\\\":2,\\\"metric_sum\\\":6.5}",
        ),
        (
            "null-heavy aggregate",
            "{\\\"row_count\\\":2,\\\"metric_sum\\\":5.0}",
        ),
        (
            "clean/cast/filter/write",
            "{\\\"row_count\\\":2,\\\"metric_sum\\\":14000.0}",
        ),
    ] {
        let output = Command::new(env!("CARGO_BIN_EXE_shardloom"))
            .args([
                "traditional-analytics-vortex-run",
                scenario,
                &fact_vortex,
                &dim_vortex,
                "--execution-mode",
                "prepared_vortex",
                "--format",
                "json",
            ])
            .output()
            .expect("shardloom prepared vortex command runs");
        let stdout = String::from_utf8(output.stdout).expect("stdout is utf8");
        let stderr = String::from_utf8(output.stderr).expect("stderr is utf8");
        let case = EnvelopeCase {
            name: scenario,
            args: &[],
            command: "traditional-analytics-vortex-run",
            status: "success",
            family: "benchmarks",
            success: true,
            allow_stderr: false,
            fields: &[],
            fragments: &[],
        };
        assert!(
            output.status.success(),
            "{}: stdout={stdout} stderr={stderr}",
            case.name
        );
        assert!(stderr.is_empty(), "{}: stderr={stderr}", case.name);
        assert_common_typed_slots(&stdout, &case);
        assert_field(&stdout, "fallback_attempted", "false", case.name);
        assert_field(&stdout, "external_engine_invoked", "false", case.name);
        assert_contains(&stdout, expected_result, case.name);
    }

    let _ = std::fs::remove_dir_all(root);
}

#[test]
#[cfg(feature = "vortex-traditional-analytics-benchmark")]
fn traditional_analytics_direct_transient_csv_success_keeps_typed_envelope() {
    let root = std::env::temp_dir().join(format!(
        "shardloom-cli-direct-transient-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("time after epoch")
            .as_nanos()
    ));
    std::fs::create_dir_all(&root).expect("temp root");
    let fact_csv = root.join("fact.csv");
    let dim_csv = root.join("dim.csv");
    std::fs::write(
        &fact_csv,
        "id,group_key,dim_key,value,metric,flag,category\n1,10,1,6000,2.5,1,A\n2,11,2,1000,3.5,0,B\n3,10,1,8000,4.0,1,A\n",
    )
    .expect("fact csv");
    std::fs::write(&dim_csv, "dim_key,dim_label,weight\n1,one,1.5\n2,two,2.0\n").expect("dim csv");
    let fact_arg = fact_csv.display().to_string();
    let dim_arg = dim_csv.display().to_string();

    let output = Command::new(env!("CARGO_BIN_EXE_shardloom"))
        .args([
            "traditional-analytics-run",
            "selective filter",
            &fact_arg,
            &dim_arg,
            "--input-format",
            "csv",
            "--execution-mode",
            "direct_compatibility_transient",
            "--format",
            "json",
        ])
        .output()
        .expect("shardloom command runs");
    let stdout = String::from_utf8(output.stdout).expect("stdout is utf8");
    let stderr = String::from_utf8(output.stderr).expect("stderr is utf8");
    let case = EnvelopeCase {
        name: "traditional analytics direct transient CSV success",
        args: &[],
        command: "traditional-analytics-run",
        status: "success",
        family: "benchmarks",
        success: true,
        allow_stderr: false,
        fields: &[],
        fragments: &[],
    };

    assert!(
        output.status.success(),
        "{}: stdout={stdout} stderr={stderr}",
        case.name
    );
    assert!(stderr.is_empty(), "{}: stderr={stderr}", case.name);
    assert_common_typed_slots(&stdout, &case);
    for (key, value) in [
        ("requested_execution_mode", "direct_compatibility_transient"),
        ("selected_execution_mode", "direct_compatibility_transient"),
        ("execution_mode_family", "compatibility"),
        ("mode_supported", "true"),
        ("support_status", "supported"),
        ("direct_transient_execution", "true"),
        ("vortex_native_claim_allowed", "false"),
        ("compatibility_import_included", "false"),
        ("vortex_write_reopen_included", "false"),
        ("compatibility_to_vortex_import_performed", "false"),
        ("vortex_file_written", "false"),
        ("vortex_file_read", "false"),
        ("upstream_vortex_scan_called", "false"),
        ("runtime_execution_certificate_status", "certified"),
        ("native_io_certificate_status", "not_vortex_native"),
        ("write_io", "false"),
        ("fallback_attempted", "false"),
        ("external_engine_invoked", "false"),
    ] {
        assert_field(&stdout, key, value, case.name);
    }
    assert_contains(
        &stdout,
        "{\\\"row_count\\\":2,\\\"metric_sum\\\":6.5}",
        case.name,
    );

    let _ = std::fs::remove_dir_all(root);
}

#[test]
#[cfg(feature = "vortex-traditional-analytics-benchmark")]
fn traditional_analytics_direct_transient_filter_projection_limit_keeps_typed_envelope() {
    let root = std::env::temp_dir().join(format!(
        "shardloom-cli-direct-transient-fpl-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("time after epoch")
            .as_nanos()
    ));
    std::fs::create_dir_all(&root).expect("temp root");
    let fact_csv = root.join("fact.csv");
    let dim_csv = root.join("dim.csv");
    std::fs::write(
        &fact_csv,
        "id,group_key,dim_key,value,metric,flag,category\n1,10,1,6000,2.5,1,A\n2,11,2,1000,3.5,0,B\n3,10,1,8000,4.0,1,A\n",
    )
    .expect("fact csv");
    std::fs::write(&dim_csv, "dim_key,dim_label,weight\n1,one,1.5\n2,two,2.0\n").expect("dim csv");
    let fact_arg = fact_csv.display().to_string();
    let dim_arg = dim_csv.display().to_string();

    let output = Command::new(env!("CARGO_BIN_EXE_shardloom"))
        .args([
            "traditional-analytics-run",
            "filter + projection + limit",
            &fact_arg,
            &dim_arg,
            "--input-format",
            "csv",
            "--execution-mode",
            "direct_compatibility_transient",
            "--format",
            "json",
        ])
        .output()
        .expect("shardloom command runs");
    let stdout = String::from_utf8(output.stdout).expect("stdout is utf8");
    let stderr = String::from_utf8(output.stderr).expect("stderr is utf8");
    let case = EnvelopeCase {
        name: "traditional analytics direct transient filter projection limit success",
        args: &[],
        command: "traditional-analytics-run",
        status: "success",
        family: "benchmarks",
        success: true,
        allow_stderr: false,
        fields: &[],
        fragments: &[],
    };

    assert!(
        output.status.success(),
        "{}: stdout={stdout} stderr={stderr}",
        case.name
    );
    assert!(stderr.is_empty(), "{}: stderr={stderr}", case.name);
    assert_common_typed_slots(&stdout, &case);
    for (key, value) in [
        ("scenario", "filter + projection + limit"),
        ("requested_execution_mode", "direct_compatibility_transient"),
        ("selected_execution_mode", "direct_compatibility_transient"),
        ("execution_mode_family", "compatibility"),
        ("mode_supported", "true"),
        ("support_status", "supported"),
        ("direct_transient_execution", "true"),
        ("vortex_native_claim_allowed", "false"),
        ("compatibility_import_included", "false"),
        ("vortex_write_reopen_included", "false"),
        ("compatibility_to_vortex_import_performed", "false"),
        ("vortex_file_written", "false"),
        ("vortex_file_read", "false"),
        ("upstream_vortex_scan_called", "false"),
        ("runtime_execution_certificate_status", "certified"),
        (
            "runtime_execution_certificate_id",
            "gar-flow-1c.direct_transient_csv_filter_projection_limit.runtime",
        ),
        (
            "benchmark_row_ref",
            "benchmark://local_vortex_analytics_v1/direct_transient_csv_filter_projection_limit",
        ),
        (
            "coverage_row_ref",
            "coverage.direct_compatibility_transient.local_csv_filter_projection_limit",
        ),
        ("native_io_certificate_status", "not_vortex_native"),
        ("write_io", "false"),
        ("fallback_attempted", "false"),
        ("external_engine_invoked", "false"),
    ] {
        assert_field(&stdout, key, value, case.name);
    }
    assert_contains(
        &stdout,
        "{\\\"row_count\\\":2,\\\"metric_sum\\\":14000.0}",
        case.name,
    );

    let _ = std::fs::remove_dir_all(root);
}
