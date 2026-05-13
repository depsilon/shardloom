use std::process::Command;

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

fn field(key: &str, value: &str) -> String {
    format!("{{\"key\":\"{key}\",\"value\":\"{value}\"}}")
}

fn assert_contains(output: &str, fragment: &str, case_name: &str) {
    assert!(
        output.contains(fragment),
        "{case_name}: missing fragment {fragment}\nstdout={output}"
    );
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
            name: "optimizer unsupported",
            args: &["optimizer-plan", "--format", "json"],
            command: "optimizer-plan",
            status: "unsupported",
            family: "optimizer_planning",
            success: false,
            allow_stderr: false,
            fields: &[("execution", "not_performed")],
            fragments: &["\"code\":\"SL_NOT_IMPLEMENTED\""],
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
            name: "vortex runtime success",
            args: &[
                "vortex-schedule-plan",
                "file://tmp/data.vortex",
                "8",
                "2",
                "--format",
                "json",
            ],
            command: "vortex-schedule-plan",
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
            name: "foundry optional harness success",
            args: &["universal-harness-plan", "--format", "json"],
            command: "universal-harness-plan",
            status: "success",
            family: "evidence_certificates",
            success: true,
            allow_stderr: false,
            fields: &[("foundry_required", "false")],
            fragments: &[],
        },
    ];

    for case in cases {
        run_case(&case);
    }
}
