use std::process::Command;

fn run_cli_json(args: &[&str]) -> String {
    let output = Command::new(env!("CARGO_BIN_EXE_shardloom"))
        .args(args)
        .output()
        .expect("shardloom command runs");

    assert!(
        output.status.success(),
        "stdout={} stderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(
        output.stderr.is_empty(),
        "stderr={}",
        String::from_utf8_lossy(&output.stderr)
    );

    String::from_utf8(output.stdout).expect("stdout is utf8")
}

fn run_api_compat_plan() -> String {
    run_cli_json(&["api-compat-plan", "--format", "json"])
}

fn run_rest_api_plan_preview(scenario: &str) -> String {
    run_cli_json(&["rest-api-plan-preview", scenario, "--format", "json"])
}

fn run_rest_api_local_lifecycle(scenario: &str) -> String {
    run_cli_json(&["rest-api-local-lifecycle", scenario, "--format", "json"])
}

fn run_rest_api_event_stream(scenario: &str) -> String {
    run_cli_json(&["rest-api-event-stream", scenario, "--format", "json"])
}

fn run_rest_api_security_governance(scenario: &str) -> String {
    run_cli_json(&["rest-api-security-governance", scenario, "--format", "json"])
}

fn run_rest_api_data_plane(scenario: &str) -> String {
    run_cli_json(&["rest-api-data-plane", scenario, "--format", "json"])
}

fn field(key: &str, value: &str) -> String {
    format!("{{\"key\":\"{key}\",\"value\":\"{value}\"}}")
}

#[test]
fn rest_api_contract_plan_json_exposes_openapi_discovery_contract() {
    let output = run_cli_json(&["rest-api-contract-plan", "--format", "json"]);

    assert!(output.contains("\"command\":\"rest-api-contract-plan\""));
    assert!(output.contains("\"status\":\"success\""));
    assert!(output.contains(&field("mode", "rest_api_contract_plan")));
    assert!(output.contains(&field("schema_version", "shardloom.rest_api_contract.v1")));
    assert!(output.contains(&field("api_version", "v1")));
    assert!(output.contains(&field("openapi_version", "3.2.0")));
    assert!(output.contains(&field(
        "openapi_contract_path",
        "docs/api/shardloom-openapi-v1.yaml"
    )));
    assert!(output.contains(&field(
        "problem_details_media_type",
        "application/problem+json"
    )));
    assert!(output.contains(&field("represented_resource_count", "21")));
    assert!(output.contains(&field(
        "execution_policy_fields",
        "engine_mode,fallback_policy,materialization_policy,result_policy,evidence_policy"
    )));
    assert!(output.contains(&field(
        "discovery_endpoint_paths",
        "/v1/health,/v1/version,/v1/capabilities,/v1/capabilities/engines,/v1/capabilities/operators,/v1/capabilities/functions,/v1/capabilities/sql,/v1/capabilities/adapters,/v1/capabilities/deployment,/v1/adapters,/v1/sources,/v1/sinks"
    )));
    assert!(output.contains(&field("discovery_endpoints_side_effect_free", "true")));
    assert!(output.contains(
        "\"lifecycle\":{\"fields\":[{\"key\":\"command_family\",\"value\":\"rest_api_planning\""
    ));
}

#[test]
fn rest_api_contract_plan_json_preserves_no_server_no_probe_policy() {
    let output = run_cli_json(&["rest-api-contract-plan", "--format", "json"]);

    assert!(output.contains(&field("server_started", "false")));
    assert!(output.contains(&field("network_listener_opened", "false")));
    assert!(output.contains(&field("network_probe", "false")));
    assert!(output.contains(&field("dataset_probe", "false")));
    assert!(output.contains(&field("object_store_io", "false")));
    assert!(output.contains(&field("catalog_probe", "false")));
    assert!(output.contains(&field("credential_resolution", "false")));
    assert!(output.contains(&field("query_execution", "false")));
    assert!(output.contains(&field("runtime_execution", "false")));
    assert!(output.contains(&field("write_io", "false")));
    assert!(output.contains(&field("external_engine_invoked", "false")));
    assert!(output.contains(&field("fallback_execution_allowed", "false")));
    assert!(output.contains(&field("fallback_attempted", "false")));
}

#[test]
fn serve_discovery_mode_json_is_contract_only_without_listener() {
    let output = run_cli_json(&[
        "serve",
        "--mode",
        "discovery",
        "--bind",
        "127.0.0.1:8787",
        "--format",
        "json",
    ]);

    assert!(output.contains("\"command\":\"serve\""));
    assert!(output.contains("\"status\":\"success\""));
    assert!(output.contains(&field("mode", "rest_api_discovery_mode")));
    assert!(output.contains(&field("server_mode", "discovery")));
    assert!(output.contains(&field("bind", "127.0.0.1:8787")));
    assert!(output.contains(&field("health_endpoint", "/v1/health")));
    assert!(output.contains(&field("capabilities_endpoint", "/v1/capabilities")));
    assert!(output.contains(&field("serve_command_contract_only", "true")));
    assert!(output.contains(&field("server_started", "false")));
    assert!(output.contains(&field("network_listener_opened", "false")));
    assert!(output.contains(&field("dataset_probe", "false")));
    assert!(output.contains(&field("query_execution", "false")));
    assert!(output.contains(&field("fallback_attempted", "false")));
}

#[test]
fn rest_api_plan_preview_json_exposes_stage_by_stage_contract() {
    let output = run_rest_api_plan_preview("certified-local-batch");

    assert!(output.contains("\"command\":\"rest-api-plan-preview\""));
    assert!(output.contains("\"status\":\"success\""));
    assert!(output.contains(&field("mode", "rest_api_plan_preview")));
    assert!(output.contains(&field(
        "schema_version",
        "shardloom.rest_api_plan_preview.v1"
    )));
    assert!(output.contains(&field("scenario", "certified-local-batch")));
    assert!(output.contains(&field("preview_status", "certified_preview")));
    assert!(output.contains(&field("plan_handle", "plan://cg23/certified-local-batch")));
    assert!(output.contains(&field(
        "preview_operations",
        "plan_handle,validate,explain,estimate,unsupported_report,certification_preview"
    )));
    assert!(output.contains(&field(
        "stage_order",
        "parser,binder,native_logical,native_physical,execution_readiness,evidence_readiness,certification"
    )));
    assert!(output.contains(&field("parser_stage_status", "ready")));
    assert!(output.contains(&field("binder_stage_status", "ready")));
    assert!(output.contains(&field("native_logical_stage_status", "ready")));
    assert!(output.contains(&field("native_physical_stage_status", "ready")));
    assert!(output.contains(&field("execution_readiness_stage_status", "ready")));
    assert!(output.contains(&field("evidence_readiness_stage_status", "ready")));
    assert!(output.contains(&field("certification_stage_status", "certified")));
    assert!(output.contains(&field("problem_details_emitted", "false")));
}

#[test]
fn rest_api_plan_preview_json_covers_partial_blocked_invalid_and_unsupported_fixtures() {
    let partial = run_rest_api_plan_preview("partial-hybrid-fixture");
    let blocked = run_rest_api_plan_preview("blocked-remote-object-store");
    let invalid = run_rest_api_plan_preview("invalid-input");
    let unsupported = run_rest_api_plan_preview("unsupported-operator");

    assert!(partial.contains("\"status\":\"warning\""));
    assert!(partial.contains(&field("preview_status", "partial_preview")));
    assert!(partial.contains(&field("native_physical_stage_status", "partial")));
    assert!(partial.contains(&field("certification_stage_status", "partial")));

    assert!(blocked.contains("\"status\":\"warning\""));
    assert!(blocked.contains(&field("preview_status", "blocked")));
    assert!(blocked.contains(&field("native_physical_stage_status", "blocked")));
    assert!(blocked.contains(&field("problem_details_emitted", "true")));
    assert!(blocked.contains(&field(
        "problem_details_diagnostic_code",
        "SL_OBJECT_STORE_UNSUPPORTED"
    )));

    assert!(invalid.contains("\"status\":\"error\""));
    assert!(invalid.contains(&field("preview_status", "invalid_input")));
    assert!(invalid.contains(&field("parser_stage_status", "invalid_input")));
    assert!(invalid.contains(&field("problem_details_status", "422")));
    assert!(invalid.contains("\"code\":\"SL_INVALID_INPUT\""));

    assert!(unsupported.contains("\"status\":\"unsupported\""));
    assert!(unsupported.contains(&field("preview_status", "unsupported")));
    assert!(unsupported.contains(&field("native_logical_stage_status", "unsupported")));
    assert!(unsupported.contains(&field(
        "problem_details_diagnostic_code",
        "SL_UNSUPPORTED_SQL"
    )));
    assert!(unsupported.contains("\"code\":\"SL_UNSUPPORTED_SQL\""));
}

#[test]
fn rest_api_plan_preview_json_preserves_no_server_no_probe_no_fallback_policy() {
    for scenario in [
        "certified-local-batch",
        "partial-hybrid-fixture",
        "blocked-remote-object-store",
        "invalid-input",
        "unsupported-operator",
    ] {
        let output = run_rest_api_plan_preview(scenario);

        assert!(output.contains(&field("server_started", "false")));
        assert!(output.contains(&field("network_listener_opened", "false")));
        assert!(output.contains(&field("network_probe", "false")));
        assert!(output.contains(&field("dataset_probe", "false")));
        assert!(output.contains(&field("object_store_io", "false")));
        assert!(output.contains(&field("catalog_probe", "false")));
        assert!(output.contains(&field("credential_resolution", "false")));
        assert!(output.contains(&field("query_execution", "false")));
        assert!(output.contains(&field("runtime_execution", "false")));
        assert!(output.contains(&field("write_io", "false")));
        assert!(output.contains(&field("external_engine_invoked", "false")));
        assert!(output.contains(&field("fallback_execution_allowed", "false")));
        assert!(output.contains(&field("fallback_attempted", "false")));
        assert!(output.contains(&field("execution_delegated", "false")));
        assert!(output.contains(&field("effect_policy_violated", "false")));
    }
}

#[test]
fn rest_api_local_lifecycle_json_exposes_certified_result_and_evidence_refs() {
    let output = run_rest_api_local_lifecycle("certified-local-batch");

    assert!(output.contains("\"command\":\"rest-api-local-lifecycle\""));
    assert!(output.contains("\"status\":\"success\""));
    assert!(output.contains(&field("mode", "rest_api_local_lifecycle")));
    assert!(output.contains(&field(
        "schema_version",
        "shardloom.rest_api_local_lifecycle.v1"
    )));
    assert!(output.contains(&field("scenario", "certified-local-batch")));
    assert!(output.contains(&field("lifecycle_status", "succeeded")));
    assert!(output.contains(&field(
        "lifecycle_operations",
        "execute,status,cancel,retry,profile,certificates,lineage,results,artifacts,cleanup"
    )));
    assert!(output.contains(&field(
        "result_ref",
        "result://cg23/certified-local-batch/0001"
    )));
    assert!(output.contains(&field(
        "result_artifact_ref",
        "artifacts/cg23/certified-local-batch/result.vortex"
    )));
    assert!(output.contains(&field("inline_json_available", "true")));
    assert!(output.contains(&field("paged_json_available", "true")));
    assert!(output.contains(&field("jsonl_ndjson_available", "true")));
    assert!(output.contains(&field("vortex_artifact_available", "true")));
    assert!(output.contains(&field(
        "arrow_ipc_materialization",
        "decoded_columnar_boundary"
    )));
    assert!(output.contains(&field("arrow_ipc_certified_native", "false")));
    assert!(output.contains(&field(
        "preferred_high_fidelity_result_modes",
        "vortex_artifact,object_reference"
    )));
    assert!(output.contains(&field("result_ttl_seconds", "3600")));
    assert!(output.contains(&field("retention_policy", "local_ephemeral")));
    assert!(output.contains(&field("cleanup_required", "true")));
    assert!(output.contains(&field(
        "execution_certificate_ref",
        "certificates/cg23/certified-local-batch/execution.json"
    )));
    assert!(output.contains(&field(
        "native_io_certificate_ref",
        "certificates/cg23/certified-local-batch/native-io.json"
    )));
    assert!(output.contains(&field(
        "materialization_boundary_report_ref",
        "artifacts/cg23/certified-local-batch/materialization.json"
    )));
    assert!(output.contains("\"certificates\":["));
    assert!(output.contains("\"result_refs\":["));
    assert!(output.contains("\"artifact_refs\":["));
}

#[test]
fn rest_api_local_lifecycle_json_covers_cancel_retry_and_blocked_diagnostics() {
    let cancel = run_rest_api_local_lifecycle("cancel-requested");
    let retry = run_rest_api_local_lifecycle("retry-requested");
    let blocked = run_rest_api_local_lifecycle("blocked-uncertified");

    assert!(cancel.contains("\"status\":\"success\""));
    assert!(cancel.contains(&field("lifecycle_status", "canceled")));
    assert!(cancel.contains(&field("cancellation_requested", "true")));
    assert!(cancel.contains(&field("cancellation_status", "canceled")));
    assert!(cancel.contains(&field("cancel_diagnostic_code", "SL_NO_FALLBACK_EXECUTION")));

    assert!(retry.contains("\"status\":\"success\""));
    assert!(retry.contains(&field("lifecycle_status", "retry_scheduled")));
    assert!(retry.contains(&field("retry_requested", "true")));
    assert!(retry.contains(&field("retry_status", "scheduled")));
    assert!(retry.contains(&field(
        "retry_diagnostic_code",
        "SL_RESOURCE_BUDGET_EXCEEDED"
    )));

    assert!(blocked.contains("\"status\":\"unsupported\""));
    assert!(blocked.contains(&field("lifecycle_status", "blocked")));
    assert!(blocked.contains(&field("non_certified_path_blocked", "true")));
    assert!(blocked.contains(&field("query_execution", "false")));
    assert!(blocked.contains(&field("runtime_execution", "false")));
    assert!(blocked.contains("\"code\":\"SL_NOT_IMPLEMENTED\""));
}

#[test]
fn rest_api_local_lifecycle_json_preserves_no_external_effects_and_no_fallback_policy() {
    for scenario in [
        "certified-local-batch",
        "cancel-requested",
        "retry-requested",
        "blocked-uncertified",
    ] {
        let output = run_rest_api_local_lifecycle(scenario);

        assert!(output.contains(&field("server_started", "false")));
        assert!(output.contains(&field("network_listener_opened", "false")));
        assert!(output.contains(&field("network_probe", "false")));
        assert!(output.contains(&field("dataset_probe", "false")));
        assert!(output.contains(&field("object_store_io", "false")));
        assert!(output.contains(&field("catalog_probe", "false")));
        assert!(output.contains(&field("credential_resolution", "false")));
        assert!(output.contains(&field("data_read", "false")));
        assert!(output.contains(&field("data_materialized", "false")));
        assert!(output.contains(&field("write_io", "false")));
        assert!(output.contains(&field("external_engine_invoked", "false")));
        assert!(output.contains(&field("fallback_execution_allowed", "false")));
        assert!(output.contains(&field("fallback_attempted", "false")));
        assert!(output.contains(&field("execution_delegated", "false")));
        assert!(output.contains(&field("effect_policy_violated", "false")));
    }
}

#[test]
fn rest_api_event_stream_json_exposes_sse_asyncapi_and_cloudevents_contracts() {
    let output = run_rest_api_event_stream("certified-live-fixture");

    assert!(output.contains("\"command\":\"rest-api-event-stream\""));
    assert!(output.contains("\"status\":\"success\""));
    assert!(output.contains(&field("mode", "rest_api_event_stream")));
    assert!(output.contains(&field(
        "schema_version",
        "shardloom.rest_api_event_stream.v1"
    )));
    assert!(output.contains(&field("scenario", "certified-live-fixture")));
    assert!(output.contains(&field("event_stream_status", "certified_fixture")));
    assert!(output.contains(&field(
        "stream_ref",
        "event-stream://cg23/live-fixture/group-count"
    )));
    assert!(output.contains(&field("engine_mode", "live")));
    assert!(output.contains(&field(
        "delivery_protocols",
        "server_sent_events,websocket_optional"
    )));
    assert!(output.contains(&field("sse_first", "true")));
    assert!(output.contains(&field("sse_media_type", "text/event-stream")));
    assert!(output.contains(&field("websocket_required", "false")));
    assert!(output.contains(&field("asyncapi_version", "3.0.0")));
    assert!(output.contains(&field(
        "asyncapi_contract_path",
        "docs/api/shardloom-asyncapi-events-v1.yaml"
    )));
    assert!(output.contains(&field("cloudevents_spec_version", "1.0")));
    assert!(output.contains(&field(
        "cloudevents_required_fields",
        "specversion,id,type,source,subject,time,datacontenttype,dataschema,data"
    )));
    assert!(output.contains(&field(
        "event_types",
        "progress,state,checkpoint,watermark,certificate,lineage,benchmark,hybrid_hot_cold_contribution"
    )));
    assert!(output.contains(&field("event_count", "7")));
    assert!(output.contains(&field("workload_certified", "true")));
    assert!(output.contains(&field("production_claim_allowed", "false")));
    assert!(output.contains(&field(
        "freshness_certificate_ref",
        "certificates/cg22/live/fixture/freshness.json"
    )));
    assert!(output.contains(&field(
        "execution_certificate_ref",
        "certificates/cg22/live/fixture/group-count/execution.json"
    )));
}

#[test]
fn rest_api_event_stream_json_covers_hybrid_and_blocked_scenarios() {
    let hybrid = run_rest_api_event_stream("certified-hybrid-fixture");
    let blocked = run_rest_api_event_stream("blocked-production-workload");
    let broker = run_rest_api_event_stream("broker-requested");

    assert!(hybrid.contains("\"status\":\"success\""));
    assert!(hybrid.contains(&field("scenario", "certified-hybrid-fixture")));
    assert!(hybrid.contains(&field("engine_mode", "hybrid")));
    assert!(hybrid.contains(&field("hybrid_fixture_certified", "true")));
    assert!(hybrid.contains(&field("hot_cold_contribution_event_count", "1")));
    assert!(hybrid.contains(&field(
        "hot_cold_contribution_report_ref",
        "artifacts/cg22/hybrid/fixture/hot-cold-contribution.json"
    )));
    assert!(hybrid.contains(&field(
        "delta_overlay_certificate_ref",
        "certificates/cg22/hybrid/fixture/delta-overlay.json"
    )));

    assert!(blocked.contains("\"status\":\"warning\""));
    assert!(blocked.contains(&field("event_stream_status", "blocked_missing_evidence")));
    assert!(blocked.contains(&field("workload_certified", "false")));
    assert!(blocked.contains(&field("cg22_workload_evidence_present", "false")));
    assert!(blocked.contains(&field("cg8_runtime_evidence_present", "false")));
    assert!(blocked.contains(&field("cg4_checkpoint_evidence_present", "false")));
    assert!(blocked.contains(&field("cg16_execution_certificate_present", "false")));
    assert!(blocked.contains("\"code\":\"SL_NOT_IMPLEMENTED\""));

    assert!(broker.contains("\"status\":\"unsupported\""));
    assert!(broker.contains(&field("event_stream_status", "unsupported_external_broker")));
    assert!(broker.contains(&field("broker_requested", "true")));
    assert!(broker.contains(&field("broker_required", "true")));
    assert!(broker.contains(&field("broker_io", "false")));
    assert!(broker.contains("\"code\":\"SL_EXTERNAL_EFFECT_DISABLED\""));
}

#[test]
fn rest_api_event_stream_json_preserves_no_broker_object_store_or_fallback_effects() {
    for scenario in [
        "certified-live-fixture",
        "certified-hybrid-fixture",
        "blocked-production-workload",
        "broker-requested",
    ] {
        let output = run_rest_api_event_stream(scenario);

        assert!(output.contains(&field("server_started", "false")));
        assert!(output.contains(&field("network_listener_opened", "false")));
        assert!(output.contains(&field("network_probe", "false")));
        assert!(output.contains(&field("broker_io", "false")));
        assert!(output.contains(&field("object_store_io", "false")));
        assert!(output.contains(&field("dataset_probe", "false")));
        assert!(output.contains(&field("catalog_probe", "false")));
        assert!(output.contains(&field("credential_resolution", "false")));
        assert!(output.contains(&field("data_read", "false")));
        assert!(output.contains(&field("data_materialized", "false")));
        assert!(output.contains(&field("runtime_execution", "false")));
        assert!(output.contains(&field("write_io", "false")));
        assert!(output.contains(&field("external_engine_invoked", "false")));
        assert!(output.contains(&field("fallback_execution_allowed", "false")));
        assert!(output.contains(&field("fallback_attempted", "false")));
        assert!(output.contains(&field("execution_delegated", "false")));
        assert!(output.contains(&field("effect_policy_violated", "false")));
    }
}

#[test]
fn rest_api_security_governance_json_exposes_auth_scope_mcp_and_evidence_contracts() {
    let output = run_rest_api_security_governance("safe-local-default");

    assert!(output.contains("\"command\":\"rest-api-security-governance\""));
    assert!(output.contains("\"status\":\"success\""));
    assert!(output.contains(&field("mode", "rest_api_security_governance")));
    assert!(output.contains(&field(
        "schema_version",
        "shardloom.rest_api_security_governance.v1"
    )));
    assert!(output.contains(&field("scenario", "safe-local-default")));
    assert!(output.contains(&field("governance_status", "available_contract")));
    assert!(output.contains(&field(
        "auth_postures",
        "local_only:available_default,token:reference_only_contract,mtls:reference_only_contract,oidc:reference_only_contract,service_account:reference_only_contract"
    )));
    assert!(output.contains(&field(
        "api_scopes",
        "read:allowed_local_metadata,plan:allowed_dry_run,execute:policy_required,write:policy_required,cancel:policy_required,admin:policy_required,benchmark:dry_run_only,migration:plan_only,agent:dry_run_explain_estimate_certify_only"
    )));
    assert!(output.contains(&field("credential_references_only", "true")));
    assert!(output.contains(&field("credentials_resolved", "false")));
    assert!(output.contains(&field(
        "token_secret_ref",
        "secret-ref://shardloom/rest/token"
    )));
    assert!(output.contains(&field("raw_secret_values_present", "false")));
    assert!(output.contains(&field("secrets_redacted", "true")));
    assert!(output.contains(&field("redaction_policy", "strict_reference_only")));
    assert!(output.contains(&field(
        "mcp_tools",
        "dry_run:allowed,explain:allowed,estimate:allowed,certify_preview:allowed,execute:blocked_policy_required,write:blocked_destructive_policy_required"
    )));
    assert!(output.contains(&field("mcp_dry_run_default", "true")));
    assert!(output.contains(&field("mcp_effectful_tools_allowed", "false")));
    assert!(output.contains(&field(
        "evidence_model_signals",
        "opentelemetry_traces,opentelemetry_metrics,opentelemetry_logs,openlineage_facets,problem_details_errors,cloudevents,certificate_refs"
    )));
    assert!(output.contains(&field("opentelemetry_exporter_enabled", "false")));
    assert!(output.contains(&field("openlineage_facets_mapped", "true")));
    assert!(output.contains(&field("problem_details_mapped", "true")));
    assert!(output.contains(&field("cloudevents_mapped", "true")));
    assert!(output.contains(&field("certificate_refs_mapped", "true")));
}

#[test]
fn rest_api_security_governance_json_blocks_destructive_and_keeps_agent_tools_dry_run() {
    let blocked = run_rest_api_security_governance("destructive-policy-required");
    let agent = run_rest_api_security_governance("agent-mcp-discovery");

    assert!(blocked.contains("\"status\":\"unsupported\""));
    assert!(blocked.contains(&field("governance_status", "blocked_policy_required")));
    assert!(blocked.contains(&field("destructive_operation_requested", "true")));
    assert!(blocked.contains(&field("destructive_policy_required", "true")));
    assert!(blocked.contains(&field("destructive_policy_present", "false")));
    assert!(blocked.contains(&field("destructive_operations_allowed", "false")));
    assert!(blocked.contains(&field("problem_details_emitted", "true")));
    assert!(blocked.contains(&field(
        "problem_details_diagnostic_code",
        "SL_EXTERNAL_EFFECT_DISABLED"
    )));
    assert!(blocked.contains("\"code\":\"SL_EXTERNAL_EFFECT_DISABLED\""));

    assert!(agent.contains("\"status\":\"success\""));
    assert!(agent.contains(&field("scenario", "agent-mcp-discovery")));
    assert!(agent.contains(&field("governance_status", "agent_dry_run_only")));
    assert!(agent.contains(&field("mcp_dry_run_default", "true")));
    assert!(agent.contains(&field("mcp_effectful_tools_allowed", "false")));
    assert!(agent.contains(&field("mcp_discovery_side_effect_free", "true")));
    assert!(agent.contains(&field("mcp_tool_execution", "false")));
}

#[test]
fn rest_api_security_governance_json_preserves_no_secret_resolution_or_effects() {
    for scenario in [
        "safe-local-default",
        "destructive-policy-required",
        "agent-mcp-discovery",
    ] {
        let output = run_rest_api_security_governance(scenario);

        assert!(output.contains(&field("server_started", "false")));
        assert!(output.contains(&field("network_listener_opened", "false")));
        assert!(output.contains(&field("network_probe", "false")));
        assert!(output.contains(&field("dataset_probe", "false")));
        assert!(output.contains(&field("object_store_io", "false")));
        assert!(output.contains(&field("catalog_probe", "false")));
        assert!(output.contains(&field("credential_resolution", "false")));
        assert!(output.contains(&field("secret_resolution", "false")));
        assert!(output.contains(&field("raw_secret_emitted", "false")));
        assert!(output.contains(&field("audit_write_io", "false")));
        assert!(output.contains(&field("mcp_tool_execution", "false")));
        assert!(output.contains(&field("data_read", "false")));
        assert!(output.contains(&field("data_materialized", "false")));
        assert!(output.contains(&field("query_execution", "false")));
        assert!(output.contains(&field("runtime_execution", "false")));
        assert!(output.contains(&field("write_io", "false")));
        assert!(output.contains(&field("external_engine_invoked", "false")));
        assert!(output.contains(&field("fallback_execution_allowed", "false")));
        assert!(output.contains(&field("fallback_attempted", "false")));
        assert!(output.contains(&field("execution_delegated", "false")));
        assert!(output.contains(&field("effect_policy_violated", "false")));
    }
}

#[test]
fn rest_api_data_plane_json_exposes_transfer_and_large_payload_policy() {
    let output = run_rest_api_data_plane("artifact-reference-default");

    assert!(output.contains("\"command\":\"rest-api-data-plane\""));
    assert!(output.contains("\"status\":\"success\""));
    assert!(output.contains(&field("mode", "rest_api_data_plane")));
    assert!(output.contains(&field("schema_version", "shardloom.rest_api_data_plane.v1")));
    assert!(output.contains(&field("scenario", "artifact-reference-default")));
    assert!(output.contains(&field("data_plane_status", "contract_available")));
    assert!(output.contains(&field("rest_control_plane_required", "true")));
    assert!(output.contains(&field(
        "rest_control_plane_sufficient_for_local_use",
        "true"
    )));
    assert!(output.contains(&field("flight_adbc_required_for_basic_local_use", "false")));
    assert!(output.contains(&field(
        "transfer_modes",
        "inline_json:decoded_rows,paged_json:decoded_rows,jsonl_ndjson:decoded_rows,vortex_artifact:native_vortex_artifact,object_reference:native_object_reference_future,arrow_ipc_decoded_boundary:decoded_columnar_boundary,flight_ticket_future:decoded_columnar_boundary,adbc_endpoint_future:decoded_columnar_boundary"
    )));
    assert!(output.contains(&field("large_payload_threshold_bytes", "1048576")));
    assert!(output.contains(&field(
        "preferred_large_payload_modes",
        "vortex_artifact,object_reference,paged_json"
    )));
    assert!(output.contains(&field("paged_json_available", "true")));
    assert!(output.contains(&field("jsonl_ndjson_available", "true")));
    assert!(output.contains(&field("vortex_artifact_available", "true")));
    assert!(output.contains(&field("object_reference_available", "true")));
    assert!(output.contains(&field("arrow_ipc_decoded_boundary_available", "true")));
    assert!(output.contains(&field("arrow_ipc_certified_native", "false")));
    assert!(output.contains(&field("decoded_columnar_boundary_declared", "true")));
    assert!(output.contains(&field("materialization_declared", "true")));
    assert!(output.contains(&field("fidelity_declared", "true")));
    assert!(output.contains(&field("result_policy_declared", "true")));
}

#[test]
fn rest_api_data_plane_json_covers_optional_flight_adbc_and_standards_matrix() {
    let flight = run_rest_api_data_plane("flight-ticket-requested");
    let adbc = run_rest_api_data_plane("adbc-endpoint-requested");
    let standards = run_rest_api_data_plane("standards-matrix");

    assert!(flight.contains("\"status\":\"warning\""));
    assert!(flight.contains(&field("data_plane_status", "optional_transport_planned")));
    assert!(flight.contains(&field("flight_ticket_requested", "true")));
    assert!(flight.contains(&field("flight_ticket_supported", "false")));
    assert!(flight.contains(&field("flight_server_started", "false")));
    assert!(flight.contains(&field("optional_transport_required", "false")));
    assert!(flight.contains("\"code\":\"SL_NOT_IMPLEMENTED\""));

    assert!(adbc.contains("\"status\":\"warning\""));
    assert!(adbc.contains(&field("adbc_endpoint_requested", "true")));
    assert!(adbc.contains(&field("adbc_endpoint_supported", "false")));
    assert!(adbc.contains(&field("adbc_endpoint_opened", "false")));
    assert!(adbc.contains(&field("optional_transport_required", "false")));

    assert!(standards.contains("\"status\":\"success\""));
    assert!(standards.contains(&field("data_plane_status", "standards_matrix_available")));
    assert!(standards.contains(&field("standards_matrix_requested", "true")));
    assert!(standards.contains(&field("standards_matrix_count", "11")));
    assert!(standards.contains(&field(
        "standards_names",
        "iceberg_rest_catalog,polaris,gravitino,delta_sharing,substrait,wasi_webassembly_components,nats_jetstream,redpanda,kafka_compatible,paimon,fluss"
    )));
}

#[test]
fn rest_api_data_plane_json_preserves_no_transport_catalog_broker_or_fallback_effects() {
    for scenario in [
        "artifact-reference-default",
        "flight-ticket-requested",
        "adbc-endpoint-requested",
        "standards-matrix",
    ] {
        let output = run_rest_api_data_plane(scenario);

        assert!(output.contains(&field("server_started", "false")));
        assert!(output.contains(&field("network_listener_opened", "false")));
        assert!(output.contains(&field("network_probe", "false")));
        assert!(output.contains(&field("flight_server_started", "false")));
        assert!(output.contains(&field("adbc_endpoint_opened", "false")));
        assert!(output.contains(&field("broker_io", "false")));
        assert!(output.contains(&field("object_store_io", "false")));
        assert!(output.contains(&field("catalog_probe", "false")));
        assert!(output.contains(&field("dataset_probe", "false")));
        assert!(output.contains(&field("credential_resolution", "false")));
        assert!(output.contains(&field("data_read", "false")));
        assert!(output.contains(&field("data_materialized", "false")));
        assert!(output.contains(&field("query_execution", "false")));
        assert!(output.contains(&field("runtime_execution", "false")));
        assert!(output.contains(&field("write_io", "false")));
        assert!(output.contains(&field("external_engine_invoked", "false")));
        assert!(output.contains(&field("fallback_execution_allowed", "false")));
        assert!(output.contains(&field("fallback_attempted", "false")));
        assert!(output.contains(&field("execution_delegated", "false")));
        assert!(output.contains(&field("effect_policy_violated", "false")));
    }
}

#[test]
fn api_compat_plan_json_exposes_cli_api_protocol_contract() {
    let output = run_api_compat_plan();

    assert!(output.contains("\"command\":\"api-compat-plan\""));
    assert!(output.contains("\"status\":\"success\""));
    assert!(output.contains("\"schema_version\":\"shardloom.output.v2\""));
    assert!(output.contains(&field("mode", "api_compat_plan")));
    assert!(output.contains(&field(
        "schema_version",
        "shardloom.cli_api_json_protocol.v1"
    )));
    assert!(output.contains(&field("protocol_id", "shardloom.cli_json.v1")));
    assert!(output.contains(&field("protocol_stability", "experimental")));
    assert!(output.contains(&field(
        "output_envelope_schema_version",
        "shardloom.output.v2"
    )));
    assert!(output.contains(&field(
        "required_envelope_fields",
        "schema_version,command,status,summary,human_text,fallback,diagnostics,result,result_refs,artifacts,artifact_refs,certificates,policy,lifecycle,capability_snapshot,fields"
    )));
    assert!(output.contains(&field("required_typed_payload_fields", "fields")));
    assert!(output.contains(&field("legacy_fields_mirror_present", "true")));
    assert!(output.contains(&field("flat_fields_primary_payload_allowed", "false")));
    assert!(output.contains(&field(
        "command_status_values",
        "success,warning,error,unsupported"
    )));
    assert!(output.contains(&field("compatibility_lock_status", "locked")));
    assert!(output.contains(&field(
        "compatibility_lock_fixture_statuses",
        "success,error,unsupported,blocked,evidence_incomplete,certified_local_execution,missing_binary,foundry_optional"
    )));
    assert!(output.contains(&field("json_error_paths_enveloped", "true")));
    assert!(output.contains(&field("unknown_command_json_enveloped", "true")));
    assert!(output.contains(&field("missing_binary_error_payload_shaped", "true")));
    assert!(output.contains(&field("output_formats", "text,json")));
}

#[test]
fn api_compat_plan_json_is_report_only_and_no_fallback() {
    let output = run_api_compat_plan();

    assert!(output.contains(&field(
        "thin_python_wrapper_boundary",
        "cli_json_subprocess_first"
    )));
    assert!(output.contains(&field("pyo3_maturin_allowed", "false")));
    assert!(output.contains(&field("foundry_required", "false")));
    assert!(output.contains(&field("dataframe_api_implemented", "false")));
    assert!(output.contains(&field("side_effect_free", "true")));
    assert!(output.contains(&field("filesystem_probe", "false")));
    assert!(output.contains(&field("network_probe", "false")));
    assert!(output.contains(&field("catalog_probe", "false")));
    assert!(output.contains(&field("adapter_probe", "false")));
    assert!(output.contains(&field("parser_executed", "false")));
    assert!(output.contains(&field("runtime_execution", "false")));
    assert!(output.contains(&field("write_io", "false")));
    assert!(output.contains(&field("external_publish", "not_performed")));
    assert!(output.contains(&field("external_publish_performed", "false")));
    assert!(output.contains(&field("fallback_execution_allowed", "false")));
    assert!(output.contains(&field("fallback_attempted", "false")));
    assert!(output.contains("\"attempted\":false"));
    assert!(output.contains("\"allowed\":false"));
}

#[test]
fn api_compat_plan_json_routes_common_fields_into_typed_slots() {
    let output = run_api_compat_plan();

    assert!(
        output.contains("\"policy\":{\"fields\":[{\"key\":\"publish_allowed\",\"value\":\"false\"")
    );
    assert!(output.contains(&field("fallback_execution_allowed", "false")));
    assert!(output.contains(
        "\"lifecycle\":{\"fields\":[{\"key\":\"command_family\",\"value\":\"rest_api_planning\""
    ));
    assert!(output.contains(&field("mode", "api_compat_plan")));
    assert!(output.contains(&field(
        "schema_version",
        "shardloom.cli_api_json_protocol.v1"
    )));
    assert!(output.contains("\"capability_snapshot\":{\"fields\":[]}"));
    assert!(output.contains("\"fields\":[{\"key\":\"mode\",\"value\":\"api_compat_plan\""));
}
