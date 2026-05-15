use std::process::Command;

fn run_observability_schema_coverage_json() -> String {
    run_json("observability-schema-coverage")
}

fn run_json(command: &str) -> String {
    let output = Command::new(env!("CARGO_BIN_EXE_shardloom"))
        .args([command, "--format", "json"])
        .output()
        .expect("observability command runs");

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

fn field(key: &str, value: &str) -> String {
    format!("{{\"key\":\"{key}\",\"value\":\"{value}\"}}")
}

#[test]
fn observability_schema_coverage_json_exposes_required_areas() {
    let output = run_observability_schema_coverage_json();

    assert!(output.contains("\"command\":\"observability-schema-coverage\""));
    assert!(output.contains("\"status\":\"success\""));
    assert!(output.contains(&field("mode", "observability_schema_coverage")));
    assert!(output.contains(&field(
        "schema_version",
        "shardloom.observability_schema_coverage.v1"
    )));
    assert!(output.contains(&field("observability_area_count", "9")));
    assert!(output.contains(&field("complete_observability_area_count", "9")));
    assert!(output.contains(&field("missing_observability_area_count", "0")));
    assert!(output.contains(&field("schema_coverage_complete", "true")));
    assert!(output.contains(&field("observability_area_2_name", "vortex_io")));
    assert!(output.contains(&field("observability_area_7_name", "certificate")));
}

#[test]
fn observability_schema_coverage_json_keeps_runtime_and_exporters_disabled() {
    let output = run_observability_schema_coverage_json();

    assert!(output.contains(&field("local_json_required", "true")));
    assert!(output.contains(&field("exporter_integration_enabled", "false")));
    assert!(output.contains(&field("runtime_collection_enabled", "false")));
    assert!(output.contains(&field("debug_bundle_schema_present", "true")));
    assert!(output.contains(&field("redaction_required", "true")));
    assert!(output.contains(&field("certificate_link_required", "true")));
    assert!(output.contains(&field("fallback_attempted", "false")));
    assert!(output.contains(&field(
        "observability_area_0_trace_span_schema",
        "report_only"
    )));
    assert!(output.contains(&field("observability_area_0_log_schema", "report_only")));
}

#[test]
fn runtime_report_json_exposes_gar_0018_introspection_boundaries() {
    let output = run_json("runtime-report");

    assert!(output.contains("\"command\":\"runtime-report\""));
    assert!(output.contains("\"status\":\"success\""));
    assert!(output.contains(&field("mode", "runtime_report")));
    assert!(output.contains(&field(
        "schema_version",
        "shardloom.runtime_observability_report.v1"
    )));
    assert!(output.contains(&field("report_id", "gar-0018-a.runtime_introspection.v1")));
    assert!(output.contains(&field("gar_id", "GAR-0018-A")));
    assert!(output.contains(&field("support_status", "report_only")));
    assert!(output.contains(&field("claim_gate_status", "not_claim_grade")));
    assert!(output.contains(&field("runtime_report_status", "diagnostic_only")));
    assert!(output.contains(&field("local_benchmark_span_schema_present", "true")));
    assert!(output.contains(&field(
        "local_benchmark_stage_timing_schema_present",
        "true"
    )));
    assert!(output.contains(&field("local_benchmark_stage_timing_field_count", "10")));
    assert!(output.contains(&field(
        "local_benchmark_stage_timing_field_order",
        "source_read_millis,compatibility_parse_millis,compatibility_to_vortex_import_millis,vortex_write_millis,vortex_reopen_millis,vortex_scan_millis,operator_compute_millis,result_sink_write_millis,evidence_render_millis,total_runtime_millis"
    )));
    assert!(output.contains(&field("benchmark_metadata_surface_present", "true")));
    assert!(output.contains(&field("local_benchmark_spans_measured", "false")));
    assert!(output.contains(&field("live_profiling_status", "unsupported")));
    assert!(output.contains(&field(
        "distributed_runtime_introspection_status",
        "unsupported"
    )));
    assert!(output.contains(&field("profiler_backend_enabled", "false")));
    assert!(output.contains(&field("trace_backend_enabled", "false")));
    assert!(output.contains(&field("exporter_integration_enabled", "false")));
    assert!(output.contains(&field("runtime_collection_enabled", "false")));
    assert!(output.contains(&field("profile_artifact_generated", "false")));
    assert!(output.contains(&field("debug_bundle_generated", "false")));
    assert!(output.contains(&field("runtime_blocker_count", "9")));
    assert!(output.contains(&field(
        "runtime_blocker_order",
        "live_profiling_collector,distributed_trace_backend,profiler_backend,metrics_exporter,coordinator_worker_runtime,execution_certificate,native_io_certificate,redaction_policy,no_fallback_evidence"
    )));
    assert!(output.contains(&field("no_runtime_collection_or_external_effects", "true")));
    assert!(output.contains(&field("external_engine_invoked", "false")));
    assert!(output.contains(&field("fallback_attempted", "false")));
    assert!(output.contains(&field("fallback_execution_allowed", "false")));
}

#[test]
fn profile_plan_json_keeps_live_profiling_unsupported_without_fallback() {
    let output = run_json("profile-plan");

    assert!(output.contains("\"command\":\"profile-plan\""));
    assert!(output.contains("\"status\":\"unsupported\""));
    assert!(output.contains(&field("mode", "profile_plan")));
    assert!(output.contains(&field("gar_id", "GAR-0018-A")));
    assert!(output.contains(&field("support_status", "unsupported")));
    assert!(output.contains(&field("claim_gate_status", "not_claim_grade")));
    assert!(output.contains(&field("live_profiling_status", "unsupported")));
    assert!(output.contains(&field("profiler_backend_enabled", "false")));
    assert!(output.contains(&field("runtime_collection_enabled", "false")));
    assert!(output.contains(&field("profile_artifact_generated", "false")));
    assert!(output.contains(&field("external_engine_invoked", "false")));
    assert!(output.contains(&field("fallback_attempted", "false")));
    assert!(output.contains(&field("fallback_execution_allowed", "false")));
}
