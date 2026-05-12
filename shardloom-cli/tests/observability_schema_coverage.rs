use std::process::Command;

fn run_observability_schema_coverage_json() -> String {
    let output = Command::new(env!("CARGO_BIN_EXE_shardloom"))
        .args(["observability-schema-coverage", "--format", "json"])
        .output()
        .expect("observability schema coverage command runs");

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
