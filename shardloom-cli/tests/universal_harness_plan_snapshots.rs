use std::process::Command;

fn run_universal_harness_plan_json() -> String {
    let output = Command::new(env!("CARGO_BIN_EXE_shardloom"))
        .args(["universal-harness-plan", "--format", "json"])
        .output()
        .expect("universal harness plan command runs");

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
fn universal_harness_json_exposes_cg18_contract() {
    let output = run_universal_harness_plan_json();

    assert!(output.contains("\"command\":\"universal-harness-plan\""));
    assert!(output.contains("\"status\":\"success\""));
    assert!(output.contains(&field("mode", "universal_harness_plan")));
    assert!(output.contains(&field("execution", "not_performed")));
    assert!(output.contains(&field("plan_only", "true")));
    assert!(output.contains(&field("schema_version", "shardloom.universal_harness.v1")));
    assert!(output.contains(&field("universal_harness_status", "evidence_incomplete")));
    assert!(output.contains(&field("surface_count", "7")));
    assert!(output.contains(&field("harness_environment_count", "5")));
    assert!(output.contains(&field("external_baseline_count", "6")));
    assert!(output.contains(&field(
        "runner_contract_field_order",
        "command,schema_version,exit_code,status,diagnostics,fallback_execution_allowed,side_effects,output_artifacts,metrics"
    )));
    assert!(output.contains(&field(
        "surface_kind_order",
        "cli_json_runner,package_import,deployment_profile,foundry_example,external_baseline_runner,comparison_report_dataset,portability_check"
    )));
    assert!(output.contains(&field(
        "harness_environment_kind_order",
        "local,ci,container,foundry_optional,benchmark_extras_optional"
    )));
    assert!(output.contains(&field(
        "baseline_engine_order",
        "spark,datafusion,polars,duckdb,dask,pandas"
    )));
}

#[test]
fn universal_harness_json_preserves_no_execution_or_publish_effects() {
    let output = run_universal_harness_plan_json();

    assert!(output.contains(&field("output_envelope_required", "true")));
    assert!(output.contains(&field("stable_command_schema_required", "true")));
    assert!(output.contains(&field("exit_code_required", "true")));
    assert!(output.contains(&field("diagnostics_required", "true")));
    assert!(output.contains(&field("side_effect_manifest_required", "true")));
    assert!(output.contains(&field("output_artifacts_required", "true")));
    assert!(output.contains(&field("metrics_required", "true")));
    assert!(output.contains(&field("comparison_dataset_required", "true")));
    assert!(output.contains(&field("correctness_evidence_required", "true")));
    assert!(output.contains(&field("benchmark_evidence_required", "true")));
    assert!(output.contains(&field("foundry_required", "false")));
    assert!(output.contains(&field("foundry_optional_example", "true")));
    assert!(output.contains(&field("local_harness_required", "true")));
    assert!(output.contains(&field("ci_harness_required", "true")));
    assert!(output.contains(&field("container_harness_required", "true")));
    assert!(output.contains(&field("foundry_optional_harness_required", "true")));
    assert!(output.contains(&field("optional_benchmark_environment_required", "true")));
    assert!(output.contains(&field(
        "external_engines_as_runtime_dependencies_allowed",
        "false"
    )));
    assert!(output.contains(&field(
        "baselines_comparison_only_runtime_dependency_free",
        "true"
    )));
    assert!(output.contains(&field("package_import_performed", "false")));
    assert!(output.contains(&field("deployment_performed", "false")));
    assert!(output.contains(&field("external_baseline_execution", "false")));
    assert!(output.contains(&field("runtime_execution", "false")));
    assert!(output.contains(&field("filesystem_probe", "false")));
    assert!(output.contains(&field("network_probe", "false")));
    assert!(output.contains(&field("catalog_probe", "false")));
    assert!(output.contains(&field("adapter_probe", "false")));
    assert!(output.contains(&field("read_io", "false")));
    assert!(output.contains(&field("write_io", "false")));
    assert!(output.contains(&field("external_publish", "false")));
    assert!(output.contains(&field("fallback_execution_allowed", "false")));
    assert!(output.contains(&field("fallback_attempted", "false")));
    assert!(output.contains(&field("production_claim_allowed", "false")));
    assert!(output.contains(&field("side_effect_free", "true")));
}
