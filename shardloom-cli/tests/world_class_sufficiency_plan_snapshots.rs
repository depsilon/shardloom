use std::process::Command;

fn run_world_class_sufficiency_plan_json() -> String {
    let output = Command::new(env!("CARGO_BIN_EXE_shardloom"))
        .args(["world-class-sufficiency-plan", "--format", "json"])
        .output()
        .expect("world-class sufficiency plan command runs");

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
fn world_class_sufficiency_json_exposes_cg20_contract() {
    let output = run_world_class_sufficiency_plan_json();

    assert!(output.contains("\"command\":\"world-class-sufficiency-plan\""));
    assert!(output.contains("\"status\":\"success\""));
    assert!(output.contains(&field("mode", "world_class_sufficiency_plan")));
    assert!(output.contains(&field("execution", "not_performed")));
    assert!(output.contains(&field("plan_only", "true")));
    assert!(output.contains(&field(
        "schema_version",
        "shardloom.world_class_sufficiency.v1"
    )));
    assert!(output.contains(&field("report_id", "cg20.world_class_sufficiency")));
    assert!(output.contains(&field(
        "workload_constitution_ref",
        "workload_constitution.pending"
    )));
    assert!(output.contains(&field("claim_level", "not_certified")));
    assert!(output.contains(&field("publication_decision", "not_certified")));
    assert!(output.contains(&field("dimension_count", "30")));
    assert!(output.contains(&field("required_dimension_count", "30")));
    assert!(output.contains(&field("evidence_insufficient_dimension_count", "30")));
    assert!(output.contains(&field(
        "dimension_kind_order",
        "workload_constitution,sql_surface,operator_surface,function_surface,adapter_surface,semantic_profiles,migration_surface,data_etl_surface,python_surface,dataframe_query_builder,notebook_experience,udf_plugin,unstructured_media,universal_adapter_catalog,event_api_saas_adapters,api_surface,observability_surface,deployment_surface,extension_surface,security_governance,native_io_certificate_coverage,execution_certificate_coverage,correctness_evidence,semantic_conformance,benchmark_evidence,memory_spill,capability_snapshots,best_choice_scorecard,best_default_dossier,no_fallback_integrity"
    )));
}

#[test]
fn world_class_sufficiency_json_preserves_no_execution_or_claim_effects() {
    let output = run_world_class_sufficiency_plan_json();

    assert!(output.contains(&field("sql_surface_status", "evidence_insufficient")));
    assert!(output.contains(&field("operator_surface_status", "evidence_insufficient")));
    assert!(output.contains(&field("function_surface_status", "evidence_insufficient")));
    assert!(output.contains(&field("adapter_surface_status", "evidence_insufficient")));
    assert!(output.contains(&field("python_surface_status", "evidence_insufficient")));
    assert!(output.contains(&field("data_etl_surface_status", "evidence_insufficient")));
    assert!(output.contains(&field(
        "unstructured_media_surface_status",
        "evidence_insufficient"
    )));
    assert!(output.contains(&field(
        "universal_adapter_catalog_status",
        "evidence_insufficient"
    )));
    assert!(output.contains(&field(
        "python_package_status",
        "source_tree_wheel_sdist_ready"
    )));
    assert!(output.contains(&field(
        "fresh_environment_smoke_status",
        "local_smoke_ready"
    )));
    assert!(output.contains(&field("conda_package_split_status", "planned")));
    assert!(output.contains(&field("conda_cli_package_status", "planned")));
    assert!(output.contains(&field("conda_python_package_status", "planned")));
    assert!(output.contains(&field("conda_metapackage_status", "planned")));
    assert!(output.contains(&field("benchmark_extras_status", "optional_planned")));
    assert!(output.contains(&field(
        "native_io_certificate_coverage",
        "evidence_insufficient"
    )));
    assert!(output.contains(&field(
        "execution_certificate_coverage",
        "evidence_insufficient"
    )));
    assert!(output.contains(&field(
        "performance_regression_budget_status",
        "evidence_insufficient"
    )));
    assert!(output.contains(&field("unsupported_rate", "not_measured")));
    assert!(output.contains(&field("materialization_rate", "not_measured")));
    assert!(output.contains(&field("best_default_claim_allowed", "false")));
    assert!(output.contains(&field("runtime_execution", "false")));
    assert!(output.contains(&field("parser_executed", "false")));
    assert!(output.contains(&field("adapter_probe", "false")));
    assert!(output.contains(&field("filesystem_probe", "false")));
    assert!(output.contains(&field("network_probe", "false")));
    assert!(output.contains(&field("catalog_probe", "false")));
    assert!(output.contains(&field("data_read", "false")));
    assert!(output.contains(&field("data_decoded", "false")));
    assert!(output.contains(&field("data_materialized", "false")));
    assert!(output.contains(&field("row_read", "false")));
    assert!(output.contains(&field("arrow_converted", "false")));
    assert!(output.contains(&field("object_store_io", "false")));
    assert!(output.contains(&field("write_io", "false")));
    assert!(output.contains(&field("spill_io_performed", "false")));
    assert!(output.contains(&field("external_engine_execution", "false")));
    assert!(output.contains(&field("fallback_execution_allowed", "false")));
    assert!(output.contains(&field("fallback_attempted", "false")));
    assert!(output.contains(&field("production_claim_allowed", "false")));
    assert!(output.contains(&field("side_effect_free", "true")));
}
