use std::process::Command;

fn run_cg20_user_capability_gate_json() -> String {
    let output = Command::new(env!("CARGO_BIN_EXE_shardloom"))
        .args(["cg20-user-capability-gate", "--format", "json"])
        .output()
        .expect("cg20-user-capability-gate command runs");

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
fn cg20_user_capability_gate_exposes_surface_order_and_existing_evidence() {
    let output = run_cg20_user_capability_gate_json();

    assert!(output.contains("\"command\":\"cg20-user-capability-gate\""));
    assert!(output.contains("\"status\":\"success\""));
    assert!(output.contains(&field("mode", "cg20_user_capability_promotion_gate")));
    assert!(output.contains(&field(
        "schema_version",
        "shardloom.user_capability_promotion_gate.v1"
    )));
    assert!(output.contains(&field("report_id", "cg20.user_capability_promotion_gate")));
    assert!(output.contains(&field("surface_count", "15")));
    assert!(output.contains(&field("existing_evidence_surface_count", "4")));
    assert!(output.contains(&field("blocked_surface_count", "11")));
    assert!(output.contains(&field(
        "surface_order",
        "world_class_sufficiency_foundation,python_wrapper_foundation,input_adapter_registry_foundation,unstructured_workflow_boundary_contracts,sql_frontend_runtime,dataframe_query_builder_runtime,notebook_runtime,udf_plugin_runtime,unstructured_media_effect_runtime,universal_adapter_runtime,event_api_saas_adapter_runtime,adapter_read_write_commit_runtime,semantic_profile_conformance_runtime,workload_certified_capability_closeout,best_default_dossier_publication"
    )));
    assert!(output.contains(&field(
        "existing_world_class_sufficiency_report_present",
        "true"
    )));
    assert!(output.contains(&field("existing_python_wrapper_foundation_present", "true")));
    assert!(output.contains(&field("existing_input_adapter_registry_present", "true")));
    assert!(output.contains(&field(
        "existing_unstructured_workflow_boundary_contracts_present",
        "true"
    )));
}

#[test]
fn cg20_user_capability_gate_blocks_runtime_effects_fallback_and_claims() {
    let output = run_cg20_user_capability_gate_json();

    for key in [
        "sql_runtime_allowed",
        "dataframe_runtime_allowed",
        "notebook_runtime_allowed",
        "udf_execution_allowed",
        "plugin_execution_allowed",
        "unstructured_media_decode_allowed",
        "ocr_transcription_embedding_llm_allowed",
        "adapter_runtime_allowed",
        "external_api_call_allowed",
        "catalog_probe_allowed",
        "object_store_io_allowed",
        "write_io_allowed",
        "external_engine_invoked",
        "best_default_claim_allowed",
        "user_capability_claim_allowed",
        "fallback_attempted",
        "fallback_execution_allowed",
    ] {
        assert!(
            output.contains(&field(key, "false")),
            "missing false field {key}"
        );
    }

    for key in [
        "world_class_sufficiency_report_required",
        "semantic_profile_required",
        "sql_coverage_required",
        "operator_coverage_required",
        "function_coverage_required",
        "adapter_certification_required",
        "native_io_certificate_required",
        "execution_certificate_required",
        "correctness_evidence_required",
        "benchmark_evidence_required",
        "workload_constitution_required",
        "materialization_boundary_required",
        "effect_policy_required",
        "security_governance_required",
        "protocol_surface_parity_required",
        "runtime_promotions_blocked",
        "claim_blocked",
        "side_effect_free",
        "plan_only",
    ] {
        assert!(
            output.contains(&field(key, "true")),
            "missing true field {key}"
        );
    }
    assert!(output.contains(&field("execution", "not_performed")));
}
