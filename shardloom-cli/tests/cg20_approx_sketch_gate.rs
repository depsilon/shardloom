use std::process::Command;

fn run_cg20_approx_sketch_gate_json() -> String {
    let output = Command::new(env!("CARGO_BIN_EXE_shardloom"))
        .args(["cg20-approx-sketch-gate", "--format", "json"])
        .output()
        .expect("cg20-approx-sketch-gate command runs");

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
fn cg20_approx_sketch_gate_exposes_function_surfaces() {
    let output = run_cg20_approx_sketch_gate_json();

    assert!(output.contains("\"command\":\"cg20-approx-sketch-gate\""));
    assert!(output.contains("\"status\":\"success\""));
    assert!(output.contains(&field("mode", "cg20_approx_sketch_function_gate")));
    assert!(output.contains(&field(
        "schema_version",
        "shardloom.approx_sketch_function_gate.v1"
    )));
    assert!(output.contains(&field("report_id", "cg20.approx_sketch_function_gate")));
    assert!(output.contains(&field("gar_id", "GAR-0021-A")));
    assert!(output.contains(&field("support_status", "report_only")));
    assert!(output.contains(&field("claim_gate_status", "not_claim_grade")));
    assert!(output.contains(&field(
        "admission_contract_status",
        "evidence_requirements_declared"
    )));
    assert!(output.contains(&field(
        "deterministic_unsupported_status",
        "blocked_until_certified"
    )));
    assert!(output.contains(&field("canonical_function_name", "approx_count_distinct")));
    assert!(output.contains(&field("alias_names", "approx_distinct,approx_n_unique")));
    assert!(output.contains(&field("surface_count", "20")));
    assert!(output.contains(&field("existing_evidence_surface_count", "2")));
    assert!(output.contains(&field("blocked_surface_count", "18")));
    assert!(output.contains(&field(
        "surface_order",
        "function_coverage_matrix_entry,rfc_sequencing_contract,canonical_approx_count_distinct,alias_approx_distinct,alias_approx_n_unique,ungrouped_approx_distinct_execution,grouped_approx_distinct_execution,partial_sketch_construction,associative_sketch_merge,sketch_serialization,sketch_deserialization,sketch_version_hash_seed_metadata,error_bounds_confidence_model,null_string_temporal_value_semantics,dictionary_encoded_strategy,run_length_encoded_strategy,selection_vector_validity_strategy,partial_decode_materialization_boundary,exact_reference_fixture_comparison,benchmark_certificate_native_io_closeout"
    )));
    assert!(output.contains(&field(
        "value_handling_order",
        "null_policy,string_values,binary_values,temporal_values,dictionary_encoded_values,run_length_encoded_values,validity_and_selection_vectors,nested_values_rejected_or_certified"
    )));
    assert!(output.contains(&field(
        "existing_function_coverage_matrix_entry_present",
        "true"
    )));
    assert!(output.contains(&field("existing_rfc_sequencing_contract_present", "true")));
}

#[test]
fn cg20_approx_sketch_gate_blocks_runtime_dependencies_and_claims() {
    let output = run_cg20_approx_sketch_gate_json();

    for key in [
        "function_registry_entry_allowed",
        "sketch_state_runtime_allowed",
        "sketch_merge_runtime_allowed",
        "sketch_serialization_runtime_allowed",
        "grouped_aggregate_runtime_allowed",
        "encoded_dictionary_strategy_allowed",
        "encoded_run_length_strategy_allowed",
        "selection_vector_strategy_allowed",
        "partial_decode_execution_allowed",
        "materialization_without_report_allowed",
        "generic_sketch_dependency_allowed",
        "exact_claim_allowed",
        "approximate_function_claim_allowed",
        "external_engine_invoked",
        "fallback_attempted",
        "fallback_execution_allowed",
    ] {
        assert!(
            output.contains(&field(key, "false")),
            "missing false field {key}"
        );
    }

    for key in [
        "function_registry_required",
        "aggregate_state_required",
        "sketch_serialization_required",
        "stable_hash_seed_policy_required",
        "error_bounds_required",
        "confidence_model_required",
        "exact_reference_fixtures_required",
        "encoded_dictionary_strategy_required",
        "encoded_run_length_strategy_required",
        "selection_vector_strategy_required",
        "partial_decode_materialization_boundary_required",
        "correctness_evidence_required",
        "benchmark_evidence_required",
        "execution_certificate_required",
        "native_io_certificate_required",
        "runtime_promotions_blocked",
        "claim_blocked",
        "admission_contract_complete",
        "deterministic_unsupported_diagnostics_ready",
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
