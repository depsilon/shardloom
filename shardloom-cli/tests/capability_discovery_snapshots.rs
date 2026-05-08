use std::process::Command;

const REPORT_ONLY_BOOL_FIELD_KEYS: [&str; 9] = [
    "fallback_execution_allowed",
    "fallback_attempted",
    "side_effect_free",
    "filesystem_probe",
    "network_probe",
    "catalog_probe",
    "adapter_probe",
    "parser_executed",
    "runtime_execution",
];

const SQL_FIELD_KEYS: [&str; 14] = [
    "scope",
    "schema_version",
    "fallback_execution_allowed",
    "fallback_attempted",
    "side_effect_free",
    "filesystem_probe",
    "network_probe",
    "catalog_probe",
    "adapter_probe",
    "parser_executed",
    "runtime_execution",
    "sql_feature_count",
    "planned_count",
    "certified_count",
];

const FUNCTION_FIELD_KEYS: [&str; 13] = [
    "scope",
    "schema_version",
    "fallback_execution_allowed",
    "fallback_attempted",
    "side_effect_free",
    "filesystem_probe",
    "network_probe",
    "catalog_probe",
    "adapter_probe",
    "parser_executed",
    "runtime_execution",
    "function_group_count",
    "planned_count",
];

const OPERATOR_FIELD_KEYS: [&str; 111] = [
    "scope",
    "schema_version",
    "fallback_execution_allowed",
    "fallback_attempted",
    "side_effect_free",
    "filesystem_probe",
    "network_probe",
    "catalog_probe",
    "adapter_probe",
    "parser_executed",
    "runtime_execution",
    "operator_family_count",
    "production_certified_count",
    "physical_operator_schema_version",
    "physical_operator_plan_id",
    "physical_operator_count",
    "physical_operator_ready_count",
    "physical_operator_missing_kernel_count",
    "physical_operator_unsupported_count",
    "physical_operator_fallback_execution_allowed",
    "physical_operator_runtime_execution",
    "physical_operator_execution_profile_schema_version",
    "physical_operator_execution_profile_count",
    "physical_operator_reference_only_level_count",
    "physical_operator_row_materialization_level_count",
    "physical_operator_arrow_conversion_level_count",
    "physical_operator_fallback_level_count",
    "metadata_physical_kernel_schema_version",
    "metadata_physical_kernel_supported_primitives",
    "metadata_physical_kernel_contextual_only",
    "metadata_physical_kernel_requires_correctness_evidence",
    "metadata_physical_kernel_requires_memory_safety_evidence",
    "metadata_physical_kernel_requires_benchmark_for_production",
    "metadata_physical_kernel_data_read",
    "metadata_physical_kernel_data_decoded",
    "metadata_physical_kernel_data_materialized",
    "metadata_physical_kernel_object_store_io",
    "metadata_physical_kernel_write_io",
    "metadata_physical_kernel_spill_io",
    "metadata_physical_kernel_runtime_execution",
    "metadata_physical_kernel_fallback_execution_allowed",
    "metadata_count_kernel_admission_schema_version",
    "metadata_count_kernel_admission_contextual_only",
    "metadata_count_kernel_admission_operator_kind",
    "metadata_count_kernel_admission_required_kernel_kind",
    "metadata_count_kernel_admission_requires_metadata_kernel_evidence",
    "metadata_count_kernel_admission_requires_correctness_evidence",
    "metadata_count_kernel_admission_requires_memory_safety_evidence",
    "metadata_count_kernel_admission_requires_benchmark_for_production",
    "metadata_count_kernel_admission_runtime_execution",
    "metadata_count_kernel_admission_fallback_execution_allowed",
    "metadata_filter_kernel_admission_schema_version",
    "metadata_filter_kernel_admission_contextual_only",
    "metadata_filter_kernel_admission_operator_kind",
    "metadata_filter_kernel_admission_required_kernel_kind",
    "metadata_filter_kernel_admission_requires_metadata_kernel_evidence",
    "metadata_filter_kernel_admission_requires_correctness_evidence",
    "metadata_filter_kernel_admission_requires_memory_safety_evidence",
    "metadata_filter_kernel_admission_requires_benchmark_for_production",
    "metadata_filter_kernel_admission_runtime_execution",
    "metadata_filter_kernel_admission_fallback_execution_allowed",
    "metadata_projection_kernel_admission_schema_version",
    "metadata_projection_kernel_admission_contextual_only",
    "metadata_projection_kernel_admission_operator_kind",
    "metadata_projection_kernel_admission_required_kernel_kind",
    "metadata_projection_kernel_admission_requires_projection_readiness",
    "metadata_projection_kernel_admission_requires_correctness_evidence",
    "metadata_projection_kernel_admission_requires_memory_safety_evidence",
    "metadata_projection_kernel_admission_requires_benchmark_for_production",
    "metadata_projection_kernel_admission_runtime_execution",
    "metadata_projection_kernel_admission_fallback_execution_allowed",
    "encoded_count_physical_kernel_schema_version",
    "encoded_count_physical_kernel_id",
    "encoded_count_physical_kernel_supported_primitive",
    "encoded_count_physical_kernel_operator_kind",
    "encoded_count_physical_kernel_kernel_kind",
    "encoded_count_physical_kernel_execution_level",
    "encoded_count_physical_kernel_contextual_only",
    "encoded_count_physical_kernel_requires_execution_certificate",
    "encoded_count_physical_kernel_requires_correctness_evidence",
    "encoded_count_physical_kernel_requires_memory_safety_evidence",
    "encoded_count_physical_kernel_requires_benchmark_for_production",
    "encoded_count_physical_kernel_discovery_reads_data",
    "encoded_count_physical_kernel_evaluated_path_reads_data",
    "encoded_count_physical_kernel_runtime_execution",
    "encoded_count_physical_kernel_fallback_execution_allowed",
    "encoded_count_kernel_admission_schema_version",
    "encoded_count_kernel_admission_contextual_only",
    "encoded_count_kernel_admission_operator_kind",
    "encoded_count_kernel_admission_required_kernel_kind",
    "encoded_count_kernel_admission_requires_physical_kernel_evidence",
    "encoded_count_kernel_admission_requires_correctness_evidence",
    "encoded_count_kernel_admission_requires_memory_safety_evidence",
    "encoded_count_kernel_admission_requires_benchmark_for_production",
    "encoded_count_kernel_admission_runtime_execution",
    "encoded_count_kernel_admission_fallback_execution_allowed",
    "encoded_count_local_guard_schema_version",
    "encoded_count_local_guard_id",
    "encoded_count_local_guard_accepted_approval_sources",
    "encoded_count_local_guard_local_execution_status",
    "encoded_count_local_guard_mode",
    "encoded_count_local_guard_layout_row_count_path_accepted",
    "encoded_count_local_guard_approved_local_scan_result_bridge_available",
    "encoded_count_local_guard_approved_local_scan_result_bridge_requires_executed_report",
    "encoded_count_local_guard_returns_count_result",
    "encoded_count_local_guard_side_effect_free",
    "encoded_count_local_guard_data_read",
    "encoded_count_local_guard_data_decoded",
    "encoded_count_local_guard_data_materialized",
    "encoded_count_local_guard_runtime_execution",
    "encoded_count_local_guard_fallback_execution_allowed",
];

const ADAPTER_FIELD_KEYS: [&str; 13] = [
    "scope",
    "schema_version",
    "fallback_execution_allowed",
    "fallback_attempted",
    "side_effect_free",
    "filesystem_probe",
    "network_probe",
    "catalog_probe",
    "adapter_probe",
    "parser_executed",
    "runtime_execution",
    "adapter_entry_count",
    "read_supported_count",
];

const SEMANTIC_PROFILE_FIELD_KEYS: [&str; 13] = [
    "scope",
    "schema_version",
    "fallback_execution_allowed",
    "fallback_attempted",
    "side_effect_free",
    "filesystem_probe",
    "network_probe",
    "catalog_probe",
    "adapter_probe",
    "parser_executed",
    "runtime_execution",
    "semantic_profile_count",
    "dimensions_declared_count",
];

const MIGRATION_FIELD_KEYS: [&str; 13] = [
    "scope",
    "schema_version",
    "fallback_execution_allowed",
    "fallback_attempted",
    "side_effect_free",
    "filesystem_probe",
    "network_probe",
    "catalog_probe",
    "adapter_probe",
    "parser_executed",
    "runtime_execution",
    "migration_report_count",
    "supported_construct_count",
];

const CERTIFICATION_FIELD_KEYS: [&str; 16] = [
    "scope",
    "schema_version",
    "fallback_execution_allowed",
    "fallback_attempted",
    "side_effect_free",
    "filesystem_probe",
    "network_probe",
    "catalog_probe",
    "adapter_probe",
    "parser_executed",
    "runtime_execution",
    "sql_feature_count",
    "operator_family_count",
    "function_group_count",
    "adapter_entry_count",
    "best_choice_claim",
];

#[test]
fn capability_discovery_json_field_keys_are_stable() {
    for (scope, expected_keys) in [
        ("sql", SQL_FIELD_KEYS.as_slice()),
        ("functions", FUNCTION_FIELD_KEYS.as_slice()),
        ("operators", OPERATOR_FIELD_KEYS.as_slice()),
        ("adapters", ADAPTER_FIELD_KEYS.as_slice()),
        ("semantic-profiles", SEMANTIC_PROFILE_FIELD_KEYS.as_slice()),
        ("migration", MIGRATION_FIELD_KEYS.as_slice()),
        ("certification", CERTIFICATION_FIELD_KEYS.as_slice()),
    ] {
        let output = run_capabilities_scope(scope);
        let keys = field_keys(&output);
        assert_eq!(keys.as_slice(), expected_keys, "scope={scope}");
    }
}

#[test]
fn capability_discovery_json_fields_remain_report_only() {
    for scope in [
        "sql",
        "functions",
        "operators",
        "adapters",
        "semantic-profiles",
        "migration",
        "certification",
    ] {
        let output = run_capabilities_scope(scope);
        for key in REPORT_ONLY_BOOL_FIELD_KEYS {
            let expected_value = key == "side_effect_free";
            assert!(
                output.contains(&field_pair(key, expected_value)),
                "scope={scope} key={key}"
            );
        }
        assert!(output.contains("\"attempted\":false"), "scope={scope}");
        assert!(output.contains("\"allowed\":false"), "scope={scope}");
        assert!(output.contains("\"diagnostics\":[]"), "scope={scope}");
        assert!(!output.contains("generated_at"), "scope={scope}");
    }
}

#[test]
fn capability_discovery_scope_values_are_stable() {
    for (scope, field_value) in [
        ("sql", "sql"),
        ("functions", "functions"),
        ("operators", "operators"),
        ("adapters", "adapters"),
        ("semantic-profiles", "semantic_profiles"),
        ("migration", "migration"),
        ("certification", "certification"),
    ] {
        let output = run_capabilities_scope(scope);
        assert!(
            output.contains(&format!(
                "{{\"key\":\"scope\",\"value\":\"{field_value}\"}}"
            )),
            "scope={scope}"
        );
    }
}

#[test]
fn operator_capability_discovery_includes_physical_plan_blockers() {
    let output = run_capabilities_scope("operators");

    assert_operator_discovery_physical_plan(&output);
    assert_operator_discovery_metadata_kernel(&output);
    assert_operator_discovery_metadata_count_kernel_admission(&output);
    assert_operator_discovery_metadata_filter_kernel_admission(&output);
    assert_operator_discovery_metadata_projection_kernel_admission(&output);
    assert_operator_discovery_encoded_count_kernel(&output);
    assert_operator_discovery_encoded_count_guard(&output);
}

fn assert_operator_discovery_physical_plan(output: &str) {
    assert!(output.contains(
        "{\"key\":\"physical_operator_schema_version\",\"value\":\"shardloom.physical_operator_plan.v1\"}"
    ));
    assert!(output.contains(
        "{\"key\":\"physical_operator_plan_id\",\"value\":\"cg7.1-physical-operator-foundation\"}"
    ));
    assert!(output.contains("{\"key\":\"physical_operator_count\",\"value\":\"3\"}"));
    assert!(output.contains("{\"key\":\"physical_operator_ready_count\",\"value\":\"0\"}"));
    assert!(
        output.contains("{\"key\":\"physical_operator_missing_kernel_count\",\"value\":\"3\"}")
    );
    assert!(output.contains(
        "{\"key\":\"physical_operator_fallback_execution_allowed\",\"value\":\"false\"}"
    ));
    assert!(
        output.contains("{\"key\":\"physical_operator_runtime_execution\",\"value\":\"false\"}")
    );
    assert!(output.contains(
        "{\"key\":\"physical_operator_execution_profile_schema_version\",\"value\":\"shardloom.physical_operator_execution_profiles.v1\"}"
    ));
    assert!(
        output.contains("{\"key\":\"physical_operator_execution_profile_count\",\"value\":\"3\"}")
    );
    assert!(
        output
            .contains("{\"key\":\"physical_operator_reference_only_level_count\",\"value\":\"0\"}")
    );
    assert!(
        output.contains("{\"key\":\"physical_operator_fallback_level_count\",\"value\":\"0\"}")
    );
}

fn assert_operator_discovery_metadata_kernel(output: &str) {
    assert!(output.contains(
        "{\"key\":\"metadata_physical_kernel_schema_version\",\"value\":\"shardloom.vortex_metadata_physical_kernel.v1\"}"
    ));
    assert!(output.contains(
        "{\"key\":\"metadata_physical_kernel_supported_primitives\",\"value\":\"count_all,count_where,filter_predicate\"}"
    ));
    assert!(
        output
            .contains("{\"key\":\"metadata_physical_kernel_contextual_only\",\"value\":\"true\"}")
    );
    assert!(output.contains(
        "{\"key\":\"metadata_physical_kernel_requires_correctness_evidence\",\"value\":\"true\"}"
    ));
    assert!(output.contains(
        "{\"key\":\"metadata_physical_kernel_requires_memory_safety_evidence\",\"value\":\"true\"}"
    ));
    assert!(output.contains(
        "{\"key\":\"metadata_physical_kernel_requires_benchmark_for_production\",\"value\":\"true\"}"
    ));
    assert!(
        output.contains("{\"key\":\"metadata_physical_kernel_data_read\",\"value\":\"false\"}")
    );
    assert!(
        output.contains(
            "{\"key\":\"metadata_physical_kernel_runtime_execution\",\"value\":\"false\"}"
        )
    );
    assert!(output.contains(
        "{\"key\":\"metadata_physical_kernel_fallback_execution_allowed\",\"value\":\"false\"}"
    ));
}

fn assert_operator_discovery_metadata_count_kernel_admission(output: &str) {
    assert!(output.contains(
        "{\"key\":\"metadata_count_kernel_admission_schema_version\",\"value\":\"shardloom.vortex_metadata_count_kernel_admission.v1\"}"
    ));
    assert!(output.contains(
        "{\"key\":\"metadata_count_kernel_admission_contextual_only\",\"value\":\"true\"}"
    ));
    assert!(output.contains(
        "{\"key\":\"metadata_count_kernel_admission_operator_kind\",\"value\":\"count_aggregate\"}"
    ));
    assert!(output.contains(
        "{\"key\":\"metadata_count_kernel_admission_required_kernel_kind\",\"value\":\"metadata\"}"
    ));
    assert!(output.contains(
        "{\"key\":\"metadata_count_kernel_admission_requires_metadata_kernel_evidence\",\"value\":\"true\"}"
    ));
    assert!(output.contains(
        "{\"key\":\"metadata_count_kernel_admission_requires_correctness_evidence\",\"value\":\"true\"}"
    ));
    assert!(output.contains(
        "{\"key\":\"metadata_count_kernel_admission_requires_memory_safety_evidence\",\"value\":\"true\"}"
    ));
    assert!(output.contains(
        "{\"key\":\"metadata_count_kernel_admission_requires_benchmark_for_production\",\"value\":\"true\"}"
    ));
    assert!(output.contains(
        "{\"key\":\"metadata_count_kernel_admission_runtime_execution\",\"value\":\"false\"}"
    ));
    assert!(output.contains(
        "{\"key\":\"metadata_count_kernel_admission_fallback_execution_allowed\",\"value\":\"false\"}"
    ));
}

fn assert_operator_discovery_metadata_filter_kernel_admission(output: &str) {
    assert!(output.contains(
        "{\"key\":\"metadata_filter_kernel_admission_schema_version\",\"value\":\"shardloom.vortex_metadata_filter_kernel_admission.v1\"}"
    ));
    assert!(output.contains(
        "{\"key\":\"metadata_filter_kernel_admission_contextual_only\",\"value\":\"true\"}"
    ));
    assert!(output.contains(
        "{\"key\":\"metadata_filter_kernel_admission_operator_kind\",\"value\":\"filter\"}"
    ));
    assert!(output.contains(
        "{\"key\":\"metadata_filter_kernel_admission_required_kernel_kind\",\"value\":\"metadata\"}"
    ));
    assert!(output.contains(
        "{\"key\":\"metadata_filter_kernel_admission_requires_metadata_kernel_evidence\",\"value\":\"true\"}"
    ));
    assert!(output.contains(
        "{\"key\":\"metadata_filter_kernel_admission_requires_correctness_evidence\",\"value\":\"true\"}"
    ));
    assert!(output.contains(
        "{\"key\":\"metadata_filter_kernel_admission_requires_memory_safety_evidence\",\"value\":\"true\"}"
    ));
    assert!(output.contains(
        "{\"key\":\"metadata_filter_kernel_admission_requires_benchmark_for_production\",\"value\":\"true\"}"
    ));
    assert!(output.contains(
        "{\"key\":\"metadata_filter_kernel_admission_runtime_execution\",\"value\":\"false\"}"
    ));
    assert!(output.contains(
        "{\"key\":\"metadata_filter_kernel_admission_fallback_execution_allowed\",\"value\":\"false\"}"
    ));
}

fn assert_operator_discovery_metadata_projection_kernel_admission(output: &str) {
    assert!(output.contains(
        "{\"key\":\"metadata_projection_kernel_admission_schema_version\",\"value\":\"shardloom.vortex_metadata_projection_kernel_admission.v1\"}"
    ));
    assert!(output.contains(
        "{\"key\":\"metadata_projection_kernel_admission_contextual_only\",\"value\":\"true\"}"
    ));
    assert!(output.contains(
        "{\"key\":\"metadata_projection_kernel_admission_operator_kind\",\"value\":\"project\"}"
    ));
    assert!(output.contains(
        "{\"key\":\"metadata_projection_kernel_admission_required_kernel_kind\",\"value\":\"metadata\"}"
    ));
    assert!(output.contains(
        "{\"key\":\"metadata_projection_kernel_admission_requires_projection_readiness\",\"value\":\"true\"}"
    ));
    assert!(output.contains(
        "{\"key\":\"metadata_projection_kernel_admission_requires_correctness_evidence\",\"value\":\"true\"}"
    ));
    assert!(output.contains(
        "{\"key\":\"metadata_projection_kernel_admission_requires_memory_safety_evidence\",\"value\":\"true\"}"
    ));
    assert!(output.contains(
        "{\"key\":\"metadata_projection_kernel_admission_requires_benchmark_for_production\",\"value\":\"true\"}"
    ));
    assert!(output.contains(
        "{\"key\":\"metadata_projection_kernel_admission_runtime_execution\",\"value\":\"false\"}"
    ));
    assert!(output.contains(
        "{\"key\":\"metadata_projection_kernel_admission_fallback_execution_allowed\",\"value\":\"false\"}"
    ));
}

fn assert_operator_discovery_encoded_count_kernel(output: &str) {
    assert!(output.contains(
        "{\"key\":\"encoded_count_physical_kernel_schema_version\",\"value\":\"shardloom.vortex_encoded_count_physical_kernel.v1\"}"
    ));
    assert!(output.contains(
        "{\"key\":\"encoded_count_physical_kernel_id\",\"value\":\"vortex.query-primitive.count_all.encoded-count-physical-kernel\"}"
    ));
    assert!(output.contains(
        "{\"key\":\"encoded_count_physical_kernel_supported_primitive\",\"value\":\"count_all\"}"
    ));
    assert!(output.contains(
        "{\"key\":\"encoded_count_physical_kernel_operator_kind\",\"value\":\"count_aggregate\"}"
    ));
    assert!(
        output.contains(
            "{\"key\":\"encoded_count_physical_kernel_kernel_kind\",\"value\":\"encoded\"}"
        )
    );
    assert!(output.contains(
        "{\"key\":\"encoded_count_physical_kernel_execution_level\",\"value\":\"encoded_native\"}"
    ));
    assert!(output.contains(
        "{\"key\":\"encoded_count_physical_kernel_contextual_only\",\"value\":\"true\"}"
    ));
    assert!(output.contains(
        "{\"key\":\"encoded_count_physical_kernel_requires_execution_certificate\",\"value\":\"true\"}"
    ));
    assert!(output.contains(
        "{\"key\":\"encoded_count_physical_kernel_requires_correctness_evidence\",\"value\":\"true\"}"
    ));
    assert!(output.contains(
        "{\"key\":\"encoded_count_physical_kernel_requires_memory_safety_evidence\",\"value\":\"true\"}"
    ));
    assert!(output.contains(
        "{\"key\":\"encoded_count_physical_kernel_requires_benchmark_for_production\",\"value\":\"true\"}"
    ));
    assert!(output.contains(
        "{\"key\":\"encoded_count_physical_kernel_discovery_reads_data\",\"value\":\"false\"}"
    ));
    assert!(output.contains(
        "{\"key\":\"encoded_count_physical_kernel_evaluated_path_reads_data\",\"value\":\"true\"}"
    ));
    assert!(output.contains(
        "{\"key\":\"encoded_count_physical_kernel_runtime_execution\",\"value\":\"false\"}"
    ));
    assert!(output.contains(
        "{\"key\":\"encoded_count_physical_kernel_fallback_execution_allowed\",\"value\":\"false\"}"
    ));
    assert!(output.contains(
        "{\"key\":\"encoded_count_kernel_admission_schema_version\",\"value\":\"shardloom.vortex_encoded_count_kernel_admission.v1\"}"
    ));
    assert!(output.contains(
        "{\"key\":\"encoded_count_kernel_admission_contextual_only\",\"value\":\"true\"}"
    ));
    assert!(output.contains(
        "{\"key\":\"encoded_count_kernel_admission_operator_kind\",\"value\":\"count_aggregate\"}"
    ));
    assert!(output.contains(
        "{\"key\":\"encoded_count_kernel_admission_required_kernel_kind\",\"value\":\"encoded\"}"
    ));
    assert!(output.contains(
        "{\"key\":\"encoded_count_kernel_admission_requires_physical_kernel_evidence\",\"value\":\"true\"}"
    ));
    assert!(output.contains(
        "{\"key\":\"encoded_count_kernel_admission_requires_correctness_evidence\",\"value\":\"true\"}"
    ));
    assert!(output.contains(
        "{\"key\":\"encoded_count_kernel_admission_requires_memory_safety_evidence\",\"value\":\"true\"}"
    ));
    assert!(output.contains(
        "{\"key\":\"encoded_count_kernel_admission_requires_benchmark_for_production\",\"value\":\"true\"}"
    ));
    assert!(output.contains(
        "{\"key\":\"encoded_count_kernel_admission_runtime_execution\",\"value\":\"false\"}"
    ));
    assert!(output.contains(
        "{\"key\":\"encoded_count_kernel_admission_fallback_execution_allowed\",\"value\":\"false\"}"
    ));
}

fn assert_operator_discovery_encoded_count_guard(output: &str) {
    assert!(output.contains(
        "{\"key\":\"encoded_count_local_guard_schema_version\",\"value\":\"shardloom.vortex_encoded_count_local_guard.v1\"}"
    ));
    assert!(output.contains(
        "{\"key\":\"encoded_count_local_guard_id\",\"value\":\"cg2.1e-layout-approved-count-local-guard\"}"
    ));
    assert!(output.contains(
        "{\"key\":\"encoded_count_local_guard_accepted_approval_sources\",\"value\":\"execution_usable_public_api_boundary,layout_row_count_approval,approved_local_scan_execution_report\"}"
    ));
    assert!(output.contains(
        "{\"key\":\"encoded_count_local_guard_local_execution_status\",\"value\":\"needs_encoded_read\"}"
    ));
    assert!(
        output.contains("{\"key\":\"encoded_count_local_guard_mode\",\"value\":\"plan_only\"}")
    );
    assert!(output.contains(
        "{\"key\":\"encoded_count_local_guard_layout_row_count_path_accepted\",\"value\":\"true\"}"
    ));
    assert!(output.contains(
        "{\"key\":\"encoded_count_local_guard_approved_local_scan_result_bridge_available\",\"value\":\"true\"}"
    ));
    assert!(output.contains(
        "{\"key\":\"encoded_count_local_guard_approved_local_scan_result_bridge_requires_executed_report\",\"value\":\"true\"}"
    ));
    assert!(output.contains(
        "{\"key\":\"encoded_count_local_guard_returns_count_result\",\"value\":\"false\"}"
    ));
    assert!(
        output.contains(
            "{\"key\":\"encoded_count_local_guard_side_effect_free\",\"value\":\"true\"}"
        )
    );
    assert!(
        output.contains("{\"key\":\"encoded_count_local_guard_data_read\",\"value\":\"false\"}")
    );
    assert!(
        output.contains(
            "{\"key\":\"encoded_count_local_guard_runtime_execution\",\"value\":\"false\"}"
        )
    );
    assert!(output.contains(
        "{\"key\":\"encoded_count_local_guard_fallback_execution_allowed\",\"value\":\"false\"}"
    ));
}

fn run_capabilities_scope(scope: &str) -> String {
    let output = Command::new(env!("CARGO_BIN_EXE_shardloom"))
        .args(["capabilities", scope, "--format", "json"])
        .output()
        .expect("shardloom binary executes");

    assert!(
        output.status.success(),
        "scope={scope} stdout={} stderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(
        output.stderr.is_empty(),
        "scope={scope} stderr={}",
        String::from_utf8_lossy(&output.stderr)
    );

    String::from_utf8(output.stdout).expect("stdout is utf8")
}

fn field_keys(output: &str) -> Vec<&str> {
    output
        .split("{\"key\":\"")
        .skip(1)
        .map(|part| {
            part.split_once('"').map_or_else(
                || panic!("field key terminator missing in {part}"),
                |(key, _)| key,
            )
        })
        .collect()
}

fn field_pair(key: &str, value: bool) -> String {
    format!("{{\"key\":\"{key}\",\"value\":\"{value}\"}}")
}
