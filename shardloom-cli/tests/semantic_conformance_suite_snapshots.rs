use std::process::Command;

fn run_semantic_suite_json(args: &[&str], expect_success: bool) -> String {
    let output = Command::new(env!("CARGO_BIN_EXE_shardloom"))
        .args(args)
        .output()
        .expect("shardloom command runs");

    assert_eq!(
        output.status.success(),
        expect_success,
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
fn semantic_conformance_suite_executes_current_fixtures_without_fallback() {
    let output = run_semantic_suite_json(&["semantic-conformance-suite", "--format", "json"], true);

    assert!(output.contains("\"command\":\"semantic-conformance-suite\""));
    assert!(output.contains("\"status\":\"success\""));
    assert!(output.contains(&field("mode", "semantic_conformance_suite")));
    assert!(output.contains(&field(
        "schema_version",
        "shardloom.semantic_conformance.v1"
    )));
    assert!(output.contains(&field("semantic_profile", "ShardLoomNative")));
    assert!(output.contains(&field(
        "suite_status",
        "partial_fixture_passed_planned_remaining"
    )));
    assert!(output.contains(&field("semantic_dimension_count", "27")));
    assert!(output.contains(&field("executed_fixture_count", "23")));
    assert!(output.contains(&field("passed_fixture_count", "23")));
    assert!(output.contains(&field("failed_fixture_count", "0")));
    assert!(output.contains(&field("planned_fixture_count", "1")));
    assert!(output.contains(&field("blocked_fixture_count", "3")));
    assert!(output.contains(&field("in_memory_fixture_execution", "true")));
    assert!(output.contains(&field("external_oracle_used", "false")));
    assert!(output.contains(&field("external_engine_invoked", "false")));
    assert!(output.contains(&field("query_execution", "false")));
    assert!(output.contains(&field("runtime_execution", "false")));
    assert!(output.contains(&field("data_read", "false")));
    assert!(output.contains(&field("write_io", "false")));
    assert!(output.contains(&field("fallback_execution_allowed", "false")));
    assert!(output.contains(&field("fallback_attempted", "false")));
    assert!(output.contains(&field("no_runtime", "true")));
    assert!(output.contains(&field("no_fallback", "true")));
    assert!(output.contains(&field("no_effects", "true")));
    assert!(output.contains(&field(
        "next_required_slice",
        "execution artifact richness and provider-evidence preservation"
    )));
    assert!(output.contains("\"artifact_kind\":\"semantic_conformance_report\""));
    assert!(output.contains("\"fallback\":{\"attempted\":false,\"allowed\":false"));
}

#[test]
#[allow(clippy::too_many_lines)]
fn semantic_conformance_suite_rows_cover_required_dimensions_and_blockers() {
    let output = run_semantic_suite_json(&["semantic-conformance-suite", "--format", "json"], true);

    assert!(output.contains(&field(
        "row_order",
        "null_comparison,three_valued_logic,null_sort_ordering,nan_equality_order,signed_zero,integer_overflow,decimal_precision_scale,timestamp_timezone,timezone_database_policy,interval_arithmetic_policy,date_parsing,string_case_sensitivity,regex_pattern_policy,locale_collation_policy,binary_equality,empty_aggregate_behavior,count_null_behavior,join_null_semantics,window_frame_defaults,duplicate_column_behavior,nested_list_equality,struct_equality_policy,variant_access_policy,union_semantics_policy,parent_child_null_policy,schema_field_identity,binary_source_runtime_policy"
    )));
    assert!(output.contains(&field(
        "semantic_row_null_comparison_fixture_status",
        "passed"
    )));
    assert!(output.contains(&field(
        "semantic_row_null_comparison_operator_family",
        "predicates"
    )));
    assert!(output.contains(&field(
        "semantic_row_three_valued_logic_fixture_executed",
        "true"
    )));
    assert!(output.contains(&field(
        "semantic_row_nan_equality_order_current_support",
        "fixture_only_operator_not_certified"
    )));
    assert!(output.contains(&field(
        "semantic_row_integer_overflow_assertion",
        "overflow is detected instead of wrapping silently"
    )));
    assert!(output.contains(&field(
        "semantic_row_string_case_sensitivity_passed",
        "true"
    )));
    assert!(output.contains(&field(
        "semantic_row_timestamp_timezone_current_support",
        "utc_timestamp_micros_fixture_certified_non_utc_blocked"
    )));
    assert!(output.contains(&field(
        "semantic_row_decimal_precision_scale_current_support",
        "scoped_fixture_certified"
    )));
    assert!(output.contains(&field(
        "semantic_row_decimal_precision_scale_blocker_id",
        "none"
    )));
    assert!(output.contains(&field(
        "semantic_row_timezone_database_policy_passed",
        "true"
    )));
    assert!(output.contains(&field(
        "semantic_row_interval_arithmetic_policy_blocker_id",
        "gar-runtime-impl-4d-f1.interval_semantics_unsupported"
    )));
    assert!(output.contains(&field("semantic_row_date_parsing_fixture_status", "passed")));
    assert!(output.contains(&field(
        "semantic_row_regex_pattern_policy_current_support",
        "scoped_utf8_regex_predicate_fixture_certified_locale_collation_blocked"
    )));
    assert!(output.contains(&field("semantic_row_regex_pattern_policy_passed", "true")));
    assert!(output.contains(&field(
        "semantic_row_locale_collation_policy_passed",
        "true"
    )));
    assert!(output.contains(&field(
        "semantic_row_binary_equality_current_support",
        "bytewise_equality_ordering_fixture_certified"
    )));
    assert!(output.contains(&field("semantic_row_binary_equality_passed", "true")));
    assert!(output.contains(&field(
        "semantic_row_nested_list_equality_current_support",
        "unsupported_diagnostic_certified"
    )));
    assert!(output.contains(&field(
        "semantic_row_nested_list_equality_blocker_id",
        "gar-runtime-impl-4d-f2.list_equality_unsupported"
    )));
    assert!(output.contains(&field("semantic_row_struct_equality_policy_passed", "true")));
    assert!(output.contains(&field(
        "semantic_row_struct_equality_policy_blocker_id",
        "gar-runtime-impl-4d-f2.struct_equality_unsupported"
    )));
    assert!(output.contains(&field(
        "semantic_row_variant_access_policy_current_support",
        "unsupported_diagnostic_certified"
    )));
    assert!(output.contains(&field(
        "semantic_row_variant_access_policy_blocker_id",
        "gar-runtime-impl-4d-f2.variant_access_unsupported"
    )));
    assert!(output.contains(&field("semantic_row_union_semantics_policy_passed", "true")));
    assert!(output.contains(&field(
        "semantic_row_union_semantics_policy_blocker_id",
        "gar-runtime-impl-4d-f2.union_semantics_unsupported"
    )));
    assert!(output.contains(&field(
        "semantic_row_parent_child_null_policy_blocker_id",
        "gar-runtime-impl-4d-f2.parent_child_null_policy_missing"
    )));
    assert!(output.contains(&field(
        "semantic_row_schema_field_identity_blocker_id",
        "gar-runtime-impl-4d-f2.schema_field_identity_unsupported"
    )));
    assert!(output.contains(&field(
        "semantic_row_binary_source_runtime_policy_blocker_id",
        "gar-runtime-impl-4d-f2.binary_source_runtime_unsupported"
    )));
    assert!(output.contains(&field(
        "semantic_row_binary_source_runtime_policy_passed",
        "true"
    )));
    assert!(output.contains(&field(
        "semantic_row_null_sort_ordering_blocker_id",
        "cg21.workflow.sort.operator_unsupported"
    )));
    assert!(output.contains(&field(
        "semantic_row_join_null_semantics_blocker_id",
        "cg21.workflow.join.operator_unsupported"
    )));
    assert!(output.contains(&field(
        "semantic_row_window_frame_defaults_blocker_id",
        "cg21.workflow.window.operator_unsupported"
    )));
    assert!(output.contains(&field(
        "semantic_row_decimal_precision_scale_required_future_evidence",
        "none"
    )));
}

#[test]
fn semantic_conformance_suite_rejects_extra_arguments() {
    let output = run_semantic_suite_json(
        &["semantic-conformance-suite", "extra", "--format", "json"],
        false,
    );

    assert!(output.contains("\"command\":\"semantic-conformance-suite\""));
    assert!(output.contains("\"status\":\"error\""));
    assert!(output.contains("semantic-conformance-suite unknown argument/value: extra"));
}
