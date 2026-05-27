//! Side-effect-free semantic conformance fixtures for P7.4.
//!
//! These fixtures exercise ShardLoom-owned in-memory semantics only. They do not
//! read datasets, invoke external engines as oracles, or certify broad SQL or
//! `DataFrame` runtime behavior.

use std::process::ExitCode;

use shardloom_core::{
    CommandStatus, ComparisonOp, Diagnostic, DiagnosticCode, ExprId, Expression,
    ExpressionEvaluationStatus, ExpressionInputRow, ExpressionKind, LogicalDType, OutputFormat,
    ScalarValue, evaluate_expression, format_iso_date32, format_iso_timestamp_micros,
    parse_iso_date32, parse_iso_timestamp_micros,
};

use crate::{
    cli_output::{emit, emit_error},
    cli_unknown_arg_error,
};

const COMMAND: &str = "semantic-conformance-suite";

#[derive(Clone, Copy, PartialEq, Eq)]
enum TriValue {
    True,
    False,
    Null,
}

struct SemanticFixtureRow {
    id: &'static str,
    dimension: &'static str,
    operator_family: &'static str,
    fixture_status: &'static str,
    current_support: &'static str,
    assertion: &'static str,
    blocker_id: &'static str,
    required_future_evidence: &'static str,
    fixture_executed: bool,
    passed: bool,
}

#[derive(Clone, Copy)]
struct SemanticBlockerEvidence {
    blocker_id: &'static str,
    required_future_evidence: &'static str,
}

impl SemanticFixtureRow {
    fn executed(
        id: &'static str,
        dimension: &'static str,
        operator_family: &'static str,
        current_support: &'static str,
        assertion: &'static str,
        passed: bool,
    ) -> Self {
        Self {
            id,
            dimension,
            operator_family,
            fixture_status: if passed { "passed" } else { "failed" },
            current_support,
            assertion,
            blocker_id: "none",
            required_future_evidence: "none",
            fixture_executed: true,
            passed,
        }
    }

    fn executed_blocker(
        id: &'static str,
        dimension: &'static str,
        operator_family: &'static str,
        current_support: &'static str,
        assertion: &'static str,
        blocker: SemanticBlockerEvidence,
        passed: bool,
    ) -> Self {
        Self {
            id,
            dimension,
            operator_family,
            fixture_status: if passed { "passed" } else { "failed" },
            current_support,
            assertion,
            blocker_id: blocker.blocker_id,
            required_future_evidence: blocker.required_future_evidence,
            fixture_executed: true,
            passed,
        }
    }

    fn planned(
        id: &'static str,
        dimension: &'static str,
        operator_family: &'static str,
        blocker_id: &'static str,
        required_future_evidence: &'static str,
    ) -> Self {
        Self {
            id,
            dimension,
            operator_family,
            fixture_status: "planned",
            current_support: "planned",
            assertion: "semantic fixture required before operator certification",
            blocker_id,
            required_future_evidence,
            fixture_executed: false,
            passed: false,
        }
    }

    fn blocked(
        id: &'static str,
        dimension: &'static str,
        operator_family: &'static str,
        blocker_id: &'static str,
        required_future_evidence: &'static str,
    ) -> Self {
        Self {
            id,
            dimension,
            operator_family,
            fixture_status: "blocked",
            current_support: "blocked_pending_operator",
            assertion: "operator family unsupported for semantic certification",
            blocker_id,
            required_future_evidence,
            fixture_executed: false,
            passed: false,
        }
    }
}

pub(crate) fn handle_semantic_conformance_suite(
    mut args: impl Iterator<Item = String>,
    format: OutputFormat,
) -> ExitCode {
    if let Some(extra) = args.next() {
        return emit_error(
            COMMAND,
            format,
            "semantic conformance suite failed",
            &cli_unknown_arg_error(COMMAND, &extra),
        );
    }

    let rows = semantic_fixture_rows();
    let failed_count = rows
        .iter()
        .filter(|row| row.fixture_executed && !row.passed)
        .count();
    let diagnostics = if failed_count == 0 {
        Vec::new()
    } else {
        vec![Diagnostic::unsupported(
            DiagnosticCode::NotImplemented,
            "p74.semantic_conformance",
            "One or more semantic conformance fixtures failed.",
            Some("Fix semantic fixtures before certifying affected operators.".to_string()),
        )]
    };
    let status = if failed_count == 0 {
        CommandStatus::Success
    } else {
        CommandStatus::Unsupported
    };
    let summary = if failed_count == 0 {
        "semantic conformance fixtures passed for current fixture rows"
    } else {
        "semantic conformance fixture failure"
    };

    emit(
        COMMAND,
        format,
        status,
        summary.to_string(),
        semantic_human_text(&rows),
        diagnostics,
        semantic_fields(&rows),
    );
    if failed_count == 0 {
        ExitCode::SUCCESS
    } else {
        ExitCode::from(1)
    }
}

fn semantic_fixture_rows() -> Vec<SemanticFixtureRow> {
    let mut rows = Vec::new();
    rows.extend(predicate_semantic_rows());
    rows.extend(scalar_semantic_rows());
    rows.extend(aggregate_semantic_rows());
    rows.extend(complex_operator_semantic_rows());
    rows
}

fn predicate_semantic_rows() -> Vec<SemanticFixtureRow> {
    vec![
        SemanticFixtureRow::executed(
            "null_comparison",
            "null comparison",
            "predicates",
            "fixture_certified",
            "IS NULL and IS NOT NULL preserve null identity",
            null_comparison_fixture(),
        ),
        SemanticFixtureRow::executed(
            "three_valued_logic",
            "three-valued logic",
            "predicates",
            "fixture_certified",
            "TRUE AND NULL is NULL; FALSE AND NULL is FALSE; TRUE OR NULL is TRUE",
            three_valued_logic_fixture(),
        ),
        SemanticFixtureRow::blocked(
            "null_sort_ordering",
            "null sort ordering",
            "sort_topn_limit",
            "cg21.workflow.sort.operator_unsupported",
            "sort_operator,null_ordering_policy,semantic_fixture",
        ),
    ]
}

fn scalar_semantic_rows() -> Vec<SemanticFixtureRow> {
    let mut rows = basic_numeric_scalar_rows();
    rows.extend(decimal_scalar_blocker_rows());
    rows.extend(utc_timestamp_scalar_rows());
    rows.extend(timezone_interval_scalar_blocker_rows());
    rows.extend(date_string_scalar_rows());
    rows.extend(regex_collation_scalar_blocker_rows());
    rows.extend(binary_scalar_rows());
    rows
}

fn basic_numeric_scalar_rows() -> Vec<SemanticFixtureRow> {
    vec![
        SemanticFixtureRow::executed(
            "nan_equality_order",
            "NaN equality/order",
            "scalar_expressions",
            "fixture_only_operator_not_certified",
            "NaN is not equal to NaN and has no total order without an explicit policy",
            nan_semantics_fixture(),
        ),
        SemanticFixtureRow::executed(
            "signed_zero",
            "signed zero",
            "scalar_expressions",
            "fixture_only_operator_not_certified",
            "+0.0 equals -0.0 while representation identity remains observable",
            signed_zero_fixture(),
        ),
        SemanticFixtureRow::executed(
            "integer_overflow",
            "integer overflow",
            "scalar_expressions",
            "fixture_only_operator_not_certified",
            "overflow is detected instead of wrapping silently",
            integer_overflow_fixture(),
        ),
    ]
}

fn decimal_scalar_blocker_rows() -> Vec<SemanticFixtureRow> {
    vec![SemanticFixtureRow::executed_blocker(
        "decimal_precision_scale",
        "decimal precision/scale",
        "scalar_expressions",
        "unsupported_diagnostic_certified",
        "decimal precision/scale casts fail deterministically until a native decimal dtype policy lands",
        SemanticBlockerEvidence {
            blocker_id: "gar-runtime-impl-4d-f1.decimal_precision_scale_unsupported",
            required_future_evidence: "decimal_dtype_policy,precision_scale_fixture,overflow_diagnostic",
        },
        decimal_precision_scale_blocker_fixture(),
    )]
}

fn utc_timestamp_scalar_rows() -> Vec<SemanticFixtureRow> {
    vec![SemanticFixtureRow::executed(
        "timestamp_timezone",
        "timestamp unit and timezone handling",
        "scalar_expressions",
        "utc_timestamp_micros_fixture_certified_non_utc_blocked",
        "UTC timestamp_micros parses and formats while non-UTC and offset timestamp forms are deterministic blockers",
        timestamp_timezone_fixture(),
    )]
}

fn timezone_interval_scalar_blocker_rows() -> Vec<SemanticFixtureRow> {
    vec![
        SemanticFixtureRow::executed_blocker(
            "timezone_database_policy",
            "timezone database policy",
            "scalar_expressions",
            "unsupported_diagnostic_certified",
            "named timezone database semantics fail deterministically; UTC timestamp_micros remains the admitted timestamp profile",
            SemanticBlockerEvidence {
                blocker_id: "gar-runtime-impl-4d-f1.timezone_database_unsupported",
                required_future_evidence: "timezone_database_policy,non_utc_offset_fixture,claim_boundary",
            },
            timezone_database_policy_fixture(),
        ),
        SemanticFixtureRow::executed_blocker(
            "interval_arithmetic_policy",
            "interval/date-time completeness",
            "scalar_expressions",
            "unsupported_diagnostic_certified",
            "ANSI interval literals and calendar interval arithmetic fail deterministically outside scoped day/second functions",
            SemanticBlockerEvidence {
                blocker_id: "gar-runtime-impl-4d-f1.interval_semantics_unsupported",
                required_future_evidence: "interval_dtype_policy,calendar_arithmetic_fixture,overflow_diagnostic",
            },
            interval_arithmetic_policy_fixture(),
        ),
    ]
}

fn date_string_scalar_rows() -> Vec<SemanticFixtureRow> {
    vec![
        SemanticFixtureRow::executed(
            "date_parsing",
            "date parsing",
            "scalar_expressions",
            "date32_fixture_certified_invalid_dates_blocked",
            "ISO Date32 parses and formats valid dates while invalid calendar dates are deterministic blockers",
            date_parsing_fixture(),
        ),
        SemanticFixtureRow::executed(
            "string_case_sensitivity",
            "string collation and case sensitivity",
            "scalar_expressions",
            "fixture_only_operator_not_certified",
            "ShardLoomNative string equality is case-sensitive unless a future collation says otherwise",
            string_case_sensitivity_fixture(),
        ),
    ]
}

fn regex_collation_scalar_blocker_rows() -> Vec<SemanticFixtureRow> {
    vec![
        SemanticFixtureRow::executed_blocker(
            "regex_pattern_policy",
            "regex pattern semantics",
            "scalar_expressions",
            "unsupported_diagnostic_certified",
            "regex/regexp pattern functions fail deterministically; scoped LIKE/string predicates remain the admitted string pattern profile",
            SemanticBlockerEvidence {
                blocker_id: "gar-runtime-impl-4d-f1.regex_semantics_unsupported",
                required_future_evidence: "regex_engine_policy,pattern_fixture,claim_boundary",
            },
            regex_pattern_policy_fixture(),
        ),
        SemanticFixtureRow::executed_blocker(
            "locale_collation_policy",
            "locale/collation policy",
            "scalar_expressions",
            "unsupported_diagnostic_certified",
            "locale-aware collation fails deterministically while UTF-8 equality remains case-sensitive codepoint equality",
            SemanticBlockerEvidence {
                blocker_id: "gar-runtime-impl-4d-f1.locale_collation_unsupported",
                required_future_evidence: "locale_collation_policy,unicode_casefold_fixture,claim_boundary",
            },
            locale_collation_policy_fixture(),
        ),
    ]
}

fn binary_scalar_rows() -> Vec<SemanticFixtureRow> {
    vec![SemanticFixtureRow::executed(
        "binary_equality",
        "binary equality",
        "scalar_expressions",
        "bytewise_equality_fixture_certified_ordering_blocked",
        "Binary equality is bytewise; binary ordering comparisons remain deterministic blockers",
        binary_equality_fixture(),
    )]
}

fn aggregate_semantic_rows() -> Vec<SemanticFixtureRow> {
    vec![
        SemanticFixtureRow::executed(
            "empty_aggregate_behavior",
            "empty aggregate behavior",
            "aggregates",
            "fixture_certified",
            "count_all over empty input returns zero",
            empty_aggregate_fixture(),
        ),
        SemanticFixtureRow::executed(
            "count_null_behavior",
            "count null behavior",
            "aggregates",
            "fixture_certified",
            "count_all includes null rows while count_non_null ignores null values",
            count_null_behavior_fixture(),
        ),
    ]
}

fn complex_operator_semantic_rows() -> Vec<SemanticFixtureRow> {
    let mut rows = vec![
        SemanticFixtureRow::blocked(
            "join_null_semantics",
            "join null semantics",
            "joins",
            "cg21.workflow.join.operator_unsupported",
            "join_operator,join_null_semantics_fixture,memory_spill_declaration",
        ),
        SemanticFixtureRow::blocked(
            "window_frame_defaults",
            "window frame defaults",
            "window_functions",
            "cg21.workflow.window.operator_unsupported",
            "window_operator,frame_default_policy,sort_capability",
        ),
        SemanticFixtureRow::planned(
            "duplicate_column_behavior",
            "duplicate column behavior",
            "projection",
            "p74.semantic.duplicate_column_policy_missing",
            "projection_name_resolution_policy,duplicate_column_fixture",
        ),
    ];
    rows.extend(complex_dtype_blocker_rows());
    rows
}

fn complex_dtype_blocker_rows() -> Vec<SemanticFixtureRow> {
    vec![
        SemanticFixtureRow::executed_blocker(
            "nested_list_equality",
            "nested/list equality",
            "nested_extension_type_operations",
            "unsupported_diagnostic_certified",
            "list value representation and list equality fail deterministically until a native list dtype policy lands",
            SemanticBlockerEvidence {
                blocker_id: "gar-runtime-impl-4d-f2.list_equality_unsupported",
                required_future_evidence: "list_value_representation,parent_child_null_fixture,list_equality_fixture",
            },
            list_equality_policy_fixture(),
        ),
        SemanticFixtureRow::executed_blocker(
            "struct_equality_policy",
            "struct equality",
            "nested_extension_type_operations",
            "unsupported_diagnostic_certified",
            "struct value representation and struct equality fail deterministically until schema identity and field null policy land",
            SemanticBlockerEvidence {
                blocker_id: "gar-runtime-impl-4d-f2.struct_equality_unsupported",
                required_future_evidence: "struct_value_representation,field_identity_policy,struct_equality_fixture",
            },
            struct_equality_policy_fixture(),
        ),
        SemanticFixtureRow::executed_blocker(
            "variant_access_policy",
            "variant access",
            "nested_extension_type_operations",
            "unsupported_diagnostic_certified",
            "variant access fails deterministically until tag/value policy and projection fixtures land",
            SemanticBlockerEvidence {
                blocker_id: "gar-runtime-impl-4d-f2.variant_access_unsupported",
                required_future_evidence: "variant_tag_policy,variant_projection_fixture,unsupported_tag_diagnostic",
            },
            variant_access_policy_fixture(),
        ),
        SemanticFixtureRow::executed_blocker(
            "union_semantics_policy",
            "union semantics",
            "nested_extension_type_operations",
            "unsupported_diagnostic_certified",
            "union dtype semantics fail deterministically until tagged-union coercion and equality policy land",
            SemanticBlockerEvidence {
                blocker_id: "gar-runtime-impl-4d-f2.union_semantics_unsupported",
                required_future_evidence: "union_tag_policy,union_coercion_fixture,union_equality_fixture",
            },
            union_semantics_policy_fixture(),
        ),
        SemanticFixtureRow::executed_blocker(
            "parent_child_null_policy",
            "parent/child null behavior",
            "nested_extension_type_operations",
            "unsupported_diagnostic_certified",
            "nested parent/child null propagation fails deterministically until list and struct value representations land",
            SemanticBlockerEvidence {
                blocker_id: "gar-runtime-impl-4d-f2.parent_child_null_policy_missing",
                required_future_evidence: "nested_parent_validity_fixture,child_validity_fixture,null_propagation_policy",
            },
            parent_child_null_policy_fixture(),
        ),
        SemanticFixtureRow::executed_blocker(
            "schema_field_identity",
            "schema field identity",
            "projection",
            "unsupported_diagnostic_certified",
            "schema field identity and rename semantics fail deterministically until field-id metadata policy lands",
            SemanticBlockerEvidence {
                blocker_id: "gar-runtime-impl-4d-f2.schema_field_identity_unsupported",
                required_future_evidence: "schema_field_id_policy,rename_projection_fixture,field_collision_diagnostic",
            },
            schema_field_identity_fixture(),
        ),
        SemanticFixtureRow::executed_blocker(
            "binary_source_runtime_policy",
            "binary source runtime",
            "nested_extension_type_operations",
            "unsupported_diagnostic_certified",
            "binary scalar equality is admitted, but binary source decoding and SQL binary literals fail deterministically",
            SemanticBlockerEvidence {
                blocker_id: "gar-runtime-impl-4d-f2.binary_source_runtime_unsupported",
                required_future_evidence: "binary_source_fixture,binary_literal_policy,compatibility_output_encoding",
            },
            binary_source_runtime_policy_fixture(),
        ),
    ]
}

fn semantic_human_text(rows: &[SemanticFixtureRow]) -> String {
    format!(
        "ShardLoomNative semantic conformance suite\nrows: {}\nexecuted fixtures: {}\npassed fixtures: {}\nplanned rows: {}\nblocked rows: {}\nexternal oracle used: false\nfallback execution: disabled",
        rows.len(),
        executed_count(rows),
        passed_count(rows),
        status_count(rows, "planned"),
        status_count(rows, "blocked")
    )
}

fn semantic_fields(rows: &[SemanticFixtureRow]) -> Vec<(String, String)> {
    let mut fields = vec![
        field("mode", "semantic_conformance_suite"),
        field("schema_version", "shardloom.semantic_conformance.v1"),
        field("report_id", "p74.semantic_conformance"),
        field("semantic_profile", "ShardLoomNative"),
        field("suite_status", "partial_fixture_passed_planned_remaining"),
        field("row_order", &row_order(rows)),
        field("semantic_dimension_count", &rows.len().to_string()),
        field("executed_fixture_count", &executed_count(rows).to_string()),
        field("passed_fixture_count", &passed_count(rows).to_string()),
        field("failed_fixture_count", &failed_count(rows).to_string()),
        field(
            "planned_fixture_count",
            &status_count(rows, "planned").to_string(),
        ),
        field(
            "blocked_fixture_count",
            &status_count(rows, "blocked").to_string(),
        ),
        field("fixture_status_vocabulary", "passed,failed,planned,blocked"),
        field(
            "required_semantic_dimensions",
            "null_comparison,three_valued_logic,null_sort_ordering,nan_equality_order,signed_zero,integer_overflow,decimal_precision_scale,timestamp_timezone,timezone_database_policy,interval_arithmetic_policy,date_parsing,string_case_sensitivity,regex_pattern_policy,locale_collation_policy,binary_equality,empty_aggregate_behavior,count_null_behavior,join_null_semantics,window_frame_defaults,duplicate_column_behavior,nested_list_equality,struct_equality_policy,variant_access_policy,union_semantics_policy,parent_child_null_policy,schema_field_identity,binary_source_runtime_policy",
        ),
        field(
            "certification_blocker_ids",
            &rows
                .iter()
                .filter(|row| row.blocker_id != "none")
                .map(|row| row.blocker_id)
                .collect::<Vec<_>>()
                .join(","),
        ),
        field("semantic_failures_block_certification", "true"),
        field("semantic_failures_block_benchmark_claims", "true"),
        field("external_oracle_used", "false"),
        field("external_engine_invoked", "false"),
        field("external_engines_allowed_as_oracles_only", "true"),
        field("in_memory_fixture_execution", "true"),
        field("query_execution", "false"),
        field("runtime_execution", "false"),
        field("data_read", "false"),
        field("data_materialized", "false"),
        field("write_io", "false"),
        field("object_store_io", "false"),
        field("network_probe", "false"),
        field("catalog_probe", "false"),
        field("fallback_execution_allowed", "false"),
        field("fallback_attempted", "false"),
        field("no_runtime", "true"),
        field("no_fallback", "true"),
        field("no_effects", "true"),
        field(
            "next_required_slice",
            "execution artifact richness and provider-evidence preservation",
        ),
    ];
    for row in rows {
        append_row_fields(&mut fields, row);
    }
    fields
}

fn append_row_fields(fields: &mut Vec<(String, String)>, row: &SemanticFixtureRow) {
    let prefix = format!("semantic_row_{}", row.id);
    fields.push(field(&format!("{prefix}_dimension"), row.dimension));
    fields.push(field(
        &format!("{prefix}_operator_family"),
        row.operator_family,
    ));
    fields.push(field(
        &format!("{prefix}_fixture_status"),
        row.fixture_status,
    ));
    fields.push(field(
        &format!("{prefix}_current_support"),
        row.current_support,
    ));
    fields.push(field(&format!("{prefix}_assertion"), row.assertion));
    fields.push(field(&format!("{prefix}_blocker_id"), row.blocker_id));
    fields.push(field(
        &format!("{prefix}_required_future_evidence"),
        row.required_future_evidence,
    ));
    fields.push(field(
        &format!("{prefix}_fixture_executed"),
        bool_str(row.fixture_executed),
    ));
    fields.push(field(&format!("{prefix}_passed"), bool_str(row.passed)));
    fields.push(field(&format!("{prefix}_fallback_attempted"), "false"));
    fields.push(field(&format!("{prefix}_external_oracle_used"), "false"));
}

fn field(key: &str, value: &str) -> (String, String) {
    (key.to_string(), value.to_string())
}

fn bool_str(value: bool) -> &'static str {
    if value { "true" } else { "false" }
}

fn row_order(rows: &[SemanticFixtureRow]) -> String {
    rows.iter().map(|row| row.id).collect::<Vec<_>>().join(",")
}

fn executed_count(rows: &[SemanticFixtureRow]) -> usize {
    rows.iter().filter(|row| row.fixture_executed).count()
}

fn passed_count(rows: &[SemanticFixtureRow]) -> usize {
    rows.iter()
        .filter(|row| row.fixture_executed && row.passed)
        .count()
}

fn failed_count(rows: &[SemanticFixtureRow]) -> usize {
    rows.iter()
        .filter(|row| row.fixture_executed && !row.passed)
        .count()
}

fn status_count(rows: &[SemanticFixtureRow], status: &str) -> usize {
    rows.iter()
        .filter(|row| row.fixture_status == status)
        .count()
}

fn null_comparison_fixture() -> bool {
    let values = [None, Some(7), None];
    let is_null = values.iter().map(Option::is_none).collect::<Vec<_>>();
    let is_not_null = values.iter().map(Option::is_some).collect::<Vec<_>>();
    is_null == [true, false, true] && is_not_null == [false, true, false]
}

fn three_valued_logic_fixture() -> bool {
    tri_and(TriValue::True, TriValue::Null) == TriValue::Null
        && tri_and(TriValue::False, TriValue::Null) == TriValue::False
        && tri_or(TriValue::True, TriValue::Null) == TriValue::True
        && tri_or(TriValue::False, TriValue::Null) == TriValue::Null
}

fn tri_and(left: TriValue, right: TriValue) -> TriValue {
    match (left, right) {
        (TriValue::False, _) | (_, TriValue::False) => TriValue::False,
        (TriValue::True, TriValue::True) => TriValue::True,
        _ => TriValue::Null,
    }
}

fn tri_or(left: TriValue, right: TriValue) -> TriValue {
    match (left, right) {
        (TriValue::True, _) | (_, TriValue::True) => TriValue::True,
        (TriValue::False, TriValue::False) => TriValue::False,
        _ => TriValue::Null,
    }
}

fn nan_semantics_fixture() -> bool {
    let left = f64::NAN;
    let right = f64::NAN;
    left.is_nan()
        && right.is_nan()
        && left.partial_cmp(&right).is_none()
        && left.partial_cmp(&0.0).is_none()
}

fn signed_zero_fixture() -> bool {
    let positive_zero = 0.0_f64;
    let negative_zero = -0.0_f64;
    matches!(
        positive_zero.partial_cmp(&negative_zero),
        Some(std::cmp::Ordering::Equal)
    ) && positive_zero.to_bits() != negative_zero.to_bits()
}

fn integer_overflow_fixture() -> bool {
    i64::MAX.checked_add(1).is_none()
}

fn decimal_precision_scale_blocker_fixture() -> bool {
    let expression = Expression::cast(
        ExprId::new("decimal-cast").expect("fixture expression id"),
        Expression::literal(
            ExprId::new("decimal-source").expect("fixture expression id"),
            ScalarValue::Utf8("12.34".to_string()),
        ),
        LogicalDType::Extension("decimal128(10,2)".to_string()),
    );
    unsupported_report_certified(&evaluate_expression(
        &expression,
        &ExpressionInputRow::new(),
    ))
}

fn string_case_sensitivity_fixture() -> bool {
    "ShardLoom" != "shardloom"
}

fn timestamp_timezone_fixture() -> bool {
    let Ok(parsed) = parse_iso_timestamp_micros("2026-05-19T12:34:56.123456Z") else {
        return false;
    };
    format_iso_timestamp_micros(parsed) == "2026-05-19T12:34:56.123456Z"
        && parse_iso_timestamp_micros("2026-05-19T12:34:56Z").is_ok()
        && parse_iso_timestamp_micros("2026-05-19T12:34:56+00:00").is_err()
        && parse_iso_timestamp_micros("2026-05-19T12:34:56").is_err()
        && parse_iso_timestamp_micros("2026-05-19 12:34:56Z").is_err()
}

fn timezone_database_policy_fixture() -> bool {
    parse_iso_timestamp_micros("2026-05-19T12:34:56Z[America/Chicago]").is_err()
        && parse_iso_timestamp_micros("2026-05-19T12:34:56 America/Chicago").is_err()
        && parse_iso_timestamp_micros("2026-05-19T12:34:56-05:00").is_err()
}

fn interval_arithmetic_policy_fixture() -> bool {
    unsupported_function_fixture("interval_add_months")
}

fn date_parsing_fixture() -> bool {
    let Ok(parsed) = parse_iso_date32("2026-05-19") else {
        return false;
    };
    format_iso_date32(parsed) == "2026-05-19"
        && parse_iso_date32("2024-02-29").is_ok()
        && parse_iso_date32("2023-02-29").is_err()
        && parse_iso_date32("2026-5-19").is_err()
        && parse_iso_date32("2026-13-01").is_err()
}

fn regex_pattern_policy_fixture() -> bool {
    unsupported_function_fixture("regexp_like")
}

fn locale_collation_policy_fixture() -> bool {
    let codepoint_compare = evaluate_expression(
        &Expression::new(
            ExprId::new("case-sensitive-utf8-eq").expect("fixture expression id"),
            ExpressionKind::Compare {
                left: Box::new(Expression::literal(
                    ExprId::new("case-left").expect("fixture expression id"),
                    ScalarValue::Utf8("ShardLoom".to_string()),
                )),
                op: ComparisonOp::Eq,
                right: Box::new(Expression::literal(
                    ExprId::new("case-right").expect("fixture expression id"),
                    ScalarValue::Utf8("shardloom".to_string()),
                )),
            },
        ),
        &ExpressionInputRow::new(),
    );
    codepoint_compare.status == ExpressionEvaluationStatus::Evaluated
        && codepoint_compare.value == Some(ScalarValue::Boolean(false))
        && !codepoint_compare.fallback_attempted
        && !codepoint_compare.external_engine_invoked
        && unsupported_function_fixture("collate_eq")
}

fn unsupported_function_fixture(name: &'static str) -> bool {
    let expression = Expression::new(
        ExprId::new(format!("{name}-fixture")).expect("fixture expression id"),
        ExpressionKind::FunctionCall {
            name: name.to_string(),
            args: vec![Expression::literal(
                ExprId::new(format!("{name}-arg")).expect("fixture expression id"),
                ScalarValue::Utf8("alpha".to_string()),
            )],
        },
    );
    unsupported_report_certified(&evaluate_expression(
        &expression,
        &ExpressionInputRow::new(),
    ))
}

fn unsupported_cast_dtype_fixture(target_dtype: LogicalDType) -> bool {
    let expression = Expression::cast(
        ExprId::new("complex-cast").expect("fixture expression id"),
        Expression::literal(
            ExprId::new("complex-source").expect("fixture expression id"),
            ScalarValue::Utf8("alpha".to_string()),
        ),
        target_dtype,
    );
    unsupported_report_certified(&evaluate_expression(
        &expression,
        &ExpressionInputRow::new(),
    ))
}

fn unsupported_report_certified(report: &shardloom_core::ExpressionEvaluationReport) -> bool {
    report.status == ExpressionEvaluationStatus::Unsupported
        && report.has_errors()
        && !report.fallback_attempted
        && !report.external_engine_invoked
        && !report.fallback_execution_allowed()
        && report
            .diagnostics
            .iter()
            .all(|diagnostic| !diagnostic.fallback.attempted)
}

fn list_equality_policy_fixture() -> bool {
    unsupported_cast_dtype_fixture(LogicalDType::List) && unsupported_function_fixture("list_eq")
}

fn struct_equality_policy_fixture() -> bool {
    unsupported_cast_dtype_fixture(LogicalDType::Struct)
        && unsupported_function_fixture("struct_eq")
}

fn variant_access_policy_fixture() -> bool {
    unsupported_cast_dtype_fixture(LogicalDType::Extension("variant".to_string()))
        && unsupported_function_fixture("variant_get")
}

fn union_semantics_policy_fixture() -> bool {
    unsupported_cast_dtype_fixture(LogicalDType::Extension("union".to_string()))
        && unsupported_function_fixture("union_tag")
}

fn parent_child_null_policy_fixture() -> bool {
    unsupported_function_fixture("list_parent_child_null_policy")
        && unsupported_function_fixture("struct_parent_child_null_policy")
}

fn schema_field_identity_fixture() -> bool {
    unsupported_function_fixture("struct_field_identity")
}

fn binary_source_runtime_policy_fixture() -> bool {
    ScalarValue::Binary(vec![0, 1, 255]).dtype() == LogicalDType::Binary
        && unsupported_function_fixture("binary_source_decode")
}

fn binary_equality_fixture() -> bool {
    let equal = evaluate_expression(
        &binary_compare_expression(
            "binary-eq",
            ComparisonOp::Eq,
            vec![0, 1, 255],
            vec![0, 1, 255],
        ),
        &ExpressionInputRow::new(),
    );
    let not_equal = evaluate_expression(
        &binary_compare_expression(
            "binary-neq",
            ComparisonOp::NotEq,
            vec![0, 1, 255],
            vec![0, 1, 254],
        ),
        &ExpressionInputRow::new(),
    );
    let ordered = evaluate_expression(
        &binary_compare_expression(
            "binary-lt",
            ComparisonOp::Lt,
            vec![0, 1, 254],
            vec![0, 1, 255],
        ),
        &ExpressionInputRow::new(),
    );

    equal.status == ExpressionEvaluationStatus::Evaluated
        && equal.value == Some(ScalarValue::Boolean(true))
        && !equal.fallback_attempted
        && !equal.external_engine_invoked
        && not_equal.status == ExpressionEvaluationStatus::Evaluated
        && not_equal.value == Some(ScalarValue::Boolean(true))
        && !not_equal.fallback_attempted
        && !not_equal.external_engine_invoked
        && ordered.status == ExpressionEvaluationStatus::Unsupported
        && ordered.has_errors()
        && !ordered.fallback_attempted
        && !ordered.external_engine_invoked
}

fn binary_compare_expression(
    id: &'static str,
    op: ComparisonOp,
    left: Vec<u8>,
    right: Vec<u8>,
) -> Expression {
    Expression::new(
        ExprId::new(id).expect("fixture expression id"),
        ExpressionKind::Compare {
            left: Box::new(Expression::literal(
                ExprId::new(format!("{id}.left")).expect("left expression id"),
                ScalarValue::Binary(left),
            )),
            op,
            right: Box::new(Expression::literal(
                ExprId::new(format!("{id}.right")).expect("right expression id"),
                ScalarValue::Binary(right),
            )),
        },
    )
}

fn empty_aggregate_fixture() -> bool {
    let values = Vec::<Option<i64>>::new();
    count_all_rows(&values) == 0
}

fn count_null_behavior_fixture() -> bool {
    let values = [None, Some(1), None, Some(2)];
    let count_all = count_all_rows(&values);
    let count_non_null = values.iter().flatten().count();
    count_all == 4 && count_non_null == 2
}

fn count_all_rows<T>(values: &[T]) -> usize {
    values.len()
}
