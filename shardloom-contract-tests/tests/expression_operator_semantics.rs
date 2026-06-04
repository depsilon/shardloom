use shardloom_core::{
    BinaryOp, ColumnRef, ComparisonOp, ExprId, Expression, ExpressionEvaluationStatus,
    ExpressionInputRow, ExpressionKind, LogicalDType, NullBehavior, ScalarValue, UnaryOp,
    date32_day, date32_month, date32_year, decimal128_dtype, evaluate_expression, evaluate_filter,
    evaluate_limit, evaluate_projection, format_iso_date32, format_iso_timestamp_micros,
    parse_iso_date32, parse_iso_timestamp_micros, timestamp_micros_day, timestamp_micros_hour,
    timestamp_micros_minute, timestamp_micros_month, timestamp_micros_second,
    timestamp_micros_year,
};

fn expr_id(value: &str) -> ExprId {
    ExprId::new(value).expect("expression id")
}

fn col(value: &str) -> ColumnRef {
    ColumnRef::new(value).expect("column")
}

fn row(values: &[(&str, ScalarValue)]) -> ExpressionInputRow {
    values
        .iter()
        .map(|(name, value)| ((*name).to_string(), value.clone()))
        .collect()
}

#[test]
fn expression_semantics_baseline_declares_no_fallback_evidence() {
    let expression = Expression::new(
        expr_id("predicate"),
        ExpressionKind::Binary {
            left: Box::new(Expression::new(
                expr_id("not-null"),
                ExpressionKind::Unary {
                    op: UnaryOp::IsNotNull,
                    expr: Box::new(Expression::column(expr_id("amount"), col("amount"))),
                },
            )),
            op: BinaryOp::And,
            right: Box::new(Expression::new(
                expr_id("gte"),
                ExpressionKind::Compare {
                    left: Box::new(Expression::column(expr_id("amount-ref"), col("amount"))),
                    op: ComparisonOp::GtEq,
                    right: Box::new(Expression::literal(expr_id("min"), ScalarValue::Int64(10))),
                },
            )),
        },
    );

    let report = evaluate_expression(&expression, &row(&[("amount", ScalarValue::Int64(12))]));

    assert_eq!(report.schema_version, "shardloom.expression_semantics.v1");
    assert_eq!(report.status, ExpressionEvaluationStatus::Evaluated);
    assert_eq!(report.value, Some(ScalarValue::Boolean(true)));
    assert_eq!(report.output_dtype, Some(LogicalDType::Boolean));
    assert_eq!(report.null_behavior, NullBehavior::NullAware);
    assert_eq!(report.claim_gate_status, "not_claim_grade");
    assert!(!report.fallback_attempted);
    assert!(!report.external_engine_invoked);
    assert!(!report.fallback_execution_allowed());
    assert!(report.diagnostics.is_empty());
}

#[test]
fn expression_semantics_baseline_keeps_unsupported_paths_deterministic() {
    let expression = Expression::new(
        expr_id("udf"),
        ExpressionKind::FunctionCall {
            name: "custom_udf".to_string(),
            args: Vec::new(),
        },
    );

    let report = evaluate_expression(&expression, &ExpressionInputRow::new());

    assert_eq!(report.status, ExpressionEvaluationStatus::Unsupported);
    assert!(report.has_errors());
    assert_eq!(report.claim_gate_status, "not_claim_grade");
    assert!(!report.fallback_attempted);
    assert!(!report.external_engine_invoked);
    assert!(report.diagnostics.iter().all(|d| !d.fallback.attempted));
    assert!(
        report
            .diagnostics
            .iter()
            .any(|d| d.feature.as_deref() == Some("function_call"))
    );
}

#[test]
fn expression_semantics_advanced_scalar_blockers_are_deterministic() {
    let decimal = Expression::cast(
        expr_id("decimal-cast"),
        Expression::literal(
            expr_id("decimal-source"),
            ScalarValue::Utf8("12.34".to_string()),
        ),
        LogicalDType::Extension("decimal128(10,2)".to_string()),
    );
    let decimal_report = evaluate_expression(&decimal, &ExpressionInputRow::new());

    assert_eq!(decimal_report.status, ExpressionEvaluationStatus::Evaluated);
    assert_eq!(
        decimal_report.value,
        Some(ScalarValue::Decimal128 {
            value: 1234,
            precision: 10,
            scale: 2
        })
    );
    assert!(!decimal_report.has_errors());
    assert_eq!(decimal_report.output_dtype, Some(decimal128_dtype(10, 2)));
    assert!(!decimal_report.fallback_attempted);
    assert!(!decimal_report.external_engine_invoked);
    assert!(
        decimal_report
            .diagnostics
            .iter()
            .all(|d| !d.fallback.attempted)
    );

    for function_name in ["interval_add_months", "regexp_extract", "collate_eq"] {
        let expression = Expression::new(
            expr_id(function_name),
            ExpressionKind::FunctionCall {
                name: function_name.to_string(),
                args: vec![Expression::literal(
                    expr_id(&format!("{function_name}-arg")),
                    ScalarValue::Utf8("alpha".to_string()),
                )],
            },
        );
        let report = evaluate_expression(&expression, &ExpressionInputRow::new());

        assert_eq!(report.status, ExpressionEvaluationStatus::Unsupported);
        assert!(report.has_errors());
        assert!(!report.fallback_attempted);
        assert!(!report.external_engine_invoked);
        assert!(
            report
                .diagnostics
                .iter()
                .all(|diagnostic| !diagnostic.fallback.attempted)
        );
    }

    assert_eq!(
        format_iso_timestamp_micros(
            parse_iso_timestamp_micros("2026-05-19T12:34:56+00:00")
                .expect("fixed offset timestamp parses")
        ),
        "2026-05-19T12:34:56Z"
    );
    assert!(parse_iso_timestamp_micros("2026-05-19T12:34:56Z[America/Chicago]").is_err());
}

#[test]
fn expression_semantics_complex_dtype_blockers_are_deterministic() {
    for (target_name, target_dtype) in [
        ("list", LogicalDType::List),
        ("struct", LogicalDType::Struct),
        ("variant", LogicalDType::Extension("variant".to_string())),
        ("union", LogicalDType::Extension("union".to_string())),
    ] {
        let expression = Expression::cast(
            expr_id(&format!("{target_name}-cast")),
            Expression::literal(
                expr_id(&format!("{target_name}-source")),
                ScalarValue::Utf8("alpha".to_string()),
            ),
            target_dtype.clone(),
        );
        let report = evaluate_expression(&expression, &ExpressionInputRow::new());

        assert_eq!(report.status, ExpressionEvaluationStatus::Unsupported);
        assert_eq!(report.output_dtype, Some(target_dtype));
        assert!(report.has_errors());
        assert!(!report.fallback_attempted);
        assert!(!report.external_engine_invoked);
        assert!(
            report
                .diagnostics
                .iter()
                .all(|diagnostic| !diagnostic.fallback.attempted)
        );
    }

    for function_name in [
        "list_eq",
        "struct_eq",
        "variant_get",
        "union_tag",
        "list_parent_child_null_policy",
        "struct_field_identity",
        "binary_source_decode",
    ] {
        let expression = Expression::new(
            expr_id(function_name),
            ExpressionKind::FunctionCall {
                name: function_name.to_string(),
                args: vec![Expression::literal(
                    expr_id(&format!("{function_name}-arg")),
                    ScalarValue::Utf8("alpha".to_string()),
                )],
            },
        );
        let report = evaluate_expression(&expression, &ExpressionInputRow::new());

        assert_eq!(report.status, ExpressionEvaluationStatus::Unsupported);
        assert!(report.has_errors());
        assert!(!report.fallback_attempted);
        assert!(!report.external_engine_invoked);
        assert!(
            report
                .diagnostics
                .iter()
                .all(|diagnostic| !diagnostic.fallback.attempted)
        );
    }

    assert_eq!(
        ScalarValue::Binary(vec![0, 1, 255]).dtype(),
        LogicalDType::Binary
    );
}

#[test]
fn expression_semantics_binary_equality_is_bytewise_and_null_propagating() {
    let equal = Expression::new(
        expr_id("binary-eq"),
        ExpressionKind::Compare {
            left: Box::new(Expression::column(expr_id("payload"), col("payload"))),
            op: ComparisonOp::Eq,
            right: Box::new(Expression::literal(
                expr_id("needle"),
                ScalarValue::Binary(vec![0, 1, 255]),
            )),
        },
    );
    let equal_report = evaluate_expression(
        &equal,
        &row(&[("payload", ScalarValue::Binary(vec![0, 1, 255]))]),
    );

    assert_eq!(equal_report.status, ExpressionEvaluationStatus::Evaluated);
    assert_eq!(equal_report.value, Some(ScalarValue::Boolean(true)));
    assert_eq!(equal_report.output_dtype, Some(LogicalDType::Boolean));
    assert_eq!(equal_report.operator_family, "comparison");
    assert_eq!(equal_report.null_behavior, NullBehavior::NullPropagating);
    assert!(!equal_report.fallback_attempted);
    assert!(!equal_report.external_engine_invoked);

    let unequal = Expression::new(
        expr_id("binary-neq"),
        ExpressionKind::Compare {
            left: Box::new(Expression::column(expr_id("payload-neq"), col("payload"))),
            op: ComparisonOp::NotEq,
            right: Box::new(Expression::literal(
                expr_id("other"),
                ScalarValue::Binary(vec![0, 1, 254]),
            )),
        },
    );
    let unequal_report = evaluate_expression(
        &unequal,
        &row(&[("payload", ScalarValue::Binary(vec![0, 1, 255]))]),
    );

    assert_eq!(unequal_report.status, ExpressionEvaluationStatus::Evaluated);
    assert_eq!(unequal_report.value, Some(ScalarValue::Boolean(true)));
    assert_eq!(unequal_report.output_dtype, Some(LogicalDType::Boolean));
    assert!(!unequal_report.fallback_attempted);
    assert!(!unequal_report.external_engine_invoked);

    let null_report = evaluate_expression(&equal, &row(&[("payload", ScalarValue::Null)]));

    assert_eq!(null_report.status, ExpressionEvaluationStatus::Evaluated);
    assert_eq!(null_report.value, Some(ScalarValue::Null));
    assert_eq!(null_report.output_dtype, Some(LogicalDType::Boolean));
    assert!(!null_report.fallback_attempted);
    assert!(!null_report.external_engine_invoked);
}

#[test]
fn expression_semantics_binary_ordering_is_bytewise_without_fallback() {
    let ordered = Expression::new(
        expr_id("binary-gt"),
        ExpressionKind::Compare {
            left: Box::new(Expression::literal(
                expr_id("left"),
                ScalarValue::Binary(vec![1, 2, 4]),
            )),
            op: ComparisonOp::Gt,
            right: Box::new(Expression::literal(
                expr_id("right"),
                ScalarValue::Binary(vec![1, 2, 3]),
            )),
        },
    );
    let report = evaluate_expression(&ordered, &ExpressionInputRow::new());

    assert_eq!(report.status, ExpressionEvaluationStatus::Evaluated);
    assert_eq!(report.operator_family, "comparison");
    assert_eq!(report.value, Some(ScalarValue::Boolean(true)));
    assert_eq!(report.output_dtype, Some(LogicalDType::Boolean));
    assert!(!report.has_errors());
    assert!(!report.fallback_attempted);
    assert!(!report.external_engine_invoked);
}

#[test]
fn expression_semantics_evaluates_utf8_string_predicates_without_fallback() {
    let starts_with = Expression::new(
        expr_id("starts-with"),
        ExpressionKind::FunctionCall {
            name: "utf8_starts_with".to_string(),
            args: vec![
                Expression::column(expr_id("label"), col("label")),
                Expression::literal(expr_id("prefix"), ScalarValue::Utf8("al".to_string())),
            ],
        },
    );
    let starts_with_report = evaluate_expression(
        &starts_with,
        &row(&[("label", ScalarValue::Utf8("alpha".into()))]),
    );

    assert_eq!(
        starts_with_report.status,
        ExpressionEvaluationStatus::Evaluated
    );
    assert_eq!(starts_with_report.value, Some(ScalarValue::Boolean(true)));
    assert_eq!(starts_with_report.output_dtype, Some(LogicalDType::Boolean));
    assert_eq!(starts_with_report.operator_family, "string_predicate");
    assert_eq!(
        starts_with_report.null_behavior,
        NullBehavior::NullPropagating
    );
    assert!(starts_with_report.data_materialized);
    assert!(!starts_with_report.fallback_attempted);
    assert!(!starts_with_report.external_engine_invoked);

    let contains = Expression::new(
        expr_id("contains"),
        ExpressionKind::FunctionCall {
            name: "utf8_contains".to_string(),
            args: vec![
                Expression::column(expr_id("label-contains"), col("label")),
                Expression::literal(expr_id("needle"), ScalarValue::Utf8("ha".to_string())),
            ],
        },
    );
    let contains_report = evaluate_expression(
        &contains,
        &row(&[("label", ScalarValue::Utf8("alpha".into()))]),
    );

    assert_eq!(
        contains_report.status,
        ExpressionEvaluationStatus::Evaluated
    );
    assert_eq!(contains_report.value, Some(ScalarValue::Boolean(true)));
    assert_eq!(contains_report.operator_family, "string_predicate");
    assert!(!contains_report.fallback_attempted);
    assert!(!contains_report.external_engine_invoked);

    let regex = Expression::new(
        expr_id("regex"),
        ExpressionKind::FunctionCall {
            name: "regexp_like".to_string(),
            args: vec![
                Expression::column(expr_id("label-regex"), col("label")),
                Expression::literal(expr_id("pattern"), ScalarValue::Utf8("^a.*a$".to_string())),
            ],
        },
    );
    let regex_report = evaluate_expression(
        &regex,
        &row(&[("label", ScalarValue::Utf8("alpha".into()))]),
    );

    assert_eq!(regex_report.status, ExpressionEvaluationStatus::Evaluated);
    assert_eq!(regex_report.value, Some(ScalarValue::Boolean(true)));
    assert_eq!(regex_report.operator_family, "string_predicate");
    assert_eq!(regex_report.output_dtype, Some(LogicalDType::Boolean));
    assert!(!regex_report.fallback_attempted);
    assert!(!regex_report.external_engine_invoked);
}

#[test]
fn expression_semantics_string_predicates_propagate_nulls_and_block_non_utf8() {
    let starts_with = Expression::new(
        expr_id("starts-with-null"),
        ExpressionKind::FunctionCall {
            name: "utf8_starts_with".to_string(),
            args: vec![
                Expression::column(expr_id("label"), col("label")),
                Expression::literal(expr_id("prefix"), ScalarValue::Utf8("al".to_string())),
            ],
        },
    );
    let null_report = evaluate_expression(&starts_with, &row(&[("label", ScalarValue::Null)]));

    assert_eq!(null_report.status, ExpressionEvaluationStatus::Evaluated);
    assert_eq!(null_report.value, Some(ScalarValue::Null));
    assert_eq!(null_report.output_dtype, Some(LogicalDType::Boolean));
    assert_eq!(null_report.operator_family, "string_predicate");
    assert!(!null_report.fallback_attempted);
    assert!(!null_report.external_engine_invoked);

    let invalid = Expression::new(
        expr_id("starts-with-invalid"),
        ExpressionKind::FunctionCall {
            name: "utf8_starts_with".to_string(),
            args: vec![
                Expression::column(expr_id("amount"), col("amount")),
                Expression::literal(
                    expr_id("prefix-invalid"),
                    ScalarValue::Utf8("1".to_string()),
                ),
            ],
        },
    );
    let invalid_report = evaluate_expression(&invalid, &row(&[("amount", ScalarValue::Int64(10))]));

    assert_eq!(
        invalid_report.status,
        ExpressionEvaluationStatus::Unsupported
    );
    assert!(invalid_report.has_errors());
    assert!(!invalid_report.fallback_attempted);
    assert!(!invalid_report.external_engine_invoked);
    assert!(
        invalid_report
            .diagnostics
            .iter()
            .all(|d| !d.fallback.attempted)
    );
    assert!(
        invalid_report
            .diagnostics
            .iter()
            .any(|d| d.feature.as_deref() == Some("string_predicate"))
    );

    let invalid_regex = Expression::new(
        expr_id("invalid-regex"),
        ExpressionKind::FunctionCall {
            name: "regexp_like".to_string(),
            args: vec![
                Expression::column(expr_id("label-regex"), col("label")),
                Expression::literal(
                    expr_id("pattern-invalid"),
                    ScalarValue::Utf8("[".to_string()),
                ),
            ],
        },
    );
    let invalid_regex_report = evaluate_expression(
        &invalid_regex,
        &row(&[("label", ScalarValue::Utf8("alpha".into()))]),
    );

    assert_eq!(
        invalid_regex_report.status,
        ExpressionEvaluationStatus::InvalidInput
    );
    assert!(invalid_regex_report.has_errors());
    assert!(!invalid_regex_report.fallback_attempted);
    assert!(!invalid_regex_report.external_engine_invoked);
    assert!(invalid_regex_report.diagnostics.iter().any(|diagnostic| {
        diagnostic
            .reason
            .as_deref()
            .is_some_and(|reason| reason.contains("invalid regex pattern"))
    }));
}

#[test]
fn expression_semantics_evaluates_utf8_string_transforms_without_fallback() {
    let row = row(&[("label", ScalarValue::Utf8("  Alpha  ".into()))]);

    for (name, expected) in [
        ("utf8_lower", "  alpha  "),
        ("utf8_upper", "  ALPHA  "),
        ("utf8_trim", "Alpha"),
    ] {
        let expression = Expression::new(
            expr_id(&format!("{name}-expr")),
            ExpressionKind::FunctionCall {
                name: name.to_string(),
                args: vec![Expression::column(
                    expr_id(&format!("{name}-column")),
                    col("label"),
                )],
            },
        );
        let report = evaluate_expression(&expression, &row);

        assert_eq!(report.status, ExpressionEvaluationStatus::Evaluated);
        assert_eq!(report.value, Some(ScalarValue::Utf8(expected.to_string())));
        assert_eq!(report.output_dtype, Some(LogicalDType::Utf8));
        assert_eq!(report.operator_family, "string_transform");
        assert_eq!(report.null_behavior, NullBehavior::NullPropagating);
        assert!(report.data_materialized);
        assert!(!report.fallback_attempted);
        assert!(!report.external_engine_invoked);
    }
}

#[test]
fn expression_semantics_string_transforms_propagate_nulls_and_block_non_utf8() {
    let expression = Expression::new(
        expr_id("lower-null"),
        ExpressionKind::FunctionCall {
            name: "utf8_lower".to_string(),
            args: vec![Expression::column(expr_id("label"), col("label"))],
        },
    );
    let null_report = evaluate_expression(&expression, &row(&[("label", ScalarValue::Null)]));

    assert_eq!(null_report.status, ExpressionEvaluationStatus::Evaluated);
    assert_eq!(null_report.value, Some(ScalarValue::Null));
    assert_eq!(null_report.output_dtype, Some(LogicalDType::Utf8));
    assert_eq!(null_report.operator_family, "string_transform");
    assert!(!null_report.fallback_attempted);
    assert!(!null_report.external_engine_invoked);

    let invalid_report =
        evaluate_expression(&expression, &row(&[("label", ScalarValue::Int64(1))]));

    assert_eq!(
        invalid_report.status,
        ExpressionEvaluationStatus::Unsupported
    );
    assert!(invalid_report.has_errors());
    assert!(!invalid_report.fallback_attempted);
    assert!(!invalid_report.external_engine_invoked);
    assert!(
        invalid_report
            .diagnostics
            .iter()
            .all(|d| !d.fallback.attempted)
    );
    assert!(
        invalid_report
            .diagnostics
            .iter()
            .any(|d| d.feature.as_deref() == Some("string_transform"))
    );
}

#[test]
fn expression_semantics_parses_formats_and_extracts_date32_without_fallback() {
    assert_eq!(parse_iso_date32("1970-01-01").expect("epoch date"), 0);
    assert_eq!(parse_iso_date32("1970-01-02").expect("next date"), 1);
    assert_eq!(parse_iso_date32("1969-12-31").expect("previous date"), -1);
    assert!(parse_iso_date32("2023-02-29").is_err());

    let value = parse_iso_date32("2026-05-19").expect("valid date");
    assert_eq!(format_iso_date32(value), "2026-05-19");
    assert_eq!(date32_year(value), 2026);
    assert_eq!(date32_month(value), 5);
    assert_eq!(date32_day(value), 19);

    let row = row(&[("event_date", ScalarValue::Date32(value))]);
    for (name, expected) in [
        ("date_year", 2026_i64),
        ("date_month", 5_i64),
        ("date_day", 19_i64),
    ] {
        let expression = Expression::new(
            expr_id(&format!("{name}-expr")),
            ExpressionKind::FunctionCall {
                name: name.to_string(),
                args: vec![Expression::column(
                    expr_id(&format!("{name}-column")),
                    col("event_date"),
                )],
            },
        );
        let report = evaluate_expression(&expression, &row);

        assert_eq!(report.status, ExpressionEvaluationStatus::Evaluated);
        assert_eq!(report.value, Some(ScalarValue::Int64(expected)));
        assert_eq!(report.output_dtype, Some(LogicalDType::Int64));
        assert_eq!(report.operator_family, "date_extract");
        assert_eq!(report.null_behavior, NullBehavior::NullPropagating);
        assert!(report.data_materialized);
        assert!(!report.fallback_attempted);
        assert!(!report.external_engine_invoked);
    }
}

#[test]
fn expression_semantics_casts_iso_utf8_and_date32_without_fallback() {
    let value = parse_iso_date32("2026-05-19").expect("valid date");
    let utf8_to_date = Expression::cast(
        expr_id("utf8-to-date32"),
        Expression::literal(
            expr_id("date-text"),
            ScalarValue::Utf8("2026-05-19".to_string()),
        ),
        LogicalDType::Date32,
    );
    let date_report = evaluate_expression(&utf8_to_date, &ExpressionInputRow::new());

    assert_eq!(date_report.status, ExpressionEvaluationStatus::Evaluated);
    assert_eq!(date_report.value, Some(ScalarValue::Date32(value)));
    assert_eq!(date_report.output_dtype, Some(LogicalDType::Date32));
    assert_eq!(date_report.operator_family, "cast");
    assert!(!date_report.fallback_attempted);
    assert!(!date_report.external_engine_invoked);

    let date_to_utf8 = Expression::cast(
        expr_id("date32-to-utf8"),
        Expression::literal(expr_id("date32"), ScalarValue::Date32(value)),
        LogicalDType::Utf8,
    );
    let utf8_report = evaluate_expression(&date_to_utf8, &ExpressionInputRow::new());

    assert_eq!(utf8_report.status, ExpressionEvaluationStatus::Evaluated);
    assert_eq!(
        utf8_report.value,
        Some(ScalarValue::Utf8("2026-05-19".to_string()))
    );
    assert_eq!(utf8_report.output_dtype, Some(LogicalDType::Utf8));
    assert_eq!(utf8_report.operator_family, "cast");
    assert!(!utf8_report.fallback_attempted);
    assert!(!utf8_report.external_engine_invoked);
}

#[test]
fn expression_semantics_parses_formats_extracts_and_casts_timestamp_micros_without_fallback() {
    let value = parse_iso_timestamp_micros("2026-05-19T12:30:45.123456Z").expect("valid timestamp");
    assert_eq!(
        format_iso_timestamp_micros(value),
        "2026-05-19T12:30:45.123456Z"
    );
    assert_eq!(
        format_iso_timestamp_micros(
            parse_iso_timestamp_micros("1970-01-01T00:00:00Z").expect("epoch timestamp")
        ),
        "1970-01-01T00:00:00Z"
    );
    assert_eq!(
        format_iso_timestamp_micros(
            parse_iso_timestamp_micros("1969-12-31T23:59:59.999999Z").expect("negative timestamp")
        ),
        "1969-12-31T23:59:59.999999Z"
    );
    assert_eq!(
        format_iso_timestamp_micros(
            parse_iso_timestamp_micros("2026-05-19T12:30:45+00:00")
                .expect("zero fixed offset timestamp")
        ),
        "2026-05-19T12:30:45Z"
    );
    assert_eq!(
        format_iso_timestamp_micros(
            parse_iso_timestamp_micros("2026-05-19T12:30:45-05:00")
                .expect("negative fixed offset timestamp")
        ),
        "2026-05-19T17:30:45Z"
    );
    assert!(parse_iso_timestamp_micros("2026-05-19T12:30:45.1234567Z").is_err());
    assert!(parse_iso_timestamp_micros("2026-05-19T12:30:45Z[America/Chicago]").is_err());
    assert!(parse_iso_timestamp_micros("2026-05-19T12:30:60Z").is_err());

    assert_eq!(timestamp_micros_year(value), 2026);
    assert_eq!(timestamp_micros_month(value), 5);
    assert_eq!(timestamp_micros_day(value), 19);
    assert_eq!(timestamp_micros_hour(value), 12);
    assert_eq!(timestamp_micros_minute(value), 30);
    assert_eq!(timestamp_micros_second(value), 45);

    let row = row(&[("event_ts", ScalarValue::TimestampMicros(value))]);
    for (name, expected) in [
        ("timestamp_year", 2026_i64),
        ("timestamp_month", 5_i64),
        ("timestamp_day", 19_i64),
        ("timestamp_hour", 12_i64),
        ("timestamp_minute", 30_i64),
        ("timestamp_second", 45_i64),
    ] {
        let expression = Expression::new(
            expr_id(&format!("{name}-expr")),
            ExpressionKind::FunctionCall {
                name: name.to_string(),
                args: vec![Expression::column(
                    expr_id(&format!("{name}-column")),
                    col("event_ts"),
                )],
            },
        );
        let report = evaluate_expression(&expression, &row);

        assert_eq!(report.status, ExpressionEvaluationStatus::Evaluated);
        assert_eq!(report.value, Some(ScalarValue::Int64(expected)));
        assert_eq!(report.output_dtype, Some(LogicalDType::Int64));
        assert_eq!(report.operator_family, "timestamp_extract");
        assert_eq!(report.null_behavior, NullBehavior::NullPropagating);
        assert!(!report.fallback_attempted);
        assert!(!report.external_engine_invoked);
    }

    let utf8_to_timestamp = Expression::cast(
        expr_id("utf8-to-timestamp"),
        Expression::literal(
            expr_id("timestamp-text"),
            ScalarValue::Utf8("2026-05-19T12:30:45.123456Z".to_string()),
        ),
        LogicalDType::TimestampMicros,
    );
    let timestamp_report = evaluate_expression(&utf8_to_timestamp, &ExpressionInputRow::new());
    assert_eq!(
        timestamp_report.status,
        ExpressionEvaluationStatus::Evaluated
    );
    assert_eq!(
        timestamp_report.value,
        Some(ScalarValue::TimestampMicros(value))
    );
    assert_eq!(
        timestamp_report.output_dtype,
        Some(LogicalDType::TimestampMicros)
    );
    assert!(!timestamp_report.fallback_attempted);
    assert!(!timestamp_report.external_engine_invoked);

    let timestamp_to_utf8 = Expression::cast(
        expr_id("timestamp-to-utf8"),
        Expression::literal(expr_id("timestamp"), ScalarValue::TimestampMicros(value)),
        LogicalDType::Utf8,
    );
    let utf8_report = evaluate_expression(&timestamp_to_utf8, &ExpressionInputRow::new());
    assert_eq!(
        utf8_report.value,
        Some(ScalarValue::Utf8("2026-05-19T12:30:45.123456Z".to_string()))
    );

    let timestamp_to_date32 = Expression::cast(
        expr_id("timestamp-to-date32"),
        Expression::literal(
            expr_id("timestamp-date"),
            ScalarValue::TimestampMicros(value),
        ),
        LogicalDType::Date32,
    );
    let date_report = evaluate_expression(&timestamp_to_date32, &ExpressionInputRow::new());
    assert_eq!(
        date_report.value,
        Some(ScalarValue::Date32(
            parse_iso_date32("2026-05-19").expect("date")
        ))
    );
}

#[test]
fn expression_semantics_date_extract_nulls_and_blockers_are_deterministic() {
    let expression = Expression::new(
        expr_id("date-year-null"),
        ExpressionKind::FunctionCall {
            name: "date_year".to_string(),
            args: vec![Expression::column(expr_id("event-date"), col("event_date"))],
        },
    );
    let null_report = evaluate_expression(&expression, &row(&[("event_date", ScalarValue::Null)]));

    assert_eq!(null_report.status, ExpressionEvaluationStatus::Evaluated);
    assert_eq!(null_report.value, Some(ScalarValue::Null));
    assert_eq!(null_report.output_dtype, Some(LogicalDType::Int64));
    assert_eq!(null_report.operator_family, "date_extract");
    assert!(!null_report.fallback_attempted);
    assert!(!null_report.external_engine_invoked);

    let invalid_report = evaluate_expression(
        &expression,
        &row(&[("event_date", ScalarValue::Utf8("2026-05-19".to_string()))]),
    );

    assert_eq!(
        invalid_report.status,
        ExpressionEvaluationStatus::Unsupported
    );
    assert!(invalid_report.has_errors());
    assert!(!invalid_report.fallback_attempted);
    assert!(!invalid_report.external_engine_invoked);
    assert!(
        invalid_report
            .diagnostics
            .iter()
            .all(|d| !d.fallback.attempted)
    );
    assert!(
        invalid_report
            .diagnostics
            .iter()
            .any(|d| d.feature.as_deref() == Some("date_extract"))
    );
}

#[test]
fn projection_filter_and_limit_share_the_semantics_baseline() {
    let rows = vec![
        row(&[
            ("id", ScalarValue::Int64(1)),
            ("amount", ScalarValue::Int64(8)),
        ]),
        row(&[
            ("id", ScalarValue::Int64(2)),
            ("amount", ScalarValue::Int64(15)),
        ]),
        row(&[("id", ScalarValue::Int64(3)), ("amount", ScalarValue::Null)]),
    ];
    let predicate = Expression::new(
        expr_id("amount-filter"),
        ExpressionKind::Compare {
            left: Box::new(Expression::column(expr_id("amount"), col("amount"))),
            op: ComparisonOp::Gt,
            right: Box::new(Expression::literal(
                expr_id("threshold"),
                ScalarValue::Int64(10),
            )),
        },
    );
    let filter = evaluate_filter(&predicate, &rows);

    assert_eq!(filter.schema_version, "shardloom.filter_semantics.v1");
    assert_eq!(filter.status, ExpressionEvaluationStatus::Evaluated);
    assert_eq!(filter.selected_row_indexes, vec![1]);
    assert_eq!(filter.null_predicate_row_count, 1);
    assert!(!filter.data_decoded);
    assert!(filter.data_materialized);
    assert!(!filter.fallback_attempted);
    assert!(!filter.external_engine_invoked);

    let projection = evaluate_projection(
        &[
            Expression::column(expr_id("id"), col("id")),
            Expression::new(
                expr_id("amount_alias"),
                ExpressionKind::Alias {
                    expr: Box::new(Expression::column(expr_id("amount-ref"), col("amount"))),
                    alias: "amount_selected".to_string(),
                },
            ),
        ],
        &rows[filter.selected_row_indexes[0]],
    );

    assert_eq!(
        projection.schema_version,
        "shardloom.projection_semantics.v1"
    );
    assert_eq!(projection.status, ExpressionEvaluationStatus::Evaluated);
    assert_eq!(projection.projected_columns.len(), 2);
    assert_eq!(projection.projected_columns[0].name, "id");
    assert_eq!(projection.projected_columns[1].name, "amount_selected");
    assert!(!projection.data_decoded);
    assert!(projection.data_materialized);
    assert!(!projection.fallback_attempted);
    assert!(!projection.external_engine_invoked);

    let limit = evaluate_limit(filter.selected_row_count(), 1);
    assert_eq!(limit.schema_version, "shardloom.limit_semantics.v1");
    assert_eq!(limit.output_row_count, 1);
    assert!(!limit.data_decoded);
    assert!(!limit.data_materialized);
    assert!(!limit.fallback_attempted);
    assert!(!limit.external_engine_invoked);
}
