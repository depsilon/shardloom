use shardloom_core::{
    BinaryOp, ColumnRef, ComparisonOp, ExprId, Expression, ExpressionEvaluationStatus,
    ExpressionInputRow, ExpressionKind, LogicalDType, NullBehavior, ScalarValue, UnaryOp,
    evaluate_expression, evaluate_filter, evaluate_limit, evaluate_projection,
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
