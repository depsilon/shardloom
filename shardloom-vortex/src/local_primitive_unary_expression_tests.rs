use super::*;
use crate::{VortexExpressionProjectionRequest, VortexExpressionRewrite as Rewrite};

fn request(
    fixture: &Fixture,
    names: &[&str],
    rewrites: Vec<Rewrite>,
) -> VortexQueryPrimitiveRequest {
    VortexQueryPrimitiveRequest::expression_project_rows(
        fixture.uri(),
        projection(names),
        VortexExpressionProjectionRequest::new(rewrites),
    )
}

#[test]
fn unary_expression_preserves_rewrite_order_filtered_ordinals_and_empty_schema() {
    use shardloom_core::{ComparisonOp, PredicateExpr};
    let fixture = Fixture::new(
        &[Some(1), Some(2), Some(3), Some(4), Some(5)],
        &[10, 20, 30, 40, 50],
        2,
    );
    let target = ColumnRef::new(VALUE).unwrap();
    let mut request = request(
        &fixture,
        &[VALUE],
        vec![
            Rewrite::ReplaceScalar {
                target_column: target.clone(),
                to_replace: shardloom_core::ScalarValue::UInt64(20),
                replacement: shardloom_core::ScalarValue::UInt64(99),
            },
            Rewrite::MaskScalar {
                target_column: target.clone(),
                predicate: PredicateExpr::Compare {
                    column: target.clone(),
                    op: ComparisonOp::Eq,
                    value: StatValue::UInt64(99),
                },
                replacement: shardloom_core::ScalarValue::UInt64(5),
            },
            Rewrite::NumericScalarArithmetic {
                target_column: target,
                operator: "*".into(),
                operand: shardloom_core::ScalarValue::UInt64(2),
            },
            Rewrite::RowNumber {
                target_column: ColumnRef::new("ordinal").unwrap(),
                start: 7,
            },
        ],
    );
    request.predicate = Some(PredicateExpr::Compare {
        column: ColumnRef::new(KEY).unwrap(),
        op: ComparisonOp::Gt,
        value: StatValue::UInt64(1),
    });
    let prepared = prepare(&request);
    let result = prepared.execute_owned().unwrap();
    assert_eq!(
        json_rows(&result.result),
        vec![
            serde_json::json!({"amount":10,"ordinal":7}),
            serde_json::json!({"amount":60,"ordinal":8}),
            serde_json::json!({"amount":80,"ordinal":9}),
            serde_json::json!({"amount":100,"ordinal":10}),
        ]
    );
    assert!(result.execution.native_io_certificate.is_certified());
    assert_eq!(values(&prepared, "ordinal"), vec![7, 8, 9, 10]);
    assert_eq!(prepared.snapshot().prepared_source_opens, 1);
    assert_eq!(prepared.snapshot().completed_executions, 2);
    let empty = Fixture::new(&[], &[], 1);
    request.source_uri = Some(empty.uri());
    request.predicate = None;
    let empty = prepare(&request).execute_owned().unwrap();
    assert_eq!(empty.result.row_count(), 0);
    assert_eq!(empty.result.dtype(), result.result.dtype());
}

#[test]
fn unary_expression_forward_fill_steps_are_independent_across_batches_and_calls() {
    let rows = BATCH_ROWS + 5;
    let mut keys = vec![None; rows];
    keys[0] = Some(5);
    keys[BATCH_ROWS + 1] = Some(9);
    for chunk in [31, 8192] {
        let fixture = Fixture::new(&keys, &vec![0; rows], chunk);
        let prepared = prepare(&request(
            &fixture,
            &[KEY],
            vec![
                Rewrite::ForwardFillNull {
                    target_column: ColumnRef::new(KEY).unwrap(),
                    limit: Some(2),
                },
                Rewrite::ForwardFillNull {
                    target_column: ColumnRef::new(KEY).unwrap(),
                    limit: Some(1),
                },
            ],
        ));
        let expected = (0..rows)
            .map(|index| {
                if index < 4 {
                    serde_json::json!(5)
                } else if index > BATCH_ROWS {
                    serde_json::json!(9)
                } else {
                    serde_json::Value::Null
                }
            })
            .collect::<Vec<_>>();
        assert_eq!(values(&prepared, KEY), expected);
        assert_eq!(values(&prepared, KEY), expected);
    }
}

fn text_fixture(text: &[&str]) -> Fixture {
    let values =
        vortex::array::arrays::VarBinViewArray::from_iter_str(text.iter().copied()).into_array();
    Fixture::from_array(
        StructArray::new(
            ["label"].into(),
            vec![values],
            text.len(),
            Validity::NonNullable,
        )
        .into_array(),
        2,
    )
}

#[test]
fn unary_expression_string_replacements_preserve_unicode_empty_matches_and_capture_syntax() {
    let fixture = text_fixture(&["a1b22", "π$7", ""]);
    let target = ColumnRef::new("label").unwrap();
    let plain = prepare(&request(
        &fixture,
        &["label"],
        vec![Rewrite::StringReplaceScalar {
            target_column: target.clone(),
            needle: String::new(),
            replacement: "=".into(),
        }],
    ));
    assert_eq!(values(&plain, "label"), vec!["=a=1=b=2=2=", "=π=$=7=", "="]);
    let regex = prepare(&request(
        &fixture,
        &["label"],
        vec![Rewrite::RegexReplaceScalar {
            target_column: target,
            pattern: "([a-z])(\\d+)".into(),
            replacement: "\\2-\\1-$".into(),
        }],
    ));
    assert_eq!(values(&regex, "label"), vec!["1-a-$22-b-$", "π$7", ""]);
    assert_eq!(values(&regex, "label"), vec!["1-a-$22-b-$", "π$7", ""]);
}

#[test]
fn unary_expression_rejects_overflow_and_reserves_string_growth_before_building() {
    let fixture = Fixture::new(&[Some(1)], &[u64::MAX], 1);
    let prepared = prepare(&request(
        &fixture,
        &[VALUE],
        vec![Rewrite::NumericScalarArithmetic {
            target_column: ColumnRef::new(VALUE).unwrap(),
            operator: "+".into(),
            operand: shardloom_core::ScalarValue::UInt64(1),
        }],
    ));
    assert!(
        prepared
            .execute_owned()
            .err()
            .expect("overflow")
            .to_string()
            .contains("overflow")
    );
    let text = "a".repeat(5000);
    let fixture = text_fixture(&[&text]);
    let request = request(
        &fixture,
        &["label"],
        vec![Rewrite::StringReplaceScalar {
            target_column: ColumnRef::new("label").unwrap(),
            needle: "a".into(),
            replacement: "x".repeat(1000),
        }],
    );
    let session = ResidentVortexSession::new(1 << 20, 1).unwrap();
    let prepared = prepare_unary_in_session(
        &request,
        VortexLocalPrimitiveExecutionPolicy::single_threaded(),
        &session,
    )
    .unwrap();
    let error = prepared
        .execute_owned()
        .err()
        .expect("string growth must reserve first");
    assert!(error.to_string().contains("memory reservation denied"));
    drop(prepared);
    assert_eq!(session.snapshot().memory.reserved_bytes, 0);
}
