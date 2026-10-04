use super::*;
use shardloom_core::{ComparisonOp, PredicateExpr};

fn weighted(weights: &[Option<f64>]) -> ArrayRef {
    StructArray::new(
        FieldNames::from([KEY, VALUE]),
        vec![
            PrimitiveArray::new(
                (0..weights.len() as u64).collect::<Vec<_>>(),
                Validity::NonNullable,
            )
            .into_array(),
            PrimitiveArray::new(
                weights.iter().map(|v| v.unwrap_or(0.0)).collect::<Vec<_>>(),
                Validity::from_iter(weights.iter().map(Option::is_some)),
            )
            .into_array(),
        ],
        weights.len(),
        Validity::NonNullable,
    )
    .into_array()
}

fn consume(
    request: &VortexQueryPrimitiveRequest,
    input: &ArrayRef,
    upper_rows: Option<u64>,
    batch_rows: usize,
) -> Result<(Vec<serde_json::Value>, report::StateUsage)> {
    let session = ResidentVortexSession::new(32 << 20, 1)?;
    let bound = BoundUnary::for_relation(request, input.dtype(), session.memory())?;
    let baseline = session.snapshot().memory.reserved_bytes;
    let mut rows = Vec::new();
    let mut producer_calls = 0;
    let result = session.with_native_execution_context(&CancellationToken::default(), |context| {
        let mut execution = context.native_session().create_execution_ctx();
        bound.consume_relation(
            context,
            upper_rows,
            2,
            |consumer| {
                producer_calls += 1;
                for start in (0..input.len()).step_by(batch_rows) {
                    consumer(
                        input
                            .slice(start..input.len().min(start + batch_rows))
                            .unwrap(),
                    )?;
                }
                Ok(())
            },
            &mut |array| {
                let columns = bound
                    .output_columns
                    .iter()
                    .map(|name| runtime::logical_field_from_native_array(&array, name))
                    .collect::<Result<Vec<_>>>()?;
                for row in 0..array.len() {
                    let mut object = serde_json::Map::new();
                    for (name, column) in bound.output_columns.iter().zip(&columns) {
                        object.insert(
                            name.clone(),
                            scalar_json(&column.execute_scalar(row, &mut execution).unwrap()),
                        );
                    }
                    rows.push(object.into());
                }
                Ok(())
            },
        )
    });
    assert_eq!(producer_calls, 1, "cardinality must never replay the input");
    assert_eq!(session.snapshot().memory.reserved_bytes, baseline);
    drop(bound);
    assert_eq!(session.snapshot().memory.reserved_bytes, 0);
    result.map(|usage| (rows, usage))
}

fn sample(fraction: bool) -> VortexQueryPrimitiveRequest {
    let mut request = VortexQueryPrimitiveRequest::for_relational_input(
        VortexQueryPrimitiveKind::SampleRows,
        projection(&[KEY]),
    );
    request.source_order_limit = (!fraction).then_some(2);
    request.sample_fraction = fraction.then_some(0.5);
    request.sample_seed = Some(7);
    request.sample_weight_column = Some(ColumnRef::new(VALUE).unwrap());
    request
}

#[test]
fn composed_unary_sampling_ties_ignore_row_bounds_and_chunking() {
    let tiny = f64::from_bits(1);
    let input = weighted(&[Some(tiny), Some(tiny), Some(tiny), Some(1.0)]);
    let expected = vec![serde_json::json!({KEY:0}), serde_json::json!({KEY:3})];
    for fraction in [false, true] {
        let request = sample(fraction);
        for upper in [None, Some(4), Some(5), Some(20)] {
            for batch in [1, 3, 4] {
                let (rows, usage) = consume(&request, &input, upper, batch).unwrap();
                assert_eq!(rows, expected);
                if fraction && upper.is_none() {
                    assert_eq!(usage.items, 4);
                    assert!(usage.all_input_retained);
                } else if !fraction {
                    assert_eq!(usage.items, 2);
                    assert!(!usage.all_input_retained);
                }
            }
        }
        let fixture = Fixture::from_array(input.clone(), 2);
        let mut direct = request;
        direct.source_uri = Some(fixture.uri());
        assert_eq!(
            json_rows(&prepare(&direct).execute_owned().unwrap().result),
            expected
        );
    }
    // This is the pre-existing compatibility helper, with literal tied scores.
    // Its result must match the native candidate heap for every candidate cap.
    for cap in [2, 3, 4] {
        let mut selected = Vec::new();
        for (ordinal, score) in [
            f64::NEG_INFINITY,
            f64::NEG_INFINITY,
            f64::NEG_INFINITY,
            -1.0,
        ]
        .into_iter()
        .enumerate()
        {
            runtime::insert_weighted_sample_row_export_candidate(
                &mut selected,
                cap,
                score,
                ordinal,
                vec![],
            );
        }
        runtime::truncate_weighted_sample_candidates_to_target(&mut selected, 2);
        assert_eq!(selected.iter().map(|row| row.1).collect::<Vec<_>>(), [0, 3]);
    }
}

#[test]
fn composed_unary_sampling_filters_before_weights_and_releases_invalid_state() {
    let tiny = f64::from_bits(1);
    let input = weighted(&[Some(0.0), Some(tiny), Some(tiny), Some(tiny), Some(1.0)]);
    let mut request = sample(true);
    request.predicate = Some(PredicateExpr::Compare {
        column: ColumnRef::new(KEY).unwrap(),
        op: ComparisonOp::Gt,
        value: StatValue::UInt64(0),
    });
    for upper in [None, Some(5), Some(20)] {
        assert_eq!(
            consume(&request, &input, upper, 2).unwrap().0,
            vec![serde_json::json!({KEY:1}), serde_json::json!({KEY:4})]
        );
    }
    for weight in [
        None,
        Some(0.0),
        Some(-1.0),
        Some(f64::INFINITY),
        Some(f64::NAN),
    ] {
        let input = weighted(&[Some(1.0), weight]);
        let error = consume(&sample(false), &input, None, 1)
            .err()
            .unwrap()
            .to_string();
        assert!(error.contains("weight"), "{error}");
        assert!(error.contains("no fallback"), "{error}");
    }
}

#[test]
fn composed_unary_unknown_rolling_and_tail_keep_bounded_state() {
    let input = weighted(&[None, Some(10.0), Some(20.0), None, Some(40.0)]);
    for (center, expected) in [
        (false, serde_json::json!([10.0, 30.0, 30.0, 60.0])),
        (true, serde_json::json!([10.0, 30.0, 30.0, 60.0, 40.0])),
    ] {
        let mut request = VortexQueryPrimitiveRequest::for_relational_input(
            VortexQueryPrimitiveKind::RollingWindowRows,
            projection(&[VALUE]),
        );
        request.rolling_window = Some(
            crate::VortexRollingWindowRequest::new(
                ColumnRef::new(VALUE).unwrap(),
                "total".into(),
                3,
                1,
                "sum".into(),
            )
            .with_center(center),
        );
        for upper in [None, Some(5), Some(20)] {
            let (rows, usage) = consume(&request, &input, upper, 2).unwrap();
            assert_eq!(
                rows.iter()
                    .map(|row| row["total"].clone())
                    .collect::<Vec<_>>(),
                *expected.as_array().unwrap()
            );
            assert!(usage.items <= 3);
            assert!(!usage.all_input_retained);
        }
        let nulls = weighted(&[None, None, None, None, None]);
        assert_eq!(
            consume(&request, &nulls, None, 2).unwrap().0,
            Vec::<serde_json::Value>::new()
        );
    }
    let mut request = VortexQueryPrimitiveRequest::for_relational_input(
        VortexQueryPrimitiveKind::TailRows,
        projection(&[KEY, VALUE]),
    );
    request.source_order_limit = Some(2);
    let (rows, usage) = consume(&request, &input, None, 2).unwrap();
    assert_eq!(
        rows,
        vec![
            serde_json::json!({KEY:3,VALUE:null}),
            serde_json::json!({KEY:4,VALUE:40.0})
        ]
    );
    assert_eq!(usage.items, 2);
    assert!(!usage.all_input_retained);
}

#[test]
fn composed_unary_rewrites_bind_nullable_targets_before_looking_at_rows() {
    use crate::{VortexExpressionProjectionRequest, VortexExpressionRewrite as Rewrite};
    use vortex::array::arrays::VarBinViewArray;

    let input = weighted(&[None, Some(3.0)]);
    let mut request = VortexQueryPrimitiveRequest::for_relational_input(
        VortexQueryPrimitiveKind::ExpressionProjectRows,
        projection(&[VALUE]),
    );
    request.expression_projection = Some(VortexExpressionProjectionRequest::new(vec![
        Rewrite::MaskScalar {
            target_column: ColumnRef::new(VALUE).unwrap(),
            predicate: PredicateExpr::AlwaysTrue,
            replacement: shardloom_core::ScalarValue::Int64(7),
        },
    ]));
    let expected = vec![
        serde_json::json!({VALUE:7.0}),
        serde_json::json!({VALUE:7.0}),
    ];
    assert_eq!(consume(&request, &input, None, 1).unwrap().0, expected);
    let fixture = Fixture::from_array(input.clone(), 1);
    let mut direct = request.clone();
    direct.source_uri = Some(fixture.uri());
    assert_eq!(
        json_rows(&prepare(&direct).execute_owned().unwrap().result),
        expected
    );
    request.expression_projection = Some(VortexExpressionProjectionRequest::new(vec![
        Rewrite::MaskScalar {
            target_column: ColumnRef::new(VALUE).unwrap(),
            predicate: PredicateExpr::AlwaysTrue,
            replacement: shardloom_core::ScalarValue::Utf8("invalid numeric replacement".into()),
        },
    ]));
    assert!(
        BoundUnary::for_relation(
            &request,
            input.slice(0..0).unwrap().dtype(),
            ResidentVortexSession::new(32 << 20, 1).unwrap().memory(),
        )
        .is_err()
    );
    request.expression_projection = Some(VortexExpressionProjectionRequest::new(vec![
        Rewrite::NumericScalarArithmetic {
            target_column: ColumnRef::new(VALUE).unwrap(),
            operator: "+".into(),
            operand: shardloom_core::ScalarValue::Int64(2),
        },
    ]));
    assert_eq!(
        consume(&request, &input, None, 1).unwrap().0,
        vec![
            serde_json::json!({VALUE:null}),
            serde_json::json!({VALUE:5.0})
        ]
    );
    let text = StructArray::new(
        FieldNames::from([VALUE]),
        vec![
            VarBinViewArray::from_iter([None, Some("a'b")], DType::Utf8(Nullability::Nullable))
                .into_array(),
        ],
        2,
        Validity::NonNullable,
    )
    .into_array();
    for rewrite in [
        Rewrite::ReplaceScalar {
            target_column: ColumnRef::new(VALUE).unwrap(),
            to_replace: shardloom_core::ScalarValue::Utf8("a'b".into()),
            replacement: shardloom_core::ScalarValue::Utf8("changed".into()),
        },
        Rewrite::StringReplaceScalar {
            target_column: ColumnRef::new(VALUE).unwrap(),
            needle: "a'b".into(),
            replacement: "changed".into(),
        },
        Rewrite::RegexReplaceScalar {
            target_column: ColumnRef::new(VALUE).unwrap(),
            pattern: "a.b".into(),
            replacement: "changed".into(),
        },
    ] {
        request.expression_projection = Some(VortexExpressionProjectionRequest::new(vec![rewrite]));
        assert_eq!(
            consume(&request, &text, None, 1).unwrap().0,
            vec![
                serde_json::json!({VALUE:null}),
                serde_json::json!({VALUE:"changed"})
            ]
        );
    }
}
