use super::*;
use crate::{VortexAggregateOrderExpr, VortexSimpleAggregateMeasure};
use vortex::array::{
    ArrayRef, IntoArray as _, VortexSessionExecute as _,
    arrays::{PrimitiveArray, StructArray},
    validity::Validity,
};

fn request(fused: bool) -> VortexSimpleAggregateRequest {
    let functions = if fused {
        vec!["sum", "avg"]
    } else {
        vec!["avg"]
    };
    VortexSimpleAggregateRequest::new(
        functions
            .into_iter()
            .map(|function| {
                VortexSimpleAggregateMeasure::new(
                    function,
                    Some(ColumnRef::new("amount").unwrap()),
                    function.into(),
                )
            })
            .collect(),
    )
}

fn assert_totals(states: &SimpleAggregateStates, count: u64, sum: f64) {
    for state in &states.states {
        assert_eq!(state.count, count);
        let expected = if count == 0 {
            serde_json::Value::Null
        } else {
            serde_json::json!(if state.function == SimpleAggregateFunction::Avg {
                simple_average_value(sum, count)
            } else {
                sum
            })
        };
        assert_eq!(state.result_json().unwrap(), expected);
    }
}

#[test]
fn independent_integer_average_preserves_cancellation_nulls_selection_and_fusion() {
    let columns = ["amount".into()];
    let values = [
        Some(1_000_000_000_000_000_000_i64),
        None,
        Some(1),
        Some(-1_000_000_000_000_000_000),
    ];
    let array = PrimitiveArray::from_option_iter(values).into_array();
    let materialized = [values
        .into_iter()
        .map(|value| value.map_or(StatValue::Null, StatValue::Int64))
        .collect::<Vec<_>>()];
    let mut ctx = vortex::array::array_session().create_execution_ctx();
    let (accessor, _) = aggregate_column_accessor_with_work("amount", &array).unwrap();
    for fused in [false, true] {
        for (selection, count, sum) in [
            (None, 3, 1.0),
            (Some([0, 2, 2, 3].as_slice()), 4, 2.0),
            (Some([].as_slice()), 0, 0.0),
        ] {
            let mut native = SimpleAggregateStates::new(&request(fused), &columns).unwrap();
            assert!(
                native
                    .update_direct_from_chunk(
                        &array,
                        &columns,
                        selection,
                        &mut NativeNumericAccessorWork::default(),
                        &mut ctx
                    )
                    .unwrap()
            );
            assert_totals(&native, count, sum);
            let mut rows = SimpleAggregateStates::new(&request(fused), &columns).unwrap();
            let indices = selection.map_or_else(|| (0..4).collect(), <[usize]>::to_vec);
            for &row in &indices {
                rows.update_direct_row_from_accessors(std::slice::from_ref(&accessor), row, 4)
                    .unwrap();
            }
            assert_totals(&rows, count, sum);
            let mut decoded = SimpleAggregateStates::new(&request(fused), &columns).unwrap();
            if selection.is_none() {
                decoded.update(&materialized, 4).unwrap();
            } else {
                for &row in &indices {
                    decoded.update_row(&materialized, row).unwrap();
                }
            }
            assert_totals(&decoded, count, sum);
        }
    }
    let mut left = SimpleAggregateStates::new(&request(true), &columns).unwrap();
    let mut right = left.clone();
    left.update_row(&materialized, 0).unwrap();
    left.update_row(&materialized, 2).unwrap();
    right.update_row(&materialized, 3).unwrap();
    left.merge_preaggregated_from(&right).unwrap();
    assert_totals(&left, 3, 1.0);
}

#[test]
fn independent_integer_average_covers_run_end_and_compact_groups() {
    let mut ctx = vortex::array::array_session().create_execution_ctx();
    let runend = vortex::encodings::runend::RunEnd::try_new(
        PrimitiveArray::from_iter([1_u32, 17, 18]).into_array(),
        PrimitiveArray::from_iter([1_000_000_000_000_000_000_i64, 1, -1_000_000_000_000_000_000])
            .into_array(),
        &mut ctx,
    )
    .unwrap()
    .into_array();
    for fused in [false, true] {
        let columns = ["amount".into()];
        let mut states = SimpleAggregateStates::new(&request(fused), &columns).unwrap();
        assert!(
            states
                .update_direct_from_chunk(
                    &runend,
                    &columns,
                    None,
                    &mut NativeNumericAccessorWork::default(),
                    &mut ctx
                )
                .unwrap()
        );
        assert_totals(&states, 18, 16.0);
    }
    for (amount, expected) in [
        (
            PrimitiveArray::from_iter([
                1_000_000_000_000_000_000_i64,
                1,
                -1_000_000_000_000_000_000,
            ])
            .into_array(),
            1.0,
        ),
        (
            PrimitiveArray::from_iter([1e18_f64, 1.0, -1e18]).into_array(),
            0.0,
        ),
    ] {
        let columns = ["key".into(), "amount".into()];
        let array: ArrayRef = StructArray::new(
            ["key", "amount"].into(),
            vec![PrimitiveArray::from_iter([0_u8; 3]).into_array(), amount],
            3,
            Validity::NonNullable,
        )
        .into_array();
        let mut grouped = request(true);
        grouped.group_by = vec![ColumnRef::new("key").unwrap()];
        grouped.order_by = vec![VortexAggregateOrderExpr::new("key", false)];
        let mut states =
            GroupedAggregateStates::new(&grouped, None, &columns, false, false).unwrap();
        assert!(
            states
                .update_compact_direct_from_chunk(&array, &columns, None)
                .unwrap()
        );
        let (_, summary) = states.result_row_count_and_summary(None).unwrap();
        let result: serde_json::Value = serde_json::from_str(&summary).unwrap();
        assert_eq!(
            result["values"],
            serde_json::json!([{"key":0,"sum":expected,"avg":expected/3.0}])
        );
    }
}

#[test]
fn independent_relational_integer_totals_preserve_cross_batch_residues_and_float_order() {
    use crate::resident_session::ResidentVortexSession;
    use native_relational_aggregate::{Aggregate, Measure, Spec};
    use shardloom_exec::compute_pool::CancellationToken;
    use vortex::array::{
        arrays::Primitive,
        dtype::{DType, Nullability, PType},
    };

    for (amount, expected) in [
        (
            PrimitiveArray::from_option_iter([
                Some(1_000_000_000_000_000_000_i64),
                None,
                Some(1),
                Some(-1_000_000_000_000_000_000),
            ])
            .into_array(),
            1.0_f64,
        ),
        (
            PrimitiveArray::from_option_iter([Some(1e18_f64), None, Some(1.0), Some(-1e18)])
                .into_array(),
            0.0_f64,
        ),
    ] {
        for grouped in [false, true] {
            let key_dtype = DType::Primitive(PType::U8, Nullability::NonNullable);
            let output_dtype = DType::Primitive(PType::F64, Nullability::Nullable);
            let groups = if grouped {
                vec![("key".into(), key_dtype)]
            } else {
                vec![]
            };
            let mut fields = groups.clone();
            fields.extend([
                ("sum".into(), output_dtype.clone()),
                ("avg".into(), output_dtype.clone()),
            ]);
            let spec = Spec {
                fields,
                group_names: if grouped { vec!["key".into()] } else { vec![] },
                groups,
                measures: [SimpleAggregateFunction::Sum, SimpleAggregateFunction::Avg]
                    .into_iter()
                    .map(|function| Measure {
                        secondary_column: None,
                        filter_column: None,
                        quantile_level: None,
                        selection: None,
                        separator: None,
                        function: function.into(),
                        column: Some("amount".into()),
                        dtype: output_dtype.clone(),
                        decimal_source: None,
                        distinct_fields: vec![],
                        distinct_names: vec![],
                    })
                    .collect(),
            };
            let array = StructArray::new(
                ["key", "amount"].into(),
                vec![
                    PrimitiveArray::from_iter([0_u8; 4]).into_array(),
                    amount.clone(),
                ],
                4,
                Validity::NonNullable,
            )
            .into_array();
            let session = ResidentVortexSession::new(1 << 20, 1).unwrap();
            session
                .with_native_execution_context(&CancellationToken::default(), |context| {
                    let mut aggregate = Aggregate::new(&spec, context.memory())?;
                    aggregate.consume(&array.slice(0..3).unwrap(), context, 2)?;
                    aggregate.consume(&array.slice(3..4).unwrap(), context, 2)?;
                    let mut output_rows = 0;
                    aggregate.finish(context, 2, &mut |output| {
                        output_rows += output.len();
                        for (name, expected) in [("sum", expected), ("avg", expected / 3.0)] {
                            let column = logical_field_from_native_array(&output, name)?;
                            assert_eq!(
                                column.as_opt::<Primitive>().unwrap().as_slice::<f64>(),
                                &[expected]
                            );
                        }
                        Ok(())
                    })?;
                    assert_eq!(output_rows, 1);
                    Ok(())
                })
                .unwrap();
            assert_eq!(session.memory().snapshot().reserved_bytes, 0);
        }
    }
}

#[test]
fn independent_regex_domain_keeps_nulls_cache_identity_and_embedded_rewrites_separate() {
    let transform = AggregateValueTransform::RegexDomain;
    assert_eq!(transform.apply(&StatValue::Null).unwrap(), StatValue::Null);
    assert!(transform.apply(&StatValue::Int64(1)).is_err());
    assert!(embedded_derived_column_for_transform(transform, "Referer").is_none());
    assert!(
        embedded_derived_column_for_transform(AggregateValueTransform::UrlDomain, "Referer")
            .is_some()
    );
    let text = std::sync::Arc::<str>::from("http://example.test");
    let mut cache = TransformedDictionaryKeyCache::default();
    let key = |value: &str| AggregateGroupKey::single(AggregateDistinctValue::Utf8(value.into()));
    cache.insert(
        AggregateValueTransform::UrlDomain,
        text.clone(),
        key("example.test"),
    );
    cache.insert(transform, text.clone(), key(&text));
    assert_eq!(cache.len(), 2);
    assert_ne!(
        cache.get(transform, &text),
        cache.get(AggregateValueTransform::UrlDomain, &text)
    );
    assert!(matches!(
        aggregate_materialized_string_partial_key(&StatValue::Null, transform).unwrap(),
        AggregateMaterializedStringPartialKey::Null
    ));
}

#[test]
fn independent_regex_dictionary_admission_needs_shared_length_not_url_domain_metadata() {
    let length = shardloom_utf8_length_derived_column("Referer");
    let domain = shardloom_url_domain_derived_column("Referer");
    let mut request = VortexSimpleAggregateRequest::new(vec![
        VortexSimpleAggregateMeasure::new("count", None, "c".into()),
        VortexSimpleAggregateMeasure::new(
            "avg",
            Some(ColumnRef::new("Referer").unwrap()),
            "l".into(),
        )
        .with_value_transform("length"),
        VortexSimpleAggregateMeasure::new(
            "min",
            Some(ColumnRef::new("Referer").unwrap()),
            "m".into(),
        ),
    ])
    .with_group_expressions(vec![crate::VortexAggregateExpression::new(
        "k".into(),
        ColumnRef::new("Referer").unwrap(),
        "regex_domain",
    )])
    .with_order_by(vec![VortexAggregateOrderExpr::new("l", true)]);
    // Regex admission must not require metadata with different semantics. The
    // same-source contract also applies when that unrelated column is absent.
    for has_domain in [false, true] {
        let mut available = std::collections::BTreeSet::from(["Referer", length.as_str()]);
        if has_domain {
            available.insert(domain.as_str());
        }
        assert!(should_preserve_transformed_dictionary_source_lane(&request, &available).unwrap());
        let saved = request.measures[1]
            .column
            .replace(ColumnRef::new("Other").unwrap());
        assert!(!should_preserve_transformed_dictionary_source_lane(&request, &available).unwrap());
        request.measures[1].column = saved;
    }
    let available = std::collections::BTreeSet::from(["Referer", length.as_str()]);
    request.group_expressions[0].function = "url_domain".into();
    assert!(!should_preserve_transformed_dictionary_source_lane(&request, &available).unwrap());
    request.group_expressions[0].function = "regex_domain".into();
    request.order_by.clear();
    assert!(!should_preserve_transformed_dictionary_source_lane(&request, &available).unwrap());
}
