use super::{AggregateDirectColumnAccessor, NativeNumericAccessorWork};
use crate::local_primitives::{
    aggregate_column_accessor_with_work, aggregate_direct_count_distinct_update,
    aggregate_direct_numeric_sum_count, aggregate_direct_stat_value,
};
use shardloom_core::StatValue;
use vortex::{
    array::{
        ArrayRef, IntoArray as _, VortexSessionExecute as _,
        arrays::{ChunkedArray, DictArray, FilterArray, PrimitiveArray},
        dtype::PType,
        scalar::Scalar,
        validity::Validity,
    },
    encodings::{
        fastlanes::{BitPackedData, FoRData},
        runend::RunEnd,
        sparse::Sparse,
    },
    mask::Mask,
};

fn exact(array: &ArrayRef, expected: &[StatValue]) {
    let (accessor, work) = aggregate_column_accessor_with_work("renamed_measure", array).unwrap();
    assert_eq!(accessor.len(), expected.len());
    assert!(!matches!(
        accessor,
        AggregateDirectColumnAccessor::Materialized { .. }
    ));
    assert_eq!(work.calls, 1);
    assert_eq!(work.rows, expected.len() as u64);
    assert_eq!(work.typed_value_bytes_copied, 0);
    for (row, expected) in expected.iter().enumerate() {
        assert_eq!(
            &aggregate_direct_stat_value(&accessor, row).unwrap(),
            expected
        );
    }
}

#[test]
fn native_numeric_accessor_preserves_for_bitpacked_sparse_and_runend_exact_values() {
    let mut ctx = vortex::array::legacy_session().create_execution_ctx();
    let base = 1_i64 << 60;
    let for_array = FoRData::encode(
        PrimitiveArray::from_option_iter([Some(i64::MIN), None, Some(i64::MAX), Some(base + 3)]),
        &mut ctx,
    )
    .unwrap()
    .into_array();
    exact(
        &for_array,
        &[
            StatValue::Int64(i64::MIN),
            StatValue::Null,
            StatValue::Int64(i64::MAX),
            StatValue::Int64(base + 3),
        ],
    );
    let packed = BitPackedData::encode(
        &PrimitiveArray::new(vec![0_u64, 7, 1, 7], Validity::NonNullable).into_array(),
        3,
        &mut ctx,
    )
    .unwrap()
    .into_array();
    exact(
        &packed,
        &[
            StatValue::UInt64(0),
            StatValue::UInt64(7),
            StatValue::UInt64(1),
            StatValue::UInt64(7),
        ],
    );
    let sparse = Sparse::try_new(
        PrimitiveArray::new(vec![1_u64, 3], Validity::NonNullable).into_array(),
        PrimitiveArray::new(vec![u64::MAX, (1_u64 << 60) + 7], Validity::NonNullable).into_array(),
        5,
        Scalar::from(0_u64),
    )
    .unwrap()
    .into_array();
    exact(
        &sparse,
        &[
            StatValue::UInt64(0),
            StatValue::UInt64(u64::MAX),
            StatValue::UInt64(0),
            StatValue::UInt64((1_u64 << 60) + 7),
            StatValue::UInt64(0),
        ],
    );
    let runend = RunEnd::try_new(
        PrimitiveArray::new(vec![2_u32, 3, 5], Validity::NonNullable).into_array(),
        PrimitiveArray::from_option_iter([Some(i64::MIN), None, Some(base + 3)]).into_array(),
        &mut ctx,
    )
    .unwrap()
    .into_array();
    exact(
        &runend,
        &[
            StatValue::Int64(i64::MIN),
            StatValue::Int64(i64::MIN),
            StatValue::Null,
            StatValue::Int64(base + 3),
            StatValue::Int64(base + 3),
        ],
    );
    let filtered =
        FilterArray::new(for_array, Mask::from_iter([false, true, true, true])).into_array();
    exact(
        &filtered,
        &[
            StatValue::Null,
            StatValue::Int64(i64::MAX),
            StatValue::Int64(base + 3),
        ],
    );
}

#[test]
fn native_numeric_accessor_keeps_float_bits_nulls_empty_and_existing_fast_paths() {
    let values = PrimitiveArray::from_option_iter([
        Some(-0.0_f64),
        None,
        Some(f64::MIN),
        Some(f64::MAX),
        Some(0.25),
    ])
    .into_array();
    let chunked = ChunkedArray::try_new([values.clone()], values.dtype().clone())
        .unwrap()
        .into_array();
    let (accessor, work) = aggregate_column_accessor_with_work("floating", &chunked).unwrap();
    assert_eq!(work.calls, 1);
    let AggregateDirectColumnAccessor::NativeNumeric(owner) = accessor else {
        panic!("nullable f64 remains typed");
    };
    assert_eq!(
        owner.numeric_value(0).unwrap().unwrap().to_bits(),
        (-0.0_f64).to_bits()
    );
    assert_eq!(owner.null_rows(true), vec![1]);
    assert_eq!(
        &owner.primitive().as_slice::<f64>()[2..],
        &[f64::MIN, f64::MAX, 0.25]
    );
    let values =
        PrimitiveArray::from_option_iter([Some(f32::MIN), None, Some(f32::MAX)]).into_array();
    let chunked = ChunkedArray::try_new([values.clone()], values.dtype().clone())
        .unwrap()
        .into_array();
    exact(
        &chunked,
        &[
            StatValue::Float64(f64::from(f32::MIN)),
            StatValue::Null,
            StatValue::Float64(f64::from(f32::MAX)),
        ],
    );
    let values = PrimitiveArray::from_option_iter([None::<i64>; 3]).into_array();
    let chunked = ChunkedArray::try_new([values.clone()], values.dtype().clone())
        .unwrap()
        .into_array();
    exact(
        &chunked,
        &[StatValue::Null, StatValue::Null, StatValue::Null],
    );
    let values = PrimitiveArray::new(Vec::<i64>::new(), Validity::NonNullable).into_array();
    let chunked = ChunkedArray::try_new([values.clone()], values.dtype().clone())
        .unwrap()
        .into_array();
    exact(&chunked, &[]);
    let direct = PrimitiveArray::new(vec![7_i64, i64::MAX], Validity::NonNullable).into_array();
    assert_eq!(
        aggregate_column_accessor_with_work("direct", &direct)
            .unwrap()
            .1
            .calls,
        0
    );
    let dictionary = DictArray::try_new(
        PrimitiveArray::new(vec![1_u8, 0, 1], Validity::NonNullable).into_array(),
        direct,
    )
    .unwrap()
    .into_array();
    let (accessor, work) = aggregate_column_accessor_with_work("dictionary", &dictionary).unwrap();
    assert_eq!(work.calls, 0);
    assert_eq!(accessor.i64_values().unwrap(), &[i64::MAX, 7, i64::MAX]);
}

#[test]
fn native_numeric_materialization_display_preserves_generic_columns_and_exact_sidecar() {
    use crate::local_primitives::{
        VortexLocalPrimitiveEmbeddedLayoutReport,
        annotate_simple_aggregate_layout_correlation_summary,
    };
    let work = NativeNumericAccessorWork {
        calls: 1,
        columns: ["measure,東京".to_owned(), "shared".to_owned()].into(),
        ..NativeNumericAccessorWork::default()
    };
    let original = serde_json::json!({
        "aggregate_materialized_accessor_columns": "generic,shared",
        "values": {"exact_integer": 9_007_199_254_740_993_u64},
    });
    let mut summary = original.to_string();
    work.annotate(&mut summary).unwrap();
    annotate_simple_aggregate_layout_correlation_summary(
        &mut summary,
        &VortexLocalPrimitiveEmbeddedLayoutReport::not_available(),
    )
    .unwrap();
    let actual: serde_json::Value = serde_json::from_str(&summary).unwrap();
    assert_eq!(
        actual["aggregate_materialized_accessor_columns"],
        "generic,measure,東京,shared"
    );
    assert_eq!(
        actual["aggregate_accessor_layout_correlation_columns"],
        "generic,measure,東京,shared"
    );
    assert_eq!(
        actual["aggregate_accessor_layout_correlation_status"],
        "artifact_layout_unavailable_accessor_materialized"
    );
    assert_eq!(
        actual["aggregate_native_numeric_accessor"]["columns"],
        serde_json::json!(["measure,東京", "shared"])
    );
    assert_eq!(actual["values"], original["values"]);
    let mut invalid_display = "{\"aggregate_materialized_accessor_columns\":7}".to_owned();
    assert!(work.annotate(&mut invalid_display).is_err());
    assert_eq!(
        invalid_display,
        "{\"aggregate_materialized_accessor_columns\":7}"
    );
}

#[test]
fn no_numeric_decode_preserves_existing_materialization_evidence() {
    let original = serde_json::json!({
        "aggregate_materialized_accessor_columns": "none",
        "aggregate_accessor_materialization_status": "vortex_dictionary_or_primitive_only",
        "aggregate_accessor_layout_correlation_status": "not_required_no_materialized_accessors",
        "values": {"exact_integer": 9_007_199_254_740_993_u64},
    });
    let mut summary = original.to_string();
    NativeNumericAccessorWork::default()
        .annotate(&mut summary)
        .unwrap();
    let mut actual: serde_json::Value = serde_json::from_str(&summary).unwrap();
    assert_eq!(
        actual["aggregate_native_numeric_accessor"]["native_decode_calls"],
        0
    );
    actual
        .as_object_mut()
        .unwrap()
        .remove("aggregate_native_numeric_accessor");
    assert_eq!(actual, original);
}

#[test]
fn native_numeric_owner_all_widths_keep_payload_identity_and_sliced_validity() {
    macro_rules! check {
        ($ty:ty, $ptype:ident, $variant:ident, $seed:ty) => {{
            let seeds: [$seed; 4] = [3, 7, 99, 88];
            let [three, seven, ninety_nine, eighty_eight] = seeds.map(<$ty>::from);
            let input = PrimitiveArray::from_option_iter([
                Some(ninety_nine),
                Some(three),
                None,
                Some(seven),
                Some(three),
                Some(eighty_eight),
            ])
            .into_array()
            .slice(1..5)
            .unwrap();
            let mut ctx = vortex::array::legacy_session().create_execution_ctx();
            let before = input.clone().execute::<PrimitiveArray>(&mut ctx).unwrap();
            let pointer = before.as_slice::<$ty>().as_ptr();
            let (accessor, work) =
                aggregate_column_accessor_with_work("narrow_alias", &input).unwrap();
            let AggregateDirectColumnAccessor::NativeNumeric(owner) = &accessor else {
                panic!("native owner required");
            };
            assert_eq!(owner.ptype(), PType::$ptype);
            assert_eq!(owner.primitive().as_slice::<$ty>().as_ptr(), pointer);
            assert_eq!(work.typed_value_bytes_copied, 0);
            drop(before);
            drop(input);
            drop(ctx);
            let expected = [
                StatValue::$variant(three.into()),
                StatValue::Null,
                StatValue::$variant(seven.into()),
                StatValue::$variant(three.into()),
            ];
            for (row, value) in expected.iter().enumerate() {
                assert_eq!(&aggregate_direct_stat_value(&accessor, row).unwrap(), value);
            }
            assert_eq!(owner.null_rows(true), vec![1]);
            assert_eq!(owner.non_null_count(Some(&[3, 1, 0, 3])).unwrap(), 3);
            assert_eq!(
                aggregate_direct_numeric_sum_count(&accessor, None).unwrap(),
                (3, 13.0)
            );
            assert_eq!(
                owner
                    .compare_rows(
                        "narrow_alias",
                        shardloom_core::ComparisonOp::Gt,
                        &StatValue::$variant(three.into())
                    )
                    .unwrap(),
                vec![2]
            );
            assert_eq!(
                owner
                    .in_rows(
                        "narrow_alias",
                        &[StatValue::$variant(three.into()), StatValue::Null],
                        false
                    )
                    .unwrap(),
                vec![0, 1, 3]
            );
            assert_eq!(
                owner
                    .in_count(
                        "narrow_alias",
                        &[StatValue::$variant(three.into()), StatValue::Null],
                        true
                    )
                    .unwrap(),
                1
            );
            let mut distinct = Default::default();
            let report = aggregate_direct_count_distinct_update(
                &accessor,
                Some(&[3, 0, 1, 2]),
                &mut distinct,
            )
            .unwrap();
            assert_eq!(report.non_null_rows, 3);
            assert_eq!(distinct.len(), 2);
            assert_eq!(report.dense_integer_preunion_used, owner.is_integer());
            assert!(owner.numeric_value(4).is_err());
            assert!(owner.non_null_count(Some(&[4])).is_err());
        }};
    }
    check!(u8, U8, UInt64, u8);
    check!(u16, U16, UInt64, u8);
    check!(u32, U32, UInt64, u8);
    check!(u64, U64, UInt64, u8);
    check!(i8, I8, Int64, i8);
    check!(i16, I16, Int64, i8);
    check!(i32, I32, Int64, i8);
    check!(i64, I64, Int64, i8);
    check!(f32, F32, Float64, i8);
    check!(f64, F64, Float64, i8);
}

#[test]
fn native_numeric_owner_float_sum_preserves_widening_order_and_selection_duplicates() {
    // f32 accumulation would lose the 1.0; native values must widen before each
    // addition. The f64 profile separately detects reassociated accumulation.
    for input in [
        PrimitiveArray::from_option_iter([
            Some(16_777_216_f32),
            Some(1.0),
            None,
            Some(-16_777_216.0),
        ])
        .into_array(),
        PrimitiveArray::from_option_iter([Some(1e16_f64), Some(1.0), None, Some(-1e16)])
            .into_array(),
    ] {
        let expected = if matches!(
            input.dtype(),
            vortex::array::dtype::DType::Primitive(vortex::array::dtype::PType::F32, _)
        ) {
            1.0
        } else {
            0.0
        };
        let (accessor, _) = aggregate_column_accessor_with_work("ordered_measure", &input).unwrap();
        assert_eq!(
            aggregate_direct_numeric_sum_count(&accessor, None).unwrap(),
            (3, expected)
        );
        assert_eq!(
            aggregate_direct_numeric_sum_count(&accessor, Some(&[0, 3, 1, 1])).unwrap(),
            (4, 2.0)
        );
    }
    let input = PrimitiveArray::from_option_iter([Some(f64::INFINITY), None]).into_array();
    let (accessor, _) = aggregate_column_accessor_with_work("nonfinite", &input).unwrap();
    assert!(aggregate_direct_numeric_sum_count(&accessor, None).is_err());
    assert_eq!(
        aggregate_direct_numeric_sum_count(&accessor, Some(&[1])).unwrap(),
        (0, 0.0)
    );
}

#[test]
fn native_numeric_owner_nullable_transforms_preserve_existing_errors_and_values() {
    use crate::local_primitives::{
        aggregate_direct_add_offset_key, aggregate_direct_date_trunc_minute_key,
        aggregate_direct_extract_minute_key,
    };
    let legacy = AggregateDirectColumnAccessor::NullableInt64 {
        values: vec![-61, 0, 121],
        row_nulls: vec![false, true, false],
    };
    let input = PrimitiveArray::from_option_iter([Some(-61_i16), None, Some(121)]).into_array();
    let (native, _) = aggregate_column_accessor_with_work("parity", &input).unwrap();
    for row in 0..3 {
        for (old, new) in [
            (
                aggregate_direct_add_offset_key(&legacy, row, 2),
                aggregate_direct_add_offset_key(&native, row, 2),
            ),
            (
                aggregate_direct_extract_minute_key(&legacy, row),
                aggregate_direct_extract_minute_key(&native, row),
            ),
            (
                aggregate_direct_date_trunc_minute_key(&legacy, row),
                aggregate_direct_date_trunc_minute_key(&native, row),
            ),
        ] {
            match (old, new) {
                (Ok(old), Ok(new)) => assert_eq!(old, new),
                (Err(old), Err(new)) => assert_eq!(old.to_string(), new.to_string()),
                _ => panic!("native owner changed nullable transform admission"),
            }
        }
    }
    let legacy_float = AggregateDirectColumnAccessor::NullableFloat64 {
        values: vec![0.0],
        row_nulls: vec![true],
    };
    for input in [
        PrimitiveArray::from_option_iter([None::<f32>]).into_array(),
        PrimitiveArray::from_option_iter([None::<f64>]).into_array(),
    ] {
        let (native, _) = aggregate_column_accessor_with_work("nullable_float", &input).unwrap();
        assert_eq!(
            aggregate_direct_date_trunc_minute_key(&legacy_float, 0)
                .unwrap_err()
                .to_string(),
            aggregate_direct_date_trunc_minute_key(&native, 0)
                .unwrap_err()
                .to_string(),
        );
    }
    let input = PrimitiveArray::new(vec![i64::MAX], Validity::NonNullable).into_array();
    let (native, _) = aggregate_column_accessor_with_work("overflow", &input).unwrap();
    assert!(
        aggregate_direct_add_offset_key(&native, 0, 1)
            .unwrap_err()
            .to_string()
            .contains("direct int64 offset key overflowed")
    );
}

#[test]
#[allow(clippy::too_many_lines)] // One cross-width matrix verifies owners, exact keys, selections, and stop-on-error.
fn native_numeric_integer_views_dispatch_all_width_pairs_without_copying() {
    use crate::local_primitives::{
        AggregateDirectIntegerKeySlice, aggregate_direct_integer_key_slice,
    };
    let mut fixtures = Vec::new();
    macro_rules! fixture {
        ($ty:ty, $variant:ident, $signed:expr, $wide:ty) => {{
            let values = [<$ty>::MIN, <$ty>::MAX, 0];
            let expected = values.map(|value| {
                let wide = <$wide>::from(value);
                (wide.to_ne_bytes(), $signed)
            });
            let array = PrimitiveArray::new(values.to_vec(), Validity::NonNullable).into_array();
            let (accessor, work) =
                aggregate_column_accessor_with_work("integer_alias", &array).unwrap();
            assert_eq!(work.typed_value_bytes_copied, 0);
            let keys = aggregate_direct_integer_key_slice(&accessor).unwrap();
            let AggregateDirectIntegerKeySlice::$variant(slice) = keys else {
                panic!("original width must remain borrowed")
            };
            let AggregateDirectColumnAccessor::NativeNumeric(owner) = &accessor else {
                panic!("native owner")
            };
            assert_eq!(slice.as_ptr(), owner.primitive().as_slice::<$ty>().as_ptr());
            drop(array);
            fixtures.push((
                accessor,
                expected.map(|(bytes, signed)| (u64::from_ne_bytes(bytes), signed)),
            ));
        }};
    }
    fixture!(u8, UInt8, false, u64);
    fixture!(u16, UInt16, false, u64);
    fixture!(u32, UInt32, false, u64);
    fixture!(u64, UInt64, false, u64);
    fixture!(i8, Int8, true, i64);
    fixture!(i16, Int16, true, i64);
    fixture!(i32, Int32, true, i64);
    fixture!(i64, Int64, true, i64);
    for (first, first_expected) in &fixtures {
        let first = aggregate_direct_integer_key_slice(first).unwrap();
        let mut selected = Vec::new();
        first
            .for_each(Some(&[2, 0, 2]), |row, key| {
                selected.push((row, key.bits, key.signed));
                Ok(())
            })
            .unwrap();
        assert_eq!(
            selected,
            [2, 0, 2].map(|row| (row, first_expected[row].0, first_expected[row].1))
        );
        for (second, second_expected) in &fixtures {
            let second = aggregate_direct_integer_key_slice(second).unwrap();
            for selection in [None, Some([2_usize, 0, 2].as_slice())] {
                let mut actual = Vec::new();
                first
                    .for_each_pair(second, selection, |row, pair| {
                        actual.push((row, pair.first_bits, pair.second_bits, pair.key_kinds));
                        Ok(())
                    })
                    .unwrap();
                let expected_rows = selection.unwrap_or(&[0, 1, 2]);
                let expected = expected_rows
                    .iter()
                    .map(|&row| {
                        (
                            row,
                            first_expected[row].0,
                            second_expected[row].0,
                            u8::from(first_expected[row].1)
                                | (u8::from(second_expected[row].1) << 1),
                        )
                    })
                    .collect::<Vec<_>>();
                assert_eq!(actual, expected);
            }
        }
        let mut visited = 0;
        assert!(
            first
                .for_each_pair(AggregateDirectIntegerKeySlice::UInt8(&[]), None, |_, _| {
                    visited += 1;
                    Ok(())
                })
                .is_err()
        );
        assert_eq!(visited, 0);
        assert!(
            first
                .for_each(Some(&[3]), |_, _| {
                    visited += 1;
                    Ok(())
                })
                .is_err()
        );
        assert_eq!(visited, 0);
        first
            .for_each_pair(first, Some(&[]), |_, _| {
                visited += 1;
                Ok(())
            })
            .unwrap();
        assert_eq!(visited, 0);
        let error = first
            .for_each_pair(first, None, |row, _| {
                visited += 1;
                if row == 1 {
                    return Err(shardloom_core::ShardLoomError::InvalidOperation(
                        "stop typed loop".into(),
                    ));
                }
                Ok(())
            })
            .unwrap_err();
        assert!(error.to_string().contains("stop typed loop"));
        assert_eq!(visited, 2);
    }
}

#[test]
fn native_numeric_integer_views_preserve_nullable_and_materialized_boundaries() {
    use crate::local_primitives::{
        aggregate_direct_integer_key_slice, numeric_utf8_topk_exact_chunk_counts,
    };
    let input = PrimitiveArray::from_option_iter([Some(7_i16), None, Some(9)]).into_array();
    let (nullable, _) = aggregate_column_accessor_with_work("nullable_key", &input).unwrap();
    assert!(aggregate_direct_integer_key_slice(&nullable).is_none());
    let selected = FilterArray::new(input, Mask::from_iter([true, false, true])).into_array();
    let (selected, _) = aggregate_column_accessor_with_work("selected_key", &selected).unwrap();
    assert!(aggregate_direct_integer_key_slice(&selected).is_some());
    let materialized =
        AggregateDirectColumnAccessor::materialized(vec![StatValue::Null], "test boundary");
    assert!(aggregate_direct_integer_key_slice(&materialized).is_none());
    // The absent dictionary candidate skips a numeric null before lookup, as it
    // did on the existing materialized/nullable path. The view must not broaden admission.
    assert!(
        numeric_utf8_topk_exact_chunk_counts(&materialized, &[0], &[None])
            .unwrap()
            .is_empty()
    );
    assert!(
        numeric_utf8_topk_exact_chunk_counts(&nullable, &[0, 0, 0], &[None])
            .unwrap()
            .is_empty()
    );
}

fn native_integer_fixture(array: ArrayRef) -> AggregateDirectColumnAccessor {
    let (accessor, work) = aggregate_column_accessor_with_work("fixture_alias", &array).unwrap();
    assert!(matches!(
        accessor,
        AggregateDirectColumnAccessor::NativeNumeric(_)
    ));
    assert_eq!(work.typed_value_bytes_copied, 0);
    // Every fixture consumer below must work after the original array owner drops.
    drop(array);
    accessor
}

#[test]
#[allow(clippy::too_many_lines)] // Raw signed/unsigned and prepared-minute representations share one independent oracle.
fn native_numeric_narrow_minute_dictionary_strategy_matches_renamed_reference() {
    use crate::local_primitives::{AggregateUtf8DictionarySource, GroupedAggregateStates};
    use crate::{
        VortexAggregateExpression, VortexAggregateOrderExpr, VortexSimpleAggregateMeasure,
        VortexSimpleAggregateRequest,
    };
    use shardloom_core::ColumnRef;
    use std::{collections::BTreeMap, sync::Arc};
    let actors = [42_u16, 42, 7, 7, 42, 9];
    let labels = ["alpha", "alpha", "beta", "beta", "gamma", "gamma"];
    let raw_times = [60_i32, 61, 120, 121, 62, -1];
    let mut counts = BTreeMap::new();
    for ((actor, time), label) in actors.into_iter().zip(raw_times).zip(labels) {
        *counts
            .entry((actor, time.rem_euclid(3600) / 60, label))
            .or_insert(0_u64) += 1;
    }
    let mut expected = counts.into_iter().collect::<Vec<_>>();
    expected.sort_by(|(left, lc), (right, rc)| rc.cmp(lc).then_with(|| left.cmp(right)));
    let expected = expected.into_iter().skip(1).take(3).map(|((actor, minute, phrase), count)| serde_json::json!({"actor_alias":actor,"phrase_alias":phrase,"minute_alias":minute,"n_alias":count})).collect::<Vec<_>>();
    let clocks = [
        (
            PrimitiveArray::new(raw_times.to_vec(), Validity::NonNullable).into_array(),
            false,
        ),
        (
            PrimitiveArray::new(vec![60_u32, 61, 120, 121, 62, 3599], Validity::NonNullable)
                .into_array(),
            false,
        ),
        (
            PrimitiveArray::new(vec![1_u8, 1, 2, 2, 1, 59], Validity::NonNullable).into_array(),
            true,
        ),
    ];
    for (clock, prepared) in clocks {
        let clock_name = if prepared {
            "__shardloom_derived_extract_minute_clock_alias"
        } else {
            "clock_alias"
        };
        let request = VortexSimpleAggregateRequest::grouped(
            vec![
                ColumnRef::new("actor_alias").unwrap(),
                ColumnRef::new("phrase_alias").unwrap(),
            ],
            vec![VortexSimpleAggregateMeasure::new(
                "count",
                None,
                "n_alias".into(),
            )],
        )
        .with_group_expressions(vec![VortexAggregateExpression::new(
            "minute_alias".into(),
            ColumnRef::new(clock_name).unwrap(),
            if prepared {
                "identity"
            } else {
                "extract_minute"
            },
        )])
        .with_order_by(vec![VortexAggregateOrderExpr::new("n_alias", true)])
        .with_offset(1);
        let columns = vec![
            "actor_alias".into(),
            "phrase_alias".into(),
            clock_name.into(),
        ];
        let mut states =
            GroupedAggregateStates::new(&request, Some(3), &columns, false, false).unwrap();
        let accessors = vec![
            native_integer_fixture(
                PrimitiveArray::new(actors.to_vec(), Validity::NonNullable).into_array(),
            ),
            AggregateDirectColumnAccessor::Utf8Dictionary {
                row_ids: vec![0, 0, 1, 1, 2, 2],
                values: ["alpha", "beta", "gamma"].map(Arc::<str>::from).to_vec(),
                value_nulls: None,
                row_nulls: None,
                source: AggregateUtf8DictionarySource::VortexDictArray,
            },
            native_integer_fixture(clock),
        ];
        assert!(
            states
                .update_numeric_minute_string_count_direct_from_accessors(&accessors, None)
                .unwrap()
        );
        assert!(states.numeric_minute_string_direct_slice_updates);
        assert_eq!(states.numeric_minute_string_direct_slice_update_rows, 6);
        assert_eq!(
            states
                .numeric_minute_string_group_roles
                .unwrap()
                .minute_column_prepared,
            prepared
        );
        let (rows, summary) = states.result_row_count_and_summary(Some(3)).unwrap();
        let payload: serde_json::Value = serde_json::from_str(&summary).unwrap();
        assert_eq!(rows, 3);
        assert_eq!(payload["numeric_minute_string_direct_slice_updates"], true);
        assert_eq!(payload["values"], serde_json::json!(expected));
    }
}

#[test]
#[allow(clippy::too_many_lines)] // One near-unique fixture proves both passes, mixed measures, and offset/tie ordering.
fn native_numeric_narrow_pair_near_unique_strategy_matches_mixed_measure_reference() {
    use crate::local_primitives::{
        AggregateDirectIntegerKeySlice, GroupedAggregateStates, aggregate_direct_integer_key_slice,
    };
    use crate::{
        VortexAggregateOrderExpr, VortexSimpleAggregateMeasure, VortexSimpleAggregateRequest,
    };
    use shardloom_core::ColumnRef;
    use std::collections::BTreeMap;
    let mut actors = (0..20_000_u32)
        .map(|row| (1_u64 << 60) + u64::from(row))
        .collect::<Vec<_>>();
    let mut regions = (0..20_000_u32).collect::<Vec<_>>();
    let mut flags = vec![0_u8; actors.len()];
    let mut widths = vec![100_u16; actors.len()];
    for (row, source) in [(100, 42), (101, 42), (200, 7)] {
        actors[row] = actors[source];
        regions[row] = regions[source];
    }
    flags[42] = 1;
    flags[100] = 1;
    widths[100] = 300;
    widths[101] = 200;
    let mut reference = BTreeMap::new();
    for row in 0..actors.len() {
        let entry = reference
            .entry((actors[row], regions[row]))
            .or_insert((0_u32, 0_u32, 0_u32));
        entry.0 += 1;
        entry.1 += u32::from(flags[row]);
        entry.2 += u32::from(widths[row]);
    }
    let mut reference = reference.into_iter().collect::<Vec<_>>();
    reference.sort_by(|(left, lc), (right, rc)| rc.0.cmp(&lc.0).then_with(|| left.cmp(right)));
    let accessors = vec![
        native_integer_fixture(PrimitiveArray::new(actors, Validity::NonNullable).into_array()),
        native_integer_fixture(PrimitiveArray::new(regions, Validity::NonNullable).into_array()),
        native_integer_fixture(PrimitiveArray::new(flags, Validity::NonNullable).into_array()),
        native_integer_fixture(PrimitiveArray::new(widths, Validity::NonNullable).into_array()),
    ];
    assert!(matches!(
        aggregate_direct_integer_key_slice(&accessors[0]),
        Some(AggregateDirectIntegerKeySlice::UInt64(_))
    ));
    assert!(matches!(
        aggregate_direct_integer_key_slice(&accessors[1]),
        Some(AggregateDirectIntegerKeySlice::UInt32(_))
    ));
    for offset in [0, 1] {
        let request = VortexSimpleAggregateRequest::grouped(
            vec![
                ColumnRef::new("actor_alias").unwrap(),
                ColumnRef::new("region_alias").unwrap(),
            ],
            vec![
                VortexSimpleAggregateMeasure::new("count", None, "n_alias".into()),
                VortexSimpleAggregateMeasure::new(
                    "sum",
                    Some(ColumnRef::new("flag_alias").unwrap()),
                    "sum_alias".into(),
                ),
                VortexSimpleAggregateMeasure::new(
                    "avg",
                    Some(ColumnRef::new("width_alias").unwrap()),
                    "avg_alias".into(),
                ),
            ],
        )
        .with_order_by(vec![VortexAggregateOrderExpr::new("n_alias", true)])
        .with_offset(offset);
        let columns = ["actor_alias", "region_alias", "flag_alias", "width_alias"]
            .map(str::to_string)
            .to_vec();
        let mut states =
            GroupedAggregateStates::new(&request, Some(4), &columns, true, false).unwrap();
        assert!(
            states
                .update_numeric_pair_late_measure_count_direct_from_accessors(&accessors, None)
                .unwrap()
        );
        assert!(states.numeric_pair_late_measure_near_unique_directory_updates);
        assert!(states.numeric_pair_direct_key_slice_updates);
        states
            .prepare_numeric_pair_late_measure_second_pass(Some(4))
            .unwrap();
        assert!(
            states
                .numeric_pair_late_measure_near_unique_directory
                .is_none()
        );
        assert!(
            states
                .update_numeric_pair_late_measure_direct_from_accessors(&accessors)
                .unwrap()
        );
        let (rows, summary) = states.result_row_count_and_summary(Some(4)).unwrap();
        let payload: serde_json::Value = serde_json::from_str(&summary).unwrap();
        let expected = reference.iter().skip(offset).take(4).map(|((actor, region), (count, sum, width))| serde_json::json!({"actor_alias":actor,"region_alias":region,"n_alias":count,"sum_alias":f64::from(*sum),"avg_alias":f64::from(*width)/f64::from(*count)})).collect::<Vec<_>>();
        assert_eq!(rows, 4);
        assert_eq!(
            payload["aggregate_update_strategy"],
            "numeric_pair_near_unique_directory_count_topk_late_measure_second_pass"
        );
        assert_eq!(payload["values"], serde_json::json!(expected));
    }
}

#[test]
fn native_numeric_owner_credits_follow_payload_and_pinned_builder_gap_is_explicit() {
    use crate::{
        local_primitives::aggregate_column_accessor_in_context,
        owned_buffers::ReservedHostAllocator,
    };
    use shardloom_exec::live_memory::LiveMemoryPool;
    use std::sync::Arc;
    use vortex::{
        VortexSessionDefault as _,
        array::memory::{HostAllocator as _, MemorySessionExt as _},
        buffer::{Alignment, Buffer},
        session::VortexSession,
    };
    let memory = LiveMemoryPool::new(4096).unwrap();
    let allocator = ReservedHostAllocator::new(memory.clone());
    let mut bytes = allocator.allocate(16, Alignment::new(8)).unwrap();
    bytes.as_mut_slice()[..8].copy_from_slice(&i64::MIN.to_ne_bytes());
    bytes.as_mut_slice()[8..].copy_from_slice(&i64::MAX.to_ne_bytes());
    let input = PrimitiveArray::new(
        Buffer::<i64>::from_byte_buffer(bytes.freeze()),
        Validity::NonNullable,
    )
    .into_array();
    let retained = memory.snapshot().reserved_bytes;
    let session = VortexSession::default().with_allocator(Arc::new(allocator));
    let mut ctx = session.create_execution_ctx();
    let (accessor, work) = aggregate_column_accessor_in_context("owned", &input, &mut ctx).unwrap();
    assert_eq!(work.calls, 0);
    assert_eq!(memory.snapshot().reserved_bytes, retained);
    drop(input);
    drop(ctx);
    drop(session);
    assert_eq!(memory.snapshot().reserved_bytes, retained);
    assert_eq!(
        aggregate_direct_stat_value(&accessor, 1).unwrap(),
        StatValue::Int64(i64::MAX)
    );
    drop(accessor);
    assert_eq!(memory.snapshot().reserved_bytes, 0);

    let denied_memory = LiveMemoryPool::new(1).unwrap();
    let denied_session = VortexSession::default()
        .with_allocator(Arc::new(ReservedHostAllocator::new(denied_memory.clone())));
    let mut denied_ctx = denied_session.create_execution_ctx();
    // Pinned Chunked calls builder_with_capacity_in, whose implementation
    // discards the allocator and uses the default builder. This upgrade-sensitive
    // assertion records the coverage gap; it is not an admitted memory bound.
    let child =
        PrimitiveArray::new((0..512_i64).collect::<Vec<_>>(), Validity::NonNullable).into_array();
    let input = ChunkedArray::try_new([child.clone(), child.clone()], child.dtype().clone())
        .unwrap()
        .into_array();
    let (accessor, work) =
        aggregate_column_accessor_in_context("untracked_provider_builder", &input, &mut denied_ctx)
            .unwrap();
    assert_eq!(work.calls, 1);
    assert_eq!(work.typed_value_bytes_copied, 0);
    assert_eq!(
        accessor.i64_values().unwrap(),
        (0..512_i64).chain(0..512).collect::<Vec<_>>()
    );
    assert_eq!(denied_memory.snapshot().denied_reservations, 0);
    assert_eq!(denied_memory.snapshot().peak_reserved_bytes, 0);
    drop(accessor);
    assert_eq!(denied_memory.snapshot().reserved_bytes, 0);
}

#[test]
fn native_numeric_owner_propagates_real_corrupt_zstd_payload_error() {
    use crate::local_primitives::aggregate_column_accessor_in_context;
    use vortex::encodings::zstd::{Zstd, ZstdData};
    let mut ctx = vortex::array::legacy_session().create_execution_ctx();
    let primitive = PrimitiveArray::new((0..512_i64).collect::<Vec<_>>(), Validity::NonNullable);
    let data = ZstdData::from_primitive_without_dict(&primitive, 0, 1024, &mut ctx).unwrap();
    let mut parts = data.into_parts(Validity::NonNullable);
    assert_eq!(parts.frames.len(), 1);
    let frame = parts.frames.pop().unwrap();
    assert!(frame.len() > 16);
    // Keep a valid frame header/content-size declaration while truncating the
    // compressed payload. Native construction succeeds; native decoding fails.
    let data = ZstdData::new(
        parts.dictionary,
        vec![frame.slice(..frame.len() - 1)],
        parts.metadata,
        parts.n_rows,
    );
    let corrupt = Zstd::try_new(primitive.dtype().clone(), data, parts.validity)
        .unwrap()
        .into_array();
    let provider_error = corrupt
        .clone()
        .execute::<PrimitiveArray>(&mut ctx)
        .unwrap_err()
        .to_string();
    let error = aggregate_column_accessor_in_context("corrupt_numeric", &corrupt, &mut ctx)
        .err()
        .expect("native provider error must propagate");
    assert!(error.to_string().contains(&provider_error), "{error}");
}

#[cfg(feature = "vortex-write")]
#[test]
#[allow(clippy::too_many_lines)] // Persisted renamed two-key grouping and scalar consumption share one fixture.
fn persisted_numeric_utf8_grouping_uses_typed_native_decode_and_exact_reference() {
    use crate::local_primitives::native_flat_layout::SequentialNativeFlatLayout;
    use crate::local_primitives::{
        VortexLocalPrimitiveExecutionPolicy, aggregate_direct_column_accessors_from_chunk,
        execute_vortex_local_primitive_with_policy,
    };
    use crate::{
        VortexAggregateOrderExpr, VortexQueryPrimitiveRequest, VortexSimpleAggregateMeasure,
        VortexSimpleAggregateRequest,
    };
    use shardloom_core::{ColumnRef, DatasetUri};
    use std::collections::BTreeMap;
    use vortex::{
        VortexSessionDefault as _,
        array::{
            arrays::{StructArray, VarBinViewArray},
            dtype::FieldNames,
        },
        encodings::fastlanes::FoR,
        file::WriteOptionsSessionExt as _,
        io::{
            runtime::{BlockingRuntime as _, current::CurrentThreadRuntime},
            session::RuntimeSessionExt as _,
        },
        session::VortexSession,
    };
    struct Temporary(std::path::PathBuf);
    impl Drop for Temporary {
        fn drop(&mut self) {
            let _ = std::fs::remove_file(&self.0);
        }
    }
    let path = Temporary(std::env::temp_dir().join(format!(
            "shardloom-numeric-group-{}-{}.vortex",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        )));
    let runtime = CurrentThreadRuntime::new();
    let session = VortexSession::default().with_handle(runtime.handle());
    let mut ctx = session.create_execution_ctx();
    let mut expected = BTreeMap::<(i64, String), u64>::new();
    let mut keys = Vec::new();
    let mut labels = Vec::new();
    for group in 0_i64..11 {
        let key = (1_i64 << 60) + group % 4;
        let label = format!("renamed-東京-{group}");
        for _ in 0..=group {
            keys.push(key);
            labels.push(label.clone());
            *expected.entry((key, label.clone())).or_default() += 1;
        }
    }
    let offsets = PrimitiveArray::new(
        keys.iter()
            .map(|key| key - (1_i64 << 60))
            .collect::<Vec<_>>(),
        Validity::NonNullable,
    )
    .into_array();
    let packed = BitPackedData::encode(&offsets, 2, &mut ctx)
        .unwrap()
        .into_array();
    let encoded = FoR::try_new(packed, Scalar::from(1_i64 << 60))
        .unwrap()
        .into_array();
    let array = StructArray::try_new(
        FieldNames::from(["renamed_key", "renamed_phrase"]),
        vec![
            encoded,
            VarBinViewArray::from_iter_str(labels.iter().map(String::as_str)).into_array(),
        ],
        keys.len(),
        Validity::NonNullable,
    )
    .unwrap()
    .into_array();
    let batch = aggregate_direct_column_accessors_from_chunk(
        &array,
        &["renamed_key".into(), "renamed_phrase".into()],
        &mut ctx,
    )
    .unwrap();
    assert_eq!(batch[0].i64_values().unwrap(), keys);
    assert_eq!(batch.numeric_work.calls, 1);
    let mut output = std::fs::File::create(&path.0).unwrap();
    session
        .write_options()
        .with_strategy(SequentialNativeFlatLayout::strategy(1))
        .with_file_statistics(Vec::new())
        .blocking(&runtime)
        .write(&mut output, array.to_array_iterator())
        .unwrap();
    drop(output);
    let uri = DatasetUri::new(path.0.display().to_string()).unwrap();
    let request = VortexQueryPrimitiveRequest::simple_aggregate(
        uri.clone(),
        VortexSimpleAggregateRequest::grouped(
            vec![
                ColumnRef::new("renamed_key").unwrap(),
                ColumnRef::new("renamed_phrase").unwrap(),
            ],
            vec![VortexSimpleAggregateMeasure::new("count", None, "n".into())],
        )
        .with_order_by(vec![VortexAggregateOrderExpr::new("n", true)]),
    )
    .with_source_order_limit(7);
    // Prove the same publicly admitted query uses its exact configured context
    // at numeric execution, and propagates failure without retry. This explicit
    // session-local test effect makes no claim that FoR honors HostAllocator;
    // the real malformed-codec test above covers native error propagation.
    let denied_memory = shardloom_exec::live_memory::LiveMemoryPool::new(1).unwrap();
    let assert_query_denied = |request: &VortexQueryPrimitiveRequest| {
        use vortex::{array::memory::MemorySessionExt as _, file::OpenOptionsSessionExt as _};
        let file = runtime
            .block_on(session.open_options().open_path(&path.0))
            .unwrap();
        let allocator: vortex::array::memory::HostAllocatorRef = std::sync::Arc::new(
            crate::owned_buffers::ReservedHostAllocator::new(denied_memory.clone()),
        );
        let calls = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let denied_session = VortexSession::default()
            .with_handle(runtime.handle())
            .with_allocator(std::sync::Arc::clone(&allocator))
            .with_some(super::QueryContextProbe {
                allocator,
                calls: std::sync::Arc::clone(&calls),
            });
        let error = crate::local_primitives::read_prepared_vortex_simple_aggregate_scan(
            request.source_uri.as_ref().unwrap(),
            request,
            VortexLocalPrimitiveExecutionPolicy::single_threaded(),
            &file,
            &denied_session,
            &runtime,
            None,
            None,
        )
        .err()
        .expect("real query decode must propagate its configured context failure");
        assert!(
            error
                .to_string()
                .contains("test injected query-context failure"),
            "{error}"
        );
        assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 1);
        assert_eq!(denied_memory.snapshot().reserved_bytes, 0);
    };
    assert_query_denied(&request);
    let report = execute_vortex_local_primitive_with_policy(
        &request,
        VortexLocalPrimitiveExecutionPolicy::single_threaded(),
    )
    .unwrap();
    assert!(!report.fallback_execution_allowed);
    let (_, payload) = report
        .result_summary
        .as_deref()
        .unwrap()
        .split_once(" values=")
        .unwrap();
    let actual: serde_json::Value = serde_json::from_str(payload).unwrap();
    let mut expected = expected.into_iter().collect::<Vec<_>>();
    expected.sort_by_key(|row| std::cmp::Reverse(row.1));
    let expected = expected.into_iter().take(7).map(|((key, phrase), count)|
        serde_json::json!({"renamed_key":key, "renamed_phrase":phrase, "n":count})).collect::<Vec<_>>();
    assert_eq!(actual["values"], serde_json::json!(expected));
    assert!(
        actual["aggregate_native_numeric_accessor"]["native_decode_calls"]
            .as_u64()
            .unwrap()
            > 0
    );
    assert_eq!(
        actual["aggregate_native_numeric_accessor"]["typed_value_bytes_copied"],
        0
    );
    assert_eq!(
        actual["aggregate_accessor_materialization_status"],
        "native_numeric_array_decode_with_typed_accessors_and_optional_other_accessors"
    );
    assert_eq!(
        actual["aggregate_materialized_accessor_columns"],
        "renamed_key"
    );
    assert_eq!(
        actual["aggregate_accessor_layout_correlation_columns"],
        "renamed_key"
    );
    assert_ne!(
        actual["aggregate_accessor_layout_correlation_status"],
        "not_required_no_materialized_accessors"
    );
    let scalar = VortexQueryPrimitiveRequest::simple_aggregate(
        uri,
        VortexSimpleAggregateRequest::new(vec![VortexSimpleAggregateMeasure::new(
            "count_distinct",
            Some(ColumnRef::new("renamed_key").unwrap()),
            "unique_keys".into(),
        )]),
    );
    assert_query_denied(&scalar);
    let report = execute_vortex_local_primitive_with_policy(
        &scalar,
        VortexLocalPrimitiveExecutionPolicy::single_threaded(),
    )
    .unwrap();
    let (_, payload) = report
        .result_summary
        .as_deref()
        .unwrap()
        .split_once(" values=")
        .unwrap();
    let actual: serde_json::Value = serde_json::from_str(payload).unwrap();
    assert_eq!(actual["values"]["unique_keys"], 4);
    assert_eq!(
        actual["aggregate_materialized_accessor_columns"],
        "renamed_key"
    );
    assert_eq!(
        actual["aggregate_accessor_layout_correlation_columns"],
        "renamed_key"
    );
    assert!(
        actual["aggregate_native_numeric_accessor"]["native_decode_calls"]
            .as_u64()
            .unwrap()
            > 0
    );
}
