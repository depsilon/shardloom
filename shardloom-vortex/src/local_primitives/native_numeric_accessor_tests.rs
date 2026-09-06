use super::{AggregateDirectColumnAccessor, NativeNumericAccessorWork};
use crate::local_primitives::{aggregate_column_accessor_with_work, aggregate_direct_stat_value};
use shardloom_core::StatValue;
use vortex::{
    array::{
        ArrayRef, IntoArray as _, VortexSessionExecute as _,
        arrays::{ChunkedArray, DictArray, FilterArray, PrimitiveArray},
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
    assert_eq!(work.typed_value_bytes_copied, expected.len() as u64 * 8);
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
    let AggregateDirectColumnAccessor::NullableFloat64 { values, row_nulls } = accessor else {
        panic!("nullable f64 remains typed");
    };
    assert_eq!(values[0].to_bits(), (-0.0_f64).to_bits());
    assert_eq!(row_nulls, vec![false, true, false, false, false]);
    assert_eq!(&values[2..], &[f64::MIN, f64::MAX, 0.25]);
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
        keys.len() as u64 * 8
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
