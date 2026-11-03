//! Producer nullability is proved by typed semantics, never sampled values.

use super::*;

#[cfg(all(feature = "vortex-write", feature = "vortex-local-primitives", unix))]
mod persisted;

fn append(source: ArrayRef, nullable: bool) -> Result<RecordBatch> {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "EventTime",
        source.data_type().clone(),
        nullable,
    )]));
    let batch = RecordBatch::try_new(Arc::clone(&schema), vec![source]).unwrap();
    let specs = embedded_derived_column_specs(&schema, EmbeddedDerivedColumnMode::SourceNativeOnly);
    assert_eq!(specs.len(), 2);
    let output = Arc::new(embedded_derived_column_schema(&schema, &specs));
    append_embedded_derived_columns_to_batch(&batch, &output, &specs)
}

fn assert_minutes(batch: &RecordBatch, nullable: bool, expected: &[Option<u8>]) {
    assert_eq!(batch.schema().field(1).is_nullable(), nullable);
    assert_eq!(batch.schema().field(2).is_nullable(), nullable);
    let minutes = batch
        .column(1)
        .as_any()
        .downcast_ref::<UInt8Array>()
        .unwrap();
    let buckets = batch
        .column(2)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(minutes.iter().collect::<Vec<_>>(), expected);
    assert_eq!(
        buckets.iter().collect::<Vec<_>>(),
        expected
            .iter()
            .map(|v| v.map(|v| i64::from(v) * 60))
            .collect::<Vec<_>>()
    );
}

#[test]
fn typed_minute_helpers_preserve_required_schema_across_integer_and_timestamp_units() {
    let sources: Vec<ArrayRef> = vec![
        Arc::new(Int8Array::from(vec![0, 60, 120])),
        Arc::new(Int16Array::from(vec![0, 60, 120])),
        Arc::new(Int32Array::from(vec![0, 60, 120])),
        Arc::new(Int64Array::from(vec![0, 60, 120])),
        Arc::new(UInt8Array::from(vec![0, 60, 120])),
        Arc::new(UInt16Array::from(vec![0, 60, 120])),
        Arc::new(UInt32Array::from(vec![0, 60, 120])),
        Arc::new(UInt64Array::from(vec![0, 60, 120])),
        Arc::new(TimestampSecondArray::from(vec![0, 60, 120])),
        Arc::new(TimestampMillisecondArray::from(vec![0, 60_000, 120_000])),
        Arc::new(
            TimestampMicrosecondArray::from(vec![0, 60_000_000, 120_000_000]).with_timezone("UTC"),
        ),
        Arc::new(TimestampNanosecondArray::from(vec![
            0,
            60_000_000_000,
            120_000_000_000,
        ])),
    ];
    for source in sources {
        for nullable in [false, true] {
            let batch = append(Arc::clone(&source), nullable).unwrap();
            assert_minutes(&batch, nullable, &[Some(0), Some(1), Some(2)]);
            assert_minutes(
                &append(source.slice(0, 0), nullable).unwrap(),
                nullable,
                &[],
            );
        }
    }
}

#[test]
fn nullable_minute_helpers_preserve_nulls_and_signed_bucket_errors() {
    let source: ArrayRef = Arc::new(Int64Array::from(vec![Some(0), None, Some(120)]));
    assert_minutes(
        &append(source, true).unwrap(),
        true,
        &[Some(0), None, Some(2)],
    );
    let source: ArrayRef = Arc::new(Int64Array::from(vec![None, None]));
    assert_minutes(&append(source, true).unwrap(), true, &[None, None]);
    let sources: Vec<ArrayRef> = vec![
        Arc::new(Int64Array::from(vec![i64::MIN])),
        Arc::new(TimestampSecondArray::from(vec![i64::MIN])),
        Arc::new(UInt64Array::from(vec![u64::MAX])),
    ];
    for source in sources {
        assert!(
            append(Arc::clone(&source), false)
                .unwrap_err()
                .to_string()
                .contains("exceeded int64 range")
        );
        assert!(
            date_trunc_minute_array_value(&source, 0)
                .unwrap_err()
                .to_string()
                .contains("exceeded int64 range")
        );
    }
    let source: ArrayRef = Arc::new(Int64Array::from(vec![-61, -1, 0]));
    let batch = append(source, false).unwrap();
    let minutes = batch
        .column(1)
        .as_any()
        .downcast_ref::<UInt8Array>()
        .unwrap();
    let buckets = batch
        .column(2)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(minutes.values().as_ref(), &[58, 59, 0]);
    assert_eq!(buckets.values().as_ref(), &[-120, -60, 0]);
}

#[test]
fn dictionary_and_text_helper_schema_does_not_promote_row_code_nullability() {
    for (name, dtype, count) in [
        ("EventTime", DataType::Utf8, 1),
        (
            "URL",
            DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
            2,
        ),
    ] {
        let schema = Schema::new(vec![Field::new(name, dtype, false)]);
        let specs = embedded_derived_column_specs(&schema, EmbeddedDerivedColumnMode::FullAdapter);
        assert_eq!(specs.len(), count);
        let output = embedded_derived_column_schema(&schema, &specs);
        assert!(
            output
                .fields()
                .iter()
                .skip(1)
                .all(|field| field.is_nullable())
        );
    }
    // The producer does not admit minute extraction from dictionary values.
    // Non-null row codes cannot create that missing transform contract.
    let schema = Schema::new(vec![Field::new(
        "EventTime",
        DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Int64)),
        false,
    )]);
    let specs = embedded_derived_column_specs(&schema, EmbeddedDerivedColumnMode::FullAdapter);
    assert_eq!(specs.len(), 0);
}
