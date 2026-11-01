// SPDX-License-Identifier: Apache-2.0

use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, Date32Array, Decimal128Array, Int32Array, ListArray, RecordBatch, StringArray,
    StructArray, TimestampMicrosecondArray,
};
use arrow::buffer::{NullBuffer, OffsetBuffer};
use arrow::compute::concat_batches;
use arrow::datatypes::{DataType, Field, Schema};
use bytes::Bytes;

use crate::{ArrowReaderBuilder, ArrowWriterBuilder};

fn roundtrip_stripes(batch: &RecordBatch) -> RecordBatch {
    let mut bytes = Vec::new();
    let mut writer = ArrowWriterBuilder::new(&mut bytes, batch.schema())
        .with_batch_size(1)
        .with_compression(crate::compression::CompressionType::Snappy)
        .try_build()
        .unwrap();
    writer.flush_stripe().unwrap();
    assert_eq!(writer.flushed_stripes(), 0);
    let middle = batch.num_rows() / 2;
    writer.write(&batch.slice(0, middle)).unwrap();
    assert_eq!(writer.in_progress_rows(), middle);
    // Small integer/presence streams can still reside in fixed pending state;
    // the observation counts encoded buffers, not those pending literals.
    assert_eq!(writer.flushed_stripes(), 0);
    writer.flush_stripe().unwrap();
    assert_eq!(writer.in_progress_rows(), 0);
    assert_eq!(writer.flushed_stripes(), 1);
    writer
        .write(&batch.slice(middle, batch.num_rows() - middle))
        .unwrap();
    writer.close().unwrap();
    let builder = ArrowReaderBuilder::try_new(Bytes::from(bytes)).unwrap();
    assert_eq!(builder.file_metadata().stripe_metadatas().len(), 2);
    let decoded = builder
        .with_schema(batch.schema())
        .with_batch_size(1)
        .build()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    concat_batches(&batch.schema(), decoded.iter()).unwrap()
}

#[test]
fn rich_decimal_38_digits_nulls_and_multiple_stripes() {
    let bound = 10_i128.pow(38) - 1;
    let amount: ArrayRef = Arc::new(
        Decimal128Array::from(vec![Some(bound), None, Some(-bound), Some(0)])
            .with_precision_and_scale(38, 6)
            .unwrap(),
    );
    let batch = RecordBatch::try_from_iter_with_nullable([("amount", amount, true)]).unwrap();
    assert_eq!(roundtrip_stripes(&batch), batch);
}

#[test]
fn rich_struct_list_decimal_slices_null_parents_and_column_ids() {
    let decimal: ArrayRef = Arc::new(
        Decimal128Array::from(vec![
            Some(91),
            Some(11),
            Some(-12),
            Some(99),
            Some(13),
            None,
            Some(14),
            Some(92),
        ])
        .with_precision_and_scale(2, 0)
        .unwrap(),
    );
    let elements = Arc::new(Field::new("entry", decimal.data_type().clone(), true));
    let lists: ArrayRef = Arc::new(ListArray::new(
        elements,
        OffsetBuffer::new(vec![1_i32, 3, 4, 4, 7].into()),
        decimal,
        Some(NullBuffer::from(vec![true, false, true, true])),
    ));
    let dates: ArrayRef = Arc::new(Date32Array::from(vec![Some(-1), None, Some(999), Some(1)]));
    let timestamps: ArrayRef = Arc::new(TimestampMicrosecondArray::from(vec![
        Some(-1_001_001),
        None,
        Some(999),
        Some(1_234_567),
    ]));
    let fields = vec![
        Arc::new(Field::new("values", lists.data_type().clone(), true)),
        Arc::new(Field::new("date", dates.data_type().clone(), true)),
        Arc::new(Field::new("time", timestamps.data_type().clone(), true)),
    ]
    .into();
    let structure: ArrayRef = Arc::new(StructArray::new(
        fields,
        vec![lists, dates, timestamps],
        Some(NullBuffer::from(vec![true, true, false, true])),
    ));
    let trailing: ArrayRef = Arc::new(StringArray::from(vec![
        Some("α"),
        None,
        Some("tail"),
        Some("終"),
    ]));
    let batch = RecordBatch::try_from_iter_with_nullable([
        ("nested", structure, true),
        ("trailing", trailing, true),
    ])
    .unwrap();
    assert_eq!(roundtrip_stripes(&batch), batch);
    // Nonzero outer/list offsets must not encode the discarded prefix/suffix.
    let slice = batch.slice(1, 2);
    assert_eq!(roundtrip_stripes(&slice), slice);
}

#[test]
fn rich_list_of_structs_and_empty_lists() {
    let values: ArrayRef = Arc::new(Int32Array::from(vec![
        Some(90),
        Some(1),
        None,
        Some(2),
        Some(91),
    ]));
    let fields = vec![Arc::new(Field::new("id", DataType::Int32, true))].into();
    let structs: ArrayRef = Arc::new(StructArray::new(
        fields,
        vec![values],
        Some(NullBuffer::from(vec![true, true, false, true, true])),
    ));
    let list: ArrayRef = Arc::new(ListArray::new(
        Arc::new(Field::new("item", structs.data_type().clone(), true)),
        OffsetBuffer::new(vec![1_i32, 1, 3, 4, 4].into()),
        structs,
        Some(NullBuffer::from(vec![true, true, true, false])),
    ));
    let batch = RecordBatch::try_from_iter_with_nullable([("items", list, true)]).unwrap();
    assert_eq!(roundtrip_stripes(&batch), batch);
}

#[test]
fn rich_null_parent_does_not_encode_hidden_invalid_decimal() {
    let amount: ArrayRef = Arc::new(
        Decimal128Array::from(vec![Some(999), Some(3), None, Some(-4)])
            .with_precision_and_scale(1, 0)
            .unwrap(),
    );
    let fields = vec![Arc::new(Field::new(
        "amount",
        amount.data_type().clone(),
        true,
    ))]
    .into();
    let record: ArrayRef = Arc::new(StructArray::new(
        fields,
        vec![amount],
        Some(NullBuffer::from(vec![false, true, true, true])),
    ));
    let batch = RecordBatch::try_from_iter_with_nullable([("record", record, true)]).unwrap();
    assert_eq!(roundtrip_stripes(&batch), batch);
}

#[test]
fn rich_invalid_schema_rejected_before_header_and_decimal_value_rejected() {
    for dtype in [
        DataType::UInt64,
        DataType::Decimal128(3, -1),
        DataType::Duration(arrow::datatypes::TimeUnit::Second),
    ] {
        let schema = Arc::new(Schema::new(vec![Field::new("value", dtype, true)]));
        let mut bytes = Vec::new();
        assert!(ArrowWriterBuilder::new(&mut bytes, schema)
            .try_build()
            .is_err());
        assert!(bytes.is_empty());
    }
    let amount: ArrayRef = Arc::new(
        Decimal128Array::from(vec![Some(123)])
            .with_precision_and_scale(2, 0)
            .unwrap(),
    );
    let batch = RecordBatch::try_from_iter_with_nullable([("amount", amount, true)]).unwrap();
    let mut bytes = Vec::new();
    let mut writer = ArrowWriterBuilder::new(&mut bytes, batch.schema())
        .try_build()
        .unwrap();
    assert!(writer
        .write(&batch)
        .unwrap_err()
        .to_string()
        .contains("precision"));
}

#[test]
fn rich_empty_file_preserves_recursive_schema() {
    let child = Arc::new(Field::new("item", DataType::Decimal128(20, 3), true));
    let schema = Arc::new(Schema::new(vec![Field::new(
        "values",
        DataType::List(child),
        true,
    )]));
    let mut bytes = Vec::new();
    let mut writer = ArrowWriterBuilder::new(&mut bytes, schema.clone())
        .try_build()
        .unwrap();
    writer
        .write(&RecordBatch::new_empty(schema.clone()))
        .unwrap();
    writer.close().unwrap();
    let builder = ArrowReaderBuilder::try_new(Bytes::from(bytes)).unwrap();
    assert_eq!(builder.schema(), schema);
    assert_eq!(builder.build().count(), 0);
}

#[test]
fn decoded_batches_preserve_the_declared_schema_and_metadata() {
    let values: ArrayRef = Arc::new(Int32Array::from(vec![1, 2]));
    let field = Field::new("value", DataType::Int32, false)
        .with_metadata([("meaning".to_owned(), "identifier".to_owned())].into());
    let schema = Arc::new(Schema::new_with_metadata(
        vec![field],
        [("source".to_owned(), "declared".to_owned())].into(),
    ));
    let batch = RecordBatch::try_new(schema.clone(), vec![values]).unwrap();
    let mut bytes = Vec::new();
    let mut writer = ArrowWriterBuilder::new(&mut bytes, schema.clone())
        .try_build()
        .unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
    let decoded = ArrowReaderBuilder::try_new(Bytes::from(bytes))
        .unwrap()
        .with_schema(schema.clone())
        .with_batch_size(1)
        .build()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(decoded.len(), 2);
    for (index, actual) in decoded.iter().enumerate() {
        assert_eq!(actual.schema(), schema);
        assert_eq!(actual, &batch.slice(index, 1));
    }
}

#[test]
fn rich_timestamp_units_survive_default_reading_and_explicit_precision() {
    use arrow::array::{TimestampNanosecondArray, TimestampSecondArray};
    let seconds: ArrayRef = Arc::new(TimestampSecondArray::from(vec![Some(i64::MAX), None]));
    let nanos: ArrayRef = Arc::new(TimestampNanosecondArray::from(vec![Some(1), None]));
    let micros: ArrayRef = Arc::new(TimestampMicrosecondArray::from(vec![Some(i64::MIN), None]));
    let record: ArrayRef = Arc::new(StructArray::new(
        vec![Arc::new(Field::new(
            "microseconds",
            micros.data_type().clone(),
            true,
        ))]
        .into(),
        vec![micros],
        None,
    ));
    let batch = RecordBatch::try_from_iter_with_nullable([
        ("seconds", seconds, true),
        ("nanoseconds", nanos, true),
        ("nested", record, true),
    ])
    .unwrap();
    let mut bytes = Vec::new();
    let mut writer = ArrowWriterBuilder::new(&mut bytes, batch.schema())
        .try_build()
        .unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
    let bytes = Bytes::from(bytes);
    let builder = ArrowReaderBuilder::try_new(bytes.clone()).unwrap();
    assert_eq!(builder.schema(), batch.schema());
    let decoded = builder.build().collect::<Result<Vec<_>, _>>().unwrap();
    assert_eq!(decoded, [batch]);
    // A caller-specified precision still overrides the optional annotation.
    let builder = ArrowReaderBuilder::try_new(bytes)
        .unwrap()
        .with_timestamp_precision(crate::schema::TimestampPrecision::Nanosecond);
    assert_eq!(
        builder.schema().field(0).data_type(),
        &DataType::Timestamp(arrow::datatypes::TimeUnit::Nanosecond, None)
    );
    assert!(builder.build().next().unwrap().is_err());
}

#[test]
fn original_width_timestamp_endpoints_preserve_validity_through_utc_writer_timezone() {
    use arrow::array::{TimestampMillisecondArray, TimestampNanosecondArray, TimestampSecondArray};
    let arrays: Vec<(&str, ArrayRef)> = vec![
        (
            "seconds",
            Arc::new(TimestampSecondArray::from(vec![
                Some(i64::MIN + 1_420_070_400),
                None,
                Some(i64::MAX),
            ])),
        ),
        (
            "milliseconds",
            Arc::new(TimestampMillisecondArray::from(vec![
                Some(i64::MIN),
                None,
                Some(i64::MAX),
            ])),
        ),
        (
            "microseconds",
            Arc::new(TimestampMicrosecondArray::from(vec![
                Some(i64::MIN),
                None,
                Some(i64::MAX),
            ])),
        ),
        (
            "nanoseconds",
            Arc::new(TimestampNanosecondArray::from(vec![
                Some(i64::MIN),
                None,
                Some(i64::MAX),
            ])),
        ),
    ];
    let batch = RecordBatch::try_from_iter(arrays).unwrap();
    let mut bytes = Vec::new();
    let mut writer = ArrowWriterBuilder::new(&mut bytes, batch.schema())
        .try_build()
        .unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
    let decoded = ArrowReaderBuilder::try_new(Bytes::from(bytes))
        .unwrap()
        .build()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(decoded, [batch]);
}
