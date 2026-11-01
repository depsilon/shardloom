use arrow_array::{
    Array, ArrayRef, Int64Array, ListArray, RecordBatch, StringArray,
    builder::{Int64Builder, ListBuilder},
};
use bytes::Bytes;
use parquet::{
    arrow::{
        ArrowWriter,
        arrow_reader::{ParquetRecordBatchReaderBuilder, RowSelection, RowSelectionPolicy},
    },
    file::properties::WriterProperties,
};
use std::sync::Arc;

fn encoded(batch: &RecordBatch, group_rows: usize) -> Bytes {
    let mut bytes = Vec::new();
    let properties = WriterProperties::builder()
        .set_max_row_group_row_count(Some(group_rows))
        .build();
    let mut writer = ArrowWriter::try_new(&mut bytes, batch.schema(), Some(properties)).unwrap();
    writer.write(batch).unwrap();
    writer.close().unwrap();
    bytes.into()
}

#[test]
fn parquet_adaptive_batch_limits_shrink_real_value_buffers_and_preserve_held_outputs() {
    let values: ArrayRef = Arc::new(Int64Array::from_iter_values(0..4096));
    let batch = RecordBatch::try_from_iter([("id", values)]).unwrap();
    let mut reader = ParquetRecordBatchReaderBuilder::try_new(encoded(&batch, 4096))
        .unwrap()
        .with_batch_size(2048)
        .build()
        .unwrap();
    assert!(reader.set_batch_size(0).is_err());
    let first = reader.next().unwrap().unwrap();
    assert_eq!(first.num_rows(), 2048);
    reader.set_batch_size(4).unwrap();
    let second = reader.next().unwrap().unwrap();
    assert_eq!(second.num_rows(), 4);
    assert!(second.get_array_memory_size() < first.get_array_memory_size() / 4);
    reader.set_batch_size(64).unwrap();
    let third = reader.next().unwrap().unwrap();
    assert_eq!(third.num_rows(), 64);
    assert!(third.get_array_memory_size() > second.get_array_memory_size());
    reader.set_batch_size(usize::MAX).unwrap();
    let mut batches = vec![first, second, third];
    for batch in reader.by_ref() {
        let batch = batch.unwrap();
        assert!(batch.num_rows() <= 2048);
        batches.push(batch);
    }
    drop(reader);
    let mut expected = 0;
    for batch in &batches {
        for value in batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .values()
        {
            assert_eq!(*value, expected);
            expected += 1;
        }
    }
    assert_eq!(expected, 4096);
}

fn rich_batch() -> RecordBatch {
    let mut lists = ListBuilder::new(Int64Builder::new());
    for row in 0..120_i64 {
        if row % 5 == 0 {
            lists.append(false);
        } else {
            lists.values().append_value(row);
            lists.values().append_null();
            lists.values().append_value(-row);
            lists.append(true);
        }
    }
    RecordBatch::try_from_iter(vec![
        (
            "id",
            Arc::new(Int64Array::from_iter_values(0..120)) as ArrayRef,
        ),
        (
            "text",
            Arc::new(StringArray::from_iter(
                (0..120).map(|row| (row % 7 != 0).then(|| format!("東京-{row}"))),
            )),
        ),
        ("items", Arc::new(lists.finish())),
    ])
    .unwrap()
}

#[test]
fn parquet_adaptive_batches_preserve_masks_selectors_nested_nulls_and_prefixes() {
    let input = rich_batch();
    let bytes = encoded(&input, 37);
    let selected = (0..120).filter(|row| row % 3 != 1).collect::<Vec<_>>();
    for policy in [RowSelectionPolicy::Selectors, RowSelectionPolicy::Mask] {
        let selection =
            RowSelection::from_consecutive_ranges(selected.iter().map(|row| *row..*row + 1), 120);
        let mut reader = ParquetRecordBatchReaderBuilder::try_new(bytes.clone())
            .unwrap()
            .with_batch_size(32)
            .with_row_selection(selection)
            .with_row_selection_policy(policy)
            .with_offset(2)
            .with_limit(53)
            .build()
            .unwrap();
        let mut expected = selected.iter().skip(2).take(53);
        let mut seen = 0;
        loop {
            let limit = [1, 13, 2, 7, 32][seen % 5];
            reader.set_batch_size(limit).unwrap();
            let Some(batch) = reader.next() else {
                break;
            };
            let batch = batch.unwrap();
            assert!(batch.num_rows() > 0 && batch.num_rows() <= limit);
            let ids = batch
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            let text = batch
                .column(1)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            let lists = batch
                .column(2)
                .as_any()
                .downcast_ref::<ListArray>()
                .unwrap();
            for row in 0..batch.num_rows() {
                let ordinal = *expected.next().expect("no extra provider row");
                assert_eq!(ids.value(row), i64::try_from(ordinal).unwrap());
                assert_eq!(text.is_null(row), ordinal % 7 == 0);
                if !text.is_null(row) {
                    assert_eq!(text.value(row), format!("東京-{ordinal}"));
                }
                assert_eq!(lists.is_null(row), ordinal % 5 == 0);
                if !lists.is_null(row) {
                    let values = lists.value(row);
                    let values = values.as_any().downcast_ref::<Int64Array>().unwrap();
                    assert_eq!(values.len(), 3);
                    assert_eq!(values.value(0), i64::try_from(ordinal).unwrap());
                    assert!(values.is_null(1));
                    assert_eq!(values.value(2), -i64::try_from(ordinal).unwrap());
                }
                seen += 1;
            }
        }
        assert_eq!(seen, 53);
        assert!(expected.next().is_none());
    }
}

#[test]
fn parquet_adaptive_empty_and_invalid_batch_limits_never_invent_rows() {
    let batch = RecordBatch::try_from_iter([(
        "id",
        Arc::new(Int64Array::from(Vec::<i64>::new())) as ArrayRef,
    )])
    .unwrap();
    let mut reader = ParquetRecordBatchReaderBuilder::try_new(encoded(&batch, 16))
        .unwrap()
        .with_batch_size(16)
        .build()
        .unwrap();
    assert!(reader.set_batch_size(0).is_err());
    reader.set_batch_size(usize::MAX).unwrap();
    assert!(reader.next().is_none());
}
