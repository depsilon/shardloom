// SPDX-License-Identifier: Apache-2.0
//! Regression coverage for Arrow IPC file and stream variants.

use super::*;
use arrow_array::builder::LargeListBuilder;
use arrow_ipc::{
    CompressionType,
    writer::{DictionaryHandling, IpcWriteOptions},
};
use std::path::PathBuf;

struct IpcFixture(PathBuf);

impl IpcFixture {
    fn new() -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let path = std::env::temp_dir().join(format!(
            "shardloom-ipc-variants-{}-{}.arrow",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        Self(path)
    }
}

impl Drop for IpcFixture {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.0);
    }
}

fn ipc_options(compression: Option<CompressionType>) -> IpcWriteOptions {
    IpcWriteOptions::default()
        .try_with_compression(compression)
        .unwrap()
}

fn write_file(
    path: &Path,
    schema: &Schema,
    batches: &[RecordBatch],
    compression: Option<CompressionType>,
) {
    let mut writer = arrow_ipc::writer::FileWriter::try_new_with_options(
        File::create(path).unwrap(),
        schema,
        ipc_options(compression),
    )
    .unwrap();
    for batch in batches {
        writer.write(batch).unwrap();
    }
    writer.finish().unwrap();
}

fn write_stream(
    path: &Path,
    schema: &Schema,
    batches: &[RecordBatch],
    compression: Option<CompressionType>,
) {
    let mut writer = arrow_ipc::writer::StreamWriter::try_new_with_options(
        File::create(path).unwrap(),
        schema,
        ipc_options(compression),
    )
    .unwrap();
    for batch in batches {
        writer.write(batch).unwrap();
    }
    writer.finish().unwrap();
}

fn assert_batches(actual: &[RecordBatch], expected: &[RecordBatch]) {
    assert_eq!(actual.len(), expected.len());
    for (actual, expected) in actual.iter().zip(expected) {
        assert_eq!(actual.schema().as_ref(), expected.schema().as_ref());
        assert_eq!(actual.num_rows(), expected.num_rows());
        for (actual, expected) in actual.columns().iter().zip(expected.columns()) {
            assert_eq!(actual.to_data(), expected.to_data());
        }
    }
}

#[test]
fn ipc_file_and_stream_read_uncompressed_lz4_and_zstd() {
    let metadata = std::collections::HashMap::from([("owner".to_string(), "ipc-test".to_string())]);
    let schema = Schema::new_with_metadata(
        vec![
            Field::new("id", DataType::Int64, true).with_metadata(metadata.clone()),
            Field::new("label", DataType::Utf8, true),
        ],
        metadata.clone(),
    );
    let first = RecordBatch::try_new(
        Arc::new(schema.clone()),
        vec![
            Arc::new(Int64Array::from(vec![Some(7), None, Some(-3)])),
            Arc::new(StringArray::from(vec![Some("λ"), Some(""), None])),
        ],
    )
    .unwrap();
    let empty = RecordBatch::new_empty(Arc::new(schema.clone()));
    let second = RecordBatch::try_new(
        Arc::new(schema.clone()),
        vec![
            Arc::new(Int64Array::from(vec![Some(11)])),
            Arc::new(StringArray::from(vec![Some("tail")])),
        ],
    )
    .unwrap();
    let expected = [first, empty, second];

    for (stream, compression) in [
        None,
        Some(CompressionType::LZ4_FRAME),
        Some(CompressionType::ZSTD),
    ]
    .into_iter()
    .flat_map(|compression| [(false, compression), (true, compression)])
    {
        let fixture = IpcFixture::new();
        if stream {
            write_stream(&fixture.0, &schema, &expected, compression);
            let mut source = stream_flat_arrow_ipc_columnar_source(&fixture.0, usize::MAX).unwrap();
            assert_eq!(source.record_batch_count_hint, None);
            assert_eq!(source.source_stream_unit_count_hint, None);
            let batches = source
                .reader
                .by_ref()
                .map(|batch| batch.unwrap())
                .collect::<Vec<_>>();
            assert_batches(&batches, &expected);
        } else {
            write_file(&fixture.0, &schema, &expected, compression);
            let source = read_flat_arrow_ipc_columnar_source(&fixture.0, usize::MAX).unwrap();
            assert_eq!(source.batches.len(), expected.len());
            assert_batches(&source.batches, &expected);
            let streamed = stream_flat_arrow_ipc_columnar_source(&fixture.0, usize::MAX).unwrap();
            assert_eq!(streamed.record_batch_count_hint, Some(expected.len()));
            assert_eq!(streamed.source_stream_unit_count_hint, Some(expected.len()));
        }
    }
}

#[test]
fn ipc_stream_schema_only_and_truncated_frame_behavior() {
    let schema = Schema::new(vec![Field::new("id", DataType::Int32, true)]);
    let fixture = IpcFixture::new();
    write_stream(&fixture.0, &schema, &[], None);
    let mut source = stream_flat_arrow_ipc_columnar_source(&fixture.0, usize::MAX).unwrap();
    assert_eq!(source.reader.schema().as_ref(), &schema);
    assert_eq!(source.reader.by_ref().count(), 0);

    let mut bytes = std::fs::read(&fixture.0).unwrap();
    // Remove the writer's optional EOS marker and start an incomplete message frame.
    bytes.truncate(bytes.len().saturating_sub(8));
    bytes.extend_from_slice(&[0xff, 0xff, 0xff, 0xff, 0x20, 0x00, 0x00, 0x00, 0x01, 0x02]);
    std::fs::write(&fixture.0, bytes).unwrap();
    let mut source = stream_flat_arrow_ipc_columnar_source(&fixture.0, usize::MAX).unwrap();
    assert!(source.reader.next().is_some_and(|batch| batch.is_err()));
}

#[test]
fn ipc_columnar_projection_preserves_requested_array_and_full_schema_metadata() {
    let metadata =
        std::collections::HashMap::from([("field-note".to_string(), "kept".to_string())]);
    let schema = Schema::new_with_metadata(
        vec![
            Field::new("id", DataType::Int32, true).with_metadata(metadata.clone()),
            Field::new("label", DataType::Utf8, true).with_metadata(metadata.clone()),
        ],
        metadata.clone(),
    );
    let batch = RecordBatch::try_new(
        Arc::new(schema.clone()),
        vec![
            Arc::new(Int32Array::from(vec![Some(2), None, Some(5)])),
            Arc::new(StringArray::from(vec![Some("a"), None, Some("b")])),
        ],
    )
    .unwrap();
    let fixture = IpcFixture::new();
    write_stream(&fixture.0, &schema, std::slice::from_ref(&batch), None);

    let projected = read_flat_arrow_ipc_columnar_source_with_projection(
        &fixture.0,
        usize::MAX,
        &["id".to_string()],
    )
    .unwrap();
    assert_eq!(projected.header, ["id", "label"]);
    assert_eq!(projected.materialized_columns, ["id"]);
    assert_eq!(projected.batches[0].schema().field(0).name(), "id");
    assert_eq!(projected.batches[0].schema().field(0).metadata(), &metadata);
    assert_eq!(
        projected.batches[0].column(0).to_data(),
        batch.column(0).to_data()
    );
    assert_eq!(projected.batches[0].schema().metadata(), &metadata);
    assert_eq!(schema.field(1).metadata(), &metadata);
}

#[test]
fn ipc_stream_preserves_decimal_nested_values_and_dictionary_replacement() {
    let schema = Schema::new(vec![
        Field::new("amount", DataType::Decimal128(10, 2), true),
        Field::new(
            "items",
            DataType::List(Arc::new(Field::new("item", DataType::Int32, true))),
            true,
        ),
        Field::new(
            "detail",
            DataType::Struct(Fields::from(vec![Field::new(
                "ok",
                DataType::Boolean,
                true,
            )])),
            true,
        ),
        Field::new(
            "label",
            DataType::Dictionary(Box::new(DataType::Int8), Box::new(DataType::Utf8)),
            true,
        ),
    ]);
    let make_batch = |amounts: Vec<Option<i128>>, labels: [&str; 2]| {
        let amounts = Decimal128Array::from(amounts)
            .with_precision_and_scale(10, 2)
            .unwrap();
        let mut items = ListBuilder::new(Int32Builder::new());
        items.values().append_value(1);
        items.values().append_value(2);
        items.append(true);
        items.append(false);
        let items = items.finish();
        let detail = StructArray::new(
            Fields::from(vec![Field::new("ok", DataType::Boolean, true)]),
            vec![Arc::new(BooleanArray::from(vec![Some(true), None]))],
            None,
        );
        let keys = Int8Array::from(vec![Some(0), Some(1)]);
        let dictionary = DictionaryArray::<Int8Type>::try_new(
            keys,
            Arc::new(StringArray::from(vec![labels[0], labels[1]])),
        )
        .unwrap();
        RecordBatch::try_new(
            Arc::new(schema.clone()),
            vec![
                Arc::new(amounts),
                Arc::new(items),
                Arc::new(detail),
                Arc::new(dictionary),
            ],
        )
        .unwrap()
    };
    let expected = [
        make_batch(vec![Some(1234), None], ["red", "blue"]),
        make_batch(vec![Some(-5), Some(0)], ["green", "gold"]),
    ];
    let fixture = IpcFixture::new();
    write_stream(&fixture.0, &schema, &expected, None);
    let mut source = stream_flat_arrow_ipc_columnar_source(&fixture.0, usize::MAX).unwrap();
    let actual = source
        .reader
        .by_ref()
        .map(|batch| batch.unwrap())
        .collect::<Vec<_>>();
    assert_batches(&actual, &expected);
}

#[test]
fn ipc_stream_dictionary_delta_extends_domain_and_preserves_decoded_values() {
    let schema = Schema::new(vec![Field::new(
        "label",
        DataType::Dictionary(Box::new(DataType::Int8), Box::new(DataType::Utf8)),
        true,
    )]);
    let make_dictionary = |keys: Vec<Option<i8>>, values: Vec<&str>| {
        let array = DictionaryArray::<Int8Type>::try_new(
            Int8Array::from(keys),
            Arc::new(StringArray::from(values)),
        )
        .unwrap();
        RecordBatch::try_new(Arc::new(schema.clone()), vec![Arc::new(array)]).unwrap()
    };
    let expected = [
        make_dictionary(vec![Some(0), Some(1), None], vec!["red", "blue"]),
        make_dictionary(vec![Some(0), Some(2), Some(1)], vec!["red", "blue", "gold"]),
    ];
    let fixture = IpcFixture::new();
    let options = ipc_options(None).with_dictionary_handling(DictionaryHandling::Delta);
    let mut writer = arrow_ipc::writer::StreamWriter::try_new_with_options(
        File::create(&fixture.0).unwrap(),
        &schema,
        options,
    )
    .unwrap();
    for batch in &expected {
        writer.write(batch).unwrap();
    }
    writer.finish().unwrap();

    let mut source = stream_flat_arrow_ipc_columnar_source(&fixture.0, usize::MAX).unwrap();
    let actual = source
        .reader
        .by_ref()
        .map(|batch| batch.unwrap())
        .collect::<Vec<_>>();
    assert_batches(&actual, &expected);
}

#[test]
fn ipc_file_and_stream_preserve_large_offset_array_variants() {
    let schema = Schema::new(vec![
        Field::new("text", DataType::LargeUtf8, true),
        Field::new(
            "items",
            DataType::LargeList(Arc::new(Field::new("item", DataType::Int64, true))),
            true,
        ),
    ]);
    let mut items = LargeListBuilder::new(Int64Builder::new());
    items.values().append_value(9);
    items.values().append_value(-2);
    items.append(true);
    items.append(false);
    let batch = RecordBatch::try_new(
        Arc::new(schema.clone()),
        vec![
            Arc::new(LargeStringArray::from(vec![Some("small λ"), None])),
            Arc::new(items.finish()),
        ],
    )
    .unwrap();

    for is_stream in [false, true] {
        let fixture = IpcFixture::new();
        if is_stream {
            write_stream(&fixture.0, &schema, std::slice::from_ref(&batch), None);
            let mut source = stream_flat_arrow_ipc_columnar_source(&fixture.0, usize::MAX).unwrap();
            let actual = source
                .reader
                .by_ref()
                .map(|batch| batch.unwrap())
                .collect::<Vec<_>>();
            assert_batches(&actual, std::slice::from_ref(&batch));
        } else {
            write_file(&fixture.0, &schema, std::slice::from_ref(&batch), None);
            let source = read_flat_arrow_ipc_columnar_source(&fixture.0, usize::MAX).unwrap();
            assert_batches(&source.batches, std::slice::from_ref(&batch));
        }
    }
}
