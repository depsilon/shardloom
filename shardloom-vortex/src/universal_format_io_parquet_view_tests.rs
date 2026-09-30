//! The large-source schema hint changes string representation while retaining
//! logical values, metadata, ordered reads and the existing dictionary policy.

use super::*;
use parquet::arrow::arrow_reader::{ArrowReaderMetadata, ParquetRecordBatchReaderBuilder};

#[test]
fn parquet_view_hint_preserves_metadata_and_non_string_fields() {
    let metadata = std::collections::HashMap::from([("owner".to_string(), "fixture".to_string())]);
    let schema = Arc::new(
        Schema::new(vec![
            Field::new("note", DataType::Utf8, true).with_metadata(metadata.clone()),
            Field::new("large_note", DataType::LargeUtf8, false),
            Field::new("view", DataType::Utf8View, true),
            Field::new(
                "dict",
                DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
                true,
            ),
            Field::new("id", DataType::Int64, false),
            Field::new("binary", DataType::Binary, true),
        ])
        .with_metadata(metadata),
    );
    let plan =
        parquet_dictionary_schema_plan(&schema, Some(PRODUCT_COLUMNAR_LARGE_STREAM_ROW_THRESHOLD));
    let hinted = plan.schema_hint.unwrap();
    assert_eq!(hinted.as_ref(), plan.stream_schema.as_ref());
    assert_eq!(hinted.metadata(), schema.metadata());
    for (index, field) in hinted.fields().iter().enumerate() {
        let original = schema.field(index);
        assert_eq!(field.name(), original.name());
        assert_eq!(field.is_nullable(), original.is_nullable());
        assert_eq!(field.metadata(), original.metadata());
        if index < 2 {
            assert_eq!(field.data_type(), &DataType::Utf8View);
        } else {
            assert_eq!(field.as_ref(), original);
        }
    }
    let no_offset_strings = Arc::new(Schema::new(hinted.fields().clone()));
    assert!(parquet_string_view_schema_hint(&no_offset_strings).is_none());
    for rows in [None, Some(PRODUCT_COLUMNAR_LARGE_STREAM_ROW_THRESHOLD - 1)] {
        let smaller = parquet_dictionary_schema_plan(&schema, rows);
        assert_eq!(smaller.stream_schema.as_ref(), schema.as_ref());
        assert!(smaller.schema_hint.is_none());
    }
}

fn assert_view_rows(
    reader: Box<dyn RecordBatchReader + Send>,
    expected: &[Option<&str>],
    expected_schema: &SchemaRef,
) {
    assert_eq!(reader.schema().as_ref(), expected_schema.as_ref());
    let mut seen = 0;
    for result in reader {
        let batch = result.unwrap();
        assert_eq!(batch.schema().as_ref(), expected_schema.as_ref());
        for row in 0..batch.num_rows() {
            for column in [0, 1] {
                assert_eq!(batch.column(column).data_type(), &DataType::Utf8View);
                assert_eq!(
                    utf8_array_value(batch.column(column), row).unwrap(),
                    expected[seen]
                );
            }
            assert_eq!(
                batch
                    .column(2)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap()
                    .value(row),
                i64::try_from(seen).unwrap(),
            );
            seen += 1;
        }
    }
    assert_eq!(seen, expected.len());
}

#[test]
fn parquet_view_readers_preserve_all_values_across_pages_and_ordered_tasks() {
    let values = [
        Some("discarded prefix"),
        Some(""),
        None,
        Some("λ東京"),
        Some("a longer outlined string crossing the inline threshold"),
        Some("a longer outlined string crossing the inline threshold"),
        Some("last distinct outlined value"),
        Some("z"),
    ];
    for dictionary in [false, true] {
        let path = std::env::temp_dir().join(format!(
            "shardloom-parquet-views-{}-{dictionary}-{}.parquet",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos(),
        ));
        let schema = Arc::new(Schema::new(vec![
            Field::new("note", DataType::Utf8, true),
            Field::new("large_note", DataType::LargeUtf8, true),
            Field::new("id", DataType::Int64, false),
        ]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(StringArray::from_iter(values)),
                Arc::new(LargeStringArray::from_iter(values)),
                Arc::new(Int64Array::from_iter_values(0..8)),
            ],
        )
        .unwrap();
        let properties = parquet::file::properties::WriterProperties::builder()
            .set_dictionary_enabled(dictionary)
            .set_max_row_group_row_count(Some(2))
            .set_data_page_row_count_limit(1)
            .set_write_batch_size(1)
            .build();
        let mut writer = parquet::arrow::ArrowWriter::try_new(
            File::create(&path).unwrap(),
            schema,
            Some(properties),
        )
        .unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
        #[cfg(unix)]
        let identity = Some(Arc::new(SourceIdentity::capture(&path).unwrap()));
        #[cfg(not(unix))]
        let identity: Option<Arc<SourceIdentity>> = None;
        let builder = ParquetRecordBatchReaderBuilder::try_new_with_options(
            open_parquet_generation_file(&path, identity.as_deref()).unwrap(),
            parquet_metadata_first_reader_options(),
        )
        .unwrap();
        // Exercise the production large-source plan on a bounded real fixture.
        let plan = parquet_dictionary_schema_plan(
            builder.schema(),
            Some(PRODUCT_COLUMNAR_LARGE_STREAM_ROW_THRESHOLD),
        );
        let metadata = ArrowReaderMetadata::try_new(
            Arc::clone(builder.metadata()),
            parquet_reader_options_with_optional_schema_hint(plan.schema_hint.as_ref()),
        )
        .unwrap();
        assert_eq!(builder.metadata().num_row_groups(), 4);
        let serial =
            parquet_stream_record_batch_reader(&path, identity.as_deref(), &metadata, 3).unwrap();
        assert_view_rows(serial, &values, &plan.stream_schema);
        let tasks = (0..4)
            .map(|task_index| ParquetRowGroupReadTask {
                task_index,
                row_groups: vec![task_index],
            })
            .collect();
        let parallel = ParquetRowGroupParallelRecordBatchReader::new(
            &path,
            identity,
            Arc::clone(&plan.stream_schema),
            tasks,
            1,
            2,
            &metadata,
        )
        .unwrap();
        assert_view_rows(Box::new(parallel), &values, &plan.stream_schema);
        assert!(stream_flat_parquet_columnar_source_with_parallelism(&path, 7, 2).is_err());
        std::fs::remove_file(path).unwrap();
    }
}
