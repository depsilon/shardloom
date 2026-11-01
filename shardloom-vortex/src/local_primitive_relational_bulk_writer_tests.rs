use super::*;
use crate::local_primitives::VortexLocalPrimitiveRowExportFormat as Format;
use arrow_array::{Array as _, LargeStringArray, RecordBatch, StringArray, StringViewArray};
use std::{fs::File, io::BufReader};
use vortex::array::arrays::{StructArray, VarBinViewArray};

const ROWS: usize = 131_073;

fn binary_formats() -> [Format; 4] {
    [Format::Parquet, Format::ArrowIpc, Format::Avro, Format::Orc]
}

fn read_batches(path: &std::path::Path, format: Format) -> Vec<RecordBatch> {
    let reader: Box<dyn arrow_array::RecordBatchReader> = match format {
        Format::Parquet => Box::new(
            parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(
                File::open(path).unwrap(),
            )
            .unwrap()
            .build()
            .unwrap(),
        ),
        Format::ArrowIpc => Box::new(
            arrow_ipc::reader::FileReader::try_new(File::open(path).unwrap(), None).unwrap(),
        ),
        Format::Avro => Box::new(
            arrow_avro::reader::ReaderBuilder::new()
                .with_batch_size(8192)
                .build(BufReader::new(File::open(path).unwrap()))
                .unwrap(),
        ),
        Format::Orc => Box::new(
            orc_rust::ArrowReaderBuilder::try_new(File::open(path).unwrap())
                .unwrap()
                .with_batch_size(8192)
                .build(),
        ),
        _ => panic!("unexpected format {format:?}"),
    };
    reader.map(|batch| batch.unwrap()).collect()
}

fn assert_bulk_rows(path: &std::path::Path, format: Format) {
    let batches = read_batches(path, format);
    let mut ordinal = 0usize;
    for batch in batches {
        assert_eq!(batch.num_columns(), 2, "{format:?}");
        let ids = batch
            .column(0)
            .as_any()
            .downcast_ref::<arrow_array::Int64Array>()
            .unwrap();
        let labels = batch.column(1).as_ref();
        for row in 0..batch.num_rows() {
            assert_eq!(
                ids.value(row),
                i64::try_from(ordinal).unwrap(),
                "{format:?} row {ordinal}"
            );
            if ordinal.is_multiple_of(7) {
                assert!(labels.is_null(row), "{format:?} row {ordinal}");
            } else {
                assert_eq!(string_value(labels, row), format!("repeat-{}", ordinal % 8));
            }
            ordinal += 1;
        }
    }
    assert_eq!(ordinal, ROWS, "{format:?}");
}

fn string_value(array: &dyn arrow_array::Array, row: usize) -> &str {
    match array.data_type() {
        arrow_schema::DataType::Utf8 => array
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .value(row),
        arrow_schema::DataType::LargeUtf8 => array
            .as_any()
            .downcast_ref::<LargeStringArray>()
            .unwrap()
            .value(row),
        arrow_schema::DataType::Utf8View => array
            .as_any()
            .downcast_ref::<StringViewArray>()
            .unwrap()
            .value(row),
        other => panic!("unexpected string readback type {other:?}"),
    }
}

fn bulk_source() -> Fixture {
    let ids =
        PrimitiveArray::from_iter((0..ROWS).map(|row| i64::try_from(row).unwrap())).into_array();
    let labels = VarBinViewArray::from_iter_nullable_str(
        (0..ROWS).map(|row| (row % 7 != 0).then(|| format!("repeat-{}", row % 8))),
    )
    .into_array();
    let array = StructArray::try_new(
        FieldNames::from(["id", "label"]),
        vec![ids, labels],
        ROWS,
        Validity::NonNullable,
    )
    .unwrap()
    .into_array();
    Fixture::new(array, 2048)
}

#[test]
fn bulk_compatibility_writers_accumulate_format_units_and_reopen_complete_rows() {
    let fixture = bulk_source();
    let mut configured = policy();
    configured.resource_envelope.memory_budget_bytes = 256 << 20;
    let prepared = prepare_relational(&fixture.scan(), configured).unwrap();

    for format in [Format::Parquet, Format::Orc] {
        let path = fixture.0.join(format!("bulk.{}", format.as_str()));
        let report = prepared
            .write(&path, format, false)
            .unwrap_or_else(|error| panic!("{format:?}: {error}"));
        assert_eq!(report.output.rows_written, ROWS as u64);
        assert!(!report.output.evidence.side_effects.fallback_attempted);
        assert!(
            !report
                .output
                .evidence
                .side_effects
                .fallback_execution_allowed
        );
        let sink = report
            .output
            .evidence
            .native_array_sink
            .as_ref()
            .expect("compatibility sink evidence");
        let compatibility = sink.compatibility.as_ref().expect("compatibility counters");
        assert!(compatibility.arrow_batches > 0);
        assert_eq!(
            compatibility.output_file_units,
            if format == Format::Parquet {
                let metadata =
                    parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(
                        File::open(&path).unwrap(),
                    )
                    .unwrap()
                    .metadata()
                    .clone();
                assert!(
                    metadata.num_row_groups()
                        < usize::try_from(compatibility.arrow_batches).unwrap()
                );
                for group in metadata.row_groups() {
                    assert!(group.column(0).statistics().is_some());
                    assert_eq!(
                        group.column(0).compression(),
                        parquet::basic::Compression::SNAPPY
                    );
                    assert!(group.column(1).statistics().is_some());
                    assert_eq!(
                        group.column(1).compression(),
                        parquet::basic::Compression::SNAPPY
                    );
                    assert!(group.column(1).encodings().any(|encoding| matches!(
                        encoding,
                        parquet::basic::Encoding::PLAIN_DICTIONARY
                            | parquet::basic::Encoding::RLE_DICTIONARY
                    )));
                }
                metadata.num_row_groups() as u64
            } else {
                let builder =
                    orc_rust::ArrowReaderBuilder::try_new(File::open(&path).unwrap()).unwrap();
                let stripes = builder.file_metadata().stripe_metadatas();
                assert!(matches!(
                    builder
                        .file_metadata()
                        .compression()
                        .map(|compression| compression.compression_type()),
                    Some(orc_rust::compression::CompressionType::Snappy)
                ));
                assert!(stripes.len() < usize::try_from(compatibility.arrow_batches).unwrap());
                stripes.len() as u64
            }
        );
        assert_bulk_rows(&path, format);
    }
}

fn long_value_source() -> (Fixture, String, Vec<String>) {
    let field_name = "f".repeat(300);
    let long_70k = "a".repeat(70 * 1024);
    let long_9m = "β".repeat(9 * 1024 * 1024 / "β".len());
    let unicode = "雪\nquote\"🙂".repeat(512);
    let expected = vec![long_70k.clone(), long_9m.clone(), unicode.clone()];
    let values =
        VarBinViewArray::from_iter_str([long_70k.as_str(), long_9m.as_str(), unicode.as_str()])
            .into_array();
    let array = StructArray::try_new(
        FieldNames::from([field_name.as_str()]),
        vec![values],
        expected.len(),
        Validity::NonNullable,
    )
    .unwrap()
    .into_array();
    (Fixture::new(array, 2), field_name, expected)
}

#[test]
fn wide_names_and_large_unicode_values_survive_bounded_conversion_and_reopen() {
    let (fixture, name, expected) = long_value_source();
    let mut configured = policy();
    configured.resource_envelope.memory_budget_bytes = 512 << 20;
    let prepared = prepare_relational(&fixture.scan(), configured).unwrap();

    for format in binary_formats() {
        let path = fixture.0.join(format!("long.{}", format.as_str()));
        let report = prepared
            .write(&path, format, false)
            .unwrap_or_else(|error| panic!("{format:?}: {error}"));
        assert_eq!(report.output.rows_written, expected.len() as u64);
        assert!(!report.output.evidence.side_effects.fallback_attempted);
        assert!(
            !report
                .output
                .evidence
                .side_effects
                .fallback_execution_allowed
        );
        let sink = report.output.evidence.native_array_sink.as_ref().unwrap();
        let compatibility = sink.compatibility.as_ref().unwrap();
        assert!(compatibility.large_value_batches >= 1, "{format:?}");
        let batches = read_batches(&path, format);
        let mut actual = Vec::new();
        for batch in batches {
            assert_eq!(batch.schema().field(0).name(), &name);
            let strings = batch.column(0).as_ref();
            actual.extend((0..strings.len()).map(|row| string_value(strings, row).to_owned()));
        }
        assert_eq!(actual, expected, "{format:?}");
    }
}

#[test]
fn direct_ipc_large_contiguous_text_splits_without_retaining_other_rows() {
    use crate::universal_format_io::demanded::LocalColumnarSource;
    use arrow_schema::{DataType, Field, Schema};
    use shardloom_plan::ProjectionRequest;
    use std::sync::Arc;

    let fixture = Fixture(std::env::temp_dir().join(format!(
        "shardloom-direct-large-text-{}-{}",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    )));
    fs::create_dir(&fixture.0).unwrap();
    let input = fixture.0.join("input.arrow");
    let large = "雪,\"line\n".repeat((9 << 20) / 10 + 1);
    let expected = [None, Some(""), Some("null"), Some(large.as_str())];
    let schema = Arc::new(Schema::new(vec![Field::new("text", DataType::Utf8, true)]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(StringArray::from_iter(expected))],
    )
    .unwrap();
    let mut writer =
        arrow_ipc::writer::FileWriter::try_new(File::create(&input).unwrap(), &schema).unwrap();
    writer.write(&batch).unwrap();
    writer.finish().unwrap();
    drop((writer, batch));
    let mut configured = policy();
    configured.resource_envelope.memory_budget_bytes = 512 << 20;
    let prepared = prepare_relational_with_schema(configured, |preparation| {
        let source = LocalColumnarSource::admit(
            &input,
            "arrow-ipc",
            &preparation.shared_memory_pool(),
            &preparation.cancellation_token(),
        )?;
        let uri = DatasetUri::new(input.display().to_string())?;
        preparation.register_local_source(uri.clone(), source)?;
        Ok(VortexRelationalPlan::Scan(VortexRelationalScan {
            source_uri: uri,
            projection: ProjectionRequest::All,
            predicate: None,
        }))
    })
    .unwrap();
    let memory = prepared.session.memory().clone();
    let baseline = memory.snapshot().reserved_bytes;
    for format in binary_formats() {
        let path = fixture.0.join(format!("direct.{}", format.as_str()));
        let report = prepared
            .write(&path, format, false)
            .unwrap_or_else(|error| panic!("{format:?}: {error}"));
        let compatibility = report
            .output
            .evidence
            .native_array_sink
            .as_ref()
            .unwrap()
            .compatibility
            .as_ref()
            .unwrap();
        assert!(compatibility.conversion_splits > 0, "{format:?}");
        assert!(compatibility.large_value_batches > 0, "{format:?}");
        let mut row = 0;
        for batch in read_batches(&path, format) {
            let strings = batch.column(0).as_ref();
            for index in 0..batch.num_rows() {
                let actual = (!strings.is_null(index)).then(|| string_value(strings, index));
                assert!(actual == expected[row], "{format:?} row {row}");
                row += 1;
            }
        }
        assert_eq!(row, expected.len());
        assert!(!report.output.evidence.side_effects.fallback_attempted);
        drop(report);
        assert_eq!(memory.snapshot().reserved_bytes, baseline, "{format:?}");
    }
}

#[test]
fn large_text_writers_preserve_whole_values_without_a_synthetic_batch_byte_limit() {
    let (fixture, name, expected) = long_value_source();
    let mut configured = policy();
    configured.resource_envelope.memory_budget_bytes = 512 << 20;
    let prepared = prepare_relational(&fixture.scan(), configured).unwrap();
    let memory = prepared.session.memory().clone();
    let baseline = memory.snapshot().reserved_bytes;
    for format in [Format::Json, Format::Jsonl, Format::Csv] {
        let path = fixture.0.join(format!("long-text.{}", format.as_str()));
        let report = prepared
            .write(&path, format, false)
            .unwrap_or_else(|error| panic!("{format:?}: {error}"));
        assert_eq!(report.output.rows_written, 3);
        assert!(!report.output.evidence.side_effects.fallback_attempted);
        let output = fs::read_to_string(&path).unwrap();
        if format == Format::Csv {
            // The first two values contain no delimiter/quote/newline. The
            // last is one quoted multiline field with doubled quote bytes.
            let expected_csv = format!(
                "{name}\n{}\n{}\n\"{}\"\n",
                expected[0],
                expected[1],
                expected[2].replace('"', "\"\"")
            );
            assert_eq!(output.len(), expected_csv.len());
            assert!(output == expected_csv, "complete CSV bytes differ");
        } else {
            let rows: Vec<serde_json::Value> = if format == Format::Json {
                serde_json::from_str(&output).unwrap()
            } else {
                output
                    .lines()
                    .map(|line| serde_json::from_str(line).unwrap())
                    .collect()
            };
            assert_eq!(rows.len(), expected.len());
            for (row, wanted) in rows.iter().zip(&expected) {
                assert_eq!(row.as_object().unwrap().len(), 1);
                assert!(
                    row[&name].as_str() == Some(wanted.as_str()),
                    "{format:?} value differs"
                );
            }
        }
        drop(report);
        assert_eq!(memory.snapshot().reserved_bytes, baseline, "{format:?}");
    }
}

#[test]
fn low_writer_memory_and_precancel_leave_no_output_or_staging_and_release_reservations() {
    let (fixture, _, _) = long_value_source();
    let mut configured = policy();
    configured.resource_envelope.memory_budget_bytes = 32 << 20;
    let prepared = prepare_relational(&fixture.scan(), configured).unwrap();
    let memory = prepared.session.memory().clone();
    let baseline = memory.snapshot().reserved_bytes;

    for format in binary_formats() {
        let path = fixture.0.join(format!("denied.{}", format.as_str()));
        let error = prepared
            .write(&path, format, false)
            .err()
            .expect("writer memory/admission must reject the 9 MiB single value");
        assert!(
            error.to_string().to_lowercase().contains("memory")
                || error.to_string().to_lowercase().contains("resource")
                || error.to_string().to_lowercase().contains("admission"),
            "{format:?}: {error}"
        );
        assert!(!path.exists(), "{format:?}");
        assert_eq!(
            fs::read_dir(&fixture.0).unwrap().count(),
            1,
            "staging artifact for {format:?}"
        );
        assert_eq!(memory.snapshot().reserved_bytes, baseline, "{format:?}");
    }

    for format in binary_formats()
        .into_iter()
        .chain([Format::Json, Format::Jsonl, Format::Csv])
    {
        let path = fixture.0.join(format!("cancelled.{}", format.as_str()));
        let cancellation = CancellationToken::default();
        cancellation.cancel();
        assert!(
            prepared
                .write_controlled(&path, format, false, &cancellation)
                .is_err()
        );
        assert!(!path.exists(), "{format:?}");
        assert_eq!(
            fs::read_dir(&fixture.0).unwrap().count(),
            1,
            "staging artifact for cancelled {format:?}"
        );
        assert_eq!(
            memory.snapshot().reserved_bytes,
            baseline,
            "cancelled {format:?}"
        );
    }
}

#[test]
fn ordinary_file_fanout_executes_once_and_reopens_every_complete_destination() {
    let fixture = bulk_source();
    let mut configured = policy();
    configured.resource_envelope.memory_budget_bytes = 256 << 20;
    let prepared = prepare_relational(&fixture.scan(), configured).unwrap();
    let targets = [Format::Parquet, Format::ArrowIpc].map(|format| {
        (
            fixture.0.join(format!("fanout.{}", format.as_str())),
            format,
        )
    });
    let before = prepared.snapshot().completed_executions;
    let written = prepared
        .write_many_shared_controlled(&targets, false, &CancellationToken::default())
        .unwrap();
    assert_eq!(prepared.snapshot().completed_executions, before + 1);
    let replay = written.replay.as_ref().unwrap();
    assert_eq!(replay.replays, targets.len());
    assert_eq!(replay.rows_detached, ROWS as u64);
    assert!(replay.buffer_bytes_copied > 0);
    assert_eq!(written.execution.output_rows, ROWS as u64);
    for ((path, format), report) in targets.iter().zip(&written.outputs) {
        assert_eq!(report.rows_written, ROWS as u64);
        assert_bulk_rows(path, *format);
    }
}

#[test]
fn fanout_report_reservation_failure_cleans_staging_before_any_publication() {
    let fixture = bulk_source();
    let mut configured = policy();
    configured.resource_envelope.memory_budget_bytes = 256 << 20;
    let prepared = prepare_relational(&fixture.scan(), configured).unwrap();
    let memory = prepared.session.memory().clone();
    let baseline = memory.snapshot().reserved_bytes;
    let targets = [Format::Parquet, Format::ArrowIpc].map(|format| {
        (
            fixture.0.join(format!("denied-report.{}", format.as_str())),
            format,
        )
    });
    let called = std::cell::Cell::new(false);
    let result = prepared.write_many_with_completion(
        &targets,
        false,
        &CancellationToken::default(),
        None,
        |_| {
            called.set(true);
            let _denied = memory.reserve(memory.snapshot().limit_bytes)?;
            Ok(())
        },
    );
    assert!(called.get());
    assert!(
        result
            .err()
            .unwrap()
            .to_string()
            .contains("reservation denied")
    );
    for (path, _) in &targets {
        assert!(!path.exists());
    }
    assert_eq!(fs::read_dir(&fixture.0).unwrap().count(), 1);
    assert_eq!(memory.snapshot().reserved_bytes, baseline);
}

#[test]
fn large_nested_documents_survive_width_splitting_and_all_binary_writers() {
    use vortex::array::arrays::ListViewArray;
    let document = "東京🙂".repeat((9 << 20) / "東京🙂".len());
    let children = StructArray::new(
        FieldNames::from(["document"]),
        vec![
            VarBinViewArray::from_iter_nullable_str([Some(document.as_str()), None, Some("tail")])
                .into_array(),
        ],
        3,
        Validity::from_iter([true, false, true]),
    )
    .into_array();
    let lists = ListViewArray::try_new(
        children,
        PrimitiveArray::from_iter([0_u64, 0, 3, 2]).into_array(),
        PrimitiveArray::from_iter([1_u64, 2, 0, 1]).into_array(),
        Validity::from_iter([true, false, true, true]),
        &mut vortex::array::VortexSessionExecute::create_execution_ctx(
            &vortex::array::array_session(),
        ),
    )
    .unwrap()
    .into_array();
    let input = StructArray::new(
        FieldNames::from(["items"]),
        vec![lists],
        4,
        Validity::NonNullable,
    )
    .into_array();
    let fixture = Fixture::new(input, 4);
    let mut configured = policy();
    configured.resource_envelope.memory_budget_bytes = 512 << 20;
    let prepared = prepare_relational(&fixture.scan(), configured).unwrap();
    for format in binary_formats() {
        let path = fixture.0.join(format!("nested-long.{}", format.as_str()));
        let report = prepared
            .write(&path, format, false)
            .unwrap_or_else(|error| panic!("{format:?}: {error}"));
        assert_eq!(report.output.rows_written, 4);
        let mut actual = Vec::new();
        for batch in read_batches(&path, format) {
            let lists = batch.column(0);
            for row in 0..batch.num_rows() {
                if lists.is_null(row) {
                    actual.push(None);
                    continue;
                }
                let values = match lists.data_type() {
                    arrow_schema::DataType::List(_) => lists
                        .as_any()
                        .downcast_ref::<arrow_array::ListArray>()
                        .unwrap()
                        .value(row),
                    arrow_schema::DataType::LargeList(_) => lists
                        .as_any()
                        .downcast_ref::<arrow_array::LargeListArray>()
                        .unwrap()
                        .value(row),
                    other => panic!("unexpected list {other:?}"),
                };
                let structs = values
                    .as_any()
                    .downcast_ref::<arrow_array::StructArray>()
                    .unwrap();
                actual.push(Some(
                    (0..structs.len())
                        .map(|index| {
                            assert!(!structs.is_null(index));
                            string_value(structs.column(0).as_ref(), index).to_owned()
                        })
                        .collect::<Vec<_>>(),
                ));
            }
        }
        assert_eq!(
            actual,
            [
                Some(vec![document.clone()]),
                None,
                Some(Vec::new()),
                Some(vec!["tail".into()])
            ],
            "{format:?}"
        );
    }
}
