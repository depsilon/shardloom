use super::*;
use crate::local_primitives::VortexLocalPrimitiveRowExportFormat as Format;
use crate::relational_query::VortexRelationalLimit;
use serde_json::{Value, json};

fn complex_fixture() -> (Fixture, Vec<Value>) {
    let coordinates = FixedSizeListArray::try_new(
        PrimitiveArray::from_iter([-1i16, 2, 99, 99, 3, 4, 5, 6]).into_array(),
        2,
        Validity::from_iter([true, false, true, true]),
        4,
    )
    .unwrap()
    .into_array();
    let detail = StructArray::new(
        FieldNames::from(["tag", "coordinates"]),
        vec![
            VarBinArray::from(vec![Some("東京"), Some("hidden"), None, Some("a'b")]).into_array(),
            coordinates,
        ],
        4,
        Validity::from_iter([true, false, true, true]),
    )
    .into_array();
    let input = StructArray::new(
        FieldNames::from(["id", "items", "detail"]),
        vec![
            PrimitiveArray::from_iter([1u32, 2, 3, 4]).into_array(),
            lists(),
            detail,
        ],
        4,
        Validity::NonNullable,
    )
    .into_array();
    (
        Fixture::new(input, 2),
        vec![
            json!({"id":1,"items":[9,null],"detail":{"tag":"東京","coordinates":[-1,2]}}),
            json!({"id":2,"items":[],"detail":null}),
            json!({"id":3,"items":null,"detail":{"tag":null,"coordinates":[3,4]}}),
            json!({"id":4,"items":[-4],"detail":{"tag":"a'b","coordinates":[5,6]}}),
        ],
    )
}

fn struct_list_fixture() -> (Fixture, Vec<Value>) {
    let records = StructArray::new(
        FieldNames::from(["code", "label"]),
        vec![
            lists(),
            VarBinArray::from(vec![Some("a'b"), None, Some("東京"), Some("tail")]).into_array(),
        ],
        4,
        Validity::from_iter([true, false, true, true]),
    )
    .into_array();
    let items = ListViewArray::try_new(
        records.clone(),
        PrimitiveArray::from_iter([0u64, 2, 2, 2]).into_array(),
        PrimitiveArray::from_iter([2u64, 0, 0, 2]).into_array(),
        Validity::from_iter([true, true, false, true]),
    )
    .unwrap()
    .into_array();
    let detail = StructArray::new(
        FieldNames::from(["nested"]),
        vec![records],
        4,
        Validity::NonNullable,
    )
    .into_array();
    let input = StructArray::new(
        FieldNames::from(["id", "items", "detail"]),
        vec![
            PrimitiveArray::from_iter([1u32, 2, 3, 4]).into_array(),
            items,
            detail,
        ],
        4,
        Validity::NonNullable,
    )
    .into_array();
    let a = json!({"code":[9,null],"label":"a'b"});
    let b = json!({"code":null,"label":"東京"});
    let c = json!({"code":[-4],"label":"tail"});
    (
        Fixture::new(input, 2),
        vec![
            json!({"id":1,"items":[a,null],"detail":{"nested":a}}),
            json!({"id":2,"items":[],"detail":{"nested":null}}),
            json!({"id":3,"items":null,"detail":{"nested":b}}),
            json!({"id":4,"items":[b,c],"detail":{"nested":c}}),
        ],
    )
}

fn json_scalar(value: shardloom_core::ScalarValue) -> Value {
    use shardloom_core::ScalarValue as S;
    match value {
        S::Null => Value::Null,
        S::Boolean(v) => v.into(),
        S::Int64(v) => v.into(),
        S::UInt64(v) => v.into(),
        S::Float64(v) => json!(v),
        S::Utf8(v) => v.into(),
        S::List(items) => Value::Array(items.into_iter().map(json_scalar).collect()),
        S::Struct(fields) => Value::Object(
            fields
                .into_iter()
                .map(|(key, value)| (key, json_scalar(value)))
                .collect(),
        ),
        other => panic!("unexpected nested fixture scalar {other:?}"),
    }
}

fn reopen(path: &std::path::Path, format: Format, dtype: &DType) -> Vec<Value> {
    match format {
        Format::Vortex => {
            let plan = VortexRelationalPlan::Scan(VortexRelationalScan {
                source_uri: DatasetUri::new(path.display().to_string()).unwrap(),
                projection: shardloom_plan::ProjectionRequest::All,
                predicate: None,
            });
            let prepared = prepare_relational(&plan, policy()).unwrap();
            assert_eq!(&prepared.output_dtype().unwrap(), dtype);
            json_rows(
                &prepared
                    .collect_jsonl(&CancellationToken::default())
                    .unwrap(),
            )
        }
        Format::Json => serde_json::from_slice(&fs::read(path).unwrap()).unwrap(),
        Format::Jsonl => fs::read_to_string(path)
            .unwrap()
            .lines()
            .map(|line| serde_json::from_str(line).unwrap())
            .collect(),
        _ => {
            let table = match format {
                Format::ArrowIpc => crate::read_flat_arrow_ipc_source(path, 100),
                Format::Parquet => crate::read_flat_parquet_source(path, 100),
                Format::Avro => crate::read_flat_avro_source(path, 100),
                _ => panic!("not an admitted nested destination"),
            }
            .unwrap();
            assert_eq!(table.header, vec!["id", "items", "detail"]);
            table
                .rows
                .into_iter()
                .map(|row| {
                    Value::Object(
                        row.into_iter()
                            .map(|(key, value)| (key, json_scalar(value)))
                            .collect(),
                    )
                })
                .collect()
        }
    }
}

#[test]
fn native_nested_six_writers_preserve_complete_values_and_typed_empty_results() {
    let (fixture, expected) = complex_fixture();
    check_six_writers(&fixture, &expected);
}

#[test]
fn native_nested_six_writers_preserve_list_struct_and_struct_struct_payloads() {
    let (fixture, expected) = struct_list_fixture();
    check_six_writers(&fixture, &expected);
}

fn check_six_writers(fixture: &Fixture, expected: &[Value]) {
    assert_eq!(collect(&fixture.scan()), expected);
    for count in [4, 0] {
        let plan = VortexRelationalPlan::Limit(Box::new(VortexRelationalLimit {
            input: fixture.scan(),
            offset: 0,
            count,
        }));
        let prepared = prepare_relational(&plan, policy()).unwrap();
        for format in [
            Format::Vortex,
            Format::Json,
            Format::Jsonl,
            Format::ArrowIpc,
            Format::Parquet,
            Format::Avro,
        ] {
            let path = fixture
                .0
                .join(format!("nested-{count}.{}", format.as_str()));
            let result = prepared
                .write(&path, format, false)
                .unwrap_or_else(|error| panic!("{format:?} {count}: {error}"));
            assert_eq!(result.output.rows_written, count as u64);
            assert!(result.execution.native_io_certificate.is_certified());
            if matches!(format, Format::Json | Format::Jsonl) {
                let copied = expected[..count]
                    .iter()
                    .map(utf8_payload_bytes)
                    .sum::<u64>();
                assert_eq!(
                    result
                        .output
                        .evidence
                        .native_array_sink
                        .as_ref()
                        .unwrap()
                        .adapter_payload_bytes_copied,
                    copied,
                    "{format:?} {count} UTF-8 payload copies"
                );
            }
            assert_eq!(
                reopen(&path, format, &prepared.output_dtype().unwrap()),
                expected[..count],
                "{format:?} {count}"
            );
        }
        for format in [Format::Csv, Format::Orc] {
            let path = fixture
                .0
                .join(format!("denied-{count}.{}", format.as_str()));
            let executions = prepared.snapshot().completed_executions;
            let error = prepared.write(&path, format, false).err().unwrap();
            assert!(error.to_string().contains("nested"), "{error}");
            assert!(!path.exists());
            assert_eq!(prepared.snapshot().completed_executions, executions);
        }
    }
}

fn utf8_payload_bytes(value: &Value) -> u64 {
    match value {
        Value::String(text) => text.len() as u64,
        Value::Array(items) => items.iter().map(utf8_payload_bytes).sum(),
        Value::Object(fields) => fields.values().map(utf8_payload_bytes).sum(),
        _ => 0,
    }
}

#[test]
fn native_nested_columnar_intake_reuses_shared_owned_stream_and_empty_schema() {
    for (fixture, expected) in [complex_fixture(), struct_list_fixture()] {
        check_nested_intake(&fixture, &expected);
    }
}

fn write_nested_intake(
    ipc: &std::path::Path,
    native: &std::path::Path,
    mode: &str,
) -> crate::vortex_ingest::VortexPreparedStateWriteReport {
    use crate::vortex_ingest::{
        VortexPreparedStateColumnarStreamWriteRequest, VortexPreparedStateColumnarWriteRequest,
        write_flat_columnar_vortex_prepared_state,
        write_flat_columnar_vortex_prepared_state_streaming,
    };
    if mode == "buffered" {
        let source = crate::read_flat_arrow_ipc_columnar_source(ipc, 100).unwrap();
        write_flat_columnar_vortex_prepared_state(VortexPreparedStateColumnarWriteRequest::new(
            native, source,
        ))
    } else {
        assert!(matches!(mode, "streamed" | "budgeted"));
        let source = crate::stream_flat_arrow_ipc_columnar_source(ipc, 100).unwrap();
        let request = VortexPreparedStateColumnarStreamWriteRequest::new(native, source);
        write_flat_columnar_vortex_prepared_state_streaming(if mode == "budgeted" {
            request.shared_native_memory_budget_bytes(16 << 20)
        } else {
            request
        })
    }
    .unwrap()
}

fn check_nested_intake(fixture: &Fixture, expected: &[Value]) {
    for count in [4, 0] {
        let plan = VortexRelationalPlan::Limit(Box::new(VortexRelationalLimit {
            input: fixture.scan(),
            offset: 0,
            count,
        }));
        let prepared = prepare_relational(&plan, policy()).unwrap();
        let exported = fixture.0.join(format!("intake-export-{count}.arrow"));
        prepared.write(&exported, Format::ArrowIpc, false).unwrap();
        let source =
            arrow_ipc::reader::FileReader::try_new(fs::File::open(exported).unwrap(), None)
                .unwrap();
        let ipc = fixture.0.join(format!("intake-{count}.arrow"));
        let mut writer = arrow_ipc::writer::FileWriter::try_new(
            fs::File::create(&ipc).unwrap(),
            &source.schema(),
        )
        .unwrap();
        for batch in source {
            let batch = batch.unwrap();
            for start in (0..batch.num_rows()).step_by(2) {
                writer
                    .write(&batch.slice(start, (batch.num_rows() - start).min(2)))
                    .unwrap();
            }
        }
        writer.finish().unwrap();
        for mode in ["buffered", "streamed", "budgeted"] {
            let native = fixture.0.join(format!("intake-{count}-{mode}.vortex"));
            let report = write_nested_intake(&ipc, &native, mode);
            assert_eq!(report.reopen_row_count, count as u64);
            assert!(
                report
                    .writer_layout_strategy_applied
                    .contains("nested_field_layout=chunked_flat_preserving_validity")
            );
            assert!(
                report
                    .writer_compression_policy
                    .contains("scope=scalar_fields;nested_fields=preserved_uncompressed")
            );
            assert!(
                report
                    .writer_coalescing_policy_status
                    .contains("scope=scalar_fields;nested_fields=source_chunks")
            );
            if mode == "budgeted" {
                let memory = report.shared_native_memory.as_ref().unwrap();
                assert_eq!(memory.final_reserved_bytes, 0);
                assert!(memory.peak_reserved_bytes <= memory.limit_bytes);
                assert!(report.manual_scalar_copy_avoided);
            }
            let reopened = VortexRelationalPlan::Scan(VortexRelationalScan {
                source_uri: DatasetUri::new(native.display().to_string()).unwrap(),
                projection: shardloom_plan::ProjectionRequest::All,
                predicate: None,
            });
            let reopened = prepare_relational(&reopened, policy()).unwrap();
            assert_eq!(
                json_rows(
                    &reopened
                        .collect_jsonl(&CancellationToken::default())
                        .unwrap_or_else(|error| panic!(
                            "{count} {mode} {}: {error}",
                            reopened.output_dtype().unwrap()
                        ))
                ),
                expected[..count],
            );
            assert!(
                reopened.output_dtype().unwrap() == prepared.output_dtype().unwrap(),
                "{count} {mode}: {} != {}",
                reopened.output_dtype().unwrap(),
                prepared.output_dtype().unwrap()
            );
        }
    }
}

#[test]
fn native_nested_empty_intake_retains_nonnullable_lists_and_structs() {
    use crate::vortex_ingest::{
        VortexPreparedStateColumnarWriteRequest, write_flat_columnar_vortex_prepared_state,
    };
    use arrow_schema::{DataType, Field, Schema};
    use vortex::arrow::ArrowSessionExt as _;
    let fixture = fixture();
    for nullable in [false, true] {
        let item = std::sync::Arc::new(Field::new("item", DataType::Int32, true));
        let schema = std::sync::Arc::new(Schema::new(vec![
            Field::new("list", DataType::List(item.clone()), nullable),
            Field::new("large", DataType::LargeList(item.clone()), nullable),
            Field::new("fixed", DataType::FixedSizeList(item, 2), nullable),
            Field::new(
                "record",
                DataType::Struct(vec![Field::new("label", DataType::Utf8, true)].into()),
                nullable,
            ),
        ]));
        let ipc = fixture.0.join(format!("empty-fields-{nullable}.arrow"));
        arrow_ipc::writer::FileWriter::try_new(fs::File::create(&ipc).unwrap(), &schema)
            .unwrap()
            .finish()
            .unwrap();
        let source = crate::read_flat_arrow_ipc_columnar_source(&ipc, 1).unwrap();
        assert_eq!(source.row_count, 0);
        let expected = vortex::session::VortexSession::default()
            .arrow()
            .from_arrow_datatype(
                &DataType::Struct(schema.fields().clone()),
                Nullability::NonNullable,
            )
            .unwrap();
        for mode in ["buffered", "streamed", "budgeted"] {
            let native = fixture
                .0
                .join(format!("empty-fields-{nullable}-{mode}.vortex"));
            let report = write_nested_intake(&ipc, &native, mode);
            assert_eq!(report.reopen_row_count, 0);
            let scan = VortexRelationalPlan::Scan(VortexRelationalScan {
                source_uri: DatasetUri::new(native.display().to_string()).unwrap(),
                projection: shardloom_plan::ProjectionRequest::All,
                predicate: None,
            });
            let actual = prepare_relational(&scan, policy())
                .unwrap()
                .output_dtype()
                .unwrap();
            assert!(actual == expected, "{mode}: {actual} != {expected}");
        }

        let mut no_fields = source;
        no_fields.batches.clear();
        let denied = fixture.0.join(format!("missing-fields-{nullable}.vortex"));
        let error = write_flat_columnar_vortex_prepared_state(
            VortexPreparedStateColumnarWriteRequest::new(&denied, no_fields),
        )
        .unwrap_err();
        assert!(error.to_string().contains("retaining field nullability"));
        assert!(!denied.exists());
    }
}

#[test]
fn native_nested_json_writer_counts_flat_utf8_bytes_without_names_or_escapes() {
    let values = vec![Some("東京"), Some("a\n\"λ"), None, Some("")];
    let fixture = Fixture::new(
        StructArray::new(
            FieldNames::from(["a_long_field_name"]),
            vec![VarBinArray::from(values.clone()).into_array()],
            values.len(),
            Validity::NonNullable,
        )
        .into_array(),
        2,
    );
    let prepared = prepare_relational(&fixture.scan(), policy()).unwrap();
    for format in [Format::Json, Format::Jsonl, Format::Csv] {
        let output = fixture.0.join(format!("utf8.{}", format.as_str()));
        let report = prepared.write(&output, format, false).unwrap();
        assert_eq!(
            report
                .output
                .evidence
                .native_array_sink
                .unwrap()
                .adapter_payload_bytes_copied,
            11
        );
    }
}
