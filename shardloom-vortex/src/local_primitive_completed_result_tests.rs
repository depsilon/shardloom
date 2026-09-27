use super::*;

#[cfg(feature = "universal-format-io")]
fn read_binary_values(
    path: &Path,
    format: crate::VortexLocalPrimitiveRowExportFormat,
    columns: &[String],
) -> serde_json::Value {
    use crate::VortexLocalPrimitiveRowExportFormat as Format;
    if format == Format::Vortex {
        let session =
            crate::resident_session::ResidentVortexSession::new(256 * 1024 * 1024, 1).unwrap();
        let source = session.prepare_file(path).unwrap();
        let names = columns.iter().map(String::as_str).collect::<Vec<_>>();
        let result = source
            .prepare_projection(&names, 65536, 8 * 1024 * 1024)
            .unwrap()
            .execute()
            .unwrap();
        return serde_json::from_str(result.to_bounded_json(columns, 65536).unwrap().value())
            .unwrap();
    }
    let table = match format {
        Format::Parquet => crate::read_flat_parquet_source(path, 65536),
        Format::ArrowIpc => crate::read_flat_arrow_ipc_source(path, 65536),
        Format::Avro => crate::read_flat_avro_source(path, 65536),
        Format::Orc => crate::read_flat_orc_source(path, 65536),
        _ => panic!("binary format required"),
    }
    .unwrap();
    assert_eq!(table.header, columns);
    serde_json::Value::Array(
        table
            .rows
            .into_iter()
            .map(|row| {
                serde_json::Value::Object(
                    row.into_iter()
                        .map(|(name, value)| {
                            let value = match value {
                                shardloom_core::ScalarValue::Null => serde_json::Value::Null,
                                shardloom_core::ScalarValue::Int64(value) => {
                                    serde_json::json!(value)
                                }
                                shardloom_core::ScalarValue::UInt64(value) => {
                                    serde_json::json!(value)
                                }
                                shardloom_core::ScalarValue::Float64(value) => {
                                    serde_json::json!(value)
                                }
                                shardloom_core::ScalarValue::Utf8(value) => {
                                    serde_json::json!(value)
                                }
                                other => panic!("unexpected test value {other:?}"),
                            };
                            (name, value)
                        })
                        .collect(),
                )
            })
            .collect(),
    )
}

fn mixed_request(path: &Path, grouped: bool) -> VortexQueryPrimitiveRequest {
    let measures = vec![
        VortexSimpleAggregateMeasure::new("count", None, "n".into()),
        VortexSimpleAggregateMeasure::new(
            "sum",
            Some(ColumnRef::new(VALUE).unwrap()),
            "total".into(),
        ),
        VortexSimpleAggregateMeasure::new(
            "avg",
            Some(ColumnRef::new(VALUE).unwrap()),
            "mean".into(),
        ),
        VortexSimpleAggregateMeasure::new(
            "min",
            Some(ColumnRef::new(KEY).unwrap()),
            "smallest".into(),
        ),
        VortexSimpleAggregateMeasure::new(
            "max",
            Some(ColumnRef::new(KEY).unwrap()),
            "largest".into(),
        ),
    ];
    let aggregate = if grouped {
        VortexSimpleAggregateRequest::grouped(vec![ColumnRef::new(KEY).unwrap()], measures)
            .with_order_by(vec![VortexAggregateOrderExpr::new(KEY, false)])
    } else {
        VortexSimpleAggregateRequest::new(measures)
    };
    let request = VortexQueryPrimitiveRequest::simple_aggregate(
        DatasetUri::new(path.display().to_string()).unwrap(),
        aggregate,
    );
    if grouped {
        request.with_source_order_limit(10)
    } else {
        request
    }
}

#[test]
fn completed_mixed_aggregate_preserves_scalar_grouped_empty_schema_and_native_ownership() {
    for empty in [false, true] {
        let fixture = Fixture::new();
        let keys = if empty {
            vec![]
        } else {
            vec![i64::MIN, i64::MIN, i64::MAX]
        };
        let values = if empty { vec![] } else { vec![2, 4, 9] };
        let path = fixture.source(
            PrimitiveArray::new(keys, Validity::NonNullable).into_array(),
            values,
        );
        for grouped in [false, true] {
            let query = mixed_request(&path, grouped);
            let columns = query.simple_aggregate.as_ref().unwrap().output_columns();
            let prepared = prepare_aggregate(
                &query,
                VortexLocalPrimitiveExecutionPolicy::single_threaded(),
            )
            .unwrap();
            let memory = prepared.session.memory().clone();
            let result = prepared.execute_owned().unwrap();
            assert_eq!(prepared.snapshot().completed_executions, 1);
            assert!(result.execution.native_io_certificate.is_certified());
            let fields = result.result.dtype().as_struct_fields_opt().unwrap();
            assert_eq!(
                fields.field("n"),
                Some(DType::Primitive(PType::U64, Nullability::NonNullable))
            );
            assert_eq!(
                fields.field("total"),
                Some(DType::Primitive(PType::F64, Nullability::Nullable))
            );
            assert_eq!(
                fields.field("smallest"),
                Some(DType::Primitive(PType::I64, Nullability::Nullable))
            );
            let rows: serde_json::Value = serde_json::from_str(
                result
                    .result
                    .to_bounded_json(&columns, 65536)
                    .unwrap()
                    .value(),
            )
            .unwrap();
            let expected = match (empty, grouped) {
                (true, true) => serde_json::json!([]),
                (true, false) => {
                    serde_json::json!([{"n":0,"total":null,"mean":null,"smallest":null,"largest":null}])
                }
                (false, false) => {
                    serde_json::json!([{"n":3,"total":15.0,"mean":5.0,"smallest":i64::MIN,"largest":i64::MAX}])
                }
                (false, true) => serde_json::json!([
                    {KEY:i64::MIN,"n":2,"total":6.0,"mean":3.0,"smallest":i64::MIN,"largest":i64::MIN},
                    {KEY:i64::MAX,"n":1,"total":9.0,"mean":9.0,"smallest":i64::MAX,"largest":i64::MAX}
                ]),
            };
            assert_eq!(rows, expected);
            drop(prepared);
            assert_eq!(
                serde_json::from_str::<serde_json::Value>(
                    result
                        .result
                        .to_bounded_json(&columns, 65536)
                        .unwrap()
                        .value()
                )
                .unwrap(),
                expected
            );
            drop(result);
            assert_eq!(memory.snapshot().reserved_bytes, 0);
        }
    }
}

#[cfg(feature = "universal-format-io")]
#[test]
fn completed_mixed_aggregate_exports_all_binary_formats_and_preserves_existing_files() {
    let fixture = Fixture::new();
    let path = fixture.source(
        PrimitiveArray::new(vec![1_i64, 1, 3], Validity::NonNullable).into_array(),
        vec![2, 4, 9],
    );
    let query = mixed_request(&path, true);
    for format in [
        crate::VortexLocalPrimitiveRowExportFormat::Vortex,
        crate::VortexLocalPrimitiveRowExportFormat::Parquet,
        crate::VortexLocalPrimitiveRowExportFormat::ArrowIpc,
        crate::VortexLocalPrimitiveRowExportFormat::Avro,
        crate::VortexLocalPrimitiveRowExportFormat::Orc,
    ] {
        let output = fixture.0.join(format!("mixed.{}", format.as_str()));
        let prepared = prepare_aggregate(
            &query,
            VortexLocalPrimitiveExecutionPolicy::single_threaded(),
        )
        .unwrap();
        let report = prepared
            .execute_owned()
            .unwrap()
            .write(&output, format, false)
            .unwrap();
        assert_eq!(report.rows_written, 2);
        assert_eq!(prepared.snapshot().completed_executions, 1);
        let bytes = fs::read(&output).unwrap();
        assert!(!bytes.is_empty());
        assert!(
            prepared
                .execute_owned()
                .unwrap()
                .write(&output, format, false)
                .is_err()
        );
        assert_eq!(fs::read(&output).unwrap(), bytes);
        assert_eq!(
            read_binary_values(
                &output,
                format,
                &query.simple_aggregate.as_ref().unwrap().output_columns()
            ),
            serde_json::json!([
                {KEY:1,"n":2,"total":6.0,"mean":3.0,"smallest":1,"largest":1},
                {KEY:3,"n":1,"total":9.0,"mean":9.0,"smallest":3,"largest":3}
            ])
        );
    }
}

#[cfg(feature = "universal-format-io")]
#[test]
fn completed_sorted_and_empty_results_export_every_format_without_losing_values() {
    use crate::VortexLocalPrimitiveRowExportFormat as Format;
    for empty in [false, true] {
        let fixture = Fixture::new();
        let (keys, values) = if empty {
            (vec![], vec![])
        } else {
            (vec![i64::MAX, i64::MIN, 5], vec![9, 3, 8])
        };
        let source = fixture.source(
            PrimitiveArray::new(keys, Validity::NonNullable).into_array(),
            values,
        );
        let query = VortexQueryPrimitiveRequest::sort_rows(
            DatasetUri::new(source.display().to_string()).unwrap(),
            super::super::super::ProjectionRequest::All,
            None,
            crate::VortexSortRowsRequest::new(vec![VortexAggregateOrderExpr::new(KEY, false)]),
            2,
        );
        let columns = vec![KEY.to_string(), VALUE.to_string()];
        let expected = if empty {
            serde_json::json!([])
        } else {
            serde_json::json!([{KEY:i64::MIN,VALUE:3},{KEY:5,VALUE:8}])
        };
        for format in [
            Format::Vortex,
            Format::Parquet,
            Format::ArrowIpc,
            Format::Avro,
            Format::Orc,
            Format::Json,
            Format::Jsonl,
            Format::Csv,
        ] {
            let output = fixture.0.join(format!("sorted.{}", format.as_str()));
            let report = crate::execute_vortex_local_primitive_row_export_with_policy(
                &query,
                &output,
                format,
                false,
                VortexLocalPrimitiveExecutionPolicy::single_threaded(),
            )
            .unwrap();
            assert!(!report.has_errors(), "{format:?}: {report:?}");
            assert_eq!(report.rows_written, if empty { 0 } else { 2 });
            match format {
                Format::Json => assert_eq!(
                    serde_json::from_slice::<serde_json::Value>(&fs::read(&output).unwrap())
                        .unwrap(),
                    expected
                ),
                Format::Jsonl => {
                    let rows = fs::read_to_string(&output)
                        .unwrap()
                        .lines()
                        .map(|row| serde_json::from_str(row).unwrap())
                        .collect::<Vec<serde_json::Value>>();
                    assert_eq!(serde_json::Value::Array(rows), expected);
                }
                Format::Csv => assert_eq!(
                    fs::read_to_string(&output).unwrap(),
                    if empty {
                        format!("{KEY},{VALUE}\n")
                    } else {
                        format!("{KEY},{VALUE}\n{},3\n5,8\n", i64::MIN)
                    }
                ),
                _ => assert_eq!(read_binary_values(&output, format, &columns), expected),
            }
        }
        assert!(fs::read_dir(&fixture.0).unwrap().all(|entry| {
            !entry
                .unwrap()
                .file_name()
                .to_string_lossy()
                .starts_with(".shardloom-json-")
        }));
    }
}
