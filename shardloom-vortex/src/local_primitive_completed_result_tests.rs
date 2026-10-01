#[cfg(feature = "universal-format-io")]
use super::super::super as runtime;
use super::*;
#[cfg(feature = "universal-format-io")]
use vortex::array::VortexSessionExecute as _;

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
        return source
            .with_native_execution(|file, session, runtime_handle| {
                let mut execution_context = session.create_execution_ctx();
                let mut rows = Vec::new();
                for array in file
                    .scan()
                    .unwrap()
                    .with_ordered(true)
                    .into_array_iter(runtime_handle)
                    .unwrap()
                {
                    let array = array.unwrap();
                    let fields = columns
                        .iter()
                        .map(|name| runtime::logical_field_from_native_array(&array, name).unwrap())
                        .collect::<Vec<_>>();
                    for row in 0..array.len() {
                        let mut object = serde_json::Map::with_capacity(columns.len());
                        for (name, field) in columns.iter().zip(&fields) {
                            let scalar = field.execute_scalar(row, &mut execution_context).unwrap();
                            let value = if scalar.is_null() {
                                serde_json::Value::Null
                            } else {
                                let value = runtime::vortex_scalar_to_stat_value(&scalar).unwrap();
                                runtime::stat_value_to_json_value(&value).unwrap()
                            };
                            object.insert(name.clone(), value);
                        }
                        rows.push(serde_json::Value::Object(object));
                    }
                }
                Ok(serde_json::Value::Array(rows))
            })
            .unwrap();
    }
    let table = match format {
        Format::Parquet => crate::read_flat_parquet_source(path, 100_000),
        Format::ArrowIpc => crate::read_flat_arrow_ipc_source(path, 100_000),
        Format::Avro => crate::read_flat_avro_source(path, 100_000),
        Format::Orc => crate::read_flat_orc_source(path, 100_000),
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

#[test]
fn completed_general_result_clones_keep_buffer_credit_after_all_producer_owners_drop() {
    let fixture = Fixture::new();
    let path = fixture.source(
        PrimitiveArray::new(vec![1_i64, 1, 3], Validity::NonNullable).into_array(),
        vec![2, 4, 9],
    );
    let query = mixed_request(&path, true);
    let prepared = prepare_aggregate(
        &query,
        VortexLocalPrimitiveExecutionPolicy::single_threaded(),
    )
    .unwrap();
    let memory = prepared.session.memory().clone();
    let executed = prepared.execute_owned().unwrap();
    let slice = executed.result.arrays()[0].slice(1..2).unwrap();
    let clone = slice.clone();
    drop((executed, prepared, slice));
    assert!(memory.snapshot().reserved_bytes > 0);
    assert_eq!(clone.len(), 1);
    drop(clone);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

#[cfg(feature = "universal-format-io")]
#[test]
fn completed_mixed_aggregate_exports_all_binary_formats_and_preserves_existing_files() {
    let fixture = Fixture::new();
    let path = fixture.source(
        PrimitiveArray::new(vec![1_i64, 1, 3], Validity::NonNullable).into_array(),
        vec![2, 4, 9],
    );
    let mut query = mixed_request(&path, true);
    query.source_order_limit = None;
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
        assert_ne!(bytes, [] as [u8; 0]);
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
fn completed_grouped_output_without_limit_rejects_excess_cardinality() {
    let fixture = Fixture::new();
    let keys = (0_i64..65_537).collect::<Vec<_>>();
    let path = fixture.source(
        PrimitiveArray::new(keys, Validity::NonNullable).into_array(),
        vec![1; 65_537],
    );
    let mut query = mixed_request(&path, true);
    query.source_order_limit = None;
    let prepared = prepare_aggregate(
        &query,
        VortexLocalPrimitiveExecutionPolicy::single_threaded(),
    )
    .unwrap();
    let Err(error) = prepared.execute_owned() else {
        panic!("unbounded result was admitted");
    };
    assert!(
        error
            .to_string()
            .contains("without a limit exceeds 65536 groups"),
        "{error}"
    );
    // The same large input remains admitted with a small explicit result limit.
    query.source_order_limit = Some(2);
    let prepared = prepare_aggregate(
        &query,
        VortexLocalPrimitiveExecutionPolicy::single_threaded(),
    )
    .unwrap();
    assert_eq!(prepared.execute_owned().unwrap().result.row_count(), 2);
}

#[cfg(feature = "universal-format-io")]
#[test]
#[allow(clippy::too_many_lines)] // Keep the full-format ordered row acceptance proof together.
fn result_stream_aggregate_exports_every_format_above_collect_limit() {
    use std::fmt::Write as _;

    use crate::VortexLocalPrimitiveRowExportFormat as Format;

    const ROWS: usize = 70_017;
    let fixture = Fixture::new();
    let path = fixture.0.join("large-stream-source.vortex");
    let runtime = super::super::super::local_vortex_runtime(
        VortexLocalPrimitiveExecutionPolicy::single_threaded(),
    );
    let session = VortexSession::default().with_handle(runtime.handle());
    let keys = PrimitiveArray::new(
        (0..ROWS)
            .map(|row| i64::try_from(row).unwrap())
            .collect::<Vec<_>>(),
        Validity::NonNullable,
    )
    .into_array();
    let values = PrimitiveArray::new(vec![1_u64; ROWS], Validity::NonNullable).into_array();
    let array = StructArray::new(
        [KEY, VALUE].into(),
        vec![keys, values],
        ROWS,
        Validity::NonNullable,
    )
    .into_array();
    let dtype = array.dtype().clone();
    let mut file = fs::File::create(&path).unwrap();
    let mut writer = session
        .write_options()
        .with_strategy(
            super::super::super::native_flat_layout::SequentialNativeFlatLayout::strategy(
                ROWS.div_ceil(4096),
            ),
        )
        .with_file_statistics(Vec::new())
        .blocking(&runtime)
        .writer(&mut file, dtype);
    for start in (0..ROWS).step_by(4096) {
        writer
            .push(array.slice(start..ROWS.min(start + 4096)).unwrap())
            .unwrap();
    }
    assert_eq!(writer.finish().unwrap().row_count(), ROWS as u64);

    let query = VortexQueryPrimitiveRequest::simple_aggregate(
        DatasetUri::new(path.display().to_string()).unwrap(),
        VortexSimpleAggregateRequest::grouped(
            vec![ColumnRef::new(KEY).unwrap()],
            vec![VortexSimpleAggregateMeasure::new("count", None, "n".into())],
        )
        .with_order_by(vec![VortexAggregateOrderExpr::new(KEY, false)]),
    )
    .with_source_order_limit(ROWS);
    let columns = vec![KEY.to_string(), "n".to_string()];
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
        let output = fixture.0.join(format!("stream.{}", format.as_str()));
        let prepared = prepare_aggregate(
            &query,
            VortexLocalPrimitiveExecutionPolicy::single_threaded(),
        )
        .unwrap();
        let report = prepared.write(&output, format, false).unwrap();
        assert_eq!(report.rows_written, ROWS as u64, "{format:?}");

        let rows = match format {
            Format::Json => {
                serde_json::from_slice::<serde_json::Value>(&fs::read(&output).unwrap()).unwrap()
            }
            Format::Jsonl => serde_json::Value::Array(
                fs::read_to_string(&output)
                    .unwrap()
                    .lines()
                    .map(|line| serde_json::from_str(line).unwrap())
                    .collect(),
            ),
            Format::Csv => {
                let mut expected = format!("{KEY},n\n");
                for row in 0..ROWS {
                    writeln!(expected, "{row},1").unwrap();
                }
                assert_eq!(fs::read_to_string(&output).unwrap(), expected);
                continue;
            }
            _ => read_binary_values(&output, format, &columns),
        };
        let rows = rows.as_array().expect("all formats decode to a row array");
        assert_eq!(rows.len(), ROWS, "{format:?}");
        for (index, row) in rows.iter().enumerate() {
            assert_eq!(
                row[KEY],
                serde_json::json!(i64::try_from(index).unwrap()),
                "{format:?}, row {index}"
            );
            assert_eq!(
                row["n"],
                serde_json::json!(1_u64),
                "{format:?}, row {index}"
            );
        }
    }
}

#[test]
fn result_stream_text_preserves_nullable_utf8_and_escaping() {
    use crate::VortexLocalPrimitiveRowExportFormat as Format;
    use vortex::array::arrays::VarBinViewArray;

    let expected = serde_json::json!([
        {KEY: null, "n": 1},
        {KEY: "", "n": 1},
        {KEY: "comma,quote\"", "n": 1},
        {KEY: "line\nbreak", "n": 1},
        {KEY: "雪", "n": 1},
    ]);
    let fixture = Fixture::new();
    let path = fixture.0.join("nullable-utf8-source.vortex");
    let runtime = super::super::super::local_vortex_runtime(
        VortexLocalPrimitiveExecutionPolicy::single_threaded(),
    );
    let session = VortexSession::default().with_handle(runtime.handle());
    let keys = VarBinViewArray::from_iter_nullable_str([
        Some("雪"),
        Some(""),
        None,
        Some("comma,quote\""),
        Some("line\nbreak"),
    ])
    .into_array();
    let array = StructArray::new([KEY].into(), vec![keys], 5, Validity::NonNullable).into_array();
    let mut file = fs::File::create(&path).unwrap();
    let mut writer = session
        .write_options()
        .with_strategy(
            super::super::super::native_flat_layout::SequentialNativeFlatLayout::strategy(1),
        )
        .with_file_statistics(Vec::new())
        .blocking(&runtime)
        .writer(&mut file, array.dtype().clone());
    writer.push(array).unwrap();
    assert_eq!(writer.finish().unwrap().row_count(), 5);

    let query = VortexQueryPrimitiveRequest::simple_aggregate(
        DatasetUri::new(path.display().to_string()).unwrap(),
        VortexSimpleAggregateRequest::grouped(
            vec![ColumnRef::new(KEY).unwrap()],
            vec![VortexSimpleAggregateMeasure::new("count", None, "n".into())],
        )
        .with_order_by(vec![VortexAggregateOrderExpr::new(KEY, false)]),
    )
    .with_source_order_limit(5);
    for format in [Format::Json, Format::Jsonl, Format::Csv] {
        let output = fixture.0.join(format!("nullable.{}", format.as_str()));
        let report = prepare_aggregate(
            &query,
            VortexLocalPrimitiveExecutionPolicy::single_threaded(),
        )
        .unwrap()
        .write(&output, format, false)
        .unwrap();
        assert_eq!(report.rows_written, 5, "{format:?}");
        match format {
            Format::Json => assert_eq!(
                serde_json::from_slice::<serde_json::Value>(&fs::read(&output).unwrap()).unwrap(),
                expected,
            ),
            Format::Jsonl => {
                let rows = fs::read_to_string(&output)
                    .unwrap()
                    .lines()
                    .map(|line| serde_json::from_str(line).unwrap())
                    .collect::<Vec<serde_json::Value>>();
                assert_eq!(serde_json::Value::Array(rows), expected);
            }
            Format::Csv => assert_eq!(
                fs::read_to_string(&output).unwrap(),
                format!("{KEY},n\n,1\n\"\",1\n\"comma,quote\"\"\",1\n\"line\nbreak\",1\n雪,1\n"),
            ),
            _ => unreachable!("only text formats are listed"),
        }
    }
}

#[cfg(all(feature = "universal-format-io", feature = "vortex-write"))]
#[test]
fn result_stream_preserves_or_rejects_uint64_boundary_at_each_sink() {
    use crate::VortexLocalPrimitiveRowExportFormat as Format;

    let fixture = Fixture::new();
    let signed_max = u64::try_from(i64::MAX).unwrap();
    let source = fixture.source(
        PrimitiveArray::new(vec![-1_i64, 0, 1], Validity::NonNullable).into_array(),
        vec![0_u64, signed_max, u64::MAX],
    );
    let query = VortexQueryPrimitiveRequest::simple_aggregate(
        DatasetUri::new(source.display().to_string()).unwrap(),
        VortexSimpleAggregateRequest::grouped(
            vec![ColumnRef::new(KEY).unwrap()],
            vec![VortexSimpleAggregateMeasure::new(
                "max",
                Some(ColumnRef::new(VALUE).unwrap()),
                "peak".into(),
            )],
        )
        .with_order_by(vec![VortexAggregateOrderExpr::new(KEY, false)]),
    )
    .with_source_order_limit(3);
    let columns = vec![KEY.to_string(), "peak".to_string()];
    let expected = serde_json::json!([
        {KEY:-1,"peak":0_u64},
        {KEY:0,"peak":signed_max},
        {KEY:1,"peak":u64::MAX}
    ]);

    for format in [
        Format::Vortex,
        Format::Parquet,
        Format::ArrowIpc,
        Format::Json,
        Format::Jsonl,
        Format::Csv,
    ] {
        let output = fixture
            .0
            .join(format!("uint64-boundary.{}", format.as_str()));
        let prepared = prepare_aggregate(
            &query,
            VortexLocalPrimitiveExecutionPolicy::single_threaded(),
        )
        .unwrap();
        let report = prepared.write(&output, format, false).unwrap();
        assert_eq!(report.rows_written, 3, "{format:?}");
        assert_eq!(prepared.snapshot().completed_executions, 1, "{format:?}");
        let rows = match format {
            Format::Json => {
                serde_json::from_slice::<serde_json::Value>(&fs::read(&output).unwrap()).unwrap()
            }
            Format::Jsonl => serde_json::Value::Array(
                fs::read_to_string(&output)
                    .unwrap()
                    .lines()
                    .map(|line| serde_json::from_str(line).unwrap())
                    .collect(),
            ),
            Format::Csv => {
                assert_eq!(
                    fs::read_to_string(&output).unwrap(),
                    format!("{KEY},peak\n-1,0\n0,{}\n1,{}\n", i64::MAX, u64::MAX),
                    "{format:?}"
                );
                continue;
            }
            _ => read_binary_values(&output, format, &columns),
        };
        assert_eq!(rows, expected, "{format:?}");
    }

    for format in [Format::Avro, Format::Orc] {
        let output = fixture
            .0
            .join(format!("uint64-boundary.{}", format.as_str()));
        let before = fs::read_dir(&fixture.0)
            .unwrap()
            .map(|entry| entry.unwrap().file_name())
            .collect::<std::collections::BTreeSet<_>>();
        let prepared = prepare_aggregate(
            &query,
            VortexLocalPrimitiveExecutionPolicy::single_threaded(),
        )
        .unwrap();
        let Err(error) = prepared.write(&output, format, false) else {
            panic!("{format:?} unexpectedly represented u64::MAX")
        };
        let message = error.to_string().to_ascii_lowercase();
        assert!(
            ["represent", "range", "overflow", "i64::max", "signed"]
                .iter()
                .any(|needle| message.contains(needle)),
            "{format:?} did not report a UInt64 representability error: {error}"
        );
        assert!(!output.exists(), "{format:?} published a failed output");
        let after = fs::read_dir(&fixture.0)
            .unwrap()
            .map(|entry| entry.unwrap().file_name())
            .collect::<std::collections::BTreeSet<_>>();
        assert_eq!(after, before, "{format:?} left a staging artifact");
        assert_eq!(prepared.snapshot().completed_executions, 0, "{format:?}");
    }
}

#[test]
fn completed_finalization_checks_limited_width_and_reserves_before_row_allocation() {
    use crate::local_primitives::completed_result::CompletedRows;
    use shardloom_exec::live_memory::LiveMemoryPool;
    let memory = LiveMemoryPool::new(1024 * 1024).unwrap();
    let fields = (0..128)
        .map(|index| (format!("field{index}"), DType::Utf8(Nullability::Nullable)))
        .collect();
    let output = CompletedRows::new(fields, &memory).unwrap();
    assert!(output.admit_group_count(Some(65536), 65536).is_err());
    assert_eq!(output.admit_group_count(Some(1), 100_000).unwrap(), 1);
    assert!(output.reserve_finalization(1, 8 * 1024 * 1024).is_err());
    // Logical values fit the output cap, but their intermediate representation
    // must obtain a pool lease before a finalizer is allowed to allocate it.
    assert!(output.reserve_finalization(100, 1).is_err());
    assert_eq!(memory.snapshot().denied_reservations, 1);
    let lease = output.reserve_finalization(1, 1).unwrap();
    assert!(memory.snapshot().reserved_bytes > 65536);
    drop(lease);
    assert_eq!(memory.snapshot().reserved_bytes, 65536);
    drop(output);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

#[test]
fn completed_scalar_and_grouped_minmax_reject_oversized_text_before_finalization() {
    use vortex::array::arrays::VarBinViewArray;
    let fixture = Fixture::new();
    let text = "x".repeat(8 * 1024 * 1024 + 1);
    let path = fixture.source(
        VarBinViewArray::from_iter_str([text.as_str()]).into_array(),
        vec![1],
    );
    for grouped in [false, true] {
        let query = mixed_request(&path, grouped);
        let prepared = prepare_aggregate(
            &query,
            VortexLocalPrimitiveExecutionPolicy::single_threaded(),
        )
        .unwrap();
        let memory = prepared.session.memory().clone();
        let Err(error) = prepared.execute_owned() else {
            panic!("oversized result admitted")
        };
        assert!(error.to_string().contains("before finalization"), "{error}");
        drop(prepared);
        assert_eq!(memory.snapshot().reserved_bytes, 0);
    }
}

#[cfg(feature = "universal-format-io")]
#[test]
#[allow(clippy::too_many_lines)] // Keep the exact wide-value acceptance matrix together.
fn result_stream_wide_ordered_output_exceeds_eight_mib_in_every_format() {
    use crate::VortexLocalPrimitiveRowExportFormat as Format;
    use std::fmt::Write as _;
    use vortex::array::arrays::VarBinViewArray;

    const SOURCE_ROWS: usize = 4_113;
    const OFFSET: usize = 7;
    const OUTPUT_ROWS: usize = 4_097;
    let fixture = Fixture::new();
    let path = fixture.0.join("wide-sort-source.vortex");
    let runtime = super::super::super::local_vortex_runtime(
        VortexLocalPrimitiveExecutionPolicy::single_threaded(),
    );
    let session = VortexSession::default().with_handle(runtime.handle());
    let keys = PrimitiveArray::new(
        (0..SOURCE_ROWS)
            .map(|row| i64::try_from(row).unwrap())
            .collect::<Vec<_>>(),
        Validity::NonNullable,
    )
    .into_array();
    let payload = "data\"line,\n雪".repeat(256);
    let notes = VarBinViewArray::from_iter_nullable_str(
        (0..SOURCE_ROWS).map(|row| (row % 7 != 0).then_some(payload.as_str())),
    )
    .into_array();
    let array = StructArray::new(
        [KEY, "delivery_note"].into(),
        vec![keys, notes],
        SOURCE_ROWS,
        Validity::NonNullable,
    )
    .into_array();
    let mut file = fs::File::create(&path).unwrap();
    let mut writer = session
        .write_options()
        .with_strategy(
            super::super::super::native_flat_layout::SequentialNativeFlatLayout::strategy(
                SOURCE_ROWS.div_ceil(512),
            ),
        )
        .with_file_statistics(Vec::new())
        .blocking(&runtime)
        .writer(&mut file, array.dtype().clone());
    for start in (0..SOURCE_ROWS).step_by(512) {
        writer
            .push(array.slice(start..SOURCE_ROWS.min(start + 512)).unwrap())
            .unwrap();
    }
    assert_eq!(
        writer.finish().unwrap().row_count(),
        u64::try_from(SOURCE_ROWS).unwrap()
    );

    let query = VortexQueryPrimitiveRequest::sort_rows(
        DatasetUri::new(path.display().to_string()).unwrap(),
        super::super::super::ProjectionRequest::All,
        None,
        crate::VortexSortRowsRequest::new(vec![VortexAggregateOrderExpr::new(KEY, true)])
            .with_offset(OFFSET),
        OUTPUT_ROWS,
    );
    let columns = vec![KEY.to_string(), "delivery_note".to_string()];
    let mut expected_rows = Vec::with_capacity(OUTPUT_ROWS);
    let mut expected_csv = format!("{KEY},delivery_note\n");
    let escaped_payload = format!("\"{}\"", payload.replace('"', "\"\""));
    for index in 0..OUTPUT_ROWS {
        let ordinal = SOURCE_ROWS - 1 - OFFSET - index;
        let note = if ordinal.is_multiple_of(7) {
            serde_json::Value::Null
        } else {
            serde_json::json!(payload)
        };
        let mut object = serde_json::Map::with_capacity(2);
        object.insert(KEY.into(), serde_json::json!(ordinal));
        object.insert("delivery_note".into(), note);
        expected_rows.push(serde_json::Value::Object(object));
        if ordinal.is_multiple_of(7) {
            writeln!(expected_csv, "{ordinal},").unwrap();
        } else {
            writeln!(expected_csv, "{ordinal},{escaped_payload}").unwrap();
        }
    }
    let expected = serde_json::Value::Array(expected_rows);

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
        let output = fixture.0.join(format!("wide-sort.{}", format.as_str()));
        let report = runtime::execute_vortex_local_primitive_row_export_with_policy(
            &query,
            &output,
            format,
            false,
            VortexLocalPrimitiveExecutionPolicy::single_threaded(),
        )
        .unwrap();
        assert_eq!(
            report.rows_written,
            u64::try_from(OUTPUT_ROWS).unwrap(),
            "{format:?}"
        );
        assert!(!report.evidence.side_effects.fallback_attempted);
        assert!(!report.evidence.side_effects.fallback_execution_allowed);
        let sink = report.evidence.native_array_sink.as_ref().unwrap();
        assert!(
            sink.native_array_logical_bytes > 8 * 1024 * 1024,
            "{format:?}"
        );
        let batch_bound = if format == Format::Vortex { 8192 } else { 2048 };
        assert!(sink.scan_row_bound <= batch_bound, "{format:?}");
        match format {
            Format::Json => assert_eq!(
                serde_json::from_slice::<serde_json::Value>(&fs::read(&output).unwrap()).unwrap(),
                expected,
            ),
            Format::Jsonl => {
                let rows = fs::read_to_string(&output)
                    .unwrap()
                    .lines()
                    .map(|line| serde_json::from_str(line).unwrap())
                    .collect::<Vec<serde_json::Value>>();
                assert_eq!(serde_json::Value::Array(rows), expected);
            }
            Format::Csv => assert_eq!(fs::read_to_string(&output).unwrap(), expected_csv),
            _ => assert_eq!(read_binary_values(&output, format, &columns), expected),
        }
        fs::remove_file(&output).unwrap();
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
