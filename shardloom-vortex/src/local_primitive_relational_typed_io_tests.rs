use super::*;
use crate::local_primitives::VortexLocalPrimitiveRowExportFormat as Format;

fn reopen(path: &std::path::Path, format: Format, dtype: &DType) -> Vec<Value> {
    if format == Format::Vortex {
        let plan = VortexRelationalPlan::Scan(VortexRelationalScan {
            source_uri: DatasetUri::new(path.display().to_string()).unwrap(),
            projection: shardloom_plan::ProjectionRequest::All,
            predicate: None,
        });
        assert_eq!(
            prepare_relational(&plan, policy())
                .unwrap()
                .output_dtype()
                .as_ref(),
            Some(dtype)
        );
        return collect(&plan);
    }
    if format == Format::Json {
        return serde_json::from_slice(&fs::read(path).unwrap()).unwrap();
    }
    if format == Format::Jsonl {
        return fs::read_to_string(path)
            .unwrap()
            .lines()
            .map(|line| serde_json::from_str(line).unwrap())
            .collect();
    }
    let table = match format {
        Format::Parquet => crate::read_flat_parquet_source(path, 100),
        Format::ArrowIpc => crate::read_flat_arrow_ipc_source(path, 100),
        Format::Avro => crate::read_flat_avro_source(path, 100),
        Format::Orc => crate::read_flat_orc_source(path, 100),
        _ => panic!("CSV has its own exact encoding assertion"),
    }
    .unwrap();
    assert_eq!(
        table.header,
        dtype
            .as_struct_fields_opt()
            .unwrap()
            .names()
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>()
    );
    table
        .rows
        .into_iter()
        .map(|row| {
            Value::Object(
                row.into_iter()
                    .map(|(name, value)| {
                        (
                            name,
                            crate::local_primitives::scalar_value_to_json_value(&value).unwrap(),
                        )
                    })
                    .collect(),
            )
        })
        .collect()
}

#[test]
fn native_typed_payload_writers_preserve_complete_values_types_and_empty_outputs() {
    let fixture = fixture();
    for count in [4, 0] {
        let plan = VortexRelationalPlan::Limit(Box::new(VortexRelationalLimit {
            input: sort(fixture.scan(), "id"),
            offset: 0,
            count,
        }));
        let prepared = prepare_relational(&plan, policy()).unwrap();
        let mut expected = expected();
        expected.reverse();
        expected.truncate(count);
        for format in [
            Format::Vortex,
            Format::Parquet,
            Format::ArrowIpc,
            Format::Avro,
            Format::Json,
            Format::Jsonl,
        ] {
            let path = fixture.0.join(format!("typed-{count}.{}", format.as_str()));
            let report = prepared
                .write(&path, format, false)
                .unwrap_or_else(|error| panic!("{format:?} {count}: {error}"));
            assert_eq!(report.output.rows_written, count as u64);
            assert!(report.execution.native_io_certificate.is_certified());
            assert_eq!(
                reopen(&path, format, &prepared.output_dtype().unwrap()),
                expected,
                "{format:?} {count}"
            );
            if matches!(format, Format::Json | Format::Jsonl) {
                let copies: u64 = expected
                    .iter()
                    .map(|row| {
                        ["payload", "amount"]
                            .iter()
                            .filter_map(|name| row[name].as_str())
                            .map(|text| text.len() as u64)
                            .sum::<u64>()
                    })
                    .sum();
                assert_eq!(
                    report
                        .output
                        .evidence
                        .native_array_sink
                        .as_ref()
                        .unwrap()
                        .adapter_payload_bytes_copied,
                    copies
                );
            }
            if format == Format::ArrowIpc {
                let reader =
                    arrow_ipc::reader::FileReader::try_new(fs::File::open(&path).unwrap(), None)
                        .unwrap();
                let schema = reader.schema();
                assert_eq!(schema.field(1).data_type(), &arrow_schema::DataType::Binary);
                assert_eq!(
                    schema.field(2).data_type(),
                    &arrow_schema::DataType::Decimal128(38, 6)
                );
                assert_eq!(schema.field(3).data_type(), &arrow_schema::DataType::Date32);
                assert_eq!(
                    schema.field(4).data_type(),
                    &arrow_schema::DataType::Timestamp(arrow_schema::TimeUnit::Microsecond, None)
                );
            }
        }
        let path = fixture.0.join(format!("typed-denied-{count}.orc"));
        let before = prepared.snapshot().completed_executions;
        let error = prepared.write(&path, Format::Orc, false).err().unwrap();
        assert!(
            error
                .to_string()
                .contains("ORC does not admit decimal or temporal"),
            "{error}"
        );
        assert_eq!(prepared.snapshot().completed_executions, before);
        assert!(!path.exists());
        let path = fixture.0.join(format!("typed-{count}.csv"));
        prepared.write(&path, Format::Csv, false).unwrap();
        let csv = if count == 0 {
            "id,payload,amount,day,instant\n".to_owned()
        } else {
            format!(
                "id,payload,amount,day,instant\n4,\"\"\"c3a9\"\"\",\"\"\"decimal128(38,6):0\"\"\",0,0\n3,,,,\n2,\"\"\"\"\"\",\"\"\"decimal128(38,6):-{DECIMAL_EDGE}\"\"\",20000,1700000000123456\n1,\"\"\"00ff10\"\"\",\"\"\"decimal128(38,6):1234567\"\"\",-1,-1\n"
            )
        };
        assert_eq!(fs::read_to_string(path).unwrap(), csv);
    }
}

#[test]
fn native_typed_payload_binary_has_all_eight_writer_paths() {
    let mut arrays = payloads();
    let fixture = Fixture::new(
        StructArray::new(
            FieldNames::from(["payload"]),
            vec![arrays.remove(0)],
            4,
            Validity::NonNullable,
        )
        .into_array(),
        2,
    );
    let prepared = prepare_relational(&fixture.scan(), policy()).unwrap();
    let expected = vec![
        json!({"payload":"00ff10"}),
        json!({"payload":""}),
        json!({"payload":null}),
        json!({"payload":"c3a9"}),
    ];
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
        let path = fixture.0.join(format!("binary.{}", format.as_str()));
        prepared
            .write(&path, format, false)
            .unwrap_or_else(|error| panic!("{format:?}: {error}"));
        if format == Format::Csv {
            assert_eq!(
                fs::read_to_string(path).unwrap(),
                "payload\n\"\"\"00ff10\"\"\"\n\"\"\"\"\"\"\n\"\"\n\"\"\"c3a9\"\"\"\n"
            );
        } else {
            assert_eq!(
                reopen(&path, format, &prepared.output_dtype().unwrap()),
                expected,
                "{format:?}"
            );
        }
    }
}

#[test]
fn native_typed_payload_writers_preserve_full_temporal_integer_domains() {
    let fixture = Fixture::new(
        StructArray::new(
            FieldNames::from(["id", "day", "instant"]),
            vec![
                PrimitiveArray::from_iter([1u32, 2, 3, 4]).into_array(),
                ExtensionArray::new(
                    Date::new(TimeUnit::Days, Nullability::NonNullable).erased(),
                    PrimitiveArray::from_iter([i32::MIN, -1, 0, i32::MAX]).into_array(),
                )
                .into_array(),
                ExtensionArray::new(
                    Timestamp::new(TimeUnit::Microseconds, Nullability::NonNullable).erased(),
                    PrimitiveArray::from_iter([i64::MIN, -1, 0, i64::MAX]).into_array(),
                )
                .into_array(),
            ],
            4,
            Validity::NonNullable,
        )
        .into_array(),
        2,
    );
    let prepared = prepare_relational(&sort(fixture.scan(), "id"), policy()).unwrap();
    let expected = vec![
        json!({"id":4,"day":i32::MAX,"instant":i64::MAX}),
        json!({"id":3,"day":0,"instant":0}),
        json!({"id":2,"day":-1,"instant":-1}),
        json!({"id":1,"day":i32::MIN,"instant":i64::MIN}),
    ];
    for format in [
        Format::Vortex,
        Format::Parquet,
        Format::ArrowIpc,
        Format::Avro,
        Format::Json,
        Format::Jsonl,
        Format::Csv,
    ] {
        let path = fixture.0.join(format!("temporal-edge.{}", format.as_str()));
        prepared
            .write(&path, format, false)
            .unwrap_or_else(|error| panic!("{format:?}: {error}"));
        if format == Format::Csv {
            assert_eq!(
                fs::read_to_string(path).unwrap(),
                format!(
                    "id,day,instant\n4,{},{}\n3,0,0\n2,-1,-1\n1,{},{}\n",
                    i32::MAX,
                    i64::MAX,
                    i32::MIN,
                    i64::MIN
                )
            );
        } else {
            assert_eq!(
                reopen(&path, format, &prepared.output_dtype().unwrap()),
                expected,
                "{format:?}"
            );
        }
    }
}
