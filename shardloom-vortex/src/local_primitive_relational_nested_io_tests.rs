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
            assert_eq!(&prepared.output_dtype(), dtype);
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
            assert_eq!(
                reopen(&path, format, &prepared.output_dtype()),
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
