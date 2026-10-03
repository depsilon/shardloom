use super::*;
use arrow_array::{
    BinaryArray, Date32Array, Decimal128Array, Int64Array, RecordBatch, TimestampMicrosecondArray,
};
use arrow_schema::{DataType, Field, Schema, TimeUnit};
use serde_json::{Value, json};
use std::{fs, path::PathBuf, sync::Arc};

struct Fixture {
    root: PathBuf,
    vortex: PathBuf,
    empty_vortex: PathBuf,
}

impl Fixture {
    fn new() -> Self {
        let root = unique_vortex_binding_dir("public-typed-sql");
        fs::create_dir_all(&root).unwrap();
        let ipc = root.join("typed.arrow");
        let vortex = root.join("typed.vortex");
        let empty_ipc = root.join("typed-empty.arrow");
        let empty_vortex = root.join("typed-empty.vortex");
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("payload", DataType::Binary, true),
            Field::new("amount", DataType::Decimal128(38, 6), true),
            Field::new("day", DataType::Date32, true),
            Field::new(
                "occurred",
                DataType::Timestamp(TimeUnit::Microsecond, None),
                true,
            ),
        ]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int64Array::from(vec![1, 2, 3, 4])),
                Arc::new(BinaryArray::from(vec![
                    Some(&[0xff, 0x00][..]),
                    Some(&[][..]),
                    None,
                    Some(&[0x00][..]),
                ])),
                Arc::new(
                    Decimal128Array::from(vec![Some(10_i128), Some(0), None, Some(-10)])
                        .with_precision_and_scale(38, 6)
                        .unwrap(),
                ),
                Arc::new(Date32Array::from(vec![Some(-1), Some(0), None, Some(1)])),
                Arc::new(TimestampMicrosecondArray::from(vec![
                    Some(-1),
                    Some(0),
                    None,
                    Some(1),
                ])),
            ],
        )
        .unwrap();
        write_ipc(&ipc, &schema, &batch);
        prepare_vortex(&ipc, &vortex);

        let empty_batch = RecordBatch::new_empty(Arc::clone(&schema));
        write_ipc(&empty_ipc, &schema, &empty_batch);
        prepare_vortex(&empty_ipc, &empty_vortex);
        Self {
            root,
            vortex,
            empty_vortex,
        }
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.root);
    }
}

fn write_ipc(path: &std::path::Path, schema: &Arc<Schema>, batch: &RecordBatch) {
    let mut writer =
        arrow_ipc::writer::FileWriter::try_new(fs::File::create(path).unwrap(), schema).unwrap();
    writer.write(batch).unwrap();
    writer.finish().unwrap();
}

fn prepare_vortex(input: &std::path::Path, output: &std::path::Path) {
    let (success, stdout) = run_facade(&[
        "prepare",
        "dataframe",
        "--input",
        input.to_str().unwrap(),
        "--input-format",
        "arrow_ipc",
        "--output",
        output.to_str().unwrap(),
        "--memory-gb",
        "1",
        "--max-parallelism",
        "2",
        "--format",
        "json",
    ]);
    assert!(success, "{stdout}");
    assert!(stdout.contains("\"status\":\"success\""), "{stdout}");
    assert!(output.exists(), "{stdout}");
}

fn sql(path: &std::path::Path, statement: &str) -> String {
    format!("{statement} FROM '{}'", path.display())
}

fn collect(path: &std::path::Path, statement: &str) -> (bool, String) {
    run_facade(&[
        "run",
        "sql",
        "--input",
        path.to_str().unwrap(),
        "--input-format",
        "vortex",
        "--sql",
        statement,
        "--request",
        "collect",
        "--bounded",
        "true",
        "--execution-policy",
        "native_vortex",
        "--materialization-policy",
        "bounded",
        "--memory-gb",
        "1",
        "--max-parallelism",
        "2",
        "--format",
        "json",
    ])
}

fn envelope_field<'a>(envelope: &'a Value, key: &str) -> Option<&'a str> {
    envelope
        .get("fields")?
        .as_array()?
        .iter()
        .find(|field| field.get("key").and_then(Value::as_str) == Some(key))?
        .get("value")?
        .as_str()
}

fn assert_complete_result(stdout: &str, expected: &Value) {
    let envelope: Value = serde_json::from_str(stdout).unwrap();
    assert_eq!(envelope["status"], "success", "{stdout}");
    let jsonl = envelope_field(&envelope, "result_jsonl")
        .unwrap_or_else(|| panic!("complete result_jsonl missing: {stdout}"));
    let rows = jsonl
        .lines()
        .map(|line| serde_json::from_str::<Value>(line).unwrap())
        .collect::<Vec<_>>();
    assert_eq!(&Value::Array(rows), expected, "{stdout}");
    assert_eq!(
        envelope_field(&envelope, "result_payload_complete"),
        Some("true")
    );
    assert_eq!(
        envelope_field(&envelope, "fallback_attempted"),
        Some("false")
    );
    assert_eq!(
        envelope_field(&envelope, "external_engine_invoked"),
        Some("false")
    );
    if let Some(opens) = envelope_field(&envelope, "resident_source_opens") {
        assert_eq!(opens, "1", "{stdout}");
    }
}

fn key_value(key: &str, row: usize) -> Value {
    match key {
        "payload" => payload_value(row),
        "amount" => amount_value(row),
        "day" | "occurred" => temporal_value(row),
        _ => unreachable!("supported typed key"),
    }
}

fn payload_value(index: usize) -> Value {
    match index {
        0 => json!("ff00"),
        1 => json!(""),
        2 => Value::Null,
        3 => json!("00"),
        _ => unreachable!("fixture row"),
    }
}

fn amount_value(index: usize) -> Value {
    match index {
        0 => json!("decimal128(38,6):10"),
        1 => json!("decimal128(38,6):0"),
        2 => Value::Null,
        3 => json!("decimal128(38,6):-10"),
        _ => unreachable!("fixture row"),
    }
}

fn temporal_value(index: usize) -> Value {
    match index {
        0 => json!(-1),
        1 => json!(0),
        2 => Value::Null,
        3 => json!(1),
        _ => unreachable!("fixture row"),
    }
}

fn row(index: usize) -> Value {
    json!({
        "id": index + 1,
        "payload": payload_value(index),
        "amount": amount_value(index),
        "day": temporal_value(index),
        "occurred": temporal_value(index)
    })
}

fn ordered_indices(key: &str) -> &'static [usize] {
    match key {
        "payload" => &[0, 3, 1, 2],
        "amount" => &[0, 1, 3, 2],
        "day" | "occurred" => &[3, 1, 0, 2],
        _ => unreachable!("supported typed key"),
    }
}

fn typed_sql_cases(path: &std::path::Path) -> Vec<(String, Value)> {
    let mut cases = Vec::new();
    for key in ["payload", "amount", "day", "occurred"] {
        let source = format!("'{}'", path.display());
        cases.push((
            format!("SELECT {key},COUNT(*) AS n FROM {source} GROUP BY {key}"),
            Value::Array(
                (0..4)
                    .map(|index| {
                        let mut row = json!({"n": 1});
                        row[key] = key_value(key, index);
                        row
                    })
                    .collect(),
            ),
        ));
        cases.push((
            format!("SELECT * FROM {source} ORDER BY {key} DESC NULLS LAST LIMIT 4"),
            Value::Array(
                ordered_indices(key)
                    .iter()
                    .map(|&index| row(index))
                    .collect(),
            ),
        ));
        cases.push((
            format!("SELECT * FROM {source} WHERE {key} IS NULL LIMIT 4"),
            json!([row(2)]),
        ));
        cases.push((
            format!("SELECT * FROM {source} ORDER BY {key} DESC NULLS LAST LIMIT 2 OFFSET 1"),
            Value::Array(
                ordered_indices(key)[1..3]
                    .iter()
                    .map(|&index| row(index))
                    .collect(),
            ),
        ));
        cases.push((
            format!("SELECT * FROM {source} ORDER BY {key} LIMIT 0"),
            json!([]),
        ));
    }
    cases
}

#[cfg(all(unix, feature = "vortex-local-primitives", feature = "vortex-write"))]
#[test]
fn public_sql_typed_keys_execute_complete_native_results() {
    let fixture = Fixture::new();
    for (statement, expected) in typed_sql_cases(&fixture.vortex) {
        let (success, stdout) = collect(&fixture.vortex, &statement);
        assert!(success, "{statement}: {stdout}");
        assert_complete_result(&stdout, &expected);
    }
    let empty_derived = format!(
        "SELECT * FROM (SELECT * FROM '{}' LIMIT 0) AS empty WHERE amount IS NULL",
        fixture.empty_vortex.display()
    );
    let (success, stdout) = collect(&fixture.empty_vortex, &empty_derived);
    assert!(success, "{empty_derived}: {stdout}");
    assert_complete_result(&stdout, &json!([]));
}

#[cfg(all(unix, feature = "vortex-local-primitives", feature = "vortex-write"))]
#[test]
fn public_sql_route_inspects_typed_queries_without_touching_missing_sources() {
    let root = unique_vortex_binding_dir("public-typed-sql-missing");
    let missing = root.join("does-not-exist.vortex");
    let mut statements = typed_sql_cases(&missing);
    statements.push((
        format!(
            "SELECT * FROM (SELECT * FROM '{}' LIMIT 0) AS empty WHERE amount IS NULL",
            missing.display()
        ),
        json!([]),
    ));
    for (statement, _) in statements {
        assert!(!missing.exists());
        let (success, stdout) = run_facade(&[
            "route",
            "sql",
            "--input",
            missing.to_str().unwrap(),
            "--input-format",
            "vortex",
            "--sql",
            &statement,
            "--request",
            "collect",
            "--bounded",
            "true",
            "--execution-policy",
            "native_vortex",
            "--format",
            "json",
        ]);
        assert!(success, "{statement}: {stdout}");
        let envelope: Value = serde_json::from_str(&stdout).unwrap();
        assert_eq!(envelope["status"], "success", "{statement}: {stdout}");
        assert_eq!(
            envelope_field(&envelope, "source_io_performed"),
            Some("false")
        );
        assert_eq!(
            envelope_field(&envelope, "output_io_performed"),
            Some("false")
        );
        assert_eq!(
            envelope_field(&envelope, "fallback_attempted"),
            Some("false")
        );
        assert_eq!(
            envelope_field(&envelope, "external_engine_invoked"),
            Some("false")
        );
        assert!(!missing.exists(), "route created source for {statement}");
    }
}

#[cfg(all(unix, feature = "vortex-local-primitives", feature = "vortex-write"))]
#[test]
fn public_sql_typed_cast_and_arithmetic_on_empty_source_remain_errors() {
    let fixture = Fixture::new();
    for statement in [
        sql(
            &fixture.empty_vortex,
            "SELECT CAST(amount AS VARCHAR) AS converted",
        ),
        sql(&fixture.empty_vortex, "SELECT amount + 1 AS adjusted"),
    ] {
        let (success, stdout) = collect(&fixture.empty_vortex, &statement);
        assert!(!success, "{statement} unexpectedly succeeded: {stdout}");
        let envelope: Value = serde_json::from_str(&stdout).unwrap();
        assert_ne!(envelope["status"], "success", "{statement}: {stdout}");
        assert_eq!(envelope["fallback"]["attempted"], false, "{stdout}");
        assert_eq!(envelope["fallback"]["allowed"], false, "{stdout}");
        assert!(
            envelope["certificates"]
                .as_array()
                .is_none_or(Vec::is_empty),
            "{stdout}"
        );
        assert!(
            envelope["artifacts"].as_array().is_none_or(Vec::is_empty),
            "{stdout}"
        );
    }
}
