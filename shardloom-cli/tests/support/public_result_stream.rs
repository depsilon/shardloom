use super::{field, run_facade, unique_vortex_binding_dir};
use arrow_array::{Int64Array, RecordBatch, UInt64Array};
use arrow_schema::{DataType, Field, Schema};
use std::{fs, path::PathBuf, sync::Arc};

const ROWS: usize = 70_017;
const LIMIT: usize = 70_004;
const FIRST_KEY: i64 = 13;

struct Fixture {
    root: PathBuf,
    source: PathBuf,
}

impl Fixture {
    fn new() -> Self {
        let root = unique_vortex_binding_dir("public-result-stream");
        fs::create_dir(&root).unwrap();
        let ipc = root.join("source.arrow");
        let source = root.join("source.vortex");
        let schema = Arc::new(Schema::new(vec![
            Field::new("delivery_zone", DataType::Int64, false),
            Field::new("package_identifier", DataType::UInt64, false),
        ]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int64Array::from_iter_values(
                    0..i64::try_from(ROWS).unwrap(),
                )),
                Arc::new(UInt64Array::from_iter_values(std::iter::repeat_n(1, ROWS))),
            ],
        )
        .unwrap();
        let mut writer =
            arrow_ipc::writer::FileWriter::try_new(fs::File::create(&ipc).unwrap(), &schema)
                .unwrap();
        writer.write(&batch).unwrap();
        writer.finish().unwrap();
        drop(writer);
        let columnar = shardloom_vortex::read_flat_arrow_ipc_columnar_source(&ipc, ROWS).unwrap();
        shardloom_vortex::write_flat_columnar_vortex_prepared_state(
            shardloom_vortex::VortexPreparedStateColumnarWriteRequest::new(
                &source,
                columnar,
                shardloom_core::ExecutionResources::from_gib(
                    4,
                    8,
                    shardloom_core::ExecutionResourceOrigin::ExecutionCall,
                )
                .expect("explicit fixture allocation"),
            ),
        )
        .unwrap();
        Self { root, source }
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.root);
    }
}

fn run_sink(fixture: &Fixture, surface: &str, request: &str, operation: &str) -> PathBuf {
    let extension = request.strip_prefix("write_").unwrap();
    let output = fixture
        .root
        .join(format!("{operation}-{surface}.{extension}"));
    let sql = match operation {
        "aggregate" => format!(
            "SELECT delivery_zone, COUNT(*) AS n FROM '{}' WHERE delivery_zone >= {FIRST_KEY} GROUP BY delivery_zone ORDER BY delivery_zone ASC LIMIT {LIMIT}",
            fixture.source.display()
        ),
        "sort" => format!(
            "SELECT * FROM '{}' WHERE delivery_zone >= {FIRST_KEY} ORDER BY delivery_zone DESC LIMIT {LIMIT}",
            fixture.source.display()
        ),
        _ => unreachable!("known operation"),
    };
    let aggregate = r#"{"group_by":["delivery_zone"],"measures":[{"function":"count","alias":"n"}],"order_by":[{"column":"delivery_zone","descending":false}]}"#;
    let sort = r#"{"order_by":[{"column":"delivery_zone","descending":true}]}"#;
    let output_text = output.to_str().unwrap();
    let source_text = fixture.source.to_str().unwrap();
    let mut args = vec![
        "run",
        surface,
        "--input",
        source_text,
        "--input-format",
        "vortex",
        "--request",
        request,
        "--output",
        output_text,
        "--bounded",
        "true",
        "--memory-gb",
        "4",
        "--max-parallelism",
        "1",
        "--format",
        "json",
    ];
    if surface == "sql" {
        args.extend(["--sql", &sql]);
    } else {
        args.extend([
            "--vortex-predicate",
            "gte:delivery_zone:13",
            "--vortex-source-order-limit",
            "70004",
            "--vortex-primitive",
            if operation == "aggregate" {
                "aggregate"
            } else {
                "sort_rows"
            },
            if operation == "aggregate" {
                "--vortex-aggregate"
            } else {
                "--vortex-sort-rows"
            },
            if operation == "aggregate" {
                aggregate
            } else {
                sort
            },
        ]);
    }
    let (ok, stdout) = run_facade(&args);
    assert!(ok, "{surface} {operation} {request}: {stdout}");
    let envelope: serde_json::Value = serde_json::from_str(&stdout).unwrap();
    assert_eq!(envelope["status"], "success", "{stdout}");
    for (key, value) in [
        ("public_workflow_fallback_attempted", "false"),
        ("public_workflow_external_engine_invoked", "false"),
    ] {
        assert!(stdout.contains(&field(key, value)), "{key}: {stdout}");
    }
    output
}

fn read_jsonl(path: &std::path::Path) -> Vec<serde_json::Value> {
    fs::read_to_string(path)
        .unwrap()
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect()
}

fn reopen_vortex_as_jsonl(
    fixture: &Fixture,
    vortex: &std::path::Path,
    name: &str,
) -> Vec<serde_json::Value> {
    let decoded = fixture.root.join(format!("{name}-reopened.jsonl"));
    let request = shardloom_vortex::VortexQueryPrimitiveRequest::project(
        shardloom_core::DatasetUri::new(vortex.display().to_string()).unwrap(),
        shardloom_plan::ProjectionRequest::All,
    );
    let report = shardloom_vortex::execute_vortex_local_primitive_row_export_with_policy(
        &request,
        &decoded,
        shardloom_vortex::VortexLocalPrimitiveRowExportFormat::Jsonl,
        false,
        shardloom_vortex::VortexLocalPrimitiveExecutionPolicy::new_with_memory_gb(1, 4)
            .expect("explicit fixture allocation"),
    )
    .unwrap();
    assert_eq!(
        report.rows_written,
        u64::try_from(LIMIT).unwrap(),
        "{report:?}"
    );
    read_jsonl(&decoded)
}

#[test]
#[allow(clippy::too_many_lines)]
fn public_result_stream_sql_dataframe_write_reopen_complete_values() {
    let fixture = Fixture::new();
    for operation in ["aggregate", "sort"] {
        let expected = if operation == "aggregate" {
            (FIRST_KEY..i64::try_from(ROWS).unwrap())
                .map(|delivery_zone| serde_json::json!({"delivery_zone":delivery_zone,"n":1}))
                .collect::<Vec<_>>()
        } else {
            (FIRST_KEY..i64::try_from(ROWS).unwrap())
                .rev()
                .map(|delivery_zone| serde_json::json!({"delivery_zone":delivery_zone,"package_identifier":1}))
                .collect::<Vec<_>>()
        };
        assert_eq!(expected.len(), LIMIT);
        for surface in ["sql", "dataframe"] {
            for request in ["write_vortex", "write_jsonl"] {
                let output = run_sink(&fixture, surface, request, operation);
                let actual = if request == "write_vortex" {
                    reopen_vortex_as_jsonl(&fixture, &output, &format!("{operation}-{surface}"))
                } else {
                    read_jsonl(&output)
                };
                assert_eq!(actual.len(), LIMIT, "{surface} {operation} {request}");
                assert_eq!(actual, expected, "{surface} {operation} {request}");
            }
        }
    }
}
