//! Completed shared primitive writers emit one value per report key.

use super::{complete_result, run_facade, unique_vortex_binding_dir};
use serde_json::{Value, json};
use std::{
    collections::BTreeSet,
    fs,
    path::{Path, PathBuf},
};

struct Fixture(PathBuf);

impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

fn unique_fields(envelope: &Value) {
    assert_eq!(envelope["status"], "success", "{envelope}");
    let mut keys = BTreeSet::new();
    for entry in envelope["fields"].as_array().unwrap() {
        let key = entry["key"].as_str().unwrap();
        assert!(keys.insert(key), "duplicate completed report field: {key}");
    }
}

fn assert_native_output(path: &Path) {
    let read = format!("SELECT * FROM '{}'", path.display());
    let (ok, stdout) = run_facade(&[
        "run",
        "sql",
        "--sql",
        &read,
        "--request",
        "collect",
        "--bounded",
        "true",
        "--memory-gb",
        "1",
        "--max-parallelism",
        "2",
        "--format",
        "json",
    ]);
    assert!(ok, "{stdout}");
    let reopened: Value = serde_json::from_str(&stdout).unwrap();
    unique_fields(&reopened);
    assert_eq!(
        complete_result::rows(&reopened),
        vec![json!({"row_count":3,"metric_sum":6.0})]
    );
}

#[test]
fn nested_primitive_writes_emit_unique_complete_reports_for_single_and_fanout_sinks() {
    let root = Fixture(unique_vortex_binding_dir("unique-writer-reports"));
    fs::create_dir(&root.0).unwrap();
    let source = root.0.join("source.csv");
    fs::write(
        &source,
        "id,group_key,category\n1,1,alpha\n2,2,beta\n3,3,港\n",
    )
    .unwrap();
    let bindings = json!({source.to_str().unwrap(): {
        "input_format": "csv", "source_schema": "id:int64,group_key:int64,category:utf8"
    }})
    .to_string();
    let sql = format!(
        "SELECT COUNT(*) AS row_count,SUM(group_key) AS metric_sum FROM (SELECT id,group_key,category FROM '{}') AS projected",
        source.display()
    );
    for fanout in [false, true] {
        for format in [
            "vortex",
            "parquet",
            "arrow_ipc",
            "avro",
            "orc",
            "json",
            "jsonl",
            "csv",
        ] {
            let target = root.0.join(format!("output-{fanout}.{format}"));
            let secondary = root.0.join(format!("fanout-{format}.jsonl"));
            let request = format!("write_{format}");
            let extra = format!("jsonl={}", secondary.display());
            let mut args = vec![
                "run",
                "sql",
                "--sql",
                &sql,
                "--source-bindings",
                &bindings,
                "--request",
                &request,
                "--output",
                target.to_str().unwrap(),
                "--bounded",
                "true",
                "--memory-gb",
                "1",
                "--max-parallelism",
                "2",
                "--format",
                "json",
            ];
            if fanout {
                args.extend(["--fanout-output", &extra]);
            }
            let (ok, stdout) = run_facade(&args);
            assert!(ok, "{format}/{fanout}: {stdout}");
            let envelope: Value = serde_json::from_str(&stdout).unwrap();
            unique_fields(&envelope);
            assert_eq!(
                complete_result::field_value(&envelope, "native_vortex_result_export_rows_written"),
                "1"
            );
            assert_eq!(
                complete_result::field_value(
                    &envelope,
                    "local_primitive_resource_memory_budget_bytes"
                ),
                (1_u64 << 30).to_string()
            );
            assert_eq!(
                complete_result::field_value(&envelope, "fallback_attempted"),
                "false"
            );
            assert!(target.is_file());
            if fanout {
                let rows = fs::read_to_string(&secondary)
                    .unwrap()
                    .lines()
                    .map(|line| serde_json::from_str::<Value>(line).unwrap())
                    .collect::<Vec<_>>();
                assert_eq!(rows, vec![json!({"row_count":3,"metric_sum":6.0})]);
            }
            if format == "vortex" {
                assert_native_output(&target);
            }
        }
    }
}
