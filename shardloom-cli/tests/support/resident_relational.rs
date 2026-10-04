use super::*;

impl Worker {
    fn relational(&mut self, statement: &str, extra: &[&str]) -> Value {
        let mut args = vec![
            "run",
            "sql",
            "--sql",
            statement,
            "--request",
            "collect",
            "--bounded",
            "true",
            "--materialization-policy",
            "bounded",
            "--memory-gb",
            "1",
            "--max-parallelism",
            "2",
        ];
        args.extend_from_slice(extra);
        self.request(&args)
    }
}

fn completed(result: &Value, expected: &Value, sources: usize, executions: usize) {
    assert_eq!(result["status"], "success", "{result}");
    let rows = field(result, "result_jsonl")
        .lines()
        .map(|row| serde_json::from_str::<Value>(row).unwrap())
        .collect::<Vec<_>>();
    assert_eq!(json!(rows), *expected);
    for (key, value) in [
        (
            "public_workflow_route_id",
            "native_vortex_relational_collect",
        ),
        ("resident_relational_handle_retained", "true"),
        ("result_payload_complete", "true"),
        ("fallback_attempted", "false"),
        ("external_engine_invoked", "false"),
        ("local_primitive_native_io_certificate_emitted", "true"),
        (
            "public_workflow_native_vortex_plan_payload_kind",
            "native_relational_plan",
        ),
        (
            "public_workflow_native_vortex_plan_contract_status",
            "admitted",
        ),
        (
            "public_workflow_native_vortex_required_feature_gate",
            if field(result, "relational_normalized_source_count") == "0" {
                "vortex-local-primitives"
            } else {
                "vortex-local-primitives,vortex-write,universal-format-io"
            },
        ),
    ] {
        assert_eq!(field(result, key), value, "{key}: {result}");
    }
    assert_eq!(field(result, "resident_source_opens"), sources.to_string());
    assert_eq!(
        field(result, "public_workflow_native_vortex_plan_source_count"),
        sources.to_string()
    );
    assert_eq!(
        field(result, "resident_completed_executions"),
        executions.to_string()
    );
    assert_eq!(
        field(result, "resident_relational_lowering_reused"),
        (executions > 1).to_string()
    );
    assert_eq!(
        field(result, "output_row_count"),
        expected.as_array().unwrap().len().to_string()
    );
}

#[test]
fn worker_relational_families_reuse_bound_readers_and_deliver_complete_results() {
    let source = fixture().display().to_string();
    let cases = [
        (
            format!(
                "SELECT l.value, r.value FROM '{source}' AS l LEFT JOIN '{source}' AS r ON l.value < r.value WHERE l.value >= 4 LIMIT 20"
            ),
            json!([{"l.value":4,"r.value":5},{"l.value":5,"r.value":null}]),
        ),
        (
            format!(
                "SELECT value FROM '{source}' WHERE value <= 2 UNION ALL SELECT value FROM '{source}' WHERE value >= 4 ORDER BY value DESC LIMIT 20"
            ),
            json!([{"value":5},{"value":4},{"value":2},{"value":1}]),
        ),
        (
            format!(
                "SELECT value, RANK() OVER (ORDER BY value DESC) AS ranked FROM '{source}' LIMIT 2"
            ),
            json!([{"value":1,"ranked":5},{"value":2,"ranked":4}]),
        ),
        (
            format!(
                "SELECT value FROM '{source}' WHERE value IN (SELECT COUNT(*) AS n FROM '{source}' WHERE value <= outer.value HAVING COUNT(*) > 3) LIMIT 20"
            ),
            json!([{"value":4},{"value":5}]),
        ),
    ];
    let mut worker = Worker::new();
    for (statement, expected) in cases {
        for execution in 1..=3 {
            completed(&worker.relational(&statement, &[]), &expected, 1, execution);
        }
    }
}

#[test]
fn worker_flat_declared_sources_preserve_zero_limit_and_zero_offset() {
    let source = fixture().display().to_string();
    let bindings = json!({source.clone(): {"input_format":"vortex"}}).to_string();
    let mut worker = Worker::new();
    for surface in ["sql", "dataframe"] {
        for (body, tail, expected) in [
            ("value", "LIMIT 0", json!([])),
            ("COUNT(*)", "LIMIT 0", json!([])),
            ("COUNT(value) AS n", "LIMIT 0 OFFSET 0", json!([])),
            ("value, COUNT(*) AS n", "GROUP BY value LIMIT 0", json!([])),
            (
                "value",
                "WHERE value >= 3 ORDER BY value LIMIT 00",
                json!([]),
            ),
            (
                "value + 1 AS next",
                "LIMIT 2 OFFSET 0",
                json!([{"next":2}, {"next":3}]),
            ),
        ] {
            let statement = format!("SELECT {body} FROM '{source}' {tail}");
            for execution in 1..=2 {
                let result = worker.request(&[
                    "run",
                    surface,
                    "--sql",
                    &statement,
                    "--source-bindings",
                    &bindings,
                    "--request",
                    "collect",
                    "--memory-gb",
                    "1",
                    "--max-parallelism",
                    "2",
                ]);
                completed(&result, &expected, 1, execution);
            }
        }
    }
}

struct Cleanup(PathBuf);
impl Cleanup {
    fn new() -> Self {
        let stamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let path = std::env::temp_dir().join(format!(
            "shardloom-worker-relational-{}-{stamp}",
            std::process::id()
        ));
        std::fs::create_dir(&path).unwrap();
        Self(path)
    }
}
impl Drop for Cleanup {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

#[cfg(all(feature = "vortex-write", feature = "universal-format-io"))]
#[test]
fn worker_relational_retains_each_sources_declared_format_and_schema() {
    let root = Cleanup::new();
    // Explicit adapters take precedence over suffix inference, on every leaf.
    let left = root.0.join("left.json");
    let right = root.0.join("right.data");
    std::fs::write(&left, "key,amount\n001,2\n1,3\n").unwrap();
    std::fs::write(
        &right,
        "{\"key\":\"001\",\"label\":\"0009\"}\n{\"key\":\"1\",\"label\":\"0010\"}\n",
    )
    .unwrap();
    let bindings = json!({
        left.display().to_string(): {"input_format":"csv", "source_schema":"key:utf8,amount:int64"},
        right.display().to_string(): {"input_format":"jsonl", "source_schema":"key:utf8,label:utf8"},
    }).to_string();
    let statement = format!(
        "SELECT l.key AS key, l.amount AS amount, r.label AS label FROM '{}' AS l JOIN '{}' AS r ON l.key = r.key ORDER BY key",
        left.display(),
        right.display()
    );
    let mut worker = Worker::new();
    for execution in 1..=2 {
        let result = worker.relational(&statement, &["--source-bindings", &bindings]);
        completed(
            &result,
            &json!([{"key":"001","amount":2,"label":"0009"}, {"key":"1","amount":3,"label":"0010"}]),
            2,
            execution,
        );
        assert_eq!(field(&result, "relational_normalized_source_count"), "2");
        assert_eq!(
            field(&result, "public_workflow_preparation_included"),
            (execution == 1).to_string()
        );
    }
    let statement = format!(
        "SELECT key FROM '{}' UNION SELECT key FROM '{}' ORDER BY key",
        left.display(),
        right.display()
    );
    completed(
        &worker.relational(&statement, &["--source-bindings", &bindings]),
        &json!([{"key":"001"},{"key":"1"}]),
        2,
        1,
    );
}

#[test]
fn worker_relational_source_declaration_errors_precede_source_access() {
    let mut worker = Worker::new();
    let statement =
        "SELECT key FROM '/missing/left.data' UNION SELECT key FROM '/missing/right.data'";
    let cases = [
        (
            r#"{"/missing/extra.data":{"input_format":"csv"}}"#,
            "not referenced",
        ),
        (
            r#"{"/missing/left.data":{"input_format":"vortex","source_schema":"key:utf8"}}"#,
            "authoritative schema",
        ),
        (
            r#"{"/missing/left.data":{"input_format":"csv","typo":1}}"#,
            "unknown field",
        ),
        (
            r#"{"/missing/left.data":{"source_schema":"key:utf8"}}"#,
            "missing field",
        ),
    ];
    for (bindings, message) in cases {
        let result = worker.request(&[
            "route",
            "sql",
            "--sql",
            statement,
            "--source-bindings",
            bindings,
            "--bounded",
            "true",
        ]);
        assert_ne!(result["status"], "success", "{result}");
        assert!(result.to_string().contains(message), "{result}");
        assert!(!result.to_string().contains("No such file"), "{result}");
    }
    let result = worker.request(&[
        "route",
        "sql",
        "--sql",
        statement,
        "--input",
        "/missing/left.data",
        "--input-format",
        "jsonl",
        "--source-bindings",
        r#"{"/missing/left.data":{"input_format":"csv"}}"#,
        "--bounded",
        "true",
    ]);
    assert_ne!(result["status"], "success", "{result}");
    assert!(
        result.to_string().contains("conflicts with the primary"),
        "{result}"
    );
}

#[cfg(all(feature = "vortex-write", feature = "universal-format-io"))]
#[test]
fn worker_relational_normalizes_every_nested_leaf_once_and_preserves_path_literals() {
    let root = Cleanup::new();
    let left = root.0.join("left.csv");
    let right = root.0.join("right.jsonl");
    let native = fixture();
    std::fs::write(
        &left,
        format!(
            "renamed,label\n1,other\n2,{}\n3,{}\n",
            left.display(),
            left.display()
        ),
    )
    .unwrap();
    std::fs::write(&right, "{\"value\":2}\n{\"value\":3}\n{\"value\":7}\n").unwrap();
    let statement = format!(
        "SELECT renamed, CASE WHEN label = '{}' THEN 'match' ELSE 'miss' END AS flag FROM '{}' WHERE renamed IN (SELECT value FROM '{}' WHERE value IN (SELECT value FROM '{}')) LIMIT 10",
        left.display(),
        left.display(),
        right.display(),
        native.display()
    );
    let mut worker = Worker::new();
    for execution in 1..=2 {
        let result = worker.relational(&statement, &[]);
        completed(
            &result,
            &json!([{"renamed":2,"flag":"match"},{"renamed":3,"flag":"match"}]),
            3,
            execution,
        );
        assert_eq!(field(&result, "relational_normalized_source_count"), "2");
    }
    std::fs::write(&right, "{\"value\":1}\n").unwrap();
    let stale = worker.relational(&statement, &[]);
    assert_eq!(stale["status"], "error", "{stale}");
    assert!(
        stale.to_string().contains("source generation changed"),
        "{stale}"
    );
}

#[cfg(all(feature = "vortex-write", feature = "universal-format-io"))]
#[test]
fn worker_relational_writers_reject_aliases_of_original_compatibility_sources() {
    let root = Cleanup::new();
    let input = root.0.join("input.csv");
    let original = "renamed\n1\n2\n3\n";
    std::fs::write(&input, original).unwrap();
    let requests = [
        "write_vortex",
        "write_json",
        "write_jsonl",
        "write_csv",
        "write_parquet",
        "write_arrow_ipc",
        "write_avro",
        "write_orc",
    ];
    // Create every hard link before capturing the source's ctime generation.
    for request in requests {
        std::fs::hard_link(&input, root.0.join(format!("alias-{request}"))).unwrap();
    }
    let statement = format!(
        "SELECT renamed, ROW_NUMBER() OVER (ORDER BY renamed DESC) AS ranked FROM '{}' LIMIT 10",
        input.display()
    );
    let mut worker = Worker::new();
    completed(
        &worker.relational(&statement, &[]),
        &json!([{"renamed":1,"ranked":3},{"renamed":2,"ranked":2},{"renamed":3,"ranked":1}]),
        1,
        1,
    );
    for request in requests {
        let alias = root.0.join(format!("alias-{request}"));
        let result = worker.relational(
            &statement,
            &[
                "--request",
                request,
                "--output",
                alias.to_str().unwrap(),
                "--allow-overwrite",
            ],
        );
        assert_eq!(result["status"], "error", "{request}: {result}");
        assert!(
            result.to_string().contains("different files")
                || result.to_string().contains("multiple hardlinks"),
            "{request}: {result}"
        );
        assert_eq!(std::fs::read_to_string(&input).unwrap(), original);
        assert_eq!(std::fs::read_to_string(&alias).unwrap(), original);
    }
}

#[cfg(all(feature = "vortex-write", feature = "universal-format-io"))]
#[test]
fn worker_relational_projected_subquery_does_not_inherit_the_reference_value_cap() {
    let root = Cleanup::new();
    let input = root.0.join("input.csv");
    let mut csv = String::from("renamed\n");
    for value in 1..=40 {
        use std::fmt::Write as _;
        writeln!(csv, "{value}").unwrap();
    }
    std::fs::write(&input, csv).unwrap();
    for inner in [
        "",
        " LIMIT 40",
        " GROUP BY renamed HAVING COUNT(*) = 1",
        " GROUP BY renamed HAVING COUNT(*) = 1 LIMIT 40",
    ] {
        let statement = format!(
            "SELECT renamed FROM '{}' WHERE renamed IN (SELECT renamed FROM '{}'{inner}) ORDER BY renamed DESC LIMIT 2",
            input.display(),
            input.display()
        );
        completed(
            &Worker::new().relational(&statement, &[]),
            &json!([{"renamed":40},{"renamed":39}]),
            1,
            1,
        );
    }
}

#[cfg(all(feature = "vortex-write", feature = "universal-format-io"))]
#[test]
fn worker_relational_prunes_wide_sources_with_nested_outer_scope_dependencies() {
    let root = Cleanup::new();
    let input = root.0.join("wide.jsonl");
    let rows = (1..=3)
        .map(|key| {
            let mut row = serde_json::Map::new();
            row.insert("key".into(), json!(key));
            row.insert("measure".into(), json!(key + 10));
            row.insert("label".into(), json!(format!("port-{key}")));
            for field in 0..140 {
                row.insert(format!("unused_{field}"), json!(field));
            }
            serde_json::to_string(&row).unwrap() + "\n"
        })
        .collect::<String>();
    std::fs::write(&input, rows).unwrap();
    let source = input.display();
    let cases = [
        (
            format!(
                "SELECT l.key AS cargo,r.label AS dock FROM '{source}' AS l JOIN '{source}' AS r ON l.key = r.key WHERE l.measure > 11"
            ),
            json!([{"cargo":2,"dock":"port-2"},{"cargo":3,"dock":"port-3"}]),
        ),
        (
            format!("SELECT key,RANK() OVER (ORDER BY measure DESC) AS ranked FROM '{source}'"),
            json!([{"key":1,"ranked":3},{"key":2,"ranked":2},{"key":3,"ranked":1}]),
        ),
        (
            format!(
                "SELECT key FROM '{source}' WHERE key IN (SELECT COUNT(*) AS n FROM '{source}' WHERE measure <= outer.measure)"
            ),
            json!([{"key":1},{"key":2},{"key":3}]),
        ),
        (
            format!(
                "SELECT key FROM '{source}' WHERE EXISTS (SELECT measure FROM '{source}' WHERE EXISTS (SELECT key FROM '{source}' WHERE key = outer.key) AND measure > outer.measure)"
            ),
            json!([{"key":1},{"key":2}]),
        ),
    ];
    let mut worker = Worker::new();
    for (statement, expected) in cases {
        for execution in 1..=2 {
            completed(&worker.relational(&statement, &[]), &expected, 1, execution);
        }
    }
}

#[test]
fn worker_relational_invalidates_any_source_and_rebinds_after_failure() {
    let root = Cleanup::new();
    let left = root.0.join("left.vortex");
    let right = root.0.join("right.vortex");
    std::fs::copy(fixture(), &left).unwrap();
    std::fs::copy(fixture(), &right).unwrap();
    let statement = format!(
        "SELECT l.value FROM '{}' AS l JOIN '{}' AS r ON l.value = r.value LIMIT 1",
        left.display(),
        right.display()
    );
    let mut worker = Worker::new();
    completed(
        &worker.relational(&statement, &[]),
        &json!([{"l.value":1}]),
        2,
        1,
    );
    let replacement = root.0.join("replacement.vortex");
    std::fs::copy(fixture(), &replacement).unwrap();
    std::fs::rename(&replacement, &right).unwrap();
    let invalidated = worker.relational(&statement, &[]);
    assert_eq!(invalidated["status"], "error", "{invalidated}");
    assert!(
        invalidated.to_string().contains("prepared source changed"),
        "{invalidated}"
    );
    completed(
        &worker.relational(&statement, &[]),
        &json!([{"l.value":1}]),
        2,
        1,
    );
}

#[test]
fn worker_relational_route_inspection_and_zero_decode_do_not_open_sources() {
    let statement = "SELECT l.value FROM '/missing/left.vortex' AS l JOIN '/missing/right.vortex' AS r ON l.value = r.value LIMIT 10";
    let mut worker = Worker::new();
    let inspected = worker.request(&[
        "route",
        "sql",
        "--sql",
        statement,
        "--request",
        "collect",
        "--materialization-policy",
        "bounded",
    ]);
    assert_eq!(inspected["status"], "success", "{inspected}");
    assert_eq!(
        field(&inspected, "route_id"),
        "native_vortex_relational_collect"
    );
    assert_eq!(field(&inspected, "source_io_performed"), "false");
    assert_eq!(field(&inspected, "native_vortex_plan_source_count"), "2");
    assert_eq!(
        field(&inspected, "native_vortex_plan_payload_kind"),
        "native_relational_plan"
    );
    assert_eq!(
        field(&inspected, "native_vortex_plan_contract_status"),
        "admitted"
    );
    let denied = worker.relational(statement, &["--materialization-policy", "zero_decode"]);
    assert_eq!(denied["status"], "unsupported", "{denied}");
    assert_eq!(field(&denied, "source_io_performed"), "false");
    assert!(denied.to_string().contains("materialization"), "{denied}");
}

#[cfg(all(feature = "vortex-write", feature = "universal-format-io"))]
#[test]
fn worker_relational_writes_complete_nullable_results_through_all_eight_writers() {
    let root = Cleanup::new();
    let source = fixture().display().to_string();
    let statement = format!(
        "SELECT l.value + 0 AS left_value, r.value + 0 AS right_value FROM '{source}' AS l LEFT JOIN '{source}' AS r ON l.value < r.value WHERE l.value >= 4 LIMIT 20"
    );
    let expected = json!([{"left_value":4,"right_value":5},{"left_value":5,"right_value":null}]);
    let mut worker = Worker::new();
    for (format, request) in [
        ("vortex", "write_vortex"),
        ("json", "write_json"),
        ("jsonl", "write_jsonl"),
        ("csv", "write_csv"),
        ("parquet", "write_parquet"),
        ("arrow-ipc", "write_arrow_ipc"),
        ("avro", "write_avro"),
        ("orc", "write_orc"),
    ] {
        let path = root.0.join(format!("selected.{format}"));
        let result = worker.relational(
            &statement,
            &["--request", request, "--output", path.to_str().unwrap()],
        );
        assert_eq!(result["status"], "success", "{format}: {result}");
        assert_eq!(
            field(&result, "public_workflow_route_id"),
            "native_vortex_relational_write"
        );
        assert_eq!(
            field(&result, "native_vortex_result_export_rows_written"),
            "2"
        );
        assert_eq!(field(&result, "resident_completed_executions"), "1");
        let reopened = if format == "vortex" {
            path
        } else {
            let native = root.0.join(format!("reopened-{format}.vortex"));
            let prepared = worker.request(&[
                "prepare",
                "dataframe",
                "--input",
                path.to_str().unwrap(),
                "--input-format",
                format,
                "--output",
                native.to_str().unwrap(),
                "--memory-gb",
                "1",
                "--max-parallelism",
                "2",
            ]);
            assert_eq!(prepared["status"], "success", "{format}: {prepared}");
            native
        };
        let result = worker.collect(&reopened, "left_value,right_value", "2");
        assert_eq!(result["status"], "success", "{format}: {result}");
        let (_, payload) = result["human_text"]
            .as_str()
            .unwrap()
            .split_once("values=")
            .unwrap();
        let rows = serde_json::from_str::<Value>(payload);
        // The ordinary owned collect exposes the complete native values payload.
        assert_eq!(rows.unwrap()["values"], expected, "{format}");
    }
}
