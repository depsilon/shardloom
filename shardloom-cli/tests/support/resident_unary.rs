use super::*;

impl Worker {
    fn unary(&mut self, path: &Path, primitive: &str, extra: &[&str]) -> Value {
        let mut args = vec![
            "run",
            "dataframe",
            "--input",
            path.to_str().unwrap(),
            "--input-format",
            "vortex",
            "--request",
            "collect",
            "--bounded",
            "true",
            "--materialization-policy",
            "bounded",
            "--vortex-primitive",
            primitive,
            "--memory-gb",
            "1",
        ];
        if !extra.contains(&"--max-parallelism") {
            args.extend(["--max-parallelism", "2"]);
        }
        args.extend_from_slice(extra);
        self.request(&args)
    }
}

fn completed(result: &Value, expected: &Value, executions: usize) {
    assert_eq!(result["status"], "success", "{result}");
    let rows = field(result, "result_jsonl")
        .lines()
        .map(|line| serde_json::from_str::<Value>(line).unwrap())
        .collect::<Vec<_>>();
    assert_eq!(Value::Array(rows), *expected);
    for (name, value) in [
        ("resident_source_opens", "1"),
        ("resident_unary_handle_retained", "true"),
        ("result_payload_complete", "true"),
        ("fallback_attempted", "false"),
        ("external_engine_invoked", "false"),
    ] {
        assert_eq!(field(result, name), value, "{name}");
    }
    assert_eq!(
        field(result, "resident_completed_executions"),
        executions.to_string()
    );
    assert_eq!(
        field(result, "resident_footer_open_performed_this_call"),
        (executions == 1).to_string()
    );
    assert_eq!(
        field(result, "resident_unary_lowering_reused"),
        (executions > 1).to_string()
    );
    assert_eq!(
        field(result, "output_row_count"),
        expected.as_array().unwrap().len().to_string()
    );
}

#[test]
#[allow(clippy::too_many_lines)] // Keep the literal nine-family expectation table together.
fn worker_unary_reuses_sources_and_delivers_complete_values_for_each_flat_family() {
    let all = json!([{"value":1},{"value":2},{"value":3},{"value":4},{"value":5}]);
    let mut worker = Worker::new();
    let path = fixture();
    let cases = [
        ("distinct", vec!["--vortex-columns", "value"], all.clone()),
        (
            "drop_duplicates",
            vec![
                "--vortex-columns",
                "value",
                "--vortex-deduplicate-key-columns",
                "value",
                "--vortex-duplicate-keep",
                "last",
            ],
            all,
        ),
        (
            "duplicate_mask",
            vec![
                "--vortex-columns",
                "value",
                "--vortex-duplicate-keep",
                "false",
            ],
            json!([{"duplicated":false},{"duplicated":false},{"duplicated":false},{"duplicated":false},{"duplicated":false}]),
        ),
        (
            "tail",
            vec![
                "--vortex-columns",
                "value",
                "--vortex-source-order-limit",
                "2",
            ],
            json!([{"value":4},{"value":5}]),
        ),
        (
            "sample",
            vec![
                "--vortex-columns",
                "value",
                "--vortex-source-order-limit",
                "2",
                "--vortex-sample-seed",
                "7",
            ],
            json!([{"value":2},{"value":5}]),
        ),
        (
            "expression_project",
            vec![
                "--vortex-columns",
                "value",
                "--vortex-expression-projection",
                r#"{"columns":["value"],"rewrites":[{"kind":"numeric_scalar_arithmetic","target_column":"value","operator":"+","operand":{"type":"int64","value":1}}]}"#,
            ],
            json!([{"value":2},{"value":3},{"value":4},{"value":5},{"value":6}]),
        ),
        (
            "melt",
            vec![
                "--vortex-columns",
                "value,metric",
                "--vortex-melt-projection",
                r#"{"id_columns":["value"],"value_columns":["metric"],"variable_column":"measure","value_column":"amount"}"#,
            ],
            json!([{"value":1,"measure":"metric","amount":10},{"value":2,"measure":"metric","amount":20},{"value":3,"measure":"metric","amount":30},{"value":4,"measure":"metric","amount":40},{"value":5,"measure":"metric","amount":50}]),
        ),
        (
            "rolling_window",
            vec![
                "--vortex-columns",
                "metric",
                "--vortex-rolling-window",
                r#"{"source_column":"metric","output_column":"total","window_size":2,"min_periods":2,"aggregate":"sum"}"#,
            ],
            json!([{"total":30.0},{"total":50.0},{"total":70.0},{"total":90.0}]),
        ),
        (
            "pivot",
            vec![
                "--vortex-columns",
                "value,metric",
                "--vortex-pivot-projection",
                r#"{"index_column":"value","pivot_column":"value","value_column":"metric","aggregate":"sum"}"#,
            ],
            json!([
                {"value":1,"pivot_1":10.0,"pivot_2":null,"pivot_3":null,"pivot_4":null,"pivot_5":null},
                {"value":2,"pivot_1":null,"pivot_2":20.0,"pivot_3":null,"pivot_4":null,"pivot_5":null},
                {"value":3,"pivot_1":null,"pivot_2":null,"pivot_3":30.0,"pivot_4":null,"pivot_5":null},
                {"value":4,"pivot_1":null,"pivot_2":null,"pivot_3":null,"pivot_4":40.0,"pivot_5":null},
                {"value":5,"pivot_1":null,"pivot_2":null,"pivot_3":null,"pivot_4":null,"pivot_5":50.0},
            ]),
        ),
    ];
    for (primitive, extra, expected) in cases {
        for execution in 1..=3 {
            completed(
                &worker.unary(&path, primitive, &extra),
                &expected,
                execution,
            );
        }
    }
}

#[test]
fn worker_unary_computations_apply_source_filters_before_values_and_schema() {
    let mut worker = Worker::new();
    let path = fixture();
    let cases = [
        (
            "expression_project",
            vec![
                "--vortex-columns",
                "value",
                "--vortex-expression-projection",
                r#"{"columns":["value"],"rewrites":[{"kind":"numeric_scalar_arithmetic","target_column":"value","operator":"+","operand":{"type":"int64","value":1}}]}"#,
            ],
            json!([{"value":5},{"value":6}]),
        ),
        (
            "melt",
            vec![
                "--vortex-columns",
                "value,metric",
                "--vortex-melt-projection",
                r#"{"id_columns":["value"],"value_columns":["metric"],"variable_column":"measure","value_column":"amount"}"#,
            ],
            json!([{"value":4,"measure":"metric","amount":40},{"value":5,"measure":"metric","amount":50}]),
        ),
        (
            "rolling_window",
            vec![
                "--vortex-columns",
                "metric",
                "--vortex-rolling-window",
                r#"{"source_column":"metric","output_column":"total","window_size":2,"min_periods":2,"aggregate":"sum"}"#,
            ],
            json!([{"total":90.0}]),
        ),
        (
            "pivot",
            vec![
                "--vortex-columns",
                "value,metric",
                "--vortex-pivot-projection",
                r#"{"index_column":"value","pivot_column":"value","value_column":"metric","aggregate":"sum"}"#,
            ],
            json!([{"value":4,"pivot_4":40.0,"pivot_5":null},{"value":5,"pivot_4":null,"pivot_5":50.0}]),
        ),
    ];
    for (primitive, mut extra, expected) in cases {
        extra.extend(["--vortex-predicate", "gt:value:3"]);
        for execution in 1..=3 {
            completed(
                &worker.unary(&path, primitive, &extra),
                &expected,
                execution,
            );
        }
    }
}

#[test]
fn worker_unary_rejects_unsupported_predicates_before_opening() {
    let mut worker = Worker::new();
    for primitive in ["tail", "duplicate_mask"] {
        let result = worker.unary(
            Path::new("/nonexistent/unary-predicate.vortex"),
            primitive,
            &[
                "--vortex-columns",
                "value",
                "--vortex-source-order-limit",
                "2",
                "--vortex-predicate",
                "gt:value:3",
            ],
        );
        assert_eq!(result["status"], "error", "{result}");
        assert!(
            result
                .to_string()
                .contains("source predicates are not admitted")
        );
        assert_eq!(result["fallback"]["attempted"], false);
    }
}

#[test]
fn worker_unary_rebinds_changed_columns_and_resources_and_rejects_source_replacement() {
    let stamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let root = std::env::temp_dir().join(format!(
        "shardloom-worker-unary-{}-{stamp}",
        std::process::id()
    ));
    std::fs::create_dir(&root).unwrap();
    let path = root.join("source.vortex");
    std::fs::copy(fixture(), &path).unwrap();
    let mut worker = Worker::new();
    let full = json!([{"value":1},{"value":2},{"value":3},{"value":4},{"value":5}]);
    let extra = [
        "--vortex-columns",
        "value",
        "--vortex-predicate",
        "gt:value:99",
    ];
    completed(&worker.unary(&path, "distinct", &extra), &json!([]), 1);
    completed(&worker.unary(&path, "distinct", &extra), &json!([]), 2);
    let replacement = root.join("replacement.vortex");
    std::fs::copy(fixture(), &replacement).unwrap();
    std::fs::rename(replacement, &path).unwrap();
    let failed = worker.unary(&path, "distinct", &extra);
    assert_eq!(failed["status"], "error", "{failed}");
    assert!(failed.to_string().contains("prepared source changed"));
    completed(&worker.unary(&path, "distinct", &extra), &json!([]), 1);
    completed(
        &worker.unary(&path, "distinct", &["--vortex-columns", "value"]),
        &full,
        1,
    );
    completed(
        &worker.unary(
            &path,
            "distinct",
            &["--vortex-columns", "value", "--max-parallelism", "1"],
        ),
        &full,
        1,
    );
    completed(
        &worker.unary(
            &path,
            "distinct",
            &["--vortex-columns", "value", "--max-parallelism", "1"],
        ),
        &full,
        2,
    );
    completed(
        &worker.unary(
            &path,
            "tail",
            &[
                "--vortex-columns",
                "metric",
                "--vortex-source-order-limit",
                "1",
            ],
        ),
        &json!([{"metric":50}]),
        1,
    );
    drop(worker);
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn worker_unary_rejects_zero_decode_before_opening_the_source() {
    let mut worker = Worker::new();
    let result = worker.unary(
        Path::new("/nonexistent/unary-policy.vortex"),
        "distinct",
        &[
            "--vortex-columns",
            "value",
            "--materialization-policy",
            "zero_decode",
        ],
    );
    assert_eq!(result["status"], "error", "{result}");
    assert!(
        result
            .to_string()
            .contains("require admitted materialization"),
        "{result}"
    );
}

#[cfg(all(feature = "vortex-write", feature = "universal-format-io"))]
#[test]
fn worker_unary_binary_writes_reopen_complete_values_without_structured_payloads() {
    struct Cleanup(PathBuf);
    impl Drop for Cleanup {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }
    let stamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let root = std::env::temp_dir().join(format!(
        "shardloom-worker-unary-writer-{}-{stamp}",
        std::process::id()
    ));
    std::fs::create_dir(&root).unwrap();
    let _cleanup = Cleanup(root.clone());
    let mut worker = Worker::new();
    for (format, request) in [
        ("vortex", "write_vortex"),
        ("parquet", "write_parquet"),
        ("arrow-ipc", "write_arrow_ipc"),
        ("avro", "write_avro"),
        ("orc", "write_orc"),
    ] {
        let path = root.join(format!("selected.{format}"));
        let result = worker.unary(
            &fixture(),
            "distinct",
            &[
                "--vortex-columns",
                "value",
                "--vortex-source-order-limit",
                "2",
                "--request",
                request,
                "--output",
                path.to_str().unwrap(),
            ],
        );
        assert_eq!(result["status"], "success", "{format}: {result}");
        let reopened = if format == "vortex" {
            path
        } else {
            let native = root.join(format!("{format}.vortex"));
            let result = worker.request(&[
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
            assert_eq!(result["status"], "success", "{format}: {result}");
            native
        };
        completed(
            &worker.unary(&reopened, "distinct", &["--vortex-columns", "value"]),
            &json!([{"value":1}, {"value":2}]),
            1,
        );
    }
}

#[cfg(all(feature = "vortex-write", feature = "universal-format-io"))]
#[test]
fn worker_unary_explode_preserves_nullable_list_elements_and_reuses_preparation() {
    struct Cleanup(PathBuf);
    impl Drop for Cleanup {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }
    let stamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let root = std::env::temp_dir().join(format!(
        "shardloom-worker-explode-{}-{stamp}",
        std::process::id()
    ));
    std::fs::create_dir(&root).unwrap();
    let _cleanup = Cleanup(root.clone());
    let source = root.join("source.vortex");
    let mut worker = Worker::new();
    let prepared = worker.request(&[
        "run", "dataframe", "--input", fixture().to_str().unwrap(),
        "--input-format", "vortex", "--request", "write_vortex", "--output", source.to_str().unwrap(),
        "--bounded", "true", "--materialization-policy", "bounded",
        "--vortex-primitive", "expression_project", "--vortex-columns", "value",
        "--vortex-source-order-limit", "2", "--vortex-expression-projection",
        r#"{"structured_columns":[{"name":"shipment","source":"value"},{"name":"items","array":[7,null,8]}]}"#,
        "--memory-gb", "1", "--max-parallelism", "2",
    ]);
    assert_eq!(prepared["status"], "success", "{prepared}");
    let expected = json!([
        {"shipment":1,"items":7}, {"shipment":1,"items":null}, {"shipment":1,"items":8},
        {"shipment":2,"items":7}, {"shipment":2,"items":null}, {"shipment":2,"items":8},
    ]);
    for execution in 1..=3 {
        completed(
            &worker.unary(
                &source,
                "explode",
                &[
                    "--vortex-columns",
                    "shipment,items",
                    "--vortex-explode-projection",
                    r#"{"column":"items"}"#,
                ],
            ),
            &expected,
            execution,
        );
    }
    for execution in 1..=2 {
        completed(
            &worker.unary(
                &source,
                "explode",
                &[
                    "--vortex-columns",
                    "shipment,items",
                    "--vortex-explode-projection",
                    r#"{"column":"items"}"#,
                    "--vortex-predicate",
                    "gte:shipment:2",
                ],
            ),
            &json!([{"shipment":2,"items":7},{"shipment":2,"items":null},{"shipment":2,"items":8}]),
            execution,
        );
    }
}
