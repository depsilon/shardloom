use super::*;

impl Worker {
    fn aggregate(
        &mut self,
        path: &Path,
        aggregate: &str,
        predicate: Option<&str>,
        limit: Option<&str>,
        memory: &str,
        workers: &str,
    ) -> Value {
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
            "aggregate",
            "--vortex-aggregate",
            aggregate,
            "--memory-gb",
            memory,
            "--max-parallelism",
            workers,
        ];
        if let Some(predicate) = predicate {
            args.extend(["--vortex-predicate", predicate]);
        }
        if let Some(limit) = limit {
            args.extend(["--vortex-source-order-limit", limit]);
        }
        self.request(&args)
    }
}

fn scalar() -> String {
    json!({"measures":[{"function":"count","alias":"rows_alias"},
        {"function":"count_distinct","column":"value","alias":"unique_alias"},
        {"function":"sum","column":"metric","alias":"total_alias"}]})
    .to_string()
}

#[test]
fn worker_reuses_integer_extrema_and_average_with_filters_order_and_empty_results() {
    let path = fixture();
    for workers in ["1", "2", "4"] {
        let mut worker = Worker::new();
        let scalar = json!({"measures":[
            {"function":"min","column":"metric","alias":"lo"},
            {"function":"max","column":"metric","alias":"hi"},
            {"function":"avg","column":"metric","alias":"mean"}]})
        .to_string();
        for (predicate, expected) in [
            (None, json!({"lo":10,"hi":50,"mean":30.0})),
            (Some("gte:value:3"), json!({"lo":30,"hi":50,"mean":40.0})),
            (
                Some("gte:value:99"),
                json!({"lo":null,"hi":null,"mean":null}),
            ),
        ] {
            for execution in 1..=3 {
                completed(
                    &worker.aggregate(&path, &scalar, predicate, None, "1", workers),
                    &expected,
                    &execution.to_string(),
                    execution == 1,
                );
            }
        }
        let grouped = json!({"group_by":["value"], "measures":[
            {"function":"min","column":"metric","alias":"lo"},
            {"function":"max","column":"metric","alias":"hi"},
            {"function":"avg","column":"metric","alias":"mean"}],
            "order_by":[{"column":"value","descending":true}], "offset":1})
        .to_string();
        for execution in 1..=3 {
            completed(
                &worker.aggregate(&path, &grouped, None, Some("2"), "1", workers),
                &json!([
                    {"value":4,"lo":40,"hi":40,"mean":40.0},
                    {"value":3,"lo":30,"hi":30,"mean":30.0},
                ]),
                &execution.to_string(),
                execution == 1,
            );
        }
    }
}

fn values(result: &Value) -> Value {
    let summary = result["human_text"]
        .as_str()
        .unwrap()
        .lines()
        .find_map(|line| line.strip_prefix("result summary: "))
        .unwrap();
    let payload: Value = serde_json::from_str(summary.rsplit_once(" values=").unwrap().1).unwrap();
    payload["values"].clone()
}

fn completed(result: &Value, expected: &Value, executions: &str, opened: bool) {
    assert_eq!(result["status"], "success", "{result}");
    assert_eq!(&values(result), expected);
    for (key, value) in [
        ("resident_source_opens", "1"),
        ("resident_completed_executions", executions),
        ("resident_aggregate_handle_retained", "true"),
        ("local_primitive_report_present", "true"),
        ("local_primitive_native_io_certificate_emitted", "true"),
        ("local_primitive_native_io_certified", "true"),
        ("local_primitive_execution_certificate_emitted", "false"),
        ("local_primitive_no_query_answer_cache", "true"),
        ("public_workflow_fallback_attempted", "false"),
        ("public_workflow_external_engine_invoked", "false"),
    ] {
        assert_eq!(field(result, key), value, "{key}: {result}");
    }
    assert_eq!(
        field(result, "resident_footer_open_performed_this_call"),
        opened.to_string()
    );
    assert_eq!(
        field(result, "resident_aggregate_lowering_reused"),
        (!opened).to_string()
    );
}

#[test]
fn worker_reuses_aggregate_lowering_with_complete_fresh_scalar_and_grouped_values() {
    let path = fixture();
    let mut worker = Worker::new();
    let aggregate = scalar();
    for (execution, opened) in [("1", true), ("2", false), ("3", false)] {
        let result = worker.aggregate(&path, &aggregate, None, None, "1", "2");
        completed(
            &result,
            &json!({"rows_alias":5,"unique_alias":5,"total_alias":150.0}),
            execution,
            opened,
        );
    }
    let grouped = json!({"group_by":["value"],"measures":[
        {"function":"count_distinct","column":"metric","alias":"unique_alias"}],
        "order_by":[{"column":"unique_alias","descending":true},{"column":"value","descending":false}],
        "offset":1}).to_string();
    for (execution, opened) in [("1", true), ("2", false)] {
        completed(
            &worker.aggregate(&path, &grouped, None, Some("2"), "1", "2"),
            &json!([{"value":2,"unique_alias":1},{"value":3,"unique_alias":1}]),
            execution,
            opened,
        );
    }
    // Changing filter, source limit, memory and CPU grants invalidates the old
    // operation rather than reusing a stale plan or a wider runtime.
    for (memory, workers, execution) in [
        ("1", "1", "1"),
        ("1", "1", "2"),
        ("2", "1", "1"),
        ("2", "2", "1"),
    ] {
        completed(
            &worker.aggregate(&path, &aggregate, Some("gt:value:2"), None, memory, workers),
            &json!({"rows_alias":3,"unique_alias":3,"total_alias":120.0}),
            execution,
            execution == "1",
        );
    }
    assert_eq!(worker.raw_request("{invalid")["status"], "error");
    completed(
        &worker.aggregate(&path, &aggregate, None, None, "1", "2"),
        &json!({"rows_alias":5,"unique_alias":5,"total_alias":150.0}),
        "1",
        true,
    );
    assert_completed(&worker.collect(&path, "metric", "2"), "1");
    completed(
        &worker.aggregate(&path, &aggregate, None, None, "1", "2"),
        &json!({"rows_alias":5,"unique_alias":5,"total_alias":150.0}),
        "1",
        true,
    );
}

#[test]
fn worker_aggregate_source_replacement_fails_once_before_explicit_reprepare() {
    let stamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let root = std::env::temp_dir().join(format!(
        "shardloom-worker-aggregate-{}-{stamp}",
        std::process::id()
    ));
    std::fs::create_dir(&root).unwrap();
    let path = root.join("source.vortex");
    std::fs::copy(fixture(), &path).unwrap();
    let mut worker = Worker::new();
    let aggregate = scalar();
    let expected = json!({"rows_alias":0,"unique_alias":0,"total_alias":null});
    completed(
        &worker.aggregate(&path, &aggregate, Some("gt:value:99"), None, "1", "2"),
        &expected,
        "1",
        true,
    );
    let replacement = root.join("replacement.vortex");
    std::fs::copy(fixture(), &replacement).unwrap();
    std::fs::rename(replacement, &path).unwrap();
    let failed = worker.aggregate(&path, &aggregate, Some("gt:value:99"), None, "1", "2");
    assert_eq!(failed["status"], "error", "{failed}");
    assert!(failed.to_string().contains("prepared source changed"));
    completed(
        &worker.aggregate(&path, &aggregate, Some("gt:value:99"), None, "1", "2"),
        &expected,
        "1",
        true,
    );
    drop(worker);
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn worker_broader_native_aggregate_keeps_ordinary_execution_and_invalidates_reuse() {
    let path = fixture();
    let mut worker = Worker::new();
    let aggregate = scalar();
    completed(
        &worker.aggregate(&path, &aggregate, None, None, "1", "2"),
        &json!({"rows_alias":5,"unique_alias":5,"total_alias":150.0}),
        "1",
        true,
    );
    let avg =
        json!({"measures":[{"function":"avg","column":"metric","alias":"mean_alias","argument_offset":1}]}).to_string();
    for _ in 0..2 {
        let result = worker.aggregate(&path, &avg, None, None, "1", "2");
        assert_eq!(result["status"], "success", "{result}");
        assert_eq!(values(&result), json!({"mean_alias":31.0}));
        assert!(
            !result["fields"]
                .as_array()
                .unwrap()
                .iter()
                .any(|field| field["key"] == "resident_aggregate_handle_retained")
        );
    }
    completed(
        &worker.aggregate(&path, &aggregate, None, None, "1", "2"),
        &json!({"rows_alias":5,"unique_alias":5,"total_alias":150.0}),
        "1",
        true,
    );
}

#[cfg(all(feature = "vortex-write", feature = "universal-format-io"))]
#[test]
fn worker_true_empty_source_aggregate_reuses_only_preparation_and_invalidates_changed_source() {
    let stamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let root = std::env::temp_dir().join(format!(
        "shardloom-worker-aggregate-empty-{}-{stamp}",
        std::process::id()
    ));
    std::fs::create_dir(&root).unwrap();
    let source = root.join("empty.vortex");
    write_empty_source(&root, &source);
    let mut worker = Worker::new();
    let aggregate = scalar();
    let expected = json!({"rows_alias":0,"unique_alias":0,"total_alias":null});
    for (execution, opened) in [("1", true), ("2", false), ("3", false)] {
        let result = worker.aggregate(&source, &aggregate, None, None, "1", "2");
        completed(&result, &expected, execution, opened);
        assert_eq!(field(&result, "local_primitive_rows_scanned"), "0");
    }
    let grouped = json!({"group_by":["value"],"measures":[
        {"function":"count","alias":"rows_alias"},
        {"function":"count_distinct","column":"metric","alias":"unique_alias"},
        {"function":"sum","column":"metric","alias":"total_alias"}]})
    .to_string();
    for (execution, opened) in [("1", true), ("2", false)] {
        completed(
            &worker.aggregate(&source, &grouped, None, None, "1", "2"),
            &json!([]),
            execution,
            opened,
        );
    }
    let replacement = root.join("replacement.vortex");
    std::fs::copy(fixture(), &replacement).unwrap();
    std::fs::rename(replacement, &source).unwrap();
    let failed = worker.aggregate(&source, &grouped, None, None, "1", "2");
    assert_eq!(failed["status"], "error", "{failed}");
    assert!(failed.to_string().contains("prepared source changed"));
    completed(
        &worker.aggregate(&source, &aggregate, None, None, "1", "2"),
        &json!({"rows_alias":5,"unique_alias":5,"total_alias":150.0}),
        "1",
        true,
    );
    drop(worker);
    std::fs::remove_dir_all(root).unwrap();
}

#[cfg(all(feature = "vortex-write", feature = "universal-format-io"))]
fn write_empty_source(root: &Path, source: &Path) {
    let schema = arrow_schema::Schema::new(vec![
        arrow_schema::Field::new("value", arrow_schema::DataType::Int64, false),
        arrow_schema::Field::new("metric", arrow_schema::DataType::Int64, false),
    ]);
    let ipc = root.join("empty.arrow");
    let mut writer =
        arrow_ipc::writer::FileWriter::try_new(std::fs::File::create(&ipc).unwrap(), &schema)
            .unwrap();
    writer.finish().unwrap();
    drop(writer);
    let columnar = shardloom_vortex::read_flat_arrow_ipc_columnar_source(&ipc, 1).unwrap();
    assert_eq!(columnar.row_count, 0);
    shardloom_vortex::write_flat_columnar_vortex_prepared_state(
        shardloom_vortex::VortexPreparedStateColumnarWriteRequest::new(source, columnar),
    )
    .unwrap();
}

#[cfg(all(feature = "vortex-write", feature = "universal-format-io"))]
#[test]
fn worker_noninteger_aggregate_executes_once_per_open_without_retaining_broader_handle() {
    let stamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let root = std::env::temp_dir().join(format!(
        "shardloom-worker-aggregate-schema-{}-{stamp}",
        std::process::id()
    ));
    std::fs::create_dir(&root).unwrap();
    let jsonl = root.join("input.jsonl");
    let source = root.join("source.vortex");
    std::fs::write(&jsonl, "{\"text_key\":\"a\",\"amount\":1.5}\n{\"text_key\":\"b\",\"amount\":2.0}\n{\"text_key\":\"a\",\"amount\":3.5}\n").unwrap();
    let mut worker = Worker::new();
    let prepared = worker.request(&[
        "prepare",
        "dataframe",
        "--input",
        jsonl.to_str().unwrap(),
        "--input-format",
        "jsonl",
        "--output",
        source.to_str().unwrap(),
        "--memory-gb",
        "1",
        "--max-parallelism",
        "2",
    ]);
    assert_eq!(prepared["status"], "success", "{prepared}");
    let aggregate = json!({"group_by":["text_key"], "measures":[
        {"function":"count","alias":"rows_alias"},{"function":"sum","column":"amount","alias":"total_alias"}],
        "order_by":[{"column":"text_key","descending":false}]}).to_string();
    for _ in 0..3 {
        let result = worker.aggregate(&source, &aggregate, None, None, "1", "2");
        assert_eq!(result["status"], "success", "{result}");
        assert_eq!(
            values(&result),
            json!([
                {"text_key":"a","rows_alias":2,"total_alias":5.0},
                {"text_key":"b","rows_alias":1,"total_alias":2.0},
            ])
        );
        for (key, value) in [
            ("resident_source_opens", "1"),
            ("resident_completed_executions", "1"),
            ("resident_aggregate_handle_retained", "false"),
            ("resident_footer_open_performed_this_call", "true"),
            ("local_primitive_native_io_certified", "true"),
        ] {
            assert_eq!(field(&result, key), value, "{key}: {result}");
        }
    }
    drop(worker);
    std::fs::remove_dir_all(root).unwrap();
}
