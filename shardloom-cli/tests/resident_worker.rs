#![cfg(all(feature = "vortex-local-primitives", unix))]

use std::{
    io::{BufRead, BufReader, Write},
    path::{Path, PathBuf},
    process::{Child, ChildStdout, Command, Stdio},
};

use serde_json::{Value, json};

#[path = "support/complete_result.rs"]
mod complete_result;

#[path = "support/resident_aggregate.rs"]
mod aggregate;

#[path = "support/resident_unary.rs"]
mod unary;

#[path = "support/resident_relational.rs"]
mod relational;

struct Worker {
    child: Child,
    output: BufReader<ChildStdout>,
}

impl Worker {
    fn new() -> Self {
        let mut child = Command::new(env!("CARGO_BIN_EXE_shardloom"))
            .arg("python-worker")
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .spawn()
            .unwrap();
        let output = BufReader::new(child.stdout.take().unwrap());
        Self { child, output }
    }

    fn request(&mut self, args: &[&str]) -> Value {
        self.raw_request(&json!({"args": args}).to_string())
    }

    fn raw_request(&mut self, request: &str) -> Value {
        let input = self.child.stdin.as_mut().unwrap();
        writeln!(input, "{request}").unwrap();
        input.flush().unwrap();
        let mut line = String::new();
        assert!(self.output.read_line(&mut line).unwrap() > 0);
        let result: Value = serde_json::from_str(&line).unwrap();
        let mut keys = std::collections::BTreeSet::new();
        for field in result["fields"].as_array().unwrap() {
            let key = field["key"].as_str().unwrap();
            assert!(keys.insert(key), "repeated report field: {key}");
        }
        result
    }

    fn collect(&mut self, path: &Path, columns: &str, workers: &str) -> Value {
        self.request(&[
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
            "--vortex-primitive",
            "project",
            "--vortex-columns",
            columns,
            "--materialization-policy",
            "bounded",
            "--memory-gb",
            "1",
            "--max-parallelism",
            workers,
        ])
    }

    fn count(&mut self, path: &Path, workers: &str) -> Value {
        self.request(&[
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
            "--vortex-primitive",
            "count",
            "--memory-gb",
            "1",
            "--max-parallelism",
            workers,
        ])
    }

    fn count_where(&mut self, path: &Path, predicate: &str, workers: &str) -> Value {
        self.request(&[
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
            "--vortex-primitive",
            "count_where",
            "--vortex-predicate",
            predicate,
            "--memory-gb",
            "1",
            "--max-parallelism",
            workers,
        ])
    }
}

impl Drop for Worker {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

fn fixture() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../shardloom-vortex/tests/fixtures/local_primitive_struct_five.vortex")
}

fn field<'a>(result: &'a Value, name: &str) -> &'a str {
    result["fields"]
        .as_array()
        .unwrap()
        .iter()
        .find(|field| field["key"] == name)
        .unwrap()["value"]
        .as_str()
        .unwrap()
}

fn assert_completed(result: &Value, executions: &str) {
    assert_eq!(result["status"], "success", "{result}");
    assert_eq!(field(result, "resident_source_opens"), "1");
    assert_eq!(field(result, "resident_completed_executions"), executions);
    assert_eq!(field(result, "public_workflow_fallback_attempted"), "false");
    assert_eq!(
        field(result, "public_workflow_external_engine_invoked"),
        "false"
    );
    assert_eq!(field(result, "output_row_count"), "5");
}

fn assert_filtered_count(result: &Value, count: &str, executions: &str) {
    assert_count_rows(result, count);
    assert_eq!(result["status"], "success", "{result}");
    for (name, value) in [
        ("count", count),
        ("resident_source_opens", "1"),
        ("resident_completed_executions", executions),
        ("filtered_count_local_execution_count", count),
        ("local_primitive_report_present", "true"),
        ("local_primitive_native_io_certificate_emitted", "true"),
        ("local_primitive_native_io_certified", "true"),
        ("local_primitive_execution_certificate_emitted", "false"),
        ("local_primitive_no_query_answer_cache", "true"),
        ("public_workflow_fallback_attempted", "false"),
        ("public_workflow_external_engine_invoked", "false"),
    ] {
        assert_eq!(field(result, name), value, "{name}: {result}");
    }
}

#[test]
fn worker_preserves_one_lane_for_real_filtered_execution_and_rebinds_two() {
    let path = fixture();
    let mut worker = Worker::new();
    for (workers, completed) in [("1", "1"), ("1", "2"), ("2", "1"), ("1", "1")] {
        let result = worker.count_where(&path, "gt:value:2", workers);
        assert_filtered_count(&result, "3", completed);
        assert_eq!(field(&result, "data_read"), "true");
        assert_eq!(field(&result, "public_workflow_max_parallelism"), workers);
        assert_eq!(
            field(&result, "local_primitive_max_parallelism_requested"),
            workers
        );
        assert_eq!(
            field(&result, "public_workflow_dynamic_parallelism_floor_applied"),
            "false"
        );
        assert_eq!(
            field(&result, "resident_provider_background_workers"),
            if workers == "1" { "0" } else { "1" }
        );
    }
}

#[test]
fn worker_reuses_filtered_counts_and_clears_changed_or_failed_operations() {
    let path = fixture();
    let mut worker = Worker::new();
    for completed in ["1", "2", "3"] {
        let result = worker.count_where(&path, "gt:value:2", "2");
        assert_filtered_count(&result, "3", completed);
        assert_eq!(field(&result, "data_read"), "true");
    }
    let pruned = worker.count_where(&path, "gt:value:99", "2");
    assert_filtered_count(&pruned, "0", "1");
    assert_eq!(field(&pruned, "data_read"), "false");
    assert_filtered_count(&worker.count_where(&path, "gt:value:99", "2"), "0", "2");
    assert_filtered_count(&worker.count_where(&path, "gt:value:99", "1"), "0", "1");
    assert_eq!(worker.raw_request("{invalid")["status"], "error");
    assert_filtered_count(&worker.count_where(&path, "gt:value:99", "1"), "0", "1");
    assert_eq!(
        worker.count_where(&path, "gt:missing:2", "1")["status"],
        "error"
    );
    assert_filtered_count(&worker.count_where(&path, "gt:value:99", "1"), "0", "1");
    assert_completed(&worker.collect(&path, "metric", "2"), "1");
    assert_filtered_count(&worker.count_where(&path, "gt:value:2", "2"), "3", "1");
}

#[test]
fn worker_invalidates_metadata_pruned_filtered_count_before_returning_zero() {
    let stamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let root = std::env::temp_dir().join(format!(
        "shardloom-worker-filtered-count-{}-{stamp}",
        std::process::id()
    ));
    std::fs::create_dir(&root).unwrap();
    let source = root.join("input.vortex");
    std::fs::copy(fixture(), &source).unwrap();
    let mut worker = Worker::new();
    assert_filtered_count(&worker.count_where(&source, "gt:value:99", "2"), "0", "1");
    let replacement = root.join("replacement.vortex");
    std::fs::copy(fixture(), &replacement).unwrap();
    std::fs::rename(replacement, &source).unwrap();
    let invalidated = worker.count_where(&source, "gt:value:99", "2");
    assert_eq!(invalidated["status"], "error", "{invalidated}");
    assert!(invalidated.to_string().contains("prepared source changed"));
    assert_filtered_count(&worker.count_where(&source, "gt:value:99", "2"), "0", "1");
    drop(worker);
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn worker_reuses_prepared_native_execution_and_rebinds_changed_requests() {
    let path = fixture();
    let mut worker = Worker::new();
    let first = worker.collect(&path, "metric", "2");
    assert_completed(&first, "1");
    let second = worker.collect(&path, "metric", "2");
    assert_completed(&second, "2");
    assert_eq!(
        complete_result::rows(&first),
        complete_result::rows(&second)
    );
    assert_ne!(complete_result::rows(&first), [] as [serde_json::Value; 0]);
    let changed = worker.collect(&path, "metric", "3");
    assert_completed(&changed, "1");
    assert_eq!(
        complete_result::rows(&first),
        complete_result::rows(&changed)
    );
    assert_eq!(worker.request(&["--version"])["status"], "success");
    assert_completed(&worker.collect(&path, "metric", "3"), "1");
}

#[test]
fn worker_rejects_replaced_source_then_reprepares_without_stale_answers() {
    let stamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let root =
        std::env::temp_dir().join(format!("shardloom-worker-{}-{stamp}", std::process::id()));
    std::fs::create_dir(&root).unwrap();
    let source = root.join("input.vortex");
    std::fs::copy(fixture(), &source).unwrap();
    let mut worker = Worker::new();
    assert_completed(&worker.collect(&source, "metric", "2"), "1");
    let replacement = root.join("replacement.vortex");
    std::fs::copy(fixture(), &replacement).unwrap();
    std::fs::rename(replacement, &source).unwrap();
    let invalidated = worker.collect(&source, "metric", "2");
    assert_eq!(invalidated["status"], "error", "{invalidated}");
    assert!(
        invalidated.to_string().contains("prepared source changed"),
        "{invalidated}"
    );
    assert_completed(&worker.collect(&source, "metric", "2"), "1");
    drop(worker);
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn worker_releases_retained_source_before_binding_error_or_directory_dispatch() {
    let stamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let root = std::env::temp_dir().join(format!(
        "shardloom-worker-binding-{}-{stamp}",
        std::process::id()
    ));
    std::fs::create_dir(&root).unwrap();
    let source = root.join("input.vortex");
    std::fs::copy(fixture(), &source).unwrap();
    let mut worker = Worker::new();
    for with_part in [false, true] {
        assert_completed(&worker.collect(&source, "metric", "2"), "1");
        std::fs::remove_file(&source).unwrap();
        std::fs::create_dir(&source).unwrap();
        if with_part {
            std::fs::copy(fixture(), source.join("part.vortex")).unwrap();
        }
        let nonresident = worker.collect(&source, "metric", "2");
        if with_part {
            assert_eq!(nonresident["status"], "success", "{nonresident}");
        } else {
            assert_eq!(nonresident["status"], "error");
        }
        std::fs::remove_dir_all(&source).unwrap();
        std::fs::copy(fixture(), &source).unwrap();
        assert_completed(&worker.collect(&source, "metric", "2"), "1");
        assert_eq!(worker.request(&["--version"])["status"], "success");
    }
    drop(worker);
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn worker_collects_fresh_typed_memory_without_durable_publication() {
    let mut worker = Worker::new();
    let binding = serde_json::json!({"memory://inline":{"input_format":"memory","memory_input":{"kind":"rows","schema":[["id","int64"],["label","utf8"]],"rows":[["9223372036854775807","λ"],["-9223372036854775808","hello"]]}}}).to_string();
    let args = [
        "run",
        "dataframe",
        "--source-bindings",
        &binding,
        "--sql",
        "SELECT id,label FROM 'memory://inline'",
        "--request",
        "collect",
        "--bounded",
        "true",
        "--execution-policy",
        "native_vortex",
        "--memory-gb",
        "1",
        "--max-parallelism",
        "2",
    ];
    for executions in ["1", "2"] {
        let result = worker.request(&args);
        assert_eq!(result["status"], "success", "{result}");
        assert_eq!(field(&result, "output_io_performed"), "false");
        assert_eq!(field(&result, "source_io_performed"), "false");
        assert_eq!(field(&result, "result_payload_complete"), "true");
        assert_eq!(field(&result, "resident_source_opens"), "0");
        assert_eq!(field(&result, "resident_completed_executions"), executions);
        assert_eq!(field(&result, "output_row_count"), "2");
        assert_eq!(
            field(&result, "public_workflow_fallback_attempted"),
            "false"
        );
        assert_eq!(
            complete_result::rows(&result),
            vec![
                json!({"id": i64::MAX, "label": "λ"}),
                json!({"id": i64::MIN, "label": "hello"})
            ]
        );
    }
    let changed_binding = serde_json::json!({"memory://inline":{"input_format":"memory","memory_input":{"kind":"rows","schema":[["id","int64"],["label","utf8"]],"rows":[["17","fresh"]]}}}).to_string();
    let changed = [
        "run",
        "dataframe",
        "--source-bindings",
        &changed_binding,
        "--sql",
        "SELECT id,label FROM 'memory://inline'",
        "--request",
        "collect",
        "--bounded",
        "true",
        "--execution-policy",
        "native_vortex",
        "--memory-gb",
        "1",
        "--max-parallelism",
        "2",
    ];
    let result = worker.request(&changed);
    assert_eq!(result["status"], "success", "{result}");
    assert_eq!(field(&result, "resident_completed_executions"), "1");
    assert_eq!(
        field(&result, "resident_relational_declaration_reused"),
        "false"
    );
    assert_eq!(
        complete_result::rows(&result),
        vec![json!({"id": 17, "label": "fresh"})]
    );
    for extra in [
        vec!["--vortex-predicate", "gt:id:0"],
        vec!["--materialization-policy", "zero_decode"],
        vec![
            "--output",
            "/tmp/shardloom-unrequested-memory-output.vortex",
        ],
        vec!["--sql", "SELECT COUNT(*)"],
        vec!["--vortex-source-order-limit", "1"],
    ] {
        let mut invalid = args.to_vec();
        invalid.extend(extra.iter().copied());
        assert_ne!(worker.request(&invalid)["status"], "success", "{extra:?}");
    }
}

#[test]
fn worker_rebinds_file_collections_after_directory_and_manifest_changes() {
    let stamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let root = std::env::temp_dir().join(format!(
        "shardloom-worker-parts-{}-{stamp}",
        std::process::id()
    ));
    std::fs::create_dir(&root).unwrap();
    std::fs::copy(fixture(), root.join("part-1.vortex")).unwrap();
    let manifest = root.join("parts.vortex-manifest");
    let mut worker = Worker::new();
    for manifest_input in [false, true] {
        let path = if manifest_input { &manifest } else { &root };
        for (parts, count) in [(1, 5), (2, 10), (1, 5)] {
            let second = root.join("part-2.vortex");
            if parts == 2 {
                std::fs::copy(fixture(), &second).unwrap();
            } else if second.exists() {
                std::fs::remove_file(second).unwrap();
            }
            let paths = if parts == 1 {
                json!(["part-1.vortex"])
            } else {
                json!(["part-1.vortex", "part-2.vortex"])
            };
            std::fs::write(&manifest, json!({"paths": paths}).to_string()).unwrap();
            let sql = format!(
                "SELECT COUNT(*) AS n FROM (SELECT metric FROM '{}') AS all_parts",
                path.display()
            );
            let report = worker.request(&[
                "run",
                "sql",
                "--input",
                path.to_str().unwrap(),
                "--input-format",
                "vortex",
                "--sql",
                &sql,
                "--request",
                "collect",
                "--bounded",
                "true",
                "--memory-gb",
                "1",
                "--max-parallelism",
                "2",
            ]);
            assert_eq!(complete_result::rows(&report), vec![json!({"n": count})]);
            assert_eq!(
                field(&report, "resident_relational_declaration_reused"),
                "false"
            );
        }
    }
    drop(worker);
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn worker_executes_resident_footer_count_each_call_and_releases_changed_handles() {
    let path = fixture();
    let mut worker = Worker::new();
    for (executions, opened) in [("1", "true"), ("2", "false"), ("3", "false")] {
        let result = worker.count(&path, "2");
        assert_metadata_count_certificate(&result, "5", executions, opened);
        assert_eq!(result["status"], "success", "{result}");
        assert_eq!(field(&result, "count"), "5");
        assert_eq!(field(&result, "resident_source_opens"), "1");
        assert_eq!(field(&result, "resident_completed_executions"), executions);
        assert_eq!(
            field(&result, "resident_footer_open_performed_this_call"),
            opened
        );
        assert_eq!(field(&result, "data_read"), "false");
        assert_eq!(field(&result, "data_decoded"), "false");
        assert_eq!(field(&result, "row_read"), "false");
        assert_eq!(field(&result, "local_primitive_report_present"), "false");
        assert_eq!(
            field(&result, "public_workflow_fallback_attempted"),
            "false"
        );
        assert_eq!(result["human_text"], "result summary: 5\n");
    }
    let changed = worker.count(&path, "3");
    assert_metadata_count_certificate(&changed, "5", "1", "true");
    assert_eq!(field(&changed, "resident_completed_executions"), "1");
    assert_completed(&worker.collect(&path, "metric", "3"), "1");
    assert_eq!(
        field(&worker.count(&path, "3"), "resident_completed_executions"),
        "1"
    );
}

fn assert_metadata_count_certificate(result: &Value, count: &str, executions: &str, opened: &str) {
    assert_eq!(result["status"], "success", "{result}");
    assert_eq!(field(result, "count"), count);
    assert_count_rows(result, count);
    for (key, expected) in [
        ("local_primitive_native_io_certificate_emitted", "true"),
        ("local_primitive_native_io_certificate_status", "certified"),
        ("local_primitive_native_io_certified", "true"),
        (
            "local_primitive_native_io_certificate_path_id",
            "native_vortex_source_to_scalar_count_result",
        ),
        ("local_primitive_native_io_source_kind", "vortex"),
        (
            "local_primitive_native_io_pushdown_accepted_operations",
            "count_all",
        ),
        (
            "local_primitive_native_io_pushdown_guarantee",
            "exact_retained_footer_row_count",
        ),
        (
            "local_primitive_native_io_representation_transitions",
            "metadata_only->metadata_only",
        ),
        ("local_primitive_native_io_materialization_boundaries", ""),
        (
            "local_primitive_native_io_sink_target_format",
            "scalar_count_result",
        ),
        ("local_primitive_native_io_sink_requires_rows", "false"),
        (
            "local_primitive_native_io_sink_requires_decoded_columnar",
            "false",
        ),
        (
            "local_primitive_native_io_adapter_materialization_required",
            "false",
        ),
        ("local_primitive_no_query_answer_cache", "true"),
        ("local_primitive_execution_certificate_emitted", "false"),
    ] {
        assert_eq!(field(result, key), expected, "{key}");
    }
    for effect in [
        "data_read",
        "data_decoded",
        "data_materialized",
        "row_read",
        "arrow_converted",
        "object_store_io",
        "write_io",
        "spill_io_performed",
        "fallback_attempted",
        "fallback_execution_allowed",
    ] {
        assert_eq!(
            field(result, &format!("local_primitive_native_io_{effect}")),
            "false"
        );
    }
    assert_metadata_count_proof(result, count, executions, opened);
}

fn assert_count_rows(result: &Value, count: &str) {
    assert_eq!(
        complete_result::rows(result),
        vec![json!({"count_all": count.parse::<u64>().unwrap()})]
    );
    assert_eq!(
        field(result, "result_schema_format"),
        "vortex.dtype.serde.v1"
    );
    let schema: Value = serde_json::from_str(field(result, "result_schema_json")).unwrap();
    assert_eq!(
        schema,
        json!({"Struct": [{"names": ["count_all"], "dtypes": [{"Primitive": ["u64", false]}]}, false]})
    );
    assert_eq!(field(result, "output_row_count"), "1");
}

fn assert_metadata_count_proof(result: &Value, count: &str, executions: &str, opened: &str) {
    let proof = field(result, "resident_native_io_proof_basis");
    for expected in [
        format!(
            "source={};",
            field(result, "native_vortex_input_binding_sources")
        ),
        format!("vortex {};", field(result, "resident_provider_version")),
        format!("row_count={count};"),
        format!("completed_executions={executions};"),
        format!("footer_open_performed_this_call={opened};"),
        "feature=vortex-local-primitives,unix;".into(),
        "source_generation_validation=before_and_after_native_footer_count;".into(),
        "no_query_answer_cache=true;".into(),
    ] {
        assert!(proof.contains(&expected), "{proof}: missing {expected}");
    }
}

#[test]
fn worker_footer_count_rejects_source_replacement_then_reads_new_actual_count() {
    let stamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let root = std::env::temp_dir().join(format!(
        "shardloom-count-worker-{}-{stamp}",
        std::process::id()
    ));
    std::fs::create_dir(&root).unwrap();
    let source = root.join("source.vortex");
    std::fs::copy(fixture(), &source).unwrap();
    let mut worker = Worker::new();
    assert_metadata_count_certificate(&worker.count(&source, "2"), "5", "1", "true");
    let replacement = root.join("replacement.vortex");
    std::fs::copy(
        fixture().with_file_name("metadata_footer_u64_20000.vortex"),
        &replacement,
    )
    .unwrap();
    std::fs::rename(&replacement, &source).unwrap();
    let invalidated = worker.count(&source, "2");
    assert_eq!(invalidated["status"], "error", "{invalidated}");
    assert!(invalidated.to_string().contains("prepared source changed"));
    assert!(
        invalidated["fields"]
            .as_array()
            .unwrap()
            .iter()
            .all(|entry| {
                !matches!(
                    entry["key"].as_str(),
                    Some(
                        "local_primitive_native_io_certificate_emitted"
                            | "local_primitive_native_io_certified"
                    )
                ) || entry["value"] != "true"
            })
    );
    let rebound = worker.count(&source, "2");
    assert_eq!(rebound["status"], "success", "{rebound}");
    assert_eq!(field(&rebound, "count"), "20000");
    assert_eq!(field(&rebound, "resident_completed_executions"), "1");
    assert_metadata_count_certificate(&rebound, "20000", "1", "true");
    assert_metadata_count_certificate(&worker.count(&source, "2"), "20000", "2", "false");
    drop(worker);
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn worker_parse_failures_release_prepared_context_before_next_valid_request() {
    let path = fixture();
    let mut worker = Worker::new();
    for invalid in [
        json!({"args": ["run", "dataframe", "--unexpected-resident-argument"]}).to_string(),
        json!({"args": ["run", "dataframe", "--format"]}).to_string(),
        "{malformed JSON".into(),
    ] {
        assert_eq!(
            field(&worker.count(&path, "2"), "resident_completed_executions"),
            "1"
        );
        assert_eq!(
            field(&worker.count(&path, "2"), "resident_completed_executions"),
            "2"
        );
        let error = worker.raw_request(&invalid);
        assert_eq!(error["status"], "error", "{error}");
    }
    assert_eq!(
        field(&worker.count(&path, "2"), "resident_completed_executions"),
        "1"
    );
}

#[test]
fn worker_releases_memory_runtime_on_a_different_public_run_route() {
    let path = fixture();
    let binding = serde_json::json!({"memory://inline":{"input_format":"memory","memory_input":{"kind":"rows","schema":[["id","int64"]],"rows":[["42"]]}}}).to_string();
    let memory = [
        "run",
        "dataframe",
        "--source-bindings",
        &binding,
        "--sql",
        "SELECT id FROM 'memory://inline'",
        "--request",
        "collect",
        "--bounded",
        "true",
    ];
    let mut worker = Worker::new();
    assert_eq!(
        field(&worker.request(&memory), "resident_completed_executions"),
        "1"
    );
    assert_eq!(
        field(&worker.request(&memory), "resident_completed_executions"),
        "2"
    );
    let profile = worker.request(&[
        "run",
        "dataframe",
        "--input",
        path.to_str().unwrap(),
        "--input-format",
        "vortex",
        "--native-vortex-operation-family",
        "profile",
        "--request",
        "collect",
        "--bounded",
        "true",
    ]);
    // Profile is an unsupported native operation here. Its failed admission must
    // still release the preceding declared-memory runtime.
    assert_eq!(profile["status"], "unsupported", "{profile}");
    assert_eq!(
        field(&worker.request(&memory), "resident_completed_executions"),
        "1"
    );
}
