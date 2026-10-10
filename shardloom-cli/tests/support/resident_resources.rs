//! Allocation errors must not reuse a previous request's permission or state.

use super::*;

fn assert_resource_evidence(result: &Value, executions: &str) {
    assert_eq!(result["status"], "success", "{result}");
    for (name, value) in [
        ("execution_resource_declared_memory_bytes", "1500000001"),
        ("execution_resource_declared_max_parallelism", "3"),
        ("execution_resource_memory_origin", "platform"),
        ("execution_resource_parallelism_origin", "context"),
        ("execution_resource_admission_status", "admitted"),
        ("execution_resource_admitted_memory_bytes", "1500000001"),
        (
            "execution_resource_observed_peak_active_lanes",
            "unavailable",
        ),
        (
            "execution_resource_whole_process_memory_limit_enforced",
            "false",
        ),
        ("execution_resource_observed_spill_io_performed", "false"),
        ("execution_resource_observed_spill_bytes", "0"),
        ("execution_resource_spill_observation_scope", "no_spill_io"),
        ("resident_completed_executions", executions),
    ] {
        assert_eq!(field(result, name), value, "{name}: {result}");
    }
    let admitted: usize = field(result, "execution_resource_admitted_max_parallelism")
        .parse()
        .unwrap();
    assert!((1..=3).contains(&admitted));
    let live: u64 = field(result, "execution_resource_observed_native_reserved_bytes")
        .parse()
        .unwrap();
    let peak: u64 = field(
        result,
        "execution_resource_observed_native_peak_reserved_bytes",
    )
    .parse()
    .unwrap();
    assert!(live <= peak && peak <= 1_500_000_001);
    assert_eq!(
        peak.to_string(),
        field(result, "resident_peak_reserved_buffer_bytes")
    );
    assert!(
        field(result, "execution_resource_memory_observation_scope")
            .contains("session_pool_lifetime")
    );
    assert_eq!(result["fallback"]["attempted"], false);
}

#[test]
fn worker_reports_exact_declared_and_admitted_resources_with_actual_session_usage() {
    let path = fixture();
    let mut worker = Worker::new();
    let aggregate = r#"{"measures":[{"function":"sum","column":"metric","alias":"total"}]}"#;
    let cases: &[(&str, &[&str], Value)] = &[
        ("count", &[], json!([{"count_all":5}])),
        (
            "count_where",
            &["--vortex-predicate", "gt:value:3"],
            json!([{"count_all":2}]),
        ),
        (
            "tail",
            &[
                "--vortex-columns",
                "value",
                "--vortex-source-order-limit",
                "2",
            ],
            json!([{"value":4},{"value":5}]),
        ),
        (
            "aggregate",
            &["--vortex-aggregate", aggregate],
            json!([{"total":150.0}]),
        ),
    ];
    for (primitive, extra, expected) in cases {
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
            "--memory-bytes",
            "1500000001",
            "--max-parallelism",
            "3",
            "--memory-origin",
            "platform",
            "--parallelism-origin",
            "context",
        ];
        args.extend_from_slice(extra);
        for executions in ["1", "2"] {
            let result = worker.request(&args);
            assert_resource_evidence(&result, executions);
            let rows: Vec<Value> = field(&result, "result_jsonl")
                .lines()
                .map(|line| serde_json::from_str(line).unwrap())
                .collect();
            assert_eq!(Value::Array(rows), *expected, "{primitive}");
        }
    }
}

#[test]
fn worker_resource_rejection_precedes_source_and_output_and_allows_clean_reuse() {
    let nonce = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let absent = std::env::temp_dir().join(format!(
        "shardloom-worker-resource-{}-{nonce}",
        std::process::id()
    ));
    let missing_source = absent.join("source.vortex");
    let forbidden_output = absent.join("output.jsonl");
    let path = fixture();
    let mut worker = Worker::new();
    let invalid: &[&[&str]] = &[
        &[],
        &["--memory-gb", "1"],
        &["--max-parallelism", "1"],
        &["--memory-bytes", "0", "--max-parallelism", "1"],
        &["--memory-gb", "1", "--max-parallelism", "eight"],
        &[
            "--memory-gb",
            "18446744073709551615",
            "--max-parallelism",
            "1",
        ],
        &[
            "--memory-bytes",
            "100",
            "--max-parallelism",
            "2",
            "--parallelism-limit",
            "1",
        ],
        &[
            "--memory-gb",
            "1",
            "--max-parallelism",
            "1",
            "--max-parallelism",
            "2",
        ],
    ];
    for invalid in invalid {
        assert_eq!(
            field(&worker.count(&path, "2"), "resident_completed_executions"),
            "1"
        );
        assert_eq!(
            field(&worker.count(&path, "2"), "resident_completed_executions"),
            "2"
        );
        let mut args = vec![
            "run",
            "dataframe",
            "--input",
            missing_source.to_str().unwrap(),
            "--input-format",
            "vortex",
            "--request",
            "write_jsonl",
            "--output",
            forbidden_output.to_str().unwrap(),
            "--vortex-primitive",
            "project",
            "--vortex-columns",
            "value",
        ];
        args.extend_from_slice(invalid);
        let rejected = worker.request(&args);
        assert_eq!(rejected["status"], "error", "{invalid:?}: {rejected}");
        assert!(
            rejected["diagnostics"]
                .as_array()
                .unwrap()
                .iter()
                .any(|entry| { entry["code"] == "SL_CONFIGURATION_ERROR" }),
            "{invalid:?}: {rejected}"
        );
        assert_eq!(rejected["fallback"]["attempted"], false);
        assert!(
            !absent.exists(),
            "resource rejection created an output directory"
        );
    }
    assert_eq!(
        field(&worker.count(&path, "2"), "resident_completed_executions"),
        "1"
    );
}
