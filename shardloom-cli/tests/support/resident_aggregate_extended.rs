use super::*;

#[test]
fn worker_retains_wide_and_derived_aggregates_through_complete_public_calls() {
    let path = fixture();
    let mut worker = Worker::new();
    let measures: Vec<_> = (0..90)
        .map(|offset| {
            json!({
                "function":"sum", "column":"metric", "alias":format!("total_{offset}"),
                "argument_offset":offset,
            })
        })
        .collect();
    let expected: serde_json::Map<_, _> = (0..90)
        .map(|offset| {
            (
                format!("total_{offset}"),
                json!(150.0 + 5.0 * f64::from(offset)),
            )
        })
        .collect();
    let aggregate = json!({"measures":measures}).to_string();
    for execution in 1..=3 {
        completed(
            &worker.aggregate(&path, &aggregate, None, None, "1", "2"),
            &Value::Object(expected.clone()),
            &execution.to_string(),
            execution == 1,
        );
    }
    let aggregate = json!({"group_by":["value"],
        "group_expressions":[{"column":"value","function":"add_offset","argument_offset":-1,"alias":"prior"}],
        "measures":[{"function":"count","alias":"frequency"}],
        "order_by":[{"column":"value","descending":true}]}).to_string();
    for execution in 1..=3 {
        completed(
            &worker.aggregate(&path, &aggregate, None, Some("2"), "1", "2"),
            &json!([{"value":5,"prior":4,"frequency":1},{"value":4,"prior":3,"frequency":1}]),
            &execution.to_string(),
            execution == 1,
        );
    }
}

#[cfg(feature = "vortex-write")]
#[test]
fn worker_retains_explicit_spill_source_and_rebinds_changed_resource_policy() {
    let root = std::env::temp_dir().join(format!(
        "shardloom-resident-spill-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::create_dir(&root).unwrap();
    let workspace = root.join("workspace");
    std::fs::create_dir(&workspace).unwrap();
    let source = root.join("source.vortex");
    std::fs::copy(fixture(), &source).unwrap();
    let mut worker = Worker::new();
    for memory in [4 << 20, 8 << 20] {
        let aggregate = json!({"group_by":["value"],
            "measures":[{"function":"count_distinct","column":"metric","alias":"frequency"}],
            "order_by":[{"column":"frequency","descending":true},{"column":"value","descending":false}],
            "offset":1, "spill":{"workspace":workspace,"quota_bytes":64 << 20,"memory_bytes":memory}}).to_string();
        for execution in 1..=3 {
            let result = worker.aggregate(&source, &aggregate, None, Some("2"), "1", "2");
            assert_eq!(result["status"], "success", "{result}");
            assert_eq!(
                values(&result),
                json!([
                    {"value":2,"frequency":1}, {"value":3,"frequency":1}
                ])
            );
            assert_eq!(field(&result, "resident_source_opens"), "1");
            assert_eq!(
                field(&result, "resident_completed_executions"),
                execution.to_string()
            );
            assert_eq!(field(&result, "resident_aggregate_handle_retained"), "true");
            assert_eq!(
                field(&result, "resident_footer_open_performed_this_call"),
                (execution == 1).to_string()
            );
            assert_eq!(
                field(&result, "resident_aggregate_lowering_reused"),
                "false"
            );
            assert_eq!(
                field(&result, "local_primitive_native_io_certified"),
                "true"
            );
            assert_eq!(
                field(&result, "local_primitive_no_query_answer_cache"),
                "true"
            );
            assert_eq!(
                field(&result, "public_workflow_external_engine_invoked"),
                "false"
            );
            assert_eq!(std::fs::read_dir(&workspace).unwrap().count(), 0);
        }
    }
    drop(worker);
    std::fs::remove_dir_all(root).unwrap();
}
