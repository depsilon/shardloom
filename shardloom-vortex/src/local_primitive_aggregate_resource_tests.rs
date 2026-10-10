//! Ordinary aggregate execution must respect the CPU grant of its held source.

use super::*;
use crate::VortexAggregateOrderExpr;

fn fixture() -> std::path::PathBuf {
    std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/local_primitive_struct_five.vortex")
}

fn request(aggregate: VortexSimpleAggregateRequest) -> VortexQueryPrimitiveRequest {
    VortexQueryPrimitiveRequest::simple_aggregate(
        DatasetUri::new(fixture().display().to_string()).unwrap(),
        aggregate,
    )
}

fn measure(function: &str, column: Option<&str>, alias: &str) -> VortexSimpleAggregateMeasure {
    VortexSimpleAggregateMeasure::new(
        function,
        column.map(|name| ColumnRef::new(name).unwrap()),
        alias.to_owned(),
    )
}

fn execute(
    request: &VortexQueryPrimitiveRequest,
    requested: usize,
) -> (LocalVortexAggregateScan, serde_json::Value) {
    let mut policy = VortexLocalPrimitiveExecutionPolicy::new_with_memory_gb(requested, 4).unwrap();
    policy.resource_envelope.memory_budget_bytes = 16 << 20;
    let scan = read_local_vortex_simple_aggregate_scan(
        request.source_uri.as_ref().unwrap(),
        &fixture(),
        request,
        policy,
    )
    .unwrap();
    let grant = requested.min(std::thread::available_parallelism().unwrap().get());
    assert_eq!(scan.scan.max_parallelism_requested, grant);
    assert_eq!(scan.scan.resource_envelope.max_parallelism, grant);
    assert_eq!(scan.scan.scan_concurrency_per_worker, grant);
    assert_eq!(
        scan.scan.resource_envelope.scan_concurrency_per_worker,
        grant
    );
    let report = simple_aggregate_report(request, &scan).unwrap();
    assert!(!report.has_errors());
    assert!(!report.fallback_execution_allowed);
    assert!(
        local_primitive_native_io_certificate(request, &report)
            .unwrap()
            .is_certified()
    );
    let payload = serde_json::from_str(&scan.result_summary).unwrap();
    (scan, payload)
}

#[test]
fn ordinary_aggregate_caps_source_scan_grant_without_changing_scalar_values() {
    let request = request(VortexSimpleAggregateRequest::new(vec![
        measure("count", None, "rows_alias"),
        measure("min", Some("metric"), "min_alias"),
        measure("max", Some("metric"), "max_alias"),
    ]));
    assert!(!aggregate_count_workers::request_may_be_admitted(
        &request,
        crate::VortexLocalPrimitiveResourceEnvelope::new(4, 1)
            .expect("explicit fixture allocation")
    ));
    let available = std::thread::available_parallelism().unwrap().get();
    for requested in [1, available + 2] {
        let (scan, payload) = execute(&request, requested);
        assert_eq!(scan.scan.pre_limit_result_row_count, 5);
        assert_eq!(
            payload["values"],
            serde_json::json!({"rows_alias":5,"min_alias":10,"max_alias":50})
        );
        assert!(payload.get("aggregate_workers_submitted_chunks").is_none());
    }
}

#[test]
fn ordinary_aggregate_source_cap_preserves_exact_distinct_worker_admission() {
    let request = request(
        VortexSimpleAggregateRequest::grouped(
            vec![ColumnRef::new("metric").unwrap()],
            vec![measure("count_distinct", Some("value"), "distinct_alias")],
        )
        .with_order_by(vec![VortexAggregateOrderExpr::new("distinct_alias", true)]),
    )
    .with_source_order_limit(2);
    assert!(aggregate_count_workers::request_may_be_admitted(
        &request,
        crate::VortexLocalPrimitiveResourceEnvelope::new(4, 1)
            .expect("explicit fixture allocation")
    ));
    let available = std::thread::available_parallelism().unwrap().get();
    for requested in [1, available + 2] {
        let (_, payload) = execute(&request, requested);
        assert_eq!(
            payload["values"],
            serde_json::json!([
                {"metric":10,"distinct_alias":1},
                {"metric":20,"distinct_alias":1},
            ])
        );
        assert_eq!(
            payload["aggregate_update_strategy"],
            "complete_integer_pair_partition_distinct"
        );
        assert_eq!(payload["aggregate_workers_provider_background_workers"], 0);
        assert!(
            payload["aggregate_workers_submitted_chunks"]
                .as_u64()
                .unwrap()
                > 0
        );
    }
}

#[test]
fn ordinary_aggregate_restored_driver_count_matches_typed_and_json_evidence() {
    // Both keys are integers; the compound COUNT worker requires a UTF8 key.
    let request = request(VortexSimpleAggregateRequest::grouped(
        vec![
            ColumnRef::new("value").unwrap(),
            ColumnRef::new("metric").unwrap(),
        ],
        vec![measure("count", None, "count_alias")],
    ));
    assert!(aggregate_count_workers::request_may_be_admitted(
        &request,
        crate::VortexLocalPrimitiveResourceEnvelope::new(4, 1)
            .expect("explicit fixture allocation")
    ));
    let available = std::thread::available_parallelism().unwrap().get();
    for requested in [1, available + 2] {
        let (scan, payload) = execute(&request, requested);
        let drivers = requested.min(available) - 1;
        assert_eq!(scan.restored_provider_background_workers, drivers);
        assert_eq!(payload["aggregate_provider_background_workers"], drivers);
        assert!(payload.get("aggregate_workers_submitted_chunks").is_none());
        let rows = payload["values"].as_array().unwrap();
        assert_eq!(rows.len(), 5);
        for row in rows {
            let key = row["value"].as_u64().unwrap();
            assert!((1..=5).contains(&key));
            assert_eq!(row["metric"], key * 10);
            assert_eq!(row["count_alias"], 1);
        }
    }
}

fn byte_policy(bytes: u64) -> VortexLocalPrimitiveExecutionPolicy {
    VortexLocalPrimitiveExecutionPolicy::from_resources(
        shardloom_core::ExecutionResources::from_bytes(
            bytes,
            1,
            shardloom_core::ExecutionResourceOrigin::ExecutionCall,
        )
        .unwrap(),
    )
    .unwrap()
}

#[test]
fn exact_byte_aggregate_policy_never_restores_a_large_candidate_floor() {
    let count = measure("count", None, "rows");
    let distinct = measure("count_distinct", Some("UserID"), "rows");
    let cases = [
        (vec!["URL"], count.clone(), "string_heavy_hitter_topk"),
        (
            vec!["SearchPhrase"],
            distinct,
            "string_count_distinct_heavy_hitter_topk",
        ),
        (
            vec!["SearchEngineID", "SearchPhrase"],
            count,
            "numeric_utf8_heavy_hitter_topk",
        ),
    ];
    for bytes in [
        1,
        127,
        128,
        1024,
        65_535,
        4 << 20,
        (8 << 20) - 1,
        8 << 20,
        4 << 30,
    ] {
        let policy = byte_policy(bytes);
        let items = usize::try_from(bytes / 128).unwrap();
        assert_eq!(policy.resource_envelope.group_state_soft_item_budget, items);
        for (keys, measure, family) in &cases {
            let aggregate = VortexSimpleAggregateRequest::grouped(
                keys.iter()
                    .map(|key| ColumnRef::new(*key).unwrap())
                    .collect(),
                vec![measure.clone()],
            )
            .with_order_by(vec![VortexAggregateOrderExpr::new("rows", true)]);
            let request = request(aggregate).with_source_order_limit(10);
            for writer in [false, true] {
                let (effective, report) = if writer {
                    policy.with_writer_sink_physical_policy_for_request(&request)
                } else {
                    policy.with_physical_policy_for_request(&request)
                };
                assert_eq!(report.route_family, *family);
                assert!(
                    effective
                        .resource_envelope
                        .string_topk_heavy_hitter_capacity
                        <= items
                );
                assert!(
                    effective
                        .resource_envelope
                        .numeric_utf8_topk_heavy_hitter_capacity
                        <= items
                );
                assert_eq!(report.selected_group_state_soft_item_budget, items);
                assert_eq!(effective.resource_envelope.memory_budget_bytes, bytes);
            }
        }
    }
}

#[test]
fn exact_byte_aggregate_caches_and_mirrors_keep_the_small_grant() {
    let aggregate = VortexSimpleAggregateRequest::grouped(
        vec![ColumnRef::new("value").unwrap()],
        vec![measure("count", None, "rows")],
    );
    let columns = vec!["value".to_owned()];
    for bytes in [128, 1024, 16_384, 1 << 20] {
        let envelope = byte_policy(bytes).resource_envelope;
        let states = GroupedAggregateStates::new_with_resource_envelope(
            &aggregate,
            Some(1),
            &columns,
            false,
            true,
            envelope,
        )
        .unwrap();
        let items = usize::try_from(bytes / 128).unwrap();
        assert!(states.transformed_dictionary_key_cache_cap() <= items);
        assert!(states.string_count_topk_exact_mirror_capacity() <= items);
        assert!(states.numeric_utf8_topk_exact_mirror_capacity() <= items);
        assert!(
            states
                .string_count_distinct_topk_first_pass_exact_set_entry_budget()
                .unwrap()
                <= bytes / 128 / 4
        );
    }
}

#[test]
fn exact_byte_grouped_aggregate_refuses_before_opening_or_allocating_state() {
    let missing = fixture().with_file_name("resource-admission-must-not-open.vortex");
    assert!(!missing.exists());
    let aggregate = VortexSimpleAggregateRequest::grouped(
        vec![ColumnRef::new("value").unwrap()],
        vec![measure("count", None, "rows")],
    );
    let request = VortexQueryPrimitiveRequest::simple_aggregate(
        DatasetUri::new(missing.display().to_string()).unwrap(),
        aggregate.clone(),
    );
    let columns = vec!["value".to_owned()];
    for bytes in [1, 127] {
        let policy = byte_policy(bytes);
        let ordinary = read_local_vortex_simple_aggregate_scan(
            request.source_uri.as_ref().unwrap(),
            &missing,
            &request,
            policy,
        )
        .err()
        .expect("insufficient state grant");
        let prepared = prepared_aggregate::prepare_aggregate(&request, policy)
            .err()
            .expect("insufficient prepared state grant");
        let state = GroupedAggregateStates::new_with_resource_envelope(
            &aggregate,
            Some(1),
            &columns,
            false,
            true,
            policy.resource_envelope,
        )
        .err()
        .expect("insufficient direct state grant");
        for error in [ordinary, prepared, state] {
            assert!(
                error
                    .to_string()
                    .contains("grouped aggregate state admission requires at least 128 bytes"),
                "{error}"
            );
            assert!(error.to_string().contains(&format!("memory_bytes={bytes}")));
            assert!(!error.to_diagnostic().fallback.attempted);
        }
    }
}

#[test]
fn exact_byte_grouped_aggregate_preserves_every_value_with_an_admitted_grant() {
    let request = request(
        VortexSimpleAggregateRequest::grouped(
            vec![ColumnRef::new("value").unwrap()],
            vec![measure("count", None, "rows")],
        )
        .with_order_by(vec![VortexAggregateOrderExpr::new("value", false)]),
    );
    let policy = byte_policy(2 << 20);
    let scan = read_local_vortex_simple_aggregate_scan(
        request.source_uri.as_ref().unwrap(),
        &fixture(),
        &request,
        policy,
    )
    .unwrap();
    assert_eq!(
        scan.scan.resource_envelope.group_state_soft_item_budget,
        16_384
    );
    let payload: serde_json::Value = serde_json::from_str(&scan.result_summary).unwrap();
    assert_eq!(
        payload["values"],
        serde_json::json!([
            {"value":1,"rows":1},{"value":2,"rows":1},{"value":3,"rows":1},
            {"value":4,"rows":1},{"value":5,"rows":1},
        ])
    );
}

#[test]
fn aggregate_runtime_owner_preserves_nonaggregate_preparation() {
    let missing = fixture().with_file_name("shared-runtime-must-not-inspect.vortex");
    assert!(!missing.exists());
    let uri = DatasetUri::new(missing.display().to_string()).unwrap();
    for request in [
        VortexQueryPrimitiveRequest::count_all(uri.clone()),
        VortexQueryPrimitiveRequest::project(uri, ProjectionRequest::all()),
    ] {
        let session = prepared_aggregate::aggregate_session(&request, byte_policy(1 << 20))
            .expect("shared runtime setup must not require an aggregate payload");
        assert_eq!(session.snapshot().prepared_source_opens, 0);
        assert_eq!(session.snapshot().memory.reserved_bytes, 0);
    }
}
