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
    let mut policy = VortexLocalPrimitiveExecutionPolicy::new(requested).unwrap();
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
    assert!(!aggregate_count_workers::request_may_be_admitted(&request));
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
    assert!(aggregate_count_workers::request_may_be_admitted(&request));
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
    assert!(aggregate_count_workers::request_may_be_admitted(&request));
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
