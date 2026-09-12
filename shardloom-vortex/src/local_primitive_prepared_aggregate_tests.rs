use super::*;
use crate::{
    VortexAggregateOrderExpr, VortexAggregateSpillPolicy, VortexSimpleAggregateMeasure,
    VortexSimpleAggregateRequest,
};
use shardloom_core::{ColumnRef, ComparisonOp, DatasetUri, PredicateExpr, StatValue};
use std::{
    path::{Path, PathBuf},
    sync::atomic::{AtomicUsize, Ordering},
};

static NEXT_FIXTURE: AtomicUsize = AtomicUsize::new(0);

struct Fixture(PathBuf);
impl Fixture {
    fn new() -> Self {
        let directory = std::env::temp_dir().join(format!(
            "shardloom-prepared-aggregate-{}-{}",
            std::process::id(),
            NEXT_FIXTURE.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir(&directory).unwrap();
        let fixture = Self(directory);
        std::fs::copy(Self::original(), fixture.path()).unwrap();
        fixture
    }

    fn original() -> PathBuf {
        Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("tests/fixtures/local_primitive_struct_five.vortex")
    }

    fn path(&self) -> PathBuf {
        self.0.join("source.vortex")
    }

    fn request(&self, aggregate: VortexSimpleAggregateRequest) -> VortexQueryPrimitiveRequest {
        VortexQueryPrimitiveRequest::simple_aggregate(
            DatasetUri::new(self.path().display().to_string()).unwrap(),
            aggregate,
        )
    }

    fn replace(&self) {
        let replacement = self.0.join("replacement.vortex");
        std::fs::copy(Self::original(), &replacement).unwrap();
        std::fs::rename(replacement, self.path()).unwrap();
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        std::fs::remove_dir_all(&self.0).unwrap();
    }
}

fn measure(function: &str, column: Option<&str>, alias: &str) -> VortexSimpleAggregateMeasure {
    VortexSimpleAggregateMeasure::new(
        function,
        column.map(|column| ColumnRef::new(column).unwrap()),
        alias.to_owned(),
    )
}

fn scalar() -> VortexSimpleAggregateRequest {
    VortexSimpleAggregateRequest::new(vec![
        measure("count", None, "rows_alias"),
        measure("count_distinct", Some("value"), "unique_alias"),
        measure("sum", Some("metric"), "total_alias"),
    ])
}

fn payload(report: &VortexLocalPrimitiveExecutionReport) -> serde_json::Value {
    serde_json::from_str(
        report
            .result_summary
            .as_deref()
            .unwrap()
            .rsplit_once(" values=")
            .unwrap()
            .1,
    )
    .unwrap()
}

fn certified(result: &ExecutedVortexAggregate, execution: u64) {
    assert!(!result.report.has_errors());
    assert!(result.native_io_certificate.is_certified());
    assert!(!result.native_io_certificate.fallback_attempted);
    assert!(!result.report.arrow_converted && !result.report.spill_io_performed);
    assert_eq!(result.runtime.prepared_source_opens, 1);
    assert_eq!(result.runtime.completed_executions, execution);
    let proof = &result
        .native_io_certificate
        .source_pushdown_report
        .proof_basis;
    for required in [
        "aggregate_lowering_reused=true",
        "aggregate_state_reused=false",
        "no_query_answer_cache=true",
        "independent_oracle_not_run",
    ] {
        assert!(proof.contains(required), "missing proof marker: {required}");
    }
}

#[test]
fn optional_preparation_is_nonexecuting_and_shape_declines_before_source_open() {
    let fixture = Fixture::new();
    let request = fixture.request(scalar());
    let Some(PreparedAggregateDisposition::Reusable(prepared)) =
        prepare_aggregate_for_optional_reuse(
            &request,
            VortexLocalPrimitiveExecutionPolicy::single_threaded(),
        )
        .unwrap()
    else {
        panic!("integer fixture is admitted");
    };
    assert_eq!(prepared.snapshot().prepared_source_opens, 1);
    assert_eq!(prepared.snapshot().completed_executions, 0);
    let mut unsupported = request;
    unsupported.source_uri =
        Some(DatasetUri::new(fixture.0.join("absent.vortex").display().to_string()).unwrap());
    unsupported.simple_aggregate.as_mut().unwrap().measures[0].value_transform =
        Some("unsupported".into());
    assert!(
        prepare_aggregate_for_optional_reuse(
            &unsupported,
            VortexLocalPrimitiveExecutionPolicy::single_threaded(),
        )
        .unwrap()
        .is_none()
    );
    assert_eq!(prepared.snapshot().completed_executions, 0);
}

#[test]
fn prepared_scalar_reexecutes_complete_values_and_retains_only_source_and_lowering() {
    let fixture = Fixture::new();
    for parallelism in [1, 2] {
        let session = ResidentVortexSession::new(16 << 20, parallelism).unwrap();
        let prepared = prepare_aggregate_in_session(
            &fixture.request(scalar()),
            VortexLocalPrimitiveExecutionPolicy::new(parallelism).unwrap(),
            &session,
        )
        .unwrap();
        assert_eq!(prepared.snapshot().completed_executions, 0);
        let mut retained_first_report = None;
        for execution in 1..=4 {
            let result = prepared.execute().unwrap();
            certified(&result, execution);
            // Checked file: value=1..=5 and metric=10,20,30,40,50.
            assert_eq!(
                payload(&result.report)["values"],
                serde_json::json!({
                    "rows_alias":5, "unique_alias":5, "total_alias":150.0,
                })
            );
            assert_eq!(result.report.rows_selected, Some(5));
            assert_eq!(result.report.rows_projected, Some(1));
            if retained_first_report.is_none() {
                retained_first_report = Some(result.report);
            }
        }
        drop(prepared);
        assert_eq!(session.snapshot().memory.reserved_bytes, 0);
        assert_eq!(
            payload(&retained_first_report.unwrap())["values"]["unique_alias"],
            5
        );
    }
}

#[test]
fn prepared_grouped_count_preserves_complete_global_order_offset_and_limit() {
    let fixture = Fixture::new();
    let aggregate = VortexSimpleAggregateRequest::grouped(
        vec![ColumnRef::new("value").unwrap()],
        vec![measure("count", None, "count_alias")],
    )
    .with_order_by(vec![VortexAggregateOrderExpr::new("value", false)])
    .with_offset(1);
    let request = fixture.request(aggregate).with_source_order_limit(2);
    for parallelism in [1, 2, 4] {
        let prepared = prepare_aggregate(
            &request,
            VortexLocalPrimitiveExecutionPolicy::new(parallelism).unwrap(),
        )
        .unwrap();
        assert_eq!(prepared.snapshot().provider_background_workers, 0);
        for execution in 1..=3 {
            let result = prepared.execute().unwrap();
            certified(&result, execution);
            assert_eq!(
                payload(&result.report)["values"],
                serde_json::json!([
                    {"value":2,"count_alias":1}, {"value":3,"count_alias":1},
                ])
            );
            assert_eq!(result.report.rows_selected, Some(5));
            assert_eq!(result.report.rows_projected, Some(2));
        }
    }
}

#[test]
fn prepared_admission_denial_reports_actual_drivers_and_releases_them_before_reuse() {
    use super::super::{
        aggregate_count_workers::ADMISSION_TEST_PRESSURE, bounded_local_vortex_worker_count,
        exact_distinct_pairs::workers::request_schema_may_be_admitted,
    };

    let fixture = Fixture::new();
    // Exact distinct needs a positive metadata reservation during admission.
    // Single numeric COUNT may reserve zero and remain admitted under pressure.
    let aggregate = VortexSimpleAggregateRequest::grouped(
        vec![ColumnRef::new("metric").unwrap()],
        vec![measure("count_distinct", Some("value"), "count_alias")],
    )
    .with_order_by(vec![VortexAggregateOrderExpr::new("count_alias", true)])
    .with_offset(1);
    let request = fixture.request(aggregate).with_source_order_limit(2);
    for parallelism in [1, 2, 4] {
        let prepared = prepare_aggregate(
            &request,
            VortexLocalPrimitiveExecutionPolicy::new(parallelism).unwrap(),
        )
        .unwrap();
        let session = prepared.session.clone();
        assert!(request_schema_may_be_admitted(
            &request,
            prepared.source.dtype()
        ));
        assert!(prepared.worker_pool);
        assert_eq!(prepared.snapshot().completed_executions, 0);
        for (index, pressure) in [false, true, false].into_iter().enumerate() {
            let denied_before = session.memory().snapshot().denied_reservations;
            ADMISSION_TEST_PRESSURE.with(|current| current.set(pressure));
            let result = prepared.execute().unwrap();
            certified(&result, u64::try_from(index + 1).unwrap());
            assert!(!ADMISSION_TEST_PRESSURE.with(std::cell::Cell::get));
            let summary = payload(&result.report);
            assert_eq!(
                summary["values"],
                serde_json::json!([
                    {"metric":20,"count_alias":1}, {"metric":30,"count_alias":1},
                ])
            );
            let expected_drivers = if pressure {
                bounded_local_vortex_worker_count(parallelism)
            } else {
                0
            };
            assert_eq!(result.runtime.provider_background_workers, expected_drivers);
            assert_eq!(prepared.snapshot().provider_background_workers, 0);
            assert_eq!(
                session.memory().snapshot().denied_reservations > denied_before,
                pressure,
            );
            if pressure {
                assert_eq!(
                    summary["aggregate_provider_background_workers"],
                    expected_drivers
                );
                assert!(
                    summary
                        .as_object()
                        .unwrap()
                        .keys()
                        .all(|key| !key.starts_with("aggregate_workers_"))
                );
            } else {
                assert_eq!(summary["aggregate_workers_provider_background_workers"], 0);
                assert!(
                    summary
                        .get("aggregate_workers_exact_distinct_scope")
                        .is_some()
                );
                assert!(
                    summary["aggregate_workers_submitted_chunks"]
                        .as_u64()
                        .unwrap()
                        > 0
                );
                assert!(
                    summary
                        .get("aggregate_provider_background_workers")
                        .is_none()
                );
            }
            assert!(
                summary
                    .get("aggregate_workers_partition_source_replays")
                    .is_none()
            );
        }
        drop(prepared);
        assert_eq!(session.memory().snapshot().reserved_bytes, 0);
    }
}

#[test]
fn supplied_provider_session_does_not_also_launch_an_aggregate_worker_pool() {
    let fixture = Fixture::new();
    let session = ResidentVortexSession::new(16 << 20, 2).unwrap();
    let request = fixture.request(VortexSimpleAggregateRequest::grouped(
        vec![ColumnRef::new("value").unwrap()],
        vec![measure("count", None, "count_alias")],
    ));
    let prepared = prepare_aggregate_in_session(
        &request,
        VortexLocalPrimitiveExecutionPolicy::new(2).unwrap(),
        &session,
    )
    .unwrap();
    assert!(!prepared.worker_pool && !prepared.temporary_provider_drivers);
    let result = prepared.execute().unwrap();
    certified(&result, 1);
    assert_eq!(result.runtime.provider_background_workers, 1);
    assert_eq!(
        payload(&result.report)["values"].as_array().unwrap().len(),
        5
    );
    drop(prepared);
    assert_eq!(session.snapshot().memory.reserved_bytes, 0);
}

#[test]
fn supplied_narrower_session_remains_the_aggregate_cpu_ceiling() {
    let fixture = Fixture::new();
    let session = ResidentVortexSession::new(16 << 20, 1).unwrap();
    let request = fixture
        .request(VortexSimpleAggregateRequest::grouped(
            vec![ColumnRef::new("value").unwrap()],
            vec![measure("count", None, "count_alias")],
        ))
        .with_source_order_limit(5);
    let prepared = prepare_aggregate_in_session(
        &request,
        VortexLocalPrimitiveExecutionPolicy::new(4).unwrap(),
        &session,
    )
    .unwrap();
    for execution in 1..=3 {
        let result = prepared.execute().unwrap();
        certified(&result, execution);
        assert_eq!(result.report.resource_envelope.max_parallelism, 1);
        assert_eq!(result.report.physical_policy.requested_max_parallelism, 4);
        assert_eq!(result.report.physical_policy.selected_max_parallelism, 1);
        let values = payload(&result.report)["values"]
            .as_array()
            .unwrap()
            .clone();
        assert_eq!(values.len(), 5);
        for value in values {
            assert!((1..=5).contains(&value["value"].as_u64().unwrap()));
            assert_eq!(value["count_alias"], 1);
        }
    }
    drop(prepared);
    assert_eq!(session.snapshot().memory.reserved_bytes, 0);
}

#[test]
fn unsupported_two_key_worker_shape_restores_provider_lanes_on_the_same_source() {
    let fixture = Fixture::new();
    let request = fixture.request(VortexSimpleAggregateRequest::grouped(
        vec![
            ColumnRef::new("value").unwrap(),
            ColumnRef::new("metric").unwrap(),
        ],
        vec![measure("count", None, "count_alias")],
    ));
    let prepared = prepare_aggregate(
        &request,
        VortexLocalPrimitiveExecutionPolicy::new(2).unwrap(),
    )
    .unwrap();
    // The compound worker family requires one text key; both fields here are
    // integer. Generic native aggregation must retain its provider progress.
    assert!(!prepared.worker_pool && prepared.temporary_provider_drivers);
    for execution in 1..=3 {
        let result = prepared.execute().unwrap();
        certified(&result, execution);
        let actual = payload(&result.report);
        assert_eq!(actual["aggregate_provider_background_workers"], 1);
        let values = actual["values"].as_array().unwrap();
        assert_eq!(values.len(), 5);
        for value in values {
            let key = value["value"].as_u64().unwrap();
            assert!((1..=5).contains(&key));
            assert_eq!(value["metric"], key * 10);
            assert_eq!(value["count_alias"], 1);
        }
    }
}

#[test]
fn prepared_pruned_and_unprunable_empty_results_are_fresh_certified_and_invalidated() {
    let fixture = Fixture::new();
    for value in [17, 99] {
        let session = ResidentVortexSession::new(16 << 20, 1).unwrap();
        let mut request = fixture.request(VortexSimpleAggregateRequest::new(vec![measure(
            "count",
            None,
            "count_alias",
        )]));
        request.predicate = Some(PredicateExpr::Compare {
            column: ColumnRef::new("metric").unwrap(),
            op: ComparisonOp::Eq,
            value: StatValue::Int64(value),
        });
        let prepared = prepare_aggregate_in_session(
            &request,
            VortexLocalPrimitiveExecutionPolicy::single_threaded(),
            &session,
        )
        .unwrap();
        for execution in 1..=3 {
            let result = prepared.execute().unwrap();
            certified(&result, execution);
            assert_eq!(payload(&result.report)["values"]["count_alias"], 0);
            assert_eq!(result.report.arrays_read_count, 0);
            assert_eq!(
                result.report.embedded_layout.metadata_pruned_entire_input,
                value == 99
            );
            assert_eq!(result.report.data_read, value == 17);
            assert!(!result.report.row_read);
        }
        fixture.replace();
        assert!(
            prepared
                .execute()
                .err()
                .unwrap()
                .to_string()
                .contains("prepared source changed")
        );
        assert_eq!(session.snapshot().completed_executions, 3);
        drop(prepared);
        assert_eq!(session.snapshot().memory.reserved_bytes, 0);
        let fresh = prepare_aggregate_in_session(
            &request,
            VortexLocalPrimitiveExecutionPolicy::single_threaded(),
            &session,
        )
        .unwrap();
        assert_eq!(
            payload(&fresh.execute().unwrap().report)["values"]["count_alias"],
            0
        );
        assert_eq!(session.snapshot().prepared_source_opens, 2);
    }
}

#[test]
fn prepared_aggregate_rejects_extra_payload_spill_and_wider_resource_grants() {
    let fixture = Fixture::new();
    let session = ResidentVortexSession::new(16 << 20, 1).unwrap();
    let policy = VortexLocalPrimitiveExecutionPolicy::single_threaded();
    let request = fixture.request(scalar());
    let mut projection = request.clone();
    projection.projection =
        shardloom_plan::ProjectionRequest::columns(vec![ColumnRef::new("value").unwrap()]);
    let mut unsupported = request.clone();
    unsupported.simple_aggregate.as_mut().unwrap().measures[0].value_transform =
        Some("unsupported".into());
    let mut spill = request.clone();
    spill.simple_aggregate.as_mut().unwrap().spill = Some(
        VortexAggregateSpillPolicy::new(fixture.0.join("must-not-exist"), 1 << 20, 2 << 20)
            .unwrap(),
    );
    for request in [
        projection,
        unsupported,
        spill,
        request.with_source_order_limit(0),
    ] {
        assert!(prepare_aggregate_in_session(&request, policy, &session).is_err());
        assert_eq!(session.snapshot().prepared_source_opens, 0);
        assert!(!fixture.0.join("must-not-exist").exists());
    }
    let mut narrow = policy;
    narrow.resource_envelope.memory_budget_bytes = 8 << 20;
    assert!(prepare_aggregate_in_session(&fixture.request(scalar()), narrow, &session).is_err());
    assert_eq!(session.snapshot().prepared_source_opens, 0);
    let mut inconsistent = policy;
    inconsistent.resource_envelope.max_parallelism = 2;
    assert!(
        prepare_aggregate_in_session(&fixture.request(scalar()), inconsistent, &session).is_err()
    );
    assert_eq!(session.snapshot().prepared_source_opens, 0);
    let wide_cpu = ResidentVortexSession::new(16 << 20, 2).unwrap();
    assert!(prepare_aggregate_in_session(&fixture.request(scalar()), policy, &wide_cpu).is_err());
    assert_eq!(wide_cpu.snapshot().completed_executions, 0);
    assert_eq!(wide_cpu.snapshot().memory.reserved_bytes, 0);
}

#[cfg(feature = "vortex-write")]
#[path = "local_primitive_prepared_aggregate_native_tests.rs"]
mod native;
