use super::*;
use crate::{VortexQueryPrimitiveRequest, resident_session::ResidentVortexSession};
use shardloom_core::{ComparisonOp, DatasetUri, PredicateExpr, StatValue};
use vortex::{
    VortexSessionDefault as _,
    file::WriteOptionsSessionExt as _,
    io::{
        runtime::{BlockingRuntime as _, single::SingleThreadRuntime},
        session::RuntimeSessionExt as _,
    },
    session::VortexSession,
};

struct Fixture(std::path::PathBuf);
impl Fixture {
    fn new() -> Self {
        let fixture = Self(std::env::temp_dir().join(format!(
            "shardloom-compound-admission-{}-{}.vortex", std::process::id(),
            std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos(),
        )));
        let runtime = SingleThreadRuntime::default();
        let session = VortexSession::default().with_handle(runtime.handle());
        let arrays = [
            chunk(
                integers(&[9, 2, 9, 0]),
                dictionary(&[0, 1, 0, 1], &["a", "z"]),
            ),
            chunk(
                integers(&[2, 4, 7, 7]),
                dictionary(&[0, 0, 1, 1], &["z", "a"]),
            ),
            chunk(integers(&[4, 0]), dictionary(&[0, 1], &["z", "a"])),
        ];
        let mut output = std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&fixture.0)
            .unwrap();
        let mut writer = session
            .write_options()
            .with_strategy(
                super::super::native_flat_layout::SequentialNativeFlatLayout::strategy(
                    arrays.len(),
                ),
            )
            .with_file_statistics(Vec::new())
            .blocking(&runtime)
            .writer(&mut output, arrays[0].dtype().clone());
        for array in arrays {
            writer.push(array).unwrap();
        }
        assert_eq!(writer.finish().unwrap().row_count(), 10);
        fixture
    }

    fn query(&self) -> VortexQueryPrimitiveRequest {
        let mut query = VortexQueryPrimitiveRequest::simple_aggregate(
            DatasetUri::new(self.0.display().to_string()).unwrap(),
            request(false),
        )
        .with_source_order_limit(2);
        query.predicate = Some(PredicateExpr::Compare {
            column: ColumnRef::new("actor_alias").unwrap(),
            op: ComparisonOp::GtEq,
            value: StatValue::Int64(2),
        });
        query
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.0);
    }
}

#[test]
fn actual_compound_worker_admission_denial_restores_same_runtime_provider_drivers() {
    use super::super::{
        aggregate_count_workers::ADMISSION_TEST_PRESSURE,
        compound_count_workers::request_schema_may_be_admitted,
        read_prepared_vortex_simple_aggregate_scan,
    };
    let fixture = Fixture::new();
    let query = fixture.query();
    let source = query.source_uri.as_ref().unwrap();
    // Full independent values: after filtering, each of (2,z), (4,z),
    // (7,a), (9,a) occurs twice. Global key ties, offset 1 and LIMIT 2.
    let expected = serde_json::json!([
        {"actor_alias":4,"phrase_alias":"z","n_alias":2},
        {"actor_alias":7,"phrase_alias":"a","n_alias":2},
    ]);
    for parallelism in [1, 2, 4] {
        let mut policy = VortexLocalPrimitiveExecutionPolicy::new(parallelism).unwrap();
        policy.resource_envelope.memory_budget_bytes = 32 << 20;
        let resident = ResidentVortexSession::for_external_cpu_pool(32 << 20, parallelism).unwrap();
        let prepared = resident.prepare_file(&fixture.0).unwrap();
        assert!(request_schema_may_be_admitted(&query, prepared.dtype()));
        assert_eq!(resident.snapshot().provider_background_workers, 0);
        // A successful call follows the failed admission too: no retained
        // driver guard or pressure state may leak into the next execution.
        for (execution, pressure) in [false, true, false].into_iter().enumerate() {
            let denied_before = resident.memory().snapshot().denied_reservations;
            ADMISSION_TEST_PRESSURE.with(|current| current.set(pressure));
            let scan = prepared
                .with_native_execution(|file, session, runtime| {
                    read_prepared_vortex_simple_aggregate_scan(
                        source,
                        &query,
                        policy,
                        file,
                        session,
                        runtime,
                        Some(resident.memory()),
                        None,
                    )
                })
                .unwrap();
            assert!(!ADMISSION_TEST_PRESSURE.with(std::cell::Cell::get));
            let payload: serde_json::Value = serde_json::from_str(&scan.result_summary).unwrap();
            assert_eq!(payload["values"], expected);
            assert_eq!(scan.scan.pre_limit_result_row_count, 8);
            assert!(scan.scan.arrays_read_count > 0);
            assert!(
                payload
                    .get("aggregate_workers_partition_source_replays")
                    .is_none()
            );
            if pressure {
                assert!(resident.memory().snapshot().denied_reservations > denied_before);
                let drivers = super::super::bounded_local_vortex_worker_count(parallelism);
                assert_eq!(payload["aggregate_provider_background_workers"], drivers);
                assert!(
                    payload["aggregate_provider_cpu_scope"]
                        .as_str()
                        .unwrap()
                        .contains("actual_aggregate_worker_admission_declined_before_scan")
                );
                assert!(payload.get("aggregate_workers_submitted_chunks").is_none());
            } else {
                assert_eq!(
                    resident.memory().snapshot().denied_reservations,
                    denied_before
                );
                assert!(payload.get("aggregate_provider_cpu_scope").is_none());
                assert_eq!(payload["aggregate_workers_provider_background_workers"], 0);
                assert!(
                    payload["aggregate_workers_submitted_chunks"]
                        .as_u64()
                        .unwrap()
                        > 0
                );
            }
            assert_eq!(resident.snapshot().prepared_source_opens, 1);
            assert_eq!(
                resident.snapshot().completed_executions,
                u64::try_from(execution + 1).unwrap()
            );
        }
        drop(prepared);
        assert_eq!(resident.memory().snapshot().reserved_bytes, 0);
    }
}

#[test]
// One actual provider/cache lifecycle covers admission, reuse evidence, values,
// and release; separating assertions would lose the shared execution boundary.
#[allow(clippy::too_many_lines)]
fn cached_compound_admission_denial_reports_actual_restored_provider_drivers() {
    use super::super::{
        aggregate_count_workers::ADMISSION_TEST_PRESSURE,
        read_prepared_vortex_simple_aggregate_scan,
    };
    use crate::resident_session::segment_reuse::SegmentReusePolicy;

    let fixture = Fixture::new();
    let query = fixture.query();
    let source = query.source_uri.as_ref().unwrap();
    let expected = serde_json::json!([
        {"actor_alias":4,"phrase_alias":"z","n_alias":2},
        {"actor_alias":7,"phrase_alias":"a","n_alias":2},
    ]);
    for parallelism in [1, 2, 4] {
        let mut policy = VortexLocalPrimitiveExecutionPolicy::new(parallelism).unwrap();
        policy.resource_envelope.memory_budget_bytes = 32 << 20;
        let resident = ResidentVortexSession::for_external_cpu_pool(32 << 20, parallelism).unwrap();
        let prepared = resident.prepare_file(&fixture.0).unwrap();
        let drivers = super::super::bounded_local_vortex_worker_count(parallelism);
        for (execution, pressure) in [false, true, false].into_iter().enumerate() {
            ADMISSION_TEST_PRESSURE.with(|current| current.set(pressure));
            let (mut result, evidence) = prepared
                .with_native_execution_cached_retry_with_drivers(
                    SegmentReusePolicy {
                        max_retained_bytes: 8 << 20,
                        max_segment_bytes: 8 << 20,
                        max_entries: 16,
                    },
                    false,
                    |file, session, runtime, attempt| {
                        let mut retry = |error: &vortex::error::VortexError| {
                            attempt.request_uncached_retry(error)
                        };
                        read_prepared_vortex_simple_aggregate_scan(
                            source,
                            &query,
                            policy,
                            file,
                            session,
                            runtime,
                            Some(resident.memory()),
                            Some(&mut retry),
                        )
                    },
                )
                .unwrap();
            assert!(!ADMISSION_TEST_PRESSURE.with(std::cell::Cell::get));
            assert!(!evidence.admission_skipped);
            assert!(evidence.closed);
            assert!(evidence.counters.completed_segments > 0);
            assert_eq!(evidence.uncached_replays, 0);
            // The wrapper owns zero drivers; actual admission may restore
            // drivers inside the scan. Use the production typed annotation.
            assert_eq!(evidence.provider_background_workers, 0);
            let expected_drivers = if pressure { drivers } else { 0 };
            assert_eq!(
                result.restored_provider_background_workers,
                expected_drivers
            );
            result.annotate_segment_reuse(evidence).unwrap();
            let payload: serde_json::Value = serde_json::from_str(&result.result_summary).unwrap();
            assert_eq!(payload["values"], expected);
            assert_eq!(result.scan.pre_limit_result_row_count, 8);
            assert_eq!(
                payload["scan_segment_reuse"]["provider_background_workers"],
                expected_drivers
            );
            assert!(
                payload["scan_segment_reuse"]["provider_background_workers_scope"]
                    .as_str()
                    .unwrap()
                    .contains("completed_native_scan_and_wrapper_owned_drivers")
            );
            if pressure {
                assert_eq!(
                    payload["aggregate_provider_background_workers"],
                    expected_drivers
                );
                assert!(payload.get("aggregate_workers_submitted_chunks").is_none());
            } else {
                assert_eq!(payload["aggregate_workers_provider_background_workers"], 0);
                assert!(
                    payload
                        .get("aggregate_provider_background_workers")
                        .is_none()
                );
            }
            assert_eq!(resident.snapshot().prepared_source_opens, 1);
            assert_eq!(
                resident.snapshot().completed_executions,
                u64::try_from(execution + 1).unwrap()
            );
            assert_eq!(resident.snapshot().provider_background_workers, 0);
        }
        drop(prepared);
        assert_eq!(resident.memory().snapshot().reserved_bytes, 0);
    }
}
