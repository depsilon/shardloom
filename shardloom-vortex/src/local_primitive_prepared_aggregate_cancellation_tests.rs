//! Parent cancellation remains distinct from an individual native scan attempt.

use super::*;
use crate::{
    VortexAggregateOrderExpr, VortexSimpleAggregateMeasure, VortexSimpleAggregateRequest,
    memory_file_generation::{
        MemoryFileGeneration, MemoryFileGenerationBounds, MemoryFileGenerationLayout,
    },
    resident_memory_source::{
        MemoryColumn, MemoryColumnValues, MemorySourceBounds, ResidentMemorySource,
    },
};
use shardloom_core::{ColumnRef, ComparisonOp, PredicateExpr, StatValue};
use shardloom_exec::compute_pool::CancellationToken;
use std::sync::{
    Arc,
    atomic::{AtomicBool, AtomicUsize, Ordering},
};
use vortex::layout::segments::{SegmentFuture, SegmentId, SegmentSource};

fn generation(session: &ResidentVortexSession) -> MemoryFileGeneration {
    ResidentMemorySource::from_columns(
        session,
        &[
            MemoryColumn {
                name: "key",
                values: MemoryColumnValues::Int64NonNullable(&[9, 4, 9, 2, 4, 9]),
            },
            MemoryColumn {
                name: "metric",
                values: MemoryColumnValues::Int64NonNullable(&[7, 7, 7, 7, 7, 7]),
            },
        ],
        MemorySourceBounds::default(),
    )
    .unwrap()
    .file_generation_with_layout(
        MemoryFileGenerationBounds::default(),
        MemoryFileGenerationLayout {
            row_group_rows: 2,
            max_segments: 6,
        },
        None,
    )
    .unwrap()
}

fn request(generation: &MemoryFileGeneration, filtered: bool) -> VortexQueryPrimitiveRequest {
    let aggregate = VortexSimpleAggregateRequest::grouped(
        vec![ColumnRef::new("key").unwrap()],
        vec![VortexSimpleAggregateMeasure::new("count", None, "n".into())],
    )
    .with_order_by(vec![VortexAggregateOrderExpr::new("n", true)]);
    let mut request =
        VortexQueryPrimitiveRequest::simple_aggregate(generation.source_uri().clone(), aggregate)
            .with_source_order_limit(3);
    if filtered {
        request.predicate = Some(PredicateExpr::And(vec![
            PredicateExpr::Compare {
                column: ColumnRef::new("metric").unwrap(),
                op: ComparisonOp::GtEq,
                value: StatValue::Int64(1),
            },
            PredicateExpr::Compare {
                column: ColumnRef::new("metric").unwrap(),
                op: ComparisonOp::Lt,
                value: StatValue::Int64(10),
            },
        ]));
    }
    request
}

fn summary(executed: &ExecutedVortexAggregate) -> serde_json::Value {
    assert!(executed.native_io_certificate.is_certified());
    assert!(!executed.native_io_certificate.fallback_attempted);
    assert!(!executed.report.has_errors());
    assert_eq!(executed.runtime.prepared_source_opens, 0);
    let report = executed.report.result_summary.as_deref().unwrap();
    let summary: serde_json::Value =
        serde_json::from_str(report.rsplit_once(" values=").unwrap().1).unwrap();
    assert_eq!(
        summary["values"],
        serde_json::json!([
            {"key": 9, "n": 3}, {"key": 4, "n": 2}, {"key": 2, "n": 1},
        ])
    );
    summary
}

struct CancelFirstSegment {
    inner: Arc<dyn SegmentSource>,
    cancellation: CancellationToken,
    fired: AtomicBool,
    requests: AtomicUsize,
}

impl SegmentSource for CancelFirstSegment {
    fn request(&self, id: SegmentId) -> SegmentFuture {
        self.requests.fetch_add(1, Ordering::AcqRel);
        if !self.fired.swap(true, Ordering::AcqRel) {
            self.cancellation.cancel();
        }
        self.inner.request(id)
    }
}

#[test]
fn prepared_parent_cancellation_during_memory_scan_releases_attempt_and_allows_fresh_token() {
    let session = ResidentVortexSession::for_external_cpu_pool(32 << 20, 1).unwrap();
    let memory = session.memory().clone();
    let generation = generation(&session);
    let mut prepared = generation
        .prepare_aggregate(
            &request(&generation, false),
            VortexLocalPrimitiveExecutionPolicy::single_threaded(),
        )
        .unwrap();
    assert!(prepared.worker_pool);
    assert!(prepared.reuse.is_none());
    let parent = CancellationToken::default();
    let wrapped = Arc::new(CancelFirstSegment {
        inner: prepared.source.file().unwrap().file().segment_source(),
        cancellation: parent.clone(),
        fired: AtomicBool::new(false),
        requests: AtomicUsize::new(0),
    });
    let file = prepared
        .source
        .file()
        .unwrap()
        .file()
        .clone()
        .with_segment_source(wrapped.clone());
    prepared.source = PreparedAggregateSource::File(session.prepare_immutable_file(file));
    let retained = memory.snapshot().reserved_bytes;
    let error = prepared
        .execute_cancellable(&parent)
        .err()
        .expect("source-triggered cancellation must fail");
    assert!(error.to_string().contains("cancel"));
    assert!(wrapped.fired.load(Ordering::Acquire));
    assert!(wrapped.requests.load(Ordering::Acquire) > 0);
    assert!(parent.check().is_err());
    assert_eq!(session.snapshot().completed_executions, 0);
    assert_eq!(memory.snapshot().reserved_bytes, retained);
    let fresh = CancellationToken::default();
    let completed = prepared.execute_cancellable(&fresh).unwrap();
    summary(&completed);
    assert_eq!(completed.runtime.completed_executions, 1);
    fresh.check().unwrap();
    assert!(parent.check().is_err());
    assert_eq!(memory.snapshot().reserved_bytes, retained);
    drop(completed);
    drop(prepared);
    drop(wrapped);
    drop(generation);
    drop(session);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

#[test]
fn prepared_cache_pressure_replay_does_not_cancel_the_live_parent_scope() {
    use super::super::aggregate_count_workers::{SOURCE_SCAN_TEST_FAULT, SourceScanTestFault};
    struct ClearFault;
    impl Drop for ClearFault {
        fn drop(&mut self) {
            SOURCE_SCAN_TEST_FAULT.with(|fault| fault.set(None));
        }
    }
    let _clear = ClearFault;
    assert!(SOURCE_SCAN_TEST_FAULT.with(std::cell::Cell::get).is_none());
    let session = ResidentVortexSession::for_external_cpu_pool(32 << 20, 1).unwrap();
    let memory = session.memory().clone();
    let generation = generation(&session);
    let prepared = generation
        .prepare_aggregate(
            &request(&generation, true),
            VortexLocalPrimitiveExecutionPolicy::single_threaded(),
        )
        .unwrap();
    assert!(prepared.worker_pool);
    assert!(
        prepared.reuse.is_some(),
        "repeated filter-only field must admit segment reuse"
    );
    let parent = CancellationToken::default();
    let retained = memory.snapshot().reserved_bytes;
    for (index, pressure) in [false, true, false].into_iter().enumerate() {
        SOURCE_SCAN_TEST_FAULT
            .with(|fault| fault.set(pressure.then_some(SourceScanTestFault::OwnedDenial)));
        let completed = prepared.execute_cancellable(&parent).unwrap();
        assert!(
            SOURCE_SCAN_TEST_FAULT.with(std::cell::Cell::get).is_none(),
            "fault requires a committed worker partial"
        );
        let summary = summary(&completed);
        assert_eq!(
            summary["scan_segment_reuse"]["uncached_replays"],
            u64::from(pressure)
        );
        assert_eq!(
            summary["scan_segment_reuse"]["retention_live_owned_bytes"],
            0
        );
        assert_eq!(summary["scan_segment_reuse"]["closed"], true);
        assert!(
            summary["aggregate_workers_submitted_chunks"]
                .as_u64()
                .unwrap()
                > 0
        );
        assert_eq!(completed.runtime.completed_executions, (index + 1) as u64);
        parent.check().unwrap();
        assert_eq!(memory.snapshot().reserved_bytes, retained);
    }
    drop(prepared);
    drop(generation);
    drop(session);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}
