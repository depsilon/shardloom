//! Complete-cohort screen of the already-shipped owned-array source boundary.

use super::*;
use crate::{
    VortexLocalPrimitiveExecutionPolicy, VortexQueryPrimitiveRequest, VortexSimpleAggregateMeasure,
    VortexSimpleAggregateRequest,
    local_primitives::prepared_aggregate::{
        ExecutedVortexAggregate, PreparedVortexAggregate, prepare_aggregate_in_session,
    },
    owned_array_source::{OwnedArraySource, OwnedArraySourceBounds},
};
use shardloom_core::{ColumnRef, DatasetUri};

fn request(uri: DatasetUri) -> VortexQueryPrimitiveRequest {
    VortexQueryPrimitiveRequest::simple_aggregate(
        uri,
        VortexSimpleAggregateRequest::new(vec![
            VortexSimpleAggregateMeasure::new("count", None, "rows".into()),
            VortexSimpleAggregateMeasure::new(
                "count",
                Some(ColumnRef::new("renamed_text").unwrap()),
                "present".into(),
            ),
            VortexSimpleAggregateMeasure::new(
                "count_distinct",
                Some(ColumnRef::new("renamed_exact_key").unwrap()),
                "unique".into(),
            ),
            VortexSimpleAggregateMeasure::new(
                "max",
                Some(ColumnRef::new("renamed_text").unwrap()),
                "last".into(),
            ),
        ]),
    )
}

// Threads are ready before release; a panic cannot leave an unbounded Barrier wait.
fn consumers(
    prepared: &PreparedVortexAggregate,
    cancelled_first: bool,
) -> Vec<Result<ExecutedVortexAggregate>> {
    thread::scope(|scope| {
        let (ready_tx, ready_rx) = mpsc::channel();
        let mut releases = Vec::new();
        let mut workers = Vec::new();
        for index in 0..4 {
            let (tx, rx) = mpsc::sync_channel(1);
            releases.push(tx);
            let ready = ready_tx.clone();
            workers.push(scope.spawn(move || {
                let cancellation = CancellationToken::default();
                if cancelled_first && index == 0 {
                    cancellation.cancel();
                }
                ready.send(()).unwrap();
                rx.recv_timeout(DEADLINE).unwrap();
                prepared.execute_cancellable(&cancellation)
            }));
        }
        for _ in 0..4 {
            ready_rx.recv_timeout(DEADLINE).unwrap();
        }
        for release in releases {
            release.send(()).unwrap();
        }
        workers
            .into_iter()
            .map(|worker| worker.join().unwrap())
            .collect()
    })
}

fn verify_aggregates(outputs: &[ExecutedVortexAggregate]) -> Vec<Value> {
    let expected = json!({"rows": ROWS, "present": ROWS - ROWS.div_ceil(17),
        "unique": ROWS, "last": text_value(ROWS-1).unwrap()});
    outputs
        .iter()
        .map(|output| {
            assert!(output.native_io_certificate.is_certified());
            assert!(!output.report.has_errors());
            let summary = output.report.result_summary.as_ref().unwrap();
            let values: Value =
                serde_json::from_str(summary.rsplit_once(" values=").unwrap().1).unwrap();
            assert_eq!(values["values"], expected);
            values["values"].clone()
        })
        .collect()
}

fn run(source: &PreparedVortexSource, baseline: &PreparedVortexAggregate, shared: bool) -> Value {
    let complete = Instant::now();
    let policy = VortexLocalPrimitiveExecutionPolicy::new(4).unwrap();
    let (outputs, producer_nanos, handoff_prepare_nanos) = if shared {
        let producer = Instant::now();
        let result = source
            .prepare_projection(
                &["renamed_exact_key", "renamed_text"],
                ROWS as u64,
                MAX_OUTPUT_BYTES,
            )
            .unwrap()
            .execute()
            .unwrap();
        let producer_nanos = nanos(producer.elapsed());
        let handoff = Instant::now();
        let owned = OwnedArraySource::from_owned(
            result,
            OwnedArraySourceBounds::default(),
            &CancellationToken::default(),
        )
        .unwrap();
        let prepared = owned
            .prepare_aggregate(&request(owned.source_uri().clone()), policy)
            .unwrap();
        let handoff_prepare_nanos = nanos(handoff.elapsed());
        let outputs = consumers(&prepared, false);
        // Consumer handles and producer payload lifetime are charged in the complete clock.
        drop((prepared, owned));
        (outputs, producer_nanos, handoff_prepare_nanos)
    } else {
        (consumers(baseline, false), 0, 0)
    };
    let outputs = outputs.into_iter().map(Result::unwrap).collect::<Vec<_>>();
    let to_reports_nanos = nanos(complete.elapsed());
    let verification = Instant::now();
    let values = verify_aggregates(&outputs);
    let verification_nanos = nanos(verification.elapsed());
    let drop_started = Instant::now();
    drop(outputs);
    let report_drop_nanos = nanos(drop_started.elapsed());
    json!({"shared_owned_producer": shared, "producer_nanos": producer_nanos,
        "handoff_and_consumer_prepare_nanos": handoff_prepare_nanos,
        "cohort_through_complete_reports_and_shared_owner_drop_nanos": to_reports_nanos,
        "complete_values": values, "independent_verification_nanos": verification_nanos,
        "report_drop_nanos": report_drop_nanos})
}

#[test]
#[ignore = "bounded release-only owned producer fanout screen; run serially with --release --ignored --exact"]
#[allow(clippy::assertions_on_constants)]
fn existing_owned_producer_fanout_pairs() {
    assert!(!cfg!(debug_assertions));
    let fixture = Fixture::new();
    let session = ResidentVortexSession::with_serving_policy(
        SESSION_BYTES,
        4,
        ResidentServingPolicy {
            general_cpu_lanes: 1,
            reserve_metadata_lane: false,
            ..Default::default()
        },
    )
    .unwrap();
    let memory = session.memory().clone();
    let source = session.prepare_file(&fixture.path).unwrap();
    let baseline = prepare_aggregate_in_session(
        &request(DatasetUri::new(fixture.path.display().to_string()).unwrap()),
        VortexLocalPrimitiveExecutionPolicy::new(4).unwrap(),
        &session,
    )
    .unwrap();
    let mut pairs = Vec::new();
    for iteration in 0..=5 {
        let order = if iteration % 2 == 0 {
            [false, true]
        } else {
            [true, false]
        };
        let first = run(&source, &baseline, order[0]);
        let second = run(&source, &baseline, order[1]);
        pairs.push(
            json!({"iteration": iteration, "warmup": iteration == 0, "order": order,
            "first": first, "second": second}),
        );
    }
    // A caller's cancellation must not invalidate immutable input or another caller's state.
    let owned = OwnedArraySource::from_owned(
        source
            .prepare_projection(
                &["renamed_exact_key", "renamed_text"],
                ROWS as u64,
                MAX_OUTPUT_BYTES,
            )
            .unwrap()
            .execute()
            .unwrap(),
        OwnedArraySourceBounds::default(),
        &CancellationToken::default(),
    )
    .unwrap();
    let prepared = owned
        .prepare_aggregate(
            &request(owned.source_uri().clone()),
            VortexLocalPrimitiveExecutionPolicy::new(4).unwrap(),
        )
        .unwrap();
    let mut cancelled = consumers(&prepared, true).into_iter();
    assert!(
        matches!(cancelled.next().unwrap(), Err(shardloom_core::ShardLoomError::InvalidOperation(reason))
        if reason == "execution cancelled; fallback execution was not attempted")
    );
    let survivors = cancelled.map(Result::unwrap).collect::<Vec<_>>();
    assert_eq!(verify_aggregates(&survivors).len(), 3);
    assert_eq!(
        verify_aggregates(&[prepared
            .execute_cancellable(&CancellationToken::default())
            .unwrap()])
        .len(),
        1
    );
    source.validate_generation().unwrap();
    let mut report = json!({"schema": "shardloom.resident_source_fanout_screen.v1",
        "source_sha256": fixture.sha256, "source_bytes": fixture.len, "rows": ROWS,
        "source_generation": format!("{:?}", source.0.identity.as_ref().unwrap().generation),
        "scope": "warm local file, four identical complete native aggregates; no production tail claim; existing owned-array API, no new runtime cache; encoded children may still decode per consumer",
        "control": "one retained file aggregate handle; prepare excluded equally from each repeated control call",
        "candidate": "one completed file projection plus owned-source handoff and consumer preparation charged per cohort, then four independent aggregates",
        "session_bytes": SESSION_BYTES, "parallelism": 4, "general_cpu_lanes": 1,
        "all_pairs": pairs, "pre_admission_cancel_isolation_survivors": 3, "post_cancel_reexecution": true});
    drop((survivors, prepared, owned, baseline, source, session));
    let final_memory = memory.snapshot();
    assert_eq!(final_memory.reserved_bytes, 0);
    assert_eq!(final_memory.denied_reservations, 0);
    report["final_reserved_bytes"] = json!(final_memory.reserved_bytes);
    report["peak_reserved_bytes"] = json!(final_memory.peak_reserved_bytes);
    let path = fixture.directory.clone();
    drop(fixture);
    assert!(!path.exists());
    report["fixture_removed"] = json!(true);
    eprintln!("SHARDLOOM_R8_SOURCE_FANOUT {report}");
}
