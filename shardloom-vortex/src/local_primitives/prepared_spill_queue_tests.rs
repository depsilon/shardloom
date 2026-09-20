use super::*;
use crate::{
    local_primitives::prepared_aggregate::prepare_aggregate_in_session,
    resident_session::ResidentServingPolicy,
};
use shardloom_exec::compute_pool::CancellationToken;
use std::{
    sync::mpsc,
    thread,
    time::{Duration, Instant},
};

#[test]
#[allow(
    clippy::too_many_lines,
    reason = "Keep blocker release and cancellation checks together so failed assertions cannot strand a scoped worker"
)]
fn prepared_spill_cancellation_leaves_serving_queue_before_held_call_finishes() {
    // Single text COUNT uses the worker adapter; compound COUNT and integer
    // DISTINCT use temporary provider drivers in a serving session.
    for family in ["text_count", "compound_count", "integer_distinct"] {
        let fixture = Fixture::new(64, 8, true, false);
        let mut request = match family {
            "compound_count" => fixture.query(&["cohort_renamed", "label_renamed"], 0, 7),
            _ => fixture.query(&["label_renamed"], 0, 7),
        };
        if family == "integer_distinct" {
            let aggregate = request.simple_aggregate.as_mut().unwrap();
            aggregate.group_by = vec![ColumnRef::new("cohort_renamed").unwrap()];
            aggregate.measures = vec![VortexSimpleAggregateMeasure::new(
                "count_distinct",
                Some(ColumnRef::new("event_ordinal").unwrap()),
                "frequency".into(),
            )];
            aggregate.order_by = vec![
                VortexAggregateOrderExpr::new("frequency", true),
                VortexAggregateOrderExpr::new("cohort_renamed", false),
            ];
            request = VortexQueryPrimitiveRequest::simple_aggregate(
                request.source_uri.clone().unwrap(),
                aggregate.clone(),
            )
            .with_source_order_limit(7);
        }
        let expected = execute_vortex_local_primitive_with_policy(
            &request,
            VortexLocalPrimitiveExecutionPolicy::single_threaded(),
        )
        .unwrap_or_else(|error| panic!("{family} ordinary spill fixture: {error}"));
        let parallelism = thread::available_parallelism().unwrap().get().min(2);
        let session = ResidentVortexSession::with_serving_policy(
            32 << 20,
            parallelism,
            ResidentServingPolicy {
                general_cpu_lanes: parallelism,
                reserve_metadata_lane: false,
                ..Default::default()
            },
        )
        .unwrap();
        let mut prepared = prepare_aggregate_in_session(
            &request,
            VortexLocalPrimitiveExecutionPolicy::new(parallelism).unwrap(),
            &session,
        )
        .unwrap();
        let source = session.prepare_file(fixture.path()).unwrap();
        let cancellation = request
            .simple_aggregate
            .as_ref()
            .unwrap()
            .spill
            .as_ref()
            .unwrap()
            .clone();
        let (entered, waiting) = mpsc::sync_channel(1);
        let (release, held) = mpsc::sync_channel(1);
        let (done, finished) = mpsc::sync_channel(1);
        thread::scope(|threads| {
            let source_ref = &source;
            let blocker = threads.spawn(move || {
                source_ref.with_native_execution_controlled(
                    &CancellationToken::default(),
                    |_, _| {
                        entered.send(()).unwrap();
                        held.recv().unwrap();
                        Ok(())
                    },
                )
            });
            waiting.recv_timeout(Duration::from_secs(5)).unwrap();
            let queued = threads.spawn(|| {
                done.send(prepared.execute().is_err()).unwrap();
            });
            let deadline = Instant::now() + Duration::from_secs(5);
            while session.admission_snapshot().unwrap().queued_calls != 1
                && Instant::now() < deadline
            {
                thread::sleep(Duration::from_millis(1));
            }
            let observed_queue = session.admission_snapshot().unwrap().queued_calls == 1;
            cancellation.cancel();
            let cancelled = finished.recv_timeout(Duration::from_secs(5));
            // Always release the blocker even when the regression returns no
            // cancellation response, so a failing scoped test cannot deadlock.
            release.send(()).unwrap();
            blocker.join().unwrap().unwrap();
            queued.join().unwrap();
            assert!(
                observed_queue,
                "prepared spill must reach serving admission"
            );
            assert!(
                cancelled.unwrap(),
                "queued spill must cancel before blocker release"
            );
        });
        assert_eq!(session.admission_snapshot().unwrap().queued_calls, 0);
        assert_eq!(session.admission_snapshot().unwrap().active_cpu_lanes, 0);
        fixture.empty();
        let renewed = prepared.renew_spill_cancellation().unwrap();
        // A stale cancellation owner cannot poison the new execution scope.
        cancellation.cancel();
        let actual = prepared.execute().unwrap();
        assert_eq!(
            summary(&actual.report)["values"],
            summary(&expected)["values"]
        );
        assert!(actual.native_io_certificate.is_certified());
        assert_eq!(actual.runtime.prepared_source_opens, 2);
        renewed.cancel();
        assert!(prepared.execute().is_err());
        fixture.empty();
        let memory = session.memory().clone();
        drop(prepared);
        drop(source);
        drop(session);
        assert_eq!(memory.snapshot().reserved_bytes, 0);
    }
}
