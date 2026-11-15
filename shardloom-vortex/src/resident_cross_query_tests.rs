use super::*;
use vortex::expr::{get_item, gt, lit};

fn values(result: &OwnedVortexResultBatch, columns: &[&str]) -> serde_json::Value {
    result.validate_schema_and_rows().unwrap();
    let names = columns
        .iter()
        .map(|name| (*name).to_owned())
        .collect::<Vec<_>>();
    let json = result.to_bounded_json(&names, 4096).unwrap();
    serde_json::from_str(json.value()).unwrap()
}

#[test]
fn source_segment_cache_cross_call_reads_are_avoided_with_complete_values_and_schema() {
    let fixture = Fixture::new();
    let session = ResidentVortexSession::new(8 << 20, 1).unwrap();
    let memory = session.memory().clone();
    let source = session.prepare_file(fixture.input()).unwrap();
    let projection = source
        .prepare_projection(&["value", "metric"], 5, 65536)
        .unwrap();
    let expected = serde_json::json!([
        {"value":393_081,"metric":-519}, {"value":912_778,"metric":418_982},
        {"value":5719,"metric":-719}, {"value":194_797,"metric":765_741},
        {"value":616_489,"metric":832_761}
    ]);
    let cold = projection.execute().unwrap();
    assert_eq!(values(&cold, &["value", "metric"]), expected);
    let cold_reads = &cold.file_scan_evidence().sources[0];
    assert!(cold_reads.completed_read_requests > 0);
    assert!(cold_reads.completed_read_bytes > 0);
    assert_eq!(cold_reads.segment_reuse.cross_call_hits, 0);
    assert!(cold_reads.segment_reuse.stored_segments > 0);
    let dtype = cold.dtype().clone();
    drop(cold);
    let warm = projection.execute().unwrap();
    assert_eq!(warm.dtype(), &dtype);
    assert_eq!(values(&warm, &["value", "metric"]), expected);
    let work = &warm.file_scan_evidence().sources[0];
    assert_eq!(work.completed_read_requests, 0);
    assert_eq!(work.completed_read_bytes, 0);
    assert!(work.segment_reuse.cross_call_hits > 0);
    assert_eq!(work.segment_reuse.cross_call_hits, work.segment_requests);
    assert!(work.segment_reuse.cross_call_hit_segment_bytes > 0);
    let filtered = source
        .prepare_projection(&["value"], 5, 65536)
        .unwrap()
        .with_filter(Some(gt(get_item("metric", root()), lit(0_i64))))
        .unwrap()
        .execute()
        .unwrap();
    assert_eq!(
        values(&filtered, &["value"]),
        serde_json::json!([
            {"value":912_778}, {"value":194_797}, {"value":616_489}
        ])
    );
    assert!(
        filtered.file_scan_evidence().sources[0]
            .segment_reuse
            .cross_call_hits
            > 0
    );
    let independent = session.prepare_file(fixture.input()).unwrap();
    let separate = independent
        .prepare_projection(&["value"], 5, 65536)
        .unwrap()
        .execute()
        .unwrap();
    let reads = &separate.file_scan_evidence().sources[0];
    assert_eq!(reads.segment_reuse.cross_call_hits, 0);
    assert!(reads.completed_read_requests > 0);
    assert_eq!(
        values(&separate, &["value"]),
        serde_json::json!([
            {"value":393_081}, {"value":912_778}, {"value":5719}, {"value":194_797}, {"value":616_489}
        ])
    );
    drop((
        projection,
        source,
        independent,
        filtered,
        separate,
        dtype,
        session,
    ));
    assert!(
        memory.snapshot().reserved_bytes > 0,
        "result retains copied segment capacity after eviction"
    );
    assert_eq!(values(&warm, &["value", "metric"]), expected);
    drop(warm);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

#[test]
fn source_segment_cache_unpolled_cancelled_and_failed_calls_do_not_publish() {
    use vortex::layout::segments::SegmentId;
    let fixture = Fixture::new();
    let session = ResidentVortexSession::new(8 << 20, 1).unwrap();
    let memory = session.memory().clone();
    let source = session.prepare_file(fixture.input()).unwrap();
    let projection = source.prepare_projection(&["value"], 5, 65536).unwrap();
    let baseline = memory.snapshot().reserved_bytes;
    source
        .with_native_execution(|file, _, _| {
            drop(file.segment_source().request(SegmentId::from(0)));
            Ok(())
        })
        .unwrap();
    assert_eq!(
        session
            .snapshot()
            .source_segment_cache
            .unwrap()
            .retained_entries,
        0
    );
    assert_eq!(memory.snapshot().reserved_bytes, baseline);
    let token = CancellationToken::default();
    token.cancel();
    assert!(projection.execute_with_cancellation(&token).is_err());
    assert_eq!(memory.snapshot().reserved_bytes, baseline);
    let mut calls = 0;
    let failed = session.with_native_execution_context(&CancellationToken::default(), |context| {
        projection.consume_in_context(context, &mut |_| {
            calls += 1;
            Err(resident_error("consumer stopped after first actual batch"))
        })
    });
    assert!(failed.unwrap_err().to_string().contains("consumer stopped"));
    assert_eq!(calls, 1);
    assert_eq!(
        session
            .snapshot()
            .source_segment_cache
            .unwrap()
            .retained_entries,
        0
    );
    // The first scan can initialize source-owned provider metadata. The
    // disabled-cache control below proves the same persistent ownership.
    let baseline = memory.snapshot().reserved_bytes;
    let token = CancellationToken::default();
    let cancelled = session.with_native_execution_context(&token, |context| {
        projection.consume_in_context(context, &mut |_| {
            token.cancel();
            Ok(())
        })
    });
    assert!(cancelled.is_err());
    assert_eq!(
        session
            .snapshot()
            .source_segment_cache
            .unwrap()
            .retained_entries,
        0
    );
    assert_eq!(memory.snapshot().reserved_bytes, baseline);
    let recovered = projection.execute().unwrap();
    assert!(recovered.file_scan_evidence().sources[0].completed_read_requests > 0);
    assert_eq!(
        recovered.file_scan_evidence().sources[0]
            .segment_reuse
            .cross_call_hits,
        0
    );
    assert_eq!(recovered.row_count(), 5);
    drop((recovered, projection, source));
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

#[test]
fn source_segment_cache_changed_generation_invalidates_before_a_warm_result() {
    let fixture = Fixture::new();
    let session = ResidentVortexSession::new(8 << 20, 1).unwrap();
    let memory = session.memory().clone();
    let source = session.prepare_file(fixture.input()).unwrap();
    let projection = source.prepare_projection(&["value"], 5, 65536).unwrap();
    drop(projection.execute().unwrap());
    assert!(
        session
            .snapshot()
            .source_segment_cache
            .unwrap()
            .retained_entries
            > 0
    );
    std::fs::OpenOptions::new()
        .append(true)
        .open(fixture.input())
        .unwrap()
        .write_all(&[0])
        .unwrap();
    let error = projection.execute().err().unwrap();
    assert!(error.to_string().contains("changed"));
    let snapshot = session.snapshot().source_segment_cache.unwrap();
    assert_eq!(snapshot.retained_entries, 0);
    assert_eq!(snapshot.retained_capacity_bytes, 0);
    assert!(snapshot.invalidations > 0);
    assert!(
        projection
            .execute()
            .err()
            .unwrap()
            .to_string()
            .contains("invalidated")
    );
    assert_eq!(session.snapshot().completed_executions, 1);
    drop((projection, source));
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

#[test]
fn source_segment_cache_mandatory_pressure_recovers_capacity_then_next_call_reads() {
    let fixture = Fixture::new();
    let session = ResidentVortexSession::new(8 << 20, 1).unwrap();
    let memory = session.memory().clone();
    let source = session.prepare_file(fixture.input()).unwrap();
    let projection = source.prepare_projection(&["value"], 5, 65536).unwrap();
    drop(projection.execute().unwrap());
    let cached = session.snapshot().source_segment_cache.unwrap();
    assert!(cached.retained_entries > 0);
    let baseline = memory.snapshot().reserved_bytes
        - cached.table_reserved_bytes
        - cached.retained_capacity_bytes;
    let mandatory = memory.reserve((8 << 20) - baseline).unwrap();
    assert_eq!(memory.snapshot().reserved_bytes, 8 << 20);
    let evicted = session.snapshot().source_segment_cache.unwrap();
    assert_eq!(evicted.retained_entries, 0);
    assert!(evicted.pressure_reclamations > 0);
    drop(mandatory);
    assert_eq!(memory.snapshot().reserved_bytes, baseline);
    let result = projection.execute().unwrap();
    assert_eq!(
        result.file_scan_evidence().sources[0]
            .segment_reuse
            .cross_call_hits,
        0
    );
    assert!(result.file_scan_evidence().sources[0].completed_read_requests > 0);
    assert_eq!(result.row_count(), 5);
    drop((result, projection, source));
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

#[test]
fn source_segment_cache_failed_calls_retain_only_the_same_source_metadata_as_disabled_control() {
    let fixture = Fixture::new();
    let mut metadata_growth = Vec::new();
    for enabled in [false, true] {
        let memory = LiveMemoryPool::new(8 << 20).unwrap();
        let blocker =
            (!enabled).then(|| super::super::source_segment_cache::Store::new(&memory).unwrap());
        let session = ResidentVortexSession::with_memory_pool(memory.clone(), 1, false).unwrap();
        assert_eq!(
            session.snapshot().source_segment_cache.unwrap().enabled,
            enabled
        );
        let source = session.prepare_file(fixture.input()).unwrap();
        let projection = source.prepare_projection(&["value"], 5, 65536).unwrap();
        let before = memory.snapshot().reserved_bytes;
        let mut after = None;
        for _ in 0..3 {
            let mut calls = 0;
            let failed =
                session.with_native_execution_context(&CancellationToken::default(), |context| {
                    projection.consume_in_context(context, &mut |_| {
                        calls += 1;
                        Err(resident_error("failed-call ownership control"))
                    })
                });
            assert!(failed.is_err());
            assert_eq!(calls, 1);
            let retained = memory.snapshot().reserved_bytes;
            if let Some(previous) = after {
                assert_eq!(
                    retained, previous,
                    "failed calls must not accumulate ownership"
                );
            }
            after = Some(retained);
            let cache = session.snapshot().source_segment_cache.unwrap();
            assert_eq!(cache.retained_entries, 0);
            assert_eq!(cache.table_reserved_bytes, 0);
            assert_eq!(cache.retained_capacity_bytes, 0);
        }
        metadata_growth.push(after.unwrap() - before);
        drop((projection, source, session, blocker));
        assert_eq!(memory.snapshot().reserved_bytes, 0);
    }
    assert_eq!(metadata_growth[0], metadata_growth[1]);
}
