use super::{Counters, Input, QUEUE_PER_WORKER, Range, RangeQueue, Scheduling, run};
use shardloom_exec::{compute_pool::CancellationToken, live_memory::LiveMemoryPool};
use std::{
    collections::BTreeMap,
    sync::{Arc, atomic::Ordering},
};

fn oracle(rows: &[(&str, u64, u64)]) -> BTreeMap<String, u64> {
    let mut values = BTreeMap::<String, u64>::new();
    for (key, _, count) in rows {
        let value = values.entry((*key).to_owned()).or_default();
        *value = value.checked_add(*count).unwrap();
    }
    values
}

#[test]
fn owner_scheduling_exact_weighted_collisions_and_queue_limits_all_lane_counts() {
    let strings = ["", "λ", "東京\0z", "renamed cohort", "global winner"];
    let rows = (0..1024)
        .map(|index| {
            // Deliberate collisions stay exact; two occupied partitions also leave
            // idle owner queues which must close without needing a data message.
            let group = index % strings.len();
            let hash = if group == 4 {
                13_u64 << 32
            } else {
                7_u64 << 32
            };
            (strings[group], hash, if group == 4 { 11 } else { 1 })
        })
        .collect::<Vec<_>>();
    let expected = oracle(&rows);
    let source = LiveMemoryPool::new(4 << 20).unwrap();
    let input = Input::prepare(&rows, 37, &source).unwrap();
    assert_eq!(input.entries, rows.len());
    for lanes in [1, 2, 4, 8] {
        for mode in [Scheduling::Dynamic, Scheduling::Owner] {
            let memory = LiveMemoryPool::new(8 << 20).unwrap();
            let result = run(
                &input,
                mode,
                lanes,
                &memory,
                rows.len(),
                CancellationToken::default(),
                None,
            )
            .unwrap();
            assert_eq!(result.values, expected);
            assert_eq!(result.rows, input.rows);
            assert_eq!(result.completed_ranges, input.ranges);
            assert!(result.peak_active_ranges <= lanes);
            assert!(result.peak_active_ranges > 0);
            assert!(result.peak_queued_ranges <= (lanes - 1) * QUEUE_PER_WORKER);
            assert_eq!(result.background_workers, lanes - 1);
            assert_eq!(memory.snapshot().reserved_bytes, 0);
            assert!(
                input
                    .chunks
                    .iter()
                    .all(|chunk| Arc::strong_count(chunk) == 1)
            );
        }
    }
    drop(input);
    assert_eq!(source.snapshot().reserved_bytes, 0);
}

#[test]
fn owner_scheduling_cancellation_after_completed_work_drains_all_lanes() {
    // Partition zero belongs to a background owner whenever P > 1.
    let rows = (0..1024)
        .map(|index| ("same key", 0, u64::try_from(index % 3 + 1).unwrap()))
        .collect::<Vec<_>>();
    let source = LiveMemoryPool::new(4 << 20).unwrap();
    let input = Input::prepare(&rows, 16, &source).unwrap();
    for lanes in [1, 2, 4, 8] {
        for mode in [Scheduling::Dynamic, Scheduling::Owner] {
            let memory = LiveMemoryPool::new(4 << 20).unwrap();
            let token = CancellationToken::default();
            assert!(
                run(
                    &input,
                    mode,
                    lanes,
                    &memory,
                    rows.len(),
                    token.clone(),
                    Some(1)
                )
                .is_err()
            );
            assert!(
                token.is_cancelled(),
                "the hook runs only after real range reduction"
            );
            assert_eq!(memory.snapshot().reserved_bytes, 0);
            assert!(
                input
                    .chunks
                    .iter()
                    .all(|chunk| Arc::strong_count(chunk) == 1)
            );
        }
    }
    drop(input);
    assert_eq!(source.snapshot().reserved_bytes, 0);
}

#[test]
fn owner_scheduling_queued_range_retains_native_owner_and_close_discards_it() {
    let source = LiveMemoryPool::new(1 << 20).unwrap();
    let input = Input::prepare(&[("retained λ", 0, 19)], 1, &source).unwrap();
    let weak = Arc::downgrade(&input.chunks[0]);
    let memory = LiveMemoryPool::new(1 << 20).unwrap();
    let counters = Arc::new(Counters::default());
    let queue = RangeQueue::new(2, &memory, &counters).unwrap();
    queue
        .push(
            Range {
                chunk: Arc::clone(&input.chunks[0]),
                partition: 0,
                start: 0,
                end: 1,
            },
            &CancellationToken::default(),
        )
        .unwrap();
    drop(input);
    assert!(weak.upgrade().is_some());
    assert!(source.snapshot().reserved_bytes > 0);
    let value = weak.upgrade().unwrap().partial.entry(0).unwrap();
    assert_eq!(
        std::str::from_utf8(value.0.as_slice()).unwrap(),
        "retained λ"
    );
    assert_eq!(value.2, 19);
    drop(value);
    queue.close(true);
    assert!(weak.upgrade().is_none());
    assert_eq!(source.snapshot().reserved_bytes, 0);
    assert_eq!(counters.queued.load(Ordering::Acquire), 0);
    drop(queue);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

#[test]
fn owner_scheduling_empty_pressure_and_preflight_failures_refund_before_return() {
    let source = LiveMemoryPool::new(1 << 20).unwrap();
    let empty = Input::prepare(&[], 8, &source).unwrap();
    for lanes in [1, 2, 4, 8] {
        for mode in [Scheduling::Dynamic, Scheduling::Owner] {
            let memory = LiveMemoryPool::new(4 << 20).unwrap();
            let result = run(
                &empty,
                mode,
                lanes,
                &memory,
                1,
                CancellationToken::default(),
                None,
            )
            .unwrap();
            assert!(result.values.is_empty());
            assert_eq!(result.completed_ranges, 0);
            assert_eq!(memory.snapshot().reserved_bytes, 0);
        }
    }
    let rows = [("one", 0, 1), ("two", 0, 1), ("three", 0, 1)];
    let input = Input::prepare(&rows, 1, &source).unwrap();
    for mode in [Scheduling::Dynamic, Scheduling::Owner] {
        for (bytes, entry_limit) in [(1, 3), (4 << 20, 1)] {
            let memory = LiveMemoryPool::new(bytes).unwrap();
            assert!(
                run(
                    &input,
                    mode,
                    4,
                    &memory,
                    entry_limit,
                    CancellationToken::default(),
                    None
                )
                .is_err()
            );
            assert_eq!(memory.snapshot().reserved_bytes, 0);
            assert!(
                input
                    .chunks
                    .iter()
                    .all(|chunk| Arc::strong_count(chunk) == 1)
            );
        }
        let memory = LiveMemoryPool::new(4 << 20).unwrap();
        let token = CancellationToken::default();
        token.cancel();
        assert!(run(&input, mode, 4, &memory, 3, token, None).is_err());
        assert_eq!(memory.snapshot().reserved_bytes, 0);
    }
    assert!(Input::prepare(&[("x", 0, 0)], 1, &source).is_err());
    assert!(Input::prepare(&[("x", 0, u64::MAX), ("y", 0, 1)], 1, &source).is_err());
    assert!(Input::prepare(&rows, 0, &source).is_err());
    drop(input);
    drop(empty);
    assert_eq!(source.snapshot().reserved_bytes, 0);
}

#[test]
fn owner_scheduling_preserves_full_u64_weight_without_float_or_truncation() {
    let rows = [("wide exact", 0, u64::MAX - 7), ("wide exact", 0, 7)];
    let source = LiveMemoryPool::new(1 << 20).unwrap();
    let input = Input::prepare(&rows, 1, &source).unwrap();
    for lanes in [1, 2, 4, 8] {
        for mode in [Scheduling::Dynamic, Scheduling::Owner] {
            let memory = LiveMemoryPool::new(4 << 20).unwrap();
            let result = run(
                &input,
                mode,
                lanes,
                &memory,
                2,
                CancellationToken::default(),
                None,
            )
            .unwrap();
            assert_eq!(result.rows, u64::MAX);
            assert_eq!(
                result.values,
                BTreeMap::from([("wide exact".into(), u64::MAX)])
            );
            assert_eq!(memory.snapshot().reserved_bytes, 0);
        }
    }
    drop(input);
    assert_eq!(source.snapshot().reserved_bytes, 0);
}
