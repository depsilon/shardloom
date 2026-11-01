use super::*;
use crate::live_memory::LiveMemoryPool;

#[test]
fn short_fragments_do_not_reduce_the_following_full_extent() {
    let pool = LiveMemoryPool::new(10_000).unwrap();
    let mut sizing = AdaptiveWorkSizer::default();
    let cost = WorkCost {
        fixed_bytes: 0,
        bytes_per_unit: 1,
    };
    for rows in [1000, 3, 1000, 1, 1000] {
        let shape = sizing
            .select("fragments", pool.snapshot(), cost, limits(rows, 1))
            .unwrap();
        assert_eq!(shape.units(), rows);
        sizing.complete(shape, None);
    }
    assert_eq!(sizing.report().shrunk_windows, 0);
    assert_eq!(sizing.report().grown_windows, 0);
    assert_eq!(sizing.report().headroom_limited_windows, 0);
}

fn limits(max_units: u64, max_in_flight: usize) -> WorkLimits {
    WorkLimits {
        min_units: 1,
        max_units,
        max_in_flight,
    }
}

#[test]
fn retained_owners_wide_values_and_lane_overlap_limit_actual_reservations() {
    for ceiling in [257, 1024, 65_536] {
        let pool = LiveMemoryPool::new(ceiling).unwrap();
        let held = pool.reserve(ceiling / 3).unwrap();
        for unit in [1, 8, 64] {
            let shape = AdaptiveWorkSizer::default()
                .select(
                    "reader",
                    pool.snapshot(),
                    WorkCost {
                        fixed_bytes: 17,
                        bytes_per_unit: unit,
                    },
                    limits(37, 7),
                )
                .unwrap();
            assert!(shape.units() > 0 && shape.units() <= 37);
            assert!(shape.in_flight() > 0 && shape.in_flight() <= 7);
            let leases: Vec<_> = (0..shape.in_flight())
                .map(|_| pool.reserve(shape.estimated_bytes_per_lane()).unwrap())
                .collect();
            assert!(pool.snapshot().reserved_bytes <= ceiling);
            drop(leases);
            assert_eq!(pool.snapshot().reserved_bytes, held.bytes());
        }
        drop(held);
        assert_eq!(pool.snapshot().reserved_bytes, 0);
    }
}

#[test]
fn feedback_shrinks_at_next_boundary_and_regrows_only_after_two_completions() {
    let pool = LiveMemoryPool::new(1000).unwrap();
    let mut sizing = AdaptiveWorkSizer::default();
    let cost = WorkCost {
        fixed_bytes: 0,
        bytes_per_unit: 10,
    };
    let select = |s: &AdaptiveWorkSizer| {
        s.select("scan", pool.snapshot(), cost, limits(100, 1))
            .unwrap()
    };
    let initial = select(&sizing);
    assert_eq!(initial.units(), 100);
    sizing.complete(initial, None);
    let held = pool.reserve(900).unwrap();
    let narrow = select(&sizing);
    assert_eq!(narrow.units(), 10);
    sizing.complete(narrow, None);
    drop(held);
    for _ in 0..100 {
        assert_eq!(select(&sizing).units(), 10);
    }
    assert_eq!(
        sizing.report().completed_windows,
        2,
        "previews do not apply feedback"
    );
    sizing.complete(select(&sizing), None);
    assert_eq!(select(&sizing).units(), 10);
    sizing.complete(select(&sizing), None);
    assert_eq!(select(&sizing).units(), 20);
    sizing.complete(select(&sizing), None);
    assert_eq!(sizing.report().shrunk_windows, 1);
    assert_eq!(sizing.report().grown_windows, 1);
    assert_eq!(pool.snapshot().reserved_bytes, 0);
}

#[test]
fn observed_long_values_change_following_work_without_changing_the_ceiling() {
    let pool = LiveMemoryPool::new(1024).unwrap();
    let mut sizing = AdaptiveWorkSizer::default();
    let cost = WorkCost {
        fixed_bytes: 128,
        bytes_per_unit: 1,
    };
    let first = sizing
        .select("conversion", pool.snapshot(), cost, limits(100, 1))
        .unwrap();
    sizing.complete(first, Some(64));
    let second = sizing
        .select("conversion", pool.snapshot(), cost, limits(100, 1))
        .unwrap();
    assert_eq!(second.units(), 14);
    sizing.complete(second, Some(1));
    assert_eq!(
        sizing
            .select("conversion", pool.snapshot(), cost, limits(100, 1))
            .unwrap()
            .units(),
        14
    );
    assert_eq!(pool.snapshot().limit_bytes, 1024);
}

#[test]
fn minimum_workspace_and_overflow_fail_without_credit_or_progress() {
    let pool = LiveMemoryPool::new(100).unwrap();
    let sizing = AdaptiveWorkSizer::default();
    let error = sizing
        .select(
            "orc_stripe",
            pool.snapshot(),
            WorkCost {
                fixed_bytes: 90,
                bytes_per_unit: 11,
            },
            limits(8192, 1),
        )
        .unwrap_err()
        .to_string();
    assert!(error.contains("owner=orc_stripe"));
    assert!(error.contains("memory reservation denied at adaptive preflight"));
    assert!(error.contains("required_additional_bytes=101"));
    assert!(error.contains("accounted_headroom_bytes=100"));
    assert!(error.contains("ceiling_bytes=100"));
    assert!(
        sizing
            .select(
                "merge",
                pool.snapshot(),
                WorkCost {
                    fixed_bytes: u64::MAX,
                    bytes_per_unit: 1
                },
                limits(1, 1)
            )
            .is_err()
    );
    assert_eq!(sizing.report(), AdaptiveWorkReport::default());
    assert_eq!(pool.snapshot().reserved_bytes, 0);
}

#[test]
fn stale_selection_never_bypasses_the_atomic_pool_admission() {
    let pool = LiveMemoryPool::new(1024).unwrap();
    let sizing = AdaptiveWorkSizer::default();
    let shape = sizing
        .select(
            "merge",
            pool.snapshot(),
            WorkCost {
                fixed_bytes: 0,
                bytes_per_unit: 64,
            },
            limits(16, 1),
        )
        .unwrap();
    let concurrent = pool.reserve(1).unwrap();
    assert!(pool.reserve(shape.estimated_bytes_per_lane()).is_err());
    assert_eq!(sizing.report().completed_windows, 0);
    drop(concurrent);
    assert_eq!(pool.snapshot().reserved_bytes, 0);
}

#[test]
fn invalid_limits_fail_and_extreme_valid_arithmetic_stays_bounded() {
    let pool = LiveMemoryPool::new(u64::MAX).unwrap();
    let sizing = AdaptiveWorkSizer::default();
    let cost = WorkCost {
        fixed_bytes: 0,
        bytes_per_unit: 1,
    };
    for invalid in [
        limits(0, 1),
        limits(1, 0),
        WorkLimits {
            min_units: 2,
            ..limits(1, 1)
        },
    ] {
        assert!(
            sizing
                .select("invalid", pool.snapshot(), cost, invalid)
                .is_err()
        );
    }
    let shape = sizing
        .select(
            "extreme",
            pool.snapshot(),
            cost,
            limits(u64::MAX, usize::MAX),
        )
        .unwrap();
    assert_eq!(shape.units(), u64::MAX);
    assert_eq!(shape.in_flight(), 1);
    assert_eq!(grow_target(u64::MAX, u64::MAX), u64::MAX);
    assert_eq!(shrink_target(1, 1), 1);
}
