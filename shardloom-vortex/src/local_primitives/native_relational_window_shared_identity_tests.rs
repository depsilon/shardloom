//! Candidate-only controls for the private compatibility predicate.
//! Mount beneath `native_relational_window_frame` after `can_share` is implemented.

use super::*;
use crate::relational_query::VortexRelationalNullOrder;
use shardloom_core::{
    decimal_output::{DecimalOutput, DecimalRounding},
    temporal::{
        Ambiguity, TemporalDType,
        civil::MonthEnd,
        operations::{Kernel, Operation},
    },
};

fn spec(function: Aggregate) -> Spec {
    Spec {
        function: Function::Aggregate(function),
        column: Some("value".into()),
        key: Some(1),
        secondary_key: None,
        filter_key: None,
        quantile_level: None,
        separator: None,
        selection_order: vec![],
        decimal: None,
        decimal_output: None,
        frame: Frame {
            unit: VortexRelationalFrameUnit::Rows,
            start: Bound::Preceding(Offset::Count(3)),
            end: Bound::CurrentRow,
            exclusion: VortexRelationalFrameExclusion::NoOthers,
            order: Some(OrderKey {
                key: 0,
                descending: false,
                nulls: Some(VortexRelationalNullOrder::First),
            }),
        },
    }
}

#[test]
fn shared_window_identity_admits_only_the_declared_arithmetic_and_single_moment_families() {
    for primary in ["sum", "avg"] {
        for companion in ["sum", "avg", "count"] {
            assert!(can_share(
                &spec(Aggregate::parse(primary).unwrap()),
                &spec(Aggregate::parse(companion).unwrap())
            ));
        }
    }
    for primary in ["var_pop", "var_samp", "stddev_pop", "stddev_samp"] {
        for companion in ["var_pop", "var_samp", "stddev_pop", "stddev_samp"] {
            assert!(can_share(
                &spec(Aggregate::parse(primary).unwrap()),
                &spec(Aggregate::parse(companion).unwrap())
            ));
        }
    }
    for name in [
        "count",
        "count_distinct",
        "sum_distinct",
        "avg_distinct",
        "min",
        "max",
        "corr",
    ] {
        let other = spec(Aggregate::parse(name).unwrap());
        assert!(!can_share(&other, &spec(Aggregate::Sum)), "{name}");
        if name != "count" {
            assert!(!can_share(&spec(Aggregate::Sum), &other), "{name}");
        }
    }
    assert!(!can_share(
        &spec(Aggregate::Sum),
        &spec(Aggregate::parse("var_pop").unwrap())
    ));
    assert!(!can_share(
        &spec(Aggregate::parse("var_pop").unwrap()),
        &spec(Aggregate::Count)
    ));
    let mut count_all = spec(Aggregate::Count);
    count_all.key = None;
    count_all.column = None;
    assert!(!can_share(&spec(Aggregate::Sum), &count_all));
}

#[test]
fn shared_window_identity_keeps_input_filter_frame_and_order_domains_distinct() {
    let changes: &[fn(&mut Spec)] = &[
        |value| value.key = Some(2),
        |value| value.key = None,
        |value| value.column = Some("other".into()),
        |value| value.filter_key = Some(2),
        |value| value.secondary_key = Some(2),
        |value| value.separator = Some(",".into()),
        |value| {
            value
                .selection_order
                .push(value.frame.order.as_ref().unwrap().clone());
        },
        |value| value.frame.unit = VortexRelationalFrameUnit::Groups,
        |value| value.frame.start = Bound::Preceding(Offset::Count(4)),
        |value| value.frame.start = Bound::Following(Offset::Count(3)),
        |value| value.frame.start = Bound::UnboundedPreceding,
        |value| value.frame.end = Bound::Following(Offset::Count(1)),
        |value| value.frame.end = Bound::UnboundedFollowing,
        |value| value.frame.exclusion = VortexRelationalFrameExclusion::CurrentRow,
        |value| value.frame.order.as_mut().unwrap().key = 3,
        |value| value.frame.order.as_mut().unwrap().descending = true,
        |value| value.frame.order.as_mut().unwrap().nulls = Some(VortexRelationalNullOrder::Last),
        |value| value.frame.order.as_mut().unwrap().nulls = None,
        |value| value.frame.order = None,
        |value| value.decimal = Some(DecimalDType::new(2, 0)),
    ];
    for (ordinal, change) in changes.iter().enumerate() {
        let mut other = spec(Aggregate::Avg);
        change(&mut other);
        assert!(
            !can_share(&spec(Aggregate::Sum), &other),
            "mutation {ordinal}"
        );
    }
    let mut primary = spec(Aggregate::Sum);
    let mut other = spec(Aggregate::Avg);
    primary.filter_key = Some(2);
    other.filter_key = Some(2);
    assert!(can_share(&primary, &other));
}

fn range(offset: RangeOffset) -> Bound {
    Bound::Preceding(Offset::Range(offset))
}

#[test]
fn shared_window_identity_compares_offset_variants_and_exact_numeric_domains() {
    let offsets: &[fn() -> RangeOffset] = &[
        || RangeOffset::Integer(3),
        || RangeOffset::Float(3.0),
        || RangeOffset::Decimal {
            value: 30,
            scale: 1,
        },
        || RangeOffset::Temporal {
            nanoseconds: 3000,
            tick_nanoseconds: 1000,
        },
    ];
    for (left_index, left) in offsets.iter().enumerate() {
        for (right_index, right) in offsets.iter().enumerate() {
            let mut primary = spec(Aggregate::Sum);
            let mut other = spec(Aggregate::Avg);
            primary.frame.unit = VortexRelationalFrameUnit::Range;
            other.frame.unit = VortexRelationalFrameUnit::Range;
            primary.frame.start = range(left());
            other.frame.start = range(right());
            assert_eq!(can_share(&primary, &other), left_index == right_index);
        }
    }
    for (left, right) in [
        (RangeOffset::Float(0.0), RangeOffset::Float(-0.0)),
        (
            RangeOffset::Decimal {
                value: 30,
                scale: 1,
            },
            RangeOffset::Decimal {
                value: 300,
                scale: 2,
            },
        ),
        (
            RangeOffset::Temporal {
                nanoseconds: 3000,
                tick_nanoseconds: 1000,
            },
            RangeOffset::Temporal {
                nanoseconds: 3000,
                tick_nanoseconds: 1,
            },
        ),
    ] {
        let mut primary = spec(Aggregate::Sum);
        let mut other = spec(Aggregate::Avg);
        primary.frame.unit = VortexRelationalFrameUnit::Range;
        other.frame.unit = VortexRelationalFrameUnit::Range;
        primary.frame.start = range(left);
        other.frame.start = range(right);
        assert!(!can_share(&primary, &other));
    }
}

fn calendar(amount: u64, operation: Operation, dtype: &str) -> Bound {
    range(RangeOffset::Calendar {
        amount,
        kernel: Box::new(
            Kernel::bind(operation, TemporalDType::parse(dtype).unwrap(), None).unwrap(),
        ),
    })
}

#[test]
fn shared_window_identity_uses_exact_calendar_kernel_semantics() {
    let days = Operation::CalendarDays {
        ambiguity: Ambiguity::Earlier,
    };
    let mut primary = spec(Aggregate::Sum);
    primary.frame.unit = VortexRelationalFrameUnit::Range;
    primary.frame.start = calendar(1, days.clone(), "timestamp(s,America/New_York)");
    let mut other = spec(Aggregate::Avg);
    other.frame.unit = VortexRelationalFrameUnit::Range;
    other.frame.start = calendar(1, days.clone(), "timestamp(s,America/New_York)");
    assert!(can_share(&primary, &other));
    for bound in [
        calendar(2, days.clone(), "timestamp(s,America/New_York)"),
        calendar(1, days.clone(), "timestamp(ms,America/New_York)"),
        calendar(1, days, "timestamp(s,Europe/London)"),
        calendar(
            1,
            Operation::CalendarDays {
                ambiguity: Ambiguity::Later,
            },
            "timestamp(s,America/New_York)",
        ),
        calendar(
            1,
            Operation::CalendarMonths {
                ambiguity: Ambiguity::Earlier,
                month_end: MonthEnd::Clamp,
            },
            "timestamp(s,America/New_York)",
        ),
        calendar(
            1,
            Operation::CalendarMonths {
                ambiguity: Ambiguity::Earlier,
                month_end: MonthEnd::Reject,
            },
            "timestamp(s,America/New_York)",
        ),
    ] {
        other.frame.start = bound;
        assert!(!can_share(&primary, &other));
    }
}

#[test]
fn shared_window_decimal_finalizer_policy_is_separate_from_observation_identity() {
    let mut primary = spec(Aggregate::Sum);
    primary.decimal = Some(DecimalDType::new(2, 0));
    primary.decimal_output = Some(DecimalOutput::new(38, 0, DecimalRounding::Exact).unwrap());
    let mut average = spec(Aggregate::Avg);
    average.decimal = primary.decimal;
    average.decimal_output = Some(DecimalOutput::new(3, 1, DecimalRounding::HalfEven).unwrap());
    assert!(can_share(&primary, &average));
    let count = spec(Aggregate::Count);
    assert!(count.decimal.is_none());
    assert!(can_share(&primary, &count));
    assert!(!can_share(&count, &primary));
    average.decimal = Some(DecimalDType::new(2, 1));
    assert!(!can_share(&primary, &average));
}
