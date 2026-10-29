use super::*;
use crate::temporal::civil::{MonthEnd, parse_timestamp};

fn kernel(name: &str, step: Step, origin: Option<i64>) -> Kernel {
    Kernel::bind(
        Operation::Bucket { step, origin },
        TemporalDType::parse(name).unwrap(),
        None,
    )
    .unwrap()
}

fn elapsed(value: u64, unit: Unit) -> Step {
    Step::Duration { value, unit }
}

fn days(value: u64, ambiguity: Ambiguity) -> Step {
    Step::CalendarDays { value, ambiguity }
}

fn months(value: u64, month_end: MonthEnd) -> Step {
    Step::CalendarMonths {
        value,
        month_end,
        ambiguity: Ambiguity::Reject,
    }
}

fn ticks(text: &str, unit: Unit) -> i64 {
    parse_timestamp(text).unwrap().0.ticks(unit).unwrap()
}

fn date(text: &str) -> i64 {
    ticks(text, Unit::Seconds) / 86_400
}

#[test]
fn exact_fixed_grids_preserve_domains_and_floor_negative_epochs() {
    for unit in [
        Unit::Seconds,
        Unit::Milliseconds,
        Unit::Microseconds,
        Unit::Nanoseconds,
    ] {
        for family in ["timestamp", "duration", "time"] {
            let name = format!("{family}({unit})");
            let grid = kernel(&name, elapsed(5, unit), None);
            for (value, expected) in [(0, 0), (4, 0), (5, 5), (6, 5), (17, 15)] {
                assert_eq!(grid.apply(value, None).unwrap(), expected, "{name}");
                assert_eq!(
                    grid.grid_point(grid.bucket_index(value).unwrap()).unwrap(),
                    expected
                );
            }
            assert_eq!(
                grid.output_dtype().unwrap().as_str(),
                TemporalDType::parse(&name).unwrap().as_str()
            );
            if family != "time" {
                for (value, expected) in [(-1, -5), (-5, -5), (-6, -10), (-17, -20)] {
                    assert_eq!(grid.apply(value, None).unwrap(), expected, "{name}");
                }
            }
        }
    }
    for name in ["date32", "date64"] {
        let unit = if name == "date32" { 1 } else { 86_400_000 };
        let grid = kernel(name, elapsed(2 * 86_400, Unit::Seconds), None);
        for (value, expected) in [(-5, -6), (-2, -2), (-1, -2), (0, 0), (3, 2), (4, 4)] {
            assert_eq!(grid.apply(value * unit, None).unwrap(), expected * unit);
        }
    }
}

#[test]
fn widened_fixed_indices_and_neighbors_do_not_narrow_early() {
    let grid = kernel(
        "timestamp(ns)",
        elapsed(1, Unit::Nanoseconds),
        Some(i64::MIN),
    );
    assert_eq!(grid.bucket_index(i64::MAX).unwrap(), i128::from(u64::MAX));
    assert_eq!(grid.grid_point(i128::from(u64::MAX)).unwrap(), i64::MAX);
    for value in [i64::MIN, i64::MIN + 1, -1, 0, 1, i64::MAX - 1, i64::MAX] {
        assert_eq!(grid.apply(value, None).unwrap(), value);
    }
    assert!(grid.grid_point(i128::MAX).is_err());
    let grid = kernel("duration(ns)", elapsed(3, Unit::Nanoseconds), None);
    assert_eq!(grid.apply(i64::MAX, None).unwrap(), i64::MAX - 1);
    assert!(grid.apply(i64::MIN, None).is_err());
    let wide = kernel("timestamp(ns)", elapsed(u64::MAX, Unit::Seconds), None);
    assert_eq!(wide.apply(i64::MAX, None).unwrap(), 0);
    assert!(wide.apply(-1, None).is_err());
}

#[test]
fn grid_binding_rejects_zero_fractional_ticks_and_wrong_domains() {
    for (name, step, origin) in [
        ("timestamp(s)", elapsed(0, Unit::Seconds), None),
        ("timestamp(ms)", elapsed(500, Unit::Microseconds), None),
        ("timestamp(us)", elapsed(1500, Unit::Nanoseconds), None),
        ("date32", elapsed(1, Unit::Seconds), None),
        (
            "date64",
            elapsed(86_400_000_000_001, Unit::Nanoseconds),
            None,
        ),
        ("date32", days(0, Ambiguity::Reject), None),
        ("timestamp(s)", days(u64::MAX, Ambiguity::Reject), None),
        ("duration(ns)", days(1, Ambiguity::Reject), None),
        ("time(ns)", months(1, MonthEnd::Clamp), None),
        ("date32", days(1, Ambiguity::Reject), Some(i64::MAX)),
        ("date64", days(1, Ambiguity::Reject), Some(1)),
        ("time(s)", elapsed(1, Unit::Seconds), Some(-1)),
        ("time(s)", elapsed(1, Unit::Seconds), Some(86_400)),
    ] {
        assert!(
            Kernel::bind(
                Operation::Bucket { step, origin },
                TemporalDType::parse(name).unwrap(),
                None
            )
            .is_err(),
            "{name}: {step:?}"
        );
    }
    let grid = kernel("timestamp(ns)", elapsed(500, Unit::Nanoseconds), Some(123));
    for (value, expected) in [(122, -377), (123, 123), (622, 123), (623, 623)] {
        assert_eq!(grid.apply(value, None).unwrap(), expected);
    }
}

#[test]
fn calendar_months_always_reuse_the_original_anchor() {
    let origin = date("2024-01-31T00:00:00");
    let grid = kernel("date32", months(1, MonthEnd::Clamp), Some(origin));
    for (index, text) in [
        (-2, "2023-11-30T00:00:00"),
        (-1, "2023-12-31T00:00:00"),
        (0, "2024-01-31T00:00:00"),
        (1, "2024-02-29T00:00:00"),
        (2, "2024-03-31T00:00:00"),
        (13, "2025-02-28T00:00:00"),
    ] {
        assert_eq!(grid.grid_point(index).unwrap(), date(text));
        assert_eq!(grid.bucket_index(date(text)).unwrap(), index);
    }
    assert_eq!(
        grid.apply(date("2024-03-30T00:00:00"), None).unwrap(),
        date("2024-02-29T00:00:00")
    );
    let rejecting = kernel("date32", months(1, MonthEnd::Reject), Some(origin));
    assert!(rejecting.apply(origin, None).is_err());
    let leap = kernel(
        "date32",
        months(12, MonthEnd::Clamp),
        Some(date("2024-02-29T00:00:00")),
    );
    assert_eq!(leap.grid_point(1).unwrap(), date("2025-02-28T00:00:00"));
    assert_eq!(leap.grid_point(4).unwrap(), date("2028-02-29T00:00:00"));
}

#[test]
fn calendar_grid_labels_admit_full_naive_domains_and_wide_neighbors() {
    for name in [
        "date32",
        "date64",
        "timestamp(s)",
        "timestamp(ms)",
        "timestamp(us)",
        "timestamp(ns)",
    ] {
        let dtype = TemporalDType::parse(name).unwrap();
        let max = match dtype.kind() {
            Kind::Date32 => i64::from(i32::MAX),
            Kind::Date64 => i64::MAX / 86_400_000 * 86_400_000,
            _ => i64::MAX,
        };
        let grid = kernel(name, days(1, Ambiguity::Reject), None);
        let point = grid.apply(max, None).unwrap();
        assert!(point <= max);
        assert_eq!(grid.apply(point, None).unwrap(), point);
        let min = match dtype.kind() {
            Kind::Date32 => i64::from(i32::MIN),
            Kind::Date64 => i64::MIN / 86_400_000 * 86_400_000,
            _ => i64::MIN,
        };
        let anchored = kernel(name, days(1, Ambiguity::Reject), Some(min));
        assert_eq!(anchored.apply(min, None).unwrap(), min);
    }
    let grid = kernel(
        "timestamp(ns,America/New_York)",
        days(1, Ambiguity::Reject),
        None,
    );
    let label = grid.apply(i64::MAX, None).unwrap();
    assert!(label < i64::MAX);
    assert_eq!(grid.apply(label, None).unwrap(), label);
    let anchored = kernel(
        "timestamp(ns,America/New_York)",
        days(1, Ambiguity::Reject),
        Some(i64::MIN),
    );
    assert_eq!(anchored.apply(i64::MIN, None).unwrap(), i64::MIN);
}

#[test]
fn calendar_grid_uses_local_epoch_and_variable_length_days() {
    let grid = kernel(
        "timestamp(s,America/New_York)",
        days(1, Ambiguity::Reject),
        None,
    );
    assert_eq!(grid.grid_point(0).unwrap(), 18_000);
    for (text, expected, hours) in [
        ("2024-03-10T12:00:00", "2024-03-10T05:00:00", 23),
        ("2024-11-03T12:00:00", "2024-11-03T04:00:00", 25),
    ] {
        let value = ticks(text, Unit::Seconds);
        let index = grid.bucket_index(value).unwrap();
        assert_eq!(
            grid.apply(value, None).unwrap(),
            ticks(expected, Unit::Seconds)
        );
        assert_eq!(
            grid.grid_point(index + 1).unwrap() - grid.grid_point(index).unwrap(),
            hours * 3600
        );
    }
    let fixed = kernel(
        "timestamp(s,America/New_York)",
        elapsed(86_400, Unit::Seconds),
        Some(ticks("2024-03-10T05:00:00", Unit::Seconds)),
    );
    assert_eq!(
        fixed.grid_point(1).unwrap(),
        ticks("2024-03-11T05:00:00", Unit::Seconds)
    );
}

#[test]
fn gap_fold_resolution_is_explicit_and_origin_keeps_its_fold_occurrence() {
    let name = "timestamp(s,America/New_York)";
    let origin = ticks("2024-03-09T07:30:00", Unit::Seconds);
    let value = ticks("2024-03-10T07:00:00", Unit::Seconds);
    let earlier = kernel(name, days(1, Ambiguity::Earlier), Some(origin));
    let later = kernel(name, days(1, Ambiguity::Later), Some(origin));
    assert_eq!(
        earlier.apply(value, None).unwrap(),
        ticks("2024-03-10T06:30:00", Unit::Seconds)
    );
    assert_eq!(later.apply(value, None).unwrap(), origin);
    assert!(
        kernel(name, days(1, Ambiguity::Reject), Some(origin))
            .apply(value, None)
            .is_err()
    );
    let origin = ticks("2024-11-03T06:30:00", Unit::Seconds);
    for policy in [Ambiguity::Reject, Ambiguity::Earlier, Ambiguity::Later] {
        let grid = kernel(name, days(1, policy), Some(origin));
        assert_eq!(grid.grid_point(0).unwrap(), origin);
        assert_eq!(grid.apply(origin, None).unwrap(), origin);
        assert_eq!(
            grid.bucket_index(ticks("2024-11-03T05:45:00", Unit::Seconds))
                .unwrap(),
            -1
        );
        assert_eq!(
            grid.bucket_index(ticks("2024-11-03T06:45:00", Unit::Seconds))
                .unwrap(),
            0
        );
    }
}

#[test]
fn skipped_civil_days_and_provider_limits_fail_deterministically() {
    let origin = ticks("2011-12-29T22:00:00", Unit::Seconds);
    for policy in [Ambiguity::Reject, Ambiguity::Earlier, Ambiguity::Later] {
        let grid = kernel("timestamp(s,Pacific/Apia)", days(1, policy), Some(origin));
        assert!(grid.grid_point(1).is_err());
        // Earlier collapses point 1 onto point 0; Later collapses it onto
        // point 2. A different, strictly advancing interval stays usable.
        let collapsed = if policy == Ambiguity::Earlier {
            origin
        } else {
            ticks("2011-12-30T22:00:00", Unit::Seconds)
        };
        assert!(grid.apply(collapsed, None).is_err(), "{policy:?}");
    }
    assert!(
        Kernel::bind(
            Operation::Bucket {
                step: days(1, Ambiguity::Reject),
                origin: Some(i64::MAX)
            },
            TemporalDType::parse("timestamp(s,America/New_York)").unwrap(),
            None
        )
        .is_err()
    );
    let ordinary = Kernel::bind(
        Operation::Extract(super::super::Part::Day),
        TemporalDType::parse("date32").unwrap(),
        None,
    )
    .unwrap();
    assert!(ordinary.bucket_index(0).is_err());
    assert!(ordinary.grid_point(0).is_err());
}

#[test]
fn deterministic_dense_reference_proves_fixed_and_monthly_membership() {
    // Independent integer division and enumerated civil anchors, not the grid's
    // index-to-point implementation, determine every expected label.
    for width in [1_u64, 3, 7, 91] {
        for origin in [-137_i64, 0, 147] {
            let grid = kernel(
                "duration(ns)",
                elapsed(width, Unit::Nanoseconds),
                Some(origin),
            );
            for value in -777_i64..=777 {
                let delta = i128::from(value) - i128::from(origin);
                let width = i128::from(width);
                let trunc = delta / width;
                let quotient = trunc - i128::from(delta % width < 0);
                assert_eq!(
                    i128::from(grid.apply(value, None).unwrap()),
                    i128::from(origin) + quotient * width
                );
            }
        }
    }
    let grid = kernel(
        "date32",
        months(1, MonthEnd::Clamp),
        Some(date("2023-01-31T00:00:00")),
    );
    let anchors: Vec<i64> = (2022..=2025)
        .flat_map(|year| {
            (1..=12).map(move |month| {
                i64::try_from(civil::days_from_civil(
                    year,
                    month,
                    civil::days_in_month(year, month),
                ))
                .unwrap()
            })
        })
        .collect();
    for value in date("2023-01-01T00:00:00")..date("2025-01-01T00:00:00") {
        let expected = *anchors.iter().rev().find(|&&point| point <= value).unwrap();
        assert_eq!(grid.apply(value, None).unwrap(), expected);
    }
}
