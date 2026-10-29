//! Independent civil fixtures and deterministic temporal RANGE refusals.

use super::*;
use shardloom_core::temporal::civil::MonthEnd;

#[test]
fn calendar_window_month_ends_leap_years_and_negative_epochs_preserve_domains() {
    // UTC civil dates and their clamped +/-1-month boundaries, in epoch days:
    // 1969-12-31; Jan/Feb/Mar 2023; Jan/Feb28/Feb29/Mar/Apr 2024.
    let dates = [
        (-1, -32, 30),
        (19_388, 19_357, 19_416),
        (19_416, 19_385, 19_444),
        (19_447, 19_416, 19_477),
        (19_753, 19_722, 19_782),
        (19_781, 19_750, 19_810),
        (19_782, 19_751, 19_811),
        (19_813, 19_782, 19_843),
        (19_843, 19_812, 19_873),
    ];
    for (name, ticks_per_day) in [
        ("date32", 1),
        ("date64", 86_400_000),
        ("timestamp(s)", 86_400),
        ("timestamp(ms,UTC)", 86_400_000),
        ("timestamp(us)", 86_400_000_000),
        ("timestamp(ns,UTC)", 86_400_000_000_000),
    ] {
        let mut rows: Vec<_> = dates
            .iter()
            .enumerate()
            .map(|(id, &(day, lower, upper))| Row {
                key: Some(day * ticks_per_day),
                lower: i128::from(lower * ticks_per_day),
                upper: i128::from(upper * ticks_per_day),
                group: 0,
                value: Some(i64::try_from(id).unwrap()),
            })
            .collect();
        rows.rotate_left(5);
        let fixture = fixture(name, &rows, 2);
        for descending in [false, true] {
            let plan = plan(
                fixture.scan(),
                &Offset::CalendarMonths {
                    months: 1,
                    month_end: MonthEnd::Clamp,
                    ambiguity: Ambiguity::Reject,
                },
                Exclusion::NoOthers,
                descending,
                NullOrder::Last,
                true,
            );
            verify(
                &fixture,
                &plan,
                &reference(
                    &rows,
                    1,
                    Exclusion::NoOthers,
                    descending,
                    NullOrder::Last,
                    true,
                ),
                true,
            );
        }
    }
}

#[test]
fn calendar_window_zero_offsets_preserve_fold_occurrence_and_empty_types() {
    let rows: Vec<_> = [Some(FOLD[8].0), Some(FOLD[10].0), Some(FOLD[8].0), None]
        .into_iter()
        .map(|key| Row {
            key,
            lower: i128::from(key.unwrap_or(0)),
            upper: i128::from(key.unwrap_or(0)),
            group: 0,
            value: Some(1),
        })
        .collect();
    for offset in [
        Offset::DurationMicros(0),
        Offset::Duration {
            value: 0,
            unit: TemporalUnit::Nanoseconds,
        },
        Offset::CalendarDays {
            days: 0,
            ambiguity: Ambiguity::Reject,
        },
        Offset::CalendarMonths {
            months: 0,
            month_end: MonthEnd::Reject,
            ambiguity: Ambiguity::Reject,
        },
    ] {
        for population in [rows.as_slice(), &[]] {
            let fixture = fixture("timestamp(s,America/New_York)", population, 1);
            let base = plan(
                fixture.scan(),
                &offset,
                Exclusion::NoOthers,
                false,
                NullOrder::Last,
                true,
            );
            let expected = reference(
                population,
                1,
                Exclusion::NoOthers,
                false,
                NullOrder::Last,
                true,
            );
            for (start, end) in [
                (
                    Bound::Preceding(offset.clone()),
                    Bound::Following(offset.clone()),
                ),
                (Bound::Following(offset.clone()), Bound::CurrentRow),
                (Bound::CurrentRow, Bound::Preceding(offset.clone())),
                (
                    Bound::Following(offset.clone()),
                    Bound::Preceding(offset.clone()),
                ),
            ] {
                let mut plan = base.clone();
                let VortexRelationalPlan::Window(window) = &mut plan else {
                    panic!("window fixture")
                };
                for expression in &mut window.expressions {
                    let frame = expression.frame.as_mut().unwrap();
                    frame.start = start.clone();
                    frame.end = end.clone();
                }
                // Empty windows have no bounds records; still check both strategies and schema.
                let resident = prepare_relational(&plan, policy()).unwrap();
                let spilled = ordered(&fixture, &plan);
                assert_eq!(resident.output_dtype(), spilled.output_dtype());
                assert_rows(
                    &json_rows(
                        &resident
                            .collect_jsonl(&CancellationToken::default())
                            .unwrap(),
                    ),
                    &expected,
                );
                assert_rows(&complete(&spilled, 1).0, &expected);
            }
        }
    }
}

#[test]
fn calendar_window_elapsed_extremes_use_widened_arithmetic() {
    for name in ["timestamp(s)", "timestamp(ns)", "duration(s)"] {
        let nanos = if name.contains("(ns)") {
            1
        } else {
            1_000_000_000i128
        };
        let distance = i128::from(u64::MAX) * 1_000_000_000;
        let rows: Vec<_> = [i64::MIN, -1, 0, i64::MAX]
            .into_iter()
            .map(|key| Row {
                key: Some(key),
                lower: i128::from(key) * nanos - distance,
                upper: i128::from(key) * nanos + distance,
                group: 0,
                value: Some(1),
            })
            .collect();
        let fixture = fixture(name, &rows, 1);
        let plan = plan(
            fixture.scan(),
            &Offset::Duration {
                value: u64::MAX,
                unit: TemporalUnit::Seconds,
            },
            Exclusion::NoOthers,
            false,
            NullOrder::Last,
            false,
        );
        verify(
            &fixture,
            &plan,
            &reference(
                &rows,
                nanos,
                Exclusion::NoOthers,
                false,
                NullOrder::Last,
                false,
            ),
            true,
        );
    }
}

#[test]
fn calendar_window_refusals_release_owners_and_never_certify_completion() {
    let days = Offset::CalendarDays {
        days: 1,
        ambiguity: Ambiguity::Reject,
    };
    let months = Offset::CalendarMonths {
        months: 1,
        month_end: MonthEnd::Reject,
        ambiguity: Ambiguity::Reject,
    };
    for (name, key, offset, binding, message) in [
        ("duration(s)", 0, days.clone(), true, "temporal operation"),
        ("time(s)", 0, months.clone(), true, "temporal operation"),
        (
            "date32",
            0,
            Offset::DurationMicros(1),
            true,
            "whole elapsed days",
        ),
        (
            "timestamp(s,America/New_York)",
            1_730_701_800,
            days.clone(),
            false,
            "ambiguous",
        ),
        (
            "timestamp(s,America/New_York)",
            1_710_138_600,
            days.clone(),
            false,
            "nonexistent",
        ),
        ("date32", 19_753, months, false, "month"),
        (
            "timestamp(s)",
            i64::MAX,
            days,
            false,
            "exceeds signed 64-bit ticks",
        ),
    ] {
        let row = Row {
            key: Some(key),
            lower: 0,
            upper: 0,
            group: 0,
            value: Some(1),
        };
        let fixture = fixture(name, &[row], 1);
        let plan = plan(
            fixture.scan(),
            &offset,
            Exclusion::NoOthers,
            false,
            NullOrder::Last,
            false,
        );
        if binding {
            let error = prepare_relational(&plan, policy())
                .err()
                .expect("invalid domain must fail at binding");
            assert!(error.to_string().contains(message), "{name}: {error}");
            continue;
        }
        for spilled in [false, true] {
            let prepared = if spilled {
                ordered(&fixture, &plan)
            } else {
                prepare_relational(&plan, policy()).unwrap()
            };
            let baseline = prepared.snapshot().memory.reserved_bytes;
            let error = prepared
                .collect_jsonl(&CancellationToken::default())
                .err()
                .expect("calendar policy/range failure");
            assert!(error.to_string().contains(message), "{name}: {error}");
            assert_eq!(prepared.snapshot().completed_executions, 0);
            assert_eq!(prepared.snapshot().memory.reserved_bytes, baseline);
            if spilled {
                assert_eq!(
                    fs::read_dir(fixture.0.join("window-runs")).unwrap().count(),
                    0
                );
            }
        }
    }
}
