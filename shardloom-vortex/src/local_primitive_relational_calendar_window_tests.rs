//! Calendar membership uses independent epoch-boundary fixtures, never the
//! production temporal kernel as its reference. Both execution modes agree.

use super::*;
use crate::relational_query::{
    VortexRelationalFrameBound as Bound, VortexRelationalFrameOffset as Offset,
    VortexRelationalOrderKey, VortexRelationalWindow, VortexRelationalWindowExpression,
};
use shardloom_core::temporal::{Ambiguity, TemporalDType, TemporalUnit};
use vortex::array::arrays::{DictArray, ExtensionArray, VarBinViewArray};

#[path = "local_primitive_relational_calendar_window_edge_tests.rs"]
mod edge_tests;

#[path = "local_primitive_relational_calendar_window_pressure_tests.rs"]
mod pressure_tests;

#[derive(Clone)]
struct Row {
    key: Option<i64>,
    lower: i128,
    upper: i128,
    group: u8,
    value: Option<i64>,
}

fn column(name: &str) -> ColumnRef {
    ColumnRef::new(name).unwrap()
}

fn fixture(name: &str, rows: &[Row], chunk: usize) -> Fixture {
    let dtype = crate::native_temporal::vortex_dtype(
        &TemporalDType::parse(name).unwrap(),
        Nullability::Nullable,
    )
    .unwrap();
    let storage = if crate::native_temporal::temporal_storage(&dtype) == Some(PType::I32) {
        PrimitiveArray::from_option_iter(
            rows.iter()
                .map(|row| row.key.map(|value| i32::try_from(value).unwrap())),
        )
        .into_array()
    } else {
        PrimitiveArray::from_option_iter(rows.iter().map(|row| row.key)).into_array()
    };
    let DType::Extension(extension) = dtype else {
        panic!("temporal domain")
    };
    let groups = DictArray::try_new(
        PrimitiveArray::from_iter(rows.iter().map(|row| row.group)).into_array(),
        VarBinViewArray::from_iter_nullable_str([Some("東京"), None]).into_array(),
    )
    .unwrap()
    .into_array();
    Fixture::new(
        StructArray::new(
            FieldNames::from(["id", "group", "time", "value"]),
            vec![
                PrimitiveArray::from_iter(0..rows.len() as u64).into_array(),
                groups,
                ExtensionArray::try_new(extension, storage)
                    .unwrap()
                    .into_array(),
                PrimitiveArray::from_option_iter(rows.iter().map(|row| row.value)).into_array(),
            ],
            rows.len(),
            Validity::NonNullable,
        )
        .into_array(),
        chunk,
    )
}

fn functions() -> Vec<(&'static str, Function)> {
    let mut result = vec![
        ("n", Function::CountAll),
        ("nonnull", Function::Count(column("value"))),
        ("distinct", Function::CountDistinct(column("value"))),
        ("sum", Function::Sum(column("value"))),
        ("mean", Function::Avg(column("value"))),
        ("min", Function::Min(column("value"))),
        ("max", Function::Max(column("value"))),
        ("first", Function::FirstValue(column("value"))),
        ("last", Function::LastValue(column("value"))),
        (
            "second",
            Function::NthValue {
                column: column("value"),
                index: 2,
            },
        ),
    ];
    for function in ["var_pop", "median", "sum_distinct", "list"] {
        result.push((
            function,
            Function::Aggregate(Box::new(
                crate::query_primitive::VortexSimpleAggregateMeasure::new(
                    function,
                    Some(column("value")),
                    function.into(),
                ),
            )),
        ));
    }
    result
}

fn plan(
    input: VortexRelationalPlan,
    offset: &Offset,
    exclusion: Exclusion,
    descending: bool,
    nulls: NullOrder,
    broad: bool,
) -> VortexRelationalPlan {
    let mut functions = functions();
    if !broad {
        functions.truncate(4);
    }
    VortexRelationalPlan::Window(Box::new(VortexRelationalWindow {
        input,
        columns: vec![column("id")],
        expressions: functions
            .into_iter()
            .map(|(name, function)| VortexRelationalWindowExpression {
                output_column: name.into(),
                function: VortexRelationalWindowFunction::Framed(function),
                partition_by: vec![column("group")],
                order_by: vec![VortexRelationalOrderKey {
                    column: column("time"),
                    descending,
                    nulls: Some(nulls),
                }],
                frame: Some(Frame {
                    unit: Unit::Range,
                    start: Bound::Preceding(offset.clone()),
                    end: Bound::Following(offset.clone()),
                    exclusion,
                }),
            })
            .collect(),
    }))
}

#[allow(clippy::cast_precision_loss)] // Independent small integer fixture arithmetic.
fn reference(
    rows: &[Row],
    tick_nanos: i128,
    exclusion: Exclusion,
    descending: bool,
    nulls: NullOrder,
    broad: bool,
) -> Vec<Value> {
    rows.iter().enumerate().map(|(id, row)| {
        let mut members: Vec<_> = rows.iter().enumerate().filter(|(other_id, other)| {
            if other.group != row.group { return false; }
            let inside = match (row.key, other.key) {
                (None, None) => true,
                (Some(_), Some(key)) => row.lower <= i128::from(key) * tick_nanos && i128::from(key) * tick_nanos <= row.upper,
                _ => false,
            };
            inside && match exclusion {
                Exclusion::NoOthers => true,
                Exclusion::CurrentRow => *other_id != id,
                Exclusion::Group => other.key != row.key,
                Exclusion::Ties => *other_id == id || other.key != row.key,
            }
        }).collect();
        members.sort_by(|(left_id, left), (right_id, right)| {
            let order = match (left.key, right.key) {
                (None, None) => std::cmp::Ordering::Equal,
                (None, Some(_)) => if nulls == NullOrder::First { std::cmp::Ordering::Less } else { std::cmp::Ordering::Greater },
                (Some(_), None) => if nulls == NullOrder::First { std::cmp::Ordering::Greater } else { std::cmp::Ordering::Less },
                (Some(left), Some(right)) => if descending { right.cmp(&left) } else { left.cmp(&right) },
            };
            order.then_with(|| left_id.cmp(right_id))
        });
        let mut values: Vec<_> = members.iter().filter_map(|(_, row)| row.value).collect();
        let total = values.iter().sum::<i64>() as f64;
        let mean = (!values.is_empty()).then(|| total / values.len() as f64);
        let variance = mean.map(|mean| values.iter().map(|&value| (value as f64 - mean).powi(2)).sum::<f64>() / values.len() as f64);
        values.sort_unstable();
        let median = (!values.is_empty()).then(|| {
            (values[(values.len()-1)/2] + values[values.len()/2]) as f64 / 2.0
        });
        let nonnull = values.len();
        values.dedup();
        let mut result = json!({"id":id,"n":members.len(),"nonnull":nonnull,"distinct":values.len(),
            "sum": if nonnull == 0 { None } else { Some(total) }});
        if broad {
            let object = result.as_object_mut().unwrap();
            for (name, value) in [
                ("mean", json!(mean)), ("min", json!(values.first())), ("max", json!(values.last())),
                ("first", json!(members.first().and_then(|(_, row)| row.value))),
                ("last", json!(members.last().and_then(|(_, row)| row.value))),
                ("second", json!(members.get(1).and_then(|(_, row)| row.value))),
                ("var_pop", json!(variance)), ("median", json!(median)),
                ("sum_distinct", json!((nonnull != 0).then(|| values.iter().sum::<i64>() as f64))),
                ("list", json!((!members.is_empty()).then(|| members.iter().map(|(_, row)| row.value).collect::<Vec<_>>()))),
            ] { object.insert(name.into(), value); }
        }
        result
    }).collect()
}

fn assert_rows(actual: &[Value], expected: &[Value]) {
    assert_eq!(actual.len(), expected.len());
    for (actual, expected) in actual.iter().zip(expected) {
        let mut actual = actual.clone();
        let mut expected = expected.clone();
        for name in ["mean", "var_pop"] {
            if let (Some(left), Some(right)) = (actual[name].as_f64(), expected[name].as_f64()) {
                assert!((left - right).abs() < 1e-12, "{actual} != {expected}");
                actual.as_object_mut().unwrap().remove(name);
                expected.as_object_mut().unwrap().remove(name);
            }
        }
        assert_eq!(actual, expected);
    }
}

fn verify(fixture: &Fixture, plan: &VortexRelationalPlan, expected: &[Value], spilled: bool) {
    let resident = prepare_relational(plan, policy()).unwrap();
    let baseline = resident.snapshot().memory.reserved_bytes;
    assert_rows(
        &json_rows(
            &resident
                .collect_jsonl(&CancellationToken::default())
                .unwrap(),
        ),
        expected,
    );
    assert_eq!(resident.snapshot().memory.reserved_bytes, baseline);
    if spilled {
        let stored = ordered(fixture, plan);
        let baseline = stored.snapshot().memory.reserved_bytes;
        let (rows, report) = complete(&stored, 3);
        assert_rows(&rows, expected);
        assert_eq!(stored.output_dtype(), resident.output_dtype());
        assert!(report.ordered_window_bounds_rows > 0);
        drop(report);
        assert_eq!(stored.snapshot().memory.reserved_bytes, baseline);
    }
}

#[test]
fn calendar_window_fixed_durations_preserve_every_temporal_unit_without_rounding() {
    for unit in [
        TemporalUnit::Seconds,
        TemporalUnit::Milliseconds,
        TemporalUnit::Microseconds,
        TemporalUnit::Nanoseconds,
    ] {
        for name in [
            format!("timestamp({unit})"),
            format!("timestamp({unit},America/Chicago)"),
            format!("time({unit})"),
            format!("duration({unit})"),
        ] {
            for (offset, nanos) in [
                (Offset::DurationMicros(1_500_000), 1_500_000_000i128),
                (
                    Offset::Duration {
                        value: 500,
                        unit: TemporalUnit::Nanoseconds,
                    },
                    500,
                ),
            ] {
                let second = unit.ticks_per_second();
                let rows: Vec<_> = [
                    Some(second * 3),
                    Some(0),
                    Some(second),
                    None,
                    Some(second + 1),
                    Some(second),
                    Some(second * 2),
                    None,
                ]
                .into_iter()
                .enumerate()
                .map(|(id, key)| Row {
                    key,
                    lower: i128::from(key.unwrap_or(0)) * unit.nanoseconds() - nanos,
                    upper: i128::from(key.unwrap_or(0)) * unit.nanoseconds() + nanos,
                    group: u8::from(id >= 5),
                    value: (id != 2).then_some(i64::try_from(id % 3).unwrap()),
                })
                .collect();
                let fixture = fixture(&name, &rows, 2);
                let plan = plan(
                    fixture.scan(),
                    &offset,
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
                        unit.nanoseconds(),
                        Exclusion::NoOthers,
                        false,
                        NullOrder::Last,
                        false,
                    ),
                    true,
                );
            }
        }
    }
}

#[test]
fn calendar_window_fixed_date_ranges_use_whole_elapsed_days() {
    for (name, ticks, nanos) in [
        ("date32", 1, 86_400_000_000_000i128),
        ("date64", 86_400_000, 1_000_000),
    ] {
        let distance = 86_400_000_000_000i128;
        let rows: Vec<_> = [-2, 0, 1, 3, 1]
            .into_iter()
            .map(|day| Row {
                key: Some(day * ticks),
                lower: i128::from(day * ticks) * nanos - distance,
                upper: i128::from(day * ticks) * nanos + distance,
                group: 0,
                value: Some(day),
            })
            .collect();
        let fixture = fixture(name, &rows, 2);
        let plan = plan(
            fixture.scan(),
            &Offset::DurationMicros(86_400_000_000),
            Exclusion::NoOthers,
            true,
            NullOrder::First,
            false,
        );
        verify(
            &fixture,
            &plan,
            &reference(
                &rows,
                nanos,
                Exclusion::NoOthers,
                true,
                NullOrder::First,
                false,
            ),
            true,
        );
    }
}

// UTC instants and their local-calendar +/-1-day boundaries at the 2024 New
// York fall transition. Independently calculated from the documented -04/-05
// offsets (ambiguous destination chooses the earlier occurrence).
const FOLD: [(i64, i64, i64); 18] = [
    (1_730_520_000, 1_730_433_600, 1_730_606_400),
    (1_730_523_600, 1_730_437_200, 1_730_610_000),
    (1_730_525_400, 1_730_439_000, 1_730_611_800),
    (1_730_527_200, 1_730_440_800, 1_730_617_200),
    (1_730_529_000, 1_730_442_600, 1_730_619_000),
    (1_730_530_800, 1_730_444_400, 1_730_620_800),
    (1_730_606_400, 1_730_520_000, 1_730_696_400),
    (1_730_610_000, 1_730_523_600, 1_730_700_000),
    (1_730_611_800, 1_730_525_400, 1_730_701_800),
    (1_730_613_600, 1_730_523_600, 1_730_700_000),
    (1_730_615_400, 1_730_525_400, 1_730_701_800),
    (1_730_617_200, 1_730_527_200, 1_730_703_600),
    (1_730_696_400, 1_730_606_400, 1_730_782_800),
    (1_730_700_000, 1_730_610_000, 1_730_786_400),
    (1_730_701_800, 1_730_611_800, 1_730_788_200),
    (1_730_703_600, 1_730_617_200, 1_730_790_000),
    (1_730_705_400, 1_730_619_000, 1_730_791_800),
    (1_730_707_200, 1_730_620_800, 1_730_793_600),
];

#[test]
fn calendar_window_fold_retreats_match_independent_membership_in_both_native_strategies() {
    assert!(
        FOLD.windows(2)
            .any(|rows| rows[0].1 > rows[1].1 && rows[0].2 > rows[1].2)
    );
    let mut rows: Vec<_> = FOLD
        .iter()
        .enumerate()
        .map(|(id, &(key, lower, upper))| Row {
            key: Some(key),
            lower: i128::from(lower),
            upper: i128::from(upper),
            group: 0,
            value: (id % 5 != 0).then_some(i64::try_from(id % 7).unwrap() - 3),
        })
        .collect();
    rows.push(rows[8].clone());
    for id in [2, 7, 8, 9, 10, 14] {
        let mut row = rows[id].clone();
        row.group = 1;
        rows.push(row);
    }
    for group in [0, 0, 1] {
        rows.push(Row {
            key: None,
            lower: 0,
            upper: 0,
            group,
            value: Some(9),
        });
    }
    // Unsorted input crosses source batches; output still retains original ids.
    rows.rotate_left(9);
    rows.swap(2, 18);
    let fixture = fixture("timestamp(s,America/New_York)", &rows, 3);
    for descending in [false, true] {
        for nulls in [NullOrder::First, NullOrder::Last] {
            for exclusion in [
                Exclusion::NoOthers,
                Exclusion::CurrentRow,
                Exclusion::Group,
                Exclusion::Ties,
            ] {
                let plan = plan(
                    fixture.scan(),
                    &Offset::CalendarDays {
                        days: 1,
                        ambiguity: Ambiguity::Earlier,
                    },
                    exclusion,
                    descending,
                    nulls,
                    true,
                );
                verify(
                    &fixture,
                    &plan,
                    &reference(&rows, 1, exclusion, descending, nulls, true),
                    true,
                );
            }
        }
    }
}
