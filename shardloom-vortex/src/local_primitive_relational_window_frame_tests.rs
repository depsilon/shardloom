use super::*;
use crate::relational_query::{
    VortexRelationalFrameBound as Bound, VortexRelationalFrameExclusion as Exclusion,
    VortexRelationalFrameFunction as Function, VortexRelationalFrameOffset as Offset,
    VortexRelationalFrameUnit as Unit, VortexRelationalNullOrder as NullOrder,
    VortexRelationalOrderKey as OrderKey, VortexRelationalWindow as Window,
    VortexRelationalWindowExpression as WindowExpression, VortexRelationalWindowFrame as Frame,
    VortexRelationalWindowFunction,
};
use serde_json::{Value, json};
use shardloom_core::ScalarValue;
use vortex::array::arrays::{DictArray, VarBinViewArray};

const DATA: [(u8, Option<i64>, Option<i64>); 10] = [
    (0, Some(3), Some(7)),
    (1, Some(1), Some(4)),
    (0, Some(1), None),
    (0, Some(1), Some(7)),
    (0, Some(5), Some(-2)),
    (1, None, None),
    (0, None, Some(3)),
    (1, Some(2), Some(4)),
    (1, Some(2), Some(9)),
    (0, Some(4), Some(1)),
];

fn column(name: &str) -> ColumnRef {
    ColumnRef::new(name).unwrap()
}
fn distance(value: u64) -> Offset {
    Offset::Number(ScalarValue::UInt64(value))
}

fn fixture(chunk: usize) -> Fixture {
    let groups = DictArray::try_new(
        PrimitiveArray::from_iter(DATA.iter().map(|row| row.0)).into_array(),
        VarBinViewArray::from_iter_nullable_str([Some("東京"), None]).into_array(),
    )
    .unwrap()
    .into_array();
    Fixture::new(
        StructArray::new(
            FieldNames::from(["id", "group", "priority", "value"]),
            vec![
                PrimitiveArray::from_iter(0u64..10).into_array(),
                groups,
                PrimitiveArray::from_option_iter(DATA.iter().map(|row| row.1)).into_array(),
                PrimitiveArray::from_option_iter(DATA.iter().map(|row| row.2)).into_array(),
            ],
            DATA.len(),
            Validity::NonNullable,
        )
        .into_array(),
        chunk,
    )
}

fn functions() -> Vec<(&'static str, Function)> {
    vec![
        ("count_all", Function::CountAll),
        ("nonnull", Function::Count(column("value"))),
        ("distinct", Function::CountDistinct(column("value"))),
        ("sum", Function::Sum(column("value"))),
        ("avg", Function::Avg(column("value"))),
        ("min", Function::Min(column("value"))),
        ("max", Function::Max(column("value"))),
        ("first", Function::FirstValue(column("value"))),
        ("last", Function::LastValue(column("value"))),
        (
            "nth",
            Function::NthValue {
                column: column("value"),
                index: 2,
            },
        ),
    ]
}

fn plan(
    input: VortexRelationalPlan,
    unit: Unit,
    exclusion: Exclusion,
    descending: bool,
    nulls: NullOrder,
) -> VortexRelationalPlan {
    VortexRelationalPlan::Window(Box::new(Window {
        input,
        columns: vec![column("id")],
        expressions: functions()
            .into_iter()
            .map(|(name, function)| WindowExpression {
                output_column: name.into(),
                function: VortexRelationalWindowFunction::Framed(function),
                partition_by: vec![column("group")],
                order_by: vec![OrderKey {
                    column: column("priority"),
                    descending,
                    nulls: Some(nulls),
                }],
                frame: Some(Frame {
                    unit,
                    start: Bound::Preceding(distance(1)),
                    end: Bound::Following(distance(1)),
                    exclusion,
                }),
            })
            .collect(),
    }))
}

#[allow(clippy::cast_precision_loss)] // Reference integers are small exact F64 values.
fn reference(unit: Unit, exclusion: Exclusion, descending: bool, nulls: NullOrder) -> Vec<Value> {
    DATA.iter().enumerate().map(|(id, current)| {
        let mut ordered = DATA.iter().enumerate().filter(|(_, row)| row.0 == current.0)
            .map(|(id, _)| id).collect::<Vec<_>>();
        ordered.sort_by(|&left, &right| {
            let value = match (DATA[left].1, DATA[right].1) {
                (None, None) => std::cmp::Ordering::Equal,
                (None, Some(_)) => if nulls == NullOrder::First { std::cmp::Ordering::Less } else { std::cmp::Ordering::Greater },
                (Some(_), None) => if nulls == NullOrder::First { std::cmp::Ordering::Greater } else { std::cmp::Ordering::Less },
                (Some(left), Some(right)) => if descending { right.cmp(&left) } else { left.cmp(&right) },
            };
            value.then(left.cmp(&right))
        });
        let position = ordered.iter().position(|&row| row == id).unwrap();
        let mut groups = Vec::new();
        let mut group = 0usize;
        for (position, &row) in ordered.iter().enumerate() {
            if position != 0 && DATA[row].1 != DATA[ordered[position - 1]].1 { group += 1; }
            groups.push(group);
        }
        let selected = ordered.iter().enumerate().filter(|&(candidate, &row)| {
            let in_bounds = match unit {
                Unit::Rows => candidate.abs_diff(position) <= 1,
                Unit::Groups => groups[candidate].abs_diff(groups[position]) <= 1,
                Unit::Range => match (DATA[row].1, current.1) {
                    (None, None) => true, (Some(left), Some(right)) => left.abs_diff(right) <= 1, _ => false,
                },
            };
            in_bounds && match exclusion {
                Exclusion::NoOthers => true,
                Exclusion::CurrentRow => row != id,
                Exclusion::Group => DATA[row].1 != current.1,
                Exclusion::Ties => DATA[row].1 != current.1 || row == id,
            }
        }).map(|(_, &row)| row).collect::<Vec<_>>();
        let values = selected.iter().filter_map(|&row| DATA[row].2).collect::<Vec<_>>();
        let distinct = values.iter().copied().collect::<std::collections::BTreeSet<_>>().len();
        let sum = (!values.is_empty()).then(|| values.iter().sum::<i64>() as f64);
        json!({"id":id, "count_all":selected.len(), "nonnull":values.len(), "distinct":distinct,
            "sum":sum, "avg":sum.map(|sum| sum / values.len() as f64), "min":values.iter().min(), "max":values.iter().max(),
            "first":selected.first().and_then(|&row| DATA[row].2), "last":selected.last().and_then(|&row| DATA[row].2),
            "nth":selected.get(1).and_then(|&row| DATA[row].2)})
    }).collect()
}

#[test]
fn native_window_frames_match_independent_membership_with_peers_exclusions_and_chunks() {
    for chunk in [1, 3, 64] {
        let fixture = fixture(chunk);
        for unit in [Unit::Rows, Unit::Groups, Unit::Range] {
            for exclusion in [
                Exclusion::NoOthers,
                Exclusion::CurrentRow,
                Exclusion::Group,
                Exclusion::Ties,
            ] {
                for descending in [false, true] {
                    for nulls in [NullOrder::First, NullOrder::Last] {
                        let prepared = prepare_relational(
                            &plan(fixture.scan(), unit, exclusion, descending, nulls),
                            policy(),
                        )
                        .unwrap();
                        let PreparedRoot::Bound(root) = &prepared.root else {
                            panic!("static window");
                        };
                        let NodeKind::Window { spec, .. } = &root.kind else {
                            panic!("window");
                        };
                        assert_eq!(spec.groups.len(), 1);
                        let baseline = prepared.snapshot().memory.reserved_bytes;
                        for call in 1..=2 {
                            let collected = prepared
                                .collect_jsonl(&CancellationToken::default())
                                .unwrap();
                            assert_eq!(
                                json_rows(&collected),
                                reference(unit, exclusion, descending, nulls),
                                "{chunk} {unit:?} {exclusion:?} {descending} {nulls:?}"
                            );
                            assert_eq!(collected.execution.runtime.completed_executions, call);
                            assert_eq!(collected.execution.runtime.prepared_source_opens, 1);
                            assert!(
                                !collected
                                    .execution
                                    .native_io_certificate
                                    .side_effects
                                    .fallback_attempted
                            );
                        }
                        assert_eq!(prepared.snapshot().memory.reserved_bytes, baseline);
                    }
                }
            }
        }
    }
}

fn one(
    fixture: &Fixture,
    function: VortexRelationalWindowFunction,
    frame: Option<Frame>,
    order: Option<&str>,
) -> VortexRelationalPlan {
    VortexRelationalPlan::Window(Box::new(Window {
        input: fixture.scan(),
        columns: vec![],
        expressions: vec![WindowExpression {
            output_column: "out".into(),
            function,
            partition_by: vec![],
            order_by: order
                .map(|name| OrderKey {
                    column: column(name),
                    descending: false,
                    nulls: Some(NullOrder::Last),
                })
                .into_iter()
                .collect(),
            frame,
        }],
    }))
}

fn collect(plan: &VortexRelationalPlan) -> Vec<Value> {
    let prepared = prepare_relational(plan, policy()).unwrap();
    json_rows(
        &prepared
            .collect_jsonl(&CancellationToken::default())
            .unwrap(),
    )
    .into_iter()
    .map(|row| row["out"].clone())
    .collect()
}

#[test]
fn native_window_frames_defaults_empty_intervals_and_extreme_offsets() {
    let fixture = fixture(2);
    let framed = |function| VortexRelationalWindowFunction::Framed(function);
    assert_eq!(
        collect(&one(&fixture, framed(Function::CountAll), None, None)),
        vec![json!(10); 10]
    );
    assert_eq!(
        collect(&one(
            &fixture,
            framed(Function::CountAll),
            None,
            Some("priority")
        )),
        [6, 3, 3, 3, 8, 10, 10, 5, 5, 7].map(|value| json!(value))
    );
    for (start, end) in [
        (Bound::Preceding(distance(1)), Bound::Preceding(distance(2))),
        (
            Bound::Following(distance(u64::MAX)),
            Bound::UnboundedFollowing,
        ),
    ] {
        for function in [
            Function::CountAll,
            Function::Sum(column("value")),
            Function::FirstValue(column("value")),
            Function::NthValue {
                column: column("value"),
                index: u64::MAX,
            },
        ] {
            let expected = if function == Function::CountAll {
                json!(0)
            } else {
                Value::Null
            };
            assert_eq!(
                collect(&one(
                    &fixture,
                    framed(function),
                    Some(Frame {
                        unit: Unit::Rows,
                        start: start.clone(),
                        end: end.clone(),
                        exclusion: Exclusion::NoOthers,
                    }),
                    Some("id")
                )),
                vec![expected; DATA.len()]
            );
        }
    }
    let excluded = Frame {
        unit: Unit::Range,
        start: Bound::UnboundedPreceding,
        end: Bound::UnboundedFollowing,
        exclusion: Exclusion::Group,
    };
    assert_eq!(
        collect(&one(
            &fixture,
            framed(Function::CountAll),
            Some(excluded),
            None
        )),
        vec![json!(0); 10]
    );
    let ignored = Frame {
        unit: Unit::Rows,
        start: Bound::CurrentRow,
        end: Bound::CurrentRow,
        exclusion: Exclusion::CurrentRow,
    };
    assert_eq!(
        collect(&one(
            &fixture,
            VortexRelationalWindowFunction::RowNumber,
            Some(ignored),
            Some("id")
        )),
        (1..=10).map(|value| json!(value)).collect::<Vec<_>>()
    );
}

#[test]
fn native_window_frames_reject_invalid_policy_on_empty_input() {
    let fixture = Fixture::new(
        single(
            "value",
            PrimitiveArray::from_iter(Vec::<f64>::new()).into_array(),
        ),
        1,
    );
    let base = Frame {
        unit: Unit::Rows,
        start: Bound::UnboundedPreceding,
        end: Bound::CurrentRow,
        exclusion: Exclusion::NoOthers,
    };
    for frame in [
        Frame {
            start: Bound::UnboundedFollowing,
            ..base.clone()
        },
        Frame {
            end: Bound::UnboundedPreceding,
            ..base.clone()
        },
        Frame {
            start: Bound::Following(distance(1)),
            ..base.clone()
        },
        Frame {
            start: Bound::Preceding(Offset::Number(ScalarValue::Int64(-1))),
            ..base.clone()
        },
        Frame {
            start: Bound::Preceding(Offset::Number(ScalarValue::Float64(1.5))),
            ..base.clone()
        },
        Frame {
            start: Bound::Preceding(Offset::DurationMicros(1)),
            ..base.clone()
        },
        Frame {
            unit: Unit::Range,
            start: Bound::Preceding(Offset::Number(ScalarValue::Float64(f64::NAN))),
            ..base.clone()
        },
        Frame {
            unit: Unit::Range,
            start: Bound::Preceding(Offset::Number(ScalarValue::Float64(-1.0))),
            ..base.clone()
        },
    ] {
        for function in [
            VortexRelationalWindowFunction::Framed(Function::CountAll),
            VortexRelationalWindowFunction::Rank,
        ] {
            assert!(
                prepare_relational(
                    &one(&fixture, function, Some(frame.clone()), Some("value")),
                    policy()
                )
                .is_err()
            );
        }
    }
    for unit in [Unit::Range, Unit::Groups] {
        let frame = Frame {
            unit,
            start: Bound::Preceding(distance(1)),
            ..base.clone()
        };
        assert!(
            prepare_relational(
                &one(
                    &fixture,
                    VortexRelationalWindowFunction::Framed(Function::CountAll),
                    Some(frame),
                    None
                ),
                policy()
            )
            .is_err()
        );
    }
    assert!(
        prepare_relational(
            &one(
                &fixture,
                VortexRelationalWindowFunction::Framed(Function::NthValue {
                    column: column("value"),
                    index: 0,
                }),
                None,
                None
            ),
            policy()
        )
        .is_err()
    );
}

#[test]
fn native_window_frames_empty_output_retains_aggregate_type() {
    let fixture = Fixture::new(
        single(
            "value",
            PrimitiveArray::from_iter(Vec::<f64>::new()).into_array(),
        ),
        1,
    );
    let prepared = prepare_relational(
        &one(
            &fixture,
            VortexRelationalWindowFunction::Framed(Function::Sum(column("value"))),
            None,
            None,
        ),
        policy(),
    )
    .unwrap();
    let result = prepared.execute_owned().unwrap();
    assert_eq!(result.execution.output_rows, 0);
    assert_eq!(
        result.result.dtype(),
        &DType::struct_(
            [("out", DType::Primitive(PType::F64, Nullability::Nullable))],
            Nullability::NonNullable
        )
    );
}

#[test]
fn native_window_frames_float_removal_oversized_means_and_range_rounding() {
    let fixture = Fixture::new(
        StructArray::new(
            FieldNames::from(["id", "value"]),
            vec![
                PrimitiveArray::from_iter([0u64, 1, 2]).into_array(),
                PrimitiveArray::from_iter([1e300, 1.0, 0.0]).into_array(),
            ],
            3,
            Validity::NonNullable,
        )
        .into_array(),
        1,
    );
    let frame = Frame {
        unit: Unit::Rows,
        start: Bound::Preceding(distance(1)),
        end: Bound::CurrentRow,
        exclusion: Exclusion::NoOthers,
    };
    assert_eq!(
        collect(&one(
            &fixture,
            VortexRelationalWindowFunction::Framed(Function::Sum(column("value"))),
            Some(frame),
            Some("id")
        )),
        vec![json!(1e300), json!(1e300), json!(1.0)]
    );
    let maximum = Fixture::new(
        single(
            "value",
            PrimitiveArray::from_iter([f64::MAX, f64::MAX]).into_array(),
        ),
        1,
    );
    assert_eq!(
        collect(&one(
            &maximum,
            VortexRelationalWindowFunction::Framed(Function::Avg(column("value"))),
            None,
            None
        )),
        vec![json!(f64::MAX); 2]
    );
    let prepared = prepare_relational(
        &one(
            &maximum,
            VortexRelationalWindowFunction::Framed(Function::Sum(column("value"))),
            None,
            None,
        ),
        policy(),
    )
    .unwrap();
    let baseline = prepared.snapshot().memory.reserved_bytes;
    let mut delivered = 0;
    assert!(
        prepared
            .for_each_batch(&CancellationToken::default(), |_, _| {
                delivered += 1;
                Ok(())
            })
            .is_err()
    );
    assert_eq!(delivered, 0);
    assert_eq!(prepared.snapshot().memory.reserved_bytes, baseline);
    let fixture = Fixture::new(
        single(
            "value",
            PrimitiveArray::from_iter([1.0f64, f64::from_bits(1.0f64.to_bits() + 1)]).into_array(),
        ),
        1,
    );
    let frame = Frame {
        unit: Unit::Range,
        start: Bound::CurrentRow,
        end: Bound::Following(Offset::Number(ScalarValue::Float64(2f64.powi(-53)))),
        exclusion: Exclusion::NoOthers,
    };
    assert_eq!(
        collect(&one(
            &fixture,
            VortexRelationalWindowFunction::Framed(Function::CountAll),
            Some(frame),
            Some("value")
        )),
        vec![json!(1), json!(1)]
    );
}

#[test]
fn native_window_frames_nested_selection_extrema_distinct_and_decimal_totals() {
    use vortex::array::arrays::{DecimalArray, ListViewArray};
    use vortex::array::dtype::DecimalDType;
    let lists = ListViewArray::try_new(
        PrimitiveArray::from_option_iter([Some(9i64), None, Some(-4)]).into_array(),
        PrimitiveArray::from_iter([0u64, 2, 2, 2]).into_array(),
        PrimitiveArray::from_iter([2u64, 0, 0, 1]).into_array(),
        Validity::from_iter([true, true, false, true]),
    )
    .unwrap()
    .into_array();
    let fixture = Fixture::new(single("value", lists), 1);
    for (function, expected) in [
        (Function::Count(column("value")), json!(3)),
        (Function::CountDistinct(column("value")), json!(3)),
        (Function::Min(column("value")), json!([])),
        (Function::Max(column("value")), json!([9, null])),
        (Function::FirstValue(column("value")), json!([9, null])),
        (Function::LastValue(column("value")), json!([-4])),
        (
            Function::NthValue {
                column: column("value"),
                index: 3,
            },
            Value::Null,
        ),
    ] {
        assert_eq!(
            collect(&one(
                &fixture,
                VortexRelationalWindowFunction::Framed(function),
                None,
                None
            )),
            vec![expected; 4]
        );
    }
    let maximum = 10i128.pow(38) - 1;
    let fixture = Fixture::new(
        single(
            "value",
            DecimalArray::from_iter([maximum, maximum, -maximum], DecimalDType::new(38, 6))
                .into_array(),
        ),
        1,
    );
    assert_eq!(
        collect(&one(
            &fixture,
            VortexRelationalWindowFunction::Framed(Function::Sum(column("value"))),
            None,
            None
        )),
        vec![json!(format!("decimal128(38,6):{maximum}")); 3]
    );
    assert_eq!(
        collect(&one(
            &fixture,
            VortexRelationalWindowFunction::Framed(Function::Avg(column("value"))),
            None,
            None
        )),
        vec![json!(format!("decimal128(38,6):{}", maximum / 3)); 3]
    );
}

#[test]
fn native_window_frames_cancellation_denied_grants_and_retry_release_state() {
    let fixture = Fixture::new(
        single(
            "value",
            PrimitiveArray::from_iter((0u64..8192).rev()).into_array(),
        ),
        512,
    );
    let prepared = prepare_relational(
        &one(
            &fixture,
            VortexRelationalWindowFunction::Framed(Function::CountDistinct(column("value"))),
            None,
            Some("value"),
        ),
        policy(),
    )
    .unwrap();
    let baseline = prepared.snapshot().memory.reserved_bytes;
    let limit = prepared.snapshot().memory.limit_bytes;
    let pressure = prepared
        .session
        .memory()
        .reserve(limit - baseline - 256 * 1024)
        .unwrap();
    let mut delivered = 0;
    assert!(
        prepared
            .for_each_batch(&CancellationToken::default(), |_, _| {
                delivered += 1;
                Ok(())
            })
            .is_err()
    );
    assert_eq!(delivered, 0);
    drop(pressure);
    assert_eq!(prepared.snapshot().memory.reserved_bytes, baseline);
    let cancel = CancellationToken::default();
    assert!(
        prepared
            .for_each_batch(&cancel, |_, _| {
                delivered += 1;
                cancel.cancel();
                Ok(())
            })
            .is_err()
    );
    assert_eq!(delivered, 1);
    assert_eq!(prepared.snapshot().completed_executions, 0);
    assert_eq!(prepared.snapshot().memory.reserved_bytes, baseline);
    let result = prepared
        .for_each_batch(&CancellationToken::default(), |_, _| Ok(()))
        .unwrap();
    assert_eq!(result.output_rows, 8192);
    assert_eq!(result.runtime.prepared_source_opens, 1);
    assert_eq!(result.runtime.completed_executions, 1);
    drop(result);
    assert_eq!(prepared.snapshot().memory.reserved_bytes, baseline);
}

#[test]
fn native_window_frames_range_extreme_integer_and_decimal_endpoints_do_not_overflow() {
    use vortex::array::arrays::DecimalArray;
    use vortex::array::dtype::DecimalDType;
    let integer = Fixture::new(
        single(
            "value",
            PrimitiveArray::from_iter([i64::MIN, 0, i64::MAX]).into_array(),
        ),
        1,
    );
    let maximum = 10i128.pow(38) - 1;
    let decimal = Fixture::new(
        single(
            "value",
            DecimalArray::from_iter([-maximum, 0, maximum], DecimalDType::new(38, 0)).into_array(),
        ),
        1,
    );
    for (fixture, offset, expected) in [
        (&integer, distance(u64::MAX), [1, 2, 3]),
        (
            &decimal,
            Offset::Number(ScalarValue::Decimal128 {
                value: 1,
                precision: 38,
                scale: 38,
            }),
            [1, 1, 1],
        ),
        (
            &decimal,
            Offset::Number(ScalarValue::Decimal128 {
                value: maximum,
                precision: 38,
                scale: 0,
            }),
            [1, 2, 2],
        ),
    ] {
        for descending in [false, true] {
            let frame = Frame {
                unit: Unit::Range,
                start: Bound::Preceding(offset.clone()),
                end: Bound::CurrentRow,
                exclusion: Exclusion::NoOthers,
            };
            let mut plan = one(
                fixture,
                VortexRelationalWindowFunction::Framed(Function::CountAll),
                Some(frame),
                Some("value"),
            );
            let VortexRelationalPlan::Window(window) = &mut plan else {
                unreachable!()
            };
            window.expressions[0].order_by[0].descending = descending;
            let mut expected = expected;
            if descending {
                expected.reverse();
            }
            assert_eq!(collect(&plan), expected.map(|value| json!(value)));
        }
    }
}

#[test]
fn native_window_frames_count_and_excluded_values_do_not_eagerly_validate_observations() {
    let fixture = Fixture::new(
        single(
            "value",
            PrimitiveArray::from_option_iter([Some(f64::NAN), Some(f64::INFINITY), None])
                .into_array(),
        ),
        1,
    );
    assert_eq!(
        collect(&one(
            &fixture,
            VortexRelationalWindowFunction::Framed(Function::Count(column("value"))),
            None,
            None
        )),
        vec![json!(2); 3]
    );
    let excluded = Frame {
        exclusion: Exclusion::Group,
        ..Frame::default()
    };
    for function in [
        Function::Sum(column("value")),
        Function::Avg(column("value")),
        Function::Min(column("value")),
        Function::Max(column("value")),
        Function::CountDistinct(column("value")),
    ] {
        let empty = if matches!(function, Function::CountDistinct(_)) {
            json!(0)
        } else {
            Value::Null
        };
        assert_eq!(
            collect(&one(
                &fixture,
                VortexRelationalWindowFunction::Framed(function.clone()),
                Some(excluded.clone()),
                None
            )),
            vec![empty; 3]
        );
        let prepared = prepare_relational(
            &one(
                &fixture,
                VortexRelationalWindowFunction::Framed(function),
                None,
                None,
            ),
            policy(),
        )
        .unwrap();
        let baseline = prepared.snapshot().memory.reserved_bytes;
        assert!(prepared.execute_owned().is_err());
        assert_eq!(prepared.snapshot().memory.reserved_bytes, baseline);
        assert_eq!(prepared.snapshot().completed_executions, 0);
    }
}

#[test]
fn native_window_frames_selected_output_retains_native_ownership_after_plan_drop() {
    let fixture = Fixture::new(
        single(
            "value",
            DictArray::try_new(
                PrimitiveArray::from_iter([0u8, 1, 0]).into_array(),
                VarBinViewArray::from_iter_nullable_str([Some("λ,港"), None]).into_array(),
            )
            .unwrap()
            .into_array(),
        ),
        1,
    );
    for function in [
        Function::FirstValue(column("value")),
        Function::Min(column("value")),
    ] {
        let prepared = prepare_relational(
            &one(
                &fixture,
                VortexRelationalWindowFunction::Framed(function),
                None,
                None,
            ),
            policy(),
        )
        .unwrap();
        let memory = prepared.session.memory().clone();
        let result = prepared.execute_owned().unwrap();
        let slice = result.result.arrays()[0].slice(0..1).unwrap();
        drop((result, prepared));
        assert!(memory.snapshot().reserved_bytes > 0);
        let field =
            crate::local_primitives::logical_field_from_native_array(&slice, "out").unwrap();
        let mut context = VortexSession::default().create_execution_ctx();
        assert_eq!(
            result_batch::scalar_value(&field, 0, &mut context)
                .unwrap()
                .into_json()
                .unwrap(),
            json!("λ,港")
        );
        drop((field, slice, context));
        assert_eq!(memory.snapshot().reserved_bytes, 0);
    }
}

#[cfg(feature = "universal-format-io")]
#[test]
fn native_window_frames_writers_preserve_values_and_roll_back_arithmetic_errors() {
    use crate::local_primitives::VortexLocalPrimitiveRowExportFormat as Format;
    let fixture = Fixture::new(
        single(
            "value",
            PrimitiveArray::from_option_iter([Some(1i64), None, Some(3)]).into_array(),
        ),
        1,
    );
    let frame = Frame {
        unit: Unit::Rows,
        start: Bound::UnboundedPreceding,
        end: Bound::UnboundedFollowing,
        exclusion: Exclusion::CurrentRow,
    };
    let plan = one(
        &fixture,
        VortexRelationalWindowFunction::Framed(Function::Sum(column("value"))),
        Some(frame),
        None,
    );
    writer_tests::verify_writers(
        &fixture,
        &plan,
        "framed",
        &[json!({"out":3.0}), json!({"out":4.0}), json!({"out":1.0})],
        "out\n3\n4\n1\n",
    );

    let overflow = Fixture::new(
        single(
            "value",
            PrimitiveArray::from_iter([f64::MAX, f64::MAX]).into_array(),
        ),
        1,
    );
    let prepared = prepare_relational(
        &one(
            &overflow,
            VortexRelationalWindowFunction::Framed(Function::Sum(column("value"))),
            None,
            None,
        ),
        policy(),
    )
    .unwrap();
    let baseline = prepared.snapshot().memory.reserved_bytes;
    for format in [
        Format::Vortex,
        Format::Parquet,
        Format::ArrowIpc,
        Format::Avro,
        Format::Orc,
        Format::Json,
        Format::Jsonl,
        Format::Csv,
    ] {
        let path = overflow.0.join(format!("rollback.{}", format.as_str()));
        assert!(prepared.write(&path, format, false).is_err());
        assert!(!path.exists());
        assert_eq!(fs::read_dir(&overflow.0).unwrap().count(), 1);
        assert_eq!(prepared.snapshot().memory.reserved_bytes, baseline);
        fs::write(&path, b"existing destination").unwrap();
        assert!(prepared.write(&path, format, true).is_err());
        assert_eq!(fs::read(&path).unwrap(), b"existing destination");
        assert_eq!(fs::read_dir(&overflow.0).unwrap().count(), 2);
        fs::remove_file(path).unwrap();
    }
    assert_eq!(prepared.snapshot().completed_executions, 0);
}
