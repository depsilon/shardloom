use super::*;
use crate::relational_query::{
    VortexRelationalFrameBound as Bound, VortexRelationalFrameExclusion as Exclusion,
    VortexRelationalFrameFunction as Function, VortexRelationalFrameOffset as Offset,
    VortexRelationalFrameUnit as Unit, VortexRelationalOrderKey as Order,
    VortexRelationalWindow as Window, VortexRelationalWindowExpression as Expression,
    VortexRelationalWindowFrame as Frame, VortexRelationalWindowFunction as Framed,
};

const X: [Option<i32>; 4] = [Some(1), Some(3), Some(5), None];
const Y: [Option<i32>; 4] = [Some(4), None, Some(8), Some(9)];
const KEYS: [i32; 4] = [0, 0, 2, 3];
const PEERS: [usize; 4] = [0, 0, 1, 2];
const NAMES: [&str; 7] = [
    "var_pop",
    "var_samp",
    "stddev_pop",
    "stddev_samp",
    "covar_pop",
    "covar_samp",
    "corr",
];

fn expected(row: usize, selected: &[usize]) -> Value {
    // Independent two-pass arithmetic on small exact offsets; translating X by
    // 2^60 cannot change its variance, covariance or correlation.
    let x = selected
        .iter()
        .filter_map(|&row| X[row])
        .map(f64::from)
        .collect::<Vec<_>>();
    let pairs = selected
        .iter()
        .filter_map(|&row| X[row].zip(Y[row]))
        .map(|(x, y)| (f64::from(x), f64::from(y)))
        .collect::<Vec<_>>();
    let n = f64::from(u32::try_from(x.len()).unwrap());
    let mean = x.iter().sum::<f64>() / n;
    let m2 = x.iter().map(|x| (x - mean).powi(2)).sum::<f64>();
    let population = (!x.is_empty()).then_some(m2 / n);
    let sample = (x.len() > 1).then_some(m2 / (n - 1.0));
    let n = f64::from(u32::try_from(pairs.len()).unwrap());
    let mean_x = pairs.iter().map(|row| row.0).sum::<f64>() / n;
    let mean_y = pairs.iter().map(|row| row.1).sum::<f64>() / n;
    let cross = pairs
        .iter()
        .map(|&(x, y)| (x - mean_x) * (y - mean_y))
        .sum::<f64>();
    let xx = pairs
        .iter()
        .map(|&(x, _)| (x - mean_x).powi(2))
        .sum::<f64>();
    let yy = pairs
        .iter()
        .map(|&(_, y)| (y - mean_y).powi(2))
        .sum::<f64>();
    json!({"id":row,"var_pop":population,"var_samp":sample,
        "stddev_pop":population.map(f64::sqrt),"stddev_samp":sample.map(f64::sqrt),
        "covar_pop":(!pairs.is_empty()).then_some(cross/n),
        "covar_samp":(pairs.len()>1).then_some(cross/(n-1.0)),
        "corr":(pairs.len()>1 && xx>0.0 && yy>0.0).then_some(cross/(xx*yy).sqrt())})
}

fn frame_reference(unit: Unit, exclusion: Exclusion) -> Vec<Value> {
    (0usize..4)
        .map(|row| {
            let selected = (0usize..4)
                .filter(|&other| {
                    let nearby = match unit {
                        Unit::Rows => other.abs_diff(row) <= 1,
                        Unit::Groups => PEERS[other].abs_diff(PEERS[row]) <= 1,
                        Unit::Range => KEYS[other].abs_diff(KEYS[row]) <= 1,
                    };
                    nearby
                        && match exclusion {
                            Exclusion::NoOthers => true,
                            Exclusion::CurrentRow => other != row,
                            Exclusion::Group => KEYS[other] != KEYS[row],
                            Exclusion::Ties => other == row || KEYS[other] != KEYS[row],
                        }
                })
                .collect::<Vec<_>>();
            expected(row, &selected)
        })
        .collect()
}

#[test]
fn native_statistical_windows_match_independent_rows_groups_range_and_exclusion_membership() {
    let fixture = Fixture::new(
        StructArray::new(
            FieldNames::from(["id", "key", "number", "paired"]),
            vec![
                PrimitiveArray::from_iter(0u64..4).into_array(),
                PrimitiveArray::from_iter(KEYS).into_array(),
                PrimitiveArray::from_option_iter(X.map(|x| x.map(|x| (1i64 << 60) + i64::from(x))))
                    .into_array(),
                PrimitiveArray::from_option_iter(Y).into_array(),
            ],
            4,
            Validity::NonNullable,
        )
        .into_array(),
        1,
    );
    for unit in [Unit::Rows, Unit::Groups, Unit::Range] {
        for exclusion in [
            Exclusion::NoOthers,
            Exclusion::CurrentRow,
            Exclusion::Group,
            Exclusion::Ties,
        ] {
            let expected = frame_reference(unit, exclusion);
            let plan = VortexRelationalPlan::Window(Box::new(Window {
                input: fixture.scan(),
                columns: vec![ColumnRef::new("id").unwrap()],
                expressions: NAMES
                    .map(|name| {
                        let mut measure = measure(name, Some("number"), name);
                        if name.starts_with("covar") || name == "corr" {
                            measure.secondary_column = Some(ColumnRef::new("paired").unwrap());
                        }
                        Expression {
                            output_column: name.into(),
                            function: Framed::Framed(Function::Aggregate(Box::new(measure))),
                            partition_by: vec![],
                            order_by: vec![Order {
                                column: ColumnRef::new("key").unwrap(),
                                descending: false,
                                nulls: None,
                            }],
                            frame: Some(Frame {
                                unit,
                                exclusion,
                                start: Bound::Preceding(Offset::Number(
                                    shardloom_core::ScalarValue::UInt64(1),
                                )),
                                end: Bound::Following(Offset::Number(
                                    shardloom_core::ScalarValue::UInt64(1),
                                )),
                            }),
                        }
                    })
                    .to_vec(),
            }));
            for spill in [false, true] {
                let prepared = prepare_relational(&plan, policy()).unwrap();
                let workspace = fixture.0.join("window-runs");
                let prepared = if spill {
                    fs::create_dir_all(&workspace).unwrap();
                    prepared
                        .with_spill(
                            VortexRelationalSpillPolicy::new(&workspace, 64 << 20, 1 << 20)
                                .unwrap(),
                        )
                        .unwrap()
                } else {
                    prepared
                };
                let baseline = prepared.snapshot().memory.reserved_bytes;
                let result = prepared
                    .collect_jsonl(&CancellationToken::default())
                    .unwrap();
                assert_close(&json_rows(&result), &expected);
                assert!(result.execution.native_io_certificate.is_certified());
                if spill {
                    assert!(
                        result
                            .execution
                            .spill
                            .as_ref()
                            .unwrap()
                            .owned_cleanup_completed
                    );
                    assert_eq!(fs::read_dir(&workspace).unwrap().count(), 0);
                }
                drop(result);
                assert_eq!(prepared.snapshot().memory.reserved_bytes, baseline);
            }
        }
    }
}

fn one(fixture: &Fixture, name: &str, frame: Frame) -> VortexRelationalPlan {
    let mut measure = measure(name, Some("number"), "out");
    if name.starts_with("covar") || name == "corr" {
        measure.secondary_column = Some(ColumnRef::new("paired").unwrap());
    }
    VortexRelationalPlan::Window(Box::new(Window {
        input: fixture.scan(),
        columns: vec![],
        expressions: vec![Expression {
            output_column: "out".into(),
            function: Framed::Framed(Function::Aggregate(Box::new(measure))),
            partition_by: vec![],
            order_by: vec![Order {
                column: ColumnRef::new("id").unwrap(),
                descending: false,
                nulls: None,
            }],
            frame: Some(frame),
        }],
    }))
}

#[test]
fn native_statistical_windows_preserve_scaled_tiny_and_large_finite_results() {
    for value in [1e-200f64, 1e200, f64::from_bits(2)] {
        let fixture = Fixture::new(
            StructArray::new(
                FieldNames::from(["id", "number", "paired"]),
                vec![
                    PrimitiveArray::from_iter(0u64..3).into_array(),
                    PrimitiveArray::from_iter([0.0, value, value]).into_array(),
                    PrimitiveArray::from_iter([0.0, -value, -value]).into_array(),
                ],
                3,
                Validity::NonNullable,
            )
            .into_array(),
            1,
        );
        for (name, expected) in [
            ("stddev_pop", [Some(0.0), Some(value * 0.5), Some(0.0)]),
            ("corr", [None, Some(-1.0), None]),
        ] {
            let plan = one(
                &fixture,
                name,
                Frame {
                    unit: Unit::Rows,
                    exclusion: Exclusion::NoOthers,
                    start: Bound::Preceding(Offset::Number(shardloom_core::ScalarValue::UInt64(1))),
                    end: Bound::CurrentRow,
                },
            );
            for spill in [false, true] {
                let prepared = prepare_relational(&plan, policy()).unwrap();
                let workspace = fixture.0.join("scaled-window-runs");
                fs::create_dir_all(&workspace).unwrap();
                let prepared = if spill {
                    prepared
                        .with_spill(
                            VortexRelationalSpillPolicy::new(&workspace, 64 << 20, 1 << 20)
                                .unwrap(),
                        )
                        .unwrap()
                } else {
                    prepared
                };
                let baseline = prepared.snapshot().memory.reserved_bytes;
                let result = prepared
                    .collect_jsonl(&CancellationToken::default())
                    .unwrap();
                assert_close(
                    &json_rows(&result),
                    &expected.map(|value| json!({"out":value})),
                );
                assert!(result.execution.native_io_certificate.is_certified());
                drop(result);
                assert_eq!(prepared.snapshot().memory.reserved_bytes, baseline);
                assert_eq!(fs::read_dir(&workspace).unwrap().count(), 0);
            }
        }
    }
}

#[test]
fn native_statistical_windows_observe_only_selected_values_and_clean_up_failure() {
    let fixture = Fixture::new(
        StructArray::new(
            FieldNames::from(["id", "number", "paired"]),
            vec![
                PrimitiveArray::from_iter(0u64..3).into_array(),
                PrimitiveArray::from_option_iter([Some(f64::NAN), Some(f64::INFINITY), None])
                    .into_array(),
                PrimitiveArray::from_iter([1i64, 2, 3]).into_array(),
            ],
            3,
            Validity::NonNullable,
        )
        .into_array(),
        1,
    );
    let workspace = fixture.0.join("selected-failure");
    fs::create_dir(&workspace).unwrap();
    for name in NAMES {
        for excluded in [true, false] {
            for spill in [false, true] {
                let frame = Frame {
                    unit: Unit::Rows,
                    start: Bound::CurrentRow,
                    end: Bound::CurrentRow,
                    exclusion: if excluded {
                        Exclusion::CurrentRow
                    } else {
                        Exclusion::NoOthers
                    },
                };
                let prepared = prepare_relational(&one(&fixture, name, frame), policy()).unwrap();
                let prepared = if spill {
                    prepared
                        .with_spill(
                            VortexRelationalSpillPolicy::new(&workspace, 64 << 20, 1 << 20)
                                .unwrap(),
                        )
                        .unwrap()
                } else {
                    prepared
                };
                let baseline = prepared.snapshot().memory.reserved_bytes;
                let result = prepared.collect_jsonl(&CancellationToken::default());
                if excluded {
                    assert_eq!(json_rows(&result.unwrap()), vec![json!({"out":null}); 3]);
                } else {
                    assert!(result.is_err(), "{name} observed nonfinite input");
                    assert_eq!(prepared.snapshot().completed_executions, 0);
                }
                assert_eq!(prepared.snapshot().memory.reserved_bytes, baseline);
                assert_eq!(fs::read_dir(&workspace).unwrap().count(), 0);
            }
        }
    }
}

#[test]
fn native_statistical_windows_pressure_cancellation_and_retry_release_state() {
    let fixture = Fixture::new(
        StructArray::new(
            FieldNames::from(["id", "number", "paired"]),
            vec![
                PrimitiveArray::from_iter(0u64..8192).into_array(),
                PrimitiveArray::from_iter((0u64..8192).map(|value| (1u64 << 60) + value))
                    .into_array(),
                PrimitiveArray::from_iter((0i64..8192).map(|value| -value)).into_array(),
            ],
            8192,
            Validity::NonNullable,
        )
        .into_array(),
        256,
    );
    let workspace = fixture.0.join("pressure");
    fs::create_dir(&workspace).unwrap();
    for spill in [false, true] {
        let frame = Frame {
            unit: Unit::Rows,
            start: Bound::Preceding(Offset::Number(shardloom_core::ScalarValue::UInt64(1023))),
            end: Bound::CurrentRow,
            exclusion: Exclusion::NoOthers,
        };
        let prepared = prepare_relational(&one(&fixture, "corr", frame), policy()).unwrap();
        let prepared = if spill {
            prepared
                .with_spill(
                    VortexRelationalSpillPolicy::new(&workspace, 64 << 20, 1 << 20).unwrap(),
                )
                .unwrap()
        } else {
            prepared
        };
        let baseline = prepared.snapshot().memory.reserved_bytes;
        let pressure = prepared
            .session
            .memory()
            .reserve(prepared.snapshot().memory.limit_bytes - baseline - 1024)
            .unwrap();
        assert!(
            prepared
                .collect_jsonl(&CancellationToken::default())
                .is_err()
        );
        drop(pressure);
        assert_eq!(prepared.snapshot().memory.reserved_bytes, baseline);
        let cancel = CancellationToken::default();
        let mut delivered = 0;
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
        assert_eq!(fs::read_dir(&workspace).unwrap().count(), 0);
        let result = prepared
            .collect_jsonl(&CancellationToken::default())
            .unwrap();
        let rows = json_rows(&result);
        assert_eq!(rows.len(), 8192);
        assert_eq!(rows[0], json!({"out":null}));
        assert_close(&rows[1..], &vec![json!({"out":-1.0}); 8191]);
        drop(result);
        assert_eq!(prepared.snapshot().memory.reserved_bytes, baseline);
        assert_eq!(fs::read_dir(&workspace).unwrap().count(), 0);
    }
}
