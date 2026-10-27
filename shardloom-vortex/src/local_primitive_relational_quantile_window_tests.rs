use super::*;
use crate::relational_query::{
    VortexRelationalFrameBound as Bound, VortexRelationalFrameExclusion as Exclusion,
    VortexRelationalFrameFunction as Function, VortexRelationalFrameOffset as Offset,
    VortexRelationalFrameUnit as Unit, VortexRelationalWindow as Window,
    VortexRelationalWindowExpression as Expression, VortexRelationalWindowFrame as Frame,
    VortexRelationalWindowFunction as Framed,
};
use shardloom_core::{ScalarValue, ShardLoomError};

const NUMBERS: [Option<i32>; 8] = [
    Some(3),
    Some(3),
    Some(-3),
    Some(3),
    None,
    Some(-3),
    Some(12),
    Some(12),
];
const KEYS: [u64; 8] = [0, 0, 1, 1, 1, 3, 4, 4];
const PEERS: [usize; 8] = [0, 0, 1, 1, 1, 2, 3, 3];

fn window(fixture: &Fixture, frame: &Frame, declarations: Vec<Measure>) -> VortexRelationalPlan {
    VortexRelationalPlan::Window(Box::new(Window {
        input: fixture.scan(),
        columns: vec![ColumnRef::new("id").unwrap()],
        expressions: declarations
            .into_iter()
            .map(|measure| Expression {
                output_column: measure.alias.clone(),
                function: Framed::Framed(Function::Aggregate(Box::new(measure))),
                partition_by: vec![],
                order_by: vec![VortexRelationalOrderKey {
                    column: ColumnRef::new("key").unwrap(),
                    descending: false,
                    nulls: None,
                }],
                frame: Some(frame.clone()),
            })
            .collect(),
    }))
}

fn reference(unit: Unit, exclusion: Exclusion, decimal: bool) -> Vec<Value> {
    (0usize..8)
        .map(|row| {
            let mut values = (0usize..8)
                .filter(|&other| {
                    let nearby = match unit {
                        Unit::Rows => other.abs_diff(row) <= 2,
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
                .filter_map(|row| NUMBERS[row])
                .collect::<Vec<_>>();
            values.sort_unstable();
            let mut result = json!({"id":row});
            for (name, quarters) in [("median", 2usize), ("c25", 1), ("c75", 3)] {
                result[name] = if values.is_empty() {
                    Value::Null
                } else {
                    let rank = quarters * (values.len() - 1);
                    let low = values[rank / 4];
                    let high = values[rank.div_ceil(4)];
                    let numerator = low * 4 + (high - low) * i32::try_from(rank % 4).unwrap();
                    if decimal {
                        json!(format!("decimal128(38,6):{}", numerator * 2500))
                    } else {
                        json!(f64::from(numerator) / 4.0)
                    }
                };
            }
            for (name, rank) in [
                ("d0", 0),
                ("d50", values.len().saturating_sub(1) / 2),
                ("d100", values.len().saturating_sub(1)),
            ] {
                result[name] = values.get(rank).map_or(Value::Null, |value| {
                    if decimal {
                        json!(format!("decimal128(10,2):{value}"))
                    } else {
                        json!(value)
                    }
                });
            }
            result
        })
        .collect()
}

#[test]
fn exact_quantile_windows_match_sorted_interval_unions_for_all_units_and_exclusions() {
    for decimal in [false, true] {
        let numbers = if decimal {
            DecimalArray::from_option_iter(
                NUMBERS.map(|value| value.map(i128::from)),
                DecimalDType::new(10, 2),
            )
            .into_array()
        } else {
            PrimitiveArray::from_option_iter(NUMBERS).into_array()
        };
        let fixture = Fixture::new(
            StructArray::new(
                FieldNames::from(["id", "key", "number"]),
                vec![
                    PrimitiveArray::from_iter(0u64..8).into_array(),
                    PrimitiveArray::from_iter(KEYS).into_array(),
                    numbers,
                ],
                8,
                Validity::NonNullable,
            )
            .into_array(),
            2,
        );
        for unit in [Unit::Rows, Unit::Groups, Unit::Range] {
            for exclusion in [
                Exclusion::NoOthers,
                Exclusion::CurrentRow,
                Exclusion::Group,
                Exclusion::Ties,
            ] {
                let distance = if unit == Unit::Rows { 2 } else { 1 };
                let frame = Frame {
                    unit,
                    exclusion,
                    start: Bound::Preceding(Offset::Number(ScalarValue::UInt64(distance))),
                    end: Bound::Following(Offset::Number(ScalarValue::UInt64(distance))),
                };
                check(
                    &fixture,
                    &window(&fixture, &frame, measures()),
                    &reference(unit, exclusion, decimal),
                    None,
                );
            }
        }
    }
}

#[test]
fn exact_quantile_window_pressure_cancellation_output_failure_and_retry_release_owners() {
    let fixture = Fixture::new(
        StructArray::new(
            FieldNames::from(["id", "key", "number"]),
            vec![
                PrimitiveArray::from_iter(0u64..256).into_array(),
                PrimitiveArray::from_iter(0u64..256).into_array(),
                PrimitiveArray::from_iter((0u64..256).map(|value| (1u64 << 60) + value))
                    .into_array(),
            ],
            256,
            Validity::NonNullable,
        )
        .into_array(),
        16,
    );
    let frame = Frame {
        unit: Unit::Rows,
        exclusion: Exclusion::NoOthers,
        start: Bound::Preceding(Offset::Number(ScalarValue::UInt64(31))),
        end: Bound::CurrentRow,
    };
    let plan = window(
        &fixture,
        &frame,
        vec![quantile("quantile_disc", 0.5, "out")],
    );
    let workspace = fixture.0.join("pressure");
    fs::create_dir(&workspace).unwrap();
    for spill in [false, true] {
        let prepared = prepare_relational(&plan, policy()).unwrap();
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
        for cancel in [false, true] {
            let token = CancellationToken::default();
            let mut delivered = 0;
            let result = prepared.for_each_batch(&token, |_, _| {
                delivered += 1;
                if cancel {
                    token.cancel();
                    Ok(())
                } else {
                    Err(ShardLoomError::InvalidOperation(
                        "test output failure".into(),
                    ))
                }
            });
            assert!(result.is_err());
            assert_eq!(delivered, 1);
            assert_eq!(prepared.snapshot().completed_executions, 0);
            assert_eq!(prepared.snapshot().memory.reserved_bytes, baseline);
            assert_eq!(fs::read_dir(&workspace).unwrap().count(), 0);
        }
        let result = prepared
            .collect_jsonl(&CancellationToken::default())
            .unwrap();
        let expected = (0u64..256)
            .map(|row| json!({"id":row,"out":(1u64<<60)+u64::midpoint(row.saturating_sub(31),row)}))
            .collect::<Vec<_>>();
        assert_eq!(json_rows(&result), expected);
        drop(result);
        assert_eq!(prepared.snapshot().memory.reserved_bytes, baseline);
        assert_eq!(fs::read_dir(&workspace).unwrap().count(), 0);
    }
}
