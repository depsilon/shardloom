//! Exact native types, observation boundaries and error precedence for stored windows.

use super::*;
use crate::relational_query::{
    VortexRelationalFrameBound as Bound, VortexRelationalFrameOffset as Offset,
    VortexRelationalOrderKey as OrderKey, VortexRelationalWindow as Window,
    VortexRelationalWindowExpression as WindowExpression,
};
use vortex::array::{
    arrays::{DecimalArray, ExtensionArray, ListViewArray, VarBinArray},
    dtype::DecimalDType,
    extension::datetime::{Date, TimeUnit, Timestamp},
};

fn column(name: &str) -> ColumnRef {
    ColumnRef::new(name).unwrap()
}

fn full() -> Frame {
    Frame {
        unit: Unit::Rows,
        end: Bound::UnboundedFollowing,
        ..Frame::default()
    }
}

fn expression(name: &str, function: Function) -> WindowExpression {
    WindowExpression {
        output_column: name.into(),
        function: VortexRelationalWindowFunction::Framed(function),
        partition_by: vec![],
        order_by: vec![],
        frame: Some(full()),
    }
}

fn rows(fixture: &Fixture, plan: &VortexRelationalPlan, expected: &[Value]) {
    let resident = prepare_relational(plan, policy()).unwrap();
    assert_eq!(
        json_rows(
            &resident
                .collect_jsonl(&CancellationToken::default())
                .unwrap()
        ),
        expected
    );
    let prepared = ordered(fixture, plan);
    let baseline = prepared.snapshot().memory.reserved_bytes;
    for batch_rows in [1, 3] {
        assert_eq!(complete(&prepared, batch_rows).0, expected);
        assert_eq!(prepared.snapshot().memory.reserved_bytes, baseline);
        assert_eq!(prepared.output_dtype(), resident.output_dtype());
    }
}

fn lists() -> ArrayRef {
    ListViewArray::try_new(
        PrimitiveArray::from_option_iter([Some(9i64), None, Some(-4)]).into_array(),
        PrimitiveArray::from_iter([0u64, 2, 2, 2]).into_array(),
        PrimitiveArray::from_iter([2u64, 0, 0, 1]).into_array(),
        Validity::from_iter([true, true, false, true]),
    )
    .unwrap()
    .into_array()
}

type NativeKeyCase = (ArrayRef, Vec<Value>, [u64; 4], usize, usize);

fn native_key_cases() -> [NativeKeyCase; 5] {
    let maximum = 10i128.pow(38) - 1;
    [
        (
            VarBinArray::from(vec![
                Some(&b"\x00\xff\x10"[..]),
                Some(&b""[..]),
                None,
                Some(&b"\xc3\xa9"[..]),
            ])
            .into_array(),
            vec![json!("00ff10"), json!(""), Value::Null, json!("c3a9")],
            [2, 1, 4, 3],
            1,
            3,
        ),
        (
            DecimalArray::from_option_iter(
                [Some(1_234_567i128), Some(-maximum), None, Some(0)],
                DecimalDType::new(38, 6),
            )
            .into_array(),
            vec![
                json!("decimal128(38,6):1234567"),
                json!(format!("decimal128(38,6):-{maximum}")),
                Value::Null,
                json!("decimal128(38,6):0"),
            ],
            [3, 1, 4, 2],
            1,
            0,
        ),
        (
            ExtensionArray::new(
                Date::new(TimeUnit::Days, Nullability::Nullable).erased(),
                PrimitiveArray::from_option_iter([Some(-1i32), Some(20_000), None, Some(0)])
                    .into_array(),
            )
            .into_array(),
            vec![json!(-1), json!(20_000), Value::Null, json!(0)],
            [1, 3, 4, 2],
            0,
            1,
        ),
        (
            ExtensionArray::new(
                Timestamp::new(TimeUnit::Microseconds, Nullability::Nullable).erased(),
                PrimitiveArray::from_option_iter([
                    Some(-1i64),
                    Some(1_700_000_000_123_456),
                    None,
                    Some(0),
                ])
                .into_array(),
            )
            .into_array(),
            vec![
                json!(-1),
                json!(1_700_000_000_123_456i64),
                Value::Null,
                json!(0),
            ],
            [1, 3, 4, 2],
            0,
            1,
        ),
        (
            lists(),
            vec![json!([9, null]), json!([]), Value::Null, json!([-4])],
            [3, 1, 4, 2],
            1,
            0,
        ),
    ]
}

#[test]
fn ordered_window_native_binary_temporal_decimal_and_nested_keys_preserve_exact_selection() {
    for (array, values, ranks, minimum, maximum) in native_key_cases() {
        for chunk in [1, 4] {
            let fixture = Fixture::new(single("value", array.clone()), chunk);
            let mut expressions = [
                ("minimum", Function::Min(column("value"))),
                ("maximum", Function::Max(column("value"))),
                ("first", Function::FirstValue(column("value"))),
                ("last", Function::LastValue(column("value"))),
                ("distinct", Function::CountDistinct(column("value"))),
            ]
            .into_iter()
            .map(|(name, function)| expression(name, function))
            .collect::<Vec<_>>();
            expressions.push(WindowExpression {
                output_column: "rank".into(),
                function: VortexRelationalWindowFunction::Rank,
                partition_by: vec![],
                order_by: vec![],
                frame: None,
            });
            for expression in &mut expressions {
                expression.order_by.push(OrderKey {
                    column: column("value"),
                    descending: false,
                    nulls: Some(NullOrder::Last),
                });
            }
            let plan = VortexRelationalPlan::Window(Box::new(Window {
                input: fixture.scan(),
                columns: vec![column("value")],
                expressions,
            }));
            let expected = (0..4).map(|row| json!({"value":values[row],"minimum":values[minimum],"maximum":values[maximum],"first":values[minimum],"last":null,"distinct":3,"rank":ranks[row]})).collect::<Vec<_>>();
            rows(&fixture, &plan, &expected);
        }
    }
}

#[test]
fn ordered_window_nonempty_frames_ignore_unobserved_invalid_decimal_storage() {
    for chunk in [1, 3] {
        let fixture = Fixture::new(
            single(
                "value",
                DecimalArray::from_iter([999i128, 2, 3], DecimalDType::new(2, 0)).into_array(),
            ),
            chunk,
        );
        let frame = Frame {
            unit: Unit::Rows,
            start: Bound::Following(Offset::Number(shardloom_core::ScalarValue::UInt64(1))),
            end: Bound::UnboundedFollowing,
            ..Frame::default()
        };
        for (function, expected) in [
            (
                Function::Sum(column("value")),
                vec![
                    json!("decimal128(38,0):5"),
                    json!("decimal128(38,0):3"),
                    Value::Null,
                ],
            ),
            (
                Function::Min(column("value")),
                vec![
                    json!("decimal128(2,0):2"),
                    json!("decimal128(2,0):3"),
                    Value::Null,
                ],
            ),
            (
                Function::Max(column("value")),
                vec![
                    json!("decimal128(2,0):3"),
                    json!("decimal128(2,0):3"),
                    Value::Null,
                ],
            ),
            (
                Function::CountDistinct(column("value")),
                vec![json!(2), json!(1), json!(0)],
            ),
        ] {
            let plan = window_frame_tests::one(
                &fixture,
                VortexRelationalWindowFunction::Framed(function),
                Some(frame.clone()),
                None,
            );
            rows(
                &fixture,
                &plan,
                &expected
                    .into_iter()
                    .map(|out| json!({"out":out}))
                    .collect::<Vec<_>>(),
            );
        }
    }
}

#[test]
fn ordered_window_decimal_totals_widen_precision_and_preserve_exact_cancellation() {
    let maximum = 10i128.pow(38) - 1;
    for (values, dtype, sum, average) in [
        (vec![99i128, 99], DecimalDType::new(2, 1), 198, 9_900_000),
        (
            vec![maximum, maximum, -maximum],
            DecimalDType::new(38, 6),
            maximum,
            maximum / 3,
        ),
    ] {
        let length = values.len();
        let fixture = Fixture::new(
            single("value", DecimalArray::from_iter(values, dtype).into_array()),
            1,
        );
        for (function, value, scale) in [
            (Function::Sum(column("value")), sum, dtype.scale()),
            (Function::Avg(column("value")), average, 6),
        ] {
            let plan = window_frame_tests::one(
                &fixture,
                VortexRelationalWindowFunction::Framed(function),
                Some(full()),
                None,
            );
            rows(
                &fixture,
                &plan,
                &vec![json!({"out":format!("decimal128(38,{scale}):{value}")}); length],
            );
        }
    }
}

fn error(fixture: &Fixture, plan: &VortexRelationalPlan, expected: &str) {
    let resident = prepare_relational(plan, policy()).unwrap();
    let native = ordered(fixture, plan);
    let mut prior = None;
    for prepared in [&resident, &native] {
        let baseline = prepared.snapshot().memory.reserved_bytes;
        let mut emitted = 0;
        let error = prepared
            .for_each_batch(&CancellationToken::default(), |_, _| {
                emitted += 1;
                Ok(())
            })
            .err()
            .unwrap()
            .to_string();
        assert!(error.contains(expected), "{expected}: {error}");
        if let Some(prior) = prior {
            assert_eq!(prior, error);
        }
        prior = Some(error);
        assert_eq!(emitted, 0);
        assert_eq!(prepared.snapshot().completed_executions, 0);
        assert_eq!(prepared.snapshot().memory.reserved_bytes, baseline);
    }
    assert_eq!(
        fs::read_dir(fixture.0.join("window-runs")).unwrap().count(),
        0
    );
}

#[test]
fn ordered_window_error_order_follows_groups_partitions_functions_and_final_selection() {
    let fixture = Fixture::new(
        StructArray::new(
            FieldNames::from(["partition", "id", "float", "decimal"]),
            vec![
                PrimitiveArray::from_iter([1i64, 0]).into_array(),
                PrimitiveArray::from_iter([0i64, 1]).into_array(),
                PrimitiveArray::from_iter([f64::NAN, 1.0]).into_array(),
                DecimalArray::from_iter([1i128, 999], DecimalDType::new(2, 0)).into_array(),
            ],
            2,
            Validity::NonNullable,
        )
        .into_array(),
        1,
    );
    for separate_groups in [false, true] {
        let mut expressions = vec![
            expression("first", Function::Sum(column("float"))),
            expression("second", Function::Sum(column("decimal"))),
        ];
        for (index, expression) in expressions.iter_mut().enumerate() {
            expression.partition_by.push(column("partition"));
            expression.order_by.push(OrderKey {
                column: column("id"),
                descending: separate_groups && index == 1,
                nulls: None,
            });
        }
        let plan = VortexRelationalPlan::Window(Box::new(Window {
            input: fixture.scan(),
            columns: vec![],
            expressions,
        }));
        error(
            &fixture,
            &plan,
            if separate_groups {
                "nonfinite"
            } else {
                "declared precision"
            },
        );
    }
    let selected = VortexRelationalPlan::Window(Box::new(Window {
        input: fixture.scan(),
        columns: vec![],
        expressions: vec![
            expression("selected", Function::FirstValue(column("float"))),
            expression("sum", Function::Sum(column("decimal"))),
        ],
    }));
    error(&fixture, &selected, "declared precision");
    let mut expressions = vec![expression("sum", Function::Sum(column("decimal")))];
    expressions[0].order_by.push(OrderKey {
        column: column("float"),
        descending: false,
        nulls: None,
    });
    let ordered_key = VortexRelationalPlan::Window(Box::new(Window {
        input: fixture.scan(),
        columns: vec![],
        expressions,
    }));
    error(&fixture, &ordered_key, "nonfinite");
}

#[test]
fn ordered_window_signed_zero_extrema_select_first_ordered_tie_without_changing_bits() {
    for descending in [false, true] {
        let fixture = Fixture::new(
            StructArray::new(
                FieldNames::from(["id", "value"]),
                vec![
                    PrimitiveArray::from_iter([0i64, 1]).into_array(),
                    PrimitiveArray::from_iter([-0.0f64, 0.0]).into_array(),
                ],
                2,
                Validity::NonNullable,
            )
            .into_array(),
            1,
        );
        let mut expressions = vec![
            expression("minimum", Function::Min(column("value"))),
            expression("maximum", Function::Max(column("value"))),
        ];
        for expression in &mut expressions {
            expression.order_by.push(OrderKey {
                column: column("id"),
                descending,
                nulls: None,
            });
        }
        let plan = VortexRelationalPlan::Window(Box::new(Window {
            input: fixture.scan(),
            columns: vec![],
            expressions,
        }));
        let prepared = ordered(&fixture, &plan);
        let baseline = prepared.snapshot().memory.reserved_bytes;
        let report = prepared
            .for_each_batch(&CancellationToken::default(), |array, context| {
                let mut execution = context.native_session().create_execution_ctx();
                for name in ["minimum", "maximum"] {
                    let field =
                        crate::local_primitives::logical_field_from_native_array(&array, name)?;
                    let values = field.execute::<PrimitiveArray>(&mut execution).unwrap();
                    for value in values.to_buffer::<f64>().iter() {
                        assert_eq!(
                            value.to_bits(),
                            if descending { 0.0f64 } else { -0.0f64 }.to_bits()
                        );
                    }
                }
                Ok(())
            })
            .unwrap();
        assert_eq!(report.output_rows, 2);
        drop(report);
        assert_eq!(prepared.snapshot().memory.reserved_bytes, baseline);
    }
}

#[test]
fn ordered_window_empty_singleton_and_extreme_offsets_keep_types_and_values() {
    for empty in [true, false] {
        let array = PrimitiveArray::from_iter(if empty { vec![] } else { vec![7i64] }).into_array();
        let fixture = Fixture::new(single("value", array), 1);
        for (function, frame, expected) in [
            (
                VortexRelationalWindowFunction::Ntile {
                    buckets: usize::MAX,
                },
                None,
                json!(1),
            ),
            (
                VortexRelationalWindowFunction::PercentRank,
                None,
                json!(0.0),
            ),
            (VortexRelationalWindowFunction::CumeDist, None, json!(1.0)),
            (
                VortexRelationalWindowFunction::Lag {
                    column: column("value"),
                    offset: usize::MAX,
                },
                None,
                Value::Null,
            ),
            (
                VortexRelationalWindowFunction::Lead {
                    column: column("value"),
                    offset: usize::MAX,
                },
                None,
                Value::Null,
            ),
            (
                VortexRelationalWindowFunction::Framed(Function::CountDistinct(column("value"))),
                Some(Frame {
                    unit: Unit::Rows,
                    start: Bound::Following(Offset::Number(shardloom_core::ScalarValue::UInt64(
                        u64::MAX,
                    ))),
                    end: Bound::UnboundedFollowing,
                    ..Frame::default()
                }),
                json!(0),
            ),
            (
                VortexRelationalWindowFunction::Framed(Function::Min(column("value"))),
                Some(Frame {
                    unit: Unit::Groups,
                    start: Bound::Following(Offset::Number(shardloom_core::ScalarValue::UInt64(
                        u64::MAX,
                    ))),
                    end: Bound::UnboundedFollowing,
                    ..Frame::default()
                }),
                Value::Null,
            ),
        ] {
            let plan = window_frame_tests::one(&fixture, function, frame, Some("value"));
            rows(
                &fixture,
                &plan,
                &if empty {
                    vec![]
                } else {
                    vec![json!({"out":expected})]
                },
            );
        }
    }
}
