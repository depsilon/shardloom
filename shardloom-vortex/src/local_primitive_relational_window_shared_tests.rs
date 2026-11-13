//! Draft baseline regressions for compatible resident window state sharing.
//! Mount below the existing `window_spill_tests` module after index acceptance.

use super::*;
use crate::query_primitive::VortexSimpleAggregateMeasure as Measure;
use crate::relational_query::{
    VortexRelationalFrameBound as Bound, VortexRelationalFrameOffset as Offset,
    VortexRelationalOrderKey as Order, VortexRelationalWindow as Window,
    VortexRelationalWindowExpression as Expression,
};
use shardloom_core::decimal_output::{DecimalOutput, DecimalRounding};
use vortex::array::{arrays::DecimalArray, dtype::DecimalDType};

#[cfg(debug_assertions)]
#[path = "local_primitive_relational_window_shared_attribution_tests.rs"]
mod attribution_tests;

fn measure(function: &str, column: Option<&str>, alias: &str) -> Measure {
    Measure::new(
        function,
        column.map(|name| ColumnRef::new(name).unwrap()),
        alias.into(),
    )
}

fn assert_resident(prepared: &PreparedVortexRelational, expected: &[Value]) {
    let baseline = prepared.snapshot().memory.reserved_bytes;
    let result = prepared
        .collect_jsonl(&CancellationToken::default())
        .unwrap();
    assert_eq!(json_rows(&result).as_slice(), expected);
    assert!(result.execution.native_io_certificate.is_certified());
    assert!(
        !result
            .execution
            .native_io_certificate
            .side_effects
            .fallback_attempted
    );
    assert_eq!(result.execution.ordered_window_stages, 0);
    drop(result);
    assert_eq!(prepared.snapshot().memory.reserved_bytes, baseline);
}

fn moving(measure: Measure) -> Expression {
    let mut expression = growing(measure);
    expression.frame.as_mut().unwrap().start =
        Bound::Preceding(Offset::Number(shardloom_core::ScalarValue::UInt64(1)));
    expression
}

fn fixture(input: ArrayRef) -> Fixture {
    let rows = input.len();
    Fixture::new(
        StructArray::new(
            FieldNames::from(["id", "value"]),
            vec![
                PrimitiveArray::from_iter(0..u64::try_from(rows).unwrap()).into_array(),
                input,
            ],
            rows,
            Validity::NonNullable,
        )
        .into_array(),
        1,
    )
}

fn growing(measure: Measure) -> Expression {
    Expression {
        output_column: measure.alias.clone(),
        function: VortexRelationalWindowFunction::Framed(Function::Aggregate(Box::new(measure))),
        partition_by: vec![],
        order_by: vec![Order {
            column: ColumnRef::new("id").unwrap(),
            descending: false,
            nulls: None,
        }],
        frame: Some(Frame {
            unit: Unit::Rows,
            start: Bound::UnboundedPreceding,
            end: Bound::CurrentRow,
            exclusion: Exclusion::NoOthers,
        }),
    }
}

fn plan(fixture: &Fixture, measures: Vec<Measure>) -> VortexRelationalPlan {
    VortexRelationalPlan::Window(Box::new(Window {
        input: fixture.scan(),
        columns: vec![ColumnRef::new("id").unwrap()],
        expressions: measures.into_iter().map(growing).collect(),
    }))
}

fn same_error(fixture: &Fixture, plan: &VortexRelationalPlan, expected: &str, case: &str) {
    let resident = prepare_relational(plan, policy()).unwrap();
    let stored = ordered(fixture, plan);
    let mut prior = None;
    for (stored, prepared) in [(false, &resident), (true, &stored)] {
        let baseline = prepared.snapshot().memory.reserved_bytes;
        let mut emitted = 0;
        #[cfg(debug_assertions)]
        let observation = crate::local_primitives::native_relational_observation::Session::start();
        let error = prepared
            .for_each_batch(&CancellationToken::default(), |_, _| {
                emitted += 1;
                Ok(())
            })
            .err()
            .unwrap()
            .to_string();
        #[cfg(debug_assertions)]
        let counts = observation.finish();
        assert!(
            error.contains(expected),
            "{case}: expected {expected}: {error}"
        );
        if let Some(prior) = &prior {
            assert_eq!(prior, &error);
        }
        assert_eq!(emitted, 0);
        assert_eq!(prepared.snapshot().completed_executions, 0);
        assert_eq!(prepared.snapshot().memory.reserved_bytes, baseline);
        assert!(!prepared.root_has_file_kernel(), "{case}, stored={stored}");
        #[cfg(debug_assertions)]
        eprintln!(
            "WINDOW_SHARED_ERROR_WORK {}",
            json!({"case":case, "stored":stored,
            "error":error, "observations":counts, "emitted_batches":emitted,
            "credits_refunded":true, "completed_executions":0, "engine_timing_claimed":false})
        );
        prior = Some(error);
    }
    assert_eq!(
        fs::read_dir(fixture.0.join("window-runs")).unwrap().count(),
        0
    );
}

#[test]
fn compatible_window_later_primary_observation_precedes_secondary_sum_overflow() {
    let fixture = fixture(PrimitiveArray::from_iter([f64::MAX, f64::MAX, f64::NAN]).into_array());
    for (first, second, expected) in [
        ("avg", "sum", "nonfinite"),
        ("sum", "avg", "final result is not finite"),
    ] {
        same_error(
            &fixture,
            &plan(
                &fixture,
                vec![
                    measure(first, Some("value"), "a"),
                    measure(second, Some("value"), "b"),
                ],
            ),
            expected,
            &format!("later_primary_{first}"),
        );
    }
}

#[test]
fn compatible_window_primary_success_preserves_secondary_sum_error() {
    let fixture = fixture(PrimitiveArray::from_iter([f64::MAX, f64::MAX]).into_array());
    same_error(
        &fixture,
        &plan(
            &fixture,
            vec![
                measure("avg", Some("value"), "mean"),
                measure("sum", Some("value"), "total"),
            ],
        ),
        "final result is not finite",
        "secondary_sum",
    );
}

#[test]
fn compatible_window_earlier_secondary_later_row_error_keeps_declaration_priority() {
    let fixture =
        fixture(DecimalArray::from_iter([5_i128, 5, 6], DecimalDType::new(2, 0)).into_array());
    let primary = measure("avg", Some("value"), "rounded")
        .with_decimal_output(DecimalOutput::new(38, 0, DecimalRounding::HalfEven).unwrap());
    let earlier = measure("avg", Some("value"), "exact")
        .with_decimal_output(DecimalOutput::new(38, 0, DecimalRounding::Exact).unwrap());
    let later = measure("sum", Some("value"), "narrow")
        .with_decimal_output(DecimalOutput::new(1, 0, DecimalRounding::Exact).unwrap());
    for (measures, expected, case) in [
        (
            vec![primary.clone(), earlier.clone(), later.clone()],
            "nonzero fractional digits",
            "decimal_earlier_exact",
        ),
        (
            vec![primary, later, earlier],
            "precision overflow",
            "decimal_earlier_precision",
        ),
    ] {
        same_error(&fixture, &plan(&fixture, measures), expected, case);
    }
}

#[test]
fn compatible_statistical_window_preserves_observation_and_finalizer_error_order() {
    for nonfinite in [false, true] {
        let mut values = vec![1.0e200, -1.0e200];
        if nonfinite {
            values.push(f64::NAN);
        }
        let fixture = fixture(PrimitiveArray::from_iter(values).into_array());
        for (first, second) in [("stddev_pop", "var_pop"), ("var_pop", "stddev_pop")] {
            same_error(
                &fixture,
                &plan(
                    &fixture,
                    vec![
                        measure(first, Some("value"), "a"),
                        measure(second, Some("value"), "b"),
                    ],
                ),
                if nonfinite && first == "stddev_pop" {
                    "nonfinite"
                } else {
                    "statistical result is outside its finite domain"
                },
                &format!("moment_{first}_{nonfinite}"),
            );
        }
    }
}

#[test]
fn compatible_decimal_window_counts_use_input_nulls_across_result_policies() {
    let fixture = fixture(
        DecimalArray::from_option_iter([Some(5_i128), None, Some(6)], DecimalDType::new(2, 0))
            .into_array(),
    );
    let plan = plan(
        &fixture,
        vec![
            measure("avg", Some("value"), "mean")
                .with_decimal_output(DecimalOutput::new(3, 1, DecimalRounding::HalfEven).unwrap()),
            measure("count", Some("value"), "nonnull"),
            measure("sum", Some("value"), "total")
                .with_decimal_output(DecimalOutput::new(3, 0, DecimalRounding::Exact).unwrap()),
            measure("count", None, "all_rows"),
        ],
    );
    let expected = vec![
        json!({"id":0,"mean":"decimal128(3,1):50","nonnull":1,"total":"decimal128(3,0):5","all_rows":1}),
        json!({"id":1,"mean":"decimal128(3,1):50","nonnull":1,"total":"decimal128(3,0):5","all_rows":2}),
        json!({"id":2,"mean":"decimal128(3,1):55","nonnull":2,"total":"decimal128(3,0):11","all_rows":3}),
    ];
    let resident = prepare_relational(&plan, policy()).unwrap();
    let baseline = resident.snapshot().memory.reserved_bytes;
    let result = resident
        .collect_jsonl(&CancellationToken::default())
        .unwrap();
    assert_eq!(json_rows(&result), expected);
    assert!(result.execution.native_io_certificate.is_certified());
    assert!(
        !result
            .execution
            .native_io_certificate
            .side_effects
            .fallback_attempted
    );
    drop(result);
    assert_eq!(resident.snapshot().memory.reserved_bytes, baseline);
    let stored = ordered(&fixture, &plan);
    let baseline = stored.snapshot().memory.reserved_bytes;
    let (rows, report) = complete(&stored, 1);
    assert_eq!(rows, expected);
    assert_eq!(stored.output_dtype(), resident.output_dtype());
    drop(report);
    assert_eq!(stored.snapshot().memory.reserved_bytes, baseline);
}

#[test]
fn compatible_window_arithmetic_first_matches_independent_exclusion_membership() {
    let fixture = window_frame_tests::fixture(3);
    for unit in [Unit::Rows, Unit::Groups, Unit::Range] {
        for exclusion in [
            Exclusion::NoOthers,
            Exclusion::CurrentRow,
            Exclusion::Group,
            Exclusion::Ties,
        ] {
            for descending in [false, true] {
                for nulls in [NullOrder::First, NullOrder::Last] {
                    let mut plan = window_frame_tests::plan(
                        fixture.scan(),
                        unit,
                        exclusion,
                        descending,
                        nulls,
                    );
                    let VortexRelationalPlan::Window(window) = &mut plan else {
                        unreachable!()
                    };
                    window.expressions.sort_by_key(|expression| {
                        match expression.output_column.as_str() {
                            "sum" => 0,
                            "avg" => 1,
                            "nonnull" => 2,
                            _ => 3,
                        }
                    });
                    let expected =
                        window_frame_tests::reference(unit, exclusion, descending, nulls);
                    let resident = prepare_relational(&plan, policy()).unwrap();
                    assert_resident(&resident, &expected);
                    let stored = ordered(&fixture, &plan);
                    let baseline = stored.snapshot().memory.reserved_bytes;
                    let (rows, report) = complete(&stored, 3);
                    assert_eq!(rows, expected);
                    assert_eq!(stored.output_dtype(), resident.output_dtype());
                    drop(report);
                    assert_eq!(stored.snapshot().memory.reserved_bytes, baseline);
                }
            }
        }
    }
}

#[test]
fn compatible_window_near_matches_keep_distinct_values_and_membership() {
    use vortex::array::arrays::BoolArray;
    let fixture = Fixture::new(
        StructArray::new(
            FieldNames::from(["id", "value", "other", "keep"]),
            vec![
                PrimitiveArray::from_iter(0u64..6).into_array(),
                PrimitiveArray::from_option_iter([
                    Some(2_i64),
                    None,
                    Some(4),
                    Some(8),
                    None,
                    Some(16),
                ])
                .into_array(),
                PrimitiveArray::from_iter([20_i64, 30, 40, 50, 60, 70]).into_array(),
                BoolArray::from_iter([true, false, true, false, true, false]).into_array(),
            ],
            6,
            Validity::NonNullable,
        )
        .into_array(),
        2,
    );
    let mut two = moving(measure("avg", Some("value"), "two"));
    two.frame.as_mut().unwrap().start =
        Bound::Preceding(Offset::Number(shardloom_core::ScalarValue::UInt64(2)));
    let mut excluded = moving(measure("avg", Some("value"), "excluded"));
    excluded.frame.as_mut().unwrap().exclusion = Exclusion::CurrentRow;
    let mut reverse = moving(measure("avg", Some("value"), "reverse"));
    reverse.order_by[0].descending = true;
    let plan = VortexRelationalPlan::Window(Box::new(Window {
        input: fixture.scan(),
        columns: vec![ColumnRef::new("id").unwrap()],
        expressions: vec![
            moving(measure("sum", Some("value"), "total")),
            moving(measure("avg", Some("other"), "other")),
            moving(measure("avg", Some("value"), "mean")),
            moving(measure("count", Some("value"), "nonnull")),
            moving(
                measure("avg", Some("value"), "filtered")
                    .with_filter_column(ColumnRef::new("keep").unwrap()),
            ),
            moving(measure("sum", Some("value"), "again")),
            two,
            excluded,
            reverse,
            moving(measure("count", None, "all_rows")),
        ],
    }));
    let total = [2.0, 2.0, 4.0, 12.0, 8.0, 16.0];
    let means = [2.0, 2.0, 4.0, 6.0, 8.0, 16.0];
    let other = [20.0, 25.0, 35.0, 45.0, 55.0, 65.0];
    let filtered = [Some(2.0), Some(2.0), Some(4.0), Some(4.0), None, None];
    let two = [2.0, 2.0, 3.0, 6.0, 6.0, 12.0];
    let excluded = [None, Some(2.0), None, Some(4.0), Some(8.0), None];
    let reverse = [2.0, 4.0, 6.0, 8.0, 16.0, 16.0];
    let expected = (0..6)
        .map(|id| {
            json!({"id":id, "total":total[id], "other":other[id],
        "mean":means[id], "nonnull":if id == 3 { 2 } else { 1 }, "filtered":filtered[id],
        "again":total[id], "two":two[id], "excluded":excluded[id], "reverse":reverse[id],
        "all_rows":if id == 0 { 1 } else { 2 }})
        })
        .collect::<Vec<_>>();
    let resident = prepare_relational(&plan, policy()).unwrap();
    assert_resident(&resident, &expected);
    let stored = ordered(&fixture, &plan);
    let baseline = stored.snapshot().memory.reserved_bytes;
    let (rows, report) = complete(&stored, 2);
    assert_eq!(rows, expected);
    assert_eq!(stored.output_dtype(), resident.output_dtype());
    drop(report);
    assert_eq!(stored.snapshot().memory.reserved_bytes, baseline);
}

#[test]
fn compatible_window_empty_and_all_null_outputs_keep_count_semantics() {
    for values in [Vec::<Option<i64>>::new(), vec![None, None, None]] {
        let rows = values.len();
        let fixture = fixture(PrimitiveArray::from_option_iter(values).into_array());
        for count_first in [false, true] {
            let mut measures = vec![
                measure("sum", Some("value"), "sum"),
                measure("avg", Some("value"), "avg"),
                measure("count", Some("value"), "nonnull"),
                measure("var_pop", Some("value"), "vp"),
                measure("var_samp", Some("value"), "vs"),
                measure("stddev_pop", Some("value"), "sp"),
                measure("stddev_samp", Some("value"), "ss"),
                measure("count", None, "all_rows"),
            ];
            if count_first {
                measures.swap(0, 2);
            }
            let plan = plan(&fixture, measures);
            let expected = (0..rows)
                .map(|id| {
                    json!({"id":id, "sum":null, "avg":null,
                "nonnull":0, "vp":null, "vs":null, "sp":null, "ss":null, "all_rows":id+1})
                })
                .collect::<Vec<_>>();
            let resident = prepare_relational(&plan, policy()).unwrap();
            assert_resident(&resident, &expected);
            if rows != 0 {
                let stored = ordered(&fixture, &plan);
                let baseline = stored.snapshot().memory.reserved_bytes;
                let (actual, report) = complete(&stored, 1);
                assert_eq!(actual, expected);
                drop(report);
                assert_eq!(stored.snapshot().memory.reserved_bytes, baseline);
            }
        }
    }
}

#[test]
fn compatible_window_families_refund_denial_cancellation_and_retry() {
    let fixture = fixture(PrimitiveArray::from_iter(0_i32..4097).into_array());
    for functions in [
        vec!["sum", "avg", "count"],
        vec!["var_pop", "var_samp", "stddev_pop", "stddev_samp"],
    ] {
        let plan = VortexRelationalPlan::Window(Box::new(Window {
            input: fixture.scan(),
            columns: vec![ColumnRef::new("id").unwrap()],
            expressions: functions
                .iter()
                .map(|function| moving(measure(function, Some("value"), function)))
                .collect(),
        }));
        let expected = (0_u32..4097).map(|id| {
            if functions[0] == "sum" {
                json!({"id":id, "sum":if id == 0 { 0.0 } else { f64::from(id*2-1) },
                    "avg":if id == 0 { 0.0 } else { f64::from(id)-0.5 }, "count":if id == 0 { 1 } else { 2 }})
            } else {
                json!({"id":id, "var_pop":if id == 0 { 0.0 } else { 0.25 },
                    "var_samp":(id != 0).then_some(0.5), "stddev_pop":if id == 0 { 0.0 } else { 0.5 },
                    "stddev_samp":(id != 0).then_some(0.5_f64.sqrt())})
            }
        }).collect::<Vec<_>>();
        for spill in [false, true] {
            let prepared = if spill {
                ordered(&fixture, &plan)
            } else {
                prepare_relational(&plan, policy()).unwrap()
            };
            let baseline = prepared.snapshot().memory.reserved_bytes;
            let pressure = prepared
                .session
                .memory()
                .reserve(prepared.snapshot().memory.limit_bytes - baseline - 1024)
                .unwrap();
            let mut emitted = 0;
            assert!(
                prepared
                    .for_each_batch(&CancellationToken::default(), |_, _| {
                        emitted += 1;
                        Ok(())
                    })
                    .is_err()
            );
            assert_eq!(emitted, 0);
            drop(pressure);
            assert_eq!(prepared.snapshot().memory.reserved_bytes, baseline);
            let cancel = CancellationToken::default();
            cancel.cancel();
            assert!(
                prepared
                    .for_each_batch(&cancel, |_, _| {
                        emitted += 1;
                        Ok(())
                    })
                    .is_err()
            );
            assert_eq!(emitted, 0);
            assert_eq!(prepared.snapshot().memory.reserved_bytes, baseline);
            let cancel = CancellationToken::default();
            assert!(
                prepared
                    .for_each_batch(&cancel, |_, _| {
                        emitted += 1;
                        cancel.cancel();
                        Ok(())
                    })
                    .is_err()
            );
            assert_eq!(emitted, 1);
            assert_eq!(prepared.snapshot().completed_executions, 0);
            assert_eq!(prepared.snapshot().memory.reserved_bytes, baseline);
            if spill {
                let (rows, report) = complete(&prepared, 257);
                assert_eq!(rows, expected);
                assert_eq!(report.runtime.completed_executions, 1);
                drop(report);
            } else {
                assert_resident(&prepared, &expected);
                assert_eq!(prepared.snapshot().completed_executions, 1);
            }
            assert_eq!(prepared.snapshot().memory.reserved_bytes, baseline);
        }
    }
}

fn extended_expected() -> [[Option<f64>; 6]; 8] {
    let nan = Some(f64::from_bits(
        shardloom_core::float_policy::CANONICAL_F64_NAN_BITS,
    ));
    let positive = Some(f64::INFINITY);
    let negative = Some(f64::NEG_INFINITY);
    let zero = Some(0.0);
    [
        [Some(1.0), Some(1.0), zero, None, zero, None],
        [positive, positive, nan, nan, nan, nan],
        [positive, positive, nan, None, nan, None],
        [negative, negative, nan, None, nan, None],
        [nan; 6],
        [nan; 6],
        [
            Some(8.0),
            Some(4.0),
            Some(1.0),
            Some(2.0),
            Some(1.0),
            Some(2.0_f64.sqrt()),
        ],
        [Some(5.0), Some(5.0), zero, None, zero, None],
    ]
}

fn assert_extended_native_values(prepared: &PreparedVortexRelational, spill: bool) {
    use crate::local_primitives::{logical_field_from_native_array, result_batch};
    let expected = extended_expected();
    let counts = [1, 2, 1, 1, 2, 2, 2, 1];
    let mut seen = 0;
    let report = prepared
        .for_each_batch(&CancellationToken::default(), |array, context| {
            let mut execution = context.native_session().create_execution_ctx();
            let id = logical_field_from_native_array(&array, "id")?;
            let count = logical_field_from_native_array(&array, "count")?;
            for row in 0..array.len() {
                assert!(seen < expected.len());
                let result_batch::Value::UInt(actual_id) =
                    result_batch::scalar_value(&id, row, &mut execution)?
                else {
                    panic!("native id type changed")
                };
                assert_eq!(actual_id, u64::try_from(seen).unwrap());
                let result_batch::Value::UInt(actual_count) =
                    result_batch::scalar_value(&count, row, &mut execution)?
                else {
                    panic!("native count type changed")
                };
                assert_eq!(actual_count, counts[seen]);
                for (column, name) in [
                    "sum",
                    "avg",
                    "var_pop",
                    "var_samp",
                    "stddev_pop",
                    "stddev_samp",
                ]
                .iter()
                .enumerate()
                {
                    let field = logical_field_from_native_array(&array, name)?;
                    let actual = result_batch::scalar_value(&field, row, &mut execution)?;
                    match (actual, expected[seen][column]) {
                        (result_batch::Value::Null, None) => {}
                        (result_batch::Value::Float(actual), Some(wanted)) => {
                            assert_eq!(actual.to_bits(), wanted.to_bits(), "row {seen}, {name}");
                        }
                        _ => panic!("native value or nullability changed at row {seen}, {name}"),
                    }
                }
                seen += 1;
            }
            Ok(())
        })
        .unwrap();
    assert_eq!(seen, expected.len());
    assert!(report.native_io_certificate.is_certified());
    assert!(!report.native_io_certificate.side_effects.fallback_attempted);
    assert_eq!(report.ordered_window_stages, u64::from(spill));
    if spill {
        let spill = report.spill.as_ref().unwrap();
        assert!(spill.owned_cleanup_completed);
        assert_eq!(fs::read_dir(&spill.workspace).unwrap().count(), 0);
    }
}

fn extended_plan(input: VortexRelationalPlan, count_first: bool) -> (VortexRelationalPlan, DType) {
    use vortex::array::dtype::Nullability;
    let mut names = [
        "sum",
        "avg",
        "count",
        "var_pop",
        "var_samp",
        "stddev_pop",
        "stddev_samp",
    ];
    if count_first {
        names.swap(0, 2);
    }
    let plan = VortexRelationalPlan::Window(Box::new(Window {
        input,
        columns: vec![ColumnRef::new("id").unwrap()],
        expressions: names
            .iter()
            .map(|name| moving(measure(name, Some("value"), name)))
            .collect(),
    }));
    let schema = DType::struct_(
        std::iter::once("id")
            .chain(names)
            .map(|name| {
                let dtype = if matches!(name, "id" | "count") {
                    DType::Primitive(PType::U64, Nullability::NonNullable)
                } else {
                    DType::Primitive(PType::F64, Nullability::Nullable)
                };
                (name.to_owned(), dtype)
            })
            .collect::<Vec<_>>(),
        Nullability::NonNullable,
    );
    (plan, schema)
}

#[test]
fn compatible_window_extended_nonfinites_keep_counts_bits_and_finite_recovery() {
    use shardloom_core::FloatPolicy;
    let fixture = fixture(
        PrimitiveArray::from_option_iter([
            Some(1.0),
            Some(f64::INFINITY),
            None,
            Some(f64::NEG_INFINITY),
            Some(f64::from_bits(0xfff8_0000_0000_0042)),
            Some(3.0),
            Some(5.0),
            None,
        ])
        .into_array(),
    );
    for count_first in [false, true] {
        let (plan, schema) = extended_plan(fixture.scan(), count_first);
        for float_policy in [FloatPolicy::Strict, FloatPolicy::Extended] {
            for spill in [false, true] {
                let session =
                    ResidentVortexSession::new_with_float_policy(32 << 20, 1, float_policy)
                        .unwrap();
                let memory = session.memory().clone();
                let mut prepared =
                    prepare_relational_in_session(&plan, policy(), &session).unwrap();
                assert!(!prepared.root_has_file_kernel());
                if spill {
                    let workspace = fixture.0.join("window-runs");
                    fs::create_dir_all(&workspace).unwrap();
                    prepared = prepared
                        .with_spill(
                            VortexRelationalSpillPolicy::new(workspace, 128 << 20, 1 << 20)
                                .unwrap(),
                        )
                        .unwrap();
                }
                assert_eq!(prepared.output_dtype().as_ref(), Some(&schema));
                let baseline = prepared.snapshot().memory.reserved_bytes;
                if float_policy == FloatPolicy::Extended {
                    assert_extended_native_values(&prepared, spill);
                    assert_eq!(prepared.snapshot().completed_executions, 1);
                } else {
                    let mut emitted = 0;
                    let error = prepared
                        .for_each_batch(&CancellationToken::default(), |_, _| {
                            emitted += 1;
                            Ok(())
                        })
                        .err()
                        .unwrap()
                        .to_string();
                    assert!(error.contains("nonfinite"), "{error}");
                    assert_eq!(emitted, 0);
                    assert_eq!(prepared.snapshot().completed_executions, 0);
                }
                assert_eq!(prepared.snapshot().memory.reserved_bytes, baseline);
                drop(prepared);
                drop(session);
                assert_eq!(memory.snapshot().reserved_bytes, 0);
                if spill {
                    assert_eq!(
                        fs::read_dir(fixture.0.join("window-runs")).unwrap().count(),
                        0
                    );
                }
            }
        }
    }
}
