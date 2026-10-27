//! Numeric accumulator reuse is limited to identical inputs and completed state.

use super::*;
use crate::resident_session::ResidentVortexSession;
use shardloom_exec::compute_pool::CancellationToken;
use vortex::array::{
    arrays::{BoolArray, PrimitiveArray},
    dtype::Nullability,
};

fn numeric_measure(function: Function, column: &str) -> Measure {
    Measure {
        function,
        column: Some(column.into()),
        secondary_column: None,
        filter_column: None,
        quantile_level: None,
        selection: None,
        separator: None,
        dtype: DType::Primitive(PType::F64, Nullability::Nullable),
        decimal_source: None,
        distinct_fields: vec![],
        distinct_names: vec![],
    }
}

fn spec(measures: Vec<Measure>, grouped: bool) -> Spec {
    let mut result = Spec {
        fields: vec![],
        groups: vec![],
        group_names: vec![],
        measures,
    };
    if grouped {
        let dtype = DType::Primitive(PType::I64, Nullability::NonNullable);
        result.groups.push(("g".into(), dtype.clone()));
        result.group_names.push("g".into());
        result.fields.push(("g".into(), dtype));
    }
    result
}

fn batch(columns: Vec<(&str, ArrayRef)>) -> ArrayRef {
    let rows = columns.first().map_or(0, |(_, values)| values.len());
    StructArray::try_new(
        FieldNames::from_iter(columns.iter().map(|(name, _)| *name)),
        columns.into_iter().map(|(_, values)| values),
        rows,
        Validity::NonNullable,
    )
    .unwrap()
    .into_array()
}

fn assert_sum_bits(actual: NumericAggregateSum, expected: NumericAggregateSum) {
    match (actual, expected) {
        (NumericAggregateSum::Ordered(a), NumericAggregateSum::Ordered(e)) => {
            assert_eq!(a.to_bits(), e.to_bits());
        }
        (
            NumericAggregateSum::Integer {
                sum: a,
                correction: ac,
            },
            NumericAggregateSum::Integer {
                sum: e,
                correction: ec,
            },
        ) => {
            assert_eq!(a.to_bits(), e.to_bits());
            assert_eq!(ac.to_bits(), ec.to_bits());
        }
        _ => panic!("numeric accumulator modes differ"),
    }
}

#[test]
fn adjacent_numeric_aliases_reuse_only_same_array_identity() {
    let session = ResidentVortexSession::new(2 << 20, 1).unwrap();
    let memory = session.memory().clone();
    session
        .with_native_execution_context(&CancellationToken::default(), |context| {
            let numbers = PrimitiveArray::from_option_iter([Some(2i64), None, Some(-1), Some(3)])
                .into_array();
            let binding = spec(
                vec![
                    numeric_measure(Function::Sum, "n"),
                    numeric_measure(Function::Avg, "alias"),
                    numeric_measure(Function::Sum, "n"),
                ],
                true,
            );
            let mut aggregate = Aggregate::new(&binding, context.memory())?;
            #[cfg(debug_assertions)]
            let observation = super::super::native_relational_observation::Session::start();
            let groups = PrimitiveArray::from_iter([1i64, 1, 2, 2]).into_array();
            for _ in 0..2 {
                aggregate.consume(
                    &batch(vec![
                        ("g", groups.clone()),
                        ("n", numbers.clone()),
                        ("alias", numbers.clone()),
                    ]),
                    context,
                    2,
                )?;
            }
            #[cfg(debug_assertions)]
            {
                let counts = observation.finish();
                assert_eq!(counts.aggregate_numeric_bound_batches, 2);
                assert_eq!(counts.aggregate_numeric_state_reuses, 4);
                assert_eq!(counts.aggregate_numeric_reused_rows, 16);
                assert_eq!(counts.aggregate_value_preparations, 2);
            }
            for group in 0..2 {
                for index in [0, 1, 2] {
                    let state = &aggregate.states.values[group * 3 + index];
                    assert_eq!(
                        (state.count, state.sum.total()),
                        (if group == 0 { 2 } else { 4 }, 4.0)
                    );
                }
            }

            let separate = PrimitiveArray::from_option_iter([Some(2i64), None, Some(-1), Some(3)])
                .into_array();
            let independent = spec(
                vec![
                    numeric_measure(Function::Sum, "n"),
                    numeric_measure(Function::Avg, "alias"),
                    numeric_measure(Function::Sum, "n"),
                ],
                false,
            );
            let mut aggregate = Aggregate::new(&independent, context.memory())?;
            #[cfg(debug_assertions)]
            let observation = super::super::native_relational_observation::Session::start();
            aggregate.consume(
                &batch(vec![("n", numbers.clone()), ("alias", separate)]),
                context,
                2,
            )?;
            #[cfg(debug_assertions)]
            assert_eq!(observation.finish().aggregate_numeric_bound_batches, 3);
            assert_eq!(
                aggregate
                    .states
                    .values
                    .iter()
                    .map(|s| s.sum.total())
                    .collect::<Vec<_>>(),
                vec![4.0, 4.0, 4.0]
            );
            Ok(())
        })
        .unwrap();
    drop(session);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

#[test]
fn numeric_reuse_declines_full_group_traversal_for_high_cardinality_and_small_later_batches() {
    let session = ResidentVortexSession::new(1 << 20, 1).unwrap();
    let memory = session.memory().clone();
    session
        .with_native_execution_context(&CancellationToken::default(), |context| {
            let binding = spec(
                vec![
                    numeric_measure(Function::Sum, "n"),
                    numeric_measure(Function::Avg, "n"),
                ],
                true,
            );
            let mut aggregate = Aggregate::new(&binding, context.memory())?;
            #[cfg(debug_assertions)]
            let observation = super::super::native_relational_observation::Session::start();
            for (groups, numbers) in [
                (vec![0i64, 1, 2, 3], vec![1i64, 2, 3, 4]),
                (vec![0], vec![5]),
            ] {
                aggregate.consume(
                    &batch(vec![
                        ("g", PrimitiveArray::from_iter(groups).into_array()),
                        ("n", PrimitiveArray::from_iter(numbers).into_array()),
                    ]),
                    context,
                    2,
                )?;
            }
            #[cfg(debug_assertions)]
            {
                let counts = observation.finish();
                assert_eq!(counts.aggregate_numeric_bound_batches, 4);
                assert_eq!(counts.aggregate_numeric_bound_rows, 10);
                assert_eq!(counts.aggregate_numeric_state_reuses, 0);
                assert_eq!(counts.aggregate_numeric_reuse_comparison_groups, 0);
            }
            for (group, expected) in [(2, 6.0), (1, 2.0), (1, 3.0), (1, 4.0)]
                .into_iter()
                .enumerate()
            {
                for state in &aggregate.states.values[group * 2..group * 2 + 2] {
                    assert_eq!((state.count, state.sum.total()), expected);
                }
            }
            Ok(())
        })
        .unwrap();
    drop(session);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

#[test]
fn numeric_reuse_declines_overflow_and_unequal_accumulator_starts() {
    let session = ResidentVortexSession::new(1 << 20, 1).unwrap();
    let memory = session.memory().clone();
    session
        .with_native_execution_context(&CancellationToken::default(), |context| {
            let numbers = PrimitiveArray::from_iter([1i64, 2]).into_array();
            let binding = spec(
                vec![
                    numeric_measure(Function::Sum, "n"),
                    numeric_measure(Function::Avg, "n"),
                ],
                false,
            );
            let input = batch(vec![("n", numbers)]);
            let mut aggregate = Aggregate::new(&binding, context.memory())?;
            aggregate.states.values[1].count = u64::MAX;
            let error = aggregate
                .consume(&input, context, 2)
                .unwrap_err()
                .to_string();
            assert!(error.contains("aggregate count overflow"), "{error}");
            assert_eq!(aggregate.states.values[0].count, 2);
            assert_eq!(aggregate.states.values[0].sum.total(), 3.0);
            assert_eq!(aggregate.states.values[1].count, u64::MAX);
            assert_eq!(aggregate.states.values[1].sum.total(), 0.0);

            let floats = PrimitiveArray::from_iter([-0.0f64]).into_array();
            let binding = spec(
                vec![
                    numeric_measure(Function::Sum, "n"),
                    numeric_measure(Function::Avg, "n"),
                ],
                false,
            );
            let mut aggregate = Aggregate::new(&binding, context.memory())?;
            aggregate.states.values[0].sum = NumericAggregateSum::Ordered(-0.0);
            aggregate.states.values[1].sum = NumericAggregateSum::Ordered(0.0);
            let input = batch(vec![("n", floats)]);
            #[cfg(debug_assertions)]
            let observation = super::super::native_relational_observation::Session::start();
            aggregate.consume(&input, context, 2)?;
            #[cfg(debug_assertions)]
            assert_eq!(observation.finish().aggregate_numeric_bound_batches, 2);
            assert_eq!(
                aggregate.states.values[0].sum.total().to_bits(),
                (-0.0f64).to_bits()
            );
            assert_eq!(
                aggregate.states.values[1].sum.total().to_bits(),
                0.0f64.to_bits()
            );

            let mut aggregate = Aggregate::new(&binding, context.memory())?;
            aggregate.states.values[0].sum = NumericAggregateSum::Integer {
                sum: 1e16,
                correction: 1.0,
            };
            aggregate.states.values[1].sum = NumericAggregateSum::Integer {
                sum: 1e16,
                correction: 0.0,
            };
            let input = batch(vec![("n", PrimitiveArray::from_iter([1i64]).into_array())]);
            let mut expected_first = State {
                sum: NumericAggregateSum::Integer {
                    sum: 1e16,
                    correction: 1.0,
                },
                ..State::default()
            };
            let mut expected_second = State {
                sum: NumericAggregateSum::Integer {
                    sum: 1e16,
                    correction: 0.0,
                },
                ..State::default()
            };
            update_numeric(&mut expected_first, 1.0, true)?;
            update_numeric(&mut expected_second, 1.0, true)?;
            #[cfg(debug_assertions)]
            let observation = super::super::native_relational_observation::Session::start();
            aggregate.consume(&input, context, 2)?;
            #[cfg(debug_assertions)]
            assert_eq!(observation.finish().aggregate_numeric_bound_batches, 2);
            assert_eq!(aggregate.states.values[0].count, expected_first.count);
            assert_eq!(aggregate.states.values[1].count, expected_second.count);
            assert_sum_bits(aggregate.states.values[0].sum, expected_first.sum);
            assert_sum_bits(aggregate.states.values[1].sum, expected_second.sum);
            Ok(())
        })
        .unwrap();
    drop(session);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

#[test]
fn same_filter_reuses_selection_and_numeric_owner_but_changed_domain_fails_in_order() {
    let session = ResidentVortexSession::new(1 << 20, 1).unwrap();
    let memory = session.memory().clone();
    session
        .with_native_execution_context(&CancellationToken::default(), |context| {
            let mut first = numeric_measure(Function::Sum, "n");
            first.filter_column = Some("keep".into());
            let mut second = numeric_measure(Function::Avg, "n");
            second.filter_column = Some("keep".into());
            let binding = spec(vec![first, second], false);
            let mut aggregate = Aggregate::new(&binding, context.memory())?;
            let data = |keep: Vec<bool>| {
                batch(vec![
                    (
                        "n",
                        PrimitiveArray::from_iter([1.0f64, f64::NAN, 3.0]).into_array(),
                    ),
                    ("keep", BoolArray::from_iter(keep).into_array()),
                    (
                        "other",
                        BoolArray::from_iter([false, true, false]).into_array(),
                    ),
                ])
            };
            #[cfg(debug_assertions)]
            let observation = super::super::native_relational_observation::Session::start();
            aggregate.consume(&data(vec![true, false, true]), context, 2)?;
            #[cfg(debug_assertions)]
            {
                let counts = observation.finish();
                assert_eq!(counts.aggregate_numeric_bound_batches, 1);
                assert_eq!(counts.aggregate_numeric_state_reuses, 1);
                assert_eq!(counts.aggregate_value_preparations, 1);
                assert_eq!(counts.aggregate_filter_selections, 1);
            }
            for state in &aggregate.states.values {
                assert_eq!((state.count, state.sum.total()), (2, 4.0));
            }
            aggregate.consume(&data(vec![false, false, false]), context, 2)?;
            for state in &aggregate.states.values {
                assert_eq!((state.count, state.sum.total()), (2, 4.0));
            }
            let mut changed = spec(
                vec![
                    numeric_measure(Function::Sum, "n"),
                    numeric_measure(Function::Avg, "n"),
                ],
                false,
            );
            changed.measures[0].filter_column = Some("keep".into());
            changed.measures[1].filter_column = Some("other".into());
            let mut aggregate = Aggregate::new(&changed, context.memory())?;
            let error = aggregate
                .consume(&data(vec![true, false, true]), context, 2)
                .unwrap_err()
                .to_string();
            assert!(error.contains("nonfinite"), "{error}");
            assert_eq!(
                (
                    aggregate.states.values[0].count,
                    aggregate.states.values[0].sum.total()
                ),
                (2, 4.0)
            );
            assert_eq!(
                (
                    aggregate.states.values[1].count,
                    aggregate.states.values[1].sum.total()
                ),
                (0, 0.0)
            );
            Ok(())
        })
        .unwrap();
    drop(session);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}
