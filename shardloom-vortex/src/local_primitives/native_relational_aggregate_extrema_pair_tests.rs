use super::super::{Aggregate, NumericAggregateSum, keys, update};
use super::*;
use crate::local_primitives::native_capacity::ReservedVec;
use crate::local_primitives::native_relational_keys::KeyColumn;
use crate::resident_session::ResidentVortexSession;
use shardloom_exec::compute_pool::CancellationToken;
use vortex::array::{
    IntoArray as _,
    arrays::{
        BoolArray, ChunkedArray, ConstantArray, DecimalArray, DictArray, ExtensionArray,
        PrimitiveArray, StructArray,
    },
    dtype::{DType, DecimalDType, FieldNames, Nullability, PType},
    extension::datetime::{Date, TimeUnit},
    scalar::Scalar,
    validity::Validity,
};

fn measure(function: Function, dtype: &DType) -> Measure {
    Measure {
        function,
        column: Some("n".into()),
        secondary_column: None,
        quantile_level: None,
        separator: None,
        filter_column: None,
        selection: None,
        dtype: match function {
            Function::Count => DType::Primitive(PType::U64, Nullability::NonNullable),
            Function::Sum | Function::Avg => DType::Primitive(PType::F64, Nullability::Nullable),
            _ => dtype.as_nullable(),
        },
        decimal_source: None,
        decimal_output: None,
        distinct_fields: vec![],
        // The ordinary binder retains these labels even for non-DISTINCT measures.
        distinct_names: vec!["group".into(), "value".into()],
    }
}

fn spec(dtype: &DType, reverse: bool, count: bool) -> Spec {
    let mut functions = vec![Function::Sum];
    functions.extend(if reverse {
        [Function::Max, Function::Min]
    } else {
        [Function::Min, Function::Max]
    });
    if count {
        functions.push(Function::Count);
    }
    functions.push(Function::Sum);
    let measures: Vec<_> = functions
        .into_iter()
        .map(|function| measure(function, dtype))
        .collect();
    let groups = vec![(
        "g".into(),
        DType::Primitive(PType::I64, Nullability::NonNullable),
    )];
    let mut fields = groups.clone();
    fields.extend(
        measures
            .iter()
            .enumerate()
            .map(|(index, measure)| (format!("v{index}"), measure.dtype.clone())),
    );
    Spec {
        fields,
        groups,
        group_names: vec!["g".into()],
        measures,
    }
}

fn states(groups: usize, width: usize, count: u64) -> Vec<State> {
    (0..groups * width)
        .map(|index| State {
            count,
            sum: if index % 2 == 0 {
                NumericAggregateSum::Ordered(-0.0)
            } else {
                NumericAggregateSum::Integer {
                    sum: 13.0,
                    correction: 0.25,
                }
            },
            extreme: None,
        })
        .collect()
}

fn copy_states(source: &[State]) -> Vec<State> {
    source
        .iter()
        .map(|state| State {
            count: state.count,
            sum: state.sum,
            extreme: state.extreme.clone(),
        })
        .collect()
}

fn sum_bits(sum: NumericAggregateSum) -> (u8, u64, u64) {
    match sum {
        NumericAggregateSum::Ordered(value) => (0, value.to_bits(), 0),
        NumericAggregateSum::Integer { sum, correction } => {
            (1, sum.to_bits(), correction.to_bits())
        }
    }
}

fn assert_states(actual: &[State], expected: &[State]) {
    assert_eq!(actual.len(), expected.len());
    for (actual, expected) in actual.iter().zip(expected) {
        assert_eq!(actual.count, expected.count);
        assert_eq!(actual.extreme, expected.extreme);
        assert_eq!(sum_bits(actual.sum), sum_bits(expected.sum));
    }
}

fn ordinals(
    rows: usize,
    groups: usize,
    context: &NativeExecutionContext<'_>,
) -> Result<GroupOrdinals> {
    let mut values = ReservedVec::new(context.memory())?;
    values.reserve(rows)?;
    values.values.extend((0..rows).map(|row| row % groups));
    Ok(GroupOrdinals::Grouped(values))
}

fn sequential(
    spec: &Spec,
    range: std::ops::Range<usize>,
    states: &mut [State],
    ordinals: &GroupOrdinals,
    values: &KeyColumn,
    context: &NativeExecutionContext<'_>,
) -> Result<()> {
    for index in range {
        for (row, group) in ordinals.iter() {
            let value = values.raw_cell(row)?;
            update(
                &mut states[group * spec.measures.len() + index],
                &spec.measures[index],
                Some(value),
                context,
            )?;
        }
    }
    Ok(())
}

fn compare_column(column: &ArrayRef) {
    let session = ResidentVortexSession::new(8 << 20, 1).unwrap();
    let memory = session.memory().clone();
    session
        .with_native_execution_context(&CancellationToken::default(), |context| {
            let values = keys(column, context)?;
            let KeyColumn::Numeric(owner) = &values else {
                panic!("integer owner expected")
            };
            for reverse in [false, true] {
                for count in [false, true] {
                    let binding = spec(column.dtype(), reverse, count);
                    let groups = if column.len() >= 6 { 3 } else { 1 };
                    let ordinals = ordinals(column.len(), groups, context)?;
                    let mut actual = states(groups, binding.measures.len(), 11);
                    let mut expected = copy_states(&actual);
                    let end = if count { 4 } else { 3 };
                    sequential(&binding, 1..end, &mut expected, &ordinals, &values, context)?;
                    let before = memory.snapshot().reserved_bytes;
                    #[cfg(debug_assertions)]
                    let observation =
                        super::super::super::native_relational_observation::Session::start();
                    let observed =
                        accumulate(&binding, 1, &mut actual, &ordinals, column, owner, context)?;
                    if column.len() < 2 {
                        assert!(observed.is_none());
                        sequential(&binding, 1..end, &mut actual, &ordinals, &values, context)?;
                    } else {
                        assert_eq!(observed, Some(end));
                    }
                    #[cfg(debug_assertions)]
                    {
                        let counts = observation.finish();
                        assert_eq!(
                            counts.aggregate_integer_extrema_fused_batches,
                            u64::from(column.len() >= 2)
                        );
                        assert_eq!(
                            counts.aggregate_integer_extrema_fused_rows,
                            if column.len() >= 2 {
                                column.len() as u64
                            } else {
                                0
                            }
                        );
                        assert_eq!(
                            counts.aggregate_integer_extrema_saved_measure_rows,
                            if column.len() >= 2 {
                                column.len() as u64 * if count { 2 } else { 1 }
                            } else {
                                0
                            }
                        );
                    }
                    assert_states(&actual, &expected);
                    assert_eq!(memory.snapshot().reserved_bytes, before);
                }
            }
            Ok(())
        })
        .unwrap();
    drop(session);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

fn wrapped(column: &ArrayRef, depth: usize) -> ArrayRef {
    let mut result = column.clone();
    for _ in 0..depth {
        result = ChunkedArray::try_new(vec![result], column.dtype().clone())
            .unwrap()
            .into_array();
    }
    result
}

#[test]
fn integer_extrema_pairs_match_every_width_nulls_slices_and_exact_limits() {
    macro_rules! check {
        ($($ty:ty),+) => { $(
            let values = PrimitiveArray::from_iter([
                <$ty>::MAX, 0, <$ty>::MIN, 1, 0, <$ty>::MAX, <$ty>::MIN, 1,
            ]).into_array();
            compare_column(&values);
            compare_column(&wrapped(&values, 1));
            compare_column(&wrapped(&values, 8));
            compare_column(&values.slice(1..7).unwrap());
            compare_column(&values.slice(2..2).unwrap());
            compare_column(&values.slice(2..3).unwrap());
            let values = PrimitiveArray::from_option_iter((0..2053).map(|row| match row % 7 {
                0 | 4 => None,
                1 | 5 => Some(<$ty>::MIN),
                2 | 6 => Some(<$ty>::MAX),
                _ => Some(1),
            })).into_array();
            compare_column(&values);
            compare_column(&wrapped(&values, 1));
            compare_column(&wrapped(&values, 8));
            compare_column(&PrimitiveArray::from_option_iter(std::iter::repeat_n(None::<$ty>, 2053)).into_array());
        )+ };
    }
    check!(i8, i16, i32, i64, u8, u16, u32, u64);
}

#[test]
fn integer_extrema_pair_admission_declines_without_changing_any_state() {
    let session = ResidentVortexSession::new(2 << 20, 1).unwrap();
    let memory = session.memory().clone();
    session
        .with_native_execution_context(&CancellationToken::default(), |context| {
            let column = PrimitiveArray::from_iter([1i64, 2, 3, 4, 5, 6, 7, 8]).into_array();
            let values = keys(&column, context)?;
            let KeyColumn::Numeric(owner) = &values else {
                panic!("numeric owner expected")
            };
            for variant in 0..15 {
                let mut source = column.clone();
                let mut binding = spec(column.dtype(), false, true);
                let mut actual = states(1, binding.measures.len(), 0);
                let mut ordinals = ordinals(8, 1, context)?;
                match variant {
                    0 => binding.measures[2].column = Some("different".into()),
                    1 => binding.measures[1].filter_column = Some("keep".into()),
                    2 => binding.measures[2].filter_column = Some("keep".into()),
                    3 => binding.measures[2].function = Function::Count,
                    4 => binding.measures[2].secondary_column = Some("other".into()),
                    5 => binding.measures[1]
                        .distinct_fields
                        .push(("n".into(), column.dtype().clone())),
                    6 => actual[1].count = 1,
                    7 => actual[3].count = 1,
                    8 => actual[1].extreme = Some(Cell::Float(1.0_f64.to_bits())),
                    9 => actual[2].extreme = Some(Cell::Date(1)),
                    10 => {
                        actual[1].count = u64::MAX - 7;
                        actual[2].count = u64::MAX - 7;
                        actual[3].count = u64::MAX - 7;
                    }
                    11 => ordinals = GroupOrdinals::Scalar(8),
                    12 => actual = states(5, binding.measures.len(), 0),
                    13 => source = wrapped(&column, 9),
                    14 => {
                        source =
                            PrimitiveArray::from_iter([1i32, 2, 3, 4, 5, 6, 7, 8]).into_array();
                    }
                    _ => unreachable!(),
                }
                let expected = copy_states(&actual);
                assert!(
                    accumulate(&binding, 1, &mut actual, &ordinals, &source, owner, context)?
                        .is_none()
                );
                assert_states(&actual, &expected);
            }
            Ok(())
        })
        .unwrap();
    drop(session);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

#[test]
fn integer_extrema_pair_declines_nonprimitive_and_float_sources() {
    let session = ResidentVortexSession::new(2 << 20, 1).unwrap();
    let memory = session.memory().clone();
    session
        .with_native_execution_context(&CancellationToken::default(), |context| {
            let primitive = PrimitiveArray::from_iter([3i64, -2, 1, 3, -2, 1, 3, -2]).into_array();
            let dictionary = DictArray::try_new(
                PrimitiveArray::from_iter([0u8, 1, 2, 0, 1, 2, 0, 1]).into_array(),
                PrimitiveArray::from_iter([3i64, -2, 1]).into_array(),
            )
            .unwrap()
            .into_array();
            let chunked = ChunkedArray::try_new(
                vec![
                    primitive.slice(0..3).unwrap(),
                    primitive.slice(3..8).unwrap(),
                ],
                primitive.dtype().clone(),
            )
            .unwrap()
            .into_array();
            let constant = ConstantArray::new(Scalar::from(3i64), 8).into_array();
            let floating =
                PrimitiveArray::from_iter([0.0_f64, -0.0, 1.0, -1.0, 3.0, 4.0, 5.0, 6.0])
                    .into_array();
            for column in [dictionary, chunked, constant, floating] {
                let values = keys(&column, context)?;
                let KeyColumn::Numeric(owner) = &values else {
                    panic!("numeric owner expected")
                };
                let binding = spec(column.dtype(), false, true);
                let ordinals = ordinals(column.len(), 1, context)?;
                let mut actual = states(1, binding.measures.len(), 0);
                let expected = copy_states(&actual);
                assert!(
                    accumulate(&binding, 1, &mut actual, &ordinals, &column, owner, context)?
                        .is_none()
                );
                assert_states(&actual, &expected);
                sequential(&binding, 1..4, &mut actual, &ordinals, &values, context)?;
            }
            Ok(())
        })
        .unwrap();
    drop(session);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

#[test]
fn integer_extrema_pair_keeps_encoded_count_validity_at_original_position() {
    let session = ResidentVortexSession::new(2 << 20, 1).unwrap();
    let memory = session.memory().clone();
    session
        .with_native_execution_context(&CancellationToken::default(), |context| {
            let mask = ChunkedArray::try_new(
                vec![
                    BoolArray::from_iter([true, false, true, false]).into_array(),
                    BoolArray::from_iter([true, true, false, true]).into_array(),
                ],
                DType::Bool(Nullability::NonNullable),
            )
            .unwrap()
            .into_array();
            let primitive =
                PrimitiveArray::new(vec![3i64, -2, 1, 3, -2, 1, 3, -2], Validity::Array(mask))
                    .into_array();
            for depth in [0, 1, 8] {
                let column = wrapped(&primitive, depth);
                let values = keys(&column, context)?;
                assert!(matches!(values, KeyColumn::Numeric(_)));
                assert!(!count_ready(&primitive));
                let mut binding = spec(column.dtype(), false, true);
                binding.measures.remove(0);
                binding.measures.pop();
                let input = batch(PrimitiveArray::from_iter([1i64; 8]).into_array(), column);
                let mut aggregate = Aggregate::new(&binding, &memory)?;
                let ordinals = ordinals(8, 1, context)?;
                let mut expected: Vec<_> = (0..3).map(|_| State::default()).collect();
                #[cfg(debug_assertions)]
                let observation =
                    super::super::super::native_relational_observation::Session::start();
                sequential(&binding, 0..3, &mut expected, &ordinals, &values, context)?;
                aggregate.consume(&input, context, 8)?;
                assert_states(&aggregate.states.values, &expected);
                // Only COUNT now overflows. Both extrema must finish first,
                // and COUNT must fail in its ordinary per-measure traversal.
                aggregate.states.values[2].count = u64::MAX - 2;
                expected = copy_states(&aggregate.states.values);
                let expected_error =
                    sequential(&binding, 0..3, &mut expected, &ordinals, &values, context)
                        .unwrap_err();
                let actual_error = aggregate.consume(&input, context, 8).unwrap_err();
                assert_eq!(actual_error.to_string(), expected_error.to_string());
                assert!(
                    actual_error
                        .to_string()
                        .contains("aggregate count overflow")
                );
                assert_states(&aggregate.states.values, &expected);
                #[cfg(debug_assertions)]
                {
                    let counts = observation.finish();
                    assert_eq!(counts.aggregate_integer_extrema_fused_batches, 2);
                    assert_eq!(counts.aggregate_integer_extrema_fused_rows, 16);
                    assert_eq!(counts.aggregate_integer_extrema_saved_measure_rows, 16);
                }
            }
            Ok(())
        })
        .unwrap();
    drop(session);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

fn batch(groups: ArrayRef, numbers: ArrayRef) -> ArrayRef {
    let rows = numbers.len();
    StructArray::try_new(
        FieldNames::from(["g", "n"]),
        vec![groups, numbers],
        rows,
        Validity::NonNullable,
    )
    .unwrap()
    .into_array()
}

#[test]
fn integer_extrema_pair_overflow_decline_preserves_measure_first_errors() {
    let session = ResidentVortexSession::new(2 << 20, 1).unwrap();
    let memory = session.memory().clone();
    session
        .with_native_execution_context(&CancellationToken::default(), |context| {
            let column = PrimitiveArray::from_iter([3i64, -2, 1, 3, -2, 1, 3, -2]).into_array();
            let input = batch(
                PrimitiveArray::from_iter([1i64; 8]).into_array(),
                column.clone(),
            );
            let mut binding = spec(column.dtype(), false, true);
            // Remove untouched SUM sentinels so the paired measures are first.
            binding.measures.remove(0);
            binding.measures.pop();
            for failing in 0..3 {
                let mut aggregate = Aggregate::new(&binding, &memory)?;
                aggregate.consume(&input, context, 8)?;
                aggregate.states.values[failing].count = u64::MAX - 2;
                let mut expected = copy_states(&aggregate.states.values);
                let values = keys(&column, context)?;
                let ordinals = ordinals(8, 1, context)?;
                let expected_error =
                    sequential(&binding, 0..3, &mut expected, &ordinals, &values, context)
                        .unwrap_err();
                #[cfg(debug_assertions)]
                let observation =
                    super::super::super::native_relational_observation::Session::start();
                let actual_error = aggregate.consume(&input, context, 8).unwrap_err();
                assert_eq!(actual_error.to_string(), expected_error.to_string());
                assert!(
                    actual_error
                        .to_string()
                        .contains("aggregate count overflow")
                );
                assert_states(&aggregate.states.values, &expected);
                #[cfg(debug_assertions)]
                assert_eq!(
                    observation.finish().aggregate_integer_extrema_fused_batches,
                    0
                );
            }
            Ok(())
        })
        .unwrap();
    drop(session);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

#[test]
fn integer_extrema_pair_reuses_count_mask_across_batches_and_keeps_outputs_owned() {
    let session = ResidentVortexSession::new(4 << 20, 1).unwrap();
    let memory = session.memory().clone();
    let output = session
        .with_native_execution_context(&CancellationToken::default(), |context| {
            let dtype = DType::Primitive(PType::I64, Nullability::Nullable);
            let mut binding = spec(&dtype, false, true);
            binding.measures[4] = measure(Function::Count, &dtype);
            binding.fields[5].1 = binding.measures[4].dtype.clone();
            let mut aggregate = Aggregate::new(&binding, &memory)?;
            #[cfg(debug_assertions)]
            let observation = super::super::super::native_relational_observation::Session::start();
            for pass in 0..3 {
                let numbers = PrimitiveArray::from_option_iter((0..2053).map(|row| {
                    if row % 7 == 0 {
                        None
                    } else {
                        Some(i64::from((row + pass) % 23) - 11)
                    }
                }))
                .into_array();
                let groups = PrimitiveArray::from_iter(
                    (0..2053).map(|row| i64::from((row + pass) % (pass + 2))),
                )
                .into_array();
                aggregate.consume(&batch(groups, numbers), context, 7)?;
            }
            #[cfg(debug_assertions)]
            {
                let counts = observation.finish();
                assert_eq!(counts.aggregate_integer_extrema_fused_batches, 3);
                assert_eq!(counts.aggregate_integer_extrema_fused_rows, 6159);
                assert_eq!(counts.aggregate_integer_extrema_saved_measure_rows, 12318);
            }
            for (group, row) in aggregate
                .states
                .values
                .as_chunks::<5>()
                .0
                .iter()
                .enumerate()
            {
                let numbers = (0..3)
                    .flat_map(|pass| {
                        (0..2053)
                            .filter(move |index| {
                                index % 7 != 0 && (index + pass) % (pass + 2) == group
                            })
                            .map(move |index| i64::try_from((index + pass) % 23).unwrap() - 11)
                    })
                    .collect::<Vec<_>>();
                assert_eq!(
                    row[1].extreme,
                    numbers.iter().min().map(|&n| if n < 0 {
                        Cell::NegativeInteger(n)
                    } else {
                        Cell::NonnegativeInteger(n.cast_unsigned())
                    })
                );
                assert_eq!(
                    row[2].extreme,
                    numbers.iter().max().map(|&n| if n < 0 {
                        Cell::NegativeInteger(n)
                    } else {
                        Cell::NonnegativeInteger(n.cast_unsigned())
                    })
                );
                for state in row {
                    assert_eq!(state.count, numbers.len() as u64);
                }
            }
            let mut output = Vec::new();
            aggregate.finish(context, 7, &mut |array| {
                output.push(array);
                Ok(())
            })?;
            Ok(output)
        })
        .unwrap();
    assert!(memory.snapshot().reserved_bytes > 0);
    drop(session);
    let copied = output.clone();
    drop(output);
    assert_eq!(copied.iter().map(ArrayRef::len).sum::<usize>(), 4);
    let receiver = ResidentVortexSession::new(2 << 20, 1).unwrap();
    receiver
        .with_native_execution_context(&CancellationToken::default(), |context| {
            for array in &copied {
                for name in ["v3", "v4"] {
                    let column = super::super::logical_field_from_native_array(array, name)?;
                    let values = keys(&column, context)?;
                    for row in 0..column.len() {
                        assert!(matches!(values.raw_cell(row)?, Cell::NonnegativeInteger(_)));
                    }
                }
            }
            Ok(())
        })
        .unwrap();
    drop(copied);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

#[test]
fn integer_extrema_pair_adds_no_reservation_under_pressure_and_cancels_cleanly() {
    let session = ResidentVortexSession::new(1 << 20, 1).unwrap();
    let memory = session.memory().clone();
    session
        .with_native_execution_context(&CancellationToken::default(), |context| {
            let column = PrimitiveArray::from_iter(std::iter::repeat_n(1i64, 2053)).into_array();
            let binding = spec(column.dtype(), false, true);
            let values = keys(&column, context)?;
            let KeyColumn::Numeric(owner) = &values else {
                panic!("numeric owner expected")
            };
            let ordinals = ordinals(column.len(), 3, context)?;
            let mut state = states(3, binding.measures.len(), 0);
            let snapshot = memory.snapshot();
            let pressure = memory.reserve(snapshot.limit_bytes - snapshot.reserved_bytes)?;
            assert_eq!(
                accumulate(&binding, 1, &mut state, &ordinals, &column, owner, context)?,
                Some(4)
            );
            assert_eq!(memory.snapshot().reserved_bytes, snapshot.limit_bytes);
            drop(pressure);
            Ok(())
        })
        .unwrap();
    let token = CancellationToken::default();
    let cancelled = session.with_native_execution_context(&token, |context| {
        let column = PrimitiveArray::from_iter(std::iter::repeat_n(1i64, 2053)).into_array();
        let binding = spec(column.dtype(), false, true);
        let values = keys(&column, context)?;
        let KeyColumn::Numeric(owner) = &values else {
            panic!("numeric owner expected")
        };
        let ordinals = ordinals(column.len(), 3, context)?;
        let mut state = states(3, binding.measures.len(), 0);
        let expected = copy_states(&state);
        token.cancel();
        let failure =
            accumulate(&binding, 1, &mut state, &ordinals, &column, owner, context).unwrap_err();
        assert_states(&state, &expected);
        Err::<(), _>(failure)
    });
    assert!(cancelled.is_err());
    drop(session);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

fn compare_declined_aggregate(column: &ArrayRef) {
    let session = ResidentVortexSession::new(2 << 20, 1).unwrap();
    let memory = session.memory().clone();
    session
        .with_native_execution_context(&CancellationToken::default(), |context| {
            let mut binding = spec(column.dtype(), false, true);
            binding.measures.remove(0);
            binding.measures.pop();
            let mut aggregate = Aggregate::new(&binding, &memory)?;
            let values = keys(column, context)?;
            let ordinals = ordinals(column.len(), 1, context)?;
            let mut expected: Vec<_> = (0..3).map(|_| State::default()).collect();
            let expected_error =
                sequential(&binding, 0..3, &mut expected, &ordinals, &values, context)
                    .err()
                    .map(|error| error.to_string());
            let groups =
                PrimitiveArray::from_iter(std::iter::repeat_n(0i64, column.len())).into_array();
            #[cfg(debug_assertions)]
            let observation = super::super::super::native_relational_observation::Session::start();
            let actual_error = aggregate
                .consume(&batch(groups, column.clone()), context, 8)
                .err()
                .map(|error| error.to_string());
            assert_eq!(actual_error, expected_error);
            assert_states(&aggregate.states.values, &expected);
            #[cfg(debug_assertions)]
            assert_eq!(
                observation.finish().aggregate_integer_extrema_fused_batches,
                0
            );
            Ok(())
        })
        .unwrap();
    drop(session);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

#[test]
fn integer_extrema_pair_keeps_float_bits_and_nonfinite_error_order_in_original_path() {
    for last in [3.0_f64, f64::INFINITY, f64::NEG_INFINITY, f64::NAN] {
        compare_declined_aggregate(
            &PrimitiveArray::from_option_iter([
                Some(1.0),
                Some(-0.0),
                Some(0.0),
                None,
                Some(last),
                Some(-2.0),
                None,
                Some(1.0),
            ])
            .into_array(),
        );
    }
}

#[test]
fn integer_extrema_pair_keeps_decimal_and_date_extrema_in_original_path() {
    let decimal = DecimalArray::from_option_iter(
        [
            Some(123_i128),
            None,
            Some(-9),
            Some(0),
            None,
            Some(88),
            Some(-100),
            Some(20),
        ],
        DecimalDType::new(8, 2),
    )
    .into_array();
    let date = ExtensionArray::try_new(
        Date::new(TimeUnit::Days, Nullability::Nullable).erased(),
        PrimitiveArray::from_option_iter([
            Some(-1_i32),
            None,
            Some(20_000),
            Some(0),
            None,
            Some(12_000),
            Some(-50),
            Some(20),
        ])
        .into_array(),
    )
    .unwrap()
    .into_array();
    compare_declined_aggregate(&decimal);
    compare_declined_aggregate(&date);
}
