use super::super::{Aggregate, Function, Measure, Spec, keys, update as scalar_update};
use super::*;
use crate::local_primitives::{
    NumericAggregateSum, native_capacity::ReservedVec, native_relational_keys::KeyColumn,
    vortex_error,
};
use crate::resident_session::ResidentVortexSession;
use shardloom_exec::compute_pool::CancellationToken;
use vortex::array::{
    ArrayRef, IntoArray as _,
    arrays::{ChunkedArray, ConstantArray, DictArray, PrimitiveArray, StructArray},
    dtype::{DType, FieldNames},
    scalar::Scalar,
    validity::Validity,
};

fn spec(function: Function, dtype: &DType) -> Spec {
    Spec {
        fields: vec![("extreme".into(), dtype.as_nullable())],
        groups: vec![],
        group_names: vec![],
        measures: vec![Measure {
            function,
            column: Some("n".into()),
            secondary_column: None,
            quantile_level: None,
            separator: None,
            filter_column: None,
            selection: None,
            dtype: dtype.as_nullable(),
            decimal_source: None,
            decimal_output: None,
            distinct_fields: vec![],
            distinct_names: vec![],
        }],
    }
}

fn batch(column: ArrayRef) -> ArrayRef {
    let rows = column.len();
    StructArray::try_new(
        FieldNames::from(["n"]),
        vec![column],
        rows,
        Validity::NonNullable,
    )
    .unwrap()
    .into_array()
}

fn assert_same(actual: &State, expected: &State) {
    assert_eq!(actual.count, expected.count);
    assert_eq!(actual.extreme, expected.extreme);
    let sum_bits = |sum: NumericAggregateSum| match sum {
        NumericAggregateSum::Ordered(value) => (0, value.to_bits(), 0),
        NumericAggregateSum::Integer { sum, correction } => {
            (1, sum.to_bits(), correction.to_bits())
        }
    };
    assert_eq!(sum_bits(actual.sum), sum_bits(expected.sum));
}

fn compare_column(column: &ArrayRef, count: u64) {
    let session = ResidentVortexSession::new(8 << 20, 1).unwrap();
    let memory = session.memory().clone();
    session
        .with_native_execution_context(&CancellationToken::default(), |context| {
            let keys = keys(column, context)?;
            let KeyColumn::Numeric(owner) = &keys else {
                panic!("numeric owner expected")
            };
            for function in [Function::Min, Function::Max] {
                let binding = spec(function, column.dtype());
                let measure = &binding.measures[0];
                let minimum = function == Function::Min;
                let mut actual = State {
                    count,
                    ..State::default()
                };
                let mut expected = State {
                    count,
                    ..State::default()
                };
                let error = owner
                    .visit_values(|row, value| {
                        let reference = keys.raw_cell(row).and_then(|value| {
                            scalar_update(&mut expected, measure, Some(value), context)
                        });
                        let observed = update(&mut actual, &value, minimum);
                        assert_eq!(
                            observed.as_ref().err().map(ToString::to_string),
                            reference.as_ref().err().map(ToString::to_string)
                        );
                        assert_same(&actual, &expected);
                        observed
                    })
                    .err()
                    .map(|error| error.to_string());
                let mut aggregate = Aggregate::new(&binding, &memory)?;
                aggregate.states.values[0].count = count;
                let observed = aggregate.consume(&batch(column.clone()), context, 7);
                assert_eq!(observed.err().map(|error| error.to_string()), error);
                assert_same(&aggregate.states.values[0], &expected);

                // Interleaved groups and an untouched neighboring measure make
                // the shared scalar/grouped addressing contract observable.
                let mut mapping = ReservedVec::new(&memory)?;
                mapping.reserve(column.len())?;
                mapping.values.extend((0..column.len()).map(|row| row % 3));
                let groups = GroupOrdinals::Grouped(mapping);
                let mut actual = std::array::from_fn::<_, 6, _>(|_| State {
                    count,
                    ..State::default()
                });
                let mut expected = std::array::from_fn::<_, 6, _>(|_| State {
                    count,
                    ..State::default()
                });
                let mut error = None;
                for (row, group) in groups.iter() {
                    if let Err(failure) = keys.raw_cell(row).and_then(|value| {
                        scalar_update(&mut expected[group * 2 + 1], measure, Some(value), context)
                    }) {
                        error = Some(failure.to_string());
                        break;
                    }
                }
                let reserved = memory.snapshot().reserved_bytes;
                let observed = accumulate(&mut actual, &groups, 2, 1, minimum, owner, context);
                assert_eq!(observed.err().map(|error| error.to_string()), error);
                for (actual, expected) in actual.iter().zip(&expected) {
                    assert_same(actual, expected);
                }
                assert_eq!(memory.snapshot().reserved_bytes, reserved);
            }
            Ok(())
        })
        .unwrap();
    drop(session);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

#[test]
fn numeric_extrema_match_all_widths_nulls_slices_empty_and_exact_limits() {
    macro_rules! check {
        ($ty:ty, $one:expr) => {{
            let array =
                PrimitiveArray::from_iter([<$ty>::MAX, $one, <$ty>::MIN, $one]).into_array();
            compare_column(&array, 0);
            let array = PrimitiveArray::from_option_iter([
                Some(<$ty>::MAX),
                None,
                Some(<$ty>::MIN),
                Some($one),
                None,
            ])
            .into_array();
            compare_column(&array, 0);
            compare_column(&array.slice(1..4).unwrap(), 0);
            compare_column(&array.slice(2..2).unwrap(), 0);
            compare_column(
                &PrimitiveArray::from_option_iter([None::<$ty>; 5]).into_array(),
                0,
            );
        }};
    }
    check!(u8, 1);
    check!(u16, 1);
    check!(u32, 1);
    check!(u64, 1);
    check!(i8, 1);
    check!(i16, 1);
    check!(i32, 1);
    check!(i64, 1);
    check!(f32, 1.0);
    check!(f64, 1.0);
}

#[test]
fn numeric_extrema_keep_encoded_inputs_and_first_equal_zero_bits() {
    let values =
        PrimitiveArray::from_option_iter([Some(u64::MAX), None, Some(1), Some(0)]).into_array();
    compare_column(
        &DictArray::try_new(
            PrimitiveArray::from_iter([2u8, 0, 1, 3, 0]).into_array(),
            values.clone(),
        )
        .unwrap()
        .into_array(),
        0,
    );
    compare_column(
        &ChunkedArray::try_new(
            [values.slice(0..0).unwrap(), values.clone(), values.clone()],
            values.dtype().clone(),
        )
        .unwrap()
        .into_array(),
        0,
    );
    compare_column(
        &ConstantArray::new(Scalar::from(i64::MIN), 9).into_array(),
        0,
    );
    compare_column(
        &ConstantArray::new(Scalar::null(values.dtype().clone()), 9).into_array(),
        0,
    );
    for first in [-0.0_f64, 0.0] {
        compare_column(
            &PrimitiveArray::from_option_iter([Some(first), None, Some(-first)]).into_array(),
            0,
        );
    }
    compare_column(
        &PrimitiveArray::from_iter([
            f64::from_bits(1),
            -f64::from_bits(1),
            f64::MIN_POSITIVE,
            -f64::MIN_POSITIVE,
        ])
        .into_array(),
        0,
    );
}

#[test]
fn numeric_extrema_preserve_validation_overflow_precedence_and_partial_state() {
    for value in [
        f64::INFINITY,
        f64::NEG_INFINITY,
        f64::from_bits(0x7ff8_0000_0000_0042),
    ] {
        compare_column(
            &PrimitiveArray::from_option_iter([Some(7.0), None, Some(value), Some(-1.0)])
                .into_array(),
            0,
        );
        compare_column(&PrimitiveArray::from_iter([value]).into_array(), u64::MAX);
    }
    for count in [u64::MAX, u64::MAX - 1] {
        compare_column(
            &PrimitiveArray::from_option_iter([None, Some(7i64), Some(-1)]).into_array(),
            count,
        );
    }
}

#[test]
fn numeric_extrema_keep_exact_cross_batch_mixed_numeric_comparisons() {
    let session = ResidentVortexSession::new(1 << 20, 1).unwrap();
    session
        .with_native_execution_context(&CancellationToken::default(), |context| {
            for function in [Function::Min, Function::Max] {
                let binding = spec(function, PrimitiveArray::from_iter([0i64]).dtype());
                let mut actual = State::default();
                let mut expected = State::default();
                for array in [
                    PrimitiveArray::from_iter([u64::MAX, 9_007_199_254_740_993]).into_array(),
                    PrimitiveArray::from_iter([9_007_199_254_740_992.0, -0.0]).into_array(),
                    PrimitiveArray::from_iter([i64::MIN, 0]).into_array(),
                    PrimitiveArray::from_iter([-9_223_372_036_854_775_808.0, f64::MAX])
                        .into_array(),
                ] {
                    let keys = keys(&array, context)?;
                    let KeyColumn::Numeric(owner) = &keys else {
                        unreachable!()
                    };
                    owner.visit_values(|row, value| {
                        scalar_update(
                            &mut expected,
                            &binding.measures[0],
                            Some(keys.raw_cell(row)?),
                            context,
                        )?;
                        update(&mut actual, &value, function == Function::Min)?;
                        assert_same(&actual, &expected);
                        Ok(())
                    })?;
                }
            }
            Ok(())
        })
        .unwrap();
    assert_eq!(session.memory().snapshot().reserved_bytes, 0);
}

#[test]
fn numeric_extrema_preserve_filtered_domain_and_activate_shared_consumer() {
    let session = ResidentVortexSession::new(1 << 20, 1).unwrap();
    session
        .with_native_execution_context(&CancellationToken::default(), |context| {
            let numbers =
                PrimitiveArray::from_iter([f64::INFINITY, 5.0, -3.0, f64::NAN]).into_array();
            let mut binding = spec(Function::Min, numbers.dtype());
            binding.measures[0].filter_column = Some("keep".into());
            let array = StructArray::try_new(
                FieldNames::from(["n", "keep"]),
                vec![
                    numbers,
                    vortex::array::arrays::BoolArray::from_iter([false, true, true, false])
                        .into_array(),
                ],
                4,
                Validity::NonNullable,
            )
            .map_err(vortex_error)?
            .into_array();
            let mut aggregate = Aggregate::new(&binding, context.memory())?;
            #[cfg(debug_assertions)]
            let observation = super::super::super::native_relational_observation::Session::start();
            aggregate.consume(&array, context, 2)?;
            assert_eq!(aggregate.states.values[0].count, 2);
            assert_eq!(
                aggregate.states.values[0].extreme,
                Some(Cell::Float((-3.0_f64).to_bits()))
            );
            #[cfg(debug_assertions)]
            {
                let counts = observation.finish();
                assert_eq!(counts.aggregate_extrema_bound_batches, 1);
                assert_eq!(counts.aggregate_extrema_bound_rows, 2);
                assert_eq!(counts.aggregate_row_updates, 0);
            }
            Ok(())
        })
        .unwrap();
    assert_eq!(session.memory().snapshot().reserved_bytes, 0);
}

#[test]
fn numeric_extrema_allocate_no_kernel_and_check_cancellation_on_null_rows() {
    for cancelled in [false, true] {
        let session = ResidentVortexSession::new(1 << 20, 1).unwrap();
        let token = CancellationToken::default();
        let result = session.with_native_execution_context(&token, |context| {
            let array = PrimitiveArray::from_option_iter(std::iter::repeat_n(None::<u64>, 2049))
                .into_array();
            let keys = keys(&array, context)?;
            let KeyColumn::Numeric(owner) = keys else {
                unreachable!()
            };
            let mut states = [State::default()];
            let memory = context.memory();
            let occupied =
                memory.reserve(memory.snapshot().limit_bytes - memory.snapshot().reserved_bytes)?;
            if cancelled {
                token.cancel();
            }
            let result = accumulate(
                &mut states,
                &GroupOrdinals::Scalar(array.len()),
                1,
                0,
                true,
                &owner,
                context,
            );
            assert_eq!(result.is_err(), cancelled);
            if let Err(error) = result {
                assert!(error.to_string().contains("cancel"));
            }
            assert_same(&states[0], &State::default());
            drop(occupied);
            Ok(())
        });
        assert_eq!(result.is_err(), cancelled);
        drop(session);
    }
}
