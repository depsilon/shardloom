//! The shared typed consumer must preserve every ordered update, including
//! failure precedence and partial state, against the former scalar-cell path.

use super::*;
use crate::resident_session::ResidentVortexSession;
use shardloom_exec::compute_pool::CancellationToken;
use vortex::array::{
    arrays::{ChunkedArray, ConstantArray, DictArray, PrimitiveArray},
    dtype::Nullability,
    scalar::Scalar,
};

fn scalar_spec(function: Function) -> Spec {
    Spec {
        fields: vec![(
            "total".into(),
            DType::Primitive(PType::F64, Nullability::Nullable),
        )],
        groups: vec![],
        group_names: vec![],
        measures: vec![Measure {
            function,
            column: Some("n".into()),
            dtype: DType::Primitive(PType::F64, Nullability::Nullable),
            decimal_source: None,
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
    let bits = |sum: NumericAggregateSum| match sum {
        NumericAggregateSum::Ordered(value) => (0, value.to_bits(), 0),
        NumericAggregateSum::Integer { sum, correction } => {
            (1, sum.to_bits(), correction.to_bits())
        }
    };
    assert_eq!(bits(actual.sum), bits(expected.sum));
    assert_eq!(actual.extreme, expected.extreme);
}

fn compare(column: &ArrayRef, count: u64) {
    let session = ResidentVortexSession::new(4 << 20, 1).unwrap();
    let memory = session.memory().clone();
    session
        .with_native_execution_context(&CancellationToken::default(), |context| {
            let values = keys(column, context)?;
            let KeyColumn::Numeric(owner) = &values else {
                panic!("expected numeric owner")
            };
            let kernel = owner.bind_row_update(update_numeric);
            assert!(
                std::mem::size_of_val(&*kernel) + std::mem::size_of_val(&kernel)
                    <= usize::try_from(NUMERIC_KERNEL_BYTES).unwrap()
            );
            for function in [Function::Sum, Function::Avg] {
                let binding = scalar_spec(function);
                let mut actual = State {
                    count,
                    ..State::default()
                };
                let mut expected = State {
                    count,
                    ..State::default()
                };
                let mut error = None;
                for row in 0..column.len() {
                    let reference = values.raw_cell(row).and_then(|value| {
                        update(&mut expected, &binding.measures[0], Some(value), context)
                    });
                    let observed = kernel(&mut actual, row);
                    assert_eq!(
                        observed.as_ref().err().map(ToString::to_string),
                        reference.as_ref().err().map(ToString::to_string)
                    );
                    assert_same(&actual, &expected);
                    if let Err(failure) = reference {
                        error = Some(failure.to_string());
                        break;
                    }
                }
                let mut aggregate = Aggregate::new(&binding, context.memory())?;
                aggregate.states.values[0].count = count;
                let observed = aggregate.consume(&batch(column.clone()), context, 2);
                assert_eq!(observed.err().map(|failure| failure.to_string()), error);
                assert_same(&aggregate.states.values[0], &expected);
            }
            Ok(())
        })
        .unwrap();
    drop(session);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

#[test]
fn composed_numeric_consumer_matches_every_physical_type_and_validity_shape() {
    macro_rules! check {
        ($type:ty) => {
            check!($type, 1)
        };
        ($type:ty, $one:expr) => {{
            let direct = PrimitiveArray::from_iter([<$type>::MIN, $one, <$type>::MAX]).into_array();
            compare(&direct, 0);
            let nullable = PrimitiveArray::from_option_iter([
                Some(<$type>::MAX),
                None,
                Some(<$type>::MIN),
                Some($one),
                None,
            ])
            .into_array();
            compare(&nullable, 0);
            compare(&nullable.slice(1..4).unwrap(), 0);
            compare(&nullable.slice(2..2).unwrap(), 0);
            compare(
                &PrimitiveArray::from_option_iter([None::<$type>; 3]).into_array(),
                0,
            );
        }};
    }
    check!(u8);
    check!(u16);
    check!(u32);
    check!(u64);
    check!(i8);
    check!(i16);
    check!(i32);
    check!(i64);
    check!(f32, 1.0);
    check!(f64, 1.0);
}

#[test]
fn composed_numeric_consumer_preserves_dictionary_constant_and_chunked_inputs() {
    let values = PrimitiveArray::from_option_iter([Some(7i64), None, Some(i64::MAX)]).into_array();
    let dictionary = DictArray::try_new(
        PrimitiveArray::from_iter([2u8, 1, 0, 2, 0]).into_array(),
        values.clone(),
    )
    .unwrap()
    .into_array();
    compare(&dictionary, 0);
    compare(&dictionary.slice(1..4).unwrap(), 0);
    let chunks = ChunkedArray::try_new(
        [values.slice(0..0).unwrap(), values.clone(), values.clone()],
        values.dtype().clone(),
    )
    .unwrap()
    .into_array();
    compare(&chunks, 0);
    compare(&ConstantArray::new(Scalar::from(-7i64), 9).into_array(), 0);
    compare(
        &ConstantArray::new(Scalar::null(values.dtype().clone()), 9).into_array(),
        0,
    );
}

#[test]
fn composed_numeric_consumer_preserves_ordered_float_bits_and_failure_precedence() {
    for values in [
        vec![Some(1e16), Some(-1e16), Some(1.0)],
        vec![Some(1e16), Some(1.0), None, Some(-1e16)],
        vec![Some(-0.0), None, Some(0.0), Some(-0.0)],
        vec![Some(f64::MAX), Some(f64::MAX)],
        vec![Some(1.0), Some(f64::INFINITY)],
        vec![Some(f64::NEG_INFINITY)],
        vec![Some(f64::from_bits(0x7ff8_0000_0000_0042))],
    ] {
        compare(&PrimitiveArray::from_option_iter(values).into_array(), 0);
    }
    for value in [None, Some(1.0), Some(f64::INFINITY), Some(f64::NAN)] {
        compare(
            &PrimitiveArray::from_option_iter([value]).into_array(),
            u64::MAX,
        );
    }
    compare(
        &PrimitiveArray::from_iter([i64::MAX, 1, -i64::MAX]).into_array(),
        0,
    );
}

#[test]
fn composed_numeric_consumer_preserves_measure_order_and_releases_prepared_views() {
    let session = ResidentVortexSession::new(1 << 20, 1).unwrap();
    let memory = session.memory().clone();
    session
        .with_native_execution_context(&CancellationToken::default(), |context| {
            let mut binding = scalar_spec(Function::Sum);
            binding
                .measures
                .push(scalar_spec(Function::Avg).measures.remove(0));
            let mut aggregate = Aggregate::new(&binding, context.memory())?;
            let retained = memory.snapshot().reserved_bytes;
            let array = batch(
                PrimitiveArray::from_option_iter([Some(1e16), None, Some(-1e16)]).into_array(),
            );
            #[cfg(debug_assertions)]
            let observation = super::super::native_relational_observation::Session::start();
            aggregate.consume(&array, context, 2)?;
            aggregate.consume(
                &batch(PrimitiveArray::from_option_iter([Some(1.0)]).into_array()),
                context,
                2,
            )?;
            #[cfg(debug_assertions)]
            {
                let counts = observation.finish();
                assert_eq!(counts.aggregate_numeric_bound_batches, 4);
                assert_eq!(counts.aggregate_numeric_bound_rows, 8);
                assert_eq!(counts.aggregate_value_preparations, 2);
                assert_eq!(counts.aggregate_row_updates, 0);
            }
            for state in &aggregate.states.values {
                assert_eq!(state.count, 3);
                assert_eq!(state.sum.total().to_bits(), 1.0f64.to_bits());
            }
            assert_eq!(memory.snapshot().reserved_bytes, retained);
            let error = aggregate
                .consume(
                    &batch(PrimitiveArray::from_iter([2.0, f64::INFINITY]).into_array()),
                    context,
                    2,
                )
                .unwrap_err();
            assert!(error.to_string().contains("nonfinite scalar"));
            assert_eq!(aggregate.states.values[0].count, 4);
            assert_eq!(aggregate.states.values[0].sum.total(), 3.0);
            assert_eq!(aggregate.states.values[1].count, 3);
            assert_eq!(aggregate.states.values[1].sum.total(), 1.0);
            assert_eq!(memory.snapshot().reserved_bytes, retained);
            Ok(())
        })
        .unwrap();
    drop(session);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

#[test]
fn composed_numeric_consumer_keeps_grouped_numeric_updates_in_the_existing_consumer() {
    let session = ResidentVortexSession::new(1 << 20, 1).unwrap();
    let memory = session.memory().clone();
    let numbers = PrimitiveArray::from_iter([2i64, 1, 2]).into_array();
    let mut binding = scalar_spec(Function::Sum);
    binding.groups = vec![("n".into(), numbers.dtype().clone())];
    binding.group_names = vec!["n".into()];
    binding
        .fields
        .insert(0, ("n".into(), numbers.dtype().clone()));
    session
        .with_native_execution_context(&CancellationToken::default(), |context| {
            let mut aggregate = Aggregate::new(&binding, context.memory())?;
            #[cfg(debug_assertions)]
            let observation = super::super::native_relational_observation::Session::start();
            aggregate.consume(&batch(numbers.clone()), context, 2)?;
            #[cfg(debug_assertions)]
            {
                let counts = observation.finish();
                assert_eq!(counts.aggregate_numeric_bound_batches, 0);
                assert_eq!(counts.aggregate_row_updates, 3);
            }
            assert_eq!(aggregate.states.values.len(), 2);
            assert_eq!(aggregate.states.values[0].count, 2);
            assert_eq!(aggregate.states.values[0].sum.total(), 4.0);
            assert_eq!(aggregate.states.values[1].count, 1);
            assert_eq!(aggregate.states.values[1].sum.total(), 1.0);
            Ok(())
        })
        .unwrap();
    drop(session);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

#[test]
fn composed_numeric_consumer_accounts_kernel_and_checks_cancelled_null_rows() {
    for cancelled in [false, true] {
        let session = ResidentVortexSession::new(1 << 20, 1).unwrap();
        let memory = session.memory().clone();
        let token = CancellationToken::default();
        let result = session.with_native_execution_context(&token, |context| {
            let array = PrimitiveArray::from_option_iter(std::iter::repeat_n(None::<i64>, 2049))
                .into_array();
            let values = keys(&array, context)?;
            let KeyColumn::Numeric(owner) = &values else {
                panic!("numeric owner")
            };
            let mut state = State::default();
            let occupied = if cancelled {
                token.cancel();
                None
            } else {
                Some(memory.reserve(
                    memory.snapshot().limit_bytes
                        - memory.snapshot().reserved_bytes
                        - NUMERIC_KERNEL_BYTES
                        + 1,
                )?)
            };
            let retained = memory.snapshot().reserved_bytes;
            let error = accumulate_numeric(&mut state, owner, context)
                .unwrap_err()
                .to_string();
            assert!(
                error.contains(if cancelled {
                    "cancel"
                } else {
                    "reservation denied"
                }),
                "{error}"
            );
            assert_same(&state, &State::default());
            assert_eq!(memory.snapshot().reserved_bytes, retained);
            drop(occupied);
            Ok(())
        });
        assert_eq!(result.is_err(), cancelled);
        drop(session);
        assert_eq!(memory.snapshot().reserved_bytes, 0);
    }
}
