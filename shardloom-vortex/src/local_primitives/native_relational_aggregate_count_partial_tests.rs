use super::super::{Cell, State, compact_tests};
use super::*;
use crate::resident_session::ResidentVortexSession;
use shardloom_exec::compute_pool::CancellationToken;
use std::collections::BTreeMap;
use vortex::array::{
    arrays::{BoolArray, ChunkedArray, ConstantArray, DictArray},
    scalar::Scalar,
};

fn binding(dtype: &DType, argument: bool) -> Spec {
    let groups = vec![("g".into(), dtype.clone())];
    Spec {
        fields: vec![
            groups[0].clone(),
            (
                "n".into(),
                DType::Primitive(PType::U64, Nullability::NonNullable),
            ),
        ],
        groups,
        group_names: vec!["g".into()],
        measures: vec![compact_tests::measure(
            Function::Count,
            argument.then_some("v"),
        )],
    }
}

fn input(key: ArrayRef) -> ArrayRef {
    let rows = key.len();
    StructArray::try_new(
        FieldNames::from(["g", "v"]),
        vec![
            key,
            PrimitiveArray::from_iter((0..rows).map(|_| 11_i64)).into_array(),
        ],
        rows,
        Validity::NonNullable,
    )
    .unwrap()
    .into_array()
}

fn values(
    aggregate: &Aggregate<'_>,
    context: &NativeExecutionContext<'_>,
) -> Result<Vec<(i128, u64)>> {
    let mut result = Vec::new();
    aggregate.finish(context, 1024, &mut |array| {
        let group = keys(&logical_field_from_native_array(&array, "g")?, context)?;
        let count = keys(&logical_field_from_native_array(&array, "n")?, context)?;
        for row in 0..array.len() {
            let key = match group.raw_cell(row)? {
                Cell::NegativeInteger(key) => i128::from(key),
                Cell::NonnegativeInteger(key) => i128::from(key),
                _ => panic!("integer group"),
            };
            let Cell::NonnegativeInteger(count) = count.raw_cell(row)? else {
                panic!("count")
            };
            result.push((key, count));
        }
        Ok(())
    })?;
    Ok(result)
}

#[test]
fn count_partial_preserves_every_integer_group_across_physical_inputs() {
    let signed = [i64::MAX, -1, i64::MIN, -1, i64::MAX, i64::MIN];
    let dtype = DType::Primitive(PType::I64, Nullability::NonNullable);
    let arrays = [
        PrimitiveArray::from_iter([i32::MAX, -1, i32::MIN, -1, i32::MAX, i32::MIN]).into_array(),
        PrimitiveArray::from_iter(signed).into_array(),
        PrimitiveArray::from_iter([u64::MAX, 0, 1, 0, u64::MAX, 1]).into_array(),
        DictArray::try_new(
            PrimitiveArray::from_iter([0_u8, 1, 2, 1, 0, 2]).into_array(),
            PrimitiveArray::from_iter([i64::MAX, -1, i64::MIN]).into_array(),
        )
        .unwrap()
        .into_array(),
        ChunkedArray::try_new(
            vec![
                PrimitiveArray::from_iter(signed[..3].iter().copied()).into_array(),
                PrimitiveArray::from_iter(signed[3..].iter().copied()).into_array(),
            ],
            dtype,
        )
        .unwrap()
        .into_array(),
        ConstantArray::new(Scalar::from(i64::MIN), 6).into_array(),
    ];
    for key in arrays {
        for argument in [false, true] {
            let session = ResidentVortexSession::new(16 << 20, 1).unwrap();
            let memory = session.memory().clone();
            let spec = binding(key.dtype(), argument);
            let array = input(key.clone());
            session
                .with_native_execution_context(&CancellationToken::default(), |context| {
                    let mut reference = Aggregate::new(&spec, context.memory())?;
                    let mut candidate = Aggregate::new(&spec, context.memory())?;
                    #[cfg(debug_assertions)]
                    let observation =
                        super::super::super::native_relational_observation::Session::start();
                    for _ in 0..3 {
                        assert!(try_consume(&mut candidate, &array, context, 3)?);
                    }
                    #[cfg(debug_assertions)]
                    {
                        let counts = observation.finish();
                        assert_eq!(counts.aggregate_count_partial_batches, 3);
                        assert_eq!(counts.aggregate_count_partial_input_rows, 18);
                        assert_eq!(counts.aggregate_count_contributions, 18);
                        assert!(counts.aggregate_count_updates < 18);
                        assert_eq!(
                            counts.aggregate_count_partial_constant_batches,
                            if key.is::<Constant>() { 3 } else { 0 }
                        );
                    }
                    for _ in 0..3 {
                        reference.consume(&array, context, 3)?;
                    }
                    let expected: BTreeMap<_, _> =
                        values(&reference, context)?.into_iter().collect();
                    let actual: BTreeMap<_, _> = values(&candidate, context)?.into_iter().collect();
                    assert_eq!(actual, expected);
                    Ok(())
                })
                .unwrap();
            drop(session);
            assert_eq!(memory.snapshot().reserved_bytes, 0);
        }
    }
}

#[test]
fn count_partial_checked_weights_reach_all_state_representations() {
    for mode in ["ordinary", "typed", "generic"] {
        let session = ResidentVortexSession::new(64 << 20, 1).unwrap();
        let memory = session.memory().clone();
        let spec = binding(
            &DType::Primitive(PType::I64, Nullability::NonNullable),
            true,
        );
        session
            .with_native_execution_context(&CancellationToken::default(), |context| {
                let mut aggregate = Aggregate::new(&spec, context.memory())?;
                aggregate.specialize = mode == "typed";
                let groups = if mode == "generic" {
                    (1 << 20) / size_of::<State>() + 1
                } else {
                    1
                };
                let initial = input(
                    PrimitiveArray::from_iter((0..groups).map(|n| i64::try_from(n).unwrap()))
                        .into_array(),
                );
                aggregate.consume(&initial, context, 1024)?;
                assert_eq!(aggregate.compact.is_some(), mode != "ordinary");
                if let Some(table) = &mut aggregate.compact {
                    table.add_count_weights(&[0], [u64::MAX - 2].into_iter(), context)?;
                } else {
                    aggregate.states.values[0].count = u64::MAX - 1;
                }
                let array = input(PrimitiveArray::from_iter([0_i64, 0]).into_array());
                let error = try_consume(&mut aggregate, &array, context, 1024)
                    .unwrap_err()
                    .to_string();
                assert!(
                    error.contains("aggregate count overflow"),
                    "{mode}: {error}"
                );
                let result = values(&aggregate, context)?;
                assert_eq!(result.len(), groups);
                assert_eq!(result[0], (0, u64::MAX - 1));
                assert!(result[1..].iter().all(|(_, count)| *count == 1));
                Ok(())
            })
            .unwrap();
        drop(session);
        assert_eq!(memory.snapshot().reserved_bytes, 0);
    }
}

#[test]
fn count_partial_declines_nullability_unique_tiny_and_empty_without_state_change() {
    let session = ResidentVortexSession::new(16 << 20, 1).unwrap();
    let memory = session.memory().clone();
    session
        .with_native_execution_context(&CancellationToken::default(), |context| {
            let ordinary = input(PrimitiveArray::from_iter([3_i64, 3, 1, 1]).into_array());
            let nullable_key =
                input(PrimitiveArray::from_option_iter([Some(3_i64), None]).into_array());
            let nullable_value = StructArray::try_new(
                FieldNames::from(["g", "v"]),
                vec![
                    PrimitiveArray::from_iter([3_i64, 3]).into_array(),
                    PrimitiveArray::from_option_iter([Some(1_i64), None]).into_array(),
                ],
                2,
                Validity::NonNullable,
            )
            .map_err(vortex_error)?
            .into_array();
            let nullable_parent = StructArray::try_new(
                FieldNames::from(["g", "v"]),
                vec![
                    PrimitiveArray::from_iter([3_i64, 3]).into_array(),
                    PrimitiveArray::from_iter([1_i64, 2]).into_array(),
                ],
                2,
                Validity::Array(BoolArray::from_iter([true, false]).into_array()),
            )
            .map_err(vortex_error)?
            .into_array();
            for array in [
                nullable_key,
                nullable_value,
                nullable_parent,
                input(PrimitiveArray::from_iter([3_i64, 1, 2]).into_array()),
                input(PrimitiveArray::from_iter([3_i64]).into_array()),
                input(PrimitiveArray::from_iter([] as [i64; 0]).into_array()),
            ] {
                let key = logical_field_from_native_array(&array, "g")?;
                let spec = binding(key.dtype(), true);
                let mut aggregate = Aggregate::new(&spec, context.memory())?;
                let before = context.memory().snapshot().reserved_bytes;
                assert!(!try_consume(&mut aggregate, &array, context, 1024)?);
                assert_eq!(aggregate.groups.as_ref().unwrap().rows(), 0);
                assert_eq!(context.memory().snapshot().reserved_bytes, before);
            }
            let spec = binding(
                &DType::Primitive(PType::I64, Nullability::NonNullable),
                true,
            );
            let mut aggregate = Aggregate::new(&spec, context.memory())?;
            aggregate.consume(&ordinary, context, 1024)?;
            assert_eq!(values(&aggregate, context)?, [(3, 2), (1, 2)]);
            Ok(())
        })
        .unwrap();
    drop(session);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

#[test]
fn count_partial_optional_capacity_denial_precedes_state_mutation() {
    let session = ResidentVortexSession::new(16 << 20, 1).unwrap();
    let memory = session.memory().clone();
    let spec = binding(
        &DType::Primitive(PType::I64, Nullability::NonNullable),
        true,
    );
    session
        .with_native_execution_context(&CancellationToken::default(), |context| {
            let array = input(PrimitiveArray::from_iter([3_i64, 3, 1, 1]).into_array());
            let mut aggregate = Aggregate::new(&spec, context.memory())?;
            let initial = context.memory().snapshot().reserved_bytes;
            let blocked = context
                .memory()
                .reserve(context.memory().snapshot().limit_bytes - initial - 1)?;
            assert!(!try_consume(&mut aggregate, &array, context, 1024)?);
            assert_eq!(aggregate.groups.as_ref().unwrap().rows(), 0);
            assert_eq!(
                context.memory().snapshot().reserved_bytes,
                initial + blocked.bytes()
            );
            assert_eq!(context.memory().snapshot().denied_reservations, 1);
            drop(blocked);
            aggregate.consume_reordered_count(&array, context, 1024)?;
            assert_eq!(values(&aggregate, context)?, [(1, 2), (3, 2)]);
            Ok(())
        })
        .unwrap();
    drop(session);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

#[test]
fn count_partial_reduced_buffer_denial_and_surviving_child_keep_exact_credits() {
    let session = ResidentVortexSession::new(16 << 20, 1).unwrap();
    let memory = session.memory().clone();
    session
        .with_native_execution_context(&CancellationToken::default(), |context| {
            let mut lease = context
                .memory()
                .reserve(numeric_count_partial::partial_bytes::<i64>(6)?)?;
            let partial = numeric_count_partial::count_numeric_values(
                &[4_i64, 4, 1, 1, 4, 1],
                &ChunkWorkerContext::Inline(context.cancellation().clone()),
                &mut lease,
            )?;
            assert_eq!(
                partial.reserved_bytes(),
                6 * 16,
                "truncate retains capacity"
            );
            let initial = context.memory().snapshot().reserved_bytes;
            let metadata = crate::native_payload_schema::names_bytes(1, |_| "g")?;
            let blocked = context
                .memory()
                .reserve(context.memory().snapshot().limit_bytes - initial - metadata - 16)?;
            assert!(
                reduced_keys("g", &partial, context)?.is_none(),
                "alignment capacity must be charged"
            );
            assert_eq!(
                context.memory().snapshot().reserved_bytes,
                initial + blocked.bytes()
            );
            assert_eq!(context.memory().snapshot().denied_reservations, 1);
            drop(blocked);
            let reduced = reduced_keys("g", &partial, context)?.unwrap();
            let retained = context.memory().snapshot().reserved_bytes;
            assert!(
                retained
                    >= initial + metadata + 16 + Alignment::DEFAULT_ALIGNMENT.as_usize() as u64
            );
            let child = logical_field_from_native_array(&reduced, "g")?;
            drop(reduced);
            assert_eq!(context.memory().snapshot().reserved_bytes, retained);
            drop(child);
            assert_eq!(context.memory().snapshot().reserved_bytes, initial);
            Ok(())
        })
        .unwrap();
    drop(session);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

#[test]
fn count_partial_cancellation_does_not_consume_input_or_hide_refunds() {
    let session = ResidentVortexSession::new(16 << 20, 1).unwrap();
    let memory = session.memory().clone();
    let token = CancellationToken::default();
    let spec = binding(
        &DType::Primitive(PType::I64, Nullability::NonNullable),
        true,
    );
    let error = session
        .with_native_execution_context(&token, |context| {
            let mut aggregate = Aggregate::new(&spec, context.memory())?;
            let initial = context.memory().snapshot().reserved_bytes;
            token.cancel();
            assert!(
                try_consume(
                    &mut aggregate,
                    &input(PrimitiveArray::from_iter([1_i64, 1]).into_array()),
                    context,
                    1024
                )
                .is_err()
            );
            assert_eq!(aggregate.groups.as_ref().unwrap().rows(), 0);
            assert_eq!(context.memory().snapshot().reserved_bytes, initial);
            Ok(())
        })
        .unwrap_err()
        .to_string();
    assert!(error.contains("execution cancelled"), "{error}");
    drop(session);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}
