use super::*;
#[cfg(debug_assertions)]
use crate::local_primitives::native_relational_observation;
use crate::local_primitives::result_batch;
use vortex::array::{VortexSessionExecute as _, memory::HostMemorySessionExt as _};

#[test]
fn typed_extrema_share_only_the_completed_exact_policy_domain() {
    for policy in [FloatPolicy::Strict, FloatPolicy::Extended] {
        for prior in [false, true] {
            for reverse in [false, true] {
                let column = PrimitiveArray::from_option_iter([
                    Some(-0.0f64),
                    Some(0.0),
                    None,
                    Some(-0.0),
                    Some(0.0),
                    None,
                    Some(0.0),
                    Some(-0.0),
                ])
                .into_array();
                let mut functions = Vec::new();
                if prior {
                    functions.extend([Function::Sum, Function::Avg]);
                }
                functions.extend(if reverse {
                    [Function::Max, Function::Min]
                } else {
                    [Function::Min, Function::Max]
                });
                functions.push(Function::Count);
                let spec = binding(&column, &functions);
                let session =
                    ResidentVortexSession::new_with_float_policy(4 << 20, 1, policy).unwrap();
                let memory = session.memory().clone();
                session
                    .with_native_execution_context(&CancellationToken::default(), |context| {
                        let mut actual = Aggregate::new(&spec, &memory)?;
                        let mut expected = Aggregate::new(&spec, &memory)?;
                        expected.specialize = false;
                        let input = batch(&column, 1);
                        expected.consume(&input, context, 8)?;
                        #[cfg(debug_assertions)]
                        let observation = native_relational_observation::Session::start();
                        actual.consume(&input, context, 8)?;
                        #[cfg(debug_assertions)]
                        {
                            let counts = observation.finish();
                            assert_eq!(
                                counts.aggregate_float_extrema_fused_batches,
                                u64::from(prior)
                            );
                            assert_eq!(
                                counts.aggregate_float_extrema_saved_measure_rows,
                                if prior { 16 } else { 0 }
                            );
                            assert_eq!(counts.aggregate_numeric_state_reuses, u64::from(prior));
                        }
                        assert_aggregate(&actual, &expected);
                        let table = actual.compact.as_ref().unwrap().typed.as_ref().unwrap();
                        let index = if prior { 2 } else { 0 };
                        assert_eq!(
                            table.state(index, 0).extreme,
                            Some(Cell::Float((-0.0f64).to_bits()))
                        );
                        assert_eq!(
                            table.state(index + 1, 0).extreme,
                            Some(Cell::Float((-0.0f64).to_bits()))
                        );
                        assert_eq!(results(&actual, context)?, results(&expected, context)?);
                        Ok(())
                    })
                    .unwrap();
                drop(session);
                assert_eq!(memory.snapshot().reserved_bytes, 0);
            }
        }
    }
}

#[test]
fn typed_extrema_extended_proof_allows_nonfinite_values_without_claiming_finite() {
    let nan_bits = 0x7ff8_0000_0000_17ab;
    let column = PrimitiveArray::from_option_iter([
        Some(f64::from_bits(nan_bits)),
        Some(f64::from_bits(0xfff8_0000_0000_3217)),
        None,
        Some(f64::INFINITY),
        Some(f64::NEG_INFINITY),
        Some(-0.0),
        Some(0.0),
        Some(7.0),
    ])
    .into_array();
    let spec = binding(
        &column,
        &[
            Function::Sum,
            Function::Avg,
            Function::Min,
            Function::Max,
            Function::Count,
        ],
    );
    let session =
        ResidentVortexSession::new_with_float_policy(4 << 20, 1, FloatPolicy::Extended).unwrap();
    session
        .with_native_execution_context(&CancellationToken::default(), |context| {
            let mut actual = Aggregate::new(&spec, context.memory())?;
            let mut expected = Aggregate::new(&spec, context.memory())?;
            expected.specialize = false;
            let input = batch(&column, 1);
            expected.consume(&input, context, 8)?;
            #[cfg(debug_assertions)]
            let observation = native_relational_observation::Session::start();
            actual.consume(&input, context, 8)?;
            #[cfg(debug_assertions)]
            assert_eq!(
                observation.finish().aggregate_float_extrema_fused_batches,
                1
            );
            assert_aggregate(&actual, &expected);
            let table = actual.compact.as_ref().unwrap().typed.as_ref().unwrap();
            assert_eq!(
                table.state(2, 0).extreme,
                Some(Cell::Float(f64::NEG_INFINITY.to_bits()))
            );
            assert_eq!(table.state(3, 0).extreme, Some(Cell::Float(nan_bits)));
            assert_eq!(table.state(4, 0).count, 7);
            assert_eq!(results(&actual, context)?, results(&expected, context)?);
            Ok(())
        })
        .unwrap();
}

#[test]
fn typed_extrema_filter_and_validity_preparation_invalidate_prior_proof() {
    let session = ResidentVortexSession::new(4 << 20, 1).unwrap();
    for validity_only in [false, true] {
        let column = PrimitiveArray::from_iter([4.0f64, f64::INFINITY, 2.0, 1.0]).into_array();
        let mut spec = binding(
            &column,
            &[Function::Sum, Function::Min, Function::Max, Function::Count],
        );
        spec.measures[0].filter_column = Some("keep".into());
        if validity_only {
            spec.measures[0].function = Function::Count;
            spec.measures[0].dtype = DType::Primitive(PType::U64, Nullability::NonNullable);
        }
        session
            .with_native_execution_context(&CancellationToken::default(), |context| {
                let mut actual = Aggregate::new(&spec, context.memory())?;
                let mut expected = Aggregate::new(&spec, context.memory())?;
                expected.specialize = false;
                let input = batch(&column, 1);
                let reference = expected.consume(&input, context, 4).unwrap_err();
                #[cfg(debug_assertions)]
                let observation = native_relational_observation::Session::start();
                let error = actual.consume(&input, context, 4).unwrap_err();
                #[cfg(debug_assertions)]
                assert_eq!(
                    observation.finish().aggregate_float_extrema_fused_batches,
                    0
                );
                assert_eq!(error.to_string(), reference.to_string());
                assert_aggregate(&actual, &expected);
                let table = actual.compact.as_ref().unwrap().typed.as_ref().unwrap();
                assert_eq!(table.state(1, 0).count, 1);
                assert_eq!(table.state(2, 0).count, 0);
                Ok(())
            })
            .unwrap();
    }
}

#[test]
fn typed_extrema_keep_count_overflow_and_unprepared_validity_in_order() {
    let session = ResidentVortexSession::new(4 << 20, 1).unwrap();
    for encoded_mask in [false, true] {
        let validity = if encoded_mask {
            Validity::Array(
                ChunkedArray::try_new(
                    vec![
                        BoolArray::from_iter([true, false, true, true]).into_array(),
                        BoolArray::from_iter([true, true, false, true]).into_array(),
                    ],
                    DType::Bool(Nullability::NonNullable),
                )
                .unwrap()
                .into_array(),
            )
        } else {
            Validity::NonNullable
        };
        let column = PrimitiveArray::new(vec![3i64, -2, 1, 7, -9, 4, 5, 11], validity).into_array();
        let spec = binding(&column, &[Function::Min, Function::Max, Function::Count]);
        session
            .with_native_execution_context(&CancellationToken::default(), |context| {
                let mut actual = Aggregate::new(&spec, context.memory())?;
                let mut expected = Aggregate::new(&spec, context.memory())?;
                expected.specialize = false;
                let input = batch(&column, 1);
                actual.consume(&input, context, 8)?;
                expected.consume(&input, context, 8)?;
                let table = actual.compact.as_mut().unwrap().typed.as_mut().unwrap();
                let overflow_index = if encoded_mask { 2 } else { 0 };
                if encoded_mask {
                    table.counts[0] = u64::MAX - 2;
                } else {
                    table.signed_extrema[0].count = u64::MAX - 2;
                }
                expected.states.values[overflow_index].count = u64::MAX - 2;
                let reference = expected.consume(&input, context, 8).unwrap_err();
                let error = actual.consume(&input, context, 8).unwrap_err();
                assert_eq!(error.to_string(), reference.to_string());
                assert!(error.to_string().contains("aggregate count overflow"));
                assert_aggregate(&actual, &expected);
                Ok(())
            })
            .unwrap();
    }
}

#[test]
fn typed_initial_allocation_refusal_releases_optional_state_and_ordinary_state_can_fit() {
    let session = ResidentVortexSession::new(4 << 20, 1).unwrap();
    let memory = session.memory().clone();
    session
        .with_native_execution_context(&CancellationToken::default(), |context| {
            let column = PrimitiveArray::from_iter([3i64; 8]).into_array();
            let input = batch(&column, 1);
            let spec = binding(
                &column,
                &[
                    Function::Sum,
                    Function::Avg,
                    Function::Min,
                    Function::Max,
                    Function::Count,
                ],
            );
            let baseline = memory.snapshot().reserved_bytes;
            let ordinary_bytes = 8 * size_of::<State>() as u64;
            let denied = memory.reserve((4 << 20) - baseline - ordinary_bytes)?;
            assert!(Table::new(&spec, &input, 1, context)?.is_none());
            let mut ordinary = ReservedVec::new(&memory)?;
            ordinary.reserve(spec.measures.len())?;
            ordinary
                .values
                .resize_with(spec.measures.len(), State::default);
            assert_eq!(memory.snapshot().reserved_bytes, 4 << 20);
            drop((ordinary, denied));
            assert_eq!(memory.snapshot().reserved_bytes, baseline);
            assert!(Table::new(&spec, &input, 1, context)?.is_some());
            Ok(())
        })
        .unwrap();
    drop(session);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

#[test]
fn typed_grouped_state_cancellation_and_result_lifetime_release_each_owner() {
    let producer = ResidentVortexSession::new(4 << 20, 1).unwrap();
    let producer_memory = producer.memory().clone();
    let source = producer
        .with_native_execution_context(&CancellationToken::default(), |context| {
            result_batch::build_column(
                &DType::Primitive(PType::I16, Nullability::Nullable),
                64,
                &context.native_session().host_allocator(),
                |row| {
                    Ok(if row % 4 == 0 {
                        Value::Null
                    } else {
                        Value::Int(i64::try_from(row).unwrap())
                    })
                },
            )
        })
        .unwrap();
    let spec = binding(
        &source,
        &[
            Function::Sum,
            Function::Avg,
            Function::Min,
            Function::Max,
            Function::Count,
        ],
    );
    let input = batch(&source, 3);
    drop(source);
    let receiver = ResidentVortexSession::new(4 << 20, 1).unwrap();
    let receiver_memory = receiver.memory().clone();
    let mut aggregate = Aggregate::new(&spec, &receiver_memory).unwrap();
    let token = CancellationToken::default();
    token.cancel();
    assert!(
        receiver
            .with_native_execution_context(&token, |context| aggregate.consume(&input, context, 7))
            .is_err()
    );
    assert!(aggregate.compact.is_none());
    let mut outputs = Vec::new();
    receiver
        .with_native_execution_context(&CancellationToken::default(), |context| {
            aggregate.consume(&input, context, 7)?;
            aggregate.finish(context, 7, &mut |batch| {
                outputs.push(batch);
                Ok(())
            })
        })
        .unwrap();
    drop((aggregate, input, producer));
    assert_eq!(producer_memory.snapshot().reserved_bytes, 0);
    assert_eq!(outputs.iter().map(ArrayRef::len).sum::<usize>(), 3);
    let selected = outputs[0].slice(1..2).unwrap();
    drop((outputs, receiver));
    assert!(receiver_memory.snapshot().reserved_bytes > 0);
    let mut execution = vortex::array::array_session().create_execution_ctx();
    let value = logical_field_from_native_array(&selected, "a4").unwrap();
    assert_eq!(
        result_batch::scalar(&value, 0, &mut execution).unwrap(),
        Scalar::from(16u64)
    );
    drop((value, selected, execution));
    assert_eq!(receiver_memory.snapshot().reserved_bytes, 0);
}
