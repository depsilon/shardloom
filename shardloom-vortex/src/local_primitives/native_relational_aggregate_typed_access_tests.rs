//! Boundary and ownership controls for once-bound typed aggregate state.

use super::*;
#[cfg(debug_assertions)]
use crate::local_primitives::native_relational_observation;

#[test]
fn typed_state_bindings_follow_each_payload_width_through_page_growth() {
    for policy in [FloatPolicy::Strict, FloatPolicy::Extended] {
        for unsigned in [false, true] {
            for floating in [false, true] {
                if floating && unsigned {
                    continue;
                }
                let session =
                    ResidentVortexSession::new_with_float_policy(32 << 20, 1, policy).unwrap();
                let memory = session.memory().clone();
                session
                    .with_native_execution_context(&CancellationToken::default(), |context| {
                        let column = state_column(3, unsigned, floating);
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
                        let mut actual = Aggregate::new(&spec, &memory)?;
                        let mut expected = Aggregate::new(&spec, &memory)?;
                        expected.specialize = false;
                        for groups in [1, 8, 512, 513, 1024, 1025, 4097] {
                            let column = state_column(groups * 3, unsigned, floating);
                            let input = batch(&column, groups);
                            expected.consume(&input, context, input.len())?;
                            #[cfg(debug_assertions)]
                            let observation = native_relational_observation::Session::start();
                            actual.consume(&input, context, input.len())?;
                            #[cfg(debug_assertions)]
                            {
                                let observed = observation.finish();
                                // SUM and AVG retain separate slots, though AVG reuses
                                // completed SUM; paired extrema bind both their own
                                // two-slot payload and the one-slot COUNT payload.
                                let paged = 2 * u64::from(groups > 512) + u64::from(groups > 1024);
                                assert_eq!(observed.aggregate_typed_paged_state_binds, paged);
                                assert_eq!(
                                    observed.aggregate_typed_contiguous_state_binds,
                                    3 - paged
                                );
                                assert_eq!(
                                    observed.aggregate_typed_paired_count_proof_rows,
                                    (input.len() - input.len().div_ceil(7)) as u64
                                );
                                eprintln!("grouped_state_binding_work={}", serde_json::json!({
                                    "groups": groups, "policy": format!("{policy:?}"),
                                    "unsigned": unsigned, "floating": floating,
                                    "input_rows": input.len(),
                                    "contiguous_binds": observed.aggregate_typed_contiguous_state_binds,
                                    "paged_binds": observed.aggregate_typed_paged_state_binds,
                                    "count_proof_rows": observed.aggregate_typed_paired_count_proof_rows
                                }));
                            }
                            assert_aggregate(&actual, &expected);
                            assert_eq!(results(&actual, context)?, results(&expected, context)?);
                        }
                        Ok(())
                    })
                    .unwrap();
                drop(session);
                assert_eq!(memory.snapshot().reserved_bytes, 0);
            }
        }
    }
}

fn state_column(rows: usize, unsigned: bool, floating: bool) -> ArrayRef {
    if floating {
        PrimitiveArray::from_option_iter(
            (0..rows).map(|row| {
                (row % 7 != 0).then_some(f64::from(u32::try_from(row % 17).unwrap()) - 8.0)
            }),
        )
        .into_array()
    } else if unsigned {
        PrimitiveArray::from_option_iter(
            (0..rows).map(|row| (row % 7 != 0).then_some((row % 17) as u64)),
        )
        .into_array()
    } else {
        PrimitiveArray::from_option_iter(
            (0..rows).map(|row| (row % 7 != 0).then_some(i64::try_from(row % 17).unwrap() - 8)),
        )
        .into_array()
    }
}

#[test]
fn typed_pair_uses_last_safe_headroom_and_declines_unequal_or_unproven_counts() {
    for groups in [1, 512, 513, 1025] {
        for nullable in [false, true] {
            for reverse in [false, true] {
                // Last-safe admission, one past that conservative boundary,
                // unequal extrema counts, and unequal COUNT all differ.
                for variant in 0..4 {
                    let rows = groups * 8;
                    let column = PrimitiveArray::from_option_iter((0..rows).map(|row| {
                        (!(nullable && row >= groups && row % 3 == 0))
                            .then_some(i64::try_from(row % 17).unwrap() - 8)
                    }))
                    .into_array();
                    let spec = binding(
                        &column,
                        &if reverse {
                            [Function::Max, Function::Min, Function::Count]
                        } else {
                            [Function::Min, Function::Max, Function::Count]
                        },
                    );
                    let session = ResidentVortexSession::new(32 << 20, 1).unwrap();
                    let memory = session.memory().clone();
                    session
                        .with_native_execution_context(&CancellationToken::default(), |context| {
                            let input = batch(&column, groups);
                            let mut actual = Aggregate::new(&spec, &memory)?;
                            let mut expected = Aggregate::new(&spec, &memory)?;
                            expected.specialize = false;
                            actual.consume(&input, context, rows)?;
                            expected.consume(&input, context, rows)?;
                            let start = u64::MAX - rows as u64 + u64::from(variant == 1);
                            let table = actual.compact.as_mut().unwrap().typed.as_mut().unwrap();
                            for group in 0..groups {
                                table.signed_extrema[group * 2].count = start;
                                table.signed_extrema[group * 2 + 1].count =
                                    start - u64::from(variant == 2);
                                table.counts[group] = start - u64::from(variant == 3);
                                for index in 0..3 {
                                    expected.states.values[group * 3 + index].count =
                                        table.state(index, group).count;
                                }
                            }
                            let reference = expected.consume(&input, context, rows);
                            #[cfg(debug_assertions)]
                            let observation = native_relational_observation::Session::start();
                            let observed = actual.consume(&input, context, rows);
                            #[cfg(debug_assertions)]
                            {
                                let counts = observation.finish();
                                assert_eq!(
                                    counts.aggregate_integer_extrema_fused_batches,
                                    u64::from(variant == 0)
                                );
                                assert_eq!(
                                    counts.aggregate_typed_paired_count_proof_rows > 0,
                                    variant == 0
                                );
                                eprintln!("grouped_state_count_proof={}", serde_json::json!({
                                    "groups": groups, "nullable": nullable, "reverse": reverse,
                                    "variant": variant, "input_rows": rows,
                                    "admitted_batches": counts.aggregate_integer_extrema_fused_batches,
                                    "count_proof_rows": counts.aggregate_typed_paired_count_proof_rows,
                                    "error": observed.as_ref().err().map(ToString::to_string)
                                }));
                            }
                            assert_eq!(
                                observed.as_ref().err().map(ToString::to_string),
                                reference.as_ref().err().map(ToString::to_string)
                            );
                            assert_aggregate(&actual, &expected);
                            if groups == 1 && !nullable && variant == 0 {
                                let table =
                                    actual.compact.as_ref().unwrap().typed.as_ref().unwrap();
                                assert_eq!(table.state(0, 0).count, u64::MAX);
                                assert_eq!(table.state(1, 0).count, u64::MAX);
                                assert_eq!(table.state(2, 0).count, u64::MAX);
                            }
                            Ok(())
                        })
                        .unwrap();
                    drop(session);
                    assert_eq!(memory.snapshot().reserved_bytes, 0);
                }
            }
        }
    }
}

#[test]
fn typed_state_cancellation_after_allocation_preserves_state_and_refunds() {
    for groups in [8, 513, 1025] {
        let session = ResidentVortexSession::new(16 << 20, 1).unwrap();
        let memory = session.memory().clone();
        let column = state_column(groups * 4, false, false);
        let input = batch(&column, groups);
        let spec = binding(
            &column,
            &[Function::Sum, Function::Min, Function::Max, Function::Count],
        );
        let mut actual = Aggregate::new(&spec, &memory).unwrap();
        let mut expected = Aggregate::new(&spec, &memory).unwrap();
        expected.specialize = false;
        session
            .with_native_execution_context(&CancellationToken::default(), |context| {
                actual.consume(&input, context, input.len())?;
                expected.consume(&input, context, input.len())?;
                Ok(())
            })
            .unwrap();
        let before = memory.snapshot().reserved_bytes;
        let token = CancellationToken::default();
        token.cancel();
        assert!(
            session
                .with_native_execution_context(&token, |context| {
                    actual.consume(&input, context, input.len())
                })
                .is_err()
        );
        assert_aggregate(&actual, &expected);
        assert_eq!(memory.snapshot().reserved_bytes, before);
        drop((actual, expected, input, column, session));
        assert_eq!(memory.snapshot().reserved_bytes, 0);
    }
}

#[test]
fn typed_pair_binds_under_full_pool_pressure_without_allocating() {
    for groups in [8, 513, 1025] {
        let session = ResidentVortexSession::new(16 << 20, 1).unwrap();
        let memory = session.memory().clone();
        session
            .with_native_execution_context(&CancellationToken::default(), |context| {
                let rows = groups * 4;
                let column = PrimitiveArray::from_iter(
                    (0..rows).map(|row| i64::try_from(row % 17).unwrap() - 8),
                )
                .into_array();
                let input = batch(&column, groups);
                let spec = binding(&column, &[Function::Min, Function::Max, Function::Count]);
                let mut table = Table::new(&spec, &input, groups, context)?.unwrap();
                let values = keys(&column, context)?;
                let KeyColumn::Numeric(owner) = &values else {
                    panic!("numeric owner expected")
                };
                let mut indices = ReservedVec::new(&memory)?;
                indices.reserve(rows)?;
                indices.values.extend((0..rows).map(|row| row % groups));
                let ordinals = GroupOrdinals::Grouped(indices);
                let baseline = memory.snapshot().reserved_bytes;
                let pressure = memory.reserve((16 << 20) - baseline)?;
                assert_eq!(
                    pair::accumulate(
                        &mut table, &spec, 0, &ordinals, &column, owner, None, context
                    )?,
                    Some(3)
                );
                for group in 0..groups {
                    let expected: Vec<_> = (0..4)
                        .map(|offset| i64::try_from((group + offset * groups) % 17).unwrap() - 8)
                        .collect();
                    assert_eq!(
                        table.signed_extrema[group * 2].value,
                        *expected.iter().min().unwrap()
                    );
                    assert_eq!(
                        table.signed_extrema[group * 2 + 1].value,
                        *expected.iter().max().unwrap()
                    );
                    assert_eq!(table.counts[group], 4);
                }
                assert_eq!(memory.snapshot().reserved_bytes, 16 << 20);
                drop(pressure);
                assert_eq!(memory.snapshot().reserved_bytes, baseline);
                Ok(())
            })
            .unwrap();
        drop(session);
        assert_eq!(memory.snapshot().reserved_bytes, 0);
    }
}
