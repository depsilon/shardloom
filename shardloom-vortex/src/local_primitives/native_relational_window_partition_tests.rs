//! Stable grouping and partition-local ordering for resident analytic windows.

#[cfg(debug_assertions)]
use super::super::super::native_relational_observation;
use super::super::super::{
    logical_field_from_native_array,
    native_relational_expression::keys,
    native_relational_keys::{Cell, KeyColumn},
};
use super::super::{Group, OrderKey, Spec, Window};
use crate::{
    relational_query::{
        VortexRelationalNullOrder as NullOrder, VortexRelationalWindowFunction as Function,
    },
    resident_session::ResidentVortexSession,
};
use shardloom_core::Result;
use shardloom_exec::compute_pool::CancellationToken;
use vortex::array::{
    IntoArray as _,
    arrays::{PrimitiveArray, StructArray},
    dtype::{DType, FieldNames, Nullability, PType},
    validity::Validity,
};

fn integer(nullable: bool) -> DType {
    DType::Primitive(
        PType::I64,
        if nullable {
            Nullability::Nullable
        } else {
            Nullability::NonNullable
        },
    )
}

fn window_spec(
    partition_nullable: bool,
    order_nullable: bool,
    descending: bool,
    nulls: Option<NullOrder>,
) -> Spec {
    let partition_type = integer(partition_nullable);
    let order_type = integer(order_nullable);
    Spec {
        fields: vec![
            ("source".into(), integer(false)),
            ("g".into(), partition_type),
            ("ord".into(), order_type),
            ("rn".into(), integer(false)),
        ],
        columns: vec!["source".into(), "g".into(), "ord".into()],
        keys: vec!["g".into(), "ord".into()],
        parent_validity_keys: vec![],
        functions: vec![Function::RowNumber],
        groups: vec![Group {
            partition: vec![0],
            order: vec![OrderKey {
                key: 1,
                descending,
                nulls,
            }],
            functions: vec![0],
        }],
        frames: vec![None],
        fills: vec![None],
        ordering_keys: 2,
    }
}

fn output_key(
    batch: &vortex::array::ArrayRef,
    name: &str,
    context: &crate::resident_session::NativeExecutionContext<'_>,
) -> Result<KeyColumn> {
    let column = logical_field_from_native_array(batch, name)?;
    keys(&column, context)
}

fn nullable_input(
    source: &[i64],
    partitions: &[Option<i64>],
    order: &[Option<i64>],
) -> vortex::array::ArrayRef {
    StructArray::new(
        FieldNames::from(["source", "g", "ord"]),
        vec![
            PrimitiveArray::from_iter(source.iter().copied()).into_array(),
            PrimitiveArray::from_option_iter(partitions.iter().copied()).into_array(),
            PrimitiveArray::from_option_iter(order.iter().copied()).into_array(),
        ],
        source.len(),
        Validity::NonNullable,
    )
    .into_array()
}

#[test]
fn interleaved_partitions_keep_source_output_order_and_sort_each_partition_once() {
    for input_rows in [1, 7, 11, 33] {
        for output_rows in [1, 7, 11, 33] {
            let session = ResidentVortexSession::new(16 << 20, 1).unwrap();
            let memory = session.memory().clone();
            let spec = window_spec(false, false, false, None);
            session
                .with_native_execution_context(&CancellationToken::default(), |context| {
                    let mut window = Window::new(&spec, context.memory())?;
                    for start in (0..33).step_by(input_rows) {
                        let end = (start + input_rows).min(33);
                        let sources = (start..end)
                            .map(|row| i64::try_from(row).unwrap())
                            .collect::<Vec<_>>();
                        let partitions = (start..end)
                            .map(|row| i64::try_from((row % 3 + 2) % 3).unwrap())
                            .collect::<Vec<_>>();
                        let order = (start..end)
                            .map(|row| i64::try_from(row / 3).unwrap())
                            .collect::<Vec<_>>();
                        let input = StructArray::new(
                            FieldNames::from(["source", "g", "ord"]),
                            vec![
                                PrimitiveArray::from_iter(sources).into_array(),
                                PrimitiveArray::from_iter(partitions).into_array(),
                                PrimitiveArray::from_iter(order).into_array(),
                            ],
                            end - start,
                            Validity::NonNullable,
                        )
                        .into_array();
                        window.build(input, context)?;
                    }

                    #[cfg(debug_assertions)]
                    let observation = native_relational_observation::Session::start();
                    let mut output_rows_seen = 0usize;
                    window.finish(context, output_rows, &mut |batch| {
                        assert!(batch.len() <= output_rows);
                        let sources = output_key(&batch, "source", context)?;
                        let partitions = output_key(&batch, "g", context)?;
                        let order = output_key(&batch, "ord", context)?;
                        let row_numbers = output_key(&batch, "rn", context)?;
                        for local in 0..batch.len() {
                            let source_row = output_rows_seen + local;
                            assert_eq!(
                                sources.raw_cell(local)?,
                                Cell::NonnegativeInteger(u64::try_from(source_row).unwrap())
                            );
                            assert_eq!(
                                partitions.raw_cell(local)?,
                                Cell::NonnegativeInteger(
                                    u64::try_from((source_row % 3 + 2) % 3).unwrap()
                                )
                            );
                            assert_eq!(
                                order.raw_cell(local)?,
                                Cell::NonnegativeInteger(u64::try_from(source_row / 3).unwrap())
                            );
                            assert_eq!(
                                row_numbers.raw_cell(local)?,
                                Cell::NonnegativeInteger(
                                    u64::try_from(source_row / 3 + 1).unwrap()
                                )
                            );
                        }
                        output_rows_seen += batch.len();
                        Ok(())
                    })?;
                    assert_eq!(output_rows_seen, 33);
                    #[cfg(debug_assertions)]
                    {
                        let counts = observation.finish();
                        assert_eq!(counts.order_domain_checks, 1, "{counts:?}");
                        assert_eq!(counts.order_checks, 5, "{counts:?}");
                        assert_eq!(counts.ordered_input_sorts, 3, "{counts:?}");
                        assert_eq!(counts.ordered_input_rows, 33, "{counts:?}");
                        assert_eq!(counts.order_heap_sorts, 1, "{counts:?}");
                    }
                    Ok(())
                })
                .unwrap();
            drop(session);
            assert_eq!(memory.snapshot().reserved_bytes, 0);
        }
    }
}

#[test]
fn nullable_partitions_descending_nulls_last_and_duplicate_ties_match_reference() {
    let partitions = [
        Some(1),
        Some(1),
        Some(1),
        None,
        None,
        None,
        Some(1),
        Some(1),
        None,
    ];
    let order = [
        Some(2),
        Some(2),
        Some(5),
        Some(5),
        None,
        Some(5),
        None,
        Some(3),
        Some(2),
    ];
    let source = (0..i64::try_from(partitions.len()).unwrap()).collect::<Vec<_>>();

    // Independent decoded reference: within a partition, nonnull descending
    // values precede nulls, and source ordinal breaks ties stably.
    let expected = (0..partitions.len())
        .map(|row| {
            let preceding = (0..partitions.len())
                .filter(|&candidate| {
                    if partitions[candidate] != partitions[row] {
                        return false;
                    }
                    match (order[candidate], order[row]) {
                        (Some(left), Some(right)) if left != right => left > right,
                        (Some(_), None) => true,
                        (None, Some(_)) => false,
                        _ => candidate < row,
                    }
                })
                .count();
            preceding + 1
        })
        .collect::<Vec<_>>();

    let session = ResidentVortexSession::new(16 << 20, 1).unwrap();
    let memory = session.memory().clone();
    let spec = window_spec(true, true, true, Some(NullOrder::Last));
    session
        .with_native_execution_context(&CancellationToken::default(), |context| {
            let mut window = Window::new(&spec, context.memory())?;
            window.build(nullable_input(&source, &partitions, &order), context)?;
            let mut output_rows_seen = 0usize;
            window.finish(context, 3, &mut |batch| {
                let sources = output_key(&batch, "source", context)?;
                let row_numbers = output_key(&batch, "rn", context)?;
                for local in 0..batch.len() {
                    let row = output_rows_seen + local;
                    assert_eq!(
                        sources.raw_cell(local)?,
                        Cell::NonnegativeInteger(u64::try_from(row).unwrap())
                    );
                    assert_eq!(
                        row_numbers.raw_cell(local)?,
                        Cell::NonnegativeInteger(u64::try_from(expected[row]).unwrap())
                    );
                }
                output_rows_seen += batch.len();
                Ok(())
            })?;
            assert_eq!(output_rows_seen, source.len());
            Ok(())
        })
        .unwrap();
    drop(session);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

#[test]
fn missing_null_order_fails_before_comparison_and_releases_credits() {
    let source = [0_i64, 1, 2, 3];
    let partitions = [Some(0_i64), Some(0), Some(1), Some(1)];
    let order = [Some(0_i64), Some(1), Some(2), None];
    let session = ResidentVortexSession::new(16 << 20, 1).unwrap();
    let memory = session.memory().clone();
    let spec = window_spec(false, true, false, None);
    session
        .with_native_execution_context(&CancellationToken::default(), |context| {
            let mut window = Window::new(&spec, context.memory())?;
            window.build(nullable_input(&source, &partitions, &order), context)?;
            #[cfg(debug_assertions)]
            let observation = native_relational_observation::Session::start();
            let result = window.finish(context, 4, &mut |_| Ok(()));
            let error = result.expect_err("a null order value requires explicit null placement");
            assert!(
                error.to_string().contains(
                    "window ORDER BY requires explicit NULLS FIRST or NULLS LAST for null values"
                ),
                "{error}"
            );
            #[cfg(debug_assertions)]
            {
                let counts = observation.finish();
                assert_eq!(counts.order_domain_checks, 1, "{counts:?}");
                assert_eq!(counts.order_checks, 0, "{counts:?}");
                assert_eq!(counts.order_heap_sorts, 0, "{counts:?}");
            }
            drop(window);
            Ok(())
        })
        .unwrap();
    drop(session);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

#[test]
fn empty_and_singleton_partition_windows_deliver_exact_cardinality() {
    for row_count in [0_usize, 1] {
        let session = ResidentVortexSession::new(4 << 20, 1).unwrap();
        let memory = session.memory().clone();
        let spec = window_spec(false, false, false, None);
        session
            .with_native_execution_context(&CancellationToken::default(), |context| {
                let mut window = Window::new(&spec, context.memory())?;
                if row_count == 1 {
                    let input = StructArray::new(
                        FieldNames::from(["source", "g", "ord"]),
                        vec![
                            PrimitiveArray::from_iter([0_i64]).into_array(),
                            PrimitiveArray::from_iter([9_i64]).into_array(),
                            PrimitiveArray::from_iter([4_i64]).into_array(),
                        ],
                        1,
                        Validity::NonNullable,
                    )
                    .into_array();
                    window.build(input, context)?;
                }
                let mut output_rows_seen = 0;
                window.finish(context, 1, &mut |batch| {
                    let row_numbers = output_key(&batch, "rn", context)?;
                    for row in 0..batch.len() {
                        assert_eq!(row_numbers.raw_cell(row)?, Cell::NonnegativeInteger(1));
                        output_rows_seen += 1;
                    }
                    Ok(())
                })?;
                assert_eq!(output_rows_seen, row_count);
                drop(window);
                Ok(())
            })
            .unwrap();
        drop(session);
        assert_eq!(memory.snapshot().reserved_bytes, 0);
    }
}

#[test]
fn partition_membership_denial_preserves_original_ordinals_and_releases_temporary_credits() {
    use crate::local_primitives::native_capacity::ReservedVec;

    let session = ResidentVortexSession::new(4 << 20, 1).unwrap();
    let memory = session.memory().clone();
    let spec = window_spec(true, true, false, Some(NullOrder::First));
    session
        .with_native_execution_context(&CancellationToken::default(), |context| {
            let mut window = Window::new(&spec, context.memory())?;
            window.build(
                nullable_input(
                    &[0, 1, 2, 3],
                    &[Some(1), Some(0), Some(1), Some(0)],
                    &[Some(3), Some(2), Some(1), Some(0)],
                ),
                context,
            )?;
            let mut ordinals = ReservedVec::new(context.memory())?;
            for row in 0..4 {
                ordinals.push(row)?;
            }
            let before = memory.snapshot();
            let pressure = memory.reserve(before.limit_bytes - before.reserved_bytes)?;
            assert!(!super::order(&window, &spec.groups[0], &mut ordinals, context)?.is_admitted());
            assert_eq!(ordinals.values, [0, 1, 2, 3]);
            assert_eq!(memory.snapshot().reserved_bytes, before.limit_bytes);
            assert!(memory.snapshot().denied_reservations > before.denied_reservations);
            drop(pressure);
            assert_eq!(memory.snapshot().reserved_bytes, before.reserved_bytes);
            let ordered = super::order(&window, &spec.groups[0], &mut ordinals, context)?;
            assert!(ordered.is_admitted());
            assert_eq!(ordered.spans(), Some([2, 4].as_slice()));
            assert_eq!(ordinals.values, [3, 1, 2, 0]);
            assert!(memory.snapshot().reserved_bytes > before.reserved_bytes);
            drop(ordered);
            assert_eq!(memory.snapshot().reserved_bytes, before.reserved_bytes);
            Ok(())
        })
        .unwrap();
    drop(session);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

#[test]
fn retained_span_denial_leaves_original_ordinals_and_membership_credits_intact() {
    use crate::local_primitives::native_capacity::ReservedVec;

    let session = ResidentVortexSession::new(4 << 20, 1).unwrap();
    let memory = session.memory().clone();
    let spec = window_spec(true, true, false, Some(NullOrder::First));
    session
        .with_native_execution_context(&CancellationToken::default(), |context| {
            let mut window = Window::new(&spec, context.memory())?;
            window.build(
                nullable_input(
                    &[0, 1, 2, 3],
                    &[Some(1), Some(0), Some(1), Some(0)],
                    &[Some(3), Some(2), Some(1), Some(0)],
                ),
                context,
            )?;
            let mut ordinals = ReservedVec::new(context.memory())?;
            for row in 0..4 {
                ordinals.push(row)?;
            }
            let before = memory.snapshot();
            let membership = super::collect(&window, &spec.groups[0], &ordinals.values, context)?;
            let membership_bytes = memory.snapshot().reserved_bytes - before.reserved_bytes;
            drop(membership);
            assert_eq!(memory.snapshot().reserved_bytes, before.reserved_bytes);
            let pressure =
                memory.reserve(before.limit_bytes - before.reserved_bytes - membership_bytes)?;
            // Prove membership itself still fits; only the retained spans do not.
            let control = super::collect(&window, &spec.groups[0], &ordinals.values, context)?;
            assert_eq!(memory.snapshot().reserved_bytes, before.limit_bytes);
            assert_eq!(
                memory.snapshot().denied_reservations,
                before.denied_reservations
            );
            drop(control);
            let pressured = memory.snapshot().reserved_bytes;
            let order = super::order(&window, &spec.groups[0], &mut ordinals, context)?;
            assert!(!order.is_admitted());
            assert_eq!(ordinals.values, [0, 1, 2, 3]);
            assert_eq!(memory.snapshot().reserved_bytes, pressured);
            assert_eq!(
                memory.snapshot().denied_reservations,
                before.denied_reservations + 1
            );
            drop(pressure);
            assert_eq!(memory.snapshot().reserved_bytes, before.reserved_bytes);
            Ok(())
        })
        .unwrap();
    drop(session);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

#[test]
fn partition_membership_preserves_lexicographic_visitation_for_every_distinct_group() {
    use crate::local_primitives::native_capacity::ReservedVec;

    let session = ResidentVortexSession::new(4 << 20, 1).unwrap();
    let memory = session.memory().clone();
    let spec = window_spec(true, true, false, Some(NullOrder::First));
    session
        .with_native_execution_context(&CancellationToken::default(), |context| {
            let mut window = Window::new(&spec, context.memory())?;
            let source = (0..257_i64).collect::<Vec<_>>();
            let partitions = source.iter().map(|row| Some(256 - row)).collect::<Vec<_>>();
            let ordering = source.iter().map(|row| Some(*row)).collect::<Vec<_>>();
            window.build(nullable_input(&source, &partitions, &ordering), context)?;
            let mut ordinals = ReservedVec::new(context.memory())?;
            for row in 0..source.len() {
                ordinals.push(row)?;
            }
            let before = memory.snapshot().reserved_bytes;
            let ordered = super::order(&window, &spec.groups[0], &mut ordinals, context)?;
            assert!(ordered.is_admitted());
            assert_eq!(
                ordered.spans(),
                Some((1..=source.len()).collect::<Vec<_>>().as_slice())
            );
            assert_eq!(ordinals.values, (0..source.len()).rev().collect::<Vec<_>>());
            drop(ordered);
            assert_eq!(memory.snapshot().reserved_bytes, before);
            Ok(())
        })
        .unwrap();
    drop(session);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

#[test]
fn partition_membership_uses_normalized_zero_and_extended_nan_equality() {
    let session = ResidentVortexSession::new_with_float_policy(
        4 << 20,
        1,
        shardloom_core::FloatPolicy::Extended,
    )
    .unwrap();
    let memory = session.memory().clone();
    let mut spec = window_spec(false, false, false, None);
    spec.fields[1].1 = DType::Primitive(PType::F64, Nullability::NonNullable);
    let partitions = [
        -0.0,
        f64::from_bits(0x7ff8_0000_0000_0001),
        0.0,
        f64::from_bits(0x7ff8_0000_0000_0042),
        f64::INFINITY,
        f64::NEG_INFINITY,
    ];
    session
        .with_native_execution_context(&CancellationToken::default(), |context| {
            let input = StructArray::new(
                FieldNames::from(["source", "g", "ord"]),
                vec![
                    PrimitiveArray::from_iter(0..6_i64).into_array(),
                    PrimitiveArray::from_iter(partitions).into_array(),
                    PrimitiveArray::from_iter(0..6_i64).into_array(),
                ],
                6,
                Validity::NonNullable,
            )
            .into_array();
            let mut window = Window::new(&spec, context.memory())?;
            window.build(input, context)?;
            let expected = [1, 1, 2, 2, 1, 1];
            let mut seen = 0;
            window.finish(context, 2, &mut |batch| {
                let numbers = output_key(&batch, "rn", context)?;
                for row in 0..batch.len() {
                    assert_eq!(
                        numbers.raw_cell(row)?,
                        Cell::NonnegativeInteger(expected[seen])
                    );
                    seen += 1;
                }
                Ok(())
            })?;
            assert_eq!(seen, expected.len());
            Ok(())
        })
        .unwrap();
    drop(session);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

#[test]
fn globally_ordered_partitions_require_no_membership_reservations() {
    use crate::local_primitives::native_capacity::ReservedVec;
    let session = ResidentVortexSession::new(4 << 20, 1).unwrap();
    let memory = session.memory().clone();
    let spec = window_spec(true, true, false, Some(NullOrder::First));
    session
        .with_native_execution_context(&CancellationToken::default(), |context| {
            let mut window = Window::new(&spec, context.memory())?;
            window.build(
                nullable_input(
                    &[0, 1, 2, 3],
                    &[Some(0), Some(1), Some(2), Some(3)],
                    &[Some(3), Some(2), Some(1), Some(0)],
                ),
                context,
            )?;
            let mut ordinals = ReservedVec::new(context.memory())?;
            for row in 0..4 {
                ordinals.push(row)?;
            }
            let before = memory.snapshot();
            let pressure = memory.reserve(before.limit_bytes - before.reserved_bytes)?;
            let ordered = super::order(&window, &spec.groups[0], &mut ordinals, context)?;
            assert!(ordered.is_admitted());
            assert!(ordered.spans().is_none());
            assert_eq!(ordinals.values, [0, 1, 2, 3]);
            assert_eq!(
                memory.snapshot().denied_reservations,
                before.denied_reservations
            );
            drop(pressure);
            assert_eq!(memory.snapshot().reserved_bytes, before.reserved_bytes);
            Ok(())
        })
        .unwrap();
    drop(session);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}
