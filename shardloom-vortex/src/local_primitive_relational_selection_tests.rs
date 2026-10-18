use super::super::selection::RowSelection;
use super::*;

fn selection_fields() -> Vec<(String, DType)> {
    vec![
        (
            "entity".to_owned(),
            DType::Primitive(PType::U64, Nullability::Nullable),
        ),
        (
            "amount".to_owned(),
            DType::Primitive(PType::U32, Nullability::NonNullable),
        ),
    ]
}

#[test]
fn native_selection_preserves_values_nulls_and_row_order() {
    let input = keyed(&[Some(11), None, Some(29), Some(41)], &[5, 6, 7, 8]);
    let fields = vec![
        (
            "entity".to_owned(),
            DType::Primitive(PType::U64, Nullability::Nullable),
        ),
        (
            "amount".to_owned(),
            DType::Primitive(PType::U32, Nullability::NonNullable),
        ),
    ];
    let session = ResidentVortexSession::new(8 << 20, 1).unwrap();
    for rows in [
        vec![],
        vec![0, 1, 2, 3],
        vec![1, 2],
        vec![3],
        vec![0, 2],
        vec![2, 2, 1],
        vec![3, 2, 1, 0],
    ] {
        session
            .with_native_execution_context(&CancellationToken::default(), |context| {
                let selected = select_batch(&input, &fields, &rows, context)?;
                assert_eq!(selected.dtype(), input.dtype());
                assert_eq!(selected.len(), rows.len());
                let mut execution = context.native_session().create_execution_ctx();
                for (output, input_row) in rows.iter().enumerate() {
                    assert_eq!(
                        selected.execute_scalar(output, &mut execution).unwrap(),
                        input.execute_scalar(*input_row, &mut execution).unwrap()
                    );
                }
                Ok(())
            })
            .unwrap();
        assert_eq!(session.snapshot().memory.reserved_bytes, 0);
    }
}

#[test]
fn consecutive_native_selection_needs_no_row_index_buffer() {
    let column = vortex::array::arrays::ConstantArray::new(37i64, 4096).into_array();
    let input = StructArray::from_fields(&[("value", column)])
        .unwrap()
        .into_array();
    let fields = vec![(
        "value".to_owned(),
        DType::Primitive(PType::I64, Nullability::NonNullable),
    )];
    let session = ResidentVortexSession::new(8 << 20, 1).unwrap();
    let rows = (17..2065).collect::<Vec<_>>();
    session
        .with_native_execution_context(&CancellationToken::default(), |context| {
            let selected = select_batch(&input, &fields, &rows, context)?;
            assert_eq!(selected.len(), rows.len());
            let value =
                crate::local_primitives::logical_field_from_native_array(&selected, "value")?;
            assert_eq!(value.encoding_id().as_ref(), "vortex.constant");
            assert_eq!(value.nchildren(), 0);
            Ok(())
        })
        .unwrap();
    assert!(session.snapshot().memory.peak_reserved_bytes < (rows.len() * 8) as u64);
    assert_eq!(session.snapshot().memory.reserved_bytes, 0);
}

#[test]
fn native_selection_rejects_cancelled_and_out_of_bounds_slices() {
    let input = keyed(&[Some(1), None], &[5, 6]);
    let fields = vec![(
        "entity".to_owned(),
        DType::Primitive(PType::U64, Nullability::Nullable),
    )];
    let session = ResidentVortexSession::new(8 << 20, 1).unwrap();
    let cancelled =
        session.with_native_execution_context(&CancellationToken::default(), |context| {
            assert!(select_batch(&input, &fields, &[2], context).is_err());
            assert!(select_batch(&input, &fields, &[1, 2], context).is_err());
            context.cancellation().cancel();
            assert!(select_batch(&input, &fields, &[0, 1], context).is_err());
            assert!(select_batch(&input, &fields, &[], context).is_err());
            Ok(())
        });
    assert!(cancelled.unwrap_err().to_string().contains("cancelled"));
    assert_eq!(session.snapshot().memory.reserved_bytes, 0);
}

#[test]
fn native_compact_selection_preserves_sliced_nullable_membership_and_batch_boundaries() {
    let input = keyed(
        &(0..137_u64)
            .map(|row| (row % 7 != 0).then_some(row * 29))
            .collect::<Vec<_>>(),
        &(0..137_u32).collect::<Vec<_>>(),
    )
    .slice(5..132)
    .unwrap();
    let fields = selection_fields();
    for shape in 0..6 {
        let keep = |row: usize| match shape {
            0 => true,
            1 => false,
            2 => (13..103).contains(&row),
            3 => row % 37 == 3,
            4 => row % 37 != 3,
            _ => row.is_multiple_of(2),
        };
        let expected = (0..input.len())
            .filter(|row| keep(*row))
            .collect::<Vec<_>>();
        let session = ResidentVortexSession::new(8 << 20, 1).unwrap();
        session
            .with_native_execution_context(&CancellationToken::default(), |context| {
                let rows = RowSelection::matching(0..input.len(), context, |row| Ok(keep(row)))?;
                let selected = rows.select(&input, &fields, context)?;
                assert_eq!(selected.len(), expected.len());
                assert_eq!(selected.dtype(), input.dtype());
                let mut execution = context.native_session().create_execution_ctx();
                for (out, row) in expected.iter().enumerate() {
                    assert_eq!(
                        selected.execute_scalar(out, &mut execution).unwrap(),
                        input.execute_scalar(*row, &mut execution).unwrap()
                    );
                }
                let mut delivered = 0;
                rows.for_each_batch(&input, &fields, 11, context, &mut |batch| {
                    assert_eq!(batch.len(), (expected.len() - delivered).min(11));
                    for row in 0..batch.len() {
                        assert_eq!(
                            batch.execute_scalar(row, &mut execution).unwrap(),
                            input
                                .execute_scalar(expected[delivered], &mut execution)
                                .unwrap()
                        );
                        delivered += 1;
                    }
                    Ok(())
                })?;
                assert_eq!(delivered, expected.len());
                let empty = RowSelection::matching(0..0, context, |_| panic!("empty input"))?;
                assert!(empty.is_empty());
                assert!(empty.select(&input, &fields, context)?.is_empty());
                Ok(())
            })
            .unwrap();
        assert_eq!(session.snapshot().memory.reserved_bytes, 0);
    }
}

#[test]
#[cfg(target_pointer_width = "64")]
fn native_compact_selection_keeps_wide_ranges_without_ordinal_allocations() {
    let start = usize::try_from(u64::from(u32::MAX) + 19).unwrap();
    let input = StructArray::from_fields(&[(
        "value",
        vortex::array::arrays::ConstantArray::new(71_i64, start + 17).into_array(),
    )])
    .unwrap()
    .into_array();
    let fields = vec![(
        "value".to_owned(),
        DType::Primitive(PType::I64, Nullability::NonNullable),
    )];
    let session = ResidentVortexSession::new(8 << 20, 1).unwrap();
    session
        .with_native_execution_context(&CancellationToken::default(), |context| {
            let before = context.memory().snapshot().reserved_bytes;
            let rows = RowSelection::matching(start..start + 17, context, |_| Ok(true))?;
            assert_eq!(context.memory().snapshot().reserved_bytes, before);
            let RowSelection::Range(range) = &rows else {
                panic!("contiguous membership expanded into indices");
            };
            assert_eq!(range, &(start..start + 17));
            let selected = rows.select(&input, &fields, context)?;
            assert_eq!(selected.len(), 17);
            let value =
                crate::local_primitives::logical_field_from_native_array(&selected, "value")?;
            assert_eq!(value.encoding_id().as_ref(), "vortex.constant");

            let mixed = RowSelection::matching(start..start + 17, context, |row| {
                Ok(row == start || row == start + 16)
            })?;
            let RowSelection::Indices(indices) = &mixed else {
                panic!("non-contiguous membership lost its ordinals");
            };
            assert_eq!(indices.values, [start, start + 16]);
            assert_eq!(mixed.select(&input, &fields, context)?.len(), 2);
            Ok(())
        })
        .unwrap();
    assert_eq!(session.snapshot().memory.reserved_bytes, 0);
}

#[test]
fn native_compact_selection_admits_before_gap_allocation_and_releases_on_failure() {
    let session = ResidentVortexSession::new(8 << 20, 1).unwrap();
    session
        .with_native_execution_context(&CancellationToken::default(), |context| {
            let baseline = context.memory().snapshot().reserved_bytes;
            let available = context.memory().snapshot().limit_bytes - baseline;
            let occupied = context.memory().reserve(available - 63)?;
            let held = context.memory().snapshot().reserved_bytes;
            let all = RowSelection::matching(0..8192, context, |_| Ok(true))?;
            assert!(!all.is_empty());
            let error = RowSelection::matching(0..4, context, |row| Ok(row % 2 == 0))
                .err()
                .expect("gap construction must reserve before allocation");
            assert!(error.to_string().contains("memory reservation denied"));
            assert_eq!(context.memory().snapshot().reserved_bytes, held);
            drop(occupied);
            let retry = RowSelection::matching(0..4, context, |row| Ok(row % 2 == 0))?;
            drop(retry);
            assert_eq!(context.memory().snapshot().reserved_bytes, baseline);
            let error = RowSelection::matching(0..8192, context, |row| {
                if row == 17 {
                    return Err(failed("selection predicate failure"));
                }
                Ok(row % 2 == 0)
            });
            assert!(error.is_err());
            assert_eq!(context.memory().snapshot().reserved_bytes, baseline);
            Ok(())
        })
        .unwrap();
    assert_eq!(session.snapshot().memory.reserved_bytes, 0);
}

#[test]
fn native_compact_selection_releases_cancellation_and_retains_last_index_owner() {
    let input = keyed(&[Some(3), None, Some(7), Some(11)], &[5, 6, 7, 8]);
    let fields = selection_fields();
    let session = ResidentVortexSession::new(8 << 20, 1).unwrap();
    let memory = session.memory().clone();
    let retained = session
        .with_native_execution_context(&CancellationToken::default(), |context| {
            RowSelection::matching(0..input.len(), context, |row| Ok(row == 1 || row == 3))?
                .select(&input, &fields, context)
        })
        .unwrap();
    assert!(memory.snapshot().reserved_bytes > 0);
    let held = memory.snapshot().reserved_bytes;
    let failed = session.with_native_execution_context(&CancellationToken::default(), |context| {
        RowSelection::matching(0..8192, context, |row| {
            if row == 17 {
                context.cancellation().cancel();
            }
            Ok(row % 2 == 0)
        })?;
        Ok(())
    });
    assert!(failed.unwrap_err().to_string().contains("cancelled"));
    assert_eq!(memory.snapshot().reserved_bytes, held);
    let clone = retained.clone();
    drop((retained, input, session));
    assert!(memory.snapshot().reserved_bytes > 0);
    let mut execution = VortexSession::default().create_execution_ctx();
    assert_eq!(clone.len(), 2);
    let entity =
        crate::local_primitives::logical_field_from_native_array(&clone, "entity").unwrap();
    assert!(entity.execute_scalar(0, &mut execution).unwrap().is_null());
    assert_eq!(
        entity.execute_scalar(1, &mut execution).unwrap(),
        vortex::scalar::Scalar::from(Some(11_u64))
    );
    drop((entity, clone, execution));
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

#[test]
fn native_compact_selection_keeps_ordered_duplicate_gathers_separate() {
    let input = keyed(
        &(0..20_u64).map(Some).collect::<Vec<_>>(),
        &(0..20_u32).collect::<Vec<_>>(),
    );
    let session = ResidentVortexSession::new(8 << 20, 1).unwrap();
    session
        .with_native_execution_context(&CancellationToken::default(), |context| {
            let selected = select_batch(&input, &selection_fields(), &[17, 3, 17, 8], context)?;
            let mut execution = context.native_session().create_execution_ctx();
            for (out, row) in [17, 3, 17, 8].into_iter().enumerate() {
                assert_eq!(
                    selected.execute_scalar(out, &mut execution).unwrap(),
                    input.execute_scalar(row, &mut execution).unwrap()
                );
            }
            Ok(())
        })
        .unwrap();
    assert_eq!(session.snapshot().memory.reserved_bytes, 0);
}

#[test]
fn native_compact_selection_composes_filters_projection_and_count_with_nullable_payloads() {
    use crate::relational_query::{
        VortexRelationalAggregate, VortexRelationalFilter, VortexRelationalProject,
    };
    use shardloom_core::{ComparisonOp, ExprId, Expression, ExpressionKind, ScalarValue};
    let expression = |kind| Expression::new(ExprId::new("selection").unwrap(), kind);
    let column = |name| expression(ExpressionKind::Column(ColumnRef::new(name).unwrap()));
    let fixture = Fixture::new(
        keyed(
            &(0..32_u64)
                .map(|row| (row % 7 != 0).then_some(row * 29))
                .collect::<Vec<_>>(),
            &(0..32_u32).collect::<Vec<_>>(),
        ),
        11,
    );
    let mut plan = fixture.scan();
    for (op, value) in [(ComparisonOp::GtEq, 7), (ComparisonOp::NotEq, 17)] {
        plan = VortexRelationalPlan::Filter(Box::new(VortexRelationalFilter {
            input: plan,
            predicate: expression(ExpressionKind::Compare {
                left: Box::new(column("amount")),
                op,
                right: Box::new(expression(ExpressionKind::Literal(ScalarValue::UInt64(
                    value,
                )))),
            }),
        }));
    }
    let expected = (7..32_u64)
        .filter(|row| *row != 17)
        .map(|row| serde_json::json!({"id": row, "payload": (row % 7 != 0).then_some(row * 29)}))
        .collect::<Vec<_>>();
    let projection = VortexRelationalPlan::Project(Box::new(VortexRelationalProject {
        input: plan.clone(),
        expressions: vec![
            ("id".into(), column("amount")),
            ("payload".into(), column("entity")),
        ],
    }));
    let prepared = prepare_relational(&projection, policy()).unwrap();
    let memory = prepared.session.memory().clone();
    let selected = prepared
        .collect_jsonl(&CancellationToken::default())
        .unwrap();
    assert_eq!(json_rows(&selected), expected);
    assert!(selected.execution.native_io_certificate.is_certified());
    drop((selected, prepared));
    assert_eq!(memory.snapshot().reserved_bytes, 0);

    let count = VortexRelationalPlan::Aggregate(Box::new(VortexRelationalAggregate {
        input: plan,
        group_by: vec![],
        measures: vec![crate::query_primitive::VortexSimpleAggregateMeasure::new(
            "count",
            None,
            "rows".into(),
        )],
    }));
    let prepared = prepare_relational(&count, policy()).unwrap();
    let memory = prepared.session.memory().clone();
    let counted = prepared
        .collect_jsonl(&CancellationToken::default())
        .unwrap();
    assert_eq!(
        json_rows(&counted),
        vec![serde_json::json!({"rows": expected.len()})]
    );
    assert!(counted.execution.native_io_certificate.is_certified());
    drop((counted, prepared));
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

#[test]
fn native_compact_selection_rejects_invalid_bounds_and_stops_failed_consumers() {
    let input = keyed(&[Some(1), None, Some(3), Some(4)], &[5, 6, 7, 8]);
    let fields = selection_fields();
    let session = ResidentVortexSession::new(8 << 20, 1).unwrap();
    session
        .with_native_execution_context(&CancellationToken::default(), |context| {
            let inverted = std::ops::Range { start: 2, end: 1 };
            assert!(RowSelection::matching(inverted, context, |_| Ok(true)).is_err());
            assert!(
                RowSelection::Range(1..5)
                    .select(&input, &fields, context)
                    .is_err()
            );
            for rows in [
                RowSelection::Range(0..4),
                RowSelection::matching(0..4, context, |row| Ok(row != 1))?,
            ] {
                assert!(
                    rows.for_each_batch(&input, &fields, 0, context, &mut |_| {
                        panic!("invalid batch size reached consumer")
                    })
                    .is_err()
                );
                let mut calls = 0;
                let result = rows.for_each_batch(&input, &fields, 1, context, &mut |_| {
                    calls += 1;
                    Err(failed("selection consumer failure"))
                });
                assert!(
                    result
                        .unwrap_err()
                        .to_string()
                        .contains("selection consumer failure")
                );
                assert_eq!(calls, 1);
            }
            Ok(())
        })
        .unwrap();
    assert_eq!(session.snapshot().memory.reserved_bytes, 0);
}
