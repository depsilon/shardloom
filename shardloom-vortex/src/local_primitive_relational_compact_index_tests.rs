use super::*;
use crate::{
    local_primitives::{logical_field_from_native_array, native_relational_batch},
    resident_session::ResidentVortexSession,
};
use shardloom_exec::compute_pool::CancellationToken;
use vortex::{
    VortexSessionDefault as _,
    array::{
        IntoArray as _, VortexSessionExecute as _,
        arrays::{ConstantArray, ListViewArray, PrimitiveArray, StructArray},
        validity::Validity,
    },
    session::VortexSession,
};

fn source(rows: usize) -> ArrayRef {
    StructArray::from_fields(&[(
        "n",
        PrimitiveArray::from_option_iter(
            (0..rows).map(|row| (!row.is_multiple_of(11)).then(|| u64::try_from(row).unwrap())),
        )
        .into_array(),
    )])
    .unwrap()
    .into_array()
}

fn fields(array: &ArrayRef) -> Vec<(String, DType)> {
    let fields = array.dtype().as_struct_fields();
    fields
        .names()
        .iter()
        .zip(fields.fields())
        .map(|(name, dtype)| (name.to_string(), dtype))
        .collect()
}

#[test]
fn compact_local_selection_width_edges_preserve_ordinals_and_nulls() {
    for extent in [0, 1, 65_535, 65_536, 65_537] {
        let input = source(extent);
        let session = ResidentVortexSession::new(8 << 20, 1).unwrap();
        session
            .with_native_execution_context(&CancellationToken::default(), |context| {
                let mut selection = LocalSelection::new(extent, context)?;
                assert_eq!(
                    matches!(selection.rows, Indices::Compact(_)),
                    extent <= 65_536
                );
                assert!(selection.push(extent).is_err());
                let rows = if extent == 0 {
                    vec![]
                } else {
                    vec![extent - 1, 0, extent / 2, extent - 1]
                };
                for &row in &rows {
                    selection.push(row)?;
                }
                let mut execution = context.native_session().create_execution_ctx();
                let mut position = 0;
                selection.for_each_batch(&input, &fields(&input), 2, context, &mut |batch| {
                    assert_eq!(batch.dtype(), input.dtype());
                    for row in 0..batch.len() {
                        assert_eq!(
                            batch.execute_scalar(row, &mut execution).unwrap(),
                            input
                                .execute_scalar(rows[position], &mut execution)
                                .unwrap(),
                        );
                        position += 1;
                    }
                    Ok(())
                })?;
                assert_eq!(position, rows.len());
                if extent == 0 {
                    let empty = select_compact_batch(&input, &fields(&input), &[], context)?;
                    assert_eq!(empty.dtype(), input.dtype());
                    assert_eq!(empty.len(), 0);
                }
                Ok(())
            })
            .unwrap();
        assert_eq!(session.snapshot().memory.reserved_bytes, 0);
    }
}

#[test]
fn compact_local_selection_preserves_contiguous_native_slices() {
    let input = StructArray::from_fields(&[("n", ConstantArray::new(37i64, 4096).into_array())])
        .unwrap()
        .into_array();
    let session = ResidentVortexSession::new(8 << 20, 1).unwrap();
    session
        .with_native_execution_context(&CancellationToken::default(), |context| {
            let mut selection = LocalSelection::new(input.len(), context)?;
            for row in 17..2065 {
                selection.push(row)?;
            }
            let before = context.memory().snapshot().reserved_bytes;
            selection.for_each_batch(&input, &fields(&input), 2048, context, &mut |batch| {
                assert_eq!(batch.len(), 2048);
                let column = logical_field_from_native_array(&batch, "n")?;
                assert_eq!(column.encoding_id().as_ref(), "vortex.constant");
                assert_eq!(column.nchildren(), 0);
                Ok(())
            })?;
            assert_eq!(context.memory().snapshot().reserved_bytes, before);
            Ok(())
        })
        .unwrap();
    assert_eq!(session.snapshot().memory.reserved_bytes, 0);
}

#[test]
fn compact_local_selection_reserves_before_allocation_and_retries_after_denial() {
    let session = ResidentVortexSession::new(8 << 20, 1).unwrap();
    session
        .with_native_execution_context(&CancellationToken::default(), |context| {
            let before = context.memory().snapshot().reserved_bytes;
            let occupied = context
                .memory()
                .reserve(context.memory().snapshot().limit_bytes - before)?;
            for extent in [8192, 65_537] {
                assert!(LocalSelection::new(extent, context).is_err());
                assert_eq!(
                    context.memory().snapshot().reserved_bytes,
                    before + occupied.bytes()
                );
            }
            drop(occupied);
            for extent in [8192, 65_537] {
                let mut selection = LocalSelection::new(extent, context)?;
                let width = if extent <= 65_536 {
                    2
                } else {
                    std::mem::size_of::<usize>()
                };
                assert_eq!(
                    context.memory().snapshot().reserved_bytes - before,
                    u64::try_from(extent * width).unwrap()
                );
                selection.push(extent - 1)?;
                assert!(selection.push(extent).is_err());
                drop(selection);
                assert_eq!(context.memory().snapshot().reserved_bytes, before);
            }
            Ok(())
        })
        .unwrap();
    assert_eq!(session.snapshot().memory.denied_reservations, 2);
    assert_eq!(session.snapshot().memory.reserved_bytes, 0);
}

#[test]
fn compact_local_selection_rejects_changed_extent_zero_batch_and_failed_consumer() {
    let input = source(4);
    let session = ResidentVortexSession::new(8 << 20, 1).unwrap();
    session
        .with_native_execution_context(&CancellationToken::default(), |context| {
            let mut selection = LocalSelection::new(4, context)?;
            selection.push(3)?;
            selection.push(1)?;
            let before = context.memory().snapshot().reserved_bytes;
            let mut calls = 0;
            let mut consumer = |_batch| {
                calls += 1;
                Err(failed("consumer rejected selection"))
            };
            assert!(
                selection
                    .for_each_batch(&input, &fields(&input), 0, context, &mut consumer)
                    .is_err()
            );
            assert!(
                selection
                    .for_each_batch(
                        &input.slice(0..3).unwrap(),
                        &fields(&input),
                        2,
                        context,
                        &mut consumer
                    )
                    .is_err()
            );
            assert!(
                selection
                    .for_each_batch(&input, &fields(&input), 2, context, &mut consumer)
                    .is_err()
            );
            assert_eq!(calls, 1);
            assert_eq!(context.memory().snapshot().reserved_bytes, before);
            let mut successful = 0;
            selection.for_each_batch(&input, &fields(&input), 2, context, &mut |batch| {
                successful += batch.len();
                Ok(())
            })?;
            assert_eq!(successful, 2);
            Ok(())
        })
        .unwrap();
    assert_eq!(session.snapshot().memory.reserved_bytes, 0);
}

#[test]
fn compact_local_selection_cancellation_releases_all_scratch() {
    let session = ResidentVortexSession::new(8 << 20, 1).unwrap();
    let input = source(4096);
    let result = session.with_native_execution_context(&CancellationToken::default(), |context| {
        let mut selection = LocalSelection::new(input.len(), context)?;
        for row in [4000, 0, 17] {
            selection.push(row)?;
        }
        context.cancellation().cancel();
        assert!(LocalSelection::new(4, context).is_err());
        assert!(
            selection
                .for_each_batch(&input, &fields(&input), 2, context, &mut |_| {
                    panic!("cancelled selection reached consumer")
                })
                .is_err()
        );
        Ok(())
    });
    assert!(result.unwrap_err().to_string().contains("cancelled"));
    assert_eq!(session.snapshot().memory.reserved_bytes, 0);
}

#[test]
fn compact_local_selection_output_retains_payload_and_indices_after_owner_drop() {
    let session = ResidentVortexSession::new(8 << 20, 1).unwrap();
    let memory = session.memory().clone();
    let output = session
        .with_native_execution_context(&CancellationToken::default(), |context| {
            let column = result_batch::build_column(
                &DType::Primitive(PType::I64, Nullability::Nullable),
                4,
                &context.native_session().allocator(),
                |row| {
                    Ok(match row {
                        0 => result_batch::Value::Int(11),
                        1 => result_batch::Value::Null,
                        2 => result_batch::Value::Int(29),
                        _ => result_batch::Value::Int(41),
                    })
                },
            )?;
            let input = StructArray::from_fields(&[("n", column)])
                .unwrap()
                .into_array();
            let mut selection = LocalSelection::new(input.len(), context)?;
            for row in [3, 1, 3, 0] {
                selection.push(row)?;
            }
            let mut output = None;
            selection.for_each_batch(&input, &fields(&input), 4, context, &mut |batch| {
                output = Some(batch);
                Ok(())
            })?;
            Ok(output.unwrap())
        })
        .unwrap();
    let retained = output.clone();
    drop((output, session));
    assert!(memory.snapshot().reserved_bytes > 0);
    let native = VortexSession::default();
    let mut execution = native.create_execution_ctx();
    let values = logical_field_from_native_array(&retained, "n").unwrap();
    for (row, expected) in [Some(41i64), None, Some(41), Some(11)]
        .into_iter()
        .enumerate()
    {
        let value = values.execute_scalar(row, &mut execution).unwrap();
        if let Some(expected) = expected {
            assert_eq!(value, expected.into());
        } else {
            assert!(value.is_null());
        }
    }
    drop((values, retained, execution, native));
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

#[test]
fn compact_local_selection_nested_sliced_bases_preserve_duplicates_and_validity() {
    let nested = ListViewArray::try_new(
        PrimitiveArray::from_option_iter([Some(9i64), None, Some(-4)]).into_array(),
        PrimitiveArray::from_iter([0u64, 2, 2, 2]).into_array(),
        PrimitiveArray::from_iter([2u64, 0, 0, 1]).into_array(),
        Validity::from_iter([true, true, false, true]),
    )
    .unwrap()
    .into_array();
    let source = StructArray::from_fields(&[("items", nested)])
        .unwrap()
        .into_array();
    let input = source.slice(1..4).unwrap();
    let session = ResidentVortexSession::new(8 << 20, 1).unwrap();
    let output = session
        .with_native_execution_context(&CancellationToken::default(), |context| {
            let mut selection = LocalSelection::new(input.len(), context)?;
            for row in [2, 0, 2, 1] {
                selection.push(row)?;
            }
            let mut output = None;
            selection.for_each_batch(&input, &fields(&input), 4, context, &mut |batch| {
                output = Some(batch);
                Ok(())
            })?;
            Ok(output.unwrap())
        })
        .unwrap();
    assert_eq!(output.dtype(), input.dtype());
    let native = VortexSession::default();
    let mut execution = native.create_execution_ctx();
    let expected = [3, 1, 3, 2].map(|row| source.execute_scalar(row, &mut execution).unwrap());
    drop((source, input, session));
    for (row, expected) in expected.into_iter().enumerate() {
        assert_eq!(
            output.execute_scalar(row, &mut execution).unwrap(),
            expected
        );
    }
}

#[cfg(target_pointer_width = "64")]
#[test]
fn compact_local_selection_leaves_wide_global_gathers_ordered_nullable_and_exact() {
    let large = usize::try_from(u64::from(u32::MAX) + 18).unwrap();
    let session = ResidentVortexSession::new(8 << 20, 1).unwrap();
    session
        .with_native_execution_context(&CancellationToken::default(), |context| {
            let mut table = native_relational_batch::Table::new(context.memory())?;
            for (value, count) in [(23i64, large), (71, 7)] {
                let input = StructArray::from_fields(&[(
                    "n",
                    ConstantArray::new(value, count).into_array(),
                )])
                .unwrap()
                .into_array();
                table.push(native_relational_batch::Batch::new(input, &[], context)?)?;
            }
            let positions = [
                Some(large + 2),
                None,
                Some(large - 1),
                Some(1),
                Some(large + 2),
            ];
            let indices =
                native_relational_batch::index_array(positions.len(), true, context, |row| {
                    Ok(positions[row])
                })?;
            assert_eq!(
                indices.dtype(),
                &DType::Primitive(PType::U64, Nullability::Nullable)
            );
            let mut execution = context.native_session().create_execution_ctx();
            assert_eq!(
                indices.execute_scalar(0, &mut execution).unwrap(),
                u64::try_from(large + 2).unwrap().into()
            );
            let output = table.gather(&positions, true, context)?.column(
                "n",
                &DType::Primitive(PType::I64, Nullability::Nullable),
                context,
            )?;
            for (row, expected) in [Some(71i64), None, Some(23), Some(23), Some(71)]
                .into_iter()
                .enumerate()
            {
                let value = output.execute_scalar(row, &mut execution).unwrap();
                if let Some(expected) = expected {
                    assert_eq!(value, expected.into());
                } else {
                    assert!(value.is_null());
                }
            }
            assert!(table.gather(&[Some(large + 7)], false, context).is_err());
            Ok(())
        })
        .unwrap();
    assert_eq!(session.snapshot().memory.reserved_bytes, 0);
}

#[test]
fn compact_local_selection_predicate_failure_drops_scan_scratch_and_allows_new_work() {
    use super::super::prepare_relational_with_schema_in_session;
    use crate::{
        relational_query::{VortexRelationalFilter, VortexRelationalPlan, VortexRelationalScan},
        resident_memory_source::{
            MemoryColumn, MemoryColumnValues, MemorySourceBounds, ResidentMemorySource,
        },
    };
    use shardloom_core::{
        ColumnRef, ComparisonOp, DatasetUri, ExprId, Expression, ExpressionKind, LogicalDType,
        ScalarValue,
    };
    let mut policy =
        super::super::super::VortexLocalPrimitiveExecutionPolicy::new_with_memory_gb(1, 1).unwrap();
    policy.resource_envelope.memory_budget_bytes = 32 << 20;
    let session =
        ResidentVortexSession::new(policy.resource_envelope.memory_budget_bytes, 1).unwrap();
    let memory = session.memory().clone();
    let uri = DatasetUri::new("memory://compact-predicate-failure").unwrap();
    let expr = |kind| Expression::new(ExprId::new("predicate").unwrap(), kind);
    let scan = VortexRelationalPlan::Scan(VortexRelationalScan {
        source_uri: uri.clone(),
        projection: shardloom_plan::ProjectionRequest::All,
        predicate: None,
    });
    let filter = VortexRelationalPlan::Filter(Box::new(VortexRelationalFilter {
        input: scan.clone(),
        predicate: expr(ExpressionKind::Compare {
            left: Box::new(expr(ExpressionKind::Cast {
                expr: Box::new(expr(ExpressionKind::Column(ColumnRef::new("n").unwrap()))),
                target_dtype: LogicalDType::Int64,
            })),
            op: ComparisonOp::Gt,
            right: Box::new(expr(ExpressionKind::Literal(ScalarValue::Int64(0)))),
        }),
    }));
    for (plan, fails) in [(filter, true), (scan, false)] {
        let prepared = prepare_relational_with_schema_in_session(policy, &session, |schemas| {
            schemas.register_memory_source(uri.clone(), |session| {
                ResidentMemorySource::from_columns(
                    session,
                    &[MemoryColumn {
                        name: "n",
                        values: MemoryColumnValues::Utf8(&[Some("1"), Some("bad"), None]),
                    }],
                    MemorySourceBounds::default(),
                )
            })?;
            Ok(plan)
        })
        .unwrap();
        let baseline = memory.snapshot().reserved_bytes;
        let result = prepared.collect_jsonl(&CancellationToken::default());
        if fails {
            assert!(result.is_err());
            assert_eq!(prepared.snapshot().completed_executions, 0);
        } else {
            assert_eq!(result.as_ref().unwrap().execution.output_rows, 3);
        }
        drop(result);
        assert_eq!(memory.snapshot().reserved_bytes, baseline);
        drop(prepared);
    }
    drop(session);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}
