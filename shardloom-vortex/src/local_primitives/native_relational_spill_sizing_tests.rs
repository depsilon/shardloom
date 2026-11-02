use super::*;
use crate::{
    local_primitives::{
        logical_field_from_native_array, native_payload, result_batch, vortex_error,
    },
    relational_query::{VortexRelationalNullOrder, VortexRelationalOrderKey},
    resident_session::ResidentVortexSession,
};
use shardloom_core::ColumnRef;
use shardloom_exec::live_memory::MemoryLease;
use vortex::array::{
    IntoArray as _, VortexSessionExecute as _,
    arrays::{
        DecimalArray, FixedSizeListArray, PrimitiveArray, StructArray,
        fixed_size_list::FixedSizeListArrayExt as _,
    },
    dtype::{DecimalDType, FieldNames},
    validity::Validity,
};

const WIDTH: usize = 512;
const WINDOW: usize = 32;

fn workspace() -> std::path::PathBuf {
    let path = std::env::temp_dir().join(format!(
        "shardloom-adaptive-spill-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos(),
    ));
    std::fs::create_dir(&path).unwrap();
    path
}

fn fixture(rows: usize) -> ArrayRef {
    fixture_range(0, rows)
}

fn fixture_range(first: u64, rows: usize) -> ArrayRef {
    let start = first * WIDTH as u64;
    let wide = FixedSizeListArray::try_new(
        PrimitiveArray::from_iter(start..start + u64::try_from(rows * WIDTH).unwrap()).into_array(),
        u32::try_from(WIDTH).unwrap(),
        Validity::NonNullable,
        rows,
    )
    .unwrap()
    .into_array();
    StructArray::try_new(
        FieldNames::from(["key", "id", "wide"]),
        vec![
            PrimitiveArray::from_iter((0..rows).map(|row| ((rows - row - 1) / 3) as u64))
                .into_array(),
            PrimitiveArray::from_iter(first..first + rows as u64).into_array(),
            wide,
        ],
        rows,
        Validity::NonNullable,
    )
    .unwrap()
    .into_array()
}

fn binding(array: &ArrayRef) -> Spec {
    let DType::Struct(fields, _) = array.dtype() else {
        panic!("expected struct")
    };
    Spec {
        fields: fields
            .names()
            .iter()
            .zip(fields.fields())
            .map(|(name, dtype)| (name.to_string(), dtype))
            .collect(),
        keys: vec![VortexRelationalOrderKey {
            column: ColumnRef::new("key").unwrap(),
            descending: false,
            nulls: Some(VortexRelationalNullOrder::First),
        }],
        names: vec!["key".into()],
        copy_policy: native_payload::CopyPolicy::ValidateValues,
    }
}

fn ids(array: &ArrayRef, context: &NativeExecutionContext<'_>) -> Result<Vec<u64>> {
    let column = logical_field_from_native_array(array, "id")?;
    let mut execution = context.native_session().create_execution_ctx();
    let ids = (0..array.len())
        .map(
            |row| match result_batch::scalar_value(&column, row, &mut execution)? {
                result_batch::Value::UInt(value) => Ok(value),
                _ => Err(failed("test id type changed")),
            },
        )
        .collect::<Result<Vec<_>>>()?;
    let lists = logical_field_from_native_array(array, "wide")?
        .execute::<FixedSizeListArray>(&mut execution)
        .map_err(vortex_error)?;
    for (row, id) in ids.iter().enumerate() {
        let elements = lists
            .fixed_size_list_elements_at(row)
            .map_err(vortex_error)?;
        assert_eq!(elements.len(), WIDTH);
        for element in 0..WIDTH {
            let result_batch::Value::UInt(value) =
                result_batch::scalar_value(&elements, element, &mut execution)?
            else {
                return Err(failed("test nested value type changed"));
            };
            assert_eq!(value, id * WIDTH as u64 + element as u64);
        }
    }
    Ok(ids)
}

fn pressure_for_block(
    order: &mut Ordering<'_, '_>,
    rows: usize,
    block_rows: u64,
    context: &NativeExecutionContext<'_>,
) -> Result<MemoryLease> {
    // Reserve persistent work and the run-reference table before creating the
    // pressure owner. The remaining transient ordinal table has a known bound.
    order.work(context)?;
    order.runs.reserve_one()?;
    let cost = order.write_sizing.block_cost(
        order.spec,
        rows as u64,
        block_rows,
        order.state.run_workspace_cost()?,
    )?;
    let keep = cost.fixed_bytes
        + block_rows * cost.bytes_per_unit
        + super::super::native_capacity::bytes::<usize>(rows.max(8))?;
    let snapshot = context.memory().snapshot();
    context
        .memory()
        .reserve(snapshot.limit_bytes - snapshot.reserved_bytes - keep)
}

#[test]
fn adaptive_spill_partial_text_blocks_release_the_full_validation_window_backing() {
    use vortex::array::arrays::VarBinViewArray;

    let text = |row| format!("{row:08}{}", "x".repeat(760));
    let source = StructArray::try_new(
        FieldNames::from(["key", "id", "wide"]),
        vec![
            PrimitiveArray::from_iter(0..64_u64).into_array(),
            PrimitiveArray::from_iter(0..64_u64).into_array(),
            VarBinViewArray::from_iter_str((0..64).map(text)).into_array(),
        ],
        64,
        Validity::NonNullable,
    )
    .unwrap()
    .into_array();
    let spec = binding(&source);
    let session = ResidentVortexSession::new(8 << 20, 1).unwrap();
    session.with_native_execution_context(&CancellationToken::default(), |context| {
        let work = sizing::RunWork::new(&spec, WINDOW)?;
        let measured = Cell::new(0);
        let maximum = Cell::new(0);
        let input = (0..64).step_by(WINDOW).map(|start| {
            let window = source.slice(start..start + WINDOW).map_err(vortex_error)?;
            native_payload::detach(&window, context)
        });
        let blocks = sizing::Blocks::new(
            input, 17, WINDOW, &measured, &maximum, work.slice_credit(context)?, context,
        );
        let mut delivered = 0;
        let mut execution = context.native_session().create_execution_ctx();
        for block in blocks {
            let block = block?;
            assert!(block.nbytes() <= block.len() as u64 * 816);
            let ids = logical_field_from_native_array(&block, "id")?;
            let values = logical_field_from_native_array(&block, "wide")?;
            for row in 0..block.len() {
                assert!(matches!(result_batch::scalar_value(&ids, row, &mut execution)?, result_batch::Value::UInt(id) if id == delivered));
                let result_batch::Value::SharedText(value) = result_batch::scalar_value(&values, row, &mut execution)? else {
                    return Err(failed("physical spill text dtype changed"));
                };
                assert_eq!(value.as_str(), text(delivered));
                delivered += 1;
            }
        }
        assert_eq!(delivered, 64);
        assert!(measured.get() >= 768);
        assert!(maximum.get() < 17 * 816);
        Ok(())
    }).unwrap();
    assert_eq!(session.snapshot().memory.reserved_bytes, 0);
}

#[test]
fn adaptive_spill_records_physical_geometry_for_positional_reads_and_replay() {
    let workspace = workspace();
    let policy = VortexRelationalSpillPolicy::new(&workspace, 32 << 20, 8 << 20).unwrap();
    let session = ResidentVortexSession::new(16 << 20, 1).unwrap();
    let memory = session.memory().clone();
    let array = fixture(64);
    let spec = binding(&array);
    let mut expected: Vec<_> = (0..64_u64).collect();
    expected.sort_by_key(|row| ((63 - row) / 3, *row));
    let held = session
        .with_native_execution_context(&CancellationToken::default(), |context| {
            let state = State::new(&policy, context)?;
            let mut order = Ordering::new(&spec, &state, WINDOW, context)?;
            order.build(native_payload::detach(&array, context)?, context)?;
            let pressure = pressure_for_block(&mut order, 64, 17, context)?;
            order.flush(context)?;
            assert_eq!(order.runs.values[0].native.block_rows, 17);
            drop(pressure);
            let stored = order.retain(context)?;
            assert!(stored.is_spilled());
            assert_eq!(stored.block_rows(), 17);
            assert!(stored.read_block_at(1, context).is_err());
            let mut observed = Vec::new();
            for start in (0..stored.rows()).step_by(stored.block_rows()) {
                let block = stored.read_block_at(start, context)?.unwrap();
                assert_eq!(block.array().len(), (64 - start).min(17) as usize);
                observed.extend(ids(block.array(), context)?);
            }
            assert_eq!(observed, expected);
            let held = stored.read_block_at(0, context)?.unwrap();
            let replay = stored.into_replay(context)?;
            assert!(replay.is_spilled());
            observed.clear();
            replay.for_each(&state, &spec, context, &mut |block| {
                assert_eq!(block.len(), (64 - observed.len()).min(17));
                observed.extend(ids(block, context)?);
                Ok(())
            })?;
            assert_eq!(observed, expected);
            drop(replay);
            let mut proof = String::new();
            context.append_adaptive_work_certificate(&mut proof);
            assert!(proof.contains("adaptive_native_spill_completed_windows=1"));
            assert!(proof.contains("adaptive_native_spill_minimum_rows=17"));
            let report = state.finish()?;
            assert_eq!(report.run_block_rows, 17);
            assert!(report.owned_cleanup_completed);
            Ok(held)
        })
        .unwrap();
    drop(session);
    assert!(memory.snapshot().reserved_bytes > 0);
    assert_eq!(held.array().len(), 17);
    drop(held);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
    assert_eq!(std::fs::read_dir(&workspace).unwrap().count(), 0);
    std::fs::remove_dir(workspace).unwrap();
}

#[test]
fn adaptive_spill_preserves_the_first_payload_error_in_the_original_window() {
    let workspace = workspace();
    let policy = VortexRelationalSpillPolicy::new(&workspace, 32 << 20, 8 << 20).unwrap();
    let wide = logical_field_from_native_array(&fixture(WINDOW), "wide").unwrap();
    let array = StructArray::try_new(
        FieldNames::from(["key", "decimal", "float", "wide"]),
        vec![
            PrimitiveArray::from_iter(0..WINDOW as u64).into_array(),
            DecimalArray::from_option_iter(
                (0..WINDOW).map(|row| Some(if row + 1 == WINDOW { 1000_i128 } else { 1 })),
                DecimalDType::new(3, 0),
            )
            .into_array(),
            PrimitiveArray::from_iter(
                (0..WINDOW).map(|row| if row == 0 { f64::NAN } else { 1.0_f64 }),
            )
            .into_array(),
            wide,
        ],
        WINDOW,
        Validity::NonNullable,
    )
    .unwrap()
    .into_array();
    let spec = binding(&array);
    let mut errors = Vec::new();
    for pressured in [false, true] {
        let session = ResidentVortexSession::new(16 << 20, 1).unwrap();
        session
            .with_native_execution_context(&CancellationToken::default(), |context| {
                let state = State::new(&policy, context)?;
                let mut order = Ordering::new(&spec, &state, WINDOW, context)?;
                order.build(array.clone(), context)?;
                let pressure = pressured
                    .then(|| pressure_for_block(&mut order, WINDOW, 16, context))
                    .transpose()?;
                let error = order.flush(context).unwrap_err().to_string();
                assert!(error.contains("precision"), "{error}");
                errors.push(error);
                drop((pressure, order));
                assert!(state.finish()?.owned_cleanup_completed);
                let mut proof = String::new();
                context.append_adaptive_work_certificate(&mut proof);
                assert!(!proof.contains("adaptive_native_spill_completed_windows="));
                Ok(())
            })
            .unwrap();
        assert_eq!(session.memory().snapshot().reserved_bytes, 0);
    }
    assert_eq!(errors[0], errors[1]);
    assert_eq!(std::fs::read_dir(&workspace).unwrap().count(), 0);
    std::fs::remove_dir(workspace).unwrap();
}

#[test]
fn adaptive_spill_denial_precedes_run_creation_and_keeps_cleanup_owned() {
    let workspace = workspace();
    let policy = VortexRelationalSpillPolicy::new(&workspace, 32 << 20, 8 << 20).unwrap();
    let session = ResidentVortexSession::new(16 << 20, 1).unwrap();
    let array = fixture(64);
    let spec = binding(&array);
    session
        .with_native_execution_context(&CancellationToken::default(), |context| {
            let state = State::new(&policy, context)?;
            let mut order = Ordering::new(&spec, &state, WINDOW, context)?;
            order.build(array, context)?;
            order.work(context)?;
            order.runs.reserve_one()?;
            let snapshot = context.memory().snapshot();
            let pressure = context
                .memory()
                .reserve(snapshot.limit_bytes - snapshot.reserved_bytes - 16 * 1024)?;
            let error = order.flush(context).unwrap_err().to_string();
            assert!(error.contains("owner=native_spill_write"), "{error}");
            assert!(error.contains("required_additional_bytes="), "{error}");
            assert!(state.store.borrow().is_none());
            drop((pressure, order));
            assert!(state.finish()?.owned_cleanup_completed);
            Ok(())
        })
        .unwrap();
    assert_eq!(session.memory().snapshot().reserved_bytes, 0);
    assert_eq!(std::fs::read_dir(&workspace).unwrap().count(), 0);
    std::fs::remove_dir(workspace).unwrap();
}

#[test]
fn adaptive_spill_merges_different_run_geometries_with_stable_ties_and_complete_values() {
    let workspace = workspace();
    let policy = VortexRelationalSpillPolicy::new(&workspace, 32 << 20, 8 << 20).unwrap();
    let session = ResidentVortexSession::new(16 << 20, 1).unwrap();
    let array = fixture(64);
    let spec = binding(&array);
    let mut expected: Vec<_> = (0..128_u64).collect();
    expected.sort_by_key(|row| ((63 - row % 64) / 3, *row));
    session
        .with_native_execution_context(&CancellationToken::default(), |context| {
            let state = State::new(&policy, context)?;
            let mut order = Ordering::new(&spec, &state, WINDOW, context)?;
            order.build(native_payload::detach(&array, context)?, context)?;
            let pressure = pressure_for_block(&mut order, 64, 16, context)?;
            order.flush(context)?;
            assert_eq!(order.runs.values[0].native.block_rows, 16);
            drop(pressure);
            order.build(
                native_payload::detach(&fixture_range(64, 64), context)?,
                context,
            )?;
            order.flush(context)?;
            assert_eq!(state.merge_passes.get(), 1);
            let stored = order.retain(context)?;
            assert_eq!(stored.block_rows(), WINDOW);
            let mut actual = Vec::new();
            for start in (0..stored.rows()).step_by(stored.block_rows()) {
                let block = stored.read_block_at(start, context)?.unwrap();
                actual.extend(ids(block.array(), context)?);
            }
            assert_eq!(actual, expected);
            stored.finish(context)?;
            let mut proof = String::new();
            context.append_adaptive_work_certificate(&mut proof);
            assert!(proof.contains("adaptive_native_spill_completed_windows=3"));
            assert!(proof.contains("adaptive_native_spill_minimum_rows=16"));
            assert!(proof.contains("adaptive_native_spill_maximum_rows=32"));
            assert!(state.finish()?.owned_cleanup_completed);
            Ok(())
        })
        .unwrap();
    assert_eq!(session.memory().snapshot().reserved_bytes, 0);
    assert_eq!(std::fs::read_dir(&workspace).unwrap().count(), 0);
    std::fs::remove_dir(workspace).unwrap();
}

#[test]
fn adaptive_spill_cleanup_survives_merge_cancellation_and_consumer_refusal() {
    for cancel in [false, true] {
        let workspace = workspace();
        let policy = VortexRelationalSpillPolicy::new(&workspace, 32 << 20, 8 << 20).unwrap();
        let session = ResidentVortexSession::new(16 << 20, 1).unwrap();
        let token = CancellationToken::default();
        let result = session.with_native_execution_context(&token, |context| {
            let array = fixture(64);
            let spec = binding(&array);
            let state = State::new(&policy, context)?;
            let mut order = Ordering::new(&spec, &state, WINDOW, context)?;
            order.build(native_payload::detach(&array, context)?, context)?;
            let pressure = pressure_for_block(&mut order, 64, 16, context)?;
            order.flush(context)?;
            drop(pressure);
            if cancel {
                order.build(native_payload::detach(&array, context)?, context)?;
                let cancel = token.clone();
                AFTER_MERGE_BLOCK
                    .with(|hook| *hook.borrow_mut() = Some(Box::new(move |_| cancel.cancel())));
                assert!(
                    order
                        .flush(context)
                        .unwrap_err()
                        .to_string()
                        .contains("cancel")
                );
                assert!(AFTER_MERGE_BLOCK.with(|hook| hook.borrow().is_none()));
                drop(order);
            } else {
                let mut calls = 0;
                let error = order
                    .finish(context, WINDOW, &mut |_| {
                        calls += 1;
                        Err(failed("test spill consumer stopped"))
                    })
                    .unwrap_err();
                assert_eq!(calls, 1);
                assert!(error.to_string().contains("test spill consumer stopped"));
            }
            assert_eq!(state.open_runs.get(), 0);
            assert!(state.finish()?.owned_cleanup_completed);
            Ok(())
        });
        if cancel {
            assert!(result.unwrap_err().to_string().contains("cancel"));
        } else {
            result.unwrap();
        }
        assert_eq!(session.memory().snapshot().reserved_bytes, 0);
        assert_eq!(std::fs::read_dir(&workspace).unwrap().count(), 0);
        std::fs::remove_dir(workspace).unwrap();
    }
}
