use super::*;
use crate::resident_session::ResidentVortexSession;
use shardloom_exec::compute_pool::CancellationToken;
use vortex::array::{
    VortexSessionExecute as _,
    arrays::fixed_size_list::FixedSizeListArraySlotsExt as _,
    arrays::{FixedSizeListArray, PrimitiveArray, StructArray},
    dtype::FieldNames,
    validity::Validity,
};

fn record(value: u64, context: &NativeExecutionContext<'_>) -> Result<ArrayRef> {
    let array = StructArray::try_new(
        FieldNames::from(["n"]),
        vec![PrimitiveArray::from_iter([value]).into_array()],
        1,
        Validity::NonNullable,
    )
    .map_err(vortex_error)?
    .into_array();
    native_payload::detach(&array, context)
}

fn values(array: &ArrayRef, context: &NativeExecutionContext<'_>) -> Result<Vec<u64>> {
    let column = super::super::logical_field_from_native_array(array, "n")?;
    let mut execution = context.native_session().create_execution_ctx();
    (0..array.len())
        .map(
            |row| match super::super::result_batch::scalar_value(&column, row, &mut execution)? {
                super::super::result_batch::Value::UInt(value) => Ok(value),
                _ => Err(failed("test output changed its type")),
            },
        )
        .collect()
}

#[test]
fn output_compaction_applies_pressure_and_bounded_regrowth_with_retained_output() {
    let session = ResidentVortexSession::new(4 << 20, 1).unwrap();
    let memory = session.memory().clone();
    let retained = session
        .with_native_execution_context(&CancellationToken::default(), |context| {
            let first = record(0, context)?;
            let mut output = OutputBuffer::new(
                first.dtype().clone(),
                64,
                native_payload::CopyPolicy::ValidateValues,
                context,
            )?;
            output.push(first, context, &mut |_| panic!("unexpected early output"))?;
            for value in 1..12 {
                output.push(record(value, context)?, context, &mut |_| {
                    panic!("unexpected early output")
                })?;
            }
            assert_eq!(output.pending_rows, 12);
            let cost = output.cost(
                12,
                output.pending_payload_bytes,
                output.pending.values.len(),
                true,
            )?;
            let keep = cost.fixed_bytes + 2 * cost.bytes_per_unit;
            let snapshot = memory.snapshot();
            let mut pressure =
                Some(memory.reserve(snapshot.limit_bytes - snapshot.reserved_bytes - keep)?);
            let (mut observed, mut sizes, mut held) = (Vec::new(), Vec::new(), None);
            output.flush(context, &mut |batch| {
                observed.extend(values(&batch, context)?);
                sizes.push(batch.len());
                if held.is_none() {
                    held = Some(batch);
                    pressure.take();
                }
                Ok(())
            })?;
            assert_eq!(observed, (0..12).collect::<Vec<_>>());
            assert_eq!(sizes[0], 2);
            assert!(sizes.iter().any(|size| *size > sizes[0]), "{sizes:?}");
            assert!(sizes.windows(2).all(|pair| pair[1] <= 2 * pair[0]));
            assert_eq!(output.sizing.report().completed_windows, sizes.len() as u64);
            assert!(output.sizing.report().headroom_limited_windows > 0);
            let mut proof = String::new();
            context.append_adaptive_work_certificate(&mut proof);
            assert!(proof.contains("adaptive_native_output_completed_windows="));
            assert!(!proof.contains("adaptive_native_output_declined_windows="));
            drop(output);
            Ok(held.unwrap())
        })
        .unwrap();
    drop(session);
    assert!(memory.snapshot().reserved_bytes > 0);
    assert_eq!(retained.len(), 2);
    drop(retained);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

#[test]
fn output_compaction_declines_without_spending_existing_record_credits() {
    let session = ResidentVortexSession::new(1 << 20, 1).unwrap();
    let memory = session.memory().clone();
    session
        .with_native_execution_context(&CancellationToken::default(), |context| {
            let first = record(0, context)?;
            let mut output = OutputBuffer::new(
                first.dtype().clone(),
                64,
                native_payload::CopyPolicy::ValidateValues,
                context,
            )?;
            output.push(first, context, &mut |_| panic!("unexpected early output"))?;
            output.push(record(1, context)?, context, &mut |_| {
                panic!("unexpected early output")
            })?;
            let snapshot = memory.snapshot();
            let pressure = memory.reserve(snapshot.limit_bytes - snapshot.reserved_bytes - 1)?;
            let mut observed = Vec::new();
            output.flush(context, &mut |batch| {
                assert_eq!(batch.len(), 1);
                observed.extend(values(&batch, context)?);
                assert!(memory.snapshot().reserved_bytes < snapshot.limit_bytes);
                Ok(())
            })?;
            assert_eq!(observed, vec![0, 1]);
            assert_eq!(output.sizing.report().completed_windows, 0);
            let mut proof = String::new();
            context.append_adaptive_work_certificate(&mut proof);
            assert!(proof.contains("adaptive_native_output_completed_windows=0"));
            assert!(proof.contains("adaptive_native_output_declined_windows=1"));
            assert!(proof.contains("adaptive_native_output_decline_reason=insufficient_headroom"));
            drop((pressure, output));
            Ok(())
        })
        .unwrap();
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

#[test]
fn output_compaction_drains_owners_on_consumer_error_and_cancellation() {
    for cancel in [false, true] {
        let session = ResidentVortexSession::new(1 << 20, 1).unwrap();
        let memory = session.memory().clone();
        let token = CancellationToken::default();
        let result = session.with_native_execution_context(&token, |context| {
            let first = record(0, context)?;
            let mut output = OutputBuffer::new(
                first.dtype().clone(),
                64,
                native_payload::CopyPolicy::ValidateValues,
                context,
            )?;
            output.push(first, context, &mut |_| panic!("unexpected early output"))?;
            output.push(record(1, context)?, context, &mut |_| {
                panic!("unexpected early output")
            })?;
            if cancel {
                token.cancel();
            }
            let mut calls = 0;
            let error = output
                .flush(context, &mut |_| {
                    calls += 1;
                    Err(failed("test consumer stopped"))
                })
                .unwrap_err();
            assert_eq!(calls, usize::from(!cancel));
            assert!(
                error.to_string().contains(if cancel {
                    "cancel"
                } else {
                    "test consumer stopped"
                }),
                "{error}"
            );
            drop(output);
            Ok(())
        });
        if cancel {
            assert!(result.unwrap_err().to_string().contains("cancel"));
        } else {
            result.unwrap();
        }
        assert_eq!(memory.snapshot().reserved_bytes, 0);
    }
}

#[test]
fn output_compaction_sizes_fixed_list_gather_scratch_before_selecting_rows() {
    const WIDTH: usize = 512;
    let session = ResidentVortexSession::new(4 << 20, 1).unwrap();
    let memory = session.memory().clone();
    session
        .with_native_execution_context(&CancellationToken::default(), |context| {
            let make = |row: u64| {
                FixedSizeListArray::try_new(
                    PrimitiveArray::from_iter(
                        (0..WIDTH as u64).map(|item| row * WIDTH as u64 + item),
                    )
                    .into_array(),
                    u32::try_from(WIDTH).map_err(vortex_error)?,
                    Validity::NonNullable,
                    1,
                )
                .map(vortex::array::IntoArray::into_array)
                .map_err(vortex_error)
                .and_then(|array| native_payload::detach(&array, context))
            };
            let first = make(0)?;
            let mut output = OutputBuffer::new(
                first.dtype().clone(),
                64,
                native_payload::CopyPolicy::PreserveUnobserved,
                context,
            )?;
            output.push(first, context, &mut |_| panic!("unexpected early output"))?;
            for row in 1..6 {
                output.push(make(row)?, context, &mut |_| {
                    panic!("unexpected early output")
                })?;
            }
            let cost = output.cost(6, output.pending_payload_bytes, 6, true)?;
            let keep = cost.fixed_bytes + 2 * cost.bytes_per_unit;
            let snapshot = memory.snapshot();
            let pressure = memory.reserve(snapshot.limit_bytes - snapshot.reserved_bytes - keep)?;
            let mut rows = 0;
            let mut sizes = Vec::new();
            output.flush(context, &mut |array| {
                let mut execution = context.native_session().create_execution_ctx();
                let list = array
                    .execute::<FixedSizeListArray>(&mut execution)
                    .map_err(vortex_error)?;
                let elements = list
                    .elements()
                    .clone()
                    .execute::<PrimitiveArray>(&mut execution)
                    .map_err(vortex_error)?;
                assert_eq!(
                    elements.to_buffer::<u64>().as_ref(),
                    &(rows * WIDTH as u64..(rows + list.len() as u64) * WIDTH as u64)
                        .collect::<Vec<_>>()
                );
                rows += list.len() as u64;
                sizes.push(list.len());
                Ok(())
            })?;
            assert_eq!(rows, 6);
            assert_eq!(sizes[0], 2);
            assert!(sizes.iter().all(|rows| *rows <= 2));
            drop(pressure);
            Ok(())
        })
        .unwrap();
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}
