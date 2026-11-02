use super::*;
use crate::local_primitives::{logical_field_from_native_array, native_payload, vortex_error};
use crate::resident_session::ResidentVortexSession;
use shardloom_exec::compute_pool::CancellationToken;
use vortex::array::{
    IntoArray as _, VortexSessionExecute as _,
    arrays::{NullArray, PrimitiveArray, StructArray},
    dtype::FieldNames,
    validity::Validity,
};

const RAW_FLOAT: u64 = 0x7ff8_0000_0000_0042;

fn record(value: u64, context: &NativeExecutionContext<'_>) -> Result<ArrayRef> {
    let array = StructArray::try_new(
        FieldNames::from(["n", "raw"]),
        vec![
            PrimitiveArray::from_iter([value]).into_array(),
            PrimitiveArray::from_iter([f64::from_bits(RAW_FLOAT)]).into_array(),
        ],
        1,
        Validity::NonNullable,
    )
    .map_err(vortex_error)?
    .into_array();
    native_payload::detach_with_policy(
        &array,
        native_payload::CopyPolicy::PreserveUnobserved,
        context,
    )
}

#[test]
fn native_writer_compaction_counts_actual_copies_and_retains_raw_payload_owners() {
    let session = ResidentVortexSession::new(4 << 20, 1).unwrap();
    let memory = session.memory().clone();
    let retained = session
        .with_native_execution_context(&CancellationToken::default(), |context| {
            let first = record(0, context)?;
            let dtype = first.dtype().clone();
            let mut batches = Batches::new(&dtype, true, context)?;
            let (mut values, mut held) = (Vec::new(), None);
            let (mut copied, mut scalars, mut outputs) = (0, 0, 0);
            let mut consume = |array: ArrayRef| {
                assert!(array.len() <= ROWS);
                let mut execution = context.native_session().create_execution_ctx();
                let numbers = logical_field_from_native_array(&array, "n")?
                    .execute::<PrimitiveArray>(&mut execution)
                    .map_err(vortex_error)?;
                values.extend_from_slice(numbers.to_buffer::<u64>().as_ref());
                let raw = logical_field_from_native_array(&array, "raw")?
                    .execute::<PrimitiveArray>(&mut execution)
                    .map_err(vortex_error)?;
                assert!(
                    raw.to_buffer::<f64>()
                        .iter()
                        .all(|value| value.to_bits() == RAW_FLOAT)
                );
                if array.len() > 1 {
                    copied += array.nbytes();
                    // Integer leaf construction counts once per row; the raw
                    // floating buffer is copied without scalar construction.
                    scalars += array.len() as u64;
                }
                outputs += 1;
                if held.is_none() {
                    held = Some(array);
                }
                Ok(())
            };
            batches.push(first, context, &mut consume)?;
            for value in 1..257 {
                batches.push(record(value, context)?, context, &mut consume)?;
            }
            batches.flush(context, &mut consume)?;
            assert_eq!(values, (0..257).collect::<Vec<_>>());
            assert!(outputs > 1 && outputs < 257);
            assert!(copied > 0 && scalars > 0);
            assert_eq!(batches.copy_counts(), (copied, scalars));
            let mut proof = String::new();
            context.append_adaptive_work_certificate(&mut proof);
            assert!(proof.contains("adaptive_native_output_completed_windows="));
            Ok(held.unwrap())
        })
        .unwrap();
    drop(session);
    assert!(memory.snapshot().reserved_bytes > 0);
    assert!(retained.len() > 1);
    drop(retained);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

#[test]
fn native_writer_pressure_decline_forwards_original_owners_without_copy_counts() {
    let session = ResidentVortexSession::new(1 << 20, 1).unwrap();
    let memory = session.memory().clone();
    session
        .with_native_execution_context(&CancellationToken::default(), |context| {
            let originals = [record(0, context)?, record(1, context)?];
            let mut batches = Batches::new(originals[0].dtype(), true, context)?;
            for array in &originals {
                batches.push(array.clone(), context, &mut |_| panic!("unexpected output"))?;
            }
            let snapshot = memory.snapshot();
            let pressure = memory.reserve(snapshot.limit_bytes - snapshot.reserved_bytes - 1)?;
            let mut calls = 0;
            batches.flush(context, &mut |array| {
                assert!(ArrayRef::ptr_eq(&array, &originals[calls]));
                calls += 1;
                Ok(())
            })?;
            assert_eq!(calls, 2);
            assert_eq!(batches.copy_counts(), (0, 0));
            let mut proof = String::new();
            context.append_adaptive_work_certificate(&mut proof);
            assert!(proof.contains("adaptive_native_output_completed_windows=0"));
            assert!(proof.contains("adaptive_native_output_declined_windows=1"));
            drop(pressure);
            Ok(())
        })
        .unwrap();
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

#[test]
fn native_writer_passthrough_keeps_unsupported_copy_schemas_and_nonproducers() {
    let session = ResidentVortexSession::new(1 << 20, 1).unwrap();
    session
        .with_native_execution_context(&CancellationToken::default(), |context| {
            for (array, produced) in [
                (NullArray::new(3).into_array(), true),
                (record(7, context)?, false),
            ] {
                let mut batches = Batches::new(array.dtype(), produced, context)?;
                assert!(batches.output.is_none());
                let mut calls = 0;
                batches.push(array.clone(), context, &mut |output| {
                    assert!(ArrayRef::ptr_eq(&output, &array));
                    calls += 1;
                    Ok(())
                })?;
                batches.flush(context, &mut |_| panic!("unexpected buffered output"))?;
                assert_eq!(calls, 1);
                assert_eq!(batches.copy_counts(), (0, 0));
            }
            Ok(())
        })
        .unwrap();
    assert_eq!(session.memory().snapshot().reserved_bytes, 0);
}
