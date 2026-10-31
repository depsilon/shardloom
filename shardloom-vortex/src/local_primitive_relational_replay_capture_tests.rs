use super::*;
use crate::{
    local_primitives::logical_field_from_native_array, owned_buffers,
    resident_session::ResidentVortexSession,
};
use shardloom_exec::compute_pool::CancellationToken;
use std::sync::Arc;
use vortex::{
    array::{
        IntoArray as _, VortexSessionExecute as _,
        arrays::{Primitive, PrimitiveArray, StructArray},
        validity::Validity,
    },
    buffer::Buffer,
};

fn recorder(
    context: &NativeExecutionContext<'_>,
    copy_rows: usize,
) -> Result<Recorder<'static, 'static>> {
    Ok(Recorder {
        resident: ReservedVec::new(context.memory())?,
        copy_rows,
        capture: None,
        #[cfg(feature = "vortex-write")]
        ordering: None,
        _scope: std::marker::PhantomData,
    })
}

#[test]
fn replay_capture_releases_every_fragment_and_preserves_raw_float_bits_and_alias_credit() {
    let session = ResidentVortexSession::new(8 << 20, 1).unwrap();
    session
        .with_native_execution_context(&CancellationToken::default(), |context| {
            let mut recorder = recorder(context, 32)?;
            let bits = [
                (-0.0f64).to_bits(),
                0.0f64.to_bits(),
                0x7ff8_0000_0000_0042,
                f64::INFINITY.to_bits(),
            ];
            let mut report = CopyStats::default();
            #[cfg(debug_assertions)]
            let observed = crate::local_primitives::native_relational_observation::Session::start();
            for row in 0..67 {
                let credit = Arc::new(context.memory().reserve(512)?);
                let witness = Arc::downgrade(&credit);
                let data = owned_buffers::retain_shared_credit(
                    Buffer::from(vec![f64::from_bits(bits[row % 4])]).into_byte_buffer(),
                    credit,
                );
                let array = StructArray::from_fields(&[(
                    "value",
                    PrimitiveArray::new(
                        Buffer::<f64>::from_byte_buffer(data),
                        Validity::NonNullable,
                    )
                    .into_array(),
                )])
                .unwrap()
                .into_array();
                recorder.build(&array, &mut report, context)?;
                drop(array);
                assert!(witness.upgrade().is_none(), "replay retained its producer");
            }
            assert_eq!(report.rows, 67);
            assert_eq!(report.batches, 3);
            assert_eq!(report.buffer_bytes, 67 * 8);
            assert_eq!(report.max_batch_rows, 32);
            #[cfg(debug_assertions)]
            assert_eq!(observed.finish().replay_captured_rows, 67);
            let completed = recorder.finish(context)?;
            let Completed::Resident(arrays) = &completed else {
                panic!("resident capture spilled")
            };
            assert_eq!(
                arrays.values.iter().map(ArrayRef::len).collect::<Vec<_>>(),
                vec![32, 32, 3]
            );
            let mut actual = Vec::new();
            for array in &arrays.values {
                let column = logical_field_from_native_array(array, "value")?;
                actual.extend(
                    column
                        .as_::<Primitive>()
                        .to_buffer::<f64>()
                        .iter()
                        .map(|v| v.to_bits()),
                );
            }
            assert_eq!(actual, (0..67).map(|row| bits[row % 4]).collect::<Vec<_>>());
            let alias = logical_field_from_native_array(&arrays.values[0], "value")?;
            drop(completed);
            assert!(context.memory().snapshot().reserved_bytes > 0);
            drop(alias);
            assert_eq!(context.memory().snapshot().reserved_bytes, 0);
            Ok(())
        })
        .unwrap();
    assert_eq!(session.memory().snapshot().reserved_bytes, 0);
}

#[test]
fn replay_capture_keeps_nullable_fragments_large_controls_and_pressure_declines() {
    let session = ResidentVortexSession::new(8 << 20, 1).unwrap();
    session
        .with_native_execution_context(&CancellationToken::default(), |context| {
            let mut recorder = recorder(context, 128)?;
            let mut report = CopyStats::default();
            for size in [1, 17, 128, 17] {
                let array = StructArray::from_fields(&[(
                    "n",
                    PrimitiveArray::from_option_iter(
                        (0..size).map(|row| (row % 3 != 0).then_some(i16::try_from(row).unwrap())),
                    )
                    .into_array(),
                )])
                .unwrap()
                .into_array();
                recorder.build(&array, &mut report, context)?;
            }
            let completed = recorder.finish(context)?;
            let Completed::Resident(arrays) = completed else {
                panic!("unexpected spill")
            };
            assert_eq!(
                arrays.values.iter().map(ArrayRef::len).collect::<Vec<_>>(),
                vec![18, 128, 17]
            );
            let mut actual = Vec::new();
            let mut execution = context.native_session().create_execution_ctx();
            for array in &arrays.values {
                let values = logical_field_from_native_array(array, "n")?;
                for row in 0..array.len() {
                    actual.push(
                        crate::local_primitives::result_batch::scalar_value(
                            &values,
                            row,
                            &mut execution,
                        )?
                        .into_json()?,
                    );
                }
            }
            let expected = [1, 17, 128, 17]
                .into_iter()
                .flat_map(|size| {
                    (0..size).map(|row| {
                        (row % 3 != 0)
                            .then_some(row)
                            .map_or(serde_json::Value::Null, serde_json::Value::from)
                    })
                })
                .collect::<Vec<_>>();
            assert_eq!(actual, expected);
            drop(arrays);
            let input =
                StructArray::from_fields(&[("n", PrimitiveArray::from_iter([1u64]).into_array())])
                    .unwrap()
                    .into_array();
            let snapshot = context.memory().snapshot();
            let pressure = context
                .memory()
                .reserve(snapshot.limit_bytes - snapshot.reserved_bytes - 8192)?;
            assert!(capture::Capture::new(input.dtype(), 1024, context)?.is_none());
            drop(pressure);
            Ok(())
        })
        .unwrap();
    assert_eq!(session.memory().snapshot().reserved_bytes, 0);
}
