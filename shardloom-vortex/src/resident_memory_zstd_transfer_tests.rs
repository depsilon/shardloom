//! Receiving ownership for encoded Zstd frame tables and payloads.

use super::*;
use vortex::encodings::zstd::{Zstd, ZstdData};

fn owned_source(producer: &ResidentVortexSession, dictionary: bool) -> ArrayRef {
    producer
        .with_native_execution_context(&CancellationToken::default(), |context| {
            let plain = PrimitiveArray::from_iter((0..224i32).map(|value| value * 7 - 401));
            let mut execution = context.native_session().create_execution_ctx();
            let data = if dictionary {
                ZstdData::from_primitive(&plain, 0, 16, &mut execution)
            } else {
                ZstdData::from_primitive_without_dict(&plain, 0, 16, &mut execution)
            }
            .map_err(native_error)?;
            let column = Zstd::try_new(plain.dtype().clone(), data, Validity::NonNullable)
                .map_err(native_error)?
                .into_array()
                .slice(11..207)
                .map_err(native_error)?;
            Ok(record(column))
        })
        .unwrap()
}

#[test]
fn encoded_transfer_zstd_metadata_uses_receiving_credit_and_releases_the_source() {
    for dictionary in [false, true] {
        let producer = ResidentVortexSession::new(64 << 20, 1).unwrap();
        let source_memory = producer.shared_memory_pool();
        let source = owned_source(&producer, dictionary);
        let source_column = source.nth_child(0).unwrap().clone();
        let expected = scalars(&source_column.slice(5..9).unwrap());
        let receiving = ResidentVortexSession::new(64 << 20, 1).unwrap();
        let receiving_memory = receiving.shared_memory_pool();
        let copied = copy(&source, &receiving);
        let output = copied.arrays.values[0].0.nth_child(0).unwrap().clone();
        assert_ne!(
            source_column.as_::<Zstd>().frame_metadata().as_ptr(),
            output.as_::<Zstd>().frame_metadata().as_ptr()
        );
        assert_eq!(output.as_::<Zstd>().slice_start(), 11);
        assert_eq!(output.as_::<Zstd>().slice_stop(), 207);
        assert_eq!(output.buffer_names()[0] == "dictionary", dictionary);
        assert_eq!(copied.report.materialized_rows, 0);
        let buffers = source_column.buffer_handles();
        let payload_credit: u64 = buffers
            .iter()
            .map(|buffer| {
                let host = buffer.as_host();
                u64::try_from(
                    host.len()
                        + host
                            .alignment()
                            .max(Alignment::DEFAULT_ALIGNMENT)
                            .as_usize(),
                )
                .unwrap()
            })
            .sum();
        let root_credit =
            u64::try_from(copied.arrays.values.capacity() * size_of::<(ArrayRef, u64)>()).unwrap();
        assert_eq!(
            receiving_memory.snapshot().reserved_bytes,
            copied.report.retained_metadata_bytes + payload_credit + root_credit,
            "the report covers the one provider table credit plus transfer metadata"
        );
        drop((buffers, source_column, source, producer));
        assert_eq!(source_memory.snapshot().reserved_bytes, 0);
        let escaped = output.slice(5..9).unwrap();
        // A transferred array also owns outer schema/slot credit. Its public
        // slice retains a native Slice wrapper until admitted execution.
        assert_eq!(escaped.encoding_id().as_ref(), "vortex.slice");
        assert_eq!(escaped.as_::<Slice>().slice_range(), &(5..9));
        assert_eq!(
            escaped
                .nth_child(0)
                .unwrap()
                .as_::<Zstd>()
                .frame_metadata()
                .as_ptr(),
            output.as_::<Zstd>().frame_metadata().as_ptr()
        );
        drop((copied, output, receiving));
        assert!(receiving_memory.snapshot().reserved_bytes > 0);
        assert_eq!(scalars(&escaped), expected);
        drop(escaped);
        assert_eq!(receiving_memory.snapshot().reserved_bytes, 0);
    }
}

#[test]
fn encoded_transfer_zstd_metadata_exact_grants_and_denial_leave_source_usable() {
    let producer = ResidentVortexSession::new(64 << 20, 1).unwrap();
    let source = owned_source(&producer, true);
    let source_bytes = producer.snapshot().memory.reserved_bytes;
    let probe = ResidentVortexSession::new(64 << 20, 1).unwrap();
    drop(copy(&source, &probe));
    let peak = probe.snapshot().memory.peak_reserved_bytes;
    assert_eq!(probe.snapshot().memory.reserved_bytes, 0);
    for budget in [peak - 1, peak] {
        let receiving = ResidentVortexSession::new(budget, 1).unwrap();
        let result =
            receiving.with_native_execution_context(&CancellationToken::default(), |context| {
                copy_records(
                    std::slice::from_ref(&source),
                    context,
                    TransferPolicy::PreserveEncodings,
                )
            });
        if budget == peak {
            let copied = result.unwrap().unwrap();
            assert_eq!(copied.arrays.values[0].0.len(), source.len());
            assert_eq!(receiving.snapshot().memory.peak_reserved_bytes, peak);
            drop(copied);
        } else {
            assert!(result.is_err());
            assert_eq!(receiving.snapshot().memory.denied_reservations, 1);
        }
        assert_eq!(receiving.snapshot().memory.reserved_bytes, 0);
        assert_eq!(producer.snapshot().memory.reserved_bytes, source_bytes);
    }
    assert_eq!(
        scalars(&source.nth_child(0).unwrap().slice(0..1).unwrap()),
        vec![Scalar::from(-324i32)]
    );
    drop(source);
    assert_eq!(producer.snapshot().memory.reserved_bytes, 0);
}

#[test]
fn encoded_transfer_zstd_frame_vectors_are_not_reserved_again_by_the_transfer_plan() {
    let plain = PrimitiveArray::from_iter(0i64..512);
    let data = ZstdData::from_primitive_without_dict(
        &plain,
        0,
        8,
        &mut VortexSession::default().create_execution_ctx(),
    )
    .unwrap();
    let bytes = u64::try_from(data.metadata_capacity_bytes().unwrap()).unwrap();
    let column = Zstd::try_new(plain.dtype().clone(), data, Validity::NonNullable)
        .unwrap()
        .into_array();
    assert_eq!(column.as_::<Zstd>().frame_metadata().len(), 64);
    let receiving = ResidentVortexSession::new(1 << 20, 1).unwrap();
    receiving
        .with_native_execution_context(&CancellationToken::default(), |context| {
            let mut plan = Plan::new(context)?;
            assert!(plan.visit(&column, 0, context)?);
            assert_eq!(plan.provider_metadata_bytes, bytes);
            assert!(
                plan.metadata.bytes() < bytes,
                "the provider-sized frame vectors must not remain in the outer metadata estimate"
            );
            Ok(())
        })
        .unwrap();
    assert_eq!(receiving.snapshot().memory.reserved_bytes, 0);
}
