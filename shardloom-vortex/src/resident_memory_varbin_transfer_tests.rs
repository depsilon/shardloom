// SPDX-License-Identifier: Apache-2.0
//! Fresh receiving ownership for native variable-binary buffer tables.

use super::*;
use vortex::array::arrays::{VarBinView, VarBinViewArray};

fn owned_source(producer: &ResidentVortexSession, binary: bool) -> ArrayRef {
    producer
        .with_native_execution_context(&CancellationToken::default(), |context| {
            let raw = if binary {
                VarBinViewArray::from_iter_nullable_bin([
                    Some(b"long outlined binary value\0\xff\x80".as_slice()),
                    None,
                    Some(&[]),
                ])
            } else {
                VarBinViewArray::from_iter_nullable_str([
                    Some("long outlined λ 猫 value"),
                    None,
                    Some(""),
                ])
            };
            let dtype = raw.dtype().clone();
            let column =
                ChunkedArray::try_new(vec![raw.clone().into_array(), raw.into_array()], dtype)
                    .map_err(native_error)?
                    .into_array()
                    .execute::<VarBinViewArray>(
                        &mut context.native_session().create_execution_ctx(),
                    )
                    .map_err(native_error)?
                    .into_array();
            Ok(record(column))
        })
        .unwrap()
}

#[test]
fn encoded_transfer_varbin_tables_use_receiving_credit_and_report_it_once() {
    for binary in [false, true] {
        let producer = ResidentVortexSession::new(1 << 20, 1).unwrap();
        let source_memory = producer.shared_memory_pool();
        let source = owned_source(&producer, binary);
        let source_column = source.nth_child(0).unwrap().clone();
        let expected = detached_varbin_scalars(&source_column);
        let receiving = ResidentVortexSession::new(1 << 20, 1).unwrap();
        let receiving_memory = receiving.shared_memory_pool();
        let copied = copy(&source, &receiving);
        let output = copied.arrays.values[0].0.nth_child(0).unwrap().clone();
        let source_table = source_column.as_::<VarBinView>();
        let output_table = output.as_::<VarBinView>();
        assert!(!Arc::ptr_eq(
            source_table.data_buffers(),
            output_table.data_buffers()
        ));
        assert_eq!(source_table.data_buffers().len(), 2);
        let bytes = (2 * size_of::<BufferHandle>()) as u64;
        assert_eq!(output_table.metadata_capacity_bytes() as u64, bytes);
        assert_eq!(copied.report.materialized_rows, 0);
        // Shared payloads are copied once; each node's views/validity is distinct.
        let mut payloads = BTreeSet::new();
        let mut payload_credit = 0u64;
        for node in [&source_column, source_column.nth_child(0).unwrap()] {
            for buffer in node.buffer_handles() {
                let host = buffer.as_host();
                if payloads.insert(buffer_identity(host)) {
                    payload_credit += (host.len()
                        + host
                            .alignment()
                            .max(Alignment::DEFAULT_ALIGNMENT)
                            .as_usize()) as u64;
                }
            }
        }
        let root_credit = (copied.arrays.values.capacity() * size_of::<(ArrayRef, u64)>()) as u64;
        assert_eq!(
            receiving_memory.snapshot().reserved_bytes,
            copied.report.retained_metadata_bytes + payload_credit + root_credit
        );
        receiving
            .with_native_execution_context(&CancellationToken::default(), |context| {
                let mut plan = Plan::new(context)?;
                assert!(plan.visit(&source_column, 0, context)?);
                assert_eq!(plan.provider_metadata_bytes, bytes);
                Ok(())
            })
            .unwrap();
        drop((source_column, source, producer));
        assert_eq!(source_memory.snapshot().reserved_bytes, 0);
        assert_eq!(scalars(&output), expected);
        drop((copied, receiving));
        assert!(receiving_memory.snapshot().reserved_bytes > 0);
        assert_eq!(scalars(&output), expected);
        drop(output);
        assert_eq!(receiving_memory.snapshot().reserved_bytes, 0);
    }
}

#[test]
fn encoded_transfer_varbin_exact_grants_and_denial_preserve_source() {
    let producer = ResidentVortexSession::new(1 << 20, 1).unwrap();
    let source = owned_source(&producer, true);
    let column = source.nth_child(0).unwrap().clone();
    let expected = detached_varbin_scalars(&column);
    let source_bytes = producer.snapshot().memory.reserved_bytes;
    let probe = ResidentVortexSession::new(1 << 20, 1).unwrap();
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
            assert_eq!(
                scalars(copied.arrays.values[0].0.nth_child(0).unwrap()),
                expected
            );
            assert_eq!(receiving.snapshot().memory.peak_reserved_bytes, peak);
            drop(copied);
        } else {
            assert!(result.is_err());
            assert_eq!(receiving.snapshot().memory.denied_reservations, 1);
        }
        assert_eq!(receiving.snapshot().memory.reserved_bytes, 0);
        assert_eq!(producer.snapshot().memory.reserved_bytes, source_bytes);
    }
    assert_eq!(scalars(&column), expected);
    drop((column, source));
    assert_eq!(producer.snapshot().memory.reserved_bytes, 0);
}
