//! Query-context propagation through the shared aggregate UTF8 accessors.

use super::{aggregate_column_accessor_in_context, aggregate_direct_stat_value};
use crate::owned_buffers::ReservedHostAllocator;
use shardloom_core::StatValue;
use shardloom_exec::live_memory::LiveMemoryPool;
use std::sync::Arc;
use vortex::{
    VortexSessionDefault as _,
    array::{
        ArrayRef, IntoArray as _, VortexSessionExecute as _,
        arrays::{DictArray, PrimitiveArray, VarBinViewArray},
        memory::HostMemorySessionExt as _,
        validity::Validity,
    },
    encodings::zstd::Zstd,
    mask::Mask,
    session::VortexSession,
};

fn session(memory: &LiveMemoryPool) -> VortexSession {
    let session = VortexSession::default()
        .with_host_allocator(Arc::new(ReservedHostAllocator::new(memory.clone())));
    crate::native_provider_memory::install(&session, memory.clone());
    session
}

fn fixture() -> (ArrayRef, Vec<String>) {
    let values = (0..128)
        .map(|row| format!("{row:04}:{}:λ", "abcdefghi".repeat(128)))
        .collect::<Vec<_>>();
    let plain = VarBinViewArray::from_iter_str(values.iter().map(String::as_str));
    let encoded = Zstd::from_var_bin_view_without_dict(
        &plain,
        0,
        8,
        &mut VortexSession::default().create_execution_ctx(),
    )
    .unwrap()
    .into_array();
    (encoded, values)
}

fn decode_peak(input: &ArrayRef) -> u64 {
    let memory = LiveMemoryPool::new(16 << 20).unwrap();
    let output = input
        .clone()
        .execute::<VarBinViewArray>(&mut session(&memory).create_execution_ctx())
        .unwrap();
    let peak = memory.snapshot().peak_reserved_bytes;
    drop(output);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
    peak
}

#[test]
fn aggregate_utf8_context_sparse_decode_refuses_retries_and_retains_credits() {
    let (encoded, values) = fixture();
    let full_peak = decode_peak(&encoded);
    for range in [0..128, 3..125] {
        let selected = [0, range.len() - 1];
        let input = encoded
            .slice(range.clone())
            .unwrap()
            .filter(Mask::from_indices(range.len(), selected))
            .unwrap();
        let peak = decode_peak(&input);
        assert!(peak + 64 * 1024 < full_peak);
        let owner = Arc::new(());
        let weak = Arc::downgrade(&owner);
        let input = input.with_retained_owner(owner).unwrap();
        for budget in [1, peak - 1] {
            let denied = LiveMemoryPool::new(budget).unwrap();
            let error = aggregate_column_accessor_in_context(
                "renamed_text",
                &input,
                &mut session(&denied).create_execution_ctx(),
            )
            .err()
            .expect("aggregate UTF8 decode must obey the supplied grant");
            assert!(
                error.to_string().contains("memory reservation denied"),
                "{error}"
            );
            assert!(denied.snapshot().denied_reservations > 0);
            assert_eq!(denied.snapshot().reserved_bytes, 0);
            assert!(weak.upgrade().is_some());
        }
        let memory = LiveMemoryPool::new(peak).unwrap();
        let (accessor, work) = aggregate_column_accessor_in_context(
            "renamed_text",
            &input,
            &mut session(&memory).create_execution_ctx(),
        )
        .unwrap();
        assert_eq!(work.utf8.calls, 1);
        assert_eq!(work.utf8.rows, 2);
        assert_eq!(memory.snapshot().peak_reserved_bytes, peak);
        drop(input);
        assert!(weak.upgrade().is_none());
        for (row, index) in selected.into_iter().enumerate() {
            assert_eq!(
                aggregate_direct_stat_value(&accessor, row).unwrap(),
                StatValue::Utf8(values[range.start + index].clone())
            );
        }
        let super::AggregateDirectColumnAccessor::Utf8Dictionary {
            values: entries, ..
        } = &accessor
        else {
            panic!("native text accessor required");
        };
        let retained = entries[0].clone();
        drop(accessor);
        assert!(memory.snapshot().reserved_bytes > 0);
        assert_eq!(retained.as_ref(), values[range.start]);
        drop(retained);
        assert_eq!(memory.snapshot().reserved_bytes, 0);
    }
}

#[test]
fn aggregate_utf8_context_native_dictionary_values_use_the_query_grant() {
    let (encoded, values) = fixture();
    let peak = decode_peak(&encoded);
    let codes = (0..128_u32).rev().collect::<Vec<_>>();
    let input = DictArray::try_new(
        PrimitiveArray::new(codes.clone(), Validity::NonNullable).into_array(),
        encoded,
    )
    .unwrap()
    .into_array();
    let denied = LiveMemoryPool::new(1).unwrap();
    assert!(
        aggregate_column_accessor_in_context(
            "dictionary_text",
            &input,
            &mut session(&denied).create_execution_ctx(),
        )
        .is_err(),
        "native dictionary values must obey the supplied grant"
    );
    assert!(denied.snapshot().denied_reservations > 0);
    assert_eq!(denied.snapshot().reserved_bytes, 0);
    let memory = LiveMemoryPool::new(peak).unwrap();
    let (accessor, _) = aggregate_column_accessor_in_context(
        "dictionary_text",
        &input,
        &mut session(&memory).create_execution_ctx(),
    )
    .unwrap();
    for (row, code) in codes.into_iter().enumerate() {
        assert_eq!(
            aggregate_direct_stat_value(&accessor, row).unwrap(),
            StatValue::Utf8(values[code as usize].clone())
        );
    }
    assert_eq!(memory.snapshot().peak_reserved_bytes, peak);
    // Native dictionary entries own independent strings after decoding.
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}
