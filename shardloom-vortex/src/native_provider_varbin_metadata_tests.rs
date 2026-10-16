// SPDX-License-Identifier: Apache-2.0
//! Native buffer-table admission, ownership and reconstruction regressions.

use super::*;
use std::mem::size_of;
use vortex::{
    VortexSessionDefault as _,
    array::{
        Canonical, CanonicalValidity, RecursiveCanonical, VortexSessionExecute as _,
        arrays::{
            BoolArray, MaskedArray,
            varbinview::{VarBinViewArrayExt as _, VarBinViewData},
        },
        builtins::ArrayBuiltins as _,
        dtype::Nullability,
        memory::{
            DefaultHostAllocator, HostAllocator, HostAllocatorRef, MemorySessionExt as _,
            WritableHostBuffer,
        },
    },
    buffer::ByteBuffer,
    encodings::runend::RunEnd,
    mask::Mask,
};

fn session(memory: &LiveMemoryPool) -> VortexSession {
    let session = VortexSession::default().with_allocator(Arc::new(
        crate::owned_buffers::ReservedHostAllocator::new(memory.clone()),
    ));
    install(&session, memory.clone());
    session
}

fn raw() -> VarBinViewArray {
    VarBinViewArray::from_iter_str([
        "first outlined value with λ",
        "tiny",
        "last outlined value with 猫",
    ])
}

fn admit(input: &VarBinViewArray, session: &VortexSession) -> VortexResult<VarBinViewArray> {
    VarBinViewArray::try_new_handle_in(
        input.views_handle().clone(),
        input.data_buffers().iter().cloned(),
        input.dtype().clone(),
        input.validity()?,
        &session.allocator(),
    )
}

#[test]
fn native_varbin_table_exact_grant_and_conversion_overlap() {
    let input = raw();
    assert_eq!(input.data_buffers().len(), 1);
    let table = size_of::<BufferHandle>() as u64;
    let memory = LiveMemoryPool::new(2 * table).unwrap();
    let session = session(&memory);
    let output = admit(&input, &session).unwrap();
    assert_eq!(output.metadata_capacity_bytes() as u64, table);
    assert_eq!(memory.snapshot().reserved_bytes, table);
    assert_eq!(memory.snapshot().peak_reserved_bytes, 2 * table);
    assert_eq!(output.bytes_at(2), input.bytes_at(2));
    drop(session);
    assert_eq!(memory.snapshot().reserved_bytes, table);
    drop(output);
    assert_eq!(memory.snapshot().reserved_bytes, 0);

    let memory = LiveMemoryPool::new(2 * table - 1).unwrap();
    let error = admit(&input, &self::session(&memory)).unwrap_err();
    assert!(crate::owned_buffers::is_owned_reservation_denial(&error));
    assert_eq!(memory.snapshot().reserved_bytes, 0);
    assert_eq!(memory.snapshot().peak_reserved_bytes, table);
}

#[test]
fn native_varbin_table_competing_owners_and_raw_payload_lifetime() {
    let input = raw();
    let table = size_of::<BufferHandle>() as u64;
    let memory = LiveMemoryPool::new(2 * table).unwrap();
    let session = session(&memory);
    let output = admit(&input, &session).unwrap();
    let payload = output.data_buffers()[0].clone();
    let error = admit(&input, &session).unwrap_err();
    assert!(crate::owned_buffers::is_owned_reservation_denial(&error));
    assert_eq!(memory.snapshot().reserved_bytes, table);
    drop(output);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
    assert_eq!(payload.as_host(), input.data_buffers()[0].as_host());
    drop(payload);
    drop(admit(&input, &session).unwrap());
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

#[test]
fn native_varbin_table_sharing_survives_native_transformations() {
    let input = raw();
    let memory = LiveMemoryPool::new(1 << 20).unwrap();
    let session = session(&memory);
    let output = admit(&input, &session).unwrap();
    let table = output.metadata_capacity_bytes() as u64;
    let identity = Arc::as_ptr(output.data_buffers());
    let mut ctx = session.create_execution_ctx();
    let array = output.clone().into_array();
    let slice = array.slice(1..3).unwrap().slice(0..1).unwrap();
    let empty = array.slice(1..1).unwrap();
    let cast = array.cast(DType::Utf8(Nullability::Nullable)).unwrap();
    let filtered = array.filter(Mask::from_iter([true, false, true])).unwrap();
    let taken = array
        .take(PrimitiveArray::from_iter([2u32, 0]).into_array())
        .unwrap();
    let canonical = array
        .clone()
        .execute::<CanonicalValidity>(&mut ctx)
        .unwrap()
        .0
        .into_array();
    let recursive = array
        .clone()
        .execute::<RecursiveCanonical>(&mut ctx)
        .unwrap()
        .0
        .into_array();
    let mut copies = vec![output.clone()];
    // The ordinary empty-slice reduction discards the table. It must not pin
    // storage or its credit just to retain the source's physical identity.
    let empty = empty.execute::<VarBinViewArray>(&mut ctx).unwrap();
    assert!(empty.data_buffers().is_empty());
    assert_eq!(empty.metadata_capacity_bytes(), 0);
    for result in [slice, cast, filtered, taken, canonical, recursive] {
        let result = result.execute::<VarBinViewArray>(&mut ctx).unwrap();
        assert_eq!(Arc::as_ptr(result.data_buffers()), identity);
        assert_eq!(result.metadata_capacity_bytes() as u64, table);
        copies.push(result);
    }
    drop((array, output, ctx, session));
    assert_eq!(memory.snapshot().reserved_bytes, table);
    while let Some(copy) = copies.pop() {
        drop(copy);
        assert_eq!(
            memory.snapshot().reserved_bytes,
            if copies.is_empty() { 0 } else { table }
        );
    }
}

#[test]
fn native_varbin_table_replacement_uses_original_allocator_and_a_distinct_table() {
    let input = raw();
    let table = size_of::<BufferHandle>() as u64;
    let memory = LiveMemoryPool::new(3 * table).unwrap();
    let session = session(&memory);
    let first = admit(&input, &session).unwrap();
    drop(session);
    let buffers = first.as_ref().buffer_handles();
    let second = VarBinViewArray::try_from_parts(
        VarBinView.with_buffers(first.as_view(), &buffers).unwrap(),
    )
    .unwrap();
    assert!(!Arc::ptr_eq(first.data_buffers(), second.data_buffers()));
    assert_eq!(memory.snapshot().reserved_bytes, 2 * table);
    assert_eq!(memory.snapshot().peak_reserved_bytes, 3 * table);
    let error = VarBinView
        .with_buffers(first.as_view(), &buffers)
        .err()
        .unwrap();
    assert!(crate::owned_buffers::is_owned_reservation_denial(&error));
    assert_eq!(memory.snapshot().reserved_bytes, 2 * table);
    assert_eq!(second.bytes_at(0), input.bytes_at(0));
    drop(first);
    assert_eq!(memory.snapshot().reserved_bytes, table);
    drop(second);
    // Raw payload handles do not retain table credit.
    assert_eq!(memory.snapshot().reserved_bytes, 0);
    drop(buffers);
}

#[test]
fn native_varbin_table_mask_and_runend_expansion_share_owner_and_values() {
    let input = raw();
    let memory = LiveMemoryPool::new(1 << 20).unwrap();
    let session = session(&memory);
    let output = admit(&input, &session).unwrap();
    let table = output.metadata_capacity_bytes() as u64;
    let identity = Arc::as_ptr(output.data_buffers());
    let mut ctx = session.create_execution_ctx();
    let bitmap = BoolArray::from_iter([true, false, true]).into_array();
    let mask = output
        .clone()
        .into_array()
        .mask(bitmap.clone())
        .unwrap()
        .execute::<VarBinViewArray>(&mut ctx)
        .unwrap();
    let masked = MaskedArray::try_new(output.clone().into_array(), Validity::Array(bitmap))
        .unwrap()
        .into_array()
        .execute::<VarBinViewArray>(&mut ctx)
        .unwrap();
    for result in [&mask, &masked] {
        assert_eq!(Arc::as_ptr(result.data_buffers()), identity);
        let valid = result
            .validity()
            .unwrap()
            .execute_mask(3, &mut ctx)
            .unwrap();
        assert_eq!(
            (valid.value(0), valid.value(1), valid.value(2)),
            (true, false, true)
        );
        assert_eq!(result.bytes_at(0), input.bytes_at(0));
        assert_eq!(result.bytes_at(2), input.bytes_at(2));
    }
    let expanded = RunEnd::try_new(
        PrimitiveArray::from_iter([2u32, 5, 6]).into_array(),
        mask.into_array(),
        &mut ctx,
    )
    .unwrap()
    .into_array()
    .execute::<VarBinViewArray>(&mut ctx)
    .unwrap();
    assert_eq!(Arc::as_ptr(expanded.data_buffers()), identity);
    let valid = expanded
        .validity()
        .unwrap()
        .execute_mask(6, &mut ctx)
        .unwrap();
    for (row, source) in [0, 0, 1, 1, 1, 2].into_iter().enumerate() {
        assert_eq!(valid.value(row), source != 1);
        if source != 1 {
            assert_eq!(expanded.bytes_at(row), input.bytes_at(source));
        }
    }
    drop((output, masked, ctx, session, valid));
    assert_eq!(memory.snapshot().reserved_bytes, table);
    drop(expanded);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

#[test]
fn native_varbin_table_rebuild_validates_layout_and_keeps_outer_owner() {
    let input = raw();
    let memory = LiveMemoryPool::new(1 << 20).unwrap();
    let session = session(&memory);
    let output = admit(&input, &session).unwrap();
    let outer = Arc::new(17usize);
    let output = output
        .into_array()
        .with_retained_owner(outer.clone())
        .unwrap()
        .execute::<VarBinViewArray>(&mut session.create_execution_ctx())
        .unwrap();
    let empty = output
        .with_views_and_validity(
            output
                .views_handle()
                .slice_typed::<vortex::array::arrays::varbinview::BinaryView>(0..0),
            output.dtype().clone(),
            Validity::NonNullable,
        )
        .unwrap();
    assert_eq!(Arc::strong_count(&outer), 3);
    assert!(
        output
            .with_views_and_validity(
                BufferHandle::new_host(ByteBuffer::copy_from(&[1u8])),
                output.dtype().clone(),
                Validity::NonNullable,
            )
            .is_err()
    );
    assert!(
        output
            .with_views_and_validity(
                output.views_handle().clone(),
                DType::Binary(Nullability::NonNullable),
                Validity::NonNullable,
            )
            .is_err()
    );
    drop(output);
    assert_eq!(Arc::strong_count(&outer), 2);
    assert!(memory.snapshot().reserved_bytes > 0);
    drop(empty);
    assert_eq!(Arc::strong_count(&outer), 1);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

#[derive(Debug)]
struct NoExternalReservations;

impl HostAllocator for NoExternalReservations {
    fn allocate(&self, len: usize, alignment: Alignment) -> VortexResult<WritableHostBuffer> {
        DefaultHostAllocator.allocate(len, alignment)
    }
}

#[test]
fn native_varbin_table_empty_needs_no_element_credit_and_custom_policy_is_explicit() {
    let allocator: HostAllocatorRef = Arc::new(NoExternalReservations);
    let input = raw();
    let error = VarBinViewArray::try_new_handle_in(
        input.views_handle().clone(),
        input.data_buffers().iter().cloned(),
        input.dtype().clone(),
        Validity::NonNullable,
        &allocator,
    )
    .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("does not support external memory reservations")
    );
    let input = Canonical::empty(&DType::Utf8(Nullability::NonNullable)).into_array();
    let input = input.as_::<VarBinView>();
    let empty = VarBinViewArray::try_new_handle_in(
        input.views_handle().clone(),
        std::iter::empty(),
        input.dtype().clone(),
        Validity::NonNullable,
        &allocator,
    )
    .unwrap();
    assert_eq!(empty.metadata_capacity_bytes(), 0);
}

struct OverflowingTable;

impl Iterator for OverflowingTable {
    type Item = BufferHandle;

    fn next(&mut self) -> Option<Self::Item> {
        panic!("overflow must be rejected before reading iterator values")
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (usize::MAX, Some(usize::MAX))
    }
}

impl ExactSizeIterator for OverflowingTable {}

struct MisreportedTable(std::vec::IntoIter<BufferHandle>);

impl Iterator for MisreportedTable {
    type Item = BufferHandle;

    fn next(&mut self) -> Option<Self::Item> {
        self.0.next()
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (1, Some(1))
    }
}

impl ExactSizeIterator for MisreportedTable {}

#[test]
fn native_varbin_table_rejects_misreported_iterator_length_and_refunds() {
    let input = raw();
    for count in [0, 2] {
        let memory = LiveMemoryPool::new(1 << 20).unwrap();
        let session = session(&memory);
        let buffers = vec![input.data_buffers()[0].clone(); count];
        let result = VarBinViewData::try_new_handle_in(
            input.views_handle().clone(),
            MisreportedTable(buffers.into_iter()),
            input.dtype().clone(),
            Validity::NonNullable,
            &session.allocator(),
        );
        assert!(result.is_err());
        assert_eq!(
            memory.snapshot().peak_reserved_bytes,
            size_of::<BufferHandle>() as u64
        );
        assert_eq!(memory.snapshot().reserved_bytes, 0);
    }
}

#[test]
fn native_varbin_table_raw_parts_export_is_an_explicit_unmanaged_boundary() {
    let input = raw();
    let memory = LiveMemoryPool::new(1 << 20).unwrap();
    let output = admit(&input, &session(&memory)).unwrap();
    let parts = output.into_data_parts();
    assert_eq!(memory.snapshot().reserved_bytes, 0);
    assert_eq!(
        parts.buffers[0].as_host(),
        input.data_buffers()[0].as_host()
    );
    assert_eq!(parts.views.as_host(), input.views_handle().as_host());
}

#[test]
fn native_varbin_table_capacity_overflow_precedes_allocation() {
    let input = raw();
    let memory = LiveMemoryPool::new(1 << 20).unwrap();
    let session = session(&memory);
    let error = VarBinViewData::try_new_handle_in(
        input.views_handle().clone(),
        OverflowingTable,
        input.dtype().clone(),
        Validity::NonNullable,
        &session.allocator(),
    )
    .unwrap_err();
    assert!(error.to_string().contains("metadata capacity overflow"));
    assert_eq!(memory.snapshot().peak_reserved_bytes, 0);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}
