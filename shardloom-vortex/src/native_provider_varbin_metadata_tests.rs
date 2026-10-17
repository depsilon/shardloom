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
            varbinview::{BinaryView, VarBinViewArrayExt as _, VarBinViewData},
        },
        builtins::ArrayBuiltins as _,
        dtype::Nullability,
        memory::{
            DefaultHostAllocator, HostAllocator, HostAllocatorRef, MemorySessionExt as _,
            WritableHostBuffer,
        },
    },
    buffer::{Buffer, ByteBuffer},
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

fn malformed_host_views(input: &VarBinViewArray) -> Vec<(&'static str, BinaryView)> {
    let bytes = input.bytes_at(0);
    // The first value ends in a multibyte character. Its truncated prefix is
    // still in bounds and has the right four-byte prefix, but is not UTF-8.
    let truncated = &bytes[..bytes.len() - 1];
    assert!(std::str::from_utf8(truncated).is_err());
    let start = bytes.len() - 1;
    let payload = input.data_buffers()[0].as_host();
    let split_start = &payload[start..start + 13];
    assert!(std::str::from_utf8(split_start).is_err());
    vec![
        ("buffer index", BinaryView::new_ref(13, *b"firs", 1, 0)),
        (
            "start offset",
            BinaryView::new_ref(13, *b"firs", 0, u32::MAX),
        ),
        ("end offset", BinaryView::new_ref(u32::MAX, *b"firs", 0, 1)),
        ("prefix", BinaryView::new_ref(13, *b"WRNG", 0, 0)),
        ("inline UTF-8", BinaryView::new_inlined(&[0xff])),
        ("outlined UTF-8", BinaryView::make_view(truncated, 0, 0)),
        (
            "outlined UTF-8 start",
            BinaryView::make_view(split_start, 0, u32::try_from(start).unwrap()),
        ),
    ]
}

fn assert_parts_validate_changed_value_type() {
    for bytes in [
        &[0xff][..],
        b"outlined invalid UTF-8: \xff".as_slice(),
        "valid UTF-8: λ猫🙂".as_bytes(),
    ] {
        for validity in [Validity::AllValid, Validity::AllInvalid] {
            let memory = LiveMemoryPool::new(1 << 20).unwrap();
            let session = session(&memory);
            let raw = VarBinViewArray::from_iter_nullable_bin([Some(bytes)]);
            let source = admit(&raw, &session)
                .unwrap()
                .with_views_and_validity(
                    raw.views_handle().clone(),
                    DType::Binary(Nullability::Nullable),
                    validity,
                )
                .unwrap();
            let table = memory.snapshot().reserved_bytes;
            let changed = VarBinViewArray::try_from_parts(
                ArrayParts::new(
                    VarBinView,
                    DType::Utf8(Nullability::Nullable),
                    source.len(),
                    source.data().clone(),
                )
                .with_slots(source.slots().iter().cloned().collect()),
            );
            assert_eq!(changed.is_ok(), std::str::from_utf8(bytes).is_ok());
            if let Ok(changed) = changed {
                assert_eq!(changed.bytes_at(0), source.bytes_at(0));
            }
            assert_eq!(memory.snapshot().reserved_bytes, table);
            drop(source);
            assert_eq!(memory.snapshot().reserved_bytes, 0);
        }
    }
}

#[test]
fn native_varbin_table_safe_constructors_reject_malformed_host_views_and_refund() {
    assert_parts_validate_changed_value_type();
    let input = raw();
    let memory = LiveMemoryPool::new(1 << 20).unwrap();
    let session = session(&memory);
    let mut accepted = Vec::new();
    // Both a small input and many short views into one valid UTF-8 heap must
    // reject malformed views, even when all rows are null. The latter can use
    // whole-buffer validation but still needs every structural/boundary check.
    for (case, view, count) in malformed_host_views(&input)
        .into_iter()
        .flat_map(|(case, view)| [1, 64].map(|count| (case, view, count)))
    {
        for validity in [
            Validity::NonNullable,
            Validity::AllValid,
            Validity::AllInvalid,
            Validity::Array(BoolArray::from_iter(vec![false; count]).into_array()),
        ] {
            let dtype = DType::Utf8(validity.nullability());
            let mut views = vec![input.views()[0]; count];
            views[count - 1] = view;
            let views = BufferHandle::new_host(Buffer::copy_from(views).into_byte_buffer());
            let data = VarBinViewData::try_new_handle_in(
                views.clone(),
                input.data_buffers().iter().cloned(),
                dtype.clone(),
                validity.clone(),
                &session.allocator(),
            );
            if data.is_ok() {
                accepted.push((case, "admitted data", validity.clone()));
            }
            drop(data);
            assert_eq!(memory.snapshot().reserved_bytes, 0);
            let array = VarBinViewArray::try_new_handle_in(
                views.clone(),
                input.data_buffers().iter().cloned(),
                dtype.clone(),
                validity.clone(),
                &session.allocator(),
            );
            if array.is_ok() {
                accepted.push((case, "admitted array", validity.clone()));
            }
            drop(array);
            assert_eq!(memory.snapshot().reserved_bytes, 0);
            let data = VarBinViewData::try_new_handle(
                views,
                Arc::clone(input.data_buffers()),
                dtype,
                validity.clone(),
            );
            if data.is_ok() {
                accepted.push((case, "raw data", validity));
            }
            drop(data);
        }
    }
    // Unreferenced bytes are not string values. Invalid suffixes must not
    // change acceptance, whether the heap is dense or mostly unused. A corrupt
    // referenced value must still fail when whole-heap validation fails.
    for suffix in [vec![], vec![0xff], vec![0xff; 16_384]] {
        for corrupt_value in [false, true] {
            let mut payload = input.data_buffers()[0].as_host().as_slice().to_vec();
            payload.extend_from_slice(&suffix);
            if corrupt_value {
                payload[5] = 0xff;
            }
            let result = VarBinViewArray::try_new_handle_in(
                BufferHandle::new_host(
                    Buffer::copy_from(vec![input.views()[0]; 64]).into_byte_buffer(),
                ),
                [BufferHandle::new_host(ByteBuffer::copy_from(payload))].into_iter(),
                DType::Utf8(Nullability::NonNullable),
                Validity::NonNullable,
                &session.allocator(),
            );
            if corrupt_value {
                assert!(result.unwrap_err().to_string().contains("utf-8"));
            } else {
                let output = result.unwrap();
                for row in 0..output.len() {
                    assert_eq!(output.bytes_at(row), input.bytes_at(0));
                }
                drop(output);
            }
            assert_eq!(memory.snapshot().reserved_bytes, 0);
        }
    }
    assert!(
        accepted.is_empty(),
        "malformed views accepted: {accepted:?}"
    );
}

#[test]
fn native_varbin_table_safe_replacement_rejects_malformed_host_views_and_preserves_owner() {
    let input = raw();
    let memory = LiveMemoryPool::new(1 << 20).unwrap();
    let session = session(&memory);
    let output = admit(&input, &session).unwrap();
    let table = memory.snapshot().reserved_bytes;
    let identity = Arc::as_ptr(output.data_buffers());
    for (case, view) in malformed_host_views(&input) {
        for validity in [Validity::NonNullable, Validity::AllInvalid] {
            let result = output.with_views_and_validity(
                BufferHandle::new_host(Buffer::copy_from([view]).into_byte_buffer()),
                DType::Utf8(validity.nullability()),
                validity.clone(),
            );
            assert!(result.is_err(), "view replacement accepted {case}");
            drop(result);
            let source = output
                .with_views_and_validity(
                    output.views_handle().clone(),
                    DType::Utf8(validity.nullability()),
                    validity,
                )
                .unwrap();
            let mut changed = source.views().to_vec();
            changed[0] = view;
            assert!(
                VarBinView::with_buffer_handles_in(
                    source.as_view(),
                    BufferHandle::new_host(Buffer::copy_from(changed).into_byte_buffer()),
                    source.data_buffers().iter().cloned(),
                    &session.allocator(),
                )
                .is_err(),
                "buffer replacement accepted {case}"
            );
            assert_eq!(memory.snapshot().reserved_bytes, table);
            assert_eq!(Arc::as_ptr(output.data_buffers()), identity);
            assert_eq!(output.bytes_at(0), input.bytes_at(0));
        }
    }
    // Identical views cannot certify changed payload bytes, including null slots.
    for validity in [Validity::NonNullable, Validity::AllInvalid] {
        let source = output
            .with_views_and_validity(
                output.views_handle().clone(),
                DType::Utf8(validity.nullability()),
                validity,
            )
            .unwrap();
        for change in 0..3 {
            let mut bytes = source.data_buffers()[0].as_host().as_slice().to_vec();
            match change {
                0 => bytes[5] = 0xff,
                1 => bytes[0] = b'X',
                _ => bytes.truncate(13),
            }
            assert!(
                VarBinView::with_buffer_handles_in(
                    source.as_view(),
                    source.views_handle().clone(),
                    std::iter::once(BufferHandle::new_host(ByteBuffer::from(bytes))),
                    &session.allocator(),
                )
                .is_err()
            );
            assert_eq!(memory.snapshot().reserved_bytes, table);
        }
        assert!(
            VarBinView::with_buffer_handles_in(
                source.as_view(),
                source.views_handle().clone(),
                std::iter::empty(),
                &session.allocator(),
            )
            .is_err()
        );
        // A valid change is accepted through ordinary validation, rather than
        // making byte equality a new restriction on buffer replacement.
        let mut bytes = source.data_buffers()[0].as_host().as_slice().to_vec();
        bytes[5] = b'X';
        let changed = VarBinViewArray::try_from_parts(
            VarBinView::with_buffer_handles_in(
                source.as_view(),
                source.views_handle().clone(),
                std::iter::once(BufferHandle::new_host(ByteBuffer::from(bytes))),
                &session.allocator(),
            )
            .unwrap(),
        )
        .unwrap();
        assert_eq!(changed.bytes_at(0)[5], b'X');
        assert_eq!(source.bytes_at(0), input.bytes_at(0));
        assert_eq!(memory.snapshot().reserved_bytes, 2 * table);
        drop(changed);
        assert_eq!(memory.snapshot().reserved_bytes, table);
    }
    drop(output);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
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
    let taken_with_nulls = array
        .take(PrimitiveArray::from_option_iter([Some(2u32), None, Some(0), Some(2)]).into_array())
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
    for (result, expected) in [
        (slice, vec![Some(1)]),
        (cast, vec![Some(0), Some(1), Some(2)]),
        (filtered, vec![Some(0), Some(2)]),
        (taken, vec![Some(2), Some(0)]),
        (taken_with_nulls, vec![Some(2), None, Some(0), Some(2)]),
        (canonical, vec![Some(0), Some(1), Some(2)]),
        (recursive, vec![Some(0), Some(1), Some(2)]),
    ] {
        let result = result.execute::<VarBinViewArray>(&mut ctx).unwrap();
        assert_eq!(Arc::as_ptr(result.data_buffers()), identity);
        assert_eq!(result.metadata_capacity_bytes() as u64, table);
        assert_eq!(result.len(), expected.len());
        let valid = result
            .validity()
            .unwrap()
            .execute_mask(result.len(), &mut ctx)
            .unwrap();
        for (row, source) in expected.into_iter().enumerate() {
            assert_eq!(valid.value(row), source.is_some());
            if let Some(source) = source {
                assert_eq!(result.bytes_at(row), input.bytes_at(source));
            } else {
                assert!(result.bytes_at(row).is_empty());
            }
        }
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

    // Rebinding byte-identical handles uses the requested table allocator while
    // individual exported payload/view buffers retain their independent owners.
    // Exercise both shared byte storage and independently copied equal bytes.
    for copied in [false, true] {
        let source_memory = LiveMemoryPool::new(1 << 20).unwrap();
        let source_session = self::session(&source_memory);
        let source = admit(&input, &source_session).unwrap();
        let receiving_memory = LiveMemoryPool::new(2 * table).unwrap();
        let receiving_session = self::session(&receiving_memory);
        let buffer_memory = LiveMemoryPool::new(1 << 20).unwrap();
        let view_bytes = source.views_handle().len() as u64;
        let payload_bytes = source.data_buffers()[0].len() as u64;
        let views = if copied {
            Buffer::copy_from(source.views()).into_byte_buffer()
        } else {
            source.views_handle().as_host().clone()
        };
        let payload = if copied {
            ByteBuffer::copy_from(source.data_buffers()[0].as_host().as_slice())
        } else {
            source.data_buffers()[0].as_host().clone()
        };
        let views = crate::owned_buffers::retain_credit(
            views,
            crate::owned_buffers::reserve(&buffer_memory, view_bytes).unwrap(),
        );
        let payload = crate::owned_buffers::retain_credit(
            payload,
            crate::owned_buffers::reserve(&buffer_memory, payload_bytes).unwrap(),
        );
        let replacement = VarBinViewArray::try_from_parts(
            VarBinView::with_buffer_handles_in(
                source.as_view(),
                BufferHandle::new_host(views),
                std::iter::once(BufferHandle::new_host(payload)),
                &receiving_session.allocator(),
            )
            .unwrap(),
        )
        .unwrap();
        assert!(!Arc::ptr_eq(
            source.data_buffers(),
            replacement.data_buffers()
        ));
        assert_eq!(source_memory.snapshot().reserved_bytes, table);
        assert_eq!(receiving_memory.snapshot().reserved_bytes, table);
        assert_eq!(receiving_memory.snapshot().peak_reserved_bytes, 2 * table);
        for row in 0..source.len() {
            assert_eq!(replacement.bytes_at(row), source.bytes_at(row));
        }
        let error = VarBinView::with_buffer_handles_in(
            source.as_view(),
            replacement.views_handle().clone(),
            replacement.data_buffers().iter().cloned(),
            &receiving_session.allocator(),
        )
        .err()
        .unwrap();
        assert!(crate::owned_buffers::is_owned_reservation_denial(&error));
        assert_eq!(receiving_memory.snapshot().reserved_bytes, table);
        drop((source, source_session));
        assert_eq!(source_memory.snapshot().reserved_bytes, 0);
        let payload = replacement.data_buffers()[0].clone();
        let views = replacement.views_handle().clone();
        drop((replacement, receiving_session));
        assert_eq!(receiving_memory.snapshot().reserved_bytes, 0);
        assert_eq!(
            buffer_memory.snapshot().reserved_bytes,
            view_bytes + payload_bytes
        );
        drop(payload);
        assert_eq!(buffer_memory.snapshot().reserved_bytes, view_bytes);
        drop(views);
        assert_eq!(buffer_memory.snapshot().reserved_bytes, 0);
    }
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
                BufferHandle::new_host(ByteBuffer::copy_from([1u8])),
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
    // Identical view bytes may reuse content validation, even with a different
    // buffer owner, but cannot bypass dtype/nullability/validity-length checks.
    for views in [
        output.views_handle().clone(),
        BufferHandle::new_host(ByteBuffer::copy_from(
            output.views_handle().as_host().as_slice(),
        )),
    ] {
        for len in [output.len() - 1, output.len() + 1] {
            let validity = Validity::Array(BoolArray::from_iter(vec![false; len]).into_array());
            let error = output
                .with_views_and_validity(views.clone(), output.dtype().as_nullable(), validity)
                .unwrap_err();
            assert!(error.to_string().contains("validity length differs"));
        }
        assert!(
            output
                .with_views_and_validity(views.clone(), output.dtype().clone(), Validity::AllValid)
                .is_err()
        );
        let all_null = output
            .with_views_and_validity(views, output.dtype().as_nullable(), Validity::AllInvalid)
            .unwrap();
        let exposed = all_null
            .with_views_and_validity(
                all_null.views_handle().clone(),
                output.dtype().clone(),
                Validity::NonNullable,
            )
            .unwrap();
        for row in 0..output.len() {
            assert_eq!(exposed.bytes_at(row), input.bytes_at(row));
        }
        assert!(Arc::ptr_eq(exposed.data_buffers(), output.data_buffers()));
    }
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
