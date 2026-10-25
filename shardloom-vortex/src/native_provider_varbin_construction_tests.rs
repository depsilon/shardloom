// SPDX-License-Identifier: Apache-2.0
//! Finite buffer-table growth, finalization, and native view construction.

use super::*;
use std::mem::size_of;
use vortex::{
    VortexSessionDefault as _,
    array::{
        VortexSessionExecute as _,
        arrays::{
            BoolArray,
            varbinview::{
                BinaryView,
                build_views::{MAX_BUFFER_LEN, build_views_from_offsets_in, build_views_in},
            },
        },
        builders::VarBinViewBuilder,
        dtype::{Nullability, PType},
        memory::HostMemorySessionExt as _,
    },
    buffer::{Buffer, ByteBuffer},
};

fn session(memory: &LiveMemoryPool) -> VortexSession {
    VortexSession::default().with_host_allocator(Arc::new(
        crate::owned_buffers::ReservedHostAllocator::new(memory.clone()),
    ))
}

fn raw() -> VarBinViewArray {
    VarBinViewArray::from_iter_str(["outlined λ value with enough bytes", "tiny", ""])
}

#[test]
fn native_varbin_safe_builders_validate_utf8_per_value_and_refund() {
    let cases = [
        (vec![0xff], vec![1u32]),
        ([b'a'; 13].into_iter().chain([0xff]).collect(), vec![14]),
        ("λ猫".as_bytes().to_vec(), vec![1, 4]),
        ("λ猫".as_bytes().to_vec(), vec![0, 1, 4]),
        ("λ猫".as_bytes().to_vec(), vec![1, 0, 4]),
        ("λ".as_bytes().to_vec(), vec![1, 1, 0]),
        ("🙂".as_bytes().to_vec(), vec![3, 0, 1]),
        (
            [
                b"aaaaaaaaaaaaa".as_slice(),
                "λ".as_bytes(),
                b"bbbbbbbbbbbbb",
            ]
            .concat(),
            vec![14, 14],
        ),
    ];
    for (bytes, lengths) in cases {
        let memory = LiveMemoryPool::new(1 << 20).unwrap();
        let session = session(&memory);
        let mut ctx = session.create_execution_ctx();
        let offsets: Vec<u32> = std::iter::once(0)
            .chain(lengths.iter().scan(0, |sum, length| {
                *sum += length;
                Some(*sum)
            }))
            .collect();
        let rolling_limit = usize::try_from(*lengths.iter().max().unwrap()).unwrap();
        for (binary, from_offsets, max, validity) in [false, true].into_iter().flat_map(|binary| {
            [false, true].into_iter().flat_map(move |from_offsets| {
                [rolling_limit, MAX_BUFFER_LEN]
                    .into_iter()
                    .flat_map(move |max| {
                        [Validity::NonNullable, Validity::AllInvalid]
                            .map(|validity| (binary, from_offsets, max, validity))
                    })
            })
        }) {
            let all_invalid = matches!(&validity, Validity::AllInvalid);
            let dtype = if binary {
                DType::Binary(validity.nullability())
            } else {
                DType::Utf8(validity.nullability())
            };
            let bytes = ByteBuffer::copy_from(&bytes);
            let result = if from_offsets {
                build_views_from_offsets_in(max, bytes.clone(), &offsets, dtype, validity, &mut ctx)
            } else {
                build_views_in(max, bytes.clone(), &lengths, dtype, validity, &mut ctx)
            };
            if all_invalid {
                let output = result.unwrap();
                for row in 0..output.len() {
                    assert!(output.execute_scalar(row, &mut ctx).unwrap().is_null());
                    assert!(output.bytes_at(row).is_empty());
                }
                drop(output);
            } else if binary {
                let output = result.unwrap();
                for (row, range) in offsets.windows(2).enumerate() {
                    assert_eq!(
                        output.bytes_at(row).as_slice(),
                        &bytes[range[0] as usize..range[1] as usize]
                    );
                }
                drop(output);
            } else {
                let error = result.unwrap_err();
                assert!(error.to_string().contains("utf-8"), "{error}");
            }
            assert_eq!(memory.snapshot().reserved_bytes, 0);
        }
    }
}

#[test]
fn native_varbin_builder_exact_finalization_overlap_and_terminal_refund() {
    let input = raw();
    assert_eq!(input.data_buffers().len(), 1);
    let staging = size_of::<ByteBuffer>() as u64;
    let table = size_of::<BufferHandle>() as u64;
    let peak = staging + 2 * table;
    for grant in [peak - 1, peak] {
        let memory = LiveMemoryPool::new(grant).unwrap();
        let session = session(&memory);
        let mut builder = VarBinViewBuilder::with_admitted_buffers(
            input.dtype().clone(),
            input.len(),
            &session.host_allocator(),
        )
        .unwrap();
        builder
            .append_array(&input, &mut session.create_execution_ctx())
            .unwrap();
        assert_eq!(memory.snapshot().reserved_bytes, staging);
        let result = builder.finish();
        if grant == peak {
            let output = result.unwrap();
            assert_eq!(memory.snapshot().peak_reserved_bytes, peak);
            assert_eq!(memory.snapshot().reserved_bytes, table);
            for row in 0..input.len() {
                assert_eq!(output.bytes_at(row), input.bytes_at(row));
            }
            drop(output);
        } else {
            assert!(crate::owned_buffers::is_owned_reservation_denial(
                &result.unwrap_err()
            ));
            assert_eq!(memory.snapshot().peak_reserved_bytes, staging + table);
        }
        assert_eq!(memory.snapshot().reserved_bytes, 0);
    }
}

#[test]
fn native_varbin_builder_failed_growth_keeps_contents_and_can_retry() {
    let input = raw();
    let staging = size_of::<ByteBuffer>() as u64;
    let table = size_of::<BufferHandle>() as u64;
    let grant = 2 * staging + 4 * table;
    let memory = LiveMemoryPool::new(grant).unwrap();
    let session = session(&memory);
    let mut ctx = session.create_execution_ctx();
    let mut builder = VarBinViewBuilder::with_admitted_buffers(
        input.dtype().clone(),
        2 * input.len(),
        &session.host_allocator(),
    )
    .unwrap();
    builder.append_array(&input, &mut ctx).unwrap();
    let competing = crate::owned_buffers::reserve(&memory, grant - 2 * staging).unwrap();
    let before = memory.snapshot().reserved_bytes;
    let error = builder.append_array(&input, &mut ctx).unwrap_err();
    assert!(crate::owned_buffers::is_owned_reservation_denial(&error));
    assert_eq!(memory.snapshot().reserved_bytes, before);
    drop(competing);
    assert_eq!(memory.snapshot().reserved_bytes, staging);
    builder.append_array(&input, &mut ctx).unwrap();
    assert_eq!(memory.snapshot().reserved_bytes, 2 * staging);
    let error = builder.append_array(&input, &mut ctx).unwrap_err();
    assert!(error.to_string().contains("admitted rows"));
    assert_eq!(memory.snapshot().reserved_bytes, 2 * staging);
    let output = builder.finish().unwrap();
    assert_eq!(output.len(), 2 * input.len());
    for row in 0..output.len() {
        assert_eq!(output.bytes_at(row), input.bytes_at(row % input.len()));
    }
    assert_eq!(memory.snapshot().reserved_bytes, 2 * table);
    assert_eq!(memory.snapshot().peak_reserved_bytes, grant);
    drop(output);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

#[test]
fn native_varbin_builder_rejects_dtype_before_allocation() {
    let memory = LiveMemoryPool::new(1 << 20).unwrap();
    let session = session(&memory);
    let result = VarBinViewBuilder::with_admitted_buffers(
        DType::Primitive(PType::I64, Nullability::NonNullable),
        3,
        &session.host_allocator(),
    );
    assert!(
        result
            .err()
            .unwrap()
            .to_string()
            .contains("dtype must be binary or utf8")
    );
    assert_eq!(memory.snapshot().peak_reserved_bytes, 0);
    let result = VarBinViewBuilder::with_admitted_buffers(
        DType::Utf8(Nullability::NonNullable),
        usize::MAX / 16,
        &session.host_allocator(),
    );
    assert!(
        result
            .err()
            .unwrap()
            .to_string()
            .contains("view capacity overflow")
    );
    assert_eq!(memory.snapshot().peak_reserved_bytes, 0);
}

#[test]
fn native_varbin_builder_many_nullable_utf8_and_binary_leaves() {
    for binary in [false, true] {
        let bytes = [0xff, 0, 0x80, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14];
        let input = if binary {
            VarBinViewArray::from_iter_nullable_bin([Some(bytes.as_slice()), None, Some(&[])])
        } else {
            VarBinViewArray::from_iter_nullable_str([
                Some("many outlined λ 猫 values"),
                None,
                Some(""),
            ])
        };
        let memory = LiveMemoryPool::new(1 << 20).unwrap();
        let session = session(&memory);
        let mut ctx = session.create_execution_ctx();
        let mut builder = VarBinViewBuilder::with_admitted_buffers(
            input.dtype().clone(),
            33 * input.len(),
            &session.host_allocator(),
        )
        .unwrap();
        for _ in 0..33 {
            builder.append_array(&input, &mut ctx).unwrap();
        }
        let output = builder.finish().unwrap();
        let valid = output
            .validity()
            .unwrap()
            .execute_mask(output.len(), &mut ctx)
            .unwrap();
        assert_eq!(output.data_buffers().len(), 33);
        assert_eq!(
            output.metadata_capacity_bytes(),
            33 * size_of::<BufferHandle>()
        );
        assert_eq!(
            memory.snapshot().reserved_bytes,
            output.metadata_capacity_bytes() as u64
        );
        for row in 0..output.len() {
            assert_eq!(valid.value(row), row % 3 != 1);
            if row % 3 != 1 {
                assert_eq!(output.bytes_at(row), input.bytes_at(row % 3));
            }
        }
        drop(output);
        assert_eq!(memory.snapshot().reserved_bytes, 0);
    }
}

#[test]
fn native_varbin_view_construction_reuses_loops_for_rollover_and_offsets() {
    let values: [&[u8]; 5] = [
        "λ猫🙂".as_bytes(),
        b"tiny",
        "outlined λ 猫 value".as_bytes(),
        b"",
        "second outlined 🙂".as_bytes(),
    ];
    let lengths: Vec<u32> = values
        .iter()
        .map(|value| u32::try_from(value.len()).unwrap())
        .collect();
    let bytes: Vec<u8> = values
        .iter()
        .flat_map(|value| value.iter().copied())
        .collect();
    let mut offsets = vec![3u32];
    for len in &lengths {
        offsets.push(offsets.last().unwrap() + len);
    }
    let surrounded = [b"pre".as_slice(), &bytes, b"suffix"].concat();
    for max in [24, MAX_BUFFER_LEN] {
        for from_offsets in [false, true] {
            let memory = LiveMemoryPool::new(1 << 20).unwrap();
            let session = session(&memory);
            let mut ctx = session.create_execution_ctx();
            let dtype = DType::Utf8(Nullability::Nullable);
            let validity =
                Validity::Array(BoolArray::from_iter([true, false, true, true, true]).into_array());
            let output = if from_offsets {
                build_views_from_offsets_in(
                    max,
                    ByteBuffer::copy_from(&surrounded),
                    &offsets,
                    dtype,
                    validity,
                    &mut ctx,
                )
            } else {
                build_views_in(
                    max,
                    ByteBuffer::copy_from(&bytes),
                    &lengths,
                    dtype,
                    validity,
                    &mut ctx,
                )
            }
            .unwrap();
            assert_eq!(output.data_buffers().len(), if max == 24 { 3 } else { 1 });
            for (row, value) in values.iter().enumerate() {
                if row != 1 {
                    assert_eq!(output.bytes_at(row).as_slice(), *value);
                }
            }
            let valid = output
                .validity()
                .unwrap()
                .execute_mask(5, &mut ctx)
                .unwrap();
            assert!(!valid.value(1));
            let count = output.data_buffers().len() as u64;
            assert_eq!(
                memory.snapshot().reserved_bytes,
                count * size_of::<BufferHandle>() as u64
            );
            assert_eq!(
                memory.snapshot().peak_reserved_bytes,
                count * (size_of::<ByteBuffer>() + 2 * size_of::<BufferHandle>()) as u64
            );
            drop(output);
            assert_eq!(memory.snapshot().reserved_bytes, 0);
        }
    }
}

#[test]
fn native_varbin_view_construction_empty_null_binary_and_malformed_inputs() {
    let memory = LiveMemoryPool::new(1 << 20).unwrap();
    let session = session(&memory);
    let mut ctx = session.create_execution_ctx();
    for dtype in [
        DType::Binary(Nullability::Nullable),
        DType::Utf8(Nullability::Nullable),
    ] {
        for (lengths, validity) in [
            (vec![], Validity::AllValid),
            (vec![0u32; 3], Validity::AllInvalid),
        ] {
            let output = build_views_in(
                0,
                ByteBuffer::empty(),
                &lengths,
                dtype.clone(),
                validity,
                &mut ctx,
            )
            .unwrap();
            assert_eq!(output.len(), lengths.len());
            assert!(output.data_buffers().is_empty());
            assert_eq!(output.metadata_capacity_bytes(), 0);
        }
    }
    for validity in [
        Validity::NonNullable,
        Validity::Array(BoolArray::from_iter([true, false]).into_array()),
    ] {
        let invalid = build_views_in(
            MAX_BUFFER_LEN,
            ByteBuffer::copy_from("λ猫".as_bytes()),
            &[5u32],
            DType::Utf8(Nullability::Nullable),
            validity,
            &mut ctx,
        );
        assert!(invalid.is_err());
        assert_eq!(memory.snapshot().reserved_bytes, 0);
    }
    let dtype = DType::Binary(Nullability::Nullable);
    let bytes = ByteBuffer::copy_from([0xff, 0x80, 0]);
    let output = build_views_in(
        3,
        bytes.clone(),
        &[3u32],
        dtype.clone(),
        Validity::AllValid,
        &mut ctx,
    )
    .unwrap();
    assert_eq!(output.bytes_at(0).as_slice(), bytes.as_slice());
    drop(output);
    for (max, lengths) in [
        (0, vec![3u32]),
        (2, vec![3]),
        (3, vec![2]),
        (3, vec![4]),
        (MAX_BUFFER_LEN + 1, vec![3]),
    ] {
        assert!(
            build_views_in(
                max,
                bytes.clone(),
                &lengths,
                dtype.clone(),
                Validity::AllValid,
                &mut ctx
            )
            .is_err()
        );
        assert_eq!(memory.snapshot().reserved_bytes, 0);
    }
    for offsets in [vec![], vec![0, 4], vec![2, 1], vec![0, 2, 1, 3]] {
        assert!(
            build_views_from_offsets_in(
                3,
                bytes.clone(),
                &offsets,
                dtype.clone(),
                Validity::AllValid,
                &mut ctx
            )
            .is_err()
        );
        assert_eq!(memory.snapshot().reserved_bytes, 0);
    }
    // Host constructors keep their UTF-8/value validation boundary before table admission.
    let views: Buffer<BinaryView> = [BinaryView::make_view(&[0xff], 0, 0)].into_iter().collect();
    assert!(
        VarBinViewArray::try_new_from_buffers(
            views,
            &[],
            DType::Utf8(Nullability::NonNullable),
            Validity::NonNullable,
            &mut ctx
        )
        .is_err()
    );
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}
