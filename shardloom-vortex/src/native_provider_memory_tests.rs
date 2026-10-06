use super::*;
use vortex::{
    VortexSessionDefault as _,
    array::{VortexSessionExecute as _, arrays::VarBinArray, dtype::Nullability},
    encodings::fsst::{FSSTArrayExt as _, fsst_compress, fsst_train_compressor},
};

fn fixture(values: &[Option<&str>]) -> FSSTArray {
    let session = VortexSession::default();
    let mut ctx = session.create_execution_ctx();
    let input = VarBinArray::from_iter(values.iter().copied(), DType::Utf8(Nullability::Nullable))
        .into_array();
    let compressor = fsst_train_compressor(&input, &mut ctx).unwrap();
    fsst_compress(&input, &compressor, &mut ctx).unwrap()
}

fn session(memory: &LiveMemoryPool) -> VortexSession {
    let session = VortexSession::default();
    install(&session, memory.clone());
    session
}

#[test]
fn native_fsst_reserves_before_decode_and_keeps_credit_with_escaped_buffers() {
    let expected = [
        Some("a long repeated string with λ values"),
        None,
        Some("tiny"),
        Some(""),
    ];
    let encoded = fixture(&expected);
    let memory = LiveMemoryPool::new(1 << 20).unwrap();
    let session = session(&memory);
    let output = encoded
        .into_array()
        .execute::<VarBinViewArray>(&mut session.create_execution_ctx())
        .unwrap();
    let mut ctx = session.create_execution_ctx();
    let valid = output
        .validity()
        .unwrap()
        .execute_mask(output.len(), &mut ctx)
        .unwrap();
    for (index, expected) in expected.iter().enumerate() {
        assert_eq!(valid.value(index), expected.is_some());
        if let Some(expected) = expected {
            assert_eq!(output.bytes_at(index).as_slice(), expected.as_bytes());
        }
    }
    let used = memory.snapshot().reserved_bytes;
    assert!(used >= 16 * output.len() as u64 + expected[0].unwrap().len() as u64);
    let view_bytes = capacity(mul(output.len(), 16).unwrap()).unwrap();
    let views = output.views_handle().as_host().clone();
    let data = output.data_buffers()[0].as_host().clone();
    let pointer = data.as_ptr();
    let slice = data.slice(1..3);
    drop(output);
    drop(session);
    assert_eq!(memory.snapshot().reserved_bytes, used);
    assert_eq!(slice.as_ptr(), pointer.wrapping_add(1));
    drop(data);
    drop(slice);
    assert_eq!(memory.snapshot().reserved_bytes, view_bytes);
    drop(views);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

#[test]
fn native_fsst_denial_and_invalid_metadata_release_all_credit() {
    let encoded = fixture(&[
        Some("a repeated and sufficiently long native value"),
        Some("another value"),
    ]);
    let memory = LiveMemoryPool::new(64).unwrap();
    let session = session(&memory);
    let error = encoded
        .clone()
        .into_array()
        .execute::<VarBinViewArray>(&mut session.create_execution_ctx())
        .unwrap_err();
    assert!(crate::owned_buffers::is_owned_reservation_denial(&error));
    assert_eq!(memory.snapshot().reserved_bytes, 0);
    // The output admission fails before the pinned decoder has allocated bytes.
    assert_eq!(memory.snapshot().peak_reserved_bytes, 0);

    let memory = LiveMemoryPool::new(1 << 20).unwrap();
    let session = self::session(&memory);
    for lengths in [[-1i64, 3], [i64::MAX, 1], [1, 1]] {
        let mut ctx = session.create_execution_ctx();
        let malformed = FSST::try_new_with_symbol_table(
            encoded.dtype().clone(),
            encoded.symbol_table(),
            encoded.codes(),
            PrimitiveArray::from_iter(lengths).into_array(),
            &mut ctx,
        )
        .unwrap();
        assert!(
            malformed
                .into_array()
                .execute::<VarBinViewArray>(&mut ctx)
                .is_err()
        );
        assert_eq!(memory.snapshot().reserved_bytes, 0);
    }
}

#[test]
fn native_fsst_inline_null_empty_and_sliced_values_preserve_ownership() {
    for values in [
        vec![Some(""), Some("a"), Some("λ"), None],
        vec![None, None],
        vec![],
    ] {
        let encoded = fixture(&values);
        let memory = LiveMemoryPool::new(1 << 20).unwrap();
        let session = session(&memory);
        for start in 0..=values.len() {
            let array = encoded
                .clone()
                .into_array()
                .slice(start..values.len())
                .unwrap();
            let output = array
                .execute::<VarBinViewArray>(&mut session.create_execution_ctx())
                .unwrap();
            assert_eq!(output.len(), values.len() - start);
            let valid = output
                .validity()
                .unwrap()
                .execute_mask(output.len(), &mut session.create_execution_ctx())
                .unwrap();
            for (index, expected) in values[start..].iter().enumerate() {
                assert_eq!(valid.value(index), expected.is_some());
                if let Some(expected) = expected {
                    assert_eq!(output.bytes_at(index).as_slice(), expected.as_bytes());
                }
            }
            drop(output);
            assert_eq!(memory.snapshot().reserved_bytes, 0);
        }
    }
}

#[test]
#[cfg(all(feature = "vortex-write", unix))]
fn native_fsst_spill_sessions_charge_their_own_provider_pool() {
    let parent_memory = LiveMemoryPool::new(1 << 20).unwrap();
    let parent = session(&parent_memory);
    let child_memory = LiveMemoryPool::new(1 << 20).unwrap();
    let child = crate::native_spill_session::with_memory(&parent, child_memory.clone());
    let encoded = fixture(&[Some("a long value owned only by the spill decoder")]);
    let output = encoded
        .into_array()
        .execute::<VarBinViewArray>(&mut child.create_execution_ctx())
        .unwrap();
    assert_eq!(parent_memory.snapshot().reserved_bytes, 0);
    assert!(child_memory.snapshot().reserved_bytes > 0);
    drop(output);
    assert_eq!(child_memory.snapshot().reserved_bytes, 0);
}

fn integer_fixture(array: &PrimitiveArray, kind: usize, ctx: &mut ExecutionCtx) -> ArrayRef {
    use vortex::{
        array::{
            arrays::{DictArray, SliceArray},
            patches::Patches,
            validity::Validity,
        },
        buffer::Buffer,
        encodings::{
            fastlanes::{BitPackedData, FoRData},
            zigzag::zigzag_encode,
        },
    };
    match kind {
        0 => BitPackedData::encode(&array.clone().into_array(), 4, ctx)
            .unwrap()
            .into_array(),
        1 => FoRData::encode(array.clone(), ctx).unwrap().into_array(),
        2 => Delta::try_from_primitive_array(array, ctx)
            .unwrap()
            .into_array(),
        3 => zigzag_encode(array.as_view()).unwrap().into_array(),
        5 | 11 => {
            let values = if kind == 11 {
                PrimitiveArray::new(
                    Buffer::copy_from_aligned(array.as_slice::<i32>(), Alignment::new(4096)),
                    Validity::NonNullable,
                )
                .into_array()
            } else {
                array.clone().into_array()
            };
            DictArray::try_new(
                PrimitiveArray::from_iter((0..array.len()).map(|row| u32::try_from(row).unwrap()))
                    .into_array(),
                values,
            )
            .unwrap()
            .into_array()
        }
        6 | 10 => {
            let offset = if kind == 10 { 7 } else { 0 };
            let indices = PrimitiveArray::from_iter(
                (offset..offset + array.len()).map(|row| u64::try_from(row).unwrap()),
            )
            .into_array();
            Sparse::try_new_from_patches(
                Patches::new(
                    array.len(),
                    offset,
                    indices,
                    array.clone().into_array(),
                    None,
                )
                .unwrap(),
                0i32.into(),
            )
            .unwrap()
            .into_array()
        }
        7 => RunEnd::encode(array.clone().into_array(), ctx)
            .unwrap()
            .into_array(),
        8 => RLE::encode(array.as_view(), ctx).unwrap().into_array(),
        9 => {
            let values = array.as_slice::<i32>();
            Sequence::try_new_typed(
                values[0],
                values[1] - values[0],
                Nullability::NonNullable,
                values.len(),
            )
            .unwrap()
            .into_array()
        }
        12 => {
            let values = std::iter::once(0i32)
                .chain(array.as_slice::<i32>().iter().copied())
                .chain(std::iter::once(0));
            let input =
                BitPackedData::encode(&PrimitiveArray::from_iter(values).into_array(), 4, ctx)
                    .unwrap()
                    .into_array();
            SliceArray::try_new(input, 1..array.len() + 1)
                .unwrap()
                .into_array()
        }
        13 => sliced_rle_fixture(array, ctx),
        _ => unreachable!(),
    }
}

fn sliced_rle_fixture(array: &PrimitiveArray, ctx: &mut ExecutionCtx) -> ArrayRef {
    let values = std::iter::repeat_n(0i32, 1010)
        .chain(array.as_slice::<i32>().iter().copied())
        .chain(std::iter::repeat_n(0, 17));
    let input = PrimitiveArray::from_iter(values);
    RLE::encode(input.as_view(), ctx)
        .unwrap()
        .into_array()
        .slice(1010..1010 + array.len())
        .unwrap()
}

#[test]
fn native_fsst_admits_each_reviewed_integer_metadata_provider_before_decode() {
    use vortex::array::arrays::ConstantArray;
    let expected = "same long UTF-8 value λ repeated through every integer provider";
    let encoded = fixture(&vec![Some(expected); 33]);
    let control = VortexSession::default();
    let mut ctx = control.create_execution_ctx();
    let lengths = encoded
        .uncompressed_lengths()
        .clone()
        .execute::<PrimitiveArray>(&mut ctx)
        .unwrap();
    let offsets = encoded
        .codes_offsets()
        .clone()
        .execute::<PrimitiveArray>(&mut ctx)
        .unwrap();
    for kind in 0..14 {
        let (lengths, offsets) = if kind == 4 {
            (
                ConstantArray::new(i32::try_from(expected.len()).unwrap(), 33).into_array(),
                offsets.clone().into_array(),
            )
        } else {
            (
                integer_fixture(&lengths, kind, &mut ctx),
                integer_fixture(&offsets, kind, &mut ctx),
            )
        };
        let scratch = add(
            integer_workspace(&lengths, 0).unwrap().bytes,
            integer_workspace(&offsets, 0).unwrap().bytes,
        )
        .unwrap();
        assert!(scratch > 0);
        if kind == 2 {
            // Both 33- and 34-row arrays decode a complete 1024-row extent.
            assert_eq!(scratch, 2 * (1024 * 4 + 256));
        }
        if kind == 11 {
            // A gather followed by realignment can retain two output buffers.
            assert_eq!(scratch, (33 + 34) * 4 * 2 + 2 * (4096 + 256));
        }
        let slots = FSSTSlots {
            uncompressed_lengths: lengths,
            codes_offsets: offsets,
            codes_validity: encoded.codes_validity().cloned(),
        };
        let input = Array::<FSST>::try_from_parts(
            ArrayParts::new(
                FSST,
                encoded.dtype().clone(),
                encoded.len(),
                encoded.data().clone(),
            )
            .with_slots(slots.into_slots()),
        )
        .unwrap();
        let denied = LiveMemoryPool::new(scratch - 1).unwrap();
        // Exercise this exact metadata tree: the outer executor can reduce a
        // Slice child before entering the FSST provider, lowering its bound.
        let error = decode_fsst(
            &input,
            &mut session(&denied).create_execution_ctx(),
            &denied,
        )
        .unwrap_err();
        assert!(
            crate::owned_buffers::is_owned_reservation_denial(&error),
            "{kind}: {error}"
        );
        assert_eq!(denied.snapshot().peak_reserved_bytes, 0);
        assert_eq!(denied.snapshot().reserved_bytes, 0);
        let memory = LiveMemoryPool::new(1 << 20).unwrap();
        let output = decode_fsst(
            &input,
            &mut session(&memory).create_execution_ctx(),
            &memory,
        )
        .unwrap();
        let output = output.try_downcast::<VarBinView>().unwrap();
        for row in 0..output.len() {
            assert_eq!(output.bytes_at(row).as_slice(), expected.as_bytes());
        }
        assert_eq!(
            memory.snapshot().peak_reserved_bytes,
            memory.snapshot().reserved_bytes + scratch
        );
        drop(output);
        assert_eq!(memory.snapshot().reserved_bytes, 0);
    }
}

#[test]
fn native_fsst_filtered_metadata_does_not_leave_unowned_mask_caches() {
    use vortex::{
        array::arrays::{DictArray, FilterArray},
        mask::Mask,
    };
    let expected = "a native string decoded after lazy integer selection λ";
    let encoded = fixture(&vec![Some(expected); 17]);
    let mut ctx = VortexSession::default().create_execution_ctx();
    let lengths = encoded
        .uncompressed_lengths()
        .clone()
        .execute::<PrimitiveArray>(&mut ctx)
        .unwrap();
    let offsets = encoded
        .codes_offsets()
        .clone()
        .execute::<PrimitiveArray>(&mut ctx)
        .unwrap();
    for keep_every in [2, 9] {
        let mut masks = Vec::new();
        let mut filter = |array: &PrimitiveArray| {
            let expanded = array
                .as_slice::<i32>()
                .iter()
                .flat_map(|value| std::iter::repeat_n(i64::from(*value), keep_every));
            let values = PrimitiveArray::from_iter(expanded).into_array();
            let mask = Mask::from_iter((0..values.len()).map(|row| row % keep_every == 0));
            masks.push(mask.clone());
            let filtered = FilterArray::try_new(values, mask).unwrap().into_array();
            // A filter nested under another metadata provider must be isolated too.
            DictArray::try_new(
                PrimitiveArray::from_iter((0..array.len()).map(|row| u32::try_from(row).unwrap()))
                    .into_array(),
                filtered,
            )
            .unwrap()
            .into_array()
        };
        let slots = FSSTSlots {
            uncompressed_lengths: filter(&lengths),
            codes_offsets: filter(&offsets),
            codes_validity: encoded.codes_validity().cloned(),
        };
        let input = Array::<FSST>::try_from_parts(
            ArrayParts::new(
                FSST,
                encoded.dtype().clone(),
                encoded.len(),
                encoded.data().clone(),
            )
            .with_slots(slots.into_slots()),
        )
        .unwrap();
        let scratch = add(
            integer_workspace(input.uncompressed_lengths(), 0)
                .unwrap()
                .bytes,
            integer_workspace(input.codes_offsets(), 0).unwrap().bytes,
        )
        .unwrap();
        let denied = LiveMemoryPool::new(scratch - 1).unwrap();
        let error = decode_fsst(
            &input,
            &mut session(&denied).create_execution_ctx(),
            &denied,
        )
        .unwrap_err();
        assert!(crate::owned_buffers::is_owned_reservation_denial(&error));
        assert_eq!(denied.snapshot().peak_reserved_bytes, 0);
        let memory = LiveMemoryPool::new(1 << 20).unwrap();
        let output = input
            .into_array()
            .execute::<VarBinViewArray>(&mut session(&memory).create_execution_ctx())
            .unwrap();
        for row in 0..output.len() {
            assert_eq!(output.bytes_at(row).as_slice(), expected.as_bytes());
        }
        for mask in masks {
            let values = mask.values().unwrap();
            assert!(values.cached_indices().is_none());
            assert!(values.cached_slices().is_none());
        }
        drop(output);
        assert_eq!(memory.snapshot().reserved_bytes, 0);
    }
}

fn nullable_codes(kind: usize, present: &[bool]) -> ArrayRef {
    use vortex::{
        array::{
            arrays::{BoolArray, ConstantArray, DictArray, MaskedArray},
            scalar::Scalar,
        },
        buffer::{BitBuffer, Buffer, ByteBuffer},
    };
    let primitive = || {
        PrimitiveArray::from_option_iter(present.iter().map(|valid| valid.then_some(0i32)))
            .into_array()
    };
    match kind {
        0 => primitive(),
        1 => {
            let bits = present.iter().enumerate().fold(0u16, |bits, (row, valid)| {
                bits | (u16::from(*valid) << (row + 3))
            });
            let bitmap = BitBuffer::new_with_offset(
                ByteBuffer::copy_from_aligned(bits.to_le_bytes(), Alignment::new(4096)),
                present.len(),
                3,
            );
            let validity =
                Validity::Array(BoolArray::new(bitmap, Validity::NonNullable).into_array());
            PrimitiveArray::new(
                Buffer::copy_from_aligned(vec![0i32; present.len()], Alignment::new(4096)),
                validity,
            )
            .into_array()
        }
        2 => DictArray::try_new(primitive(), PrimitiveArray::from_iter([0i32]).into_array())
            .unwrap()
            .into_array(),
        3 => ConstantArray::new(
            Scalar::null(DType::Primitive(
                vortex::array::dtype::PType::I32,
                Nullability::Nullable,
            )),
            present.len(),
        )
        .into_array(),
        4 => ConstantArray::new(
            Scalar::primitive(0i32, Nullability::Nullable),
            present.len(),
        )
        .into_array(),
        5..=9 => {
            let values = PrimitiveArray::from_iter(vec![0i32; present.len()]);
            let mut ctx = VortexSession::default().create_execution_ctx();
            let child = match kind {
                5 => PrimitiveArray::new(
                    Buffer::copy_from_aligned(values.as_slice::<i32>(), Alignment::new(4096)),
                    Validity::NonNullable,
                )
                .into_array(),
                6 | 8 => integer_fixture(&values, 0, &mut ctx),
                7 => integer_fixture(&values, 5, &mut ctx),
                9 => PrimitiveArray::from_option_iter(vec![Some(0i32); present.len()]).into_array(),
                _ => unreachable!(),
            };
            let validity = if kind == 5 {
                // Preserve the complete over-aligned, bit-offset mask behind a slice.
                nullable_codes(1, present).validity().unwrap()
            } else {
                Validity::from_iter(present.iter().copied())
            };
            MaskedArray::try_new(child, validity).unwrap().into_array()
        }
        _ => unreachable!(),
    }
}

#[test]
fn native_fsst_nullable_gathers_admit_nested_indices_and_release_scratch() {
    use vortex::{
        array::{arrays::DictArray, builtins::ArrayBuiltins as _},
        buffer::Buffer,
    };
    let text = "a long native nullable dictionary string λ";
    for kind in 0..10 {
        let present = match kind {
            3 | 8 => vec![false; 7],
            4 | 9 => vec![true; 7],
            _ => vec![false, true, true, false, true, false, true],
        };
        let expected: Vec<_> = present.iter().map(|valid| valid.then_some(text)).collect();
        let encoded = fixture(&expected);
        let lengths = DictArray::try_new(
            nullable_codes(kind, &present),
            PrimitiveArray::new(
                Buffer::copy_from_aligned(
                    [i32::try_from(text.len()).unwrap()],
                    Alignment::new(4096),
                ),
                Validity::NonNullable,
            )
            .into_array(),
        )
        .unwrap()
        .into_array()
        .fill_null(0i32)
        .unwrap();
        let slots = FSSTSlots {
            uncompressed_lengths: lengths,
            codes_offsets: encoded.codes_offsets().clone(),
            codes_validity: encoded.codes_validity().cloned(),
        };
        let input = Array::<FSST>::try_from_parts(
            ArrayParts::new(
                FSST,
                encoded.dtype().clone(),
                encoded.len(),
                encoded.data().clone(),
            )
            .with_slots(slots.into_slots()),
        )
        .unwrap();
        let scratch = integer_workspace(input.uncompressed_lengths(), 0)
            .unwrap_or_else(|error| panic!("metadata fixture {kind}: {error}"))
            .bytes;
        let denied = LiveMemoryPool::new(scratch - 1).unwrap();
        let error = decode_fsst(
            &input,
            &mut session(&denied).create_execution_ctx(),
            &denied,
        )
        .unwrap_err();
        assert!(
            crate::owned_buffers::is_owned_reservation_denial(&error),
            "{kind}: {error}"
        );
        assert_eq!(denied.snapshot().peak_reserved_bytes, 0);
        let memory = LiveMemoryPool::new(1 << 20).unwrap();
        let mut ctx = session(&memory).create_execution_ctx();
        let output = decode_fsst(&input, &mut ctx, &memory)
            .unwrap()
            .try_downcast::<VarBinView>()
            .unwrap();
        let validity = output
            .validity()
            .unwrap()
            .execute_mask(output.len(), &mut ctx)
            .unwrap();
        for (row, expected) in expected.iter().enumerate() {
            assert_eq!(validity.value(row), expected.is_some());
            if let Some(expected) = expected {
                assert_eq!(output.bytes_at(row).as_slice(), expected.as_bytes());
            }
        }
        let total = expected
            .iter()
            .flatten()
            .map(|value| value.len() as u64)
            .sum::<u64>();
        let view_bytes = capacity(mul(output.len(), 16).unwrap()).unwrap();
        assert_eq!(
            memory.snapshot().peak_reserved_bytes,
            scratch + view_bytes + capacity(total + 7).unwrap(),
            "{kind}",
        );
        if kind == 3 {
            assert_eq!(memory.snapshot().reserved_bytes, view_bytes);
        }
        drop(output);
        assert_eq!(memory.snapshot().reserved_bytes, 0);
    }
}

#[test]
fn native_fsst_metadata_nullability_casts_preserve_checked_errors() {
    use vortex::array::dtype::PType;
    let child = PrimitiveArray::from_option_iter([Some(1i32), None]).into_array();
    let narrowed = Cast::new(
        child.clone(),
        DType::Primitive(PType::I32, Nullability::NonNullable),
    )
    .into_array();
    assert_eq!(integer_workspace(&narrowed, 0).unwrap().bytes, 0);
    let mut ctx = VortexSession::default().create_execution_ctx();
    assert!(decode_integer(&narrowed, &mut ctx, 0).is_err());
    let changed = Cast::new(
        child,
        DType::Primitive(PType::I64, Nullability::NonNullable),
    )
    .into_array();
    assert!(integer_workspace(&changed, 0).is_err());
}

#[test]
fn native_string_concatenation_retains_data_views_and_validity_independently() {
    use vortex::array::arrays::ChunkedArray;
    let first = [Some("first long FSST string λ"), None, Some("")];
    let last = [Some("last long native string λ"), Some("inline")];
    let middle = [None, Some("imported canonical string kept in place")];
    let dtype = DType::Utf8(Nullability::Nullable);
    let expected: Vec<_> = first.iter().chain(&middle).chain(&last).copied().collect();
    let nested = ChunkedArray::try_new(
        vec![
            VarBinViewArray::from_iter_nullable_str(middle).into_array(),
            fixture(&last).into_array(),
        ],
        dtype.clone(),
    )
    .unwrap()
    .into_array();
    let input = ChunkedArray::try_new(vec![fixture(&first).into_array(), nested], dtype)
        .unwrap()
        .into_array();
    let views_bytes = u64::try_from(16 * expected.len() + 16).unwrap();
    let bitmap_bytes = capacity(expected.len().div_ceil(8) as u64).unwrap();
    let denied = LiveMemoryPool::new(views_bytes + bitmap_bytes - 1).unwrap();
    let error = input
        .clone()
        .execute::<VarBinViewArray>(&mut session(&denied).create_execution_ctx())
        .unwrap_err();
    assert!(crate::owned_buffers::is_owned_reservation_denial(&error));
    assert_eq!(denied.snapshot().peak_reserved_bytes, 0);
    let memory = LiveMemoryPool::new(1 << 20).unwrap();
    let owner = session(&memory);
    let mut ctx = owner.create_execution_ctx();
    let output = input.execute::<VarBinViewArray>(&mut ctx).unwrap();
    let valid = output
        .validity()
        .unwrap()
        .execute_mask(output.len(), &mut ctx)
        .unwrap();
    for (row, expected) in expected.iter().enumerate() {
        assert_eq!(valid.value(row), expected.is_some());
        if let Some(expected) = expected {
            assert_eq!(output.bytes_at(row).as_slice(), expected.as_bytes());
        }
    }
    let payload_bytes = [&first[..], &last[..]]
        .iter()
        .map(|values| {
            capacity(
                values
                    .iter()
                    .flatten()
                    .map(|value| value.len() as u64)
                    .sum::<u64>()
                    + 7,
            )
            .unwrap()
        })
        .sum::<u64>();
    assert_eq!(
        memory.snapshot().reserved_bytes,
        views_bytes + bitmap_bytes + payload_bytes
    );
    let views = output.views_handle().as_host().clone();
    let data: Vec<_> = output
        .data_buffers()
        .iter()
        .map(|buffer| buffer.as_host().clone())
        .collect();
    let validity = output.validity().unwrap();
    drop(valid);
    drop(output);
    drop(ctx);
    drop(owner);
    assert_eq!(
        memory.snapshot().reserved_bytes,
        views_bytes + bitmap_bytes + payload_bytes
    );
    drop(data);
    assert_eq!(memory.snapshot().reserved_bytes, views_bytes + bitmap_bytes);
    drop(views);
    assert_eq!(memory.snapshot().reserved_bytes, bitmap_bytes);
    drop(validity);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

#[test]
fn native_string_concatenation_releases_earlier_chunks_on_late_decode_denial() {
    use vortex::array::arrays::ChunkedArray;
    let first = fixture(&[Some("first owned native value")]);
    let later_text = "large following chunk λ".repeat(100);
    let second = fixture(&[Some(&later_text)]);
    let dtype = first.dtype().clone();
    let input = ChunkedArray::try_new(vec![first.into_array(), second.into_array()], dtype)
        .unwrap()
        .into_array();
    let memory = LiveMemoryPool::new(1200).unwrap();
    let error = input
        .execute::<VarBinViewArray>(&mut session(&memory).create_execution_ctx())
        .unwrap_err();
    assert!(crate::owned_buffers::is_owned_reservation_denial(&error));
    assert!(memory.snapshot().peak_reserved_bytes > 500);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

#[test]
fn native_string_concatenation_preserves_binary_empty_and_null_chunks() {
    use vortex::array::arrays::ChunkedArray;
    let bytes = [0xffu8, 0x80, 0, 9, 8, 7, 6, 5, 4, 3, 2, 1, 0xfe, 0xfd];
    let cases = [
        vec![],
        vec![None, None],
        vec![Some(bytes.as_slice()), None, Some(b"".as_slice())],
    ];
    for values in cases {
        let dtype = DType::Binary(Nullability::Nullable);
        let control = VortexSession::default();
        let mut ctx = control.create_execution_ctx();
        let source = VarBinArray::from_iter(values.iter().copied(), dtype.clone()).into_array();
        let compressor = fsst_train_compressor(&source, &mut ctx).unwrap();
        let compressed = fsst_compress(&source, &compressor, &mut ctx)
            .unwrap()
            .into_array();
        let empty = VarBinViewArray::from_iter_nullable_bin(std::iter::empty::<Option<&[u8]>>())
            .into_array();
        let input = ChunkedArray::try_new(vec![empty, compressed], dtype.clone())
            .unwrap()
            .into_array();
        let memory = LiveMemoryPool::new(1 << 20).unwrap();
        let mut ctx = session(&memory).create_execution_ctx();
        let output = input.execute::<VarBinViewArray>(&mut ctx).unwrap();
        assert_eq!(output.dtype(), &dtype);
        let validity = output
            .validity()
            .unwrap()
            .execute_mask(output.len(), &mut ctx)
            .unwrap();
        for (row, expected) in values.iter().enumerate() {
            assert_eq!(validity.value(row), expected.is_some());
            if let Some(expected) = expected {
                assert_eq!(output.bytes_at(row).as_slice(), *expected);
            }
        }
        drop(validity);
        drop(output);
        assert_eq!(memory.snapshot().reserved_bytes, 0);
    }
    let dtype = DType::Binary(Nullability::NonNullable);
    let input = ChunkedArray::try_new(
        vec![
            VarBinViewArray::from_iter_bin([bytes.as_slice()]).into_array(),
            VarBinViewArray::from_iter_bin([b"inline".as_slice()]).into_array(),
        ],
        dtype.clone(),
    )
    .unwrap()
    .into_array();
    let memory = LiveMemoryPool::new(1024).unwrap();
    let output = input
        .execute::<VarBinViewArray>(&mut session(&memory).create_execution_ctx())
        .unwrap();
    assert_eq!(output.dtype(), &dtype);
    assert_eq!(output.bytes_at(0).as_slice(), &bytes);
    assert_eq!(output.bytes_at(1).as_slice(), b"inline");
    assert_eq!(memory.snapshot().reserved_bytes, 2 * 16 + 16);
    drop(output);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

#[test]
fn native_fsst_checked_sizes_reject_overflow_and_invalid_offsets() {
    assert!(add(u64::MAX, 1).is_err());
    assert!(mul(usize::MAX, 16).is_err());
    assert!(capacity(u64::MAX).is_err());
    for offsets in [vec![], vec![-1i64, 2], vec![2, 1], vec![0, 4]] {
        assert!(checked_offsets(&PrimitiveArray::from_iter(offsets), 3).is_err());
    }
    assert_eq!(
        checked_offsets(&PrimitiveArray::from_iter([1u32, 1, 3]), 3).unwrap(),
        2
    );
}

#[test]
fn native_fsst_encoded_like_does_not_admit_or_materialize_string_payloads() {
    use vortex::array::{
        arrays::bool::BoolArrayExt as _,
        arrays::{BoolArray, ConstantArray},
        scalar_fn::fns::like::{Like, LikeOptions},
    };
    let encoded = fixture(&[Some("long repeated native text"), Some("different text")]);
    let memory = LiveMemoryPool::new(1).unwrap();
    let session = session(&memory);
    let pattern = ConstantArray::new("%repeated%", 2).into_array();
    let output = Like::try_new(
        encoded.into_array(),
        pattern,
        LikeOptions {
            negated: false,
            case_insensitive: false,
        },
    )
    .unwrap()
    .into_array()
    .execute::<BoolArray>(&mut session.create_execution_ctx())
    .expect("native encoded matching must remain available");
    assert_eq!(
        output.to_bit_buffer().iter().collect::<Vec<_>>(),
        [true, false]
    );
    assert_eq!(memory.snapshot().peak_reserved_bytes, 0);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}
