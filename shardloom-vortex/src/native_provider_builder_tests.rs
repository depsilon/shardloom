use super::*;

#[path = "native_provider_builder_cost.rs"]
mod cost;
use vortex::{
    VortexSessionDefault as _,
    array::{
        Canonical, VortexSessionExecute as _,
        arrays::{BoolArray, ChunkedArray, ConstantArray, DecimalArray, bool::BoolArrayExt as _},
        dtype::{DecimalDType, DecimalType, Nullability, PType, i256},
        memory::MemorySessionExt as _,
    },
    buffer::{BitBuffer, buffer},
    encodings::{
        fastlanes::BitPackedData,
        zstd::{Zstd, ZstdData},
    },
};

fn session(memory: &LiveMemoryPool) -> VortexSession {
    let session = VortexSession::default().with_allocator(Arc::new(
        crate::owned_buffers::ReservedHostAllocator::new(memory.clone()),
    ));
    install(&session, memory.clone());
    session
}

fn chunks(values: Vec<ArrayRef>) -> ArrayRef {
    let dtype = values[0].dtype().clone();
    ChunkedArray::try_new(values, dtype).unwrap().into_array()
}

#[test]
fn native_builder_fixed_width_admission_precedes_unowned_upstream_builder() {
    let cases = [
        chunks(vec![
            PrimitiveArray::from_iter([1i64, 2]).into_array(),
            PrimitiveArray::from_iter([3i64, 4]).into_array(),
        ]),
        chunks(vec![
            BoolArray::new(BitBuffer::new_set(3), Validity::NonNullable).into_array(),
            BoolArray::new(BitBuffer::new_unset(5), Validity::NonNullable).into_array(),
        ]),
        chunks(vec![
            DecimalArray::new(
                buffer![123i128],
                DecimalDType::new(18, 2),
                Validity::NonNullable,
            )
            .into_array(),
            DecimalArray::new(
                buffer![-456i128],
                DecimalDType::new(18, 2),
                Validity::NonNullable,
            )
            .into_array(),
        ]),
    ];
    for input in cases {
        let memory = LiveMemoryPool::new(1).unwrap();
        let error = input
            .execute::<Canonical>(&mut session(&memory).create_execution_ctx())
            .unwrap_err();
        assert!(crate::owned_buffers::is_owned_reservation_denial(&error));
        assert_eq!(memory.snapshot().peak_reserved_bytes, 0);
        assert_eq!(memory.snapshot().reserved_bytes, 0);
    }
}

#[test]
fn native_builder_string_finish_replacement_is_admitted_before_append() {
    let input = chunks(vec![
        VarBinViewArray::from_iter_str(["first", "second"]).into_array(),
        VarBinViewArray::from_iter_str(["third"]).into_array(),
    ]);
    let views = aligned_capacity(mul(input.len(), 16).unwrap(), 16).unwrap();
    let memory = LiveMemoryPool::new(views).unwrap();
    let error = input
        .execute::<VarBinViewArray>(&mut session(&memory).create_execution_ctx())
        .unwrap_err();
    assert!(crate::owned_buffers::is_owned_reservation_denial(&error));
    assert_eq!(memory.snapshot().peak_reserved_bytes, 0);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

#[test]
fn native_builder_primitive_widths_preserve_bytes_and_charge_finish_overlap() {
    macro_rules! check {
        ($($ty:ty),+ $(,)?) => {$({
            let expected = PrimitiveArray::from_iter((0u8..37).map(|row| <$ty>::try_from(row).unwrap()));
            let source = expected.clone().into_array();
            let input = chunks(vec![
                source.slice(0..13).unwrap(),
                source.slice(13..37).unwrap(),
            ]);
            let data_bytes = capacity(37 * size_of::<$ty>() as u64).unwrap();
            let finish_bytes = capacity(0).unwrap();
            let memory = LiveMemoryPool::new(data_bytes + finish_bytes).unwrap();
            let output = input.execute::<PrimitiveArray>(
                &mut session(&memory).create_execution_ctx()
            ).unwrap();
            assert_eq!(output.dtype(), expected.dtype());
            assert_eq!(output.as_slice::<$ty>(), expected.as_slice::<$ty>());
            assert_eq!(memory.snapshot().peak_reserved_bytes, data_bytes + finish_bytes);
            assert_eq!(memory.snapshot().reserved_bytes, data_bytes);
            let data = output.buffer_handle().as_host().clone();
            let pointer = data.as_ptr();
            let offset = size_of::<$ty>();
            let slice = data.slice(offset..2 * offset);
            drop(output);
            drop(data);
            assert_eq!(slice.as_ptr(), pointer.wrapping_add(offset));
            assert_eq!(memory.snapshot().reserved_bytes, data_bytes);
            drop(slice);
            assert_eq!(memory.snapshot().reserved_bytes, 0);
        })+};
    }
    check!(u8, u16, u32, u64, i8, i16, i32, i64, f32, f64);
}

#[test]
fn native_builder_nested_nullable_values_and_bitmap_have_independent_owners() {
    let first = PrimitiveArray::from_option_iter([Some(i64::MIN), None, Some(37)]);
    let last = PrimitiveArray::from_option_iter([None, Some(i64::MAX)]);
    let middle = PrimitiveArray::from_option_iter([Some(-17i64), None]);
    let input = chunks(vec![
        first.into_array(),
        chunks(vec![middle.into_array(), last.into_array()]),
    ]);
    let data_bytes = capacity(7 * 8).unwrap();
    let bitmap_bytes = capacity(1).unwrap();
    let memory = LiveMemoryPool::new(data_bytes + bitmap_bytes + capacity(0).unwrap()).unwrap();
    let owner = session(&memory);
    let mut ctx = owner.create_execution_ctx();
    let output = input.execute::<PrimitiveArray>(&mut ctx).unwrap();
    let valid = output
        .validity()
        .unwrap()
        .execute_mask(7, &mut ctx)
        .unwrap();
    let expected = [
        Some(i64::MIN),
        None,
        Some(37),
        Some(-17),
        None,
        None,
        Some(i64::MAX),
    ];
    for (row, expected) in expected.into_iter().enumerate() {
        assert_eq!(valid.value(row), expected.is_some());
        if let Some(expected) = expected {
            assert_eq!(output.as_slice::<i64>()[row], expected);
        }
    }
    let values = output.buffer_handle().as_host().clone();
    let validity = output.validity().unwrap();
    drop(valid);
    drop(output);
    drop(ctx);
    drop(owner);
    assert_eq!(memory.snapshot().reserved_bytes, data_bytes + bitmap_bytes);
    drop(values);
    assert_eq!(memory.snapshot().reserved_bytes, bitmap_bytes);
    drop(validity);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

#[test]
fn native_builder_decimal_width_follows_precision_and_preserves_scale() {
    for (precision, bytes) in [(2, 1), (4, 2), (9, 4), (18, 8), (38, 16)] {
        let dtype = DecimalDType::new(precision, 1);
        let first = DecimalArray::from_option_iter([Some(9i128), None], dtype);
        let last = DecimalArray::from_option_iter([Some(-7i128)], dtype);
        let input = chunks(vec![first.into_array(), last.into_array()]);
        let data_bytes = capacity(3 * bytes).unwrap();
        let bitmap_bytes = capacity(1).unwrap();
        let memory = LiveMemoryPool::new(data_bytes + bitmap_bytes + capacity(0).unwrap()).unwrap();
        let mut ctx = session(&memory).create_execution_ctx();
        let output = input.execute::<DecimalArray>(&mut ctx).unwrap();
        assert_eq!(
            output.values_type(),
            DecimalType::smallest_decimal_value_type(&dtype)
        );
        assert_eq!(
            output.dtype(),
            &DType::Decimal(dtype, vortex::array::dtype::Nullability::Nullable)
        );
        let expected =
            DecimalArray::from_option_iter([Some(9i128), None, Some(-7i128)], dtype).into_array();
        let result = output.into_array();
        for row in 0..3 {
            assert_eq!(
                result.execute_scalar(row, &mut ctx).unwrap(),
                expected.execute_scalar(row, &mut ctx).unwrap()
            );
        }
        assert_eq!(memory.snapshot().reserved_bytes, data_bytes + bitmap_bytes);
        drop(result);
        assert_eq!(memory.snapshot().reserved_bytes, 0);
    }
}

#[test]
fn native_builder_late_codec_failure_releases_partially_built_output() {
    let values = PrimitiveArray::from_iter(0i64..33);
    let mut ctx = VortexSession::default().create_execution_ctx();
    let data = ZstdData::from_primitive_without_dict(&values, 0, 8, &mut ctx).unwrap();
    let mut parts = data.into_parts(Validity::NonNullable);
    let frame = &parts.frames[0];
    parts.frames[0] = frame.slice(..frame.len() - 1);
    let corrupt = Zstd::try_new(
        values.dtype().clone(),
        ZstdData::new(parts.dictionary, parts.frames, parts.metadata, parts.n_rows),
        parts.validity,
    )
    .unwrap()
    .into_array();
    let codec_error = corrupt
        .clone()
        .execute::<PrimitiveArray>(&mut VortexSession::default().create_execution_ctx())
        .unwrap_err()
        .to_string();
    let input = chunks(vec![
        PrimitiveArray::from_iter([71i64, 92]).into_array(),
        corrupt,
    ]);
    let denied = LiveMemoryPool::new(1).unwrap();
    let error = input
        .clone()
        .execute::<PrimitiveArray>(&mut session(&denied).create_execution_ctx())
        .unwrap_err();
    assert!(crate::owned_buffers::is_owned_reservation_denial(&error));
    assert_eq!(denied.snapshot().peak_reserved_bytes, 0);
    let memory = LiveMemoryPool::new(1 << 20).unwrap();
    let error = input
        .execute::<PrimitiveArray>(&mut session(&memory).create_execution_ctx())
        .unwrap_err();
    assert!(!crate::owned_buffers::is_owned_reservation_denial(&error));
    assert_eq!(error.to_string(), codec_error);
    assert!(memory.snapshot().peak_reserved_bytes > capacity(35 * 8).unwrap());
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

#[test]
fn native_builder_bool_bit_offsets_preserve_values_and_independent_validity() {
    for rows in [1usize, 7, 8, 9, 63, 64, 65] {
        let source = BoolArray::from_iter(
            (0..rows + 9).map(|row| (!row.is_multiple_of(5)).then_some(row.is_multiple_of(3))),
        )
        .into_array();
        let first = source.slice(3..3 + rows / 2).unwrap();
        let last = source.slice(3 + rows / 2..3 + rows).unwrap();
        let input = chunks(vec![first, last]);
        let bitmap_bytes = capacity(rows.div_ceil(8) as u64).unwrap();
        let memory = LiveMemoryPool::new(2 * bitmap_bytes + capacity(0).unwrap()).unwrap();
        let mut ctx = session(&memory).create_execution_ctx();
        let output = input.execute::<BoolArray>(&mut ctx).unwrap();
        let values = output.to_bit_buffer();
        let validity = output.as_ref().validity().unwrap();
        let mask = validity.execute_mask(rows, &mut ctx).unwrap();
        for row in 0..rows {
            assert_eq!(mask.value(row), !(row + 3).is_multiple_of(5));
            if mask.value(row) {
                assert_eq!(values.value(row), (row + 3).is_multiple_of(3));
            }
        }
        let retained_bitmap = matches!(validity, Validity::Array(_));
        drop(mask);
        drop(output);
        let slice = values.slice(0..1);
        assert_eq!(slice.inner().as_ptr(), values.inner().as_ptr());
        drop(values);
        assert_eq!(
            memory.snapshot().reserved_bytes,
            bitmap_bytes * (1 + u64::from(retained_bitmap))
        );
        drop(validity);
        assert_eq!(memory.snapshot().reserved_bytes, bitmap_bytes);
        drop(slice);
        assert_eq!(memory.snapshot().reserved_bytes, 0);
    }
}

#[test]
fn native_builder_constant_validity_releases_unused_bitmap_credit() {
    for validity in [Validity::AllValid, Validity::AllInvalid] {
        let cases = [
            PrimitiveArray::new(buffer![7i32; 17], validity.clone()).into_array(),
            BoolArray::new(BitBuffer::new_set(17), validity.clone()).into_array(),
            DecimalArray::new(
                buffer![7i128; 17],
                DecimalDType::new(38, 2),
                validity.clone(),
            )
            .into_array(),
        ];
        for (source, data_size) in cases.into_iter().zip([17 * 4, 3, 17 * 16]) {
            let input = chunks(vec![
                source.slice(0..8).unwrap(),
                source.slice(8..17).unwrap(),
            ]);
            let data_bytes = capacity(data_size).unwrap();
            let memory =
                LiveMemoryPool::new(data_bytes + capacity(3).unwrap() + capacity(0).unwrap())
                    .unwrap();
            let output = input
                .execute::<Canonical>(&mut session(&memory).create_execution_ctx())
                .unwrap()
                .into_array();
            assert_eq!(
                std::mem::discriminant(&output.validity().unwrap()),
                std::mem::discriminant(&validity)
            );
            assert_eq!(output.dtype(), source.dtype());
            assert_eq!(memory.snapshot().reserved_bytes, data_bytes);
            drop(output);
            assert_eq!(memory.snapshot().reserved_bytes, 0);
        }
    }
}

#[test]
fn native_builder_reduced_empty_and_single_chunk_keep_existing_owners() {
    let source = PrimitiveArray::from_iter([7i64, -9, 33]);
    let pointer = source.buffer_handle().as_host().as_ptr();
    let memory = LiveMemoryPool::new(1).unwrap();
    let mut ctx = session(&memory).create_execution_ctx();
    let single = chunks(vec![source.clone().into_array()]);
    let output = single.execute::<ArrayRef>(&mut ctx).unwrap();
    assert_eq!(
        output.as_::<Primitive>().buffer_handle().as_host().as_ptr(),
        pointer
    );
    let empty = ChunkedArray::try_new(Vec::new(), source.dtype().clone())
        .unwrap()
        .into_array();
    assert_eq!(empty.execute::<PrimitiveArray>(&mut ctx).unwrap().len(), 0);
    assert_eq!(memory.snapshot().peak_reserved_bytes, 0);
}

#[test]
fn native_builder_preserves_float_bits_and_widest_decimal_payload() {
    let bits = buffer![0u16, 0x8000, 0x7e01, 0xfe42, 0x7c00, 0xfc00, 1].into_byte_buffer();
    let half = PrimitiveArray::from_byte_buffer(bits.clone(), PType::F16, Validity::NonNullable)
        .into_array();
    let input = chunks(vec![half.slice(0..3).unwrap(), half.slice(3..7).unwrap()]);
    let half_data_bytes = capacity(7 * 2).unwrap();
    let half_memory = LiveMemoryPool::new(half_data_bytes + capacity(0).unwrap()).unwrap();
    let output = input
        .execute::<PrimitiveArray>(&mut session(&half_memory).create_execution_ctx())
        .unwrap();
    assert_eq!(output.buffer_handle().as_host().as_slice(), bits.as_slice());
    assert_eq!(half_memory.snapshot().reserved_bytes, half_data_bytes);
    drop(output);
    assert_eq!(half_memory.snapshot().reserved_bytes, 0);
    macro_rules! floats {
        ($ty:ty, $bits:expr) => {{
            let bits = $bits;
            let source = PrimitiveArray::from_iter(bits.map(<$ty>::from_bits)).into_array();
            let input = chunks(vec![
                source.slice(0..3).unwrap(),
                source.slice(3..bits.len()).unwrap(),
            ]);
            let data_bytes = capacity((bits.len() * size_of::<$ty>()) as u64).unwrap();
            let memory = LiveMemoryPool::new(data_bytes + capacity(0).unwrap()).unwrap();
            let output = input
                .execute::<PrimitiveArray>(&mut session(&memory).create_execution_ctx())
                .unwrap();
            assert_eq!(
                output
                    .as_slice::<$ty>()
                    .iter()
                    .map(|v| v.to_bits())
                    .collect::<Vec<_>>(),
                bits
            );
            assert_eq!(memory.snapshot().reserved_bytes, data_bytes);
            drop(output);
            assert_eq!(memory.snapshot().reserved_bytes, 0);
        }};
    }
    floats!(
        f32,
        [
            0u32,
            0x8000_0000,
            0x7fc0_0001,
            0xffc0_0042,
            0x7f80_0000,
            0xff80_0000,
            1
        ]
    );
    floats!(
        f64,
        [
            0u64,
            0x8000_0000_0000_0000,
            0x7ff8_0000_0000_0001,
            0xfff8_0000_0000_0042,
            0x7ff0_0000_0000_0000,
            0xfff0_0000_0000_0000,
            1
        ]
    );

    let large = i256::from_parts(17, 1 << 70);
    let dtype = DecimalDType::new(76, -2);
    let expected = [large, -large, i256::from_i128(0)];
    let source = DecimalArray::new(expected.into_iter().collect(), dtype, Validity::NonNullable)
        .into_array();
    let input = chunks(vec![
        source.slice(0..1).unwrap(),
        source.slice(1..3).unwrap(),
    ]);
    let data_bytes = capacity(3 * 32).unwrap();
    let memory = LiveMemoryPool::new(data_bytes + capacity(0).unwrap()).unwrap();
    let output = input
        .execute::<DecimalArray>(&mut session(&memory).create_execution_ctx())
        .unwrap();
    assert_eq!(output.values_type(), DecimalType::I256);
    assert_eq!(output.dtype(), source.dtype());
    assert_eq!(output.buffer::<i256>().as_ref(), &expected);
    let values = output.buffer::<i256>();
    drop(output);
    assert_eq!(memory.snapshot().reserved_bytes, data_bytes);
    drop(values);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

#[test]
fn native_builder_encoded_append_and_overlapping_outputs_share_one_grant() {
    let source = PrimitiveArray::from_iter((0..2051).map(|row| row % 13));
    let encoded = BitPackedData::encode(
        &source.clone().into_array(),
        4,
        &mut VortexSession::default().create_execution_ctx(),
    )
    .unwrap()
    .into_array();
    let input = chunks(vec![
        encoded.slice(0..1027).unwrap(),
        encoded.slice(1027..2051).unwrap(),
    ]);
    let data_bytes = capacity(2051 * 4).unwrap();
    let memory = LiveMemoryPool::new(2 * data_bytes + capacity(0).unwrap()).unwrap();
    let mut ctx = session(&memory).create_execution_ctx();
    let first = input.clone().execute::<PrimitiveArray>(&mut ctx).unwrap();
    let second = input.clone().execute::<PrimitiveArray>(&mut ctx).unwrap();
    assert_eq!(first.as_slice::<i32>(), source.as_slice::<i32>());
    assert_eq!(second.as_slice::<i32>(), source.as_slice::<i32>());
    assert_eq!(memory.snapshot().reserved_bytes, 2 * data_bytes);
    let error = input
        .clone()
        .execute::<PrimitiveArray>(&mut ctx)
        .unwrap_err();
    assert!(crate::owned_buffers::is_owned_reservation_denial(&error));
    assert_eq!(memory.snapshot().reserved_bytes, 2 * data_bytes);
    drop(first);
    let third = input.execute::<PrimitiveArray>(&mut ctx).unwrap();
    assert_eq!(third.as_slice::<i32>(), source.as_slice::<i32>());
    drop(second);
    drop(third);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

#[test]
fn native_builder_impossible_capacity_fails_before_allocation() {
    let memory = LiveMemoryPool::new(1 << 20).unwrap();
    for rows in [usize::MAX / 4, usize::MAX / 16] {
        let input = chunks(vec![
            ConstantArray::new(7i64, rows).into_array(),
            ConstantArray::new(-9i64, rows).into_array(),
        ]);
        let error = input
            .execute::<PrimitiveArray>(&mut session(&memory).create_execution_ctx())
            .unwrap_err();
        assert!(error.to_string().contains("overflow"));
        assert_eq!(memory.snapshot().peak_reserved_bytes, 0);
        assert_eq!(memory.snapshot().reserved_bytes, 0);
    }
}

#[test]
fn native_builder_codec_temporaries_overlap_then_release_before_result() {
    let expected: Vec<_> = (0..33i64)
        .map(|row| (row % 7 != 0).then_some(row - 17))
        .collect();
    let values = PrimitiveArray::from_option_iter(expected.iter().copied());
    let data = ZstdData::from_primitive_without_dict(
        &values,
        0,
        8,
        &mut VortexSession::default().create_execution_ctx(),
    )
    .unwrap();
    let encoded = Zstd::try_new(values.dtype().clone(), data, values.validity().unwrap())
        .unwrap()
        .into_array();
    let input = chunks(vec![encoded.clone(), encoded]);
    let retained = capacity(66 * 8).unwrap() + capacity(9).unwrap();
    let memory = LiveMemoryPool::new(1 << 20).unwrap();
    let mut ctx = session(&memory).create_execution_ctx();
    let output = input.clone().execute::<PrimitiveArray>(&mut ctx).unwrap();
    let valid = output
        .validity()
        .unwrap()
        .execute_mask(66, &mut ctx)
        .unwrap();
    for (row, expected) in expected.iter().cycle().take(66).enumerate() {
        assert_eq!(valid.value(row), expected.is_some());
        if let Some(expected) = expected {
            assert_eq!(output.as_slice::<i64>()[row], *expected);
        }
    }
    let peak = memory.snapshot().peak_reserved_bytes;
    assert!(peak > retained + capacity(0).unwrap());
    assert_eq!(memory.snapshot().reserved_bytes, retained);
    drop(valid);
    drop(output);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
    let denied = LiveMemoryPool::new(peak - 1).unwrap();
    let error = input
        .execute::<PrimitiveArray>(&mut session(&denied).create_execution_ctx())
        .unwrap_err();
    assert!(crate::owned_buffers::is_owned_reservation_denial(&error));
    assert_eq!(denied.snapshot().reserved_bytes, 0);
}

#[test]
fn native_builder_string_finish_peak_and_output_owner_are_separate() {
    let input = chunks(vec![
        VarBinViewArray::from_iter_str(["first", "second"]).into_array(),
        VarBinViewArray::from_iter_str(["third"]).into_array(),
    ]);
    let views = aligned_capacity(3 * 16, 16).unwrap();
    let grant = views + capacity(0).unwrap();
    let memory = LiveMemoryPool::new(grant).unwrap();
    let output = input
        .execute::<VarBinViewArray>(&mut session(&memory).create_execution_ctx())
        .unwrap();
    assert_eq!(output.dtype(), &DType::Utf8(Nullability::NonNullable));
    assert_eq!(memory.snapshot().peak_reserved_bytes, grant);
    assert_eq!(memory.snapshot().reserved_bytes, views);
    drop(output);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}
