use super::*;

fn serialized_roundtrip(input: &ArrayRef) -> ArrayRef {
    let session = VortexSession::default();
    let context = ArrayContext::empty();
    let mut bytes = ByteBufferMut::empty();
    for buffer in input
        .serialize(&context, &session, &SerializeOptions::default())
        .unwrap()
    {
        bytes.extend_from_slice(buffer.as_ref());
    }
    let output = SerializedArray::try_from(bytes.freeze())
        .unwrap()
        .decode(
            input.dtype(),
            input.len(),
            &ReadContext::new(context.to_ids()),
            &session,
        )
        .unwrap();
    assert!(output.is::<Zstd>());
    output
}

fn assert_primitive_bits(values: &PrimitiveArray) {
    let memory = LiveMemoryPool::new(64 << 20).unwrap();
    let session = session(&memory);
    let mut ctx = session.create_execution_ctx();
    let data = ZstdData::from_primitive_without_dict(values, 0, 3, &mut ctx).unwrap();
    let encoded = Zstd::try_new(values.dtype().clone(), data, values.validity().unwrap())
        .unwrap()
        .into_array();
    let serialized = serialized_roundtrip(&encoded);
    drop(encoded);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
    let validity = values
        .validity()
        .unwrap()
        .execute_mask(values.len(), &mut ctx)
        .unwrap();
    let expected = values.buffer_handle().try_to_host_sync().unwrap();
    let width = values.ptype().byte_width();
    for range in [0..values.len(), 1..values.len() - 1] {
        let output = serialized
            .slice(range.clone())
            .unwrap()
            .execute::<PrimitiveArray>(&mut ctx)
            .unwrap();
        let mask = output
            .validity()
            .unwrap()
            .execute_mask(output.len(), &mut ctx)
            .unwrap();
        let actual = output.buffer_handle().try_to_host_sync().unwrap();
        assert_eq!(output.dtype(), values.dtype());
        assert_eq!(output.len(), range.len());
        for (offset, index) in range.enumerate() {
            assert_eq!(mask.value(offset), validity.value(index));
            if validity.value(index) {
                assert_eq!(
                    &actual[offset * width..(offset + 1) * width],
                    &expected[index * width..(index + 1) * width],
                    "{} value {index}",
                    values.dtype()
                );
            }
        }
    }
    drop(ctx);
    drop(session);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

#[test]
fn native_zstd_compression_all_primitive_widths_preserve_nullable_bits_and_serialization() {
    macro_rules! integers {
        ($($ty:ty),+ $(,)?) => {$(
            assert_primitive_bits(&PrimitiveArray::from_option_iter(
                [Some(<$ty>::MIN), None, Some(<$ty>::MAX), Some(0), Some(1)]
                    .into_iter().cycle().take(137)
            ));
        )+};
    }
    integers!(i8, i16, i32, i64, u8, u16, u32, u64);
    let half_bits = PrimitiveArray::from_option_iter(
        [
            Some(0x7c01u16),
            Some(0x8000),
            None,
            Some(0x7c00),
            Some(0xfc00),
            Some(1),
        ]
        .into_iter()
        .cycle()
        .take(137),
    );
    assert_primitive_bits(&PrimitiveArray::from_byte_buffer(
        half_bits.buffer_handle().try_to_host_sync().unwrap(),
        PType::F16,
        half_bits.validity().unwrap(),
    ));
    assert_primitive_bits(&PrimitiveArray::from_option_iter(
        [
            Some(f32::from_bits(0x7f80_0001)),
            Some(-0.0),
            None,
            Some(f32::INFINITY),
            Some(f32::NEG_INFINITY),
            Some(f32::from_bits(1)),
        ]
        .into_iter()
        .cycle()
        .take(137),
    ));
    assert_primitive_bits(&PrimitiveArray::from_option_iter(
        [
            Some(f64::from_bits(0x7ff0_0000_0000_0001)),
            Some(-0.0),
            None,
            Some(f64::INFINITY),
            Some(f64::NEG_INFINITY),
            Some(f64::from_bits(1)),
        ]
        .into_iter()
        .cycle()
        .take(137),
    ));
}

#[test]
fn native_zstd_compression_nullable_unicode_and_binary_dictionary_roundtrip() {
    for binary in [false, true] {
        let values: Vec<_> = (0..512)
            .map(|index| {
                (index % 13 != 0).then(|| {
                    if index % 17 == 0 {
                        Vec::new()
                    } else {
                        let mut value = format!("native Unicode λ 🎉 value {index:05} ")
                            .repeat(3)
                            .into_bytes();
                        if binary {
                            value.extend_from_slice(&[0, 0x80, 0xff]);
                        }
                        value
                    }
                })
            })
            .collect();
        let input = if binary {
            VarBinViewArray::from_iter_nullable_bin(values.iter().map(Option::as_deref))
        } else {
            VarBinViewArray::from_iter_nullable_str(values.iter().map(|value| {
                value
                    .as_deref()
                    .map(|bytes| std::str::from_utf8(bytes).unwrap())
            }))
        };
        let memory = LiveMemoryPool::new(64 << 20).unwrap();
        let session = session(&memory);
        let mut ctx = session.create_execution_ctx();
        let encoded = Zstd::from_var_bin_view(&input, 3, 8, &mut ctx)
            .unwrap()
            .into_array();
        assert!(
            encoded
                .as_::<Zstd>()
                .data()
                .clone()
                .into_parts(input.validity().unwrap())
                .dictionary
                .is_some()
        );
        let serialized = serialized_roundtrip(&encoded);
        drop(encoded);
        assert_eq!(memory.snapshot().reserved_bytes, 0);
        for range in [0..512, 6..35, 251..287, 510..512] {
            let output = serialized
                .slice(range.clone())
                .unwrap()
                .execute::<VarBinViewArray>(&mut ctx)
                .unwrap();
            let mask = output
                .validity()
                .unwrap()
                .execute_mask(output.len(), &mut ctx)
                .unwrap();
            assert_eq!(output.dtype(), input.dtype());
            assert_eq!(output.len(), range.len());
            for (offset, index) in range.enumerate() {
                assert_eq!(mask.value(offset), values[index].is_some());
                if let Some(expected) = &values[index] {
                    assert_eq!(output.bytes_at(offset).as_slice(), expected);
                }
            }
        }
        drop(ctx);
        drop(session);
        assert_eq!(memory.snapshot().reserved_bytes, 0);
    }
}

#[test]
fn native_zstd_compression_all_null_input_skips_foreign_contexts() {
    let memory = LiveMemoryPool::new(4096).unwrap();
    let (session, recorder) = admission_session(&memory, None);
    let mut ctx = session.create_execution_ctx();
    let primitives = PrimitiveArray::from_option_iter(std::iter::repeat_n(None::<i64>, 19));
    let strings = VarBinViewArray::from_iter_nullable_str(std::iter::repeat_n(None::<&str>, 19));
    for (dtype, data) in [
        (
            primitives.dtype(),
            ZstdData::from_primitive(&primitives, 0, 0, &mut ctx).unwrap(),
        ),
        (
            strings.dtype(),
            ZstdData::from_var_bin_view(&strings, 0, 0, &mut ctx).unwrap(),
        ),
    ] {
        let parts = data.clone().into_parts(Validity::AllInvalid);
        assert!(parts.frames.is_empty() && parts.dictionary.is_none());
        let encoded = Zstd::try_new(dtype.clone(), data, Validity::AllInvalid)
            .unwrap()
            .into_array();
        let output = serialized_roundtrip(&encoded)
            .execute::<vortex::array::Canonical>(&mut ctx)
            .unwrap()
            .into_array();
        assert_eq!(output.len(), 19);
        assert!(
            output
                .validity()
                .unwrap()
                .execute_mask(output.len(), &mut ctx)
                .unwrap()
                .all_false()
        );
    }
    assert!(
        !recorder
            .requests
            .lock()
            .unwrap()
            .iter()
            .any(|request| matches!(request, Request::External(_)))
    );
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

#[test]
fn native_zstd_compression_extreme_levels_use_bounded_normalization() {
    let expected: Vec<u8> = (0u16..8193)
        .map(|index| u8::try_from(index.wrapping_mul(127) % 251).unwrap())
        .collect();
    let input = PrimitiveArray::from_iter(expected.iter().copied());
    // A grant, not a physical allocation: the selected-level estimator covers
    // all input sizes, while this regression compresses only eight KiB.
    let memory = LiveMemoryPool::new(8 << 30).unwrap();
    let session = session(&memory);
    let mut ctx = session.create_execution_ctx();
    for level in [i32::MIN, -1000, -1, 0, 1, 9, 19, 22, i32::MAX] {
        let data = ZstdData::from_primitive_without_dict(&input, level, 0, &mut ctx).unwrap();
        let encoded = Zstd::try_new(input.dtype().clone(), data, Validity::NonNullable)
            .unwrap()
            .into_array();
        let output = encoded.execute::<PrimitiveArray>(&mut ctx).unwrap();
        assert_eq!(output.as_slice::<u8>(), &expected, "level {level}");
        drop(output);
        assert_eq!(memory.snapshot().reserved_bytes, 0);
    }
}
