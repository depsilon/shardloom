use super::*;

#[path = "native_provider_zstd_workspace_tests.rs"]
mod workspace_tests;

use vortex::{
    VortexSessionDefault as _,
    array::{
        ArrayContext, VortexSessionExecute as _,
        arrays::ChunkedArray,
        builders::VarBinViewBuilder,
        builtins::ArrayBuiltins as _,
        dtype::{Nullability, PType},
        memory::MemorySessionExt as _,
        serde::{SerializeOptions, SerializedArray},
    },
    buffer::ByteBufferMut,
    encodings::zstd::{Zstd, ZstdData, ZstdMetadata},
    session::registry::ReadContext,
};

fn session(memory: &LiveMemoryPool) -> VortexSession {
    let session = VortexSession::default().with_allocator(Arc::new(
        crate::owned_buffers::ReservedHostAllocator::new(memory.clone()),
    ));
    install(&session, memory.clone());
    session
}

fn primitive(values: &PrimitiveArray) -> ArrayRef {
    let mut ctx = VortexSession::default().create_execution_ctx();
    let data = ZstdData::from_primitive_without_dict(values, 0, 8, &mut ctx).unwrap();
    Zstd::try_new(values.dtype().clone(), data, values.validity().unwrap())
        .unwrap()
        .into_array()
}

fn strings(values: &VarBinViewArray) -> ArrayRef {
    let mut ctx = VortexSession::default().create_execution_ctx();
    Zstd::from_var_bin_view_without_dict(values, 0, 4, &mut ctx)
        .unwrap()
        .into_array()
}

#[test]
fn native_zstd_root_denial_is_typed_and_payload_credit_follows_slices() {
    let encoded = primitive(&PrimitiveArray::from_iter(0i64..33));
    // Non-null Zstd has no child, so this exercises the root decoder itself.
    assert!(encoded.children().is_empty());
    let memory = LiveMemoryPool::new(1).unwrap();
    let session = session(&memory);
    let error = encoded
        .clone()
        .execute::<PrimitiveArray>(&mut session.create_execution_ctx())
        .unwrap_err();
    assert!(crate::owned_buffers::is_owned_reservation_denial(&error));
    assert_eq!(memory.snapshot().peak_reserved_bytes, 0);
    assert_eq!(memory.snapshot().reserved_bytes, 0);

    let memory = LiveMemoryPool::new(1 << 20).unwrap();
    let session = self::session(&memory);
    let output = encoded
        .slice(9..14)
        .unwrap()
        .execute::<PrimitiveArray>(&mut session.create_execution_ctx())
        .unwrap();
    assert_eq!(output.as_slice::<i64>(), &[9, 10, 11, 12, 13]);
    let bytes = output.as_ref().buffers()[0].clone();
    let escaped = bytes.slice(8..16);
    let retained = memory.snapshot().reserved_bytes;
    assert!(retained >= 5 * 8 + 256);
    assert!(retained < 33 * 8 + 256);
    drop(output);
    drop(bytes);
    drop(session);
    assert_eq!(memory.snapshot().reserved_bytes, retained);
    assert_eq!(escaped.as_slice(), 10i64.to_ne_bytes());
    drop(escaped);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

#[test]
fn native_zstd_nullable_primitive_scatter_is_reserved_and_exact() {
    let expected = [Some(-9i64), None, Some(i64::MAX), Some(0), None];
    let encoded = primitive(&PrimitiveArray::from_option_iter(expected));
    let memory = LiveMemoryPool::new(1 << 20).unwrap();
    let session = session(&memory);
    let mut ctx = session.create_execution_ctx();
    let output = encoded.clone().execute::<PrimitiveArray>(&mut ctx).unwrap();
    let mask = output
        .validity()
        .unwrap()
        .execute_mask(output.len(), &mut ctx)
        .unwrap();
    for (index, value) in expected.iter().enumerate() {
        assert_eq!(mask.value(index), value.is_some());
        if let Some(value) = value {
            assert_eq!(output.as_slice::<i64>()[index], *value);
        }
    }
    assert_eq!(memory.snapshot().reserved_bytes, 5 * 8 + 256);
    assert!(memory.snapshot().peak_reserved_bytes > memory.snapshot().reserved_bytes);
    let peak = memory.snapshot().peak_reserved_bytes;
    drop(output);
    assert_eq!(memory.snapshot().reserved_bytes, 0);

    let denied = LiveMemoryPool::new(peak - 1).unwrap();
    let error = encoded
        .execute::<PrimitiveArray>(&mut self::session(&denied).create_execution_ctx())
        .unwrap_err();
    assert!(crate::owned_buffers::is_owned_reservation_denial(&error));
    assert_eq!(denied.snapshot().reserved_bytes, 0);
}

#[test]
fn native_zstd_string_views_and_scalar_keep_their_native_owners() {
    let values: Vec<String> = (0..21)
        .map(|index| format!("native long Unicode λ value {index:03}"))
        .collect();
    let encoded = strings(&VarBinViewArray::from_iter_str(
        values.iter().map(String::as_str),
    ));
    let memory = LiveMemoryPool::new(1 << 20).unwrap();
    let session = session(&memory);
    let mut ctx = session.create_execution_ctx();
    let output = encoded
        .slice(5..10)
        .unwrap()
        .execute::<VarBinViewArray>(&mut ctx)
        .unwrap();
    for (index, expected) in values[5..10].iter().enumerate() {
        assert_eq!(output.bytes_at(index).as_slice(), expected.as_bytes());
    }
    let data = output.data_buffers()[0].as_host().clone();
    let views = output.views_handle().as_host().clone();
    let retained = memory.snapshot().reserved_bytes;
    drop(output);
    assert_eq!(memory.snapshot().reserved_bytes, retained);
    drop(views);
    assert!(memory.snapshot().reserved_bytes > 0);
    assert!(memory.snapshot().reserved_bytes < retained);
    drop(data);
    assert_eq!(memory.snapshot().reserved_bytes, 0);

    let scalar = encoded.execute_scalar(9, &mut ctx).unwrap();
    assert_eq!(scalar.as_utf8().value().unwrap().as_str(), values[9]);
    assert!(memory.snapshot().reserved_bytes > 0);
    drop(ctx);
    drop(session);
    assert!(memory.snapshot().reserved_bytes > 0);
    drop(scalar);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

#[test]
fn native_zstd_all_null_output_has_accounted_canonical_storage() {
    for dtype in [
        DType::Primitive(PType::I64, Nullability::Nullable),
        DType::Utf8(Nullability::Nullable),
    ] {
        let input = Zstd::try_new(
            dtype.clone(),
            ZstdData::new(
                None,
                vec![],
                ZstdMetadata {
                    dictionary_size: 0,
                    frames: vec![],
                },
                9,
            ),
            Validity::AllInvalid,
        )
        .unwrap()
        .into_array();
        let memory = LiveMemoryPool::new(1 << 20).unwrap();
        let session = session(&memory);
        let mut ctx = session.create_execution_ctx();
        let output = input
            .execute::<vortex::array::Canonical>(&mut ctx)
            .unwrap()
            .into_array();
        assert_eq!(output.dtype(), &dtype);
        assert_eq!(output.len(), 9);
        assert!(
            output
                .validity()
                .unwrap()
                .execute_mask(9, &mut ctx)
                .unwrap()
                .all_false()
        );
        let width = if dtype.is_primitive() { 8 } else { 16 };
        assert_eq!(memory.snapshot().reserved_bytes, 9 * width + 256);
        drop(output);
        assert_eq!(memory.snapshot().reserved_bytes, 0);
    }
}

#[test]
fn native_zstd_nullable_binary_preserves_bytes_and_releases_failed_scatter() {
    let long = [0xffu8, 0x80, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 0xfe];
    let expected = [Some(long.as_slice()), None, Some(b"".as_slice()), None];
    let encoded = strings(&VarBinViewArray::from_iter_nullable_bin(expected));
    let memory = LiveMemoryPool::new(1 << 20).unwrap();
    let session = session(&memory);
    let mut ctx = session.create_execution_ctx();
    let output = encoded
        .clone()
        .execute::<VarBinViewArray>(&mut ctx)
        .unwrap();
    let mask = output
        .validity()
        .unwrap()
        .execute_mask(4, &mut ctx)
        .unwrap();
    for (index, expected) in expected.iter().enumerate() {
        assert_eq!(mask.value(index), expected.is_some());
        if let Some(bytes) = expected {
            assert_eq!(output.bytes_at(index).as_slice(), *bytes);
        }
    }
    let retained = memory.snapshot().reserved_bytes;
    let peak = memory.snapshot().peak_reserved_bytes;
    assert!(peak > retained);
    let payload = output.data_buffers()[0].as_host().clone();
    let views = output.views_handle().as_host().clone();
    drop(output);
    drop(ctx);
    drop(session);
    assert_eq!(memory.snapshot().reserved_bytes, retained);
    drop(payload);
    assert_eq!(memory.snapshot().reserved_bytes, 4 * 16 + 256);
    drop(views);
    assert_eq!(memory.snapshot().reserved_bytes, 0);

    let denied = LiveMemoryPool::new(peak - 1).unwrap();
    let error = encoded
        .execute::<VarBinViewArray>(&mut self::session(&denied).create_execution_ctx())
        .unwrap_err();
    assert!(crate::owned_buffers::is_owned_reservation_denial(&error));
    assert!(denied.snapshot().peak_reserved_bytes > 0);
    assert_eq!(denied.snapshot().reserved_bytes, 0);
}

#[test]
fn native_zstd_chunked_strings_own_payload_views_and_validity() {
    let first = [Some("first long Unicode λ value"), None, Some("")];
    let second = [None, Some("second long Unicode λ value")];
    let dtype = DType::Utf8(Nullability::Nullable);
    let input = ChunkedArray::try_new(
        vec![
            strings(&VarBinViewArray::from_iter_nullable_str(first)),
            VarBinViewArray::from_iter_nullable_str([Some("inline")]).into_array(),
            strings(&VarBinViewArray::from_iter_nullable_str(second)),
        ],
        dtype.clone(),
    )
    .unwrap()
    .into_array();
    let memory = LiveMemoryPool::new(1 << 20).unwrap();
    let session = session(&memory);
    let mut ctx = session.create_execution_ctx();
    let output = input.clone().execute::<VarBinViewArray>(&mut ctx).unwrap();
    let expected = first.into_iter().chain([Some("inline")]).chain(second);
    let mask = output
        .validity()
        .unwrap()
        .execute_mask(6, &mut ctx)
        .unwrap();
    for (index, expected) in expected.enumerate() {
        assert_eq!(mask.value(index), expected.is_some());
        if let Some(value) = expected {
            assert_eq!(output.bytes_at(index).as_slice(), value.as_bytes());
        }
    }
    let data: Vec<_> = output
        .data_buffers()
        .iter()
        .map(|v| v.as_host().clone())
        .collect();
    let payload = data
        .iter()
        .map(|v| u64::try_from(v.len()).unwrap() + 256)
        .sum::<u64>();
    let views_credit = 6 * 16 + 16;
    let validity_credit = 1 + 256;
    assert_eq!(
        memory.snapshot().reserved_bytes,
        payload + views_credit + validity_credit
    );
    let views = output.views_handle().as_host().clone();
    let Validity::Array(validity) = output.validity().unwrap() else {
        panic!("mixed chunk validity must retain a bitmap")
    };
    drop(mask);
    drop(output);
    drop(ctx);
    drop(session);
    drop(data);
    assert_eq!(
        memory.snapshot().reserved_bytes,
        views_credit + validity_credit
    );
    drop(views);
    assert_eq!(memory.snapshot().reserved_bytes, validity_credit);
    drop(validity);
    assert_eq!(memory.snapshot().reserved_bytes, 0);

    let denied = LiveMemoryPool::new(views_credit + validity_credit - 1).unwrap();
    let error = input
        .execute::<VarBinViewArray>(&mut self::session(&denied).create_execution_ctx())
        .unwrap_err();
    assert!(crate::owned_buffers::is_owned_reservation_denial(&error));
    assert_eq!(denied.snapshot().peak_reserved_bytes, 0);
}

#[test]
fn native_zstd_direct_append_uses_the_same_payload_admission() {
    let values = [Some("native append long value"), None, Some("λ")];
    let input = strings(&VarBinViewArray::from_iter_nullable_str(values));
    let denied = LiveMemoryPool::new(1).unwrap();
    let mut builder = VarBinViewBuilder::with_capacity(input.dtype().clone(), input.len());
    let error = input
        .append_to_builder(&mut builder, &mut session(&denied).create_execution_ctx())
        .unwrap_err();
    assert!(crate::owned_buffers::is_owned_reservation_denial(&error));
    assert_eq!(denied.snapshot().reserved_bytes, 0);

    let memory = LiveMemoryPool::new(1 << 20).unwrap();
    input
        .append_to_builder(&mut builder, &mut session(&memory).create_execution_ctx())
        .unwrap();
    let output = builder.finish_into_varbinview();
    assert_eq!(output.len(), 3);
    assert_eq!(output.bytes_at(0).as_slice(), values[0].unwrap().as_bytes());
    assert_eq!(output.bytes_at(2).as_slice(), values[2].unwrap().as_bytes());
    assert!(memory.snapshot().reserved_bytes > 0);
    drop(output);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

#[test]
fn native_zstd_preserves_native_cast_and_serialization_identity() {
    let input = primitive(&PrimitiveArray::from_iter(0i64..33));
    let dtype = DType::Primitive(PType::I64, Nullability::Nullable);
    let cast = input.cast(dtype.clone()).unwrap();
    assert!(cast.is::<Zstd>());
    let sliced = cast.slice(9..14).unwrap();
    assert!(sliced.is::<Zstd>());

    let control = VortexSession::default();
    let context = ArrayContext::empty();
    let mut bytes = ByteBufferMut::empty();
    for buffer in cast
        .serialize(&context, &control, &SerializeOptions::default())
        .unwrap()
    {
        bytes.extend_from_slice(buffer.as_ref());
    }
    let decoded = SerializedArray::try_from(bytes.freeze())
        .unwrap()
        .decode(
            &dtype,
            cast.len(),
            &ReadContext::new(context.to_ids()),
            &control,
        )
        .unwrap();
    assert!(decoded.is::<Zstd>());
    let memory = LiveMemoryPool::new(1 << 20).unwrap();
    let mut ctx = session(&memory).create_execution_ctx();
    let output = decoded
        .slice(9..14)
        .unwrap()
        .execute::<PrimitiveArray>(&mut ctx)
        .unwrap();
    assert_eq!(output.dtype(), &dtype);
    assert_eq!(output.as_slice::<i64>(), &[9, 10, 11, 12, 13]);
    assert!(memory.snapshot().reserved_bytes > 0);
    drop(output);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

#[test]
fn native_zstd_corrupt_frame_releases_its_admitted_payload() {
    let input = primitive(&PrimitiveArray::from_iter(0i64..33));
    let mut parts = input
        .as_::<Zstd>()
        .data()
        .clone()
        .into_parts(Validity::NonNullable);
    let frame = &parts.frames[0];
    parts.frames[0] = frame.slice(..frame.len() - 1);
    let corrupt = Zstd::try_new(
        input.dtype().clone(),
        ZstdData::new(parts.dictionary, parts.frames, parts.metadata, parts.n_rows),
        parts.validity,
    )
    .unwrap()
    .into_array();
    let memory = LiveMemoryPool::new(1 << 20).unwrap();
    let error = corrupt
        .execute::<PrimitiveArray>(&mut session(&memory).create_execution_ctx())
        .unwrap_err();
    assert!(!crate::owned_buffers::is_owned_reservation_denial(&error));
    assert!(memory.snapshot().peak_reserved_bytes > 0);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

#[test]
fn native_zstd_nullable_float_values_preserve_bits() {
    let bits = [
        0u64,
        1 << 63,
        0x7ff8_0000_0000_0123,
        0x7ff0_0000_0000_0000,
        1,
    ];
    let values = [
        Some(f64::from_bits(bits[0])),
        None,
        Some(f64::from_bits(bits[1])),
        Some(f64::from_bits(bits[2])),
        Some(f64::from_bits(bits[3])),
        Some(f64::from_bits(bits[4])),
    ];
    let encoded = primitive(&PrimitiveArray::from_option_iter(values));
    let memory = LiveMemoryPool::new(1 << 20).unwrap();
    let output = encoded
        .execute::<PrimitiveArray>(&mut session(&memory).create_execution_ctx())
        .unwrap();
    for (index, expected) in values.iter().enumerate() {
        if let Some(expected) = expected {
            assert_eq!(
                output.as_slice::<f64>()[index].to_bits(),
                expected.to_bits()
            );
        }
    }
    drop(output);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}
