use super::*;
use vortex::{
    VortexSessionDefault as _,
    array::{
        Canonical, VortexSessionExecute as _,
        arrays::{ChunkedArray, ConstantArray, DecimalArray, DictArray, slice::SliceKernel as _},
        dtype::{DecimalDType, Nullability, PType},
        memory::MemorySessionExt as _,
        patches::Patches,
        scalar::Scalar,
    },
    buffer::{BitBuffer, Buffer, buffer},
    encodings::{
        alp::{ALPRDArrayExt as _, Exponents},
        fastlanes::{BitPackedData, FoRData},
        zigzag::zigzag_encode,
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

fn assert_values(actual: &ArrayRef, expected: &ArrayRef) {
    assert_eq!(actual.dtype(), expected.dtype());
    assert_eq!(actual.len(), expected.len());
    let mut ctx = VortexSession::default().create_execution_ctx();
    for row in 0..actual.len() {
        assert_eq!(
            actual.execute_scalar(row, &mut ctx).unwrap(),
            expected.execute_scalar(row, &mut ctx).unwrap(),
            "row {row}"
        );
    }
}

/// The expected scratch comes from the pinned allocation contract, independently
/// of the estimator. Exercise native registry execution with two physical leaves.
fn check_case(encoded: ArrayRef, expected: &ArrayRef, scratch: u64) {
    assert_eq!(
        workspace(&encoded, Mode::Append, 0).unwrap().unwrap().bytes,
        scratch
    );
    let input = chunks(vec![
        expected.clone(),
        chunks(vec![encoded.clone(), encoded]),
    ]);
    let rows = expected.len() * 3;
    let output_bytes = value_capacity(expected.dtype(), rows).unwrap();
    let validity_bytes = if expected.dtype().is_nullable() {
        bitmap_capacity(rows).unwrap()
    } else {
        0
    };
    let output_and_finish = output_bytes + validity_bytes + 256;
    let grant = output_and_finish + scratch;
    let denied = LiveMemoryPool::new(grant - 1).unwrap();
    let error = input
        .clone()
        .execute::<Canonical>(&mut session(&denied).create_execution_ctx())
        .unwrap_err();
    assert!(
        crate::owned_buffers::is_owned_reservation_denial(&error),
        "{error}"
    );
    assert_eq!(denied.snapshot().reserved_bytes, 0);
    assert_eq!(
        denied.snapshot().peak_reserved_bytes,
        if scratch == 0 { 0 } else { output_and_finish }
    );

    let memory = LiveMemoryPool::new(grant).unwrap();
    for _ in 0..2 {
        let output = input
            .clone()
            .execute::<Canonical>(&mut session(&memory).create_execution_ctx())
            .unwrap()
            .into_array();
        for part in 0..3 {
            assert_values(
                &output
                    .slice(part * expected.len()..(part + 1) * expected.len())
                    .unwrap(),
                expected,
            );
        }
        assert_eq!(memory.snapshot().peak_reserved_bytes, grant);
        // Scratch and finish are gone. Retained values/validity follow final
        // buffer ownership, including an independent byte-buffer slice.
        let values = output.buffers()[0].clone();
        let retained = memory.snapshot().reserved_bytes;
        assert!(retained == output_bytes || retained == output_bytes + validity_bytes);
        assert_eq!(
            retained,
            output_bytes
                + if matches!(output.validity().unwrap(), Validity::Array(_)) {
                    validity_bytes
                } else {
                    0
                }
        );
        let slice = values.slice(..values.len().min(1));
        drop(output);
        drop(values);
        assert_eq!(memory.snapshot().reserved_bytes, output_bytes);
        drop(slice);
        assert_eq!(memory.snapshot().reserved_bytes, 0);
    }
}

#[test]
fn native_decoder_direct_bitpacked_and_patched_append_need_no_full_child_buffer() {
    let mut ctx = VortexSession::default().create_execution_ctx();
    let values = PrimitiveArray::from_iter((0..2051i32).map(|row| row % 13));
    let encoded = BitPackedData::encode(&values.clone().into_array(), 4, &mut ctx)
        .unwrap()
        .into_array();
    check_case(encoded.clone(), &values.clone().into_array(), 0);
    let patches = Patches::new(
        values.len(),
        0,
        buffer![1u32, 1027, 2050].into_array(),
        buffer![-91i32, 71_337, i32::MAX].into_array(),
        None,
    )
    .unwrap();
    let patched = Patched::from_array_and_patches(encoded, &patches, &mut ctx)
        .unwrap()
        .into_array();
    let mut expected = values.as_slice::<i32>().to_vec();
    expected[1] = -91;
    expected[1027] = 71_337;
    expected[2050] = i32::MAX;
    check_case(
        patched,
        &PrimitiveArray::from_iter(expected).into_array(),
        0,
    );
}

#[test]
fn native_decoder_for_fusion_admits_one_builder_without_decoding_bitpacked_twice() {
    let mut ctx = VortexSession::default().create_execution_ctx();
    let values = PrimitiveArray::from_iter((0..1031u64).map(|row| row % 13));
    let packed = BitPackedData::encode(&values.clone().into_array(), 4, &mut ctx)
        .unwrap()
        .into_array();
    let encoded = FoR::try_new(packed, 10_000u64.into()).unwrap().into_array();
    let expected =
        PrimitiveArray::from_iter(values.as_slice::<u64>().iter().map(|v| v + 10_000)).into_array();
    check_case(encoded, &expected, 1031 * 8 + 256 + 256);
}

#[test]
fn native_decoder_delta_uses_padded_physical_extent_including_slices() {
    let mut ctx = VortexSession::default().create_execution_ctx();
    let values = PrimitiveArray::from_iter((0..1031i32).map(|v| v * 3));
    let encoded = Delta::try_from_primitive_array(&values, &mut ctx)
        .unwrap()
        .into_array();
    assert_eq!(encoded.as_::<Delta>().deltas().len(), 2048);
    check_case(
        encoded.clone(),
        &values.clone().into_array(),
        2048 * 4 + 256,
    );
    let sliced = encoded.slice(1010..1031).unwrap();
    assert!(sliced.is::<Delta>());
    assert_eq!(sliced.as_::<Delta>().deltas().len(), 2048);
    check_case(
        sliced,
        &values.into_array().slice(1010..1031).unwrap(),
        2048 * 4 + 256,
    );
}

#[test]
fn native_decoder_sliced_rle_admits_touched_chunks_and_offset_collection() {
    let mut ctx = VortexSession::default().create_execution_ctx();
    let values = PrimitiveArray::from_iter((0..2077i32).map(|v| v / 7));
    let full = RLE::encode(values.as_view(), &mut ctx).unwrap();
    let encoded = RLE::slice(full.as_view(), 1010..1043, &mut ctx)
        .unwrap()
        .unwrap();
    assert!(encoded.is::<RLE>());
    assert_eq!(encoded.as_::<RLE>().values_idx_offsets().len(), 2);
    check_case(
        encoded,
        &values.into_array().slice(1010..1043).unwrap(),
        2048 * 4 + 256 + 2 * 8,
    );
}

#[test]
fn native_decoder_integer_physical_trees_preserve_values() {
    let mut ctx = VortexSession::default().create_execution_ctx();
    let values = PrimitiveArray::from_iter((0..37i32).map(|v| 10_000 + v * 3));
    check_case(
        FoRData::encode(values.clone(), &mut ctx)
            .unwrap()
            .into_array(),
        &values.clone().into_array(),
        37 * 4 + 256,
    );
    check_case(
        zigzag_encode(values.as_view()).unwrap().into_array(),
        &values.clone().into_array(),
        37 * 4 + 256,
    );
    check_case(
        RunEnd::encode(values.clone().into_array(), &mut ctx)
            .unwrap()
            .into_array(),
        &values.clone().into_array(),
        37 * 4 + 256,
    );
    check_case(
        Sequence::try_new_typed(10_000i32, 3, Nullability::NonNullable, 37)
            .unwrap()
            .into_array(),
        &values.into_array(),
        37 * 4 + 256,
    );
}

#[test]
fn native_decoder_copy_on_write_preserves_strong_alignment_and_source_values() {
    let values = PrimitiveArray::new(
        Buffer::copy_from_aligned([0u64, 1, 2, 3, 4, 5, 6, 7, 8], Alignment::new(4096)),
        Validity::NonNullable,
    )
    .into_array();
    let encoded = ZigZag::try_new(values.clone()).unwrap().into_array();
    check_case(
        encoded,
        &PrimitiveArray::from_iter([0i64, -1, 1, -2, 2, -3, 3, -4, 4]).into_array(),
        9 * 8 + 4096,
    );
    assert_eq!(
        values.as_::<Primitive>().as_slice::<u64>(),
        &[0, 1, 2, 3, 4, 5, 6, 7, 8]
    );
}

#[test]
fn native_decoder_sparse_shift_and_dense_fill_are_both_reserved() {
    let patches = Patches::new(
        9,
        7,
        buffer![7u64, 11, 15].into_array(),
        buffer![3i32, -17, 91].into_array(),
        None,
    )
    .unwrap();
    let sparse = Sparse::try_new_from_patches(patches, 0i32.into())
        .unwrap()
        .into_array();
    check_case(
        sparse,
        &PrimitiveArray::from_iter([3i32, 0, 0, 0, -17, 0, 0, 0, 91]).into_array(),
        2 * (9 * 4 + 256) + 3 * 8 + 256,
    );
}

#[test]
fn native_decoder_alp_patches_and_alprd_parts_reserve_cow_overlap() {
    let patches = Patches::new(
        9,
        0,
        buffer![1u32, 8].into_array(),
        buffer![-0.125f64, 91.25].into_array(),
        None,
    )
    .unwrap();
    let alp = ALP::try_new(
        PrimitiveArray::from_iter(0..9i64).into_array(),
        Exponents { e: 0, f: 0 },
        Some(patches),
    )
    .unwrap()
    .into_array();
    check_case(
        alp,
        &PrimitiveArray::from_iter([0.0f64, -0.125, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 91.25])
            .into_array(),
        2 * (9 * 8 + 256),
    );

    // Explicit high/low halves give a deterministic patched ALPRD input.
    let bits = [1.0f64.to_bits(), (-2.5f64).to_bits(), 3.25f64.to_bits()];
    let left: Vec<u16> = bits
        .iter()
        .map(|bits| u16::try_from(bits >> 48).unwrap())
        .collect();
    let patches = Patches::new(
        3,
        0,
        buffer![1u32, 2].into_array(),
        Buffer::copy_from(&left[1..]).into_array(),
        None,
    )
    .unwrap();
    let rd = ALPRD::try_new(
        DType::Primitive(PType::F64, Nullability::NonNullable),
        buffer![0u16; 3].into_array(),
        buffer![left[0]],
        PrimitiveArray::from_iter(bits.map(|b| b & ((1u64 << 48) - 1))).into_array(),
        48,
        Some(patches),
    )
    .unwrap()
    .into_array();
    assert!(rd.as_::<ALPRD>().left_parts_patches().is_some());
    check_case(
        rd,
        &PrimitiveArray::from_iter([1.0f64, -2.5, 3.25]).into_array(),
        3 * 2 + 256 + 3 * 8 + 256,
    );
}

#[test]
fn native_decoder_boolean_dictionary_counts_small_source_and_whole_output_words() {
    for source_rows in [1usize, 7, 65, 4096, 4097] {
        for output_rows in [1usize, 7, 8, 63, 64, 65] {
            let values = BoolArray::new(
                (0..source_rows + 3)
                    .map(|v| v % 3 == 0)
                    .collect::<BitBuffer>()
                    .slice(3..source_rows + 3),
                Validity::NonNullable,
            )
            .into_array();
            let indices: Vec<_> = (0..output_rows)
                .map(|v| u32::try_from(v % source_rows).unwrap())
                .collect();
            let expected = BoolArray::new(
                indices.iter().map(|v| v % 3 == 0).collect(),
                Validity::NonNullable,
            )
            .into_array();
            let dict = DictArray::try_new(PrimitiveArray::from_iter(indices).into_array(), values)
                .unwrap()
                .into_array();
            let collection = if source_rows <= 4096 {
                source_rows.max(8) as u64
            } else {
                0
            };
            check_case(
                dict,
                &expected,
                output_rows.div_ceil(64) as u64 * 8 + 256 + collection,
            );
        }
    }
}

#[test]
fn native_decoder_dictionary_preserves_decimal_physical_width_and_gather_alignment() {
    let dtype = DecimalDType::new(18, 2);
    let values = DecimalArray::new(
        Buffer::copy_from_aligned([123i128, -456, 789], Alignment::new(4096)),
        dtype,
        Validity::NonNullable,
    )
    .into_array();
    let dict = DictArray::try_new(buffer![2u32, 0, 2, 1].into_array(), values)
        .unwrap()
        .into_array();
    check_case(
        dict,
        &DecimalArray::new(
            buffer![789i128, 123, 789, -456],
            dtype,
            Validity::NonNullable,
        )
        .into_array(),
        4 * 16 + 4096 + 4 * 16 + 256,
    );
}

#[test]
fn native_decoder_unreviewed_trees_are_exclusions_without_changing_execution() {
    let mut ctx = VortexSession::default().create_execution_ctx();
    let nullable = PrimitiveArray::from_option_iter([Some(7i32), None, Some(9)]);
    let nullable_values = nullable.clone().into_array();
    let validity = RunEnd::try_new(
        buffer![3u32].into_array(),
        BoolArray::new(BitBuffer::new_set(1), Validity::NonNullable).into_array(),
        &mut ctx,
    )
    .unwrap()
    .into_array();
    let encoded_validity =
        PrimitiveArray::new(buffer![7i32, 8, 9], Validity::Array(validity)).into_array();
    let general_dict = DictArray::try_new(
        buffer![2u32, 0, 1].into_array(),
        Sequence::try_new_typed(7i32, 1, Nullability::NonNullable, 3)
            .unwrap()
            .into_array(),
    )
    .unwrap()
    .into_array();
    let nullable_dict =
        DictArray::try_new(buffer![2u32, 0, 1].into_array(), nullable_values.clone())
            .unwrap()
            .into_array();
    let nested = FoR::try_new(
        chunks(vec![
            buffer![0i32, 1].into_array(),
            buffer![2i32].into_array(),
        ]),
        7i32.into(),
    )
    .unwrap()
    .into_array();
    let nullable_delta = Delta::try_from_primitive_array(&nullable, &mut ctx)
        .unwrap()
        .into_array();
    for (case, input) in [
        ("encoded validity", encoded_validity),
        ("general dictionary", general_dict),
        ("nullable dictionary", nullable_dict),
        ("nested FoR", nested),
        ("nullable delta", nullable_delta),
    ] {
        assert!(
            workspace(&input, Mode::Append, 0).unwrap().is_none(),
            "unreviewed decoder case {case} unexpectedly admitted"
        );
        let expected = input
            .clone()
            .execute::<Canonical>(&mut ctx)
            .unwrap()
            .into_array();
        let combined = chunks(vec![expected.clone(), input]);
        let memory = LiveMemoryPool::new(1 << 20).unwrap();
        let output = combined
            .execute::<Canonical>(&mut session(&memory).create_execution_ctx())
            .unwrap()
            .into_array();
        assert_values(
            &output.slice(expected.len()..2 * expected.len()).unwrap(),
            &expected,
        );
        drop(output);
        assert_eq!(memory.snapshot().reserved_bytes, 0);
    }
    assert!(
        workspace(&nullable_values, Mode::Append, 24)
            .unwrap()
            .is_none()
    );
    assert_eq!(
        workspace(&ConstantArray::new(7i32, 0).into_array(), Mode::Append, 0)
            .unwrap()
            .unwrap()
            .bytes,
        0
    );
    assert_eq!(
        workspace(
            &ConstantArray::new(Scalar::null(DType::Bool(Nullability::Nullable)), 3).into_array(),
            Mode::Append,
            0
        )
        .unwrap()
        .unwrap()
        .bytes,
        0
    );
}

#[test]
fn native_decoder_nullable_bitpacked_and_boolean_runend_use_native_append() {
    let mut ctx = VortexSession::default().create_execution_ctx();
    for values in [
        PrimitiveArray::from_option_iter((0..37i32).map(|v| (v % 7 != 0).then_some(v % 13))),
        PrimitiveArray::new(buffer![0i32; 37], Validity::AllInvalid),
        PrimitiveArray::new(buffer![3i32; 37], Validity::AllValid),
    ] {
        let packed = BitPackedData::encode(&values.clone().into_array(), 4, &mut ctx)
            .unwrap()
            .into_array();
        check_case(packed, &values.into_array(), 0);
    }
    let runend = RunEnd::try_new(
        buffer![7u32, 33, 65].into_array(),
        BoolArray::new(
            [true, false, true].into_iter().collect(),
            Validity::NonNullable,
        )
        .into_array(),
        &mut ctx,
    )
    .unwrap()
    .into_array();
    check_case(
        runend,
        &BoolArray::new(
            (0..65).map(|row| !(7..33).contains(&row)).collect(),
            Validity::NonNullable,
        )
        .into_array(),
        9 + 256,
    );
}

#[test]
fn native_decoder_late_error_releases_scratch_and_partially_filled_output() {
    let mut indices = vec![0u16; 1024];
    indices[777] = 2;
    let invalid = RLE::try_new(
        buffer![7i32, 9].into_array(),
        PrimitiveArray::from_iter(indices).into_array(),
        buffer![0u32].into_array(),
        0,
        1024,
    )
    .unwrap()
    .into_array();
    let expected_error = invalid
        .clone()
        .execute::<Canonical>(&mut VortexSession::default().create_execution_ctx())
        .unwrap_err()
        .to_string();
    assert!(expected_error.contains("out of bounds"), "{expected_error}");
    let input = chunks(vec![buffer![3i32; 7].into_array(), invalid]);
    let output_and_finish = 1031 * 4 + 256 + 256;
    let scratch = 1024 * 4 + 256 + 8;
    for grant in [output_and_finish + scratch - 1, output_and_finish + scratch] {
        let memory = LiveMemoryPool::new(grant).unwrap();
        let error = input
            .clone()
            .execute::<Canonical>(&mut session(&memory).create_execution_ctx())
            .unwrap_err();
        if grant == output_and_finish + scratch {
            assert_eq!(error.to_string(), expected_error);
            assert_eq!(memory.snapshot().peak_reserved_bytes, grant);
        } else {
            assert!(crate::owned_buffers::is_owned_reservation_denial(&error));
            assert_eq!(memory.snapshot().peak_reserved_bytes, output_and_finish);
        }
        assert_eq!(memory.snapshot().reserved_bytes, 0);
    }
}

#[test]
fn native_decoder_temporary_grant_respects_concurrent_owner_and_retry() {
    let encoded = Sequence::try_new_typed(7i32, 3, Nullability::NonNullable, 37)
        .unwrap()
        .into_array();
    let input = chunks(vec![buffer![0i32; 7].into_array(), encoded]);
    let output = 44 * 4 + 256;
    let scratch = 37 * 4 + 256;
    let grant = output + 256 + scratch + 776;
    let memory = LiveMemoryPool::new(grant).unwrap();
    let (ready_tx, ready_rx) = std::sync::mpsc::channel();
    let (release_tx, release_rx) = std::sync::mpsc::channel();
    std::thread::scope(|scope| {
        let worker_memory = memory.clone();
        let worker = scope.spawn(move || {
            let held = crate::owned_buffers::reserve(&worker_memory, 777).unwrap();
            ready_tx.send(()).unwrap();
            release_rx.recv().unwrap();
            drop(held);
        });
        ready_rx.recv().unwrap();
        let error = input
            .clone()
            .execute::<Canonical>(&mut session(&memory).create_execution_ctx())
            .unwrap_err();
        assert!(crate::owned_buffers::is_owned_reservation_denial(&error));
        assert_eq!(memory.snapshot().reserved_bytes, 777);
        release_tx.send(()).unwrap();
        worker.join().unwrap();
        let result = input
            .execute::<Canonical>(&mut session(&memory).create_execution_ctx())
            .unwrap()
            .into_array();
        let expected = PrimitiveArray::from_iter(
            std::iter::repeat_n(0i32, 7).chain((0..37).map(|row| 7 + row * 3)),
        )
        .into_array();
        assert_values(&result, &expected);
        assert_eq!(memory.snapshot().reserved_bytes, output);
        drop(result);
        assert_eq!(memory.snapshot().reserved_bytes, 0);
    });
}

#[test]
fn native_decoder_sparse_boolean_and_decimal_fill_keep_physical_widths_explicit() {
    let bool_patches = Patches::new(
        9,
        0,
        buffer![1u32, 8].into_array(),
        BoolArray::new(BitBuffer::new_set(2), Validity::NonNullable).into_array(),
        None,
    )
    .unwrap();
    check_case(
        Sparse::try_new_from_patches(bool_patches, false.into())
            .unwrap()
            .into_array(),
        &BoolArray::new(
            (0..9).map(|row| row == 1 || row == 8).collect(),
            Validity::NonNullable,
        )
        .into_array(),
        2 * (2 + 256),
    );
    let dtype = DecimalDType::new(18, 2);
    let mut ctx = VortexSession::default().create_execution_ctx();
    let scalar = DecimalArray::new(buffer![123i128], dtype, Validity::NonNullable)
        .into_array()
        .execute_scalar(0, &mut ctx)
        .unwrap();
    let fill = DecimalArray::new(buffer![0i128], dtype, Validity::NonNullable)
        .into_array()
        .execute_scalar(0, &mut ctx)
        .unwrap();
    let patches = Patches::new(
        9,
        0,
        buffer![1u32, 8].into_array(),
        ConstantArray::new(scalar, 2).into_array(),
        None,
    )
    .unwrap();
    check_case(
        Sparse::try_new_from_patches(patches, fill)
            .unwrap()
            .into_array(),
        &DecimalArray::new(
            (0..9)
                .map(|row| if row == 1 || row == 8 { 123i128 } else { 0 })
                .collect(),
            dtype,
            Validity::NonNullable,
        )
        .into_array(),
        // Constant patches retain i128; dense DecimalBuilder uses i64.
        2 * 16 + 256 + 2 * (9 * 8 + 256) + 256,
    );
}

#[test]
fn native_decoder_composed_children_include_canonical_finish_and_cow() {
    let mut ctx = VortexSession::default().create_execution_ctx();
    let values = PrimitiveArray::from_iter((0..37i32).map(|v| v % 13));
    let packed = BitPackedData::encode(&values.clone().into_array(), 4, &mut ctx)
        .unwrap()
        .into_array();
    let patches = Patches::new(
        37,
        0,
        buffer![1u32].into_array(),
        buffer![777i32].into_array(),
        None,
    )
    .unwrap();
    let patched = Patched::from_array_and_patches(packed, &patches, &mut ctx)
        .unwrap()
        .into_array();
    let encoded = FoR::try_new(patched, 1000i32.into()).unwrap().into_array();
    check_case(
        encoded,
        &PrimitiveArray::from_iter(
            (0..37i32).map(|row| if row == 1 { 1777 } else { row % 13 + 1000 }),
        )
        .into_array(),
        // BitPacked canonical output + finish, Patched COW, then FoR COW.
        3 * (37 * 4 + 256) + 256,
    );
    let values = PrimitiveArray::new(
        Buffer::copy_from_aligned([7i64, -17, 91], Alignment::new(4096)),
        Validity::NonNullable,
    )
    .into_array();
    let dict = DictArray::try_new(buffer![2u32, 0, 2, 1].into_array(), values)
        .unwrap()
        .into_array();
    check_case(
        dict,
        &buffer![91i64, 7, 91, -17].into_array(),
        4 * 8 + 4096 + 4 * 8 + 256,
    );
}

#[test]
fn native_decoder_nullable_alp_without_patches_borrows_validity() {
    let encoded = ALP::try_new(
        PrimitiveArray::from_option_iter([Some(3i64), None, Some(-7)]).into_array(),
        Exponents { e: 0, f: 0 },
        None,
    )
    .unwrap()
    .into_array();
    check_case(
        encoded,
        &PrimitiveArray::from_option_iter([Some(3.0f64), None, Some(-7.0)]).into_array(),
        3 * 8 + 256,
    );
}

#[test]
fn native_decoder_empty_and_deep_shapes_preserve_existing_execution() {
    assert!(Sequence::try_new_typed(7i32, 3, Nullability::NonNullable, 0).is_err());
    let empty = ConstantArray::new(7i32, 0).into_array();
    assert_eq!(
        workspace(&empty, Mode::Append, 0).unwrap().unwrap().bytes,
        0
    );
    let memory = LiveMemoryPool::new(512).unwrap();
    let input = chunks(vec![empty.clone(), empty]);
    let output = super::super::decode(
        &input,
        &mut session(&memory).create_execution_ctx(),
        &memory,
    )
    .unwrap();
    assert!(output.is_empty());
    assert_eq!(memory.snapshot().peak_reserved_bytes, 512);
    assert_eq!(memory.snapshot().reserved_bytes, 256);
    drop(output);
    assert_eq!(memory.snapshot().reserved_bytes, 0);

    let mut deep = buffer![0i32, 1, 2].into_array();
    for _ in 0..25 {
        deep = FoR::try_new(deep, 1i32.into()).unwrap().into_array();
    }
    assert!(workspace(&deep, Mode::Append, 0).unwrap().is_none());
    let input = chunks(vec![buffer![7i32].into_array(), deep]);
    let memory = LiveMemoryPool::new(1 << 20).unwrap();
    let output = input
        .execute::<Canonical>(&mut session(&memory).create_execution_ctx())
        .unwrap()
        .into_array();
    assert_values(&output, &buffer![7i32, 25, 26, 27].into_array());
    drop(output);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct UnreadableDevice(usize);

impl vortex::array::buffer::DeviceBuffer for UnreadableDevice {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn len(&self) -> usize {
        self.0
    }

    fn alignment(&self) -> Alignment {
        Alignment::new(256)
    }

    fn copy_to_host_sync(&self, _alignment: Alignment) -> VortexResult<vortex::buffer::ByteBuffer> {
        panic!("scratch inspection must not copy a device buffer");
    }

    fn copy_to_host(
        &self,
        _alignment: Alignment,
    ) -> VortexResult<futures::future::BoxFuture<'static, VortexResult<vortex::buffer::ByteBuffer>>>
    {
        panic!("scratch inspection must not schedule a device copy");
    }

    fn slice(&self, range: std::ops::Range<usize>) -> Arc<dyn vortex::array::buffer::DeviceBuffer> {
        assert!(range.start <= range.end && range.end <= self.0);
        Arc::new(Self(range.end - range.start))
    }

    fn aligned(
        self: Arc<Self>,
        alignment: Alignment,
    ) -> VortexResult<Arc<dyn vortex::array::buffer::DeviceBuffer>> {
        vortex_ensure!(*alignment <= 256, "test device alignment exceeded");
        Ok(self)
    }
}

#[test]
fn native_decoder_shallow_host_checks_exclude_device_values_children_and_validity() {
    let device = |bytes| BufferHandle::new_device(Arc::new(UnreadableDevice(bytes)));
    let primitive =
        PrimitiveArray::from_buffer_handle(device(12), PType::U32, Validity::NonNullable)
            .into_array();
    let boolean = BoolArray::new_handle(device(1), 0, 3, Validity::NonNullable).into_array();
    let decimal = DecimalArray::new_handle(
        device(48),
        DecimalType::I128,
        DecimalDType::new(18, 2),
        Validity::NonNullable,
    )
    .into_array();
    let nested = FoR::try_new(primitive.clone(), 7u32.into())
        .unwrap()
        .into_array();
    let validity =
        PrimitiveArray::new(buffer![1i32, 2, 3], Validity::Array(boolean.clone())).into_array();
    let mut ctx = VortexSession::default().create_execution_ctx();
    let packed = BitPackedData::encode(&buffer![1u32, 2, 3].into_array(), 2, &mut ctx)
        .unwrap()
        .into_array();
    let packed = Array::<BitPacked>::try_from_parts(
        BitPacked
            .with_buffers(
                packed.as_::<BitPacked>(),
                &[device(packed.buffer_handles()[0].len())],
            )
            .unwrap(),
    )
    .unwrap()
    .into_array();
    for input in [primitive, boolean, decimal, nested, validity, packed] {
        for mode in [Mode::Append, Mode::Canonical] {
            assert!(workspace(&input, mode, 0).unwrap().is_none());
        }
    }
}
