use super::*;
use crate::{
    resident_memory_source::ResidentMemorySource, resident_session::ResidentVortexSession,
};
use shardloom_exec::compute_pool::CancellationToken;
use vortex::{
    VortexSessionDefault as _,
    array::{
        VortexSessionExecute as _,
        arrays::{
            BoolArray, ChunkedArray, DictArray, ExtensionArray, FixedSizeListArray, ListArray,
            PrimitiveArray, StructArray, VarBinArray,
        },
        dtype::{Nullability, PType},
        extension::datetime::{Date, TimeUnit, Timestamp},
        patches::Patches,
        scalar::Scalar,
        validity::Validity,
    },
    buffer::{Alignment, Buffer},
    encodings::{
        alp::{Exponents, RDEncoder, RDEncoderExt as _, alp_encode},
        fastlanes::{BitPackedData, FoRData},
        fsst::{fsst_compress, fsst_train_compressor},
        runend::RunEnd,
        sequence::Sequence,
        zigzag::zigzag_encode,
    },
    session::VortexSession,
};

fn record(column: ArrayRef) -> ArrayRef {
    StructArray::from_fields(&[("value", column)])
        .unwrap()
        .into_array()
}

fn scalars(array: &ArrayRef) -> Vec<Scalar> {
    let mut execution = VortexSession::default().create_execution_ctx();
    (0..array.len())
        .map(|index| array.execute_scalar(index, &mut execution).unwrap())
        .collect()
}

fn copy(array: &ArrayRef, session: &ResidentVortexSession) -> CopiedRecords {
    session
        .with_native_execution_context(&CancellationToken::default(), |context| {
            Ok(copy_records(
                std::slice::from_ref(array),
                context,
                TransferPolicy::PreserveEncodings,
            )?
            .expect("fixture must select encoded transfer"))
        })
        .unwrap()
}

fn assert_physical_tree(input: &ArrayRef, output: &ArrayRef) {
    assert_eq!(input.encoding_id(), output.encoding_id());
    assert_eq!(input.dtype(), output.dtype());
    assert_eq!(input.len(), output.len());
    assert!(!ArrayRef::ptr_eq(input, output));
    assert_eq!(input.nchildren(), output.nchildren());
    for (input, output) in input.children_iter().zip(output.children_iter()) {
        assert_physical_tree(input, output);
    }
}

#[test]
#[allow(clippy::too_many_lines)] // One table shares the same full-value and ownership checks.
fn encoded_transfer_preserves_complete_values_validity_and_physical_trees() {
    let mut execution = VortexSession::default().create_execution_ctx();
    let integers = PrimitiveArray::from_iter((0i32..33).map(|value| value % 11));
    let floats = PrimitiveArray::from_option_iter([Some(-1.25_f64), None, Some(0.0), Some(17.5)]);
    let strings = VarBinArray::from_iter(
        [
            Some("λ\0猫"),
            None,
            Some(""),
            Some("shared text shared text"),
        ],
        DType::Utf8(Nullability::Nullable),
    )
    .into_array();
    let compressor = fsst_train_compressor(&strings, &mut execution).unwrap();
    let sparse = Sparse::try_new_from_patches(
        Patches::new(
            8,
            9,
            PrimitiveArray::from_iter([10u64, 15]).into_array(),
            VarBinArray::from_iter(
                [Some("long patch"), None],
                DType::Utf8(Nullability::Nullable),
            )
            .into_array(),
            None,
        )
        .unwrap(),
        Scalar::utf8("base", Nullability::Nullable),
    )
    .unwrap()
    .into_array();
    let zstd_data =
        vortex_zstd::ZstdData::from_primitive_without_dict(&integers, 0, 8, &mut execution)
            .unwrap();
    let zstd =
        vortex_zstd::Zstd::try_new(integers.dtype().clone(), zstd_data, Validity::NonNullable)
            .unwrap()
            .into_array();
    let nested_scalar = Scalar::list(
        Arc::new(DType::Utf8(Nullability::Nullable)),
        vec![
            Scalar::utf8("nested", Nullability::Nullable),
            Scalar::null(DType::Utf8(Nullability::Nullable)),
        ],
        Nullability::NonNullable,
    );
    let fixtures = vec![
        ConstantArray::new(17i64, 23).into_array(),
        ConstantArray::new(
            Scalar::null(DType::Primitive(PType::I64, Nullability::Nullable)),
            5,
        )
        .into_array(),
        ConstantArray::new(nested_scalar, 3).into_array(),
        PrimitiveArray::from_option_iter([Some(i64::MIN), None, Some(i64::MAX)]).into_array(),
        PrimitiveArray::from_iter([] as [i64; 0]).into_array(),
        BoolArray::from_iter([Some(true), None, Some(false)]).into_array(),
        strings.clone(),
        DictArray::try_new(
            PrimitiveArray::from_iter([3u8, 1, 0, 3]).into_array(),
            strings.clone(),
        )
        .unwrap()
        .into_array(),
        RunEnd::encode(integers.clone().into_array(), &mut execution)
            .unwrap()
            .into_array(),
        BitPackedData::encode(&integers.clone().into_array(), 4, &mut execution)
            .unwrap()
            .into_array(),
        FoRData::encode(integers.clone(), &mut execution)
            .unwrap()
            .into_array(),
        zigzag_encode(integers.as_view()).unwrap().into_array(),
        Sequence::try_new_typed(-7i64, 3, Nullability::NonNullable, 23)
            .unwrap()
            .into_array(),
        alp_encode(floats.as_view(), None, &mut execution)
            .unwrap()
            .into_array(),
        RDEncoder::new(&[0.2_f64])
            .encode(floats.as_view())
            .into_array(),
        fsst_compress(&strings, &compressor, &mut execution)
            .unwrap()
            .into_array(),
        sparse,
        zstd.clone(),
        zstd.slice(9..27).unwrap(),
        ChunkedArray::try_new(
            vec![strings.clone(), strings.clone()],
            strings.dtype().clone(),
        )
        .unwrap()
        .into_array(),
        SliceArray::try_new(integers.into_array(), 3..17)
            .unwrap()
            .into_array(),
        ExtensionArray::new(
            Timestamp::new(TimeUnit::Microseconds, Nullability::Nullable).erased(),
            PrimitiveArray::from_option_iter([Some(i64::MIN), None, Some(i64::MAX)]).into_array(),
        )
        .into_array(),
        ExtensionArray::new(
            Date::new(TimeUnit::Days, Nullability::NonNullable).erased(),
            PrimitiveArray::from_iter([i32::MIN, 0, i32::MAX]).into_array(),
        )
        .into_array(),
        ListArray::try_new(
            strings.clone(),
            PrimitiveArray::from_iter([0u64, 2, 4]).into_array(),
            Validity::NonNullable,
        )
        .unwrap()
        .into_array(),
        FixedSizeListArray::try_new(strings, 2, Validity::NonNullable, 2)
            .unwrap()
            .into_array(),
    ];
    for column in fixtures {
        let input = record(column);
        let expected = scalars(&input);
        let session = ResidentVortexSession::new(32 << 20, 1).unwrap();
        let copied = copy(&input, &session);
        let output = &copied.arrays.values[0].0;
        assert_physical_tree(&input, output);
        assert_eq!(
            scalars(output),
            expected,
            "{}",
            input.nth_child(0).unwrap().encoding_id()
        );
        drop(copied);
        assert_eq!(session.snapshot().memory.reserved_bytes, 0);
    }
}

#[test]
fn encoded_transfer_copies_shared_nodes_and_exact_buffer_aliases_once() {
    let buffer = Buffer::copy_from_aligned([1i64, 2, 3, 4], Alignment::new(4096));
    let first = PrimitiveArray::new(buffer.clone(), Validity::NonNullable).into_array();
    let second = PrimitiveArray::new(buffer, Validity::NonNullable).into_array();
    let input = StructArray::from_fields(&[
        ("first", first.clone()),
        ("alias", first),
        ("other_node", second),
    ])
    .unwrap()
    .into_array();
    let session = ResidentVortexSession::new(1 << 20, 1).unwrap();
    let copied = copy(&input, &session);
    let output = &copied.arrays.values[0].0;
    assert_eq!(copied.report.encoded_buffer_bytes_copied, 32);
    assert_eq!(copied.report.encoded_buffer_copies, 1);
    assert!(copied.report.reused_array_references > 0);
    assert!(copied.report.reused_buffer_references > 0);
    assert!(ArrayRef::ptr_eq(
        &output.nth_child(0).unwrap(),
        &output.nth_child(1).unwrap()
    ));
    let first = output.nth_child(0).unwrap().buffers()[0].clone();
    let second = output.nth_child(2).unwrap().buffers()[0].clone();
    assert_eq!(first.as_ptr(), second.as_ptr());
    assert_eq!(first.alignment(), Alignment::new(4096));
    drop((copied, second));
    assert!(session.snapshot().memory.reserved_bytes >= 4096 + 32);
    drop(first);
    assert_eq!(session.snapshot().memory.reserved_bytes, 0);
}

#[test]
fn additional_encoded_providers_preserve_values_and_execute_after_transfer() {
    use vortex::array::{
        Canonical,
        arrays::{DecimalArray, ListViewArray, MaskedArray},
        dtype::DecimalDType,
    };
    use vortex::encodings::{
        bytebool::ByteBool,
        fastlanes::{Delta, RLE},
    };
    let mut execution = VortexSession::default().create_execution_ctx();
    let integers = PrimitiveArray::from_iter((0..224i32).map(|n| n * 7 - 401));
    let zstd = vortex_zstd::Zstd::from_primitive(&integers, 0, 16, &mut execution).unwrap();
    let zstd = zstd.into_array();
    assert_eq!(
        zstd.buffer_names()[0],
        "dictionary",
        "fixture must include a dictionary"
    );
    let fixtures = vec![
        ByteBool::from_option_vec(vec![Some(false), None, Some(true), Some(true)]).into_array(),
        DecimalArray::try_new(
            Buffer::copy_from([-999_999_999_999i128, 0, 77, 999_999_999_999]),
            DecimalDType::new(12, 3),
            Validity::from_iter([true, false, true, true]),
        )
        .unwrap()
        .into_array(),
        ListViewArray::try_new(
            PrimitiveArray::from_option_iter([Some(51i32), None, Some(-7), Some(90)]).into_array(),
            PrimitiveArray::from_iter([2u32, 0, 1]).into_array(),
            PrimitiveArray::from_iter([2u32, 3, 0]).into_array(),
            Validity::NonNullable,
        )
        .unwrap()
        .into_array(),
        MaskedArray::try_new(
            PrimitiveArray::from_iter([19i64, -11, 7]).into_array(),
            Validity::from_iter([true, false, true]),
        )
        .unwrap()
        .into_array(),
        RLE::encode(
            PrimitiveArray::from_option_iter(
                (0..1043u32).map(|n| if n % 19 == 0 { None } else { Some(n / 13) }),
            )
            .as_view(),
            &mut execution,
        )
        .unwrap()
        .into_array()
        .slice(17..1033)
        .unwrap(),
        Delta::try_from_primitive_array(
            &PrimitiveArray::from_iter([251u8, 3, 127, 9, 200, 0, 17]),
            &mut execution,
        )
        .unwrap()
        .into_array()
        .slice(1..6)
        .unwrap(),
        zstd.clone(),
        zstd.slice(11..207).unwrap(),
    ];
    for column in fixtures {
        let encoding = column.encoding_id();
        let expected = scalars(&column);
        let input = record(column);
        let receiver = ResidentVortexSession::new(32 << 20, 1).unwrap();
        let copied = copy(&input, &receiver);
        assert_physical_tree(&input, &copied.arrays.values[0].0);
        let escaped = copied.arrays.values[0].0.nth_child(0).unwrap();
        drop((input, copied));
        assert!(receiver.snapshot().memory.reserved_bytes > 0);
        assert_eq!(scalars(&escaped), expected, "{encoding}");
        let canonical = escaped
            .clone()
            .execute::<Canonical>(&mut execution)
            .unwrap()
            .into_array();
        assert_eq!(scalars(&canonical), expected, "{encoding}");
        drop((canonical, escaped));
        assert_eq!(receiver.snapshot().memory.reserved_bytes, 0);
    }
}

#[test]
fn shared_transfer_copies_the_child_without_retaining_the_producer_cache() {
    use vortex::array::{
        Canonical,
        arrays::{Shared, SharedArray, shared::SharedArrayExt as _},
    };
    let mut execution = VortexSession::default().create_execution_ctx();
    let shared = SharedArray::new(ConstantArray::new(29i64, 73).into_array());
    let original_cached = shared
        .get_or_compute(|source| source.clone().execute::<Canonical>(&mut execution))
        .unwrap();
    let input = record(shared.into_array());
    let receiving = ResidentVortexSession::new(1 << 20, 1).unwrap();
    let copied = copy(&input, &receiving);
    let output = copied.arrays.values[0].0.nth_child(0).unwrap();
    assert_physical_tree(&input, &copied.arrays.values[0].0);
    assert_eq!(copied.report.encoded_buffer_bytes_copied, 0);
    let mut computed = false;
    let cached = output
        .as_::<Shared>()
        .get_or_compute(|source| {
            computed = true;
            source.clone().execute::<Canonical>(&mut execution)
        })
        .unwrap();
    assert!(computed, "receiving cache must start empty");
    assert!(!ArrayRef::ptr_eq(&original_cached, &cached));
    assert_eq!(scalars(&cached), vec![Scalar::from(29i64); 73]);
    drop((input, original_cached, copied, output, cached));
    assert_eq!(receiving.snapshot().memory.reserved_bytes, 0);
}

#[test]
fn record_chunks_and_slices_preserve_encoded_fields_and_share_one_transfer() {
    fn count_dictionaries(array: &ArrayRef) -> usize {
        usize::from(array.encoding_id().as_str() == "vortex.dict")
            + array.children_iter().map(count_dictionaries).sum::<usize>()
    }
    let values = VarBinArray::from_iter(
        [Some("shared dictionary λ".repeat(1024)), None],
        DType::Utf8(Nullability::Nullable),
    )
    .into_array();
    let codes = PrimitiveArray::from_iter([0u8, 1, 0, 0]).into_array();
    let rows = record(DictArray::try_new(codes, values).unwrap().into_array());
    let nested = ChunkedArray::try_new([rows.clone(), rows.clone()], rows.dtype().clone())
        .unwrap()
        .into_array();
    let whole = ChunkedArray::try_new([nested, rows.clone()], rows.dtype().clone())
        .unwrap()
        .into_array();
    let input = SliceArray::try_new(whole, 1..11).unwrap().into_array();
    let expected = scalars(&input);
    let original = ResidentVortexSession::new(4 << 20, 1).unwrap();
    let receiving = ResidentVortexSession::new(4 << 20, 1).unwrap();
    let result = original
        .own_completed_arrays(vec![input], original.memory().reserve(4096).unwrap())
        .unwrap();
    let source = ResidentMemorySource::from_completed_result_in_session(
        &receiving,
        &result,
        &CancellationToken::default(),
    )
    .unwrap();
    drop((result, rows));
    assert_eq!(original.snapshot().memory.reserved_bytes, 0);
    assert_eq!(source.row_count(), 10);
    assert_eq!(scalars(&source.0.array), expected);
    let receipt = source.intake_transfer_report();
    assert_eq!(receipt.materialized_batches, 0);
    assert_eq!(receipt.materialized_rows, 0);
    assert!(receipt.reused_array_references > 0);
    assert!(receipt.encoded_buffer_bytes_copied < 24 << 10);
    assert_eq!(count_dictionaries(&source.0.array), 3);
    let same_owner = source
        .in_session(&receiving, &CancellationToken::default())
        .unwrap();
    assert!(ArrayRef::ptr_eq(&source.0.array, &same_owner.0.array));
    assert_eq!(same_owner.intake_transfer_report(), receipt);
    drop((source, same_owner));
    assert_eq!(receiving.snapshot().memory.reserved_bytes, 0);
}

#[test]
fn empty_record_views_do_not_copy_the_unused_parent_payload() {
    let input = record(PrimitiveArray::from_iter(0i64..1_000_000).into_array());
    let schema = input.dtype().clone();
    let input = SliceArray::try_new(input, 9..9).unwrap().into_array();
    let session = ResidentVortexSession::new(128 << 10, 1).unwrap();
    let copied = copy(&input, &session);
    assert_eq!(copied.arrays.values.len(), 1);
    assert_eq!(copied.arrays.values[0].0.dtype(), &schema);
    assert_eq!(copied.arrays.values[0].0.len(), 0);
    assert_eq!(copied.report.encoded_buffer_bytes_copied, 0);
    drop(copied);
    assert_eq!(session.snapshot().memory.reserved_bytes, 0);
}

#[test]
fn constant_transfer_remains_compact_and_escaped_slices_keep_receiving_credit() {
    let original = ResidentVortexSession::new(1 << 20, 1).unwrap();
    let receiving = ResidentVortexSession::new(1 << 20, 1).unwrap();
    let producer_credit = Arc::new(original.memory().reserve(4096).unwrap());
    let child = ConstantArray::new(11i64, 1_000_003)
        .into_array()
        .with_retained_owner(producer_credit)
        .unwrap();
    let input = record(child);
    let result = original
        .own_completed_arrays(vec![input], original.memory().reserve(4096).unwrap())
        .unwrap();
    let copied = ResidentMemorySource::from_completed_result_in_session(
        &receiving,
        &result,
        &CancellationToken::default(),
    )
    .unwrap();
    assert_eq!(copied.row_count(), 1_000_003);
    assert_eq!(copied.intake_columnar_buffer_bytes_copied(), 0);
    assert!(receiving.snapshot().memory.reserved_bytes < 64 << 10);
    drop(result);
    assert_eq!(original.snapshot().memory.reserved_bytes, 0);
    let escaped = copied
        .0
        .array
        .nth_child(0)
        .unwrap()
        .slice(101..108)
        .unwrap()
        .slice(2..5)
        .unwrap();
    drop(copied);
    assert!(receiving.snapshot().memory.reserved_bytes > 0);
    assert_eq!(scalars(&escaped), vec![Scalar::from(11i64); 3]);
    drop(escaped);
    assert_eq!(receiving.snapshot().memory.reserved_bytes, 0);
}

#[test]
fn encoded_transfer_admits_constant_tuple_metadata_before_construction() {
    let scalar = Scalar::list(
        Arc::new(DType::Primitive(PType::I64, Nullability::NonNullable)),
        vec![Scalar::from(7i64); 8192],
        Nullability::NonNullable,
    );
    let input = record(ConstantArray::new(scalar, 2).into_array());
    let receiving = ResidentVortexSession::new(64 << 10, 1).unwrap();
    let result =
        receiving.with_native_execution_context(&CancellationToken::default(), |context| {
            copy_records(
                std::slice::from_ref(&input),
                context,
                TransferPolicy::PreserveEncodings,
            )
        });
    assert!(result.is_err());
    assert!(receiving.snapshot().memory.denied_reservations > 0);
    assert_eq!(receiving.snapshot().memory.reserved_bytes, 0);
}

#[test]
fn encoded_transfer_reuses_a_shared_graph_across_distinct_completed_batches() {
    let input = record(PrimitiveArray::from_iter([2i64, 5, 9]).into_array());
    let receiving = ResidentVortexSession::new(1 << 20, 1).unwrap();
    let copied = receiving
        .with_native_execution_context(&CancellationToken::default(), |context| {
            Ok(copy_records(
                &[input.clone(), input.clone()],
                context,
                TransferPolicy::PreserveEncodings,
            )?
            .unwrap())
        })
        .unwrap();
    assert_eq!(copied.report.encoded_buffer_bytes_copied, 24);
    assert!(ArrayRef::ptr_eq(
        &copied.arrays.values[0].0,
        &copied.arrays.values[1].0
    ));
    assert_eq!(scalars(&copied.arrays.values[0].0), scalars(&input));
    drop(copied);
    assert_eq!(receiving.snapshot().memory.reserved_bytes, 0);
}

#[test]
fn compact_transfer_selects_bounded_integer_and_boolean_materialization() {
    use crate::resident_memory_source::MemoryBatchSourceBuilder;
    use vortex::encodings::bytebool::ByteBool;

    let mut execution = VortexSession::default().create_execution_ctx();
    let primitive = PrimitiveArray::from_iter(0i64..128).into_array();
    let chunked = ChunkedArray::try_new(vec![primitive.clone()], primitive.dtype().clone())
        .unwrap()
        .into_array();
    let integers = PrimitiveArray::from_iter((0i32..8192).map(|value| value % 31)).into_array();
    let packed = BitPackedData::encode(&integers, 5, &mut execution)
        .unwrap()
        .into_array();
    let nullable = PrimitiveArray::from_option_iter(
        (0i32..8192).map(|value| (value % 7 != 0).then_some(value % 31)),
    )
    .into_array();
    let nullable = BitPackedData::encode(&nullable, 5, &mut execution)
        .unwrap()
        .into_array();
    let boolean = ByteBool::from_option_vec(
        (0..17)
            .map(|row| (row % 7 != 0).then_some(row % 2 == 0))
            .collect(),
    )
    .into_array();
    for (column, maximum_bytes) in [
        (chunked, 1024),
        (packed.slice(0..17).unwrap(), 68),
        (nullable.slice(3..20).unwrap(), 71),
        (boolean, 6),
    ] {
        let input = record(column);
        let expected = scalars(&input);
        let receiving = ResidentVortexSession::new(8 << 20, 1).unwrap();
        let mut builder =
            MemoryBatchSourceBuilder::new(&receiving, CancellationToken::default()).unwrap();
        builder.push_native_copy(&input).unwrap();
        let source = builder.finish().unwrap();
        assert_eq!(
            source.intake_transfer_report().materialized_rows,
            input.len() as u64
        );
        assert_eq!(
            source.intake_transfer_report().encoded_buffer_bytes_copied,
            0
        );
        assert!(source.intake_columnar_buffer_bytes_copied() <= maximum_bytes);
        drop(input);
        assert_eq!(scalars(&source.0.array), expected);
        drop(source);
        assert_eq!(receiving.snapshot().memory.reserved_bytes, 0);
    }
}

#[test]
fn compact_transfer_counts_shared_buffers_once_before_selecting_representation() {
    let column = PrimitiveArray::from_iter(0i64..128).into_array();
    let input = StructArray::from_fields(&[("first", column.clone()), ("second", column)])
        .unwrap()
        .into_array();
    let receiving = ResidentVortexSession::new(8 << 20, 1).unwrap();
    let copied = receiving
        .with_native_execution_context(&CancellationToken::default(), |context| {
            Ok(copy_records(
                &[input.clone(), input.clone()],
                context,
                TransferPolicy::MinimizeFixedWidthCopies,
            )?
            .unwrap())
        })
        .unwrap();
    assert_eq!(copied.report.encoded_buffer_bytes_copied, 1024);
    assert_eq!(copied.report.materialized_rows, 0);
    assert!(copied.report.reused_array_references >= 2);
    assert_eq!(scalars(&copied.arrays.values[0].0), scalars(&input));
    drop(copied);
    assert_eq!(receiving.snapshot().memory.reserved_bytes, 0);
}

#[test]
fn compact_transfer_does_not_narrow_float_semantics_to_the_materializer() {
    let values = [f64::NEG_INFINITY, -0.0, f64::NAN, f64::INFINITY];
    let column = PrimitiveArray::from_iter(values).into_array();
    let chunked = ChunkedArray::try_new(vec![column.clone()], column.dtype().clone())
        .unwrap()
        .into_array();
    let input = record(chunked);
    let receiving = ResidentVortexSession::new(8 << 20, 1).unwrap();
    let copied = receiving
        .with_native_execution_context(&CancellationToken::default(), |context| {
            Ok(copy_records(&[input], context, TransferPolicy::MinimizeFixedWidthCopies)?.unwrap())
        })
        .unwrap();
    assert_eq!(copied.report.materialized_rows, 0);
    let output = crate::local_primitives::logical_field_from_native_array(
        &copied.arrays.values[0].0,
        "value",
    )
    .unwrap();
    let mut execution = VortexSession::default().create_execution_ctx();
    for (row, expected) in values.iter().enumerate() {
        let actual = f64::try_from(&output.execute_scalar(row, &mut execution).unwrap()).unwrap();
        assert_eq!(actual.to_bits(), expected.to_bits());
    }
    drop((copied, output));
    assert_eq!(receiving.snapshot().memory.reserved_bytes, 0);
}

fn sliced_patches(values: ArrayRef) -> Patches {
    let patches = Patches::new(
        3000,
        0,
        PrimitiveArray::from_iter([10u64, 100, 1030, 1190, 1510, 2160]).into_array(),
        values,
        Some(PrimitiveArray::from_iter([0u64, 2, 5]).into_array()),
    )
    .unwrap();
    let sliced = patches.slice(50..2250).unwrap().unwrap();
    assert_eq!(sliced.offset_within_chunk(), Some(1));
    sliced
}

#[test]
fn encoded_transfer_preserves_sliced_patch_caches_and_alprd_dictionary_ownership() {
    let patches = sliced_patches(PrimitiveArray::from_iter([2.25f64; 6]).into_array());
    let sparse = Sparse::try_new_from_patches(patches.clone(), Scalar::from(0.125f64))
        .unwrap()
        .into_array();
    let alp = ALP::try_new(
        PrimitiveArray::from_iter(std::iter::repeat_n(1i64, 2200)).into_array(),
        Exponents { e: 0, f: 0 },
        Some(patches),
    )
    .unwrap()
    .into_array();
    let mut execution = VortexSession::default().create_execution_ctx();
    let integers = PrimitiveArray::from_iter((0..3000).map(|i| {
        if [10, 100, 1030, 1190, 1510, 2160].contains(&i) {
            127i32
        } else {
            1
        }
    }))
    .into_array();
    let packed = BitPackedData::encode(&integers, 2, &mut execution)
        .unwrap()
        .into_array()
        .slice(50..2250)
        .unwrap();
    let alprd = ALPRD::try_new(
        DType::Primitive(PType::F64, Nullability::NonNullable),
        PrimitiveArray::from_iter([0u16; 2200]).into_array(),
        Buffer::copy_from_aligned([0x3ff0u16], Alignment::new(64)),
        PrimitiveArray::from_iter(std::iter::repeat_n(0u64, 2200)).into_array(),
        48,
        Some(sliced_patches(
            PrimitiveArray::from_iter([0x4000u16; 6]).into_array(),
        )),
    )
    .unwrap()
    .into_array();
    for column in [sparse, alp, packed, alprd] {
        let input = record(column.clone());
        let expected = scalars(&column);
        let receiving = ResidentVortexSession::new(8 << 20, 1).unwrap();
        let copied = copy(&input, &receiving);
        let output = copied.arrays.values[0].0.nth_child(0).unwrap();
        assert_physical_tree(&column, &output);
        assert_eq!(scalars(&output), expected);
        let canonical = output
            .clone()
            .execute::<PrimitiveArray>(&mut execution)
            .unwrap();
        assert_eq!(scalars(&canonical.into_array()), expected);
        if output.is::<ALPRD>() {
            let dictionary = output.as_::<ALPRD>().left_parts_dictionary().clone();
            assert_eq!(dictionary.alignment(), Alignment::new(64));
            drop((copied, output));
            assert!(receiving.snapshot().memory.reserved_bytes > 0);
            assert_eq!(dictionary.as_slice(), &[0x3ff0]);
            drop(dictionary);
        } else {
            drop((copied, output));
        }
        assert_eq!(receiving.snapshot().memory.reserved_bytes, 0);
    }
}

#[test]
fn encoded_transfer_partial_allocation_denial_releases_completed_children() {
    let input = StructArray::from_fields(&[
        (
            "first",
            PrimitiveArray::from_iter(std::iter::repeat_n(7i64, 4096)).into_array(),
        ),
        (
            "second",
            PrimitiveArray::from_iter(std::iter::repeat_n(9i64, 4096)).into_array(),
        ),
    ])
    .unwrap()
    .into_array();
    let receiving = ResidentVortexSession::new(80 << 10, 1).unwrap();
    let result =
        receiving.with_native_execution_context(&CancellationToken::default(), |context| {
            copy_records(
                std::slice::from_ref(&input),
                context,
                TransferPolicy::PreserveEncodings,
            )
        });
    assert!(result.is_err());
    let memory = receiving.snapshot().memory;
    assert!(memory.denied_reservations > 0);
    assert!(memory.peak_reserved_bytes > 32 << 10);
    assert_eq!(memory.reserved_bytes, 0);
    let small = record(PrimitiveArray::from_iter([3i64]).into_array());
    drop(copy(&small, &receiving));
    assert_eq!(receiving.snapshot().memory.reserved_bytes, 0);
}

#[test]
fn encoded_transfer_cancellation_between_byte_windows_releases_partial_allocation() {
    let receiving = ResidentVortexSession::new(2 << 20, 1).unwrap();
    let cancellation = CancellationToken::default();
    let input = Buffer::copy_from(vec![13u8; 256 << 10]);
    let result: Result<()> = receiving.with_native_execution_context(&cancellation, |context| {
        let mut copier = Copier {
            context,
            metadata: Arc::new(context.memory().reserve(4096)?),
            arrays: BTreeMap::new(),
            buffers: BTreeMap::new(),
            report: NativeTransferReport::default(),
        };
        copier.copy_buffer(&Buffer::copy_from([1u8; 8]))?;
        let mut checkpoints = 0;
        let error = copier
            .copy_buffer_with_check(&input, || {
                checkpoints += 1;
                if checkpoints == 3 {
                    // Admission and one 64 KiB copy have already completed.
                    assert!(context.memory().snapshot().reserved_bytes > 256 << 10);
                    cancellation.cancel();
                }
                context.check_cancelled()
            })
            .unwrap_err();
        assert!(error.to_string().contains("cancel"));
        assert_eq!(checkpoints, 3);
        assert_eq!(copier.buffers.len(), 1);
        assert_eq!(copier.report.encoded_buffer_bytes_copied, 8);
        assert!(context.memory().snapshot().reserved_bytes < 16 << 10);
        Err(error)
    });
    assert!(result.is_err());
    assert_eq!(receiving.snapshot().memory.reserved_bytes, 0);
    let small = record(PrimitiveArray::from_iter([3i64]).into_array());
    drop(copy(&small, &receiving));
    assert_eq!(receiving.snapshot().memory.reserved_bytes, 0);
}
