use super::*;
use vortex::mask::Mask;

fn text_fixture() -> (ArrayRef, Vec<String>) {
    let values: Vec<_> = (0..128)
        .map(|row| format!("{row:04}:{}:λ", "abcdefghi".repeat(128)))
        .collect();
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

#[test]
fn native_zstd_sparse_text_filter_skips_unused_frame_allocations() {
    let (encoded, values) = text_fixture();
    let full_memory = LiveMemoryPool::new(16 << 20).unwrap();
    let full_session = session(&full_memory);
    let full = encoded
        .clone()
        .execute::<VarBinViewArray>(&mut full_session.create_execution_ctx())
        .unwrap();
    let full_peak = full_memory.snapshot().peak_reserved_bytes;
    drop(full);
    assert_eq!(full_memory.snapshot().reserved_bytes, 0);

    let memory = LiveMemoryPool::new(16 << 20).unwrap();
    let session = session(&memory);
    let selected = [1, 3, 120, 124];
    let filtered = encoded.filter(Mask::from_indices(128, selected)).unwrap();
    let output = filtered
        .execute::<VarBinViewArray>(&mut session.create_execution_ctx())
        .unwrap();
    for (row, original) in selected.into_iter().enumerate() {
        assert_eq!(output.bytes_at(row).as_slice(), values[original].as_bytes());
    }
    assert_eq!(output.len(), selected.len());
    let sparse_peak = memory.snapshot().peak_reserved_bytes;
    assert!(
        sparse_peak + 64 * 1024 < full_peak,
        "sparse filter still decoded unused frames: sparse={sparse_peak}, full={full_peak}"
    );
    let retained = output.as_ref().buffers()[0].clone();
    drop(output);
    assert!(memory.snapshot().reserved_bytes > 0);
    drop(retained);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

#[test]
fn native_zstd_text_filter_matches_literal_rows_across_shapes() {
    for kind in 0..5 {
        let nullable = (1..=3).contains(&kind);
        let nullability = if nullable {
            Nullability::Nullable
        } else {
            Nullability::NonNullable
        };
        let dtype = if kind == 4 {
            DType::Binary(nullability)
        } else {
            DType::Utf8(nullability)
        };
        let values: Vec<Option<Vec<u8>>> = (0usize..128)
            .map(|row| {
                if kind == 3 || (kind == 2 && row.is_multiple_of(5)) {
                    None
                } else if kind == 4 {
                    Some(vec![
                        0xff,
                        0x80,
                        u8::try_from(row).unwrap(),
                        0,
                        1,
                        2,
                        3,
                        4,
                        5,
                        6,
                        7,
                        8,
                        9,
                    ])
                } else {
                    Some(format!("row-{}-λ-{}", row % 13, "text".repeat(row % 9)).into_bytes())
                }
            })
            .collect();
        let plain = VarBinViewArray::from_iter(values.iter().map(Option::as_deref), dtype.clone());
        for dictionary in [false, true] {
            let mut ctx = VortexSession::default().create_execution_ctx();
            let encoded = if dictionary {
                Zstd::from_var_bin_view(&plain, 0, 8, &mut ctx)
            } else {
                Zstd::from_var_bin_view_without_dict(&plain, 0, 8, &mut ctx)
            }
            .unwrap()
            .into_array();
            for (outer, inner) in [
                (0..128, 0..128),
                (3..125, 5..119),
                (20..80, 0..4),
                (20..20, 0..0),
            ] {
                let start = outer.start + inner.start;
                let len = inner.len();
                let sliced = encoded.slice(outer).unwrap().slice(inner).unwrap();
                for pattern in 0..5 {
                    let selected: Vec<_> = (0..len)
                        .filter(|row| match pattern {
                            0 => false,
                            1 => true,
                            2 => row.is_multiple_of(2),
                            3 => row.is_multiple_of(29),
                            _ => *row == 0 || *row + 1 == len,
                        })
                        .collect();
                    let memory = LiveMemoryPool::new(16 << 20).unwrap();
                    let session = session(&memory);
                    let mut ctx = session.create_execution_ctx();
                    let output = sliced
                        .clone()
                        .filter(Mask::from_indices(len, selected.iter().copied()))
                        .unwrap()
                        .execute::<VarBinViewArray>(&mut ctx)
                        .unwrap();
                    assert_eq!(output.dtype(), &dtype);
                    assert_eq!(output.len(), selected.len());
                    let valid = output
                        .validity()
                        .unwrap()
                        .execute_mask(output.len(), &mut ctx)
                        .unwrap();
                    for (row, source) in selected.into_iter().enumerate() {
                        let expected = &values[start + source];
                        assert_eq!(valid.value(row), expected.is_some());
                        if let Some(expected) = expected {
                            assert_eq!(output.bytes_at(row).as_slice(), expected.as_slice());
                        }
                    }
                    drop(output);
                    drop(ctx);
                    drop(session);
                    assert_eq!(memory.snapshot().reserved_bytes, 0);
                }
            }
        }
    }
}

#[test]
fn native_zstd_sparse_text_filter_denial_and_retry_release_all_payload_credit() {
    let (encoded, values) = text_fixture();
    let selected = [2, 8, 121];
    let filtered = encoded.filter(Mask::from_indices(128, selected)).unwrap();
    let denied = LiveMemoryPool::new(1).unwrap();
    let error = filtered
        .clone()
        .execute::<VarBinViewArray>(&mut session(&denied).create_execution_ctx())
        .unwrap_err();
    assert!(crate::owned_buffers::is_owned_reservation_denial(&error));
    assert_eq!(denied.snapshot().reserved_bytes, 0);
    let memory = LiveMemoryPool::new(16 << 20).unwrap();
    let output = filtered
        .execute::<VarBinViewArray>(&mut session(&memory).create_execution_ctx())
        .unwrap();
    for (row, source) in selected.into_iter().enumerate() {
        assert_eq!(output.bytes_at(row).as_slice(), values[source].as_bytes());
    }
    drop(output);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

#[test]
fn native_zstd_sparse_text_filter_round_trips_retained_native_frames() {
    let (encoded, values) = text_fixture();
    let selected = [1, 3, 113, 117];
    let filtered = encoded
        .slice(3..125)
        .unwrap()
        .filter(Mask::from_indices(122, selected))
        .unwrap();
    let control = VortexSession::default();
    // Filter is a compute array, not a serializable file encoding. Its retained
    // Zstd child must round-trip independently before reapplying the row mask.
    let filter = filtered.as_::<Filter>();
    let child = filter.child();
    let mask = filter.data().filter_mask().clone();
    let context = ArrayContext::empty();
    let mut bytes = ByteBufferMut::empty();
    for buffer in child
        .serialize(&context, &control, &SerializeOptions::default())
        .unwrap()
    {
        bytes.extend_from_slice(buffer.as_ref());
    }
    let restored = SerializedArray::try_from(bytes.freeze())
        .unwrap()
        .decode(
            child.dtype(),
            child.len(),
            &ReadContext::new(context.to_ids()),
            &control,
        )
        .unwrap();
    let memory = LiveMemoryPool::new(16 << 20).unwrap();
    let output = restored
        .filter(mask)
        .unwrap()
        .execute::<VarBinViewArray>(&mut session(&memory).create_execution_ctx())
        .unwrap();
    for (row, original) in selected.into_iter().enumerate() {
        assert_eq!(
            output.bytes_at(row).as_slice(),
            values[3 + original].as_bytes()
        );
    }
    drop(output);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

#[test]
fn native_zstd_sparse_text_filter_preserves_validated_frame_parts() {
    let (encoded, _) = text_fixture();
    let source = encoded.as_::<Zstd>();
    let source_parts = source.data().clone().into_parts(Validity::NonNullable);
    for range in [0..128, 3..125] {
        let len = range.len();
        let filtered = encoded
            .slice(range)
            .unwrap()
            .filter(Mask::from_indices(len, [0, len - 1]))
            .unwrap();
        let filter = filtered.as_::<Filter>();
        let child = filter.child();
        let retained = child.as_::<Zstd>();
        let parts = retained.data().clone().into_parts(Validity::NonNullable);
        assert_eq!(retained.len(), 16);
        assert_eq!(retained.dtype(), source.dtype());
        assert_eq!(parts.frames.len(), 2);
        assert_eq!(parts.metadata.frames.len(), 2);
        for (index, original) in [0, 15].into_iter().enumerate() {
            assert_eq!(
                parts.frames[index].as_ptr(),
                source_parts.frames[original].as_ptr()
            );
            assert_eq!(
                (
                    parts.metadata.frames[index].n_values,
                    parts.metadata.frames[index].uncompressed_size
                ),
                (
                    source_parts.metadata.frames[original].n_values,
                    source_parts.metadata.frames[original].uncompressed_size
                )
            );
        }
        // Validation is explicit here, outside metadata-only reduction. The
        // retained child must still satisfy every native constructor invariant.
        retained
            .data()
            .validate(retained.dtype(), retained.len(), &Validity::NonNullable)
            .unwrap();
    }
}

#[test]
fn native_zstd_sparse_text_filter_retains_source_metadata_owner() {
    let (encoded, _) = text_fixture();
    let owner = Arc::new(());
    let weak = Arc::downgrade(&owner);
    let input = encoded.with_retained_owner(owner).unwrap();
    let filtered = input.filter(Mask::from_indices(128, [1, 126])).unwrap();
    drop(input);
    assert!(weak.upgrade().is_some());
    let filter = filtered.as_::<Filter>();
    let child = filter.child().clone();
    drop(filtered);
    assert!(weak.upgrade().is_some());
    drop(child);
    assert!(weak.upgrade().is_none());
}

#[test]
fn native_zstd_owned_filters_execute_dense_and_empty_masks_and_release_owner() {
    for selected in [Vec::new(), (0..128).collect(), vec![1, 126]] {
        let (encoded, values) = text_fixture();
        let owner = Arc::new(());
        let weak = Arc::downgrade(&owner);
        let input = encoded.with_retained_owner(owner).unwrap();
        let filtered = input
            .filter(Mask::from_indices(128, selected.iter().copied()))
            .unwrap();
        drop(input);
        assert!(weak.upgrade().is_some());
        let memory = LiveMemoryPool::new(16 << 20).unwrap();
        let output = filtered
            .execute::<VarBinViewArray>(&mut session(&memory).create_execution_ctx())
            .unwrap();
        assert_eq!(output.len(), selected.len());
        for (row, original) in selected.into_iter().enumerate() {
            assert_eq!(output.bytes_at(row).as_slice(), values[original].as_bytes());
        }
        assert!(weak.upgrade().is_none());
        drop(output);
        assert_eq!(memory.snapshot().reserved_bytes, 0);
    }
}

#[test]
fn native_zstd_sparse_text_filter_rejects_inconsistent_frame_value_counts() {
    let (encoded, _) = text_fixture();
    for count in [0, 9, u64::MAX] {
        let mut parts = encoded
            .as_::<Zstd>()
            .data()
            .clone()
            .into_parts(Validity::NonNullable);
        parts.metadata.frames[0].n_values = count;
        let malformed = Zstd::try_new(
            encoded.dtype().clone(),
            ZstdData::new(parts.dictionary, parts.frames, parts.metadata, parts.n_rows),
            parts.validity,
        )
        .unwrap()
        .into_array();
        let error = malformed
            .filter(Mask::from_indices(128, [1, 126]))
            .unwrap_err();
        assert!(error.to_string().contains("Zstd frame"));
    }
}
