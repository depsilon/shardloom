use super::*;
use vortex::array::memory::{HostAllocator, HostMemoryReservation, WritableHostBuffer};
use vortex::mask::Mask;

#[derive(Debug)]
struct OwnerCheckingAllocator {
    inner: crate::owned_buffers::ReservedHostAllocator,
    owner: std::sync::Weak<()>,
}

impl HostAllocator for OwnerCheckingAllocator {
    fn allocate(&self, len: usize, alignment: Alignment) -> VortexResult<WritableHostBuffer> {
        assert!(
            self.owner.upgrade().is_some(),
            "source owner dropped during execution"
        );
        self.inner.allocate(len, alignment)
    }

    fn reserve_external(&self, bytes: usize) -> VortexResult<HostMemoryReservation> {
        assert!(
            self.owner.upgrade().is_some(),
            "source owner dropped during execution"
        );
        self.inner.reserve_external(bytes)
    }
}

fn owner_checking_session(memory: &LiveMemoryPool, owner: std::sync::Weak<()>) -> VortexSession {
    let session = VortexSession::default().with_host_allocator(Arc::new(OwnerCheckingAllocator {
        inner: crate::owned_buffers::ReservedHostAllocator::new(memory.clone()),
        owner,
    }));
    install(&session, memory.clone());
    session
}

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

#[derive(Debug)]
struct ViewAllocationRecorder {
    inner: crate::owned_buffers::ReservedHostAllocator,
    views: std::sync::Mutex<Vec<usize>>,
}

impl HostAllocator for ViewAllocationRecorder {
    fn allocate(&self, len: usize, alignment: Alignment) -> VortexResult<WritableHostBuffer> {
        if alignment == Alignment::of::<vortex::array::arrays::varbinview::BinaryView>() {
            self.views.lock().unwrap().push(len);
        }
        self.inner.allocate(len, alignment)
    }

    fn reserve_external(&self, bytes: usize) -> VortexResult<HostMemoryReservation> {
        self.inner.reserve_external(bytes)
    }
}

#[test]
fn native_zstd_sparse_text_filter_constructs_only_selected_views() {
    let (encoded, values) = text_fixture();
    let memory = LiveMemoryPool::new(16 << 20).unwrap();
    let recorder = Arc::new(ViewAllocationRecorder {
        inner: crate::owned_buffers::ReservedHostAllocator::new(memory.clone()),
        views: std::sync::Mutex::new(Vec::new()),
    });
    let session = VortexSession::default().with_host_allocator(recorder.clone());
    install(&session, memory.clone());
    let selected = [1, 3, 120, 124];
    let output = encoded
        .filter(Mask::from_indices(128, selected))
        .unwrap()
        .execute::<VarBinViewArray>(&mut session.create_execution_ctx())
        .unwrap();
    for (row, original) in selected.into_iter().enumerate() {
        assert_eq!(output.bytes_at(row).as_slice(), values[original].as_bytes());
    }
    // The retained frames contain 16 rows, but only these four rows need views.
    // This catches both constructing the other 12 views and a second gather copy.
    assert_eq!(
        *recorder.views.lock().unwrap(),
        vec![selected.len() * size_of::<vortex::array::arrays::varbinview::BinaryView>()]
    );
    drop(output);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
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
fn native_zstd_owned_sparse_filter_keeps_frame_savings_and_owner_through_retry() {
    for (range, retain_before_slice) in [(0..128, false), (3..125, false), (3..125, true)] {
        let start = range.start;
        let len = range.len();
        let selected = [0, len - 1];
        let (encoded, values) = text_fixture();
        let input = encoded.slice(range.clone()).unwrap();
        let memory = LiveMemoryPool::new(16 << 20).unwrap();
        let output = input
            .filter(Mask::from_indices(len, selected))
            .unwrap()
            .execute::<VarBinViewArray>(&mut session(&memory).create_execution_ctx())
            .unwrap();
        let sparse_peak = memory.snapshot().peak_reserved_bytes;
        assert!(sparse_peak > 0);
        drop(output);
        assert_eq!(memory.snapshot().reserved_bytes, 0);

        let owner = Arc::new(());
        let weak = Arc::downgrade(&owner);
        let input = if retain_before_slice {
            drop(input);
            encoded
                .with_retained_owner(owner)
                .unwrap()
                .slice(range)
                .unwrap()
        } else {
            drop(encoded);
            input.with_retained_owner(owner).unwrap()
        };
        let filtered = input.filter(Mask::from_indices(len, selected)).unwrap();
        drop(input);
        assert!(weak.upgrade().is_some());

        let denied = LiveMemoryPool::new(1).unwrap();
        let error = filtered
            .clone()
            .execute::<VarBinViewArray>(
                &mut owner_checking_session(&denied, weak.clone()).create_execution_ctx(),
            )
            .unwrap_err();
        assert!(crate::owned_buffers::is_owned_reservation_denial(&error));
        assert_eq!(denied.snapshot().reserved_bytes, 0);
        assert!(weak.upgrade().is_some());

        // The owned path must fit the same measured allocation envelope as the
        // ownerless path; decoding the omitted frames exceeds this budget.
        let memory = LiveMemoryPool::new(sparse_peak).unwrap();
        let output = filtered
            .execute::<VarBinViewArray>(
                &mut owner_checking_session(&memory, weak.clone()).create_execution_ctx(),
            )
            .unwrap();
        assert_eq!(output.len(), selected.len());
        for (row, original) in selected.into_iter().enumerate() {
            assert_eq!(
                output.bytes_at(row).as_slice(),
                values[start + original].as_bytes()
            );
        }
        assert_eq!(memory.snapshot().peak_reserved_bytes, sparse_peak);
        assert!(weak.upgrade().is_none());
        drop(output);
        assert_eq!(memory.snapshot().reserved_bytes, 0);
    }
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
    let memory = LiveMemoryPool::new(16 << 20).unwrap();
    let selection = zstd_filter::prepare(
        filter.child().as_::<Zstd>(),
        filter.data().filter_mask(),
        &mut session(&memory).create_execution_ctx(),
    )
    .unwrap()
    .unwrap();
    let child = &selection.array;
    let mask = Mask::from_indices(child.len(), selection.indices.iter().copied());
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
    drop(selection);
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
        let memory = LiveMemoryPool::new(16 << 20).unwrap();
        let selection = zstd_filter::prepare(
            filter.child().as_::<Zstd>(),
            filter.data().filter_mask(),
            &mut session(&memory).create_execution_ctx(),
        )
        .unwrap()
        .unwrap();
        let retained = selection.array.as_::<Zstd>();
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
        // This temporary child exists only inside admitted execution, and still
        // satisfies the independently serializable native constructor contract.
        retained
            .data()
            .validate(retained.dtype(), retained.len(), &Validity::NonNullable)
            .unwrap();
        assert!(memory.snapshot().reserved_bytes > 0);
        drop(selection);
        assert_eq!(memory.snapshot().reserved_bytes, 0);
    }
}

#[test]
fn native_zstd_sparse_filter_normalizes_explicit_all_valid_child() {
    let (encoded, values) = text_fixture();
    for dtype in [
        DType::Utf8(Nullability::Nullable),
        DType::Binary(Nullability::Nullable),
    ] {
        let validity = Validity::Array(
            vortex::array::arrays::ConstantArray::new(true, encoded.len()).into_array(),
        );
        let input = Zstd::try_new(
            dtype.clone(),
            encoded.as_::<Zstd>().data().clone(),
            validity,
        )
        .unwrap()
        .into_array();
        assert_eq!(input.slots().iter().flatten().next().unwrap().len(), 128);
        for range in [0..128, 3..125] {
            let start = range.start;
            let len = range.len();
            let filtered = input
                .slice(range)
                .unwrap()
                .filter(Mask::from_indices(len, [0, len - 1]))
                .unwrap();
            let filter = filtered.as_::<Filter>();
            let memory = LiveMemoryPool::new(16 << 20).unwrap();
            let selection = zstd_filter::prepare(
                filter.child().as_::<Zstd>(),
                filter.data().filter_mask(),
                &mut session(&memory).create_execution_ctx(),
            )
            .unwrap()
            .unwrap();
            let retained = &selection.array;
            assert_eq!(retained.len(), 16);
            assert!(retained.slots().iter().all(Option::is_none));
            retained
                .as_::<Zstd>()
                .data()
                .validate(&dtype, retained.len(), &Validity::AllValid)
                .unwrap();

            let context = ArrayContext::empty();
            let control = VortexSession::default();
            let mut bytes = ByteBufferMut::empty();
            for buffer in retained
                .serialize(&context, &control, &SerializeOptions::default())
                .unwrap()
            {
                bytes.extend_from_slice(buffer.as_ref());
            }
            let restored = SerializedArray::try_from(bytes.freeze())
                .unwrap()
                .decode(
                    &dtype,
                    retained.len(),
                    &ReadContext::new(context.to_ids()),
                    &control,
                )
                .unwrap();
            assert!(restored.slots().iter().all(Option::is_none));
            let output = restored
                .filter(Mask::from_indices(
                    retained.len(),
                    selection.indices.iter().copied(),
                ))
                .unwrap()
                .execute::<VarBinViewArray>(&mut session(&memory).create_execution_ctx())
                .unwrap();
            assert_eq!(output.dtype(), &dtype);
            assert_eq!(output.len(), 2);
            for (row, original) in [start, start + len - 1].into_iter().enumerate() {
                assert_eq!(output.bytes_at(row).as_slice(), values[original].as_bytes());
            }
            drop(output);
            drop(selection);
            assert_eq!(memory.snapshot().reserved_bytes, 0);
        }
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
    for selected in [
        Vec::new(),
        (0..128).collect(),
        (0..64).collect(),
        vec![1, 126],
    ] {
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
        let memory = LiveMemoryPool::new(16 << 20).unwrap();
        let error = malformed
            .filter(Mask::from_indices(128, [1, 126]))
            .unwrap()
            .execute::<VarBinViewArray>(&mut session(&memory).create_execution_ctx())
            .unwrap_err();
        assert!(error.to_string().contains("Zstd frame"));
        assert_eq!(memory.snapshot().reserved_bytes, 0);
    }
}

#[test]
fn native_zstd_owned_filter_selection_error_releases_owner() {
    let (encoded, _) = text_fixture();
    for count in [0, 9, u64::MAX] {
        let mut parts = encoded
            .as_::<Zstd>()
            .data()
            .clone()
            .into_parts(Validity::NonNullable);
        parts.metadata.frames[0].n_values = count;
        let owner = Arc::new(());
        let weak = Arc::downgrade(&owner);
        let malformed = Zstd::try_new(
            encoded.dtype().clone(),
            ZstdData::new(parts.dictionary, parts.frames, parts.metadata, parts.n_rows),
            parts.validity,
        )
        .unwrap()
        .into_array()
        .with_retained_owner(owner)
        .unwrap();
        let filtered = malformed.filter(Mask::from_indices(128, [1, 126])).unwrap();
        drop(malformed);
        let memory = LiveMemoryPool::new(16 << 20).unwrap();
        let error = filtered
            .execute::<VarBinViewArray>(
                &mut owner_checking_session(&memory, weak.clone()).create_execution_ctx(),
            )
            .unwrap_err();
        assert!(error.to_string().contains("Zstd frame"));
        assert!(weak.upgrade().is_none());
        assert!(memory.snapshot().peak_reserved_bytes > 0);
        assert_eq!(memory.snapshot().reserved_bytes, 0);
    }
}

#[test]
fn native_zstd_sparse_selection_admits_metadata_before_allocation_without_mask_caches() {
    let (encoded, values) = text_fixture();
    let selected = [1, 3, 120, 124];
    let mask = Mask::from_iter((0..128).map(|row| selected.contains(&row)));
    let filtered = encoded.filter(mask.clone()).unwrap();
    let filter = filtered.as_::<Filter>();
    let input = filter.child();
    assert!(ArrayRef::ptr_eq(input, &encoded));
    assert!(mask.values().unwrap().cached_indices().is_none());
    assert!(mask.values().unwrap().cached_slices().is_none());
    let metadata = u64::try_from(
        2 * (size_of::<vortex::buffer::ByteBuffer>()
            + size_of::<vortex::encodings::zstd::ZstdFrameMetadata>()),
    )
    .unwrap();
    let index_bytes = u64::try_from(selected.len() * size_of::<usize>() + 256).unwrap();
    let denied = LiveMemoryPool::new(index_bytes + metadata - 1).unwrap();
    let error = filtered
        .clone()
        .execute::<VarBinViewArray>(&mut session(&denied).create_execution_ctx())
        .unwrap_err();
    assert!(crate::owned_buffers::is_owned_reservation_denial(&error));
    // Row indices are admitted first. The selected table is denied before its
    // vectors are constructed, and the temporary indices are then released.
    assert_eq!(denied.snapshot().peak_reserved_bytes, index_bytes);
    assert_eq!(denied.snapshot().reserved_bytes, 0);

    let ample = LiveMemoryPool::new(16 << 20).unwrap();
    let output = filtered
        .clone()
        .execute::<VarBinViewArray>(&mut session(&ample).create_execution_ctx())
        .unwrap();
    let peak = ample.snapshot().peak_reserved_bytes;
    assert!(peak > metadata);
    for (row, source) in selected.into_iter().enumerate() {
        assert_eq!(output.bytes_at(row).as_slice(), values[source].as_bytes());
    }
    drop(output);
    assert_eq!(ample.snapshot().reserved_bytes, 0);

    for budget in [peak - 1, peak] {
        let memory = LiveMemoryPool::new(budget).unwrap();
        let result = filtered
            .clone()
            .execute::<VarBinViewArray>(&mut session(&memory).create_execution_ctx());
        if budget < peak {
            assert!(crate::owned_buffers::is_owned_reservation_denial(
                &result.unwrap_err()
            ));
        } else {
            let output = result.unwrap();
            assert_eq!(output.len(), selected.len());
            assert_eq!(memory.snapshot().peak_reserved_bytes, peak);
            let retained = output.data_buffers()[0].as_host().clone();
            drop(output);
            assert!(memory.snapshot().reserved_bytes > 0);
            drop(retained);
        }
        assert_eq!(memory.snapshot().reserved_bytes, 0);
        assert!(mask.values().unwrap().cached_indices().is_none());
        assert!(mask.values().unwrap().cached_slices().is_none());
    }
}

#[derive(Debug)]
struct SelectionFailAllocator {
    inner: crate::owned_buffers::ReservedHostAllocator,
    call: std::sync::atomic::AtomicUsize,
    fail_at: usize,
}

impl HostAllocator for SelectionFailAllocator {
    fn allocate(&self, len: usize, alignment: Alignment) -> VortexResult<WritableHostBuffer> {
        let call = self.call.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        if call == self.fail_at {
            return Err(vortex_err!("injected sparse selection allocation failure"));
        }
        self.inner.allocate(len, alignment)
    }

    fn reserve_external(&self, bytes: usize) -> VortexResult<HostMemoryReservation> {
        self.inner.reserve_external(bytes)
    }
}

#[test]
fn native_zstd_sparse_selection_releases_every_failed_allocation_and_preserves_dictionary() {
    let (_, values) = text_fixture();
    let plain = VarBinViewArray::from_iter_str(values.iter().map(String::as_str));
    let source = Zstd::from_var_bin_view(
        &plain,
        1,
        8,
        &mut VortexSession::default().create_execution_ctx(),
    )
    .unwrap();
    assert!(
        source
            .data()
            .clone()
            .into_parts(Validity::NonNullable)
            .dictionary
            .is_some()
    );
    let mask = Mask::from_iter((0..128).map(|row| [0, 1, 126, 127].contains(&row)));
    let filtered = source.into_array().filter(mask.clone()).unwrap();
    let mut completed = false;
    // A successful pass establishes the exact allocation count. Every earlier
    // iteration injects an error before that allocation, including final views.
    for fail_at in 0..12 {
        let memory = LiveMemoryPool::new(16 << 20).unwrap();
        let allocator = Arc::new(SelectionFailAllocator {
            inner: crate::owned_buffers::ReservedHostAllocator::new(memory.clone()),
            call: std::sync::atomic::AtomicUsize::new(0),
            fail_at,
        });
        let session = VortexSession::default().with_host_allocator(allocator.clone());
        install(&session, memory.clone());
        match filtered
            .clone()
            .execute::<VarBinViewArray>(&mut session.create_execution_ctx())
        {
            Ok(output) => {
                assert_eq!(
                    fail_at, 5,
                    "indices, payload, decoder, dictionary, selected views"
                );
                assert_eq!(
                    allocator.call.load(std::sync::atomic::Ordering::SeqCst),
                    fail_at
                );
                for (row, source) in [0, 1, 126, 127].into_iter().enumerate() {
                    assert_eq!(output.bytes_at(row).as_slice(), values[source].as_bytes());
                }
                let views = output.views_handle().as_host().clone();
                drop(output);
                assert!(memory.snapshot().reserved_bytes > 0);
                drop(views);
                completed = true;
            }
            Err(error) => assert!(
                error
                    .to_string()
                    .contains("injected sparse selection allocation failure")
            ),
        }
        assert_eq!(memory.snapshot().reserved_bytes, 0);
        assert!(mask.values().unwrap().cached_indices().is_none());
        assert!(mask.values().unwrap().cached_slices().is_none());
        if completed {
            break;
        }
    }
    assert!(
        completed,
        "allocation failure sweep did not reach successful execution"
    );
}
