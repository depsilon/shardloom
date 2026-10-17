// SPDX-License-Identifier: Apache-2.0
// ShardLoom local provider: selected views over the existing native decoder.

use super::*;

pub(super) fn decompress(
    array: ArrayView<'_, Zstd>,
    indices: &[usize],
    ctx: &mut ExecutionCtx,
) -> VortexResult<VarBinViewArray> {
    vortex_ensure!(
        matches!(array.dtype(), DType::Utf8(_) | DType::Binary(_)),
        "Selected Zstd views require UTF-8 or binary data"
    );
    let validity = unsliced_validity(array);
    vortex_ensure!(
        matches!(validity, Validity::NonNullable | Validity::AllValid),
        "Selected Zstd views require all-valid data"
    );
    vortex_ensure!(
        indices.windows(2).all(|pair| pair[0] < pair[1])
            && indices.last().is_none_or(|last| *last < array.len()),
        "Selected Zstd view indices must be strictly increasing and in bounds"
    );
    let allocator = ctx.allocator();
    let (buffers, views) = if indices.is_empty() {
        (
            VarBinViewBufferBuilder::try_with_capacity(0, &allocator)?,
            allocator.allocate_typed::<BinaryView>(0)?.freeze_typed()?,
        )
    } else {
        let slice = array
            .data()
            .decompress_slice(array.dtype(), &validity, ctx)?
            .freeze();
        let range = slice.value_range()?;
        vortex_ensure!(
            range.len() == array.len() && range.end <= slice.n_buffered_values,
            "Selected Zstd slice value count does not match its logical rows"
        );
        reconstruct(
            &slice.bytes,
            slice.n_buffered_values,
            range.start,
            indices,
            MAX_BUFFER_LEN,
            &allocator,
        )?
    };
    // Only selected values are validated. The checked prefix walk below proves
    // every frame record's extent and count without constructing unused views.
    VarBinViewArray::try_new_from_buffers(
        views,
        &buffers,
        array.dtype().clone(),
        Validity::from(array.dtype().nullability()),
        ctx,
    )
}

fn reconstruct(
    buffer: &ByteBuffer,
    expected_values: usize,
    first_value: usize,
    indices: &[usize],
    max_buffer_len: usize,
    allocator: &HostAllocatorRef,
) -> VortexResult<(VarBinViewBufferBuilder, Buffer<BinaryView>)> {
    vortex_ensure!(
        max_buffer_len > 0,
        "Zstd view buffer extent must be positive"
    );
    // Adjacent complete-value segments exceed this extent. The same conservative
    // bound is admitted here before allocating the collection.
    let capacity = (buffer.len() / max_buffer_len)
        .checked_mul(2)
        .and_then(|count| count.checked_add(1))
        .ok_or_else(|| vortex_err!("Selected Zstd buffer count overflow"))?;
    let mut buffers = VarBinViewBufferBuilder::try_with_capacity(
        if buffer.is_empty() { 0 } else { capacity },
        allocator,
    )?;
    let mut storage = allocator.allocate_typed::<BinaryView>(indices.len())?;
    let views = storage.as_mut_slice_typed::<BinaryView>()?;
    let (mut offset, mut segment_start, mut row, mut selected) = (0usize, 0usize, 0usize, 0usize);
    let mut buffer_index = 0u32;
    let target = |index: usize| {
        indices
            .get(index)
            .map(|value| {
                first_value
                    .checked_add(*value)
                    .ok_or_else(|| vortex_err!("Selected Zstd row index overflow"))
            })
            .transpose()
    };
    let mut next = target(0)?;
    while offset < buffer.len() {
        vortex_ensure!(
            row < expected_values,
            "Zstd frame contains more values than its metadata"
        );
        let len = zstd_value_len(buffer, offset)?;
        let start = offset
            .checked_add(size_of::<ViewLen>())
            .ok_or_else(|| vortex_err!("Selected Zstd value offset overflow"))?;
        let end = start
            .checked_add(len)
            .filter(|end| *end <= buffer.len())
            .ok_or_else(|| vortex_err!("Corrupt selected Zstd value exceeds its frame buffer"))?;
        if end - segment_start > max_buffer_len && offset > segment_start {
            vortex_ensure!(
                buffers.len() < capacity,
                "Selected Zstd buffer bound exceeded"
            );
            buffers.try_push(buffer.slice(segment_start..offset))?;
            segment_start = offset;
            buffer_index = buffer_index
                .checked_add(1)
                .ok_or_else(|| vortex_err!("Selected Zstd buffer index overflow"))?;
        }
        if next == Some(row) {
            let local_offset = u32::try_from(start - segment_start)
                .map_err(|_| vortex_err!("Selected Zstd value offset does not fit in u32"))?;
            views[selected] =
                BinaryView::make_view(&buffer[start..end], buffer_index, local_offset);
            selected += 1;
            next = target(selected)?;
        }
        offset = end;
        row += 1;
    }
    vortex_ensure!(
        row == expected_values && selected == indices.len(),
        "Zstd frame value count does not cover its metadata or selected rows"
    );
    if segment_start < buffer.len() {
        vortex_ensure!(
            buffers.len() < capacity,
            "Selected Zstd buffer bound exceeded"
        );
        buffers.try_push(buffer.slice(segment_start..buffer.len()))?;
    }
    Ok((buffers, storage.freeze_typed::<BinaryView>()?))
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use vortex_array::VortexSessionExecute;
    use vortex_array::array_session;
    use vortex_array::dtype::Nullability;
    use vortex_array::memory::{
        DefaultHostAllocator, HostAllocator, MemorySessionExt, WritableHostBuffer,
    };

    use super::*;

    fn interleaved(values: &[&[u8]]) -> VortexResult<ByteBuffer> {
        let mut bytes = Vec::new();
        for value in values {
            bytes.extend_from_slice(&u32::try_from(value.len())?.to_le_bytes());
            bytes.extend_from_slice(value);
        }
        Ok(ByteBuffer::copy_from(&bytes))
    }

    #[test]
    fn selected_views_match_bytes_across_slices_and_segment_boundaries() -> VortexResult<()> {
        let cases: &[&[&[u8]]] = &[
            &[],
            &[b""],
            &[b"", b"short", b"twelve-bytes!", b"thirteen-byte", b""],
            &[
                b"a value larger than a small segment",
                b"",
                b"last long value",
            ],
            &[b"\xff\x00\x80", b"binary bytes longer than twelve", b"\x00"],
        ];
        let allocator: HostAllocatorRef = Arc::new(DefaultHostAllocator);
        let mut ctx = array_session().create_execution_ctx();
        for values in cases {
            let bytes = interleaved(values)?;
            for first in 0..=values.len() {
                let remaining = values.len() - first;
                for mask in 0usize..(1 << remaining) {
                    let selected: Vec<_> = (0..remaining)
                        .filter(|index| mask & (1 << index) != 0)
                        .collect();
                    for limit in [1, 4, 12, 16, 17, 20, 34, 1024] {
                        let (buffers, views) =
                            reconstruct(&bytes, values.len(), first, &selected, limit, &allocator)?;
                        assert_eq!(views.len(), selected.len());
                        assert!(buffers.len() <= 2 * (bytes.len() / limit) + 1);
                        let array = VarBinViewArray::try_new_from_buffers(
                            views,
                            &buffers,
                            DType::Binary(Nullability::NonNullable),
                            Validity::NonNullable,
                            &mut ctx,
                        )?;
                        for (row, index) in selected.iter().enumerate() {
                            assert_eq!(array.bytes_at(row).as_slice(), values[first + index]);
                        }
                    }
                }
            }
        }
        Ok(())
    }

    #[test]
    fn selected_views_reject_malformed_lengths_counts_and_extents() -> VortexResult<()> {
        let allocator: HostAllocatorRef = Arc::new(DefaultHostAllocator);
        for bytes in [
            ByteBuffer::copy_from(&[1, 0, 0]),
            ByteBuffer::copy_from(&[5, 0, 0, 0, b'a']),
            ByteBuffer::copy_from(&[0xff, 0xff, 0xff, 0xff]),
            ByteBuffer::copy_from(&[0, 0, 0, 0, 1]),
        ] {
            assert!(reconstruct(&bytes, 1, 0, &[0], 20, &allocator).is_err());
        }
        let bytes = interleaved(&[b"first", b"second"])?;
        for count in [0, 1, 3, usize::MAX] {
            assert!(reconstruct(&bytes, count, 0, &[0], 20, &allocator).is_err());
        }
        assert!(reconstruct(&bytes, 2, 2, &[0], 20, &allocator).is_err());
        assert!(reconstruct(&bytes, 2, usize::MAX, &[1], 20, &allocator).is_err());
        assert!(reconstruct(&bytes, 2, 0, &[0], 0, &allocator).is_err());
        // A valid selected prefix must not conceal a corrupt unselected tail.
        let mut trailing = bytes.as_slice().to_vec();
        trailing.extend_from_slice(&[9, 0]);
        assert!(
            reconstruct(
                &ByteBuffer::copy_from(&trailing),
                3,
                0,
                &[0],
                20,
                &allocator
            )
            .is_err()
        );
        Ok(())
    }

    #[derive(Debug, Default)]
    struct AllocationCounter(AtomicUsize);

    impl HostAllocator for AllocationCounter {
        fn allocate(&self, len: usize, alignment: Alignment) -> VortexResult<WritableHostBuffer> {
            if len > 0 {
                self.0.fetch_add(1, Ordering::Relaxed);
            }
            DefaultHostAllocator.allocate(len, alignment)
        }

        fn reserve_external(
            &self,
            len: usize,
        ) -> VortexResult<vortex_array::memory::HostMemoryReservation> {
            DefaultHostAllocator.reserve_external(len)
        }
    }

    #[test]
    fn selected_decoder_validates_indices_and_shape_before_allocating() -> VortexResult<()> {
        let mut ctx = array_session().create_execution_ctx();
        let plain = VarBinViewArray::from_iter_str(["first value", "second value"]);
        let encoded = Zstd::from_var_bin_view_without_dict(&plain, 0, 1, &mut ctx)?.into_array();
        let counter = Arc::new(AllocationCounter::default());
        let session = array_session().with_allocator(Arc::<AllocationCounter>::clone(&counter));
        let mut ctx = session.create_execution_ctx();
        for indices in [&[2][..], &[1, 0], &[0, 0], &[0, usize::MAX]] {
            assert!(decompress(encoded.as_::<Zstd>(), indices, &mut ctx).is_err());
            assert_eq!(counter.0.load(Ordering::Relaxed), 0);
        }
        let empty = decompress(encoded.as_::<Zstd>(), &[], &mut ctx)?;
        assert!(empty.is_empty());
        assert!(empty.data_buffers().is_empty());
        assert_eq!(counter.0.load(Ordering::Relaxed), 0);

        for values in [[Some("valid"), None], [None, None]] {
            let nulls = VarBinViewArray::from_iter(values, DType::Utf8(Nullability::Nullable));
            let nulls = Zstd::from_var_bin_view_without_dict(
                &nulls,
                0,
                1,
                &mut array_session().create_execution_ctx(),
            )?
            .into_array();
            assert!(decompress(nulls.as_::<Zstd>(), &[0], &mut ctx).is_err());
        }
        let primitive = Zstd::from_primitive(
            &PrimitiveArray::from_iter([1i64, 2]),
            0,
            0,
            &mut array_session().create_execution_ctx(),
        )?
        .into_array();
        assert!(decompress(primitive.as_::<Zstd>(), &[0], &mut ctx).is_err());
        assert_eq!(counter.0.load(Ordering::Relaxed), 0);
        Ok(())
    }

    #[test]
    fn selected_decoder_preserves_dtype_values_and_nested_slice_offsets() -> VortexResult<()> {
        for nullable in [Nullability::NonNullable, Nullability::Nullable] {
            for binary in [false, true] {
                let dtype = if binary {
                    DType::Binary(nullable)
                } else {
                    DType::Utf8(nullable)
                };
                let values: Vec<Vec<u8>> = (0..96)
                    .map(|row| {
                        let mut value =
                            format!("row {row}: λ {}", "abcdefgh".repeat(row % 9)).into_bytes();
                        if binary {
                            value.push(0xff);
                        }
                        value
                    })
                    .collect();
                let plain = VarBinViewArray::from_iter(
                    values.iter().map(|value| Some(value.as_slice())),
                    dtype.clone(),
                );
                for dictionary in [false, true] {
                    let mut ctx = array_session().create_execution_ctx();
                    let encoded = if dictionary {
                        Zstd::from_var_bin_view(&plain, 0, 8, &mut ctx)?
                    } else {
                        Zstd::from_var_bin_view_without_dict(&plain, 0, 8, &mut ctx)?
                    }
                    .into_array();
                    let sliced = encoded.slice(3..93)?.slice(5..85)?;
                    let selected = [0, 1, 17, 79];
                    let output = decompress(sliced.as_::<Zstd>(), &selected, &mut ctx)?;
                    assert_eq!(output.dtype(), &dtype);
                    assert_eq!(output.len(), selected.len());
                    for (row, index) in selected.into_iter().enumerate() {
                        assert_eq!(output.bytes_at(row).as_slice(), values[8 + index]);
                    }
                }
            }
        }
        Ok(())
    }

    #[test]
    fn selected_decoder_rejects_incorrect_encoded_value_count() -> VortexResult<()> {
        let mut ctx = array_session().create_execution_ctx();
        let plain = VarBinViewArray::from_iter_str(["first", "second"]);
        let encoded = Zstd::from_var_bin_view_without_dict(&plain, 0, 0, &mut ctx)?.into_array();
        for rows in [1usize, 3] {
            let mut parts = encoded
                .as_::<Zstd>()
                .data()
                .clone()
                .into_parts(Validity::NonNullable);
            parts.metadata.frames[0].n_values = u64::try_from(rows)?;
            let malformed = Zstd::try_new(
                plain.dtype().clone(),
                ZstdData::new(parts.dictionary, parts.frames, parts.metadata, rows),
                Validity::NonNullable,
            )?
            .into_array();
            assert!(decompress(malformed.as_::<Zstd>(), &[0], &mut ctx).is_err());
        }
        Ok(())
    }

    #[test]
    fn selected_decoder_validates_utf8_in_selected_output() -> VortexResult<()> {
        let mut ctx = array_session().create_execution_ctx();
        let plain = VarBinViewArray::from_iter(
            [Some(&b"\xff"[..]), Some(&b"valid"[..])],
            DType::Binary(Nullability::NonNullable),
        );
        let encoded = Zstd::from_var_bin_view_without_dict(&plain, 0, 0, &mut ctx)?.into_array();
        let parts = encoded
            .as_::<Zstd>()
            .data()
            .clone()
            .into_parts(Validity::NonNullable);
        let malformed = Zstd::try_new(
            DType::Utf8(Nullability::NonNullable),
            ZstdData::new(parts.dictionary, parts.frames, parts.metadata, parts.n_rows),
            Validity::NonNullable,
        )?
        .into_array();
        assert!(decompress(malformed.as_::<Zstd>(), &[0], &mut ctx).is_err());
        Ok(())
    }
}
