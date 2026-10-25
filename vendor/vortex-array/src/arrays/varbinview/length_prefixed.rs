// SPDX-License-Identifier: Apache-2.0
// ShardLoom local provider: checked text reconstruction in admitted storage.

use std::ops::Range;

use vortex_error::VortexResult;
use vortex_error::vortex_ensure;
use vortex_error::vortex_err;

use super::BinaryView;
use super::MAX_BUFFER_LEN;
use crate::ArrayParts;
use crate::ExecutionCtx;
use crate::arrays::VarBinView;
use crate::arrays::VarBinViewArray;
use crate::arrays::varbinview::VarBinViewData;
use crate::dtype::DType;
use crate::memory::HostAllocatorExt;
use crate::memory::WritableHostBuffer;
use crate::validity::Validity;

/// Build UTF-8 views from an exclusively owned sequence of `[u32 LE length][value]` records.
///
/// Every record's bounds and the exact `stored_count` are checked. Only `range` is materialized,
/// in order, into the valid slots of `rows`. Only those selected values require valid UTF-8.
/// Consumed length headers in the selected span become zeros before a single UTF-8 validation;
/// value bytes remain unchanged and the immutable output retains the original allocation owner.
/// Zero headers separate character sequences. The checked constructor validates the immutable
/// heap and every view after freezing, including independent inline-value checks. No lengths
/// vector or second payload allocation is used.
///
/// The input must fit in one native view buffer. Larger heaps can use the ordinary checked
/// multi-buffer reconstruction path. Output views and their buffer table use `ctx`'s allocator.
pub fn build_views_from_length_prefixed_in(
    mut bytes: WritableHostBuffer,
    stored_count: usize,
    range: Range<usize>,
    rows: usize,
    validity: Validity,
    ctx: &mut ExecutionCtx,
) -> VortexResult<VarBinViewArray> {
    vortex_ensure!(
        bytes.len() <= MAX_BUFFER_LEN,
        "Length-prefixed text exceeds one native view buffer"
    );
    vortex_ensure!(
        stored_count <= bytes.len() / size_of::<u32>(),
        "Length-prefixed text count exceeds its byte extent"
    );
    vortex_ensure!(
        range.start <= range.end && range.end <= stored_count,
        "Length-prefixed text value range is out of bounds"
    );
    vortex_ensure!(
        validity.maybe_len().is_none_or(|len| len == rows),
        "Length-prefixed text validity length differs from output rows"
    );
    let mask = validity.execute_mask(rows, ctx)?;
    vortex_ensure!(
        mask.true_count() == range.len(),
        "Length-prefixed text value count differs from output validity"
    );
    let allocator = ctx.host_allocator();
    let mut storage = allocator.allocate_typed::<BinaryView>(rows)?;
    let views = storage.as_mut_slice_typed::<BinaryView>()?;
    let all_valid = mask.all_true();
    if !all_valid {
        views.fill(BinaryView::default());
    }
    let mut valid_rows = mask
        .iter()
        .enumerate()
        .filter_map(|(row, valid)| valid.then_some(row));
    let byte_len = bytes.len();
    let buffer = bytes.as_mut_slice();
    vortex_ensure!(
        buffer.len() == byte_len && views.len() == rows,
        "Length-prefixed text allocator returned an incorrect extent"
    );
    let mut offset = 0usize;
    let mut span = 0..0;
    for record in 0..stored_count {
        let prefix = buffer
            .get(offset..)
            .and_then(|rest| rest.first_chunk::<{ size_of::<u32>() }>())
            .ok_or_else(|| vortex_err!("Length-prefixed text has a truncated length"))?;
        let len = u32::from_le_bytes(*prefix) as usize;
        // The prefix read proves this addition fits and remains within the buffer.
        let start = offset + size_of::<u32>();
        let end = start
            .checked_add(len)
            .filter(|end| *end <= buffer.len())
            .ok_or_else(|| vortex_err!("Length-prefixed text value exceeds its byte extent"))?;
        if range.contains(&record) {
            if record == range.start {
                span.start = offset;
            }
            let row = if all_valid {
                record - range.start
            } else {
                valid_rows
                    .next()
                    .ok_or_else(|| vortex_err!("Length-prefixed text exhausted its valid rows"))?
            };
            // The full heap is bounded by MAX_BUFFER_LEN, which is less than u32::MAX.
            let local_offset = u32::try_from(start - span.start)
                .map_err(|_| vortex_err!("Length-prefixed text offset exceeds u32"))?;
            views[row] = BinaryView::make_view(&buffer[start..end], 0, local_offset);
            buffer[offset..start].fill(0);
            span.end = end;
        }
        offset = end;
    }
    vortex_ensure!(
        offset == buffer.len(),
        "Length-prefixed text bytes exceed its stored value count"
    );
    let heap = bytes.freeze();
    vortex_ensure!(
        heap.len() == byte_len && span.end <= heap.len(),
        "Length-prefixed text allocation changed extent while freezing"
    );
    let heap = heap.slice(span);
    let dtype = DType::Utf8(validity.nullability());
    // Freeze is an allocator boundary: establish validity from the final immutable heap and
    // views rather than trusting the earlier mutable writes. This still avoids per-value scans
    // of outlined strings and retains all structural checks and table admission.
    let views = storage.freeze_typed::<BinaryView>()?;
    vortex_ensure!(
        views.len() == rows,
        "Length-prefixed text views changed extent while freezing"
    );
    let data = VarBinViewData::try_new_utf8_buffer_in(views, heap, validity.clone(), &allocator)?;
    let slots = VarBinViewData::make_slots(&validity, rows);
    VarBinViewArray::try_from_parts(ArrayParts::new(VarBinView, dtype, rows, data, slots))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use vortex_buffer::Alignment;
    use vortex_buffer::Buffer;
    use vortex_buffer::ByteBuffer;

    use super::*;
    use crate::VortexSessionExecute;
    use crate::array_session;
    use crate::arrays::varbinview::VarBinViewArrayExt;
    use crate::memory::DefaultHostAllocator;
    use crate::memory::HostAllocator;
    use crate::memory::HostAllocatorRef;

    fn writable(raw: &[u8]) -> VortexResult<WritableHostBuffer> {
        let mut bytes = DefaultHostAllocator.allocate(raw.len(), Alignment::none())?;
        bytes.as_mut_slice().copy_from_slice(raw);
        Ok(bytes)
    }

    fn records(values: &[&[u8]]) -> Vec<u8> {
        let mut bytes = Vec::new();
        for value in values {
            bytes.extend_from_slice(&u32::try_from(value.len()).unwrap().to_le_bytes());
            bytes.extend_from_slice(value);
        }
        bytes
    }

    #[test]
    fn length_prefixed_text_preserves_unicode_ranges_nulls_and_payload_identity() -> VortexResult<()>
    {
        let long = "λ".repeat(64); // Its 128-byte length header is not itself UTF-8.
        let values = [
            "",
            "a",
            "λ猫",
            "twelve-bytes!",
            "thirteen-byte",
            "\0",
            &long,
            "🦀",
        ];
        let bytes: Vec<_> = values.iter().map(|value| value.as_bytes()).collect();
        let raw = records(&bytes);
        let mut ctx = array_session().create_execution_ctx();
        for start in 0..=values.len() {
            for end in start..=values.len() {
                for nullable in [false, true] {
                    let range = start..end;
                    let rows = if nullable {
                        range.len() * 2 + 1
                    } else {
                        range.len()
                    };
                    let validity = if nullable {
                        (0..rows).map(|row| row % 2 == 1).collect()
                    } else {
                        Validity::NonNullable
                    };
                    let mut storage = writable(&raw)?;
                    let pointer = storage.as_mut_slice().as_ptr() as usize;
                    let array = build_views_from_length_prefixed_in(
                        storage,
                        values.len(),
                        range.clone(),
                        rows,
                        validity,
                        &mut ctx,
                    )?;
                    assert!(array.utf8_validated);
                    assert_eq!(array.len(), rows);
                    let mask = array.validity()?.execute_mask(rows, &mut ctx)?;
                    let mut value = start;
                    for row in 0..rows {
                        if mask.value(row) {
                            assert_eq!(array.utf8_slice_at(row)?, values[value]);
                            if values[value].len() > 12 {
                                let offset =
                                    bytes[..value].iter().map(|v| v.len() + 4).sum::<usize>() + 4;
                                assert_eq!(array.bytes_at(row).as_ptr() as usize, pointer + offset);
                            }
                            value += 1;
                        } else {
                            assert_eq!(array.utf8_slice_at(row)?, "");
                        }
                    }
                    assert_eq!(value, end);
                }
            }
        }
        Ok(())
    }

    #[test]
    fn length_prefixed_text_rejects_invalid_selected_values_and_cross_record_sequences()
    -> VortexResult<()> {
        let mut ctx = array_session().create_execution_ctx();
        for values in [
            vec![&b"\xc3"[..], &b"\xa9"[..]], // Together valid; separately invalid.
            vec![&b"\xf0\x9f"[..], &b"\xa6\x80"[..]],
            vec![&b"\xff"[..], &b""[..]],
            vec![&b"long invalid value \xff"[..]],
        ] {
            let raw = records(&values);
            assert!(
                build_views_from_length_prefixed_in(
                    writable(&raw)?,
                    values.len(),
                    0..values.len(),
                    values.len(),
                    Validity::NonNullable,
                    &mut ctx,
                )
                .is_err()
            );
        }
        let raw = records(&[b"\xff", b"selected valid Unicode \xce\xbb", b"\x80"]);
        let array = build_views_from_length_prefixed_in(
            writable(&raw)?,
            3,
            1..2,
            1,
            Validity::NonNullable,
            &mut ctx,
        )?;
        assert_eq!(array.utf8_slice_at(0)?, "selected valid Unicode λ");
        Ok(())
    }

    #[test]
    fn length_prefixed_text_rejects_bad_framing_counts_ranges_and_validity() -> VortexResult<()> {
        let mut ctx = array_session().create_execution_ctx();
        for raw in [
            vec![1, 0, 0],
            vec![5, 0, 0, 0, b'a'],
            vec![255; 4],
            vec![0, 0, 0, 0, 1],
        ] {
            assert!(
                build_views_from_length_prefixed_in(
                    writable(&raw)?,
                    1,
                    0..1,
                    1,
                    Validity::NonNullable,
                    &mut ctx,
                )
                .is_err()
            );
        }
        let raw = records(&[b"first", b"second"]);
        for count in [0, 1, 3, usize::MAX] {
            assert!(
                build_views_from_length_prefixed_in(
                    writable(&raw)?,
                    count,
                    0..1,
                    1,
                    Validity::NonNullable,
                    &mut ctx,
                )
                .is_err()
            );
        }
        for range in [Range { start: 2, end: 1 }, 0..3, 2..usize::MAX] {
            assert!(
                build_views_from_length_prefixed_in(
                    writable(&raw)?,
                    2,
                    range,
                    1,
                    Validity::NonNullable,
                    &mut ctx,
                )
                .is_err()
            );
        }
        for validity in [Validity::NonNullable, [true, false].into_iter().collect()] {
            assert!(
                build_views_from_length_prefixed_in(
                    writable(&raw)?,
                    2,
                    0..2,
                    1,
                    validity,
                    &mut ctx,
                )
                .is_err()
            );
        }
        // An unselected corrupt tail must not be hidden by a valid selected prefix.
        let mut raw = records(&[b"first"]);
        raw.extend_from_slice(&[9, 0]);
        assert!(
            build_views_from_length_prefixed_in(
                writable(&raw)?,
                2,
                0..1,
                1,
                Validity::NonNullable,
                &mut ctx,
            )
            .is_err()
        );
        Ok(())
    }

    #[test]
    fn length_prefixed_text_empty_and_all_null_have_valid_empty_views() -> VortexResult<()> {
        let mut ctx = array_session().create_execution_ctx();
        for (rows, validity) in [(0, Validity::NonNullable), (7, Validity::AllInvalid)] {
            let array = build_views_from_length_prefixed_in(
                writable(&[])?,
                0,
                0..0,
                rows,
                validity,
                &mut ctx,
            )?;
            assert_eq!(array.len(), rows);
            assert!(array.data_buffers().is_empty());
            assert!(array.utf8_validated);
            assert!(
                array
                    .views()
                    .iter()
                    .all(|view| *view == BinaryView::default())
            );
        }
        Ok(())
    }

    #[test]
    fn checked_utf8_heap_constructor_rejects_invalid_frozen_views() {
        let allocator: HostAllocatorRef = Arc::new(DefaultHostAllocator);
        let heap = ByteBuffer::copy_from("λabcdefghijklmnopqrstuv");
        let cases = [
            BinaryView::new_inlined(b"\xff"),
            BinaryView::new_ref(13, *b"xxxx", 0, 2),
            BinaryView::new_ref(13, *b"abcd", 1, 2),
            BinaryView::new_ref(13, *b"abcd", 0, u32::MAX),
            BinaryView::make_view(&heap[1..], 0, 1),
        ];
        for view in cases {
            for validity in [Validity::NonNullable, Validity::AllInvalid] {
                assert!(
                    VarBinViewData::try_new_utf8_buffer_in(
                        Buffer::copy_from([view]),
                        heap.clone(),
                        validity,
                        &allocator,
                    )
                    .is_err()
                );
            }
        }
    }
}
