// SPDX-License-Identifier: Apache-2.0
// Original ShardLoom implementation, 2026-10-13.

//! Retain compressed text frames containing selected rows, then use the native filter.
//! No payload is decoded or copied by this metadata reduction. Null-bearing arrays
//! keep their existing path because frame value offsets exclude null rows.

use vortex_array::arrays::filter::FilterReduce;
use vortex_array::dtype::DType;
use vortex_array::validity::Validity;
use vortex_array::vtable::{child_to_validity, validity_to_child};
use vortex_array::{Array, ArrayParts, ArrayRef, ArrayView, IntoArray};
use vortex_error::{VortexResult, vortex_ensure, vortex_err};
use vortex_mask::{AllOr, Mask};

use crate::{Zstd, ZstdData, ZstdMetadata, ZstdSlots};

impl FilterReduce for Zstd {
    fn filter(array: ArrayView<'_, Self>, mask: &Mask) -> VortexResult<Option<ArrayRef>> {
        let data = array.data();
        // Bound temporary structural metadata and keep dense filters on their
        // existing path. These are shape policies, independent of query names.
        if !matches!(array.dtype(), DType::Utf8(_) | DType::Binary(_))
            || !(2..=64).contains(&data.frames.len())
            || mask.true_count() > 4096
            || mask.true_count() > array.len() / 8
        {
            return Ok(None);
        }
        let validity = child_to_validity(
            array.slots()[ZstdSlots::VALIDITY].as_ref(),
            array.dtype().nullability(),
        );
        if !matches!(validity, Validity::NonNullable | Validity::AllValid) {
            return Ok(None);
        }
        let AllOr::Some(indices) = mask.indices() else {
            return Ok(None);
        };
        // The input array already owns validated frames. Revalidating it here
        // would read their headers, violating FilterReduce's buffer-free contract.
        let mut frames = Vec::new();
        let mut metadata = Vec::new();
        let mut selected = Vec::with_capacity(indices.len());
        let (mut source_start, mut kept_rows, mut cursor, mut touched) = (0usize, 0usize, 0, 0);
        for (frame, description) in data.frames.iter().zip(&data.metadata.frames) {
            let rows = usize::try_from(description.n_values)?;
            let source_stop = source_start
                .checked_add(rows)
                .ok_or_else(|| vortex_err!("Zstd frame row count overflow"))?;
            if source_start < data.slice_stop() && source_stop > data.slice_start() {
                touched += 1;
            }
            let first = cursor;
            while cursor < indices.len() {
                let source_row = data.slice_start() + indices[cursor];
                if source_row >= source_stop {
                    break;
                }
                vortex_ensure!(
                    source_row >= source_start,
                    "Invalid Zstd filter row mapping"
                );
                selected.push(
                    kept_rows
                        .checked_add(source_row - source_start)
                        .ok_or_else(|| vortex_err!("Zstd selected row count overflow"))?,
                );
                cursor += 1;
            }
            if cursor != first {
                frames.push(frame.clone());
                metadata.push(description.clone());
                kept_rows = kept_rows
                    .checked_add(rows)
                    .ok_or_else(|| vortex_err!("Zstd retained row count overflow"))?;
            }
            source_start = source_stop;
        }
        vortex_ensure!(
            source_start == data.unsliced_n_rows() && cursor == indices.len(),
            "Zstd frame value counts do not cover the non-null source rows"
        );
        if frames.len() >= touched || selected.is_empty() || kept_rows > array.len() {
            return Ok(None);
        }
        // Keep complete frames and their full logical length so the retained
        // Zstd child is independently serializable. Decline if edge frames of
        // a sliced input would expand the mask beyond its original length.
        let retained = ZstdData::new(
            data.dictionary.clone(),
            frames,
            ZstdMetadata {
                dictionary_size: data.metadata.dictionary_size,
                frames: metadata,
            },
            kept_rows,
        );
        // SAFETY: retain each validated frame together with its unchanged
        // metadata and dictionary. kept_rows is their checked value-count sum;
        // all rows are valid, and this fresh child is unsliced. Normalize the
        // admitted validity so a constant-true source child cannot retain the
        // original row count inside this smaller array. This reads no buffers.
        let array = unsafe {
            Array::from_parts_unchecked(
                ArrayParts::new(Zstd, array.dtype().clone(), kept_rows, retained).with_slots(
                    ZstdSlots {
                        validity: validity_to_child(&validity, kept_rows),
                    }
                    .into_slots(),
                ),
            )
        }
        .into_array();
        Ok(Some(array.filter(Mask::from_indices(kept_rows, selected))?))
    }
}
