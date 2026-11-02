// SPDX-License-Identifier: Apache-2.0
//! Explicit ownership for array metadata, including arrays without buffers.

use super::DecoderMemoryReservation;
use arrow_array::{ArrayRef, RecordBatch, make_array};
use arrow_buffer::{BooleanBuffer, Buffer, NullBuffer};
use arrow_data::ArrayData;
use arrow_schema::ArrowError;
use bytes::Bytes;
use std::sync::Arc;

#[cfg(test)]
#[path = "owned_record_batch_tests.rs"]
mod tests;

/// A decoded batch with its independent array-structure reservation.
///
/// Keep the reservation when retaining detached columns, slices or the batch.
/// Input message storage, the original schema and existing dictionary buffers
/// still require their own owners. This is not an allocator or RSS bound.
#[derive(Debug)]
pub struct OwnedRecordBatch {
    pub(super) batch: RecordBatch,
    pub(super) credit: Arc<dyn DecoderMemoryReservation>,
}

impl OwnedRecordBatch {
    /// Borrow the decoded batch without detaching its structure owner.
    pub fn batch(&self) -> &RecordBatch {
        &self.batch
    }

    /// Retain the structure owner alongside a detached batch or derived array.
    pub fn memory_reservation(&self) -> Arc<dyn DecoderMemoryReservation> {
        self.credit.clone()
    }

    /// Transfer both parts to an ownership-aware consumer.
    pub fn into_parts(self) -> (RecordBatch, Arc<dyn DecoderMemoryReservation>) {
        (self.batch, self.credit)
    }
}

struct OwnedBuffer {
    buffer: Buffer,
    _credit: Arc<dyn DecoderMemoryReservation>,
}

impl AsRef<[u8]> for OwnedBuffer {
    fn as_ref(&self) -> &[u8] {
        self.buffer.as_slice()
    }
}

pub(super) fn attach_buffer(buffer: Buffer, credit: &Arc<dyn DecoderMemoryReservation>) -> Buffer {
    Buffer::from(Bytes::from_owner(OwnedBuffer {
        buffer,
        _credit: credit.clone(),
    }))
}

/// Attach a pre-admitted capacity to fresh buffers. Existing shared buffers
/// keep their original independent owners without a chain of generation leases.
pub(super) fn attach_array(
    array: &ArrayRef,
    credit: &Arc<dyn DecoderMemoryReservation>,
    shared: &[&ArrayData],
) -> Result<ArrayRef, ArrowError> {
    fn existing(buffer: &Buffer, data: &ArrayData) -> bool {
        data.buffers()
            .iter()
            .any(|source| source.data_ptr() == buffer.data_ptr())
            || data
                .nulls()
                .is_some_and(|nulls| nulls.buffer().data_ptr() == buffer.data_ptr())
            || data
                .child_data()
                .iter()
                .any(|child| existing(buffer, child))
    }
    fn buffer(
        input: Buffer,
        credit: &Arc<dyn DecoderMemoryReservation>,
        shared: &[&ArrayData],
    ) -> Buffer {
        if (input.is_empty() && input.capacity() == 0)
            || shared.iter().any(|data| existing(&input, data))
        {
            input
        } else {
            attach_buffer(input, credit)
        }
    }
    fn data(
        input: ArrayData,
        credit: &Arc<dyn DecoderMemoryReservation>,
        shared: &[&ArrayData],
    ) -> Result<ArrayData, ArrowError> {
        let (dtype, len, nulls, offset, buffers, children) = input.into_parts();
        let nulls = nulls.map(|nulls| {
            let bits = nulls.into_inner();
            let bit_offset = bits.offset();
            let bit_length = bits.len();
            NullBuffer::new(BooleanBuffer::new(
                buffer(bits.into_inner(), credit, shared),
                bit_offset,
                bit_length,
            ))
        });
        let buffers = buffers
            .into_iter()
            .map(|b| buffer(b, credit, shared))
            .collect();
        let children = children
            .into_iter()
            .map(|child| data(child, credit, shared))
            .collect::<Result<Vec<_>, _>>()?;
        ArrayData::builder(dtype)
            .len(len)
            .offset(offset)
            .nulls(nulls)
            .buffers(buffers)
            .child_data(children)
            .build()
    }
    Ok(make_array(data(array.to_data(), credit, shared)?))
}
