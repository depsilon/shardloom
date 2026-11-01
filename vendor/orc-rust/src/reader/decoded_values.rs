// SPDX-License-Identifier: Apache-2.0
//! Fallible, precredited payload allocations. Arrow slices keep the same owner.
use std::{
    io,
    mem::size_of,
    ops::{Deref, DerefMut},
    sync::Arc,
};

use arrow::{
    array::OffsetSizeTrait,
    buffer::{BooleanBuffer, Buffer, OffsetBuffer},
    datatypes::ArrowNativeType,
};
use bytes::Bytes;
use snafu::ResultExt;

use super::{invalid, DecoderMemory, DecoderMemoryReservation};
use crate::error::{IoSnafu, Result};

pub(crate) type Memory = Option<Arc<dyn DecoderMemory>>;

pub(crate) struct Values<T> {
    values: Vec<T>,
    credit: Option<Box<dyn DecoderMemoryReservation>>,
}

impl<T: Copy + Default> Values<T> {
    pub(crate) fn zeroed(length: usize, memory: &Memory) -> Result<Self> {
        let bytes = length
            .checked_mul(size_of::<T>())
            .ok_or_else(|| invalid("decoded value allocation overflow"))
            .context(IoSnafu)?;
        let credit = memory
            .as_ref()
            .map(|memory| memory.reserve(bytes))
            .transpose()
            .context(IoSnafu)?;
        let mut values = Vec::new();
        values
            .try_reserve_exact(length)
            .map_err(|error| io::Error::new(io::ErrorKind::Other, error))
            .context(IoSnafu)?;
        if values.capacity() > length {
            return Err(invalid("decoded allocation exceeds its reservation")).context(IoSnafu);
        }
        values.resize(length, T::default());
        Ok(Self { values, credit })
    }
}

impl<T> Deref for Values<T> {
    type Target = [T];
    fn deref(&self) -> &[T] {
        &self.values
    }
}

impl<T> DerefMut for Values<T> {
    fn deref_mut(&mut self) -> &mut [T] {
        &mut self.values
    }
}

impl<T: ArrowNativeType> Values<T> {
    pub(crate) fn into_buffer(self) -> Buffer {
        let buffer = Buffer::from_vec(self.values);
        match self.credit {
            Some(credit) => Buffer::from(Bytes::from_owner(OwnedBuffer {
                buffer,
                _credit: credit,
            })),
            None => buffer,
        }
    }
}

struct OwnedBuffer {
    buffer: Buffer,
    // Field order frees the allocation before returning its credit.
    _credit: Box<dyn DecoderMemoryReservation>,
}

impl AsRef<[u8]> for OwnedBuffer {
    fn as_ref(&self) -> &[u8] {
        self.buffer.as_slice()
    }
}

pub(crate) fn bitmap(
    length: usize,
    memory: &Memory,
    mut value: impl FnMut(usize) -> bool,
) -> Result<BooleanBuffer> {
    let bytes = length
        .checked_add(7)
        .ok_or_else(|| invalid("bitmap length overflow"))
        .context(IoSnafu)?
        / 8;
    let mut data = Values::<u8>::zeroed(bytes, memory)?;
    for index in 0..length {
        if value(index) {
            data[index / 8] |= 1 << (index % 8);
        }
    }
    Ok(BooleanBuffer::new(data.into_buffer(), 0, length))
}

pub(crate) fn total_length(lengths: &[i64], maximum: usize) -> Result<usize> {
    lengths.iter().try_fold(0usize, |total, &length| {
        let length = usize::try_from(length)
            .map_err(|_| invalid("negative decoded length"))
            .context(IoSnafu)?;
        let total = total
            .checked_add(length)
            .filter(|total| *total <= maximum)
            .ok_or_else(|| invalid("decoded length exceeds its offset domain"))
            .context(IoSnafu)?;
        Ok(total)
    })
}

pub(crate) fn offsets<O: OffsetSizeTrait>(
    lengths: &[i64],
    memory: &Memory,
) -> Result<OffsetBuffer<O>> {
    total_length(lengths, O::MAX_OFFSET)?;
    let count = lengths
        .len()
        .checked_add(1)
        .ok_or_else(|| invalid("offset count overflow"))
        .context(IoSnafu)?;
    let mut offsets = Values::<O>::zeroed(count, memory)?;
    let mut total = 0;
    for (index, &length) in lengths.iter().enumerate() {
        total += length as usize;
        offsets[index + 1] = O::from_usize(total)
            .ok_or_else(|| invalid("offset conversion overflow"))
            .context(IoSnafu)?;
    }
    Ok(OffsetBuffer::new(offsets.into_buffer().into()))
}
