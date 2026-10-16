// SPDX-License-Identifier: Apache-2.0
// ShardLoom extension, 2026-10-16: admitted variable-binary table capacity.

use std::mem::size_of;
use std::ops::Deref;
use std::sync::Arc;

use vortex_buffer::ByteBuffer;
use vortex_error::VortexResult;
use vortex_error::vortex_ensure;
use vortex_error::vortex_err;

use crate::memory::HostAllocatorRef;
use crate::memory::HostMemoryReservation;

/// A finite, allocator-admitted list of host data buffers used while constructing
/// a VarBinView table. The fixed capacity is checked before each append. Payload
/// byte storage is owned independently; this list retains only its element credit.
#[derive(Debug)]
pub struct VarBinViewBufferBuilder {
    buffers: MetadataVec<ByteBuffer>,
}

impl VarBinViewBufferBuilder {
    /// Admit the requested element capacity before allocating the list.
    pub fn try_with_capacity(capacity: usize, allocator: &HostAllocatorRef) -> VortexResult<Self> {
        Ok(Self {
            buffers: MetadataVec::new(capacity, allocator)?,
        })
    }

    /// Append without growing beyond the admitted capacity.
    pub fn try_push(&mut self, buffer: ByteBuffer) -> VortexResult<()> {
        self.buffers.push(buffer)
    }

    /// The exact element capacity admitted before this collection was allocated.
    pub fn capacity(&self) -> usize {
        self.buffers.values.capacity()
    }
}

impl Deref for VarBinViewBufferBuilder {
    type Target = [ByteBuffer];

    fn deref(&self) -> &Self::Target {
        &self.buffers
    }
}

impl AsRef<[ByteBuffer]> for VarBinViewBufferBuilder {
    fn as_ref(&self) -> &[ByteBuffer] {
        &self.buffers
    }
}

/// Requested collection element storage, excluding allocator and Arc headers.
#[derive(Debug)]
pub(crate) struct MetadataReservation {
    pub(crate) allocator: HostAllocatorRef,
    pub(crate) bytes: usize,
    _credit: Option<HostMemoryReservation>,
}

impl MetadataReservation {
    pub(crate) fn new<T>(allocator: &HostAllocatorRef, count: usize) -> VortexResult<Self> {
        let bytes = count
            .checked_mul(size_of::<T>())
            .ok_or_else(|| vortex_err!("VarBinView metadata capacity overflow"))?;
        let credit = if bytes == 0 {
            None
        } else {
            Some(allocator.reserve_external(bytes)?)
        };
        Ok(Self {
            allocator: Arc::clone(allocator),
            bytes,
            _credit: credit,
        })
    }

    /// Admit the old/new capacity overlap before moving a vector's elements.
    /// A denial or allocation error leaves the original vector and credit intact.
    pub(crate) fn grow_vec<T>(&mut self, values: &mut Vec<T>, capacity: usize) -> VortexResult<()> {
        vortex_ensure!(
            self.bytes
                == values
                    .capacity()
                    .checked_mul(size_of::<T>())
                    .ok_or_else(|| vortex_err!("VarBinView metadata capacity overflow"))?,
            "VarBinView builder capacity differs from its admission"
        );
        if capacity <= values.capacity() {
            return Ok(());
        }
        let replacement_owner = Self::new::<T>(&self.allocator, capacity)?;
        let mut replacement = exact_vec(capacity)?;
        replacement.append(values);
        let old = std::mem::replace(values, replacement);
        drop(old);
        *self = replacement_owner;
        Ok(())
    }
}

/// A private table-building vector. Storage is always freed before its credit.
#[derive(Debug)]
pub(crate) struct MetadataVec<T> {
    values: Vec<T>,
    reservation: MetadataReservation,
}

impl<T> MetadataVec<T> {
    pub(crate) fn new(capacity: usize, allocator: &HostAllocatorRef) -> VortexResult<Self> {
        let reservation = MetadataReservation::new::<T>(allocator, capacity)?;
        let values = exact_vec(capacity)?;
        Ok(Self {
            values,
            reservation,
        })
    }

    pub(crate) fn push(&mut self, value: T) -> VortexResult<()> {
        vortex_ensure!(
            self.values.len() < self.values.capacity(),
            "VarBinView metadata exceeded its admitted element capacity"
        );
        self.values.push(value);
        Ok(())
    }

    pub(crate) fn from_iter(
        values: impl ExactSizeIterator<Item = T>,
        allocator: &HostAllocatorRef,
    ) -> VortexResult<Self> {
        let expected = values.len();
        let mut result = Self::new(expected, allocator)?;
        for value in values {
            result.push(value)?;
        }
        vortex_ensure!(
            result.len() == expected,
            "VarBinView metadata iterator changed its declared length"
        );
        Ok(result)
    }

    pub(crate) fn into_arc(self) -> VortexResult<(Arc<[T]>, Arc<MetadataReservation>)> {
        // Arc::from(Vec) allocates a distinct slice before freeing the vector.
        let final_owner = MetadataReservation::new::<T>(&self.reservation.allocator, self.len())?;
        let Self {
            values,
            reservation,
        } = self;
        let values = Arc::from(values);
        drop(reservation);
        Ok((values, Arc::new(final_owner)))
    }
}

impl<T> Deref for MetadataVec<T> {
    type Target = [T];

    fn deref(&self) -> &Self::Target {
        &self.values
    }
}

fn exact_vec<T>(capacity: usize) -> VortexResult<Vec<T>> {
    let mut values = Vec::new();
    values
        .try_reserve_exact(capacity)
        .map_err(|error| vortex_err!("VarBinView metadata allocation failed: {error}"))?;
    vortex_ensure!(
        values.capacity() == capacity,
        "VarBinView metadata allocator exceeded the requested capacity"
    );
    Ok(values)
}
