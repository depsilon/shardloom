// SPDX-License-Identifier: Apache-2.0
// ShardLoom extension, 2026-10-16. See upstream-provenance.json.
//! Shared, admitted frame tables for the pinned Zstd provider.

use std::sync::Arc;

use prost::Message as _;
use prost::encoding::{DecodeContext, WireType, check_wire_type, decode_key, skip_field, uint32};
use vortex_array::buffer::BufferHandle;
use vortex_array::memory::{HostAllocatorRef, HostMemoryReservation};
use vortex_buffer::ByteBuffer;
use vortex_error::{VortexResult, vortex_ensure, vortex_err};

use crate::{ZstdFrameMetadata, ZstdMetadata};

#[cfg(test)]
#[path = "metadata_resources_tests.rs"]
mod tests;

/// The two vectors and their credit have one lifetime. Field order releases
/// their storage before the reservation. Payload buffers retain their own owners.
#[derive(Debug)]
pub(crate) struct FrameStorage {
    pub(crate) dictionary: Option<ByteBuffer>,
    pub(crate) frames: Vec<ByteBuffer>,
    pub(crate) metadata: ZstdMetadata,
    pub(crate) allocator: Option<HostAllocatorRef>,
    _reservation: Option<HostMemoryReservation>,
}

pub(crate) fn capacity_bytes(frames: usize) -> VortexResult<usize> {
    frames
        .checked_mul(size_of::<ByteBuffer>() + size_of::<ZstdFrameMetadata>())
        .ok_or_else(|| vortex_err!("Zstd frame metadata capacity overflow"))
}

impl FrameStorage {
    pub(crate) fn unmanaged(
        dictionary: Option<ByteBuffer>,
        frames: Vec<ByteBuffer>,
        metadata: ZstdMetadata,
    ) -> Self {
        Self {
            dictionary,
            frames,
            metadata,
            allocator: None,
            _reservation: None,
        }
    }

    pub(crate) fn new_in(frames: usize, allocator: &HostAllocatorRef) -> VortexResult<Self> {
        let bytes = capacity_bytes(frames)?;
        let reservation = if bytes == 0 {
            None
        } else {
            Some(allocator.reserve_external(bytes)?)
        };
        let mut storage = Self {
            dictionary: None,
            frames: Vec::new(),
            metadata: ZstdMetadata::default(),
            allocator: Some(Arc::clone(allocator)),
            _reservation: reservation,
        };
        storage
            .frames
            .try_reserve_exact(frames)
            .map_err(|error| vortex_err!("Zstd frame handle allocation failed: {error}"))?;
        storage
            .metadata
            .frames
            .try_reserve_exact(frames)
            .map_err(|error| vortex_err!("Zstd frame metadata allocation failed: {error}"))?;
        storage.check_capacity(frames)?;
        Ok(storage)
    }

    fn check_capacity(&self, frames: usize) -> VortexResult<()> {
        vortex_ensure!(
            self.frames.capacity() == frames && self.metadata.frames.capacity() == frames,
            "Zstd frame allocation exceeded its requested capacity"
        );
        Ok(())
    }

    pub(crate) fn with_buffers_in(
        &self,
        buffers: &[BufferHandle],
        allocator: &HostAllocatorRef,
    ) -> VortexResult<Self> {
        vortex_ensure!(
            self.frames.len() == self.metadata.frames.len(),
            "Zstd frame count does not match metadata"
        );
        let mut storage = Self::new_in(self.frames.len(), allocator)?;
        storage.metadata.dictionary_size = self.metadata.dictionary_size;
        storage
            .metadata
            .frames
            .extend_from_slice(&self.metadata.frames);
        storage.fill_buffers(buffers, self.dictionary.is_some())?;
        Ok(storage)
    }

    fn fill_buffers(&mut self, buffers: &[BufferHandle], dictionary: bool) -> VortexResult<()> {
        let expected = self
            .metadata
            .frames
            .len()
            .checked_add(usize::from(dictionary))
            .ok_or_else(|| vortex_err!("Zstd buffer count overflow"))?;
        vortex_ensure!(
            buffers.len() == expected,
            "Zstd expected {expected} buffers, got {}",
            buffers.len()
        );
        let frames = if dictionary {
            let (first, frames) = buffers
                .split_first()
                .ok_or_else(|| vortex_err!("Zstd dictionary buffer is missing"))?;
            self.dictionary = Some(first.clone().try_to_host_sync()?);
            frames
        } else {
            buffers
        };
        for buffer in frames {
            self.frames.push(buffer.clone().try_to_host_sync()?);
        }
        self.check_capacity(self.metadata.frames.len())
    }

    pub(crate) fn deserialize(
        bytes: &[u8],
        buffers: &[BufferHandle],
        allocator: &HostAllocatorRef,
    ) -> VortexResult<Self> {
        let (frames, dictionary_size) = wire_shape(bytes)?;
        let expected = frames
            .checked_add(usize::from(dictionary_size != 0))
            .ok_or_else(|| vortex_err!("Zstd buffer count overflow"))?;
        vortex_ensure!(
            buffers.len() == expected,
            "Zstd expected {expected} buffers, got {}",
            buffers.len()
        );
        let mut storage = Self::new_in(frames, allocator)?;
        // The immutable first pass counted every top-level repeated message.
        // A frame contains only scalars, so Prost needs no nested collections.
        storage.metadata.merge(bytes)?;
        vortex_ensure!(
            storage.metadata.frames.len() == frames
                && storage.metadata.dictionary_size == dictionary_size,
            "Zstd metadata changed during decoding"
        );
        storage.check_capacity(frames)?;
        storage.fill_buffers(buffers, dictionary_size != 0)?;
        Ok(storage)
    }
}

/// Scan without allocating a collection. Delegate scalar/unknown-field wire
/// semantics and recursive-group limits to the same pinned Prost implementation.
fn wire_shape(mut bytes: &[u8]) -> VortexResult<(usize, u32)> {
    let mut frames = 0usize;
    let mut dictionary_size = 0;
    while !bytes.is_empty() {
        let (tag, wire_type) = decode_key(&mut bytes)?;
        match tag {
            1 => uint32::merge(
                wire_type,
                &mut dictionary_size,
                &mut bytes,
                DecodeContext::default(),
            )?,
            2 => {
                check_wire_type(WireType::LengthDelimited, wire_type)?;
                skip_field(wire_type, tag, &mut bytes, DecodeContext::default())?;
                frames = frames
                    .checked_add(1)
                    .ok_or_else(|| vortex_err!("Zstd metadata frame count overflow"))?;
            }
            _ => skip_field(wire_type, tag, &mut bytes, DecodeContext::default())?,
        }
    }
    Ok((frames, dictionary_size))
}
