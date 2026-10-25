// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors
// Modified by ShardLoom on 2026-10-21: retain allocator admission with shared
// parsed segment-table storage. See upstream-provenance.json.

use std::alloc::Layout as AllocationLayout;
use std::ops::Deref;
use std::ops::Range;
use std::sync::Arc;

use crate::flatbuffers::footer as fb;
use vortex_array::memory::HostMemoryReservation;
use vortex_array::memory::HostMemorySessionExt;
use vortex_buffer::Alignment;
use vortex_error::VortexError;
use vortex_error::VortexResult;
use vortex_error::vortex_err;
use vortex_session::VortexSession;

/// An immutable segment table whose clones retain its storage owner.
///
/// [`Self::try_from_fn`] admits the table before allocation. Conversion from an
/// existing `Arc<[SegmentSpec]>` preserves caller-owned storage without claiming
/// allocator admission. Borrowed slice access never exports an ownerless alias
/// of an admitted table; clone the handle to retain it independently.
#[derive(Clone, Debug, Default)]
pub struct SegmentMap(SegmentMapStorage);

#[derive(Clone, Debug, Default)]
enum SegmentMapStorage {
    #[default]
    Empty,
    CallerOwned(Arc<[SegmentSpec]>),
    Admitted(Arc<OwnedSegmentMap>),
}

#[derive(Debug)]
struct OwnedSegmentMap {
    values: Vec<SegmentSpec>,
    // Storage is released before its credit, including on the final clone.
    _reservation: HostMemoryReservation,
}

impl SegmentMap {
    /// Admit and construct exactly `count` segment specifications in index order.
    ///
    /// Size overflow or allocation denial occurs before `segment` is invoked.
    /// A producer error drops the partial table and its reservation. Empty maps
    /// allocate no table and do not call the producer or allocator.
    pub fn try_from_fn(
        count: usize,
        mut segment: impl FnMut(usize) -> VortexResult<SegmentSpec>,
        session: &VortexSession,
    ) -> VortexResult<Self> {
        let layout = AllocationLayout::array::<SegmentSpec>(count)
            .map_err(|_| vortex_err!("Segment map allocation size overflow"))?;
        if count == 0 {
            return Ok(Self::default());
        }
        let reservation = session.host_allocator().reserve_external(layout.size())?;
        let mut values = Vec::with_capacity(count);
        for index in 0..count {
            values.push(segment(index)?);
        }
        debug_assert_eq!(values.capacity(), count);
        Ok(Self(SegmentMapStorage::Admitted(Arc::new(
            OwnedSegmentMap {
                values,
                _reservation: reservation,
            },
        ))))
    }
}

impl From<Arc<[SegmentSpec]>> for SegmentMap {
    fn from(segments: Arc<[SegmentSpec]>) -> Self {
        if segments.is_empty() {
            Self::default()
        } else {
            Self(SegmentMapStorage::CallerOwned(segments))
        }
    }
}

impl Deref for SegmentMap {
    type Target = [SegmentSpec];

    fn deref(&self) -> &Self::Target {
        match &self.0 {
            SegmentMapStorage::Empty => &[],
            SegmentMapStorage::CallerOwned(segments) => segments,
            SegmentMapStorage::Admitted(owner) => &owner.values,
        }
    }
}

impl AsRef<[SegmentSpec]> for SegmentMap {
    fn as_ref(&self) -> &[SegmentSpec] {
        self
    }
}

/// The location of a segment within a Vortex file.
///
/// A segment is a contiguous block of bytes in a file that contains a part of the file's data.
/// The `SegmentSpec` struct specifies the location and properties of a segment.
#[derive(Clone, Copy, Debug)]
pub struct SegmentSpec {
    /// The byte offset of the segment from the start of the file.
    pub offset: u64,
    /// The length of the segment in bytes.
    pub length: u32,
    /// The memory alignment requirement of the segment.
    pub alignment: Alignment,
}

impl SegmentSpec {
    /// Returns the byte range of the segment within the file.
    ///
    /// The range starts at the segment's offset and extends for its length.
    pub fn byte_range(&self) -> Range<u64> {
        self.offset..self.offset + u64::from(self.length)
    }
}

impl From<&SegmentSpec> for fb::SegmentSpec {
    fn from(value: &SegmentSpec) -> Self {
        fb::SegmentSpec::new(value.offset, value.length, value.alignment.exponent(), 0, 0)
    }
}

impl TryFrom<&fb::SegmentSpec> for SegmentSpec {
    type Error = VortexError;

    fn try_from(value: &fb::SegmentSpec) -> Result<Self, Self::Error> {
        Ok(Self {
            offset: value.offset(),
            length: value.length(),
            // The alignment exponent comes from the file and may be corrupt, so validate it rather
            // than panicking on a too-large shift (see issue #8819).
            alignment: Alignment::try_from_untrusted_exponent(value.alignment_exponent())?,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_out_of_range_alignment_exponent() {
        // A fuzzed segment spec can declare an alignment exponent that would overflow a usize
        // shift. Parsing it must return an error rather than panicking (see issue #8819).
        let fb_spec = fb::SegmentSpec::new(0, 0, u8::MAX, 0, 0);
        let err = SegmentSpec::try_from(&fb_spec).unwrap_err();
        assert!(err.to_string().contains("too large"), "{err}");
    }

    #[test]
    fn rejects_excessive_alignment_exponent() {
        // A representable alignment can still cause an unreasonable allocation when a segment is
        // copied to satisfy it.
        let fb_spec = fb::SegmentSpec::new(0, 0, 17, 0, 0);
        let err = SegmentSpec::try_from(&fb_spec).unwrap_err();
        assert!(err.to_string().contains("exceeds"), "{err}");
    }
}
