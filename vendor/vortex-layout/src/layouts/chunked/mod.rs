// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors
// Modified by ShardLoom on 2026-10-21: admit shared row-offset storage and
// propagate reader-cache admission errors.

pub(crate) mod reader;
pub mod writer;

use std::alloc::Layout as AllocationLayout;
use std::ops::Deref;
use std::sync::Arc;

use vortex_array::EmptyMetadata;
use vortex_array::dtype::DType;
use vortex_array::memory::HostAllocatorRef;
use vortex_array::memory::HostMemoryReservation;
use vortex_array::memory::MemorySessionExt;
use vortex_error::VortexExpect;
use vortex_error::VortexResult;
use vortex_error::vortex_bail;
use vortex_error::vortex_err;
use vortex_session::VortexSession;
use vortex_session::registry::CachedId;

use crate::Layout;
use crate::LayoutChildType;
use crate::LayoutChildren;
use crate::LayoutDeserializeArgs;
use crate::LayoutId;
use crate::LayoutParts;
use crate::LayoutReaderContext;
use crate::LayoutReaderRef;
use crate::LayoutRef;
use crate::VTable;
use crate::children::OwnedLayoutChildren;
use crate::layouts::chunked::reader::ChunkedReader;
use crate::segments::SegmentSource;

/// Chunked layout vtable.
#[derive(Clone, Debug)]
pub struct Chunked;

/// Backwards-compatible name for the chunked layout plugin.
pub use Chunked as ChunkedLayoutEncoding;

/// Chunked-layout-specific data.
#[derive(Clone, Debug)]
pub struct ChunkedData {
    chunk_offsets: ChunkOffsets,
}

#[derive(Clone, Debug)]
enum ChunkOffsets {
    // The initial/final boundary for an empty layout needs no heap allocation.
    Empty,
    Shared(Arc<OwnedChunkOffsets>),
}

#[derive(Debug)]
struct OwnedChunkOffsets {
    values: Vec<u64>,
    // Last field: release storage before its credit, including data-only clones.
    _reservation: Option<HostMemoryReservation>,
}

impl Deref for ChunkOffsets {
    type Target = [u64];

    fn deref(&self) -> &Self::Target {
        match self {
            Self::Empty => &[0],
            Self::Shared(owner) => &owner.values,
        }
    }
}

/// A layout partitioned into independently readable row chunks.
pub type ChunkedLayout = Layout<Chunked>;

impl VTable for Chunked {
    type LayoutData = ChunkedData;
    type Metadata = EmptyMetadata;

    fn id(&self) -> LayoutId {
        static ID: CachedId = CachedId::new("vortex.chunked");
        *ID
    }

    fn metadata(_layout: &Layout<Self>) -> Self::Metadata {
        EmptyMetadata
    }

    fn deserialize(
        &self,
        args: &LayoutDeserializeArgs<'_>,
        _metadata: &EmptyMetadata,
    ) -> VortexResult<Self::LayoutData> {
        let chunk_offsets = chunk_offsets(args.children, Some(args.session.allocator()))?;
        if chunk_offsets.last().copied() != Some(args.row_count) {
            vortex_bail!("Chunked child row counts do not add up to parent row count");
        }
        Ok(ChunkedData { chunk_offsets })
    }

    fn child_dtype(layout: &Layout<Self>, _idx: usize) -> VortexResult<DType> {
        Ok(layout.dtype().clone())
    }

    fn child_type(layout: &Layout<Self>, idx: usize) -> LayoutChildType {
        LayoutChildType::Chunk((idx, layout.chunk_offsets[idx]))
    }

    fn new_reader(
        layout: &Layout<Self>,
        name: Arc<str>,
        segment_source: Arc<dyn SegmentSource>,
        session: &VortexSession,
        ctx: &LayoutReaderContext,
    ) -> VortexResult<LayoutReaderRef> {
        Ok(Arc::new(ChunkedReader::try_new(
            layout.clone(),
            name,
            segment_source,
            session,
            ctx.clone(),
        )?))
    }
}

impl Layout<Chunked> {
    /// Construct a chunked layout outside session allocator admission.
    ///
    /// This caller-directed compatibility constructor panics on invalid row
    /// counts. Native providers should use [`Self::try_new`].
    pub fn new(row_count: u64, dtype: DType, children: Arc<dyn LayoutChildren>) -> Self {
        let offsets =
            chunk_offsets(children.as_ref(), None).vortex_expect("chunk row counts overflow");
        assert_eq!(
            offsets.last().copied(),
            Some(row_count),
            "Row count mismatch"
        );
        LayoutParts::new(
            Chunked,
            dtype,
            row_count,
            Vec::new(),
            children,
            ChunkedData {
                chunk_offsets: offsets,
            },
        )
        .into_typed()
    }

    /// Construct a chunked layout with session-admitted offset storage.
    ///
    /// Supplied children and DTypes retain their independent ownership. Clones
    /// of this layout's data share its immutable offset storage and reservation.
    pub fn try_new(
        row_count: u64,
        dtype: DType,
        children: Arc<dyn LayoutChildren>,
        session: &VortexSession,
    ) -> VortexResult<Self> {
        let offsets = chunk_offsets(children.as_ref(), Some(session.allocator()))?;
        if offsets.last().copied() != Some(row_count) {
            vortex_bail!("Chunked child row counts do not add up to parent row count");
        }
        Ok(LayoutParts::new(
            Chunked,
            dtype,
            row_count,
            Vec::new(),
            children,
            ChunkedData {
                chunk_offsets: offsets,
            },
        )
        .into_typed())
    }

    /// Rebuild with owned children outside session allocator admission.
    ///
    /// Use [`Self::try_new`] for admitted reconstruction with a session.
    pub fn with_children(&self, children: Vec<LayoutRef>) -> Self {
        Self::new(
            self.row_count(),
            self.dtype().clone(),
            OwnedLayoutChildren::layout_children(children),
        )
    }
}

fn chunk_offsets(
    children: &dyn LayoutChildren,
    allocator: Option<HostAllocatorRef>,
) -> VortexResult<ChunkOffsets> {
    let nchildren = children.nchildren();
    if nchildren == 0 {
        return Ok(ChunkOffsets::Empty);
    }
    let count = nchildren
        .checked_add(1)
        .ok_or_else(|| vortex_err!("Chunk offset table size overflow: {nchildren}"))?;
    let bytes = AllocationLayout::array::<u64>(count)
        .map_err(|_| vortex_err!("Chunk offset table size overflow: {nchildren}"))?
        .size();
    let reservation = allocator
        .map(|allocator| allocator.reserve_external(bytes))
        .transpose()?;
    let mut offsets = Vec::with_capacity(count);
    debug_assert_eq!(offsets.capacity(), count);
    offsets.push(0u64);
    for idx in 0..nchildren {
        offsets.push(
            offsets[idx]
                .checked_add(children.child_row_count(idx))
                .ok_or_else(|| vortex_err!("Chunked child row counts overflow"))?,
        );
    }
    Ok(ChunkOffsets::Shared(Arc::new(OwnedChunkOffsets {
        values: offsets,
        _reservation: reservation,
    })))
}
