//! Bounded source-batch writer subtrees with locally scoped EOF ordering.
//!
//! Unlike the upstream Chunked strategy's global-EOF children, each child owns
//! only descendants of one input sequence. Its EOF therefore precedes the next
//! source item. The wrapped strategy still drives all of its children together.

use std::sync::Arc;

use futures::{StreamExt as _, future::BoxFuture, stream};
use shardloom_exec::live_memory::MemoryLease;
use vortex::{
    error::{VortexResult, vortex_err},
    layout::{
        LayoutRef, LayoutStrategy, LayoutWriterContext, layout_children,
        layouts::chunked::ChunkedLayout,
        segments::SegmentSinkRef,
        sequence::{
            SendableSequentialStream, SequencePointer, SequentialStreamAdapter,
            SequentialStreamExt as _,
        },
    },
    session::VortexSession,
};

pub(super) struct BoundedIngestLayout {
    child: Arc<dyn LayoutStrategy>,
    max_chunks: usize,
    _layout_references: MemoryLease,
}

impl BoundedIngestLayout {
    pub(super) fn new(
        child: Arc<dyn LayoutStrategy>,
        max_chunks: usize,
        layout_references: MemoryLease,
    ) -> Self {
        Self {
            child,
            max_chunks,
            _layout_references: layout_references,
        }
    }
}

impl LayoutStrategy for BoundedIngestLayout {
    fn write_stream<'a, 'b, 'future>(
        &'a self,
        ctx: LayoutWriterContext,
        sink: SegmentSinkRef,
        mut input: SendableSequentialStream,
        eof: SequencePointer,
        session: &'b VortexSession,
    ) -> BoxFuture<'future, VortexResult<LayoutRef>>
    where
        'a: 'future,
        'b: 'future,
        Self: 'future,
    {
        Box::pin(async move {
            let dtype = input.dtype().clone();
            let mut children = Vec::new();
            children
                .try_reserve_exact(self.max_chunks)
                .map_err(|error| {
                    vortex_err!("bounded ingest layout-reference allocation failed: {error}")
                })?;
            let mut rows = 0_u64;
            while let Some(item) = input.next().await {
                if children.len() >= self.max_chunks {
                    return Err(vortex_err!(
                        "bounded ingest exceeded its admitted source batch count"
                    ));
                }
                let (sequence, array) = item?;
                rows = rows
                    .checked_add(
                        u64::try_from(array.len())
                            .map_err(|_| vortex_err!("bounded ingest row overflow"))?,
                    )
                    .ok_or_else(|| vortex_err!("bounded ingest row overflow"))?;
                // [item,0,...] < [item,1] < [next_item]. No child EOF
                // depends on polling a later parent item or the global EOF.
                let (start, child_eof) = sequence.descend().split();
                let child = self
                    .child
                    .write_stream(
                        ctx.clone(),
                        Arc::clone(&sink),
                        SequentialStreamAdapter::new(
                            dtype.clone(),
                            stream::iter([Ok((start.downgrade(), array))]),
                        )
                        .sendable(),
                        child_eof,
                        session,
                    )
                    .await?;
                children.push(child);
            }
            // Keep the enclosing EOF alive until all locally scoped children
            // finish. Child strategies may emit metadata at their own EOF.
            drop(eof);
            Ok(ChunkedLayout::new(rows, dtype, layout_children(children)).into_layout())
        })
    }
}
