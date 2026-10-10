//! A bounded native leaf writer. The pinned upstream chunked strategy starts
//! unbounded child tasks; this narrow strategy awaits each Flat leaf before
//! admitting the next array. It does not support arbitrary child strategies:
//! those may need concurrent EOF progress, while Flat never writes at child EOF.

use futures::{StreamExt as _, future::BoxFuture, stream};
use shardloom_exec::live_memory::MemoryLease;
use std::sync::{Arc, Mutex};
use vortex::{
    error::{VortexResult, vortex_err},
    layout::{
        LayoutRef, LayoutStrategy, LayoutWriterContext, layout_children,
        layouts::{chunked::ChunkedLayout, flat::writer::FlatLayoutStrategy},
        segments::SegmentSinkRef,
        sequence::{
            SendableSequentialStream, SequencePointer, SequentialStreamAdapter,
            SequentialStreamExt as _,
        },
    },
    session::VortexSession,
};

#[cfg(all(feature = "vortex-write", unix))]
use vortex::array::{ArrayId, ArrayRef, ExecutionCtx, IntoArray as _, RecursiveCanonical};

/// Preserve admitted encodings; complete only pending native work that cannot
/// be serialized in the selected file edition. This is an explicit native
/// materialization boundary shared by durable and in-memory native sinks.
#[cfg(all(feature = "vortex-write", unix))]
pub(crate) fn complete_for_serialization(
    array: ArrayRef,
    allowed: &std::collections::BTreeSet<ArrayId>,
    context: &mut ExecutionCtx,
) -> VortexResult<(ArrayRef, bool)> {
    if array
        .depth_first_traversal()
        .any(|node| !allowed.contains(&node.encoding_id()))
    {
        Ok((
            array.execute::<RecursiveCanonical>(context)?.0.into_array(),
            true,
        ))
    } else {
        Ok((array, false))
    }
}

/// The caller reserves the admitted layout metadata before constructing a
/// writer. `max_chunks` also bounds the retained layout-reference vector.
pub(crate) struct SequentialNativeFlatLayout {
    max_chunks: usize,
    metadata: Option<Arc<Mutex<MemoryLease>>>,
}

impl SequentialNativeFlatLayout {
    pub(crate) fn strategy(max_chunks: usize) -> Arc<dyn LayoutStrategy> {
        Arc::new(Self {
            max_chunks,
            metadata: None,
        })
    }

    /// Computed streams grow footer credit before accepting each leaf; they do
    /// not allocate a worst-case source-sized layout vector in advance.
    #[cfg(all(unix, feature = "vortex-write"))]
    pub(crate) fn accounted_strategy(
        max_chunks: usize,
        metadata: Arc<Mutex<MemoryLease>>,
    ) -> Arc<dyn LayoutStrategy> {
        Arc::new(Self {
            max_chunks,
            metadata: Some(metadata),
        })
    }
}

impl LayoutStrategy for SequentialNativeFlatLayout {
    fn write_stream<'a, 'b, 'future>(
        &'a self,
        ctx: LayoutWriterContext,
        segment_sink: SegmentSinkRef,
        mut input: SendableSequentialStream,
        mut eof: SequencePointer,
        session: &'b VortexSession,
    ) -> BoxFuture<'future, VortexResult<LayoutRef>>
    where
        'a: 'future,
        'b: 'future,
        Self: 'future,
    {
        Box::pin(async move {
            let metadata_base = match &self.metadata {
                Some(metadata) => metadata
                    .lock()
                    .map_err(|_| vortex_err!("native leaf metadata owner poisoned"))?
                    .bytes(),
                None => 0,
            };
            let dtype = input.dtype().clone();
            let mut children = Vec::new();
            if self.metadata.is_none() {
                children
                    .try_reserve_exact(self.max_chunks)
                    .map_err(|error| {
                        vortex_err!("native leaf layout reservation failed: {error}")
                    })?;
            }
            let mut rows = 0_u64;
            while let Some(item) = input.next().await {
                if children.len() >= self.max_chunks {
                    return Err(vortex_err!(
                        "native leaf writer exceeded admitted chunk count"
                    ));
                }
                let item = item?;
                if let Some(metadata) = &self.metadata {
                    let bytes = u64::try_from(children.len() + 1)
                        .ok()
                        .and_then(|n| n.checked_mul(8192))
                        .and_then(|n| n.checked_add(metadata_base))
                        .ok_or_else(|| vortex_err!("native leaf metadata size overflow"))?;
                    metadata
                        .lock()
                        .map_err(|_| vortex_err!("native leaf metadata owner poisoned"))?
                        .resize(bytes)
                        .map_err(|error| {
                            vortex_err!("native leaf metadata reservation failed: {error}")
                        })?;
                    children.try_reserve_exact(1).map_err(|error| {
                        vortex_err!("native leaf layout reservation failed: {error}")
                    })?;
                }
                rows = rows
                    .checked_add(
                        u64::try_from(item.1.len())
                            .map_err(|_| vortex_err!("native leaf row overflow"))?,
                    )
                    .ok_or_else(|| vortex_err!("native leaf row overflow"))?;
                let leaf = FlatLayoutStrategy::default()
                    .write_stream(
                        ctx.clone(),
                        Arc::clone(&segment_sink),
                        SequentialStreamAdapter::new(dtype.clone(), stream::iter([Ok(item)]))
                            .sendable(),
                        eof.split_off(),
                        session,
                    )
                    .await?;
                children.push(leaf);
            }
            Ok(ChunkedLayout::new(rows, dtype, layout_children(children)).into_layout())
        })
    }
}
