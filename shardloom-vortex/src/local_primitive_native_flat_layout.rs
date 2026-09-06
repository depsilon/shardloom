//! A bounded native leaf writer. The pinned upstream chunked strategy starts
//! unbounded child tasks; this narrow strategy awaits each Flat leaf before
//! admitting the next array. It does not support arbitrary child strategies:
//! those may need concurrent EOF progress, while Flat never writes at child EOF.

use futures::{StreamExt as _, future::BoxFuture, stream};
use std::sync::Arc;
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

/// The caller reserves the admitted layout metadata before constructing a
/// writer. `max_chunks` also bounds the retained layout-reference vector.
pub(crate) struct SequentialNativeFlatLayout {
    max_chunks: usize,
}

impl SequentialNativeFlatLayout {
    pub(crate) fn strategy(max_chunks: usize) -> Arc<dyn LayoutStrategy> {
        Arc::new(Self { max_chunks })
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
            let dtype = input.dtype().clone();
            let mut children = Vec::new();
            children
                .try_reserve_exact(self.max_chunks)
                .map_err(|error| vortex_err!("native leaf layout reservation failed: {error}"))?;
            let mut rows = 0_u64;
            while let Some(item) = input.next().await {
                if children.len() >= self.max_chunks {
                    return Err(vortex_err!(
                        "native leaf writer exceeded admitted chunk count"
                    ));
                }
                let item = item?;
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
