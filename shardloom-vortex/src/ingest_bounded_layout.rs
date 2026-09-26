//! Bounded source-batch writer subtrees with locally scoped EOF ordering.
//!
//! Unlike the upstream Chunked strategy's global-EOF children, each child owns
//! only descendants of one input sequence. Its EOF therefore precedes the next
//! source item. The wrapped strategy still drives all of its children together.

use std::sync::{
    Arc, Mutex,
    atomic::{AtomicBool, Ordering},
};

use futures::{StreamExt as _, future::BoxFuture, stream};
use shardloom_exec::live_memory::MemoryLease;
use vortex::{
    array::dtype::DType,
    error::{VortexResult, vortex_err},
    layout::{
        LayoutChildren, LayoutRef, LayoutStrategy, LayoutWriterContext, layout_children,
        layouts::chunked::ChunkedLayout,
        segments::SegmentSinkRef,
        sequence::{
            SendableSequentialStream, SequencePointer, SequentialStreamAdapter,
            SequentialStreamExt as _,
        },
    },
    session::VortexSession,
};

pub(crate) struct BoundedIngestLayout {
    child: Arc<dyn LayoutStrategy>,
    initial_chunks: usize,
    // Shared with the returned children: the provider drops the strategy before
    // serializing the footer, while the root and its references remain live.
    layout_references: Arc<Mutex<MemoryLease>>,
    started: AtomicBool,
}

struct ReservedLayoutChildren {
    // Drop the native collection before releasing its reservation.
    inner: Arc<dyn LayoutChildren>,
    references: Arc<Mutex<MemoryLease>>,
}

impl LayoutChildren for ReservedLayoutChildren {
    fn to_arc(&self) -> Arc<dyn LayoutChildren> {
        Arc::new(Self {
            // Native OwnedLayoutChildren::to_arc clones its Vec. Share the
            // existing collection instead, so clones need no second Vec lease.
            inner: Arc::clone(&self.inner),
            references: Arc::clone(&self.references),
        })
    }

    fn child(&self, idx: usize, dtype: &DType) -> VortexResult<LayoutRef> {
        self.inner.child(idx, dtype)
    }

    fn child_row_count(&self, idx: usize) -> u64 {
        self.inner.child_row_count(idx)
    }

    fn nchildren(&self) -> usize {
        self.inner.nchildren()
    }

    fn child_is_indivisible(&self, idx: usize) -> bool {
        self.inner.child_is_indivisible(idx)
    }
}

impl BoundedIngestLayout {
    pub(crate) fn new(
        child: Arc<dyn LayoutStrategy>,
        initial_chunks: usize,
        layout_references: MemoryLease,
    ) -> Self {
        Self {
            child,
            initial_chunks,
            layout_references: Arc::new(Mutex::new(layout_references)),
            started: AtomicBool::new(false),
        }
    }

    fn reserved_children(&self, children: Vec<LayoutRef>) -> Arc<dyn LayoutChildren> {
        Arc::new(ReservedLayoutChildren {
            inner: layout_children(children),
            references: Arc::clone(&self.layout_references),
        })
    }

    fn grow_references(&self, children: &mut Vec<LayoutRef>, capacity: usize) -> VortexResult<()> {
        let reference_bytes = |count: usize| {
            count
                .checked_mul(std::mem::size_of::<LayoutRef>())
                .and_then(|bytes| u64::try_from(bytes).ok())
                .ok_or_else(|| vortex_err!("bounded ingest layout-reference size overflow"))
        };
        let previous = children.capacity();
        if capacity <= previous {
            return Ok(());
        }
        let transient = previous
            .checked_add(capacity)
            .ok_or_else(|| vortex_err!("bounded ingest layout-reference capacity overflow"))?;
        let mut reservation = self
            .layout_references
            .lock()
            .map_err(|_| vortex_err!("bounded ingest layout-reference reservation poisoned"))?;
        // A reallocating Vec may retain its old allocation while obtaining the
        // replacement. Reserve both before requesting either allocation.
        reservation
            .resize(reference_bytes(transient)?)
            .map_err(|error| vortex_err!("bounded ingest layout-reference admission: {error}"))?;
        if let Err(error) = children.try_reserve_exact(capacity - children.len()) {
            reservation
                .resize(reference_bytes(previous)?)
                .map_err(|error| vortex_err!("bounded ingest reservation release: {error}"))?;
            return Err(vortex_err!(
                "bounded ingest layout-reference allocation failed: {error}"
            ));
        }
        reservation
            .resize(reference_bytes(children.capacity())?)
            .map_err(|error| vortex_err!("bounded ingest layout-reference admission: {error}"))?;
        Ok(())
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
            if self.started.swap(true, Ordering::AcqRel) {
                return Err(vortex_err!(
                    "bounded ingest layout strategy is owned by one writer"
                ));
            }
            let dtype = input.dtype().clone();
            let mut children = Vec::new();
            self.grow_references(&mut children, self.initial_chunks)?;
            let mut rows = 0_u64;
            while let Some(item) = input.next().await {
                let (sequence, array) = item?;
                if array.is_empty() {
                    continue;
                }
                if children.len() == children.capacity() {
                    let capacity = children
                        .capacity()
                        .checked_mul(2)
                        .ok_or_else(|| {
                            vortex_err!("bounded ingest layout-reference capacity overflow")
                        })?
                        .max(1);
                    self.grow_references(&mut children, capacity)?;
                }
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
            Ok(ChunkedLayout::new(rows, dtype, self.reserved_children(children)).into_layout())
        })
    }
}

#[cfg(test)]
mod tests {
    use super::BoundedIngestLayout;
    use shardloom_exec::live_memory::LiveMemoryPool;
    use std::sync::Arc;
    use vortex::{
        array::dtype::{DType, Nullability},
        layout::{
            LayoutRef, layout_children,
            layouts::{chunked::ChunkedLayout, flat::writer::FlatLayoutStrategy},
        },
    };

    fn empty_child() -> LayoutRef {
        ChunkedLayout::new(
            0,
            DType::Bool(Nullability::NonNullable),
            layout_children(Vec::new()),
        )
        .into_layout()
    }

    #[test]
    fn reference_growth_charges_transient_storage_and_releases_after_error() {
        let unit = u64::try_from(std::mem::size_of::<LayoutRef>()).unwrap();
        let pool = LiveMemoryPool::new(2 * unit).unwrap();
        let strategy = BoundedIngestLayout::new(
            Arc::new(FlatLayoutStrategy::default()),
            0,
            pool.reserve(0).unwrap(),
        );
        let mut children = Vec::new();
        strategy.grow_references(&mut children, 1).unwrap();
        children.push(empty_child());
        assert_eq!(children.capacity(), 1);
        let error = strategy.grow_references(&mut children, 2).unwrap_err();
        assert!(error.to_string().contains("memory reservation denied"));
        assert_eq!(children.capacity(), 1, "denial must precede Vec growth");
        assert_eq!(pool.snapshot().reserved_bytes, unit);
        assert_eq!(pool.snapshot().denied_reservations, 1);
        drop(children);
        assert_eq!(
            pool.snapshot().reserved_bytes,
            unit,
            "writer retains footer credits"
        );
        drop(strategy);
        assert_eq!(pool.snapshot().reserved_bytes, 0);
    }

    #[test]
    fn reference_growth_has_no_fixed_batch_ceiling_and_retains_footer_credits() {
        let unit = u64::try_from(std::mem::size_of::<LayoutRef>()).unwrap();
        let capacity = 65_537;
        let pool = LiveMemoryPool::new((u64::try_from(capacity).unwrap() + 1) * unit).unwrap();
        let strategy = BoundedIngestLayout::new(
            Arc::new(FlatLayoutStrategy::default()),
            0,
            pool.reserve(0).unwrap(),
        );
        let mut children = Vec::new();
        strategy.grow_references(&mut children, 1).unwrap();
        let child = empty_child();
        children.push(Arc::clone(&child));
        strategy.grow_references(&mut children, capacity).unwrap();
        children.resize(capacity, child);
        assert_eq!(children.capacity(), capacity);
        assert_eq!(
            pool.snapshot().peak_reserved_bytes,
            (u64::try_from(capacity).unwrap() + 1) * unit
        );
        assert_eq!(
            pool.snapshot().reserved_bytes,
            u64::try_from(capacity).unwrap() * unit
        );
        let root = ChunkedLayout::new(
            0,
            DType::Bool(Nullability::NonNullable),
            strategy.reserved_children(children),
        )
        .into_layout();
        assert_eq!(root.nslots(), capacity);
        let retained_root = Arc::clone(&root);
        drop(strategy);
        assert_eq!(
            pool.snapshot().reserved_bytes,
            u64::try_from(capacity).unwrap() * unit
        );
        drop(root);
        assert!(pool.snapshot().reserved_bytes > 0);
        drop(retained_root);
        assert_eq!(pool.snapshot().reserved_bytes, 0);
    }

    #[test]
    fn cloned_layout_children_share_references_and_credits_after_strategy_drop() {
        let unit = u64::try_from(std::mem::size_of::<LayoutRef>()).unwrap();
        let pool = LiveMemoryPool::new(unit).unwrap();
        let strategy = BoundedIngestLayout::new(
            Arc::new(FlatLayoutStrategy::default()),
            0,
            pool.reserve(0).unwrap(),
        );
        let mut children = Vec::new();
        strategy.grow_references(&mut children, 1).unwrap();
        let child = empty_child();
        children.push(Arc::clone(&child));
        let children = strategy.reserved_children(children);
        let cloned = children.as_ref().to_arc();
        assert_eq!(Arc::strong_count(&child), 2, "to_arc must not copy the Vec");
        drop(strategy);
        assert_eq!(pool.snapshot().reserved_bytes, unit);
        drop(children);
        assert_eq!(pool.snapshot().reserved_bytes, unit);
        assert_eq!(cloned.nchildren(), 1);
        assert_eq!(cloned.child_row_count(0), 0);
        assert!(Arc::ptr_eq(
            &child,
            &cloned.child(0, child.dtype()).unwrap()
        ));
        drop(cloned);
        assert_eq!(Arc::strong_count(&child), 1);
        assert_eq!(pool.snapshot().reserved_bytes, 0);
    }
}
