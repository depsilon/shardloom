//! Bounded source-batch writer subtrees with locally scoped EOF ordering.
//!
//! Unlike the upstream Chunked strategy's global-EOF children, each child owns
//! only descendants of one input sequence. Its EOF therefore precedes the next
//! source item. The wrapped strategy still drives all of its children together.

use std::{
    collections::VecDeque,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
};

use futures::{StreamExt as _, future::BoxFuture, stream};
use shardloom_exec::live_memory::{LiveMemoryPool, MemoryLease};
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
    prefetch_input: bool,
    max_in_flight: usize,
    memory: Option<LiveMemoryPool>,
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
            prefetch_input: false,
            max_in_flight: 1,
            memory: None,
        }
    }

    /// Retain one next input while writing a child; admitted by the stream owner.
    pub(crate) fn with_input_prefetch(mut self, enabled: bool) -> Self {
        self.prefetch_input = enabled;
        self
    }

    /// Allow up to `max_in_flight` chunk write streams governed dynamically by
    /// the shared [`LiveMemoryPool`] headroom controller.
    #[cfg_attr(not(feature = "vortex-write"), allow(dead_code))]
    pub(crate) fn with_max_in_flight(
        mut self,
        max_in_flight: usize,
        memory: LiveMemoryPool,
    ) -> Self {
        self.max_in_flight = max_in_flight.max(1);
        self.memory = Some(memory);
        self
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

    async fn write_stream_serial(
        &self,
        ctx: &LayoutWriterContext,
        sink: &SegmentSinkRef,
        input: &mut SendableSequentialStream,
        session: &VortexSession,
        children: &mut Vec<LayoutRef>,
    ) -> VortexResult<u64> {
        let dtype = input.dtype().clone();
        let prefetch = self.prefetch_input;
        let mut rows = 0_u64;
        let mut next = input.next().await;
        while let Some(item) = next.take() {
            let (sequence, array) = item?;
            if array.is_empty() {
                drop(array);
                drop(sequence);
                next = input.next().await;
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
                self.grow_references(children, capacity)?;
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
            let child = self.child.write_stream(
                ctx.clone(),
                Arc::clone(sink),
                SequentialStreamAdapter::new(
                    dtype.clone(),
                    stream::iter([Ok((start.downgrade(), array))]),
                )
                .sendable(),
                child_eof,
                session,
            );
            let child = if prefetch {
                // Poll the child first, then prepare exactly one next input.
                // Keep one serial statistics accumulator and one live child.
                // Child failure drops the pending pull; an input error remains
                // ordered after completion of the current child.
                let (child, following) = futures::future::try_join(child, async {
                    Ok::<_, vortex::error::VortexError>(input.next().await)
                })
                .await?;
                next = following;
                child
            } else {
                child.await?
            };
            children.push(child);
            if !prefetch {
                next = input.next().await;
            }
        }
        Ok(rows)
    }

    async fn write_stream_concurrent<'a, 'b, 'future>(
        &'a self,
        ctx: &LayoutWriterContext,
        sink: &SegmentSinkRef,
        input: &mut SendableSequentialStream,
        session: &'b VortexSession,
        children: &mut Vec<LayoutRef>,
    ) -> VortexResult<u64>
    where
        'a: 'future,
        'b: 'future,
    {
        enum InFlightSlot<'future> {
            Pending(BoxFuture<'future, VortexResult<LayoutRef>>),
            Ready(VortexResult<LayoutRef>),
        }

        let dtype = input.dtype().clone();
        let mut in_flight: VecDeque<InFlightSlot<'future>> = VecDeque::new();
        let mut rows = 0_u64;
        let mut input_exhausted = false;
        let mut input_error: Option<vortex::error::VortexError> = None;
        while !input_exhausted || !in_flight.is_empty() || input_error.is_some() {
            while !input_exhausted {
                let limit = crate::ingest_cpu_lanes::governed_layout_in_flight(
                    self.max_in_flight,
                    self.memory.as_ref(),
                );
                if in_flight.len() >= limit {
                    break;
                }
                let Some(item) = input.next().await else {
                    input_exhausted = true;
                    break;
                };
                let (sequence, array) = match item {
                    Ok(pair) => pair,
                    Err(error) => {
                        input_exhausted = true;
                        input_error = Some(error);
                        break;
                    }
                };
                if array.is_empty() {
                    drop(array);
                    drop(sequence);
                    continue;
                }
                let needed = children
                    .len()
                    .checked_add(in_flight.len())
                    .and_then(|count| count.checked_add(1))
                    .ok_or_else(|| {
                        vortex_err!("bounded ingest layout-reference capacity overflow")
                    })?;
                if needed > children.capacity() {
                    let capacity = children
                        .capacity()
                        .checked_mul(2)
                        .ok_or_else(|| {
                            vortex_err!("bounded ingest layout-reference capacity overflow")
                        })?
                        .max(needed);
                    self.grow_references(children, capacity)?;
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
                let child = self.child.write_stream(
                    ctx.clone(),
                    Arc::clone(sink),
                    SequentialStreamAdapter::new(
                        dtype.clone(),
                        stream::iter([Ok((start.downgrade(), array))]),
                    )
                    .sendable(),
                    child_eof,
                    session,
                );
                in_flight.push_back(InFlightSlot::Pending(child));
            }
            let next_ready = futures::future::poll_fn(|cx| {
                for slot in &mut in_flight {
                    if let InFlightSlot::Pending(fut) = slot
                        && let std::task::Poll::Ready(result) = fut.as_mut().poll(cx)
                    {
                        *slot = InFlightSlot::Ready(result);
                    }
                }
                if matches!(in_flight.front(), Some(InFlightSlot::Ready(_)))
                    && let Some(InFlightSlot::Ready(result)) = in_flight.pop_front()
                {
                    return std::task::Poll::Ready(Some(result));
                }
                if in_flight.is_empty() {
                    return std::task::Poll::Ready(None);
                }
                std::task::Poll::Pending
            })
            .await;
            if let Some(child) = next_ready {
                children.push(child?);
            } else if let Some(error) = input_error.take() {
                return Err(error);
            }
        }
        Ok(rows)
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
            let rows = if self.max_in_flight <= 1 {
                self.write_stream_serial(&ctx, &sink, &mut input, session, &mut children)
                    .await?
            } else {
                self.write_stream_concurrent(&ctx, &sink, &mut input, session, &mut children)
                    .await?
            };
            // Keep the enclosing EOF alive until all locally scoped children
            // finish. Child strategies may emit metadata at their own EOF.
            drop(eof);
            Ok(
                ChunkedLayout::try_new(rows, dtype, self.reserved_children(children), session)?
                    .into_layout(),
            )
        })
    }
}

#[cfg(test)]
#[path = "ingest_input_prefetch_tests.rs"]
mod input_prefetch_tests;

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
