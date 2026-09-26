//! Deterministic lifecycle tests for the private one-input lookahead prototype.
//!
//! The child returns layout metadata without writing segments. Source arrays do
//! use the retained native allocator, so buffer ownership and denial are real.

use std::{
    collections::VecDeque,
    pin::Pin,
    sync::{Arc, Mutex},
    task::{Context, Poll},
};

use futures::{Stream, StreamExt as _, future::BoxFuture, task::noop_waker_ref};
use shardloom_exec::live_memory::LiveMemoryPool;
use vortex::{
    VortexSessionDefault as _,
    array::{
        ArrayContext, ArrayRef, IntoArray as _,
        arrays::{Primitive, PrimitiveArray},
        dtype::{DType, Nullability, PType},
        memory::HostAllocator as _,
        validity::Validity,
    },
    buffer::{Alignment, Buffer, ByteBuffer},
    error::{VortexResult, vortex_err},
    layout::{
        LayoutRef, LayoutStrategy, LayoutWriterContext,
        layouts::flat::FlatLayout,
        segments::{SegmentId, SegmentSink, SegmentSinkRef},
        sequence::{
            SendableSequentialStream, SequenceId, SequencePointer, SequentialStreamAdapter,
            SequentialStreamExt as _,
        },
    },
    session::{VortexSession, registry::ReadContext},
};

use crate::{
    owned_buffers::ReservedHostAllocator, vortex_ingest::bounded_ingest_layout::BoundedIngestLayout,
};

#[derive(Clone, Copy, Debug)]
enum Outcome {
    Pending,
    Complete,
    Fail,
}

#[derive(Debug, PartialEq, Eq)]
enum Event {
    Source(usize),
    Eof,
    ChildStart(usize),
    ChildComplete(usize),
    ChildDrop(usize),
}

#[derive(Default)]
struct Trace {
    events: Vec<Event>,
    rows: Vec<u64>,
    outcomes: Vec<Outcome>,
    pulls: usize,
    eof_polls: usize,
    children_started: usize,
    live_children: usize,
    max_children: usize,
    release_source: bool,
}

type SharedTrace = Arc<Mutex<Trace>>;

enum SourceStep {
    Batch(Vec<u64>),
    PendingBatch(Vec<u64>),
    Error,
}

struct OwnedSource {
    steps: VecDeque<SourceStep>,
    pending: Option<VortexResult<ArrayRef>>,
    waiting: bool,
    ended: bool,
    pointer: SequencePointer,
    allocator: ReservedHostAllocator,
    trace: SharedTrace,
}

fn owned_array(allocator: &ReservedHostAllocator, values: &[u64]) -> VortexResult<ArrayRef> {
    let mut buffer = allocator.allocate(std::mem::size_of_val(values), Alignment::new(8))?;
    for (bytes, value) in buffer.as_mut_slice().chunks_exact_mut(8).zip(values) {
        bytes.copy_from_slice(&value.to_ne_bytes());
    }
    Ok(PrimitiveArray::new(
        Buffer::<u64>::from_byte_buffer(buffer.freeze()),
        Validity::NonNullable,
    )
    .into_array())
}

impl Stream for OwnedSource {
    type Item = VortexResult<(SequenceId, ArrayRef)>;

    fn poll_next(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        assert!(!self.ended, "source polled again after EOF");
        if self.pending.is_none() {
            let Some(step) = self.steps.pop_front() else {
                self.ended = true;
                let mut trace = self.trace.lock().unwrap();
                trace.eof_polls += 1;
                trace.events.push(Event::Eof);
                return Poll::Ready(None);
            };
            {
                let mut trace = self.trace.lock().unwrap();
                let index = trace.pulls;
                trace.pulls += 1;
                trace.events.push(Event::Source(index));
            }
            self.waiting = matches!(&step, SourceStep::PendingBatch(_));
            self.pending = Some(match step {
                SourceStep::Batch(values) | SourceStep::PendingBatch(values) => {
                    owned_array(&self.allocator, &values)
                }
                SourceStep::Error => Err(vortex_err!("next input failure")),
            });
        }
        if self.waiting && !self.trace.lock().unwrap().release_source {
            return Poll::Pending;
        }
        let item = self.pending.take().unwrap();
        Poll::Ready(Some(item.map(|array| (self.pointer.advance(), array))))
    }
}

struct NoWriteSink;

impl SegmentSink for NoWriteSink {
    fn write<'a, 'future>(
        &'a self,
        _sequence: SequenceId,
        _buffers: Vec<ByteBuffer>,
    ) -> BoxFuture<'future, VortexResult<SegmentId>>
    where
        'a: 'future,
        Self: 'future,
    {
        Box::pin(async {
            Err(vortex_err!(
                "prefetch lifecycle fixture forbids segment writes"
            ))
        })
    }
}

struct ActiveChild {
    index: usize,
    trace: SharedTrace,
}

impl Drop for ActiveChild {
    fn drop(&mut self) {
        let mut trace = self.trace.lock().unwrap();
        trace.live_children -= 1;
        trace.events.push(Event::ChildDrop(self.index));
    }
}

struct ControlledChild(SharedTrace);

impl LayoutStrategy for ControlledChild {
    fn write_stream<'a, 'b, 'future>(
        &'a self,
        _ctx: LayoutWriterContext,
        _sink: SegmentSinkRef,
        mut input: SendableSequentialStream,
        eof: SequencePointer,
        _session: &'b VortexSession,
    ) -> BoxFuture<'future, VortexResult<LayoutRef>>
    where
        'a: 'future,
        'b: 'future,
        Self: 'future,
    {
        Box::pin(async move {
            let index = {
                let mut trace = self.0.lock().unwrap();
                let index = trace.children_started;
                trace.children_started += 1;
                trace.live_children += 1;
                trace.max_children = trace.max_children.max(trace.live_children);
                trace.events.push(Event::ChildStart(index));
                index
            };
            let _active = ActiveChild {
                index,
                trace: Arc::clone(&self.0),
            };
            let (sequence, array) = input.next().await.unwrap()?;
            assert!(input.next().await.is_none());
            self.0
                .lock()
                .unwrap()
                .rows
                .extend_from_slice(array.as_opt::<Primitive>().unwrap().as_slice::<u64>());
            // Tests explicitly repoll after changing the gate. No runtime,
            // sleeps, worker threads, or wake timing participates in the result.
            futures::future::poll_fn(|_cx| match self.0.lock().unwrap().outcomes[index] {
                Outcome::Pending => Poll::Pending,
                Outcome::Complete => Poll::Ready(Ok(())),
                Outcome::Fail => Poll::Ready(Err(vortex_err!("child failure"))),
            })
            .await?;
            self.0
                .lock()
                .unwrap()
                .events
                .push(Event::ChildComplete(index));
            let layout = FlatLayout::new(
                u64::try_from(array.len()).unwrap(),
                array.dtype().clone(),
                SegmentId::try_from(index).unwrap(),
                ReadContext::new([]),
            )
            .into_layout();
            // Keep both the input buffer and its sequence alive across Pending.
            drop(array);
            drop(sequence);
            drop(eof);
            Ok(layout)
        })
    }
}

fn trace(outcomes: Vec<Outcome>) -> SharedTrace {
    Arc::new(Mutex::new(Trace {
        outcomes,
        ..Trace::default()
    }))
}

fn strategy(memory: &LiveMemoryPool, trace: &SharedTrace, prefetch: bool) -> BoundedIngestLayout {
    BoundedIngestLayout::new(
        Arc::new(ControlledChild(Arc::clone(trace))),
        0,
        memory.reserve(0).unwrap(),
    )
    .with_input_prefetch(prefetch)
}

fn write<'a>(
    strategy: &'a BoundedIngestLayout,
    session: &'a VortexSession,
    memory: &LiveMemoryPool,
    trace: &SharedTrace,
    steps: Vec<SourceStep>,
) -> BoxFuture<'a, VortexResult<LayoutRef>> {
    let (pointer, eof) = SequenceId::root().split();
    let source = OwnedSource {
        steps: steps.into(),
        pending: None,
        waiting: false,
        ended: false,
        pointer,
        allocator: ReservedHostAllocator::new(memory.clone()),
        trace: Arc::clone(trace),
    };
    strategy.write_stream(
        LayoutWriterContext::new(ArrayContext::new(Vec::new())),
        Arc::new(NoWriteSink),
        SequentialStreamAdapter::new(
            DType::Primitive(PType::U64, Nullability::NonNullable),
            source,
        )
        .sendable(),
        eof,
        session,
    )
}

fn poll_write(
    future: &mut BoxFuture<'_, VortexResult<LayoutRef>>,
) -> Poll<VortexResult<LayoutRef>> {
    future
        .as_mut()
        .poll(&mut Context::from_waker(noop_waker_ref()))
}

fn ready(future: &mut BoxFuture<'_, VortexResult<LayoutRef>>) -> VortexResult<LayoutRef> {
    match poll_write(future) {
        Poll::Ready(result) => result,
        Poll::Pending => panic!("writer unexpectedly remained pending"),
    }
}

#[test]
fn child_is_polled_first_and_one_lookahead_preserves_rows_and_empty_batches() {
    let memory = LiveMemoryPool::new(4096).unwrap();
    let trace = trace(vec![Outcome::Pending; 3]);
    let strategy = strategy(&memory, &trace, true);
    let session = VortexSession::default();
    let mut future = write(
        &strategy,
        &session,
        &memory,
        &trace,
        vec![
            SourceStep::Batch(vec![10, 11]),
            SourceStep::Batch(vec![]),
            SourceStep::Batch(vec![20, 21, 22]),
            SourceStep::Batch(vec![30]),
        ],
    );
    assert!(poll_write(&mut future).is_pending());
    assert!(poll_write(&mut future).is_pending());
    {
        let mut state = trace.lock().unwrap();
        assert_eq!(
            state.events,
            [Event::Source(0), Event::ChildStart(0), Event::Source(1)]
        );
        assert_eq!(state.pulls, 2);
        assert_eq!(state.children_started, 1);
        state.outcomes[0] = Outcome::Complete;
    }
    assert!(poll_write(&mut future).is_pending());
    assert!(poll_write(&mut future).is_pending());
    {
        let mut state = trace.lock().unwrap();
        assert_eq!(state.pulls, 4);
        assert_eq!(state.children_started, 2);
        assert_eq!(state.live_children, 1);
        state.outcomes[1] = Outcome::Complete;
    }
    assert!(poll_write(&mut future).is_pending());
    assert!(poll_write(&mut future).is_pending());
    {
        let mut state = trace.lock().unwrap();
        assert_eq!(state.pulls, 4);
        assert_eq!(state.eof_polls, 1);
        assert_eq!(state.children_started, 3);
        state.outcomes[2] = Outcome::Complete;
    }
    let root = ready(&mut future).unwrap();
    assert_eq!(root.row_count(), 6);
    assert_eq!(root.nchildren(), 3);
    assert_eq!(
        root.children()
            .unwrap()
            .iter()
            .map(|child| child.row_count())
            .collect::<Vec<_>>(),
        [2, 3, 1]
    );
    {
        let state = trace.lock().unwrap();
        assert_eq!(state.rows, [10, 11, 20, 21, 22, 30]);
        assert_eq!(state.max_children, 1);
        assert_eq!(state.live_children, 0);
        assert_eq!(state.eof_polls, 1);
    }
    drop(future);
    drop(strategy);
    assert!(
        memory.snapshot().reserved_bytes > 0,
        "root retains layout reference credit"
    );
    drop(root);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

#[test]
fn empty_source_and_only_empty_batches_finish_without_repolling_eof() {
    for steps in [
        vec![],
        vec![SourceStep::Batch(vec![]), SourceStep::Batch(vec![])],
    ] {
        let memory = LiveMemoryPool::new(4096).unwrap();
        let trace = trace(vec![]);
        let strategy = strategy(&memory, &trace, true);
        let session = VortexSession::default();
        let mut future = write(&strategy, &session, &memory, &trace, steps);
        let root = ready(&mut future).unwrap();
        assert_eq!(root.row_count(), 0);
        assert_eq!(root.nchildren(), 0);
        assert_eq!(trace.lock().unwrap().children_started, 0);
        assert_eq!(trace.lock().unwrap().eof_polls, 1);
        drop(future);
        drop(root);
        drop(strategy);
        assert_eq!(memory.snapshot().reserved_bytes, 0);
    }
}

#[test]
fn child_failure_releases_pending_and_completed_lookahead_buffers() {
    for pending in [false, true] {
        let memory = LiveMemoryPool::new(4096).unwrap();
        let trace = trace(vec![Outcome::Pending]);
        let strategy = strategy(&memory, &trace, true);
        let session = VortexSession::default();
        let next = if pending {
            SourceStep::PendingBatch(vec![2])
        } else {
            SourceStep::Batch(vec![2])
        };
        let mut future = write(
            &strategy,
            &session,
            &memory,
            &trace,
            vec![SourceStep::Batch(vec![1]), next],
        );
        assert!(poll_write(&mut future).is_pending());
        assert_eq!(trace.lock().unwrap().pulls, 2);
        assert!(memory.snapshot().reserved_bytes >= 2 * native_array_credit());
        trace.lock().unwrap().outcomes[0] = Outcome::Fail;
        assert!(
            ready(&mut future)
                .unwrap_err()
                .to_string()
                .contains("child failure")
        );
        assert_eq!(trace.lock().unwrap().live_children, 0);
        assert_eq!(trace.lock().unwrap().children_started, 1);
        drop(future);
        drop(strategy);
        assert_eq!(memory.snapshot().reserved_bytes, 0);
    }
}

#[test]
fn immediately_failing_child_does_not_pull_next_input() {
    let memory = LiveMemoryPool::new(4096).unwrap();
    let trace = trace(vec![Outcome::Fail]);
    let strategy = strategy(&memory, &trace, true);
    let session = VortexSession::default();
    let mut future = write(
        &strategy,
        &session,
        &memory,
        &trace,
        vec![SourceStep::Batch(vec![1]), SourceStep::Batch(vec![2])],
    );
    assert!(
        ready(&mut future)
            .unwrap_err()
            .to_string()
            .contains("child failure")
    );
    assert_eq!(trace.lock().unwrap().pulls, 1);
    assert_eq!(trace.lock().unwrap().live_children, 0);
    drop(future);
    drop(strategy);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

#[test]
fn next_input_error_waits_for_prior_child_completion() {
    let memory = LiveMemoryPool::new(4096).unwrap();
    let trace = trace(vec![Outcome::Pending]);
    let strategy = strategy(&memory, &trace, true);
    let session = VortexSession::default();
    let mut future = write(
        &strategy,
        &session,
        &memory,
        &trace,
        vec![SourceStep::Batch(vec![1]), SourceStep::Error],
    );
    assert!(poll_write(&mut future).is_pending());
    assert_eq!(trace.lock().unwrap().pulls, 2);
    assert_eq!(trace.lock().unwrap().live_children, 1);
    trace.lock().unwrap().outcomes[0] = Outcome::Complete;
    assert!(
        ready(&mut future)
            .unwrap_err()
            .to_string()
            .contains("next input failure")
    );
    {
        let state = trace.lock().unwrap();
        assert!(state.events.contains(&Event::ChildComplete(0)));
        assert_eq!(state.children_started, 1);
        assert_eq!(state.eof_polls, 0);
        assert_eq!(state.live_children, 0);
    }
    drop(future);
    drop(strategy);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

fn native_array_credit() -> u64 {
    u64::try_from(std::mem::size_of::<u64>() + *Alignment::DEFAULT_ALIGNMENT).unwrap()
}

#[test]
fn two_live_native_inputs_deny_explicitly_while_serial_control_fits() {
    let limit =
        native_array_credit() + u64::try_from(4 * std::mem::size_of::<LayoutRef>()).unwrap();
    assert!(limit < 2 * native_array_credit());
    for prefetch in [false, true] {
        let memory = LiveMemoryPool::new(limit).unwrap();
        let trace = trace(vec![Outcome::Pending, Outcome::Complete]);
        let strategy = strategy(&memory, &trace, prefetch);
        let session = VortexSession::default();
        let mut future = write(
            &strategy,
            &session,
            &memory,
            &trace,
            vec![SourceStep::Batch(vec![1]), SourceStep::Batch(vec![2])],
        );
        assert!(poll_write(&mut future).is_pending());
        assert_eq!(memory.snapshot().denied_reservations, u64::from(prefetch));
        trace.lock().unwrap().outcomes[0] = Outcome::Complete;
        let result = ready(&mut future);
        if prefetch {
            let error = result.unwrap_err();
            assert!(error.to_string().contains("memory reservation denied"));
            #[cfg(feature = "vortex-local-primitives")]
            assert!(crate::owned_buffers::is_owned_reservation_denial(&error));
            assert_eq!(trace.lock().unwrap().children_started, 1);
        } else {
            let root = result.unwrap();
            assert_eq!(root.row_count(), 2);
            assert_eq!(trace.lock().unwrap().children_started, 2);
            drop(root);
        }
        assert!(memory.snapshot().peak_reserved_bytes <= limit);
        assert_eq!(trace.lock().unwrap().live_children, 0);
        drop(future);
        drop(strategy);
        assert_eq!(memory.snapshot().reserved_bytes, 0);
    }
}

#[test]
fn skipped_empty_owners_release_before_pulling_the_next_native_input() {
    let limit = native_array_credit() + u64::try_from(4 * size_of::<LayoutRef>()).unwrap();
    for prefetch in [false, true] {
        let memory = LiveMemoryPool::new(limit).unwrap();
        let trace = trace(vec![Outcome::Complete; 2]);
        let strategy = strategy(&memory, &trace, prefetch);
        let session = VortexSession::default();
        let mut future = write(
            &strategy,
            &session,
            &memory,
            &trace,
            vec![
                SourceStep::Batch(vec![]),
                SourceStep::Batch(vec![1]),
                SourceStep::Batch(vec![]),
                SourceStep::Batch(vec![2]),
                SourceStep::Batch(vec![]),
            ],
        );
        let root = ready(&mut future).unwrap();
        assert_eq!(root.row_count(), 2);
        assert_eq!(memory.snapshot().denied_reservations, 0);
        drop(future);
        drop(root);
        drop(strategy);
        assert_eq!(memory.snapshot().reserved_bytes, 0);
    }
}

#[test]
fn dropping_pending_outer_write_releases_child_and_lookahead_ownership() {
    for pending in [false, true] {
        let memory = LiveMemoryPool::new(4096).unwrap();
        let trace = trace(vec![Outcome::Pending]);
        let strategy = strategy(&memory, &trace, true);
        let session = VortexSession::default();
        let next = if pending {
            SourceStep::PendingBatch(vec![2])
        } else {
            SourceStep::Batch(vec![2])
        };
        let mut future = write(
            &strategy,
            &session,
            &memory,
            &trace,
            vec![SourceStep::Batch(vec![1]), next],
        );
        assert!(poll_write(&mut future).is_pending());
        assert_eq!(trace.lock().unwrap().live_children, 1);
        assert!(memory.snapshot().reserved_bytes >= 2 * native_array_credit());
        drop(future);
        assert_eq!(trace.lock().unwrap().live_children, 0);
        // A live strategy retains only its layout-reference owner, no arrays.
        assert!(memory.snapshot().reserved_bytes < native_array_credit());
        drop(strategy);
        assert_eq!(memory.snapshot().reserved_bytes, 0);
    }
}
