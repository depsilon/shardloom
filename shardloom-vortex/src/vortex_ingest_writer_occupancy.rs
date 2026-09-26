//! Test-only occupancy observer. Poll regions occupy a driver; they are not CPU
//! time and may include blocking I/O or preemption. Async readiness is unknown.

use std::{
    collections::HashMap,
    future::{Future, poll_fn},
    sync::{Arc, Mutex},
    thread::{self, ThreadId},
    time::Instant,
};

use futures::{Stream, future::BoxFuture};
use serde_json::{Value, json};
use vortex::{
    error::VortexResult,
    io::runtime::{
        AbortHandle, AbortHandleRef, BlockingRuntime, Executor, Handle, Task,
        current::CurrentThreadRuntime,
    },
    layout::{
        LayoutRef, LayoutStrategy, LayoutWriterContext,
        segments::SegmentSinkRef,
        sequence::{SendableSequentialStream, SequencePointer, SequentialStreamAdapter},
    },
    session::VortexSession,
};

const LANES: usize = 2;

#[derive(Default)]
struct Child {
    started: u64,
    last_full: u64,
    occupied: [u64; LANES + 1],
    cpu_queue_empty: [u64; LANES + 1],
    maximum_cpu_queue: usize,
}

#[derive(Default)]
struct State {
    last: u64,
    threads: HashMap<ThreadId, usize>,
    queued_cpu: usize,
    blocking_io: usize,
    blocking_calls: u64,
    cpu_calls: u64,
    polls: u64,
    child: Option<Child>,
    children: Vec<Value>,
    writer: Option<Child>,
    writer_report: Option<Value>,
    input: Option<Child>,
    input_polls: Vec<Value>,
}

impl State {
    fn advance(&mut self, now: u64) {
        let elapsed = now.checked_sub(self.last).unwrap();
        for child in [&mut self.child, &mut self.writer, &mut self.input]
            .into_iter()
            .flatten()
        {
            let occupied = self.threads.len();
            assert!(occupied <= LANES, "observer found an extra provider driver");
            child.occupied[occupied] += elapsed;
            if self.queued_cpu == 0 {
                child.cpu_queue_empty[occupied] += elapsed;
            }
            if occupied == LANES {
                child.last_full = now;
            }
            child.maximum_cpu_queue = child.maximum_cpu_queue.max(self.queued_cpu);
        }
        self.last = now;
    }

    fn enter(&mut self, id: ThreadId) {
        *self.threads.entry(id).or_default() += 1;
    }

    fn leave(&mut self, id: ThreadId) {
        let depth = self.threads.get_mut(&id).unwrap();
        *depth -= 1;
        if *depth == 0 {
            self.threads.remove(&id);
        }
    }
}

pub(super) struct Observation {
    origin: Instant,
    state: Mutex<State>,
}

impl Observation {
    pub(super) fn new() -> Arc<Self> {
        Arc::new(Self {
            origin: Instant::now(),
            state: Mutex::new(State::default()),
        })
    }

    fn change<R>(&self, change: impl FnOnce(&mut State) -> R) -> R {
        let mut state = self.state.lock().unwrap();
        // Read the clock inside the lock so event order is monotonic.
        state.advance(u64::try_from(self.origin.elapsed().as_nanos()).unwrap());
        change(&mut state)
    }

    fn poll(self: &Arc<Self>) -> PollGuard {
        let id = thread::current().id();
        self.change(|state| {
            state.polls += 1;
            state.enter(id);
        });
        PollGuard(Arc::clone(self), id)
    }

    fn child(self: &Arc<Self>) -> ChildGuard {
        self.change(|state| {
            assert!(state.child.is_none(), "screen must remain sequential");
            assert!(
                state.children.len() < 3,
                "bounded fixture exceeded three batches"
            );
            state.child = Some(Child {
                started: state.last,
                last_full: state.last,
                ..Child::default()
            });
        });
        ChildGuard(Arc::clone(self))
    }

    pub(super) fn writer(self: &Arc<Self>) -> WriterGuard {
        self.change(|state| {
            assert!(state.writer.is_none() && state.writer_report.is_none());
            state.writer = Some(Child {
                started: state.last,
                last_full: state.last,
                ..Child::default()
            });
        });
        WriterGuard(Arc::clone(self))
    }

    fn input_poll(self: &Arc<Self>) -> InputGuard {
        self.change(|state| {
            assert!(state.input.is_none());
            state.input = Some(Child {
                started: state.last,
                last_full: state.last,
                ..Child::default()
            });
        });
        InputGuard(Arc::clone(self))
    }

    pub(super) fn snapshot(&self) -> Value {
        self.change(|state| {
            assert!(state.child.is_none());
            assert!(state.writer.is_none() && state.input.is_none());
            assert!(state.threads.is_empty());
            assert_eq!(state.queued_cpu, 0);
            assert_eq!(state.blocking_io, 0);
            json!({"provider_lanes":LANES,"poll_calls":state.polls,
                "cpu_calls":state.cpu_calls,"blocking_io_calls":state.blocking_calls,
                "children":state.children,"complete_writer":state.writer_report,
                "input_poll_intervals":state.input_polls,
                "scope":"occupied_driver_wall_regions_not_CPU;async_runnable_queue_unknown;blocking_IO_pool_excluded;nesting_counts_each_thread_once;capacity_opportunity_not_speedup_bound"})
        })
    }
}

struct PollGuard(Arc<Observation>, ThreadId);
impl Drop for PollGuard {
    fn drop(&mut self) {
        self.0.change(|state| state.leave(self.1));
    }
}

struct ChildGuard(Arc<Observation>);
fn interval_report(child: Child, end: u64) -> Value {
    let unoccupied = child.occupied[0] * 2 + child.occupied[1];
    let empty_unoccupied = child.cpu_queue_empty[0] * 2 + child.cpu_queue_empty[1];
    assert_eq!(child.occupied.iter().sum::<u64>(), end - child.started);
    json!({"wall_nanos":end-child.started,
        "terminal_underoccupied_nanos":end-child.last_full,
        "occupied_driver_wall_histogram_nanos":child.occupied,
        "cpu_queue_empty_histogram_nanos":child.cpu_queue_empty,
        "unoccupied_driver_nanos":unoccupied,
        "cpu_queue_empty_unoccupied_driver_nanos":empty_unoccupied,
        "maximum_cpu_queue":child.maximum_cpu_queue})
}
impl Drop for ChildGuard {
    fn drop(&mut self) {
        self.0.change(|state| {
            let child = state.child.take().unwrap();
            state.children.push(interval_report(child, state.last));
        });
    }
}

pub(super) struct WriterGuard(Arc<Observation>);
impl Drop for WriterGuard {
    fn drop(&mut self) {
        self.0.change(|state| {
            state.writer_report = Some(interval_report(state.writer.take().unwrap(), state.last));
        });
    }
}
struct InputGuard(Arc<Observation>);
impl Drop for InputGuard {
    fn drop(&mut self) {
        self.0.change(|state| {
            let mut report = interval_report(state.input.take().unwrap(), state.last);
            report["completed_children_before_poll"] = json!(state.children.len());
            state.input_polls.push(report);
        });
    }
}

struct CpuTicket {
    observation: Arc<Observation>,
    running: Option<ThreadId>,
}
impl CpuTicket {
    fn new(observation: &Arc<Observation>) -> Self {
        observation.change(|state| {
            state.queued_cpu += 1;
            state.cpu_calls += 1;
        });
        Self {
            observation: Arc::clone(observation),
            running: None,
        }
    }
    fn start(&mut self) {
        let id = thread::current().id();
        self.observation.change(|state| {
            state.queued_cpu -= 1;
            state.enter(id);
        });
        self.running = Some(id);
    }
}
impl Drop for CpuTicket {
    fn drop(&mut self) {
        self.observation.change(|state| {
            if let Some(id) = self.running {
                state.leave(id);
            } else {
                state.queued_cpu -= 1;
            }
        });
    }
}

struct BlockingGuard(Arc<Observation>);
impl Drop for BlockingGuard {
    fn drop(&mut self) {
        self.0.change(|state| state.blocking_io -= 1);
    }
}

struct DelegatedAbort(Mutex<Option<Task<()>>>);
impl AbortHandle for DelegatedAbort {
    fn abort(self: Box<Self>) {
        drop(self.0.lock().unwrap().take());
    }
}
impl Drop for DelegatedAbort {
    fn drop(&mut self) {
        if let Some(task) = self.0.get_mut().unwrap().take() {
            task.detach();
        }
    }
}
fn delegated(task: Task<()>) -> AbortHandleRef {
    Box::new(DelegatedAbort(Mutex::new(Some(task))))
}

pub(super) struct ObservedExecutor {
    base: Handle,
    observation: Arc<Observation>,
}
impl ObservedExecutor {
    pub(super) fn new(base: Handle, observation: &Arc<Observation>) -> Arc<Self> {
        Arc::new(Self {
            base,
            observation: Arc::clone(observation),
        })
    }
    pub(super) fn handle(self: &Arc<Self>) -> Handle {
        let executor: Arc<dyn Executor> = self.clone();
        Handle::new(Arc::downgrade(&executor))
    }
}
impl Executor for ObservedExecutor {
    fn spawn(&self, mut future: BoxFuture<'static, ()>) -> AbortHandleRef {
        let observation = Arc::clone(&self.observation);
        delegated(self.base.spawn(poll_fn(move |cx| {
            let _guard = observation.poll();
            future.as_mut().poll(cx)
        })))
    }
    fn spawn_io(&self, future: BoxFuture<'static, ()>) -> AbortHandleRef {
        // CurrentThreadRuntime's default spawn_io uses the same executor.
        self.spawn(future)
    }
    fn spawn_cpu(&self, task: Box<dyn FnOnce() + Send + 'static>) -> AbortHandleRef {
        let mut ticket = CpuTicket::new(&self.observation);
        delegated(self.base.spawn_cpu(move || {
            ticket.start();
            task();
            drop(ticket);
        }))
    }
    fn spawn_blocking_io(&self, task: Box<dyn FnOnce() + Send + 'static>) -> AbortHandleRef {
        let observation = Arc::clone(&self.observation);
        delegated(self.base.spawn_blocking(move || {
            observation.change(|state| {
                state.blocking_calls += 1;
                state.blocking_io += 1;
            });
            let _guard = BlockingGuard(observation);
            task();
        }))
    }
}

pub(super) struct ObservedRuntime<'a> {
    pub(super) base: &'a CurrentThreadRuntime,
    pub(super) executor: Arc<ObservedExecutor>,
    pub(super) observation: Arc<Observation>,
}
impl BlockingRuntime for ObservedRuntime<'_> {
    type BlockingIterator<'a, R: 'a> =
        <CurrentThreadRuntime as BlockingRuntime>::BlockingIterator<'a, R>;
    fn handle(&self) -> Handle {
        self.executor.handle()
    }
    fn block_on<Fut: Future<Output = R>, R>(&self, future: Fut) -> R {
        let mut future = Box::pin(future);
        self.base.block_on(poll_fn(|cx| {
            let _guard = self.observation.poll();
            future.as_mut().poll(cx)
        }))
    }
    fn block_on_stream<'a, S, R>(&self, stream: S) -> Self::BlockingIterator<'a, R>
    where
        S: Stream<Item = R> + Send + 'a,
        R: Send + 'a,
    {
        self.base.block_on_stream(stream)
    }
}

pub(super) struct ObservedChild {
    pub(super) child: Arc<dyn LayoutStrategy>,
    pub(super) observation: Arc<Observation>,
}
impl LayoutStrategy for ObservedChild {
    fn write_stream<'a, 'b, 'future>(
        &'a self,
        ctx: LayoutWriterContext,
        sink: SegmentSinkRef,
        input: SendableSequentialStream,
        eof: SequencePointer,
        session: &'b VortexSession,
    ) -> BoxFuture<'future, VortexResult<LayoutRef>>
    where
        'a: 'future,
        'b: 'future,
        Self: 'future,
    {
        Box::pin(async move {
            let _guard = self.observation.child();
            self.child
                .write_stream(ctx, sink, input, eof, session)
                .await
        })
    }
}

pub(super) struct ObservedInput {
    pub(super) child: Arc<dyn LayoutStrategy>,
    pub(super) observation: Arc<Observation>,
}
impl LayoutStrategy for ObservedInput {
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
        let dtype = input.dtype().clone();
        let observation = Arc::clone(&self.observation);
        let stream = futures::stream::poll_fn(move |cx| {
            let _guard = observation.input_poll();
            input.as_mut().poll_next(cx)
        });
        self.child.write_stream(
            ctx,
            sink,
            Box::pin(SequentialStreamAdapter::new(dtype, stream)),
            eof,
            session,
        )
    }
}

#[test]
fn complete_writer_covers_input_and_child_intervals_without_double_counting() {
    let mut state = State {
        writer: Some(Child::default()),
        ..State::default()
    };
    state.advance(10);
    let id = thread::current().id();
    state.enter(id);
    state.input = Some(Child {
        started: 10,
        last_full: 10,
        ..Child::default()
    });
    state.advance(30);
    let input = interval_report(state.input.take().unwrap(), 30);
    state.child = Some(Child {
        started: 30,
        last_full: 30,
        ..Child::default()
    });
    state.advance(60);
    let child = interval_report(state.child.take().unwrap(), 60);
    state.leave(id);
    state.advance(70);
    let writer = interval_report(state.writer.take().unwrap(), 70);
    assert_eq!(
        input["occupied_driver_wall_histogram_nanos"],
        json!([0, 20, 0])
    );
    assert_eq!(
        child["occupied_driver_wall_histogram_nanos"],
        json!([0, 30, 0])
    );
    assert_eq!(
        writer["occupied_driver_wall_histogram_nanos"],
        json!([20, 50, 0])
    );
    assert_eq!(writer["unoccupied_driver_nanos"], 90);
}

#[test]
fn occupancy_counts_nested_driver_once_and_preserves_separate_queue_subset() {
    let mut state = State {
        child: Some(Child::default()),
        ..State::default()
    };
    let id = thread::current().id();
    state.advance(10);
    state.enter(id);
    state.enter(id);
    state.advance(30);
    state.queued_cpu = 1;
    state.leave(id);
    state.advance(40);
    state.leave(id);
    state.advance(50);
    let child = state.child.unwrap();
    assert_eq!(child.occupied, [20, 30, 0]);
    assert_eq!(child.cpu_queue_empty, [10, 20, 0]);
}

#[test]
fn cancelled_unstarted_cpu_ticket_releases_queue_and_dropped_abort_detaches() {
    let runtime = CurrentThreadRuntime::new();
    let observation = Observation::new();
    let executor = ObservedExecutor::new(runtime.handle(), &observation);
    let handle = executor.handle();
    let cancelled = handle.spawn_cpu(|| panic!("cancelled before runtime starts"));
    drop(cancelled);
    let (sent, received) = futures::channel::oneshot::channel();
    drop(executor.spawn(Box::pin(async move {
        sent.send(()).unwrap();
    })));
    runtime.block_on(received).unwrap();
    assert_eq!(observation.snapshot()["cpu_calls"], 1);
}
