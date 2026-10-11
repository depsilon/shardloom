use super::*;
use arrow_array::{Int64Array, RecordBatchIterator};
use arrow_schema::{DataType, Field, Schema};
use futures::channel::oneshot;
use std::{
    sync::mpsc,
    sync::{
        Condvar, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::{Duration, Instant},
};

fn schema() -> SchemaRef {
    Arc::new(Schema::new(vec![Field::new(
        "renamed_id",
        DataType::Int64,
        false,
    )]))
}

fn batch(value: i64) -> RecordBatch {
    RecordBatch::try_new(schema(), vec![Arc::new(Int64Array::from(vec![value]))]).unwrap()
}

/// A bounded rendezvous proves simultaneous ready work, without leaving a
/// permanently blocked test process when driver admission regresses.
struct Rendezvous {
    target: usize,
    entered: Mutex<usize>,
    changed: Condvar,
}

impl Rendezvous {
    fn new(target: usize) -> Arc<Self> {
        Arc::new(Self {
            target,
            entered: Mutex::new(0),
            changed: Condvar::new(),
        })
    }

    fn arrive(&self) {
        let deadline = Instant::now() + Duration::from_secs(10);
        let mut entered = self.entered.lock().unwrap();
        *entered += 1;
        assert!(*entered <= self.target);
        self.changed.notify_all();
        while *entered < self.target {
            let remaining = deadline.saturating_duration_since(Instant::now());
            assert!(
                !remaining.is_zero(),
                "ready work could not use all admitted drivers"
            );
            entered = self.changed.wait_timeout(entered, remaining).unwrap().0;
        }
    }
}

#[test]
fn ready_source_work_uses_the_grant_and_keeps_complete_order() {
    for requested in [1, 2, 3, 6, 8, 17, 64, 128, usize::MAX, 1] {
        let runtime = IngestRuntime::new(requested).unwrap();
        let grant = runtime.parallelism();
        let _drivers = runtime.start_drivers().unwrap();
        let gate = Rendezvous::new(grant);
        let factories = (0..grant)
            .map(|index| {
                let gate = Arc::clone(&gate);
                Box::new(move || {
                    gate.arrive();
                    let start = i64::try_from(index).unwrap() * 10;
                    Ok(Box::new(RecordBatchIterator::new(
                        (0..7).map(move |row| Ok(batch(start + row))),
                        schema(),
                    )) as Box<dyn RecordBatchReader + Send>)
                }) as ReaderFactory
            })
            .collect();
        let reader = IngestSourceReader::new(schema(), runtime, factories, requested);
        assert_eq!(reader.window, grant);
        let actual: Vec<_> = reader
            .map(|batch| {
                batch
                    .unwrap()
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap()
                    .value(0)
            })
            .collect();
        let expected: Vec<_> = (0..grant)
            .flat_map(|index| {
                let start = i64::try_from(index).unwrap() * 10;
                (0..7).map(move |row| start + row)
            })
            .collect();
        assert_eq!(actual, expected);
        assert_eq!(*gate.entered.lock().unwrap(), grant);
    }
}

struct ObservedReader {
    produced: usize,
    full: Option<oneshot::Sender<()>>,
    dropped: Arc<AtomicUsize>,
}

impl Iterator for ObservedReader {
    type Item = Batch;

    fn next(&mut self) -> Option<Self::Item> {
        self.produced += 1;
        if self.produced == IngestSourceReader::QUEUED_BATCHES {
            self.full.take().unwrap().send(()).unwrap();
        }
        assert!(
            self.produced <= IngestSourceReader::QUEUED_BATCHES + 1,
            "a paused consumer allowed unbounded source production"
        );
        Some(Ok(batch(i64::try_from(self.produced).unwrap())))
    }
}

impl RecordBatchReader for ObservedReader {
    fn schema(&self) -> SchemaRef {
        schema()
    }
}

impl Drop for ObservedReader {
    fn drop(&mut self) {
        self.dropped.fetch_add(1, Ordering::SeqCst);
    }
}

#[test]
fn full_source_queues_yield_every_driver_to_native_provider_work() {
    for requested in [1, 2, 3, 6, 8, 17, 64, 128, usize::MAX, 1] {
        let runtime = IngestRuntime::new(requested).unwrap();
        let grant = runtime.parallelism();
        let _drivers = runtime.start_drivers().unwrap();
        let dropped = Arc::new(AtomicUsize::new(0));
        let mut full = Vec::new();
        let mut factories = Vec::new();
        for _ in 0..grant {
            let (sender, receiver) = oneshot::channel();
            full.push(receiver);
            let dropped = Arc::clone(&dropped);
            factories.push(Box::new(move || {
                Ok(Box::new(ObservedReader {
                    produced: 0,
                    full: Some(sender),
                    dropped,
                }) as Box<dyn RecordBatchReader + Send>)
            }) as ReaderFactory);
        }
        let reader = IngestSourceReader::new(schema(), runtime.clone(), factories, requested);
        assert_eq!(reader.window, grant);
        runtime
            .runtime()
            .block_on(futures::future::try_join_all(full))
            .unwrap();
        let gate = Rendezvous::new(grant);
        let provider = (0..grant)
            .map(|_| {
                let gate = Arc::clone(&gate);
                runtime.runtime().handle().spawn_cpu(move || gate.arrive())
            })
            .collect::<Vec<_>>();
        runtime
            .runtime()
            .block_on(futures::future::join_all(provider));
        assert_eq!(*gate.entered.lock().unwrap(), grant);
        drop(reader);
        assert_eq!(
            dropped.load(Ordering::SeqCst),
            grant,
            "source readers must be dropped before drain returns"
        );
    }
}

#[test]
fn shared_source_errors_and_panics_are_terminal_and_drain_later_tasks() {
    for panic in [false, true] {
        let runtime = IngestRuntime::new(2).unwrap();
        let _drivers = runtime.start_drivers().unwrap();
        let first: ReaderFactory = Box::new(move || {
            assert!(!panic, "injected source panic");
            Ok(Box::new(RecordBatchIterator::new(
                vec![
                    Ok(batch(0)),
                    Err(ArrowError::ParseError("primary source error".into())),
                ],
                schema(),
            )))
        });
        let later: ReaderFactory = Box::new(|| {
            Ok(Box::new(RecordBatchIterator::new(
                (0..100).map(|_| Ok(batch(9))),
                schema(),
            )))
        });
        let mut reader = IngestSourceReader::new(schema(), runtime, vec![first, later], 2);
        if !panic {
            assert!(reader.next().unwrap().is_ok());
        }
        let error = reader.next().unwrap().unwrap_err().to_string();
        assert!(
            error.contains(if panic {
                "task panicked"
            } else {
                "primary source error"
            }),
            "{error}"
        );
        assert!(reader.next().is_none());
        assert!(reader.tasks.is_empty());
        assert!(reader.factories.is_empty());
    }
}

#[test]
fn empty_shared_source_never_creates_work() {
    let runtime = IngestRuntime::new(8).unwrap();
    let mut reader = IngestSourceReader::new(schema(), runtime, vec![], 8);
    assert!(reader.next().is_none());
    assert!(reader.tasks.is_empty());
}

#[test]
fn pre_cancelled_parent_skips_factories_and_is_a_single_terminal_error() {
    for requested in [1, 3] {
        let runtime = IngestRuntime::new(requested).unwrap();
        let grant = runtime.parallelism();
        let _drivers = runtime.start_drivers().unwrap();
        let parent = shardloom_exec::compute_pool::CancellationToken::default();
        parent.cancel();
        let factory_calls = Arc::new(AtomicUsize::new(0));
        let factories = (0..grant)
            .map(|_| {
                let factory_calls = Arc::clone(&factory_calls);
                Box::new(move || {
                    factory_calls.fetch_add(1, Ordering::SeqCst);
                    Ok(
                        Box::new(RecordBatchIterator::new(vec![Ok(batch(1))], schema()))
                            as Box<dyn RecordBatchReader + Send>,
                    )
                }) as ReaderFactory
            })
            .collect();
        let mut reader =
            IngestSourceReader::new_controlled(schema(), runtime, factories, requested, &parent);

        let error = reader
            .next()
            .expect("cancellation is reported once")
            .unwrap_err();
        assert!(error.to_string().contains("cancelled"), "{error}");
        assert!(reader.next().is_none(), "cancellation is terminal");
        assert_eq!(factory_calls.load(Ordering::SeqCst), 0);
        assert!(parent.is_cancelled());
    }
}

/// A bounded gate lets the test cancel while one cooperative source read is
/// still in progress, then release that read so the producer can drain.
struct ReadReleaseGate {
    state: Mutex<(bool, bool)>,
    changed: Condvar,
}

impl ReadReleaseGate {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            state: Mutex::new((false, false)),
            changed: Condvar::new(),
        })
    }

    fn wait_until_entered(&self) -> bool {
        let deadline = Instant::now() + Duration::from_secs(10);
        let mut state = self.state.lock().unwrap();
        while !state.0 {
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                return false;
            }
            state = self.changed.wait_timeout(state, remaining).unwrap().0;
        }
        true
    }

    fn enter_and_wait(&self) {
        let mut state = self.state.lock().unwrap();
        state.0 = true;
        self.changed.notify_all();
        let deadline = Instant::now() + Duration::from_secs(10);
        while !state.1 {
            let remaining = deadline.saturating_duration_since(Instant::now());
            assert!(!remaining.is_zero(), "test did not release the source read");
            state = self.changed.wait_timeout(state, remaining).unwrap().0;
        }
    }

    fn release(&self) {
        self.state.lock().unwrap().1 = true;
        self.changed.notify_all();
    }
}

struct CancellableObservedReader {
    index: usize,
    produced: usize,
    gate: Arc<ReadReleaseGate>,
    dropped: Arc<AtomicUsize>,
}

impl Iterator for CancellableObservedReader {
    type Item = Batch;

    fn next(&mut self) -> Option<Self::Item> {
        if self.produced == 8 {
            return None;
        }
        self.produced += 1;
        if self.index == 0 && self.produced == 5 {
            self.gate.enter_and_wait();
        }
        Some(Ok(batch(
            i64::try_from(self.index * 100 + self.produced).unwrap(),
        )))
    }
}

impl RecordBatchReader for CancellableObservedReader {
    fn schema(&self) -> SchemaRef {
        schema()
    }
}

impl Drop for CancellableObservedReader {
    fn drop(&mut self) {
        self.dropped.fetch_add(1, Ordering::SeqCst);
    }
}

#[test]
#[allow(clippy::too_many_lines)] // Keep the gated cancellation and owner-drain lifecycle together.
fn parent_cancel_stops_delivery_and_drains_active_source_owners_once() {
    for requested in [1, 3] {
        let runtime = IngestRuntime::new(requested).unwrap();
        let grant = runtime.parallelism();
        let parent = shardloom_exec::compute_pool::CancellationToken::default();
        let factory_gate = Rendezvous::new(grant);
        let read_gate = ReadReleaseGate::new();
        let dropped = (0..grant)
            .map(|_| Arc::new(AtomicUsize::new(0)))
            .collect::<Vec<_>>();
        let factories = dropped
            .iter()
            .enumerate()
            .map(|(index, dropped)| {
                let factory_gate = Arc::clone(&factory_gate);
                let gate = Arc::clone(&read_gate);
                let dropped = Arc::clone(dropped);
                Box::new(move || {
                    let reader = CancellableObservedReader {
                        index,
                        produced: 0,
                        gate,
                        dropped,
                    };
                    // Ensure every owner exists before the consumer starts.
                    factory_gate.arrive();
                    Ok(Box::new(reader) as Box<dyn RecordBatchReader + Send>)
                }) as ReaderFactory
            })
            .collect();
        let worker_parent = parent.clone();
        let (events_tx, events_rx) = mpsc::channel();
        let worker = std::thread::spawn(move || {
            let _drivers = runtime.start_drivers().unwrap();
            let mut reader = IngestSourceReader::new_controlled(
                schema(),
                runtime,
                factories,
                requested,
                &worker_parent,
            );
            let child = reader.cancellation.clone();
            for item in &mut reader {
                let event = match item {
                    Ok(batch) => Event::Value(
                        batch
                            .column(0)
                            .as_any()
                            .downcast_ref::<Int64Array>()
                            .unwrap()
                            .value(0),
                    ),
                    Err(error) => Event::Error(error.to_string()),
                };
                events_tx.send(event).unwrap();
            }
            events_tx.send(Event::End).unwrap();
            child
        });

        let first = events_rx
            .recv_timeout(Duration::from_secs(10))
            .expect("worker delivers a batch before cancellation");
        assert!(matches!(&first, Event::Value(1)), "{first:?}");
        let entered = read_gate.wait_until_entered();
        let mut pre_cancel_events = vec![first];
        let mut observed_another_batch = false;
        if entered && let Ok(event) = events_rx.recv_timeout(Duration::from_secs(10)) {
            observed_another_batch = matches!(event, Event::Value(_));
            pre_cancel_events.push(event);
        }
        parent.cancel();
        read_gate.release();
        let mut remaining = pre_cancel_events;
        while !matches!(remaining.last(), Some(Event::End)) {
            let event = events_rx
                .recv_timeout(Duration::from_secs(10))
                .expect("finite source worker terminates after cancellation");
            remaining.push(event);
        }
        remaining.pop(); // Remove the worker's terminal End marker.
        let child = worker.join().expect("source worker joins after drain");
        assert!(entered, "source did not enter the gated read");
        let errors = remaining
            .iter()
            .filter_map(|event| match event {
                Event::Error(error) => Some(error),
                _ => None,
            })
            .collect::<Vec<_>>();
        assert_eq!(errors.len(), 1, "cancellation must be reported once");
        assert!(errors[0].contains("cancelled"), "{}", errors[0]);
        assert!(matches!(remaining.last(), Some(Event::Error(_))));
        assert!(
            observed_another_batch,
            "a second batch must be delivered before cancellation"
        );
        assert!(child.is_cancelled());
        assert!(parent.is_cancelled());
        for drops in dropped {
            assert_eq!(drops.load(Ordering::SeqCst), 1);
        }
    }
}

#[derive(Debug)]
enum Event {
    Value(i64),
    Error(String),
    End,
}

#[test]
fn normal_exhaustion_and_early_drop_cancel_only_the_child() {
    for requested in [1, 3] {
        let runtime = IngestRuntime::new(requested).unwrap();
        let grant = runtime.parallelism();
        let _drivers = runtime.start_drivers().unwrap();
        let parent = shardloom_exec::compute_pool::CancellationToken::default();
        let factories = (0..grant)
            .map(|index| {
                Box::new(move || {
                    let start = i64::try_from(index * 10).unwrap();
                    Ok(Box::new(RecordBatchIterator::new(
                        [Ok(batch(start)), Ok(batch(start + 1))],
                        schema(),
                    )) as Box<dyn RecordBatchReader + Send>)
                }) as ReaderFactory
            })
            .collect();
        let mut exhausted =
            IngestSourceReader::new_controlled(schema(), runtime, factories, requested, &parent);
        let exhausted_child = exhausted.cancellation.clone();
        let mut exhausted_count = 0;
        for item in exhausted.by_ref() {
            let batch = item.unwrap();
            let _value = batch
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(0);
            exhausted_count += 1;
        }
        assert_eq!(exhausted_count, grant * 2);
        assert!(!exhausted_child.is_cancelled());
        drop(exhausted);
        assert!(exhausted_child.is_cancelled());
        assert!(parent.check().is_ok());

        let runtime = IngestRuntime::new(requested).unwrap();
        let _drivers = runtime.start_drivers().unwrap();
        let parent = shardloom_exec::compute_pool::CancellationToken::default();
        let early = IngestSourceReader::new_controlled(
            schema(),
            runtime,
            vec![Box::new(|| {
                Ok(Box::new(RecordBatchIterator::new(
                    (0..100).map(|value| Ok(batch(value))),
                    schema(),
                )) as Box<dyn RecordBatchReader + Send>)
            })],
            1,
            &parent,
        );
        let early_child = early.cancellation.clone();
        drop(early);
        assert!(early_child.is_cancelled());
        assert!(parent.check().is_ok());
    }
}
