use super::*;
use arrow_array::{Int64Array, RecordBatchIterator};
use arrow_schema::{DataType, Field, Schema};
use futures::channel::oneshot;
use std::{
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
        let runtime = IngestRuntime::new(requested);
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
        let runtime = IngestRuntime::new(requested);
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
        let runtime = IngestRuntime::new(2);
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
    let runtime = IngestRuntime::new(8);
    let mut reader = IngestSourceReader::new(schema(), runtime, vec![], 8);
    assert!(reader.next().is_none());
    assert!(reader.tasks.is_empty());
}
