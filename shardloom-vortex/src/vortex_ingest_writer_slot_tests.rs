//! Producer/writer slot-handshake integration with real native conversion jobs.
//!
//! These tests prove the producer's task/result slots plus the two explicit
//! consumer-held arrays. They do not count aliases retained inside codecs or
//! establish equal total allocation peaks between writer schedules.

use super::*;
use arrow_array::{Int64Array, RecordBatchIterator};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use std::{collections::VecDeque, sync::mpsc, time::Duration};
use vortex::array::VortexSessionExecute as _;

const TASK_BYTES: u64 = 16_384;
const MEMORY_BYTES: u64 = 1 << 20;
const BATCHES: usize = 12;

fn batch(index: usize, first_empty: bool) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "slot_id",
        DataType::Int64,
        true,
    )]));
    if index == 4 || (index == 0 && first_empty) {
        return RecordBatch::new_empty(schema);
    }
    let index = i64::try_from(index).unwrap();
    RecordBatch::try_new(
        schema,
        vec![Arc::new(Int64Array::from(vec![
            Some(index),
            None,
            Some(-index),
        ]))],
    )
    .unwrap()
}

fn shape() -> FlatColumnarSourceShape {
    FlatColumnarSourceShape {
        projected_columns: vec![ColumnarProjectedColumn {
            column: "slot_id".into(),
            reader_index: 0,
            dtype_hint: None,
            arrow_dtype_hint: Some(DataType::Int64),
        }],
    }
}

fn decision(parallelism: usize) -> VortexLayoutWriteRuntimeDecision {
    let mut decision = VortexLayoutWriteRuntimeDecision::not_requested_for_source(
        "vortex_array_kernel",
        "slot handshake fixture",
        VortexIngestCertificationLevel::IngestCertified,
        VortexWriterPhysicalDesignSourceInput::writer_only(),
    );
    // Exercise admission directly; this fixture does not claim CPU-plan coverage.
    decision.writer_runtime_requested_parallelism = parallelism;
    decision.writer_runtime_applied_parallelism = parallelism;
    decision.writer_runtime_background_workers = parallelism.saturating_sub(1);
    decision
}

struct ObservedReader {
    inner: Box<dyn arrow_array::RecordBatchReader + Send>,
    dropped: Arc<AtomicUsize>,
}

impl Iterator for ObservedReader {
    type Item = std::result::Result<RecordBatch, arrow_schema::ArrowError>;

    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next()
    }
}

impl arrow_array::RecordBatchReader for ObservedReader {
    fn schema(&self) -> SchemaRef {
        self.inner.schema()
    }
}

impl Drop for ObservedReader {
    fn drop(&mut self) {
        self.dropped.fetch_add(1, Ordering::SeqCst);
    }
}

struct Fixture {
    stream: StreamingColumnarVortexArrayIterator,
    memory: NativeIngestMemory,
    dropped: Arc<AtomicUsize>,
}

fn fixture_with_reader(
    window: usize,
    owned: bool,
    first_empty: bool,
    reader: Box<dyn arrow_array::RecordBatchReader + Send>,
) -> Fixture {
    let memory = NativeIngestMemory::new(MEMORY_BYTES).unwrap();
    let dropped = Arc::new(AtomicUsize::new(0));
    let mut lease = memory.reserve_input(1).unwrap();
    let first = record_batch_to_vortex_from_arrow_provider_profiled_with_memory(
        &batch(0, first_empty),
        &shape(),
        &IngestStageTimings::default(),
        owned.then_some((&memory, &mut lease)),
    )
    .unwrap();
    drop(lease);
    let stream = StreamingColumnarVortexArrayIterator::new(
        first.dtype().clone(),
        first,
        Box::new(ObservedReader {
            inner: reader,
            dropped: Arc::clone(&dropped),
        }),
        vec!["slot_id".into()],
        shape(),
        Arc::new(AtomicUsize::new(1)),
        VortexStreamingIngestTiming::default(),
        1,
        window,
        usize::from(window > 0),
        TASK_BYTES * u64::try_from(window.max(1)).unwrap(),
        owned.then(|| memory.clone()),
    )
    .unwrap();
    Fixture {
        stream,
        memory,
        dropped,
    }
}

fn fixture(window: usize, owned: bool, first_empty: bool) -> Fixture {
    fixture_with_reader(
        window,
        owned,
        first_empty,
        Box::new(RecordBatchIterator::new(
            (1..BATCHES).map(move |index| Ok(batch(index, first_empty))),
            batch(0, first_empty).schema(),
        )),
    )
}

fn producer_slots(stream: &StreamingColumnarVortexArrayIterator) -> usize {
    stream
        .prefetch
        .as_ref()
        .map_or(0, |prefetch| prefetch.tasks.len() + prefetch.pending.len())
}

fn assert_policy(stream: &StreamingColumnarVortexArrayIterator, window: usize) {
    if let Some(prefetch) = &stream.prefetch {
        assert_eq!(prefetch.window, window);
        assert_eq!(prefetch.task_bytes, TASK_BYTES);
        assert_eq!(prefetch.pool.snapshot().workers_created, 1);
        assert!(!prefetch.refill_after_handoff);
        assert!(producer_slots(stream) <= window);
    }
}

fn assert_values(
    array: &vortex::array::ArrayRef,
    index: usize,
    first_empty: bool,
    memory: &NativeIngestMemory,
) {
    let expected = record_batch_to_vortex_from_arrow_provider_profiled(
        &batch(index, first_empty),
        &shape(),
        &IngestStageTimings::default(),
    )
    .unwrap();
    assert_eq!(array.dtype(), expected.dtype());
    assert_eq!(array.len(), expected.len());
    let mut ctx = memory.session.create_execution_ctx();
    for row in 0..array.len() {
        assert_eq!(
            array.execute_scalar(row, &mut ctx).unwrap(),
            expected.execute_scalar(row, &mut ctx).unwrap()
        );
    }
}

#[test]
fn shared_slot_preserves_startup_steady_empty_and_eof_bounds() {
    for window in [1, 3] {
        for first_empty in [false, true] {
            let Fixture {
                mut stream,
                memory,
                dropped,
            } = fixture(window, true, first_empty);
            let owner = Arc::downgrade(&stream.prefetch.as_ref().unwrap().context);
            assert_eq!(producer_slots(&stream), window);
            let grant = stream.share_input_slot_with_writer(&decision(2));
            assert!(grant.is_some());
            assert!(stream.share_input_slot_with_writer(&decision(2)).is_none());
            assert_policy(&stream, window);
            let mut current = stream.next();
            assert_eq!(
                producer_slots(&stream),
                window,
                "first array was already owned"
            );
            let mut seen = 0;
            while let Some(item) = current {
                let array = item.unwrap();
                assert_values(&array, seen, first_empty, &memory);
                seen += 1;
                if array.is_empty() {
                    // Match the writer's early release before another pull.
                    drop(array);
                    current = stream.next();
                    assert!(producer_slots(&stream) + usize::from(current.is_some()) <= window + 1);
                    continue;
                }
                let lookahead = stream.next();
                assert_policy(&stream, window);
                if lookahead.is_some() {
                    assert!(producer_slots(&stream) < window);
                    let input_slot_limit = window + 1;
                    assert!(producer_slots(&stream) + 2 <= input_slot_limit);
                    if seen + window < BATCHES {
                        assert_eq!(
                            producer_slots(&stream),
                            window - 1,
                            "only the handed-off slot stays vacant"
                        );
                    }
                } else {
                    assert!(stream.prefetch.is_none(), "EOF drains the producer");
                }
                drop(array);
                current = lookahead;
            }
            assert_eq!(seen, BATCHES);
            assert!(stream.next().is_none());
            assert!(owner.upgrade().is_none());
            assert_eq!(dropped.load(Ordering::SeqCst), 1);
            drop((grant, stream));
            assert_eq!(memory.pool.snapshot().denied_reservations, 0);
            assert_eq!(memory.pool.snapshot().reserved_bytes, 0);
        }
    }
}

#[test]
fn no_handshake_keeps_immediate_refill_and_grants_are_not_available_late() {
    for window in [1, 3] {
        let Fixture {
            mut stream,
            memory,
            dropped,
        } = fixture(window, true, false);
        let owner = Arc::downgrade(&stream.prefetch.as_ref().unwrap().context);
        let first = stream.next().unwrap().unwrap();
        assert!(stream.share_input_slot_with_writer(&decision(2)).is_none());
        let second = stream.next().unwrap().unwrap();
        assert_eq!(producer_slots(&stream), window);
        assert!(stream.prefetch.as_ref().unwrap().refill_after_handoff);
        assert_eq!(stream.prefetch.as_ref().unwrap().task_bytes, TASK_BYTES);
        drop((first, second, stream));
        assert!(owner.upgrade().is_none());
        assert_eq!(dropped.load(Ordering::SeqCst), 1);
        assert_eq!(memory.pool.snapshot().reserved_bytes, 0);
    }
}

#[test]
fn handshake_declines_unowned_unqueued_and_single_driver_inputs_without_mutation() {
    for (window, owned, parallelism) in [(1, false, 2), (0, true, 2), (3, true, 1)] {
        let Fixture {
            mut stream,
            memory,
            dropped,
        } = fixture(window, owned, false);
        let producer_memory = stream.prefetch.as_ref().map(|p| p.pool.memory().clone());
        let before = producer_slots(&stream);
        assert!(
            stream
                .share_input_slot_with_writer(&decision(parallelism))
                .is_none()
        );
        assert_eq!(producer_slots(&stream), before);
        assert!(stream.first_array.is_some());
        assert!(
            stream
                .prefetch
                .as_ref()
                .is_none_or(|p| p.refill_after_handoff)
        );
        drop(stream);
        assert_eq!(dropped.load(Ordering::SeqCst), 1);
        assert_eq!(memory.pool.snapshot().reserved_bytes, 0);
        if let Some(pool) = producer_memory {
            assert_eq!(pool.snapshot().reserved_bytes, 0);
        }
    }
}

/// Join existing real conversion tasks without requesting another input. This
/// makes pressure injection deterministic: no worker allocation races the lease.
fn settle_window(stream: &mut StreamingColumnarVortexArrayIterator) {
    let prefetch = stream.prefetch.as_mut().unwrap();
    for task in prefetch.tasks.drain(..) {
        let (result, lease) = task.join().unwrap().into_parts();
        let (index, array) = result.unwrap();
        assert!(
            prefetch
                .pending
                .insert(index, Budgeted::new(array, lease))
                .is_none()
        );
    }
}

#[test]
fn entry_refill_denial_is_terminal_and_releases_the_shared_slot() {
    for window in [1, 3] {
        let Fixture {
            mut stream,
            memory,
            dropped,
        } = fixture(window, true, false);
        let grant = stream.share_input_slot_with_writer(&decision(2));
        assert!(grant.is_some());
        let owner = Arc::downgrade(&stream.prefetch.as_ref().unwrap().context);
        settle_window(&mut stream);
        let first = stream.next().unwrap().unwrap();
        let lookahead = stream.next().unwrap().unwrap();
        assert_eq!(producer_slots(&stream), window - 1);
        // The preceding child must release before the next entry refill.
        drop(first);
        let available = MEMORY_BYTES - memory.pool.snapshot().reserved_bytes;
        let pressure = memory.pool.reserve(available).unwrap();
        let error = stream.next().unwrap().unwrap_err();
        assert!(error.to_string().contains("memory reservation denied"));
        assert!(stream.prefetch.is_none());
        assert!(stream.next().is_none());
        assert!(owner.upgrade().is_none());
        assert_eq!(dropped.load(Ordering::SeqCst), 1);
        assert_eq!(memory.pool.snapshot().denied_reservations, 1);
        drop((lookahead, pressure, grant, stream));
        assert_eq!(memory.pool.snapshot().reserved_bytes, 0);
    }
}

struct GatedReader {
    schema: SchemaRef,
    entered: Option<mpsc::Sender<()>>,
    release: mpsc::Receiver<()>,
    released: Arc<AtomicUsize>,
    batches: VecDeque<RecordBatch>,
}

impl Iterator for GatedReader {
    type Item = std::result::Result<RecordBatch, arrow_schema::ArrowError>;

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(entered) = self.entered.take() {
            entered.send(()).unwrap();
            self.release
                .recv_timeout(Duration::from_secs(5))
                .expect("test must explicitly release the blocked reader");
            self.released.store(1, Ordering::SeqCst);
        }
        self.batches.pop_front().map(Ok)
    }
}

impl arrow_array::RecordBatchReader for GatedReader {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
}

#[test]
fn cancellation_drains_blocked_conversion_and_releases_native_input() {
    for window in [1, 3] {
        let (entered, blocked) = mpsc::channel();
        let (release, waiting) = mpsc::channel();
        let released = Arc::new(AtomicUsize::new(0));
        let Fixture {
            mut stream,
            memory,
            dropped,
        } = fixture_with_reader(
            window,
            true,
            false,
            Box::new(GatedReader {
                schema: batch(0, false).schema(),
                entered: Some(entered),
                release: waiting,
                released: Arc::clone(&released),
                batches: (1..BATCHES).map(|index| batch(index, false)).collect(),
            }),
        );
        let grant = stream.share_input_slot_with_writer(&decision(2));
        assert!(grant.is_some());
        blocked.recv_timeout(Duration::from_secs(5)).unwrap();
        let prefetch = stream.prefetch.as_ref().unwrap();
        let owner = Arc::downgrade(&prefetch.context);
        let cancellation = prefetch.cancellation.clone();
        let first = stream.next().unwrap().unwrap();
        assert_eq!(producer_slots(&stream), window);
        cancellation.cancel();
        release.send(()).unwrap();
        drop(stream);
        assert_eq!(released.load(Ordering::SeqCst), 1);
        assert!(owner.upgrade().is_none());
        assert_eq!(dropped.load(Ordering::SeqCst), 1);
        assert!(
            memory.pool.snapshot().reserved_bytes > 0,
            "consumer retains the first array"
        );
        drop((first, grant));
        assert_eq!(memory.pool.snapshot().reserved_bytes, 0);
    }
}

#[test]
fn dropping_after_handoff_releases_both_consumer_arrays_and_remaining_producer() {
    for window in [1, 3] {
        let Fixture {
            mut stream,
            memory,
            dropped,
        } = fixture(window, true, false);
        let grant = stream.share_input_slot_with_writer(&decision(2));
        assert!(grant.is_some());
        let owner = Arc::downgrade(&stream.prefetch.as_ref().unwrap().context);
        let first = stream.next().unwrap().unwrap();
        let lookahead = stream.next().unwrap().unwrap();
        assert_eq!(producer_slots(&stream), window - 1);
        drop(stream);
        assert!(owner.upgrade().is_none());
        assert_eq!(dropped.load(Ordering::SeqCst), 1);
        assert!(memory.pool.snapshot().reserved_bytes > 0);
        drop((first, lookahead, grant));
        assert_eq!(memory.pool.snapshot().reserved_bytes, 0);
    }
}
