use super::*;
use arrow_array::{Int64Array, RecordBatchIterator, StringArray};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use std::{collections::VecDeque, sync::mpsc, thread, time::Duration};
use vortex::{VortexSessionDefault as _, array::VortexSessionExecute as _};

fn scalar(array: &vortex::array::ArrayRef, row: usize) -> vortex::array::scalar::Scalar {
    array
        .execute_scalar(
            row,
            &mut vortex::session::VortexSession::default().create_execution_ctx(),
        )
        .unwrap()
}

fn batch(index: i64) -> RecordBatch {
    RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("renamed_id", DataType::Int64, true),
            Field::new("renamed_text", DataType::Utf8, true),
        ])),
        vec![
            Arc::new(Int64Array::from(vec![Some(index), None, Some(-index)])),
            Arc::new(StringArray::from(vec![Some("\u{3bb}"), None, Some("")])),
        ],
    )
    .unwrap()
}

fn shape() -> FlatColumnarSourceShape {
    FlatColumnarSourceShape {
        projected_columns: vec![
            ColumnarProjectedColumn {
                column: "renamed_id".to_string(),
                reader_index: 0,
                dtype_hint: None,
                arrow_dtype_hint: Some(DataType::Int64),
            },
            ColumnarProjectedColumn {
                column: "renamed_text".to_string(),
                reader_index: 1,
                dtype_hint: None,
                arrow_dtype_hint: Some(DataType::Utf8),
            },
        ],
    }
}

struct ObservedReader {
    schema: SchemaRef,
    batches: VecDeque<std::result::Result<RecordBatch, arrow_schema::ArrowError>>,
    pulls: Arc<AtomicUsize>,
    dropped: Arc<std::sync::atomic::AtomicBool>,
}

impl Iterator for ObservedReader {
    type Item = std::result::Result<RecordBatch, arrow_schema::ArrowError>;

    fn next(&mut self) -> Option<Self::Item> {
        self.pulls.fetch_add(1, Ordering::Relaxed);
        self.batches.pop_front()
    }
}

impl arrow_array::RecordBatchReader for ObservedReader {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
}

impl Drop for ObservedReader {
    fn drop(&mut self) {
        self.dropped.store(true, Ordering::Release);
    }
}

fn iterator(
    reader: Box<dyn arrow_array::RecordBatchReader + Send>,
    window: usize,
    budget: u64,
) -> Result<StreamingColumnarVortexArrayIterator> {
    let first = record_batch_to_vortex_from_arrow_provider(&batch(0), &shape())?;
    StreamingColumnarVortexArrayIterator::new(
        first.dtype().clone(),
        first,
        reader,
        vec!["renamed_id".to_string(), "renamed_text".to_string()],
        shape(),
        Arc::new(AtomicUsize::new(1)),
        VortexStreamingIngestTiming::default(),
        1,
        window,
        window,
        budget,
    )
}

#[test]
fn slow_writer_cannot_allow_unbounded_prefetch_and_preserves_values() {
    let pulls = Arc::new(AtomicUsize::new(0));
    let dropped = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let reader = ObservedReader {
        schema: batch(0).schema(),
        batches: (1..48).map(|index| Ok(batch(index))).collect(),
        pulls: Arc::clone(&pulls),
        dropped: Arc::clone(&dropped),
    };
    let mut stream = iterator(Box::new(reader), 3, 3 * 65536).unwrap();
    let memory = stream.prefetch.as_ref().unwrap().pool.memory().clone();
    let deadline = Instant::now() + Duration::from_secs(5);
    while stream
        .prefetch
        .as_ref()
        .unwrap()
        .pool
        .snapshot()
        .completed_jobs
        < 3
    {
        assert!(Instant::now() < deadline);
        thread::yield_now();
    }
    assert_eq!(pulls.load(Ordering::Relaxed), 3);
    assert_eq!(memory.snapshot().reserved_bytes, 3 * 65536);
    for index in 0..48 {
        let actual = stream.next().unwrap().unwrap();
        let expected = record_batch_to_vortex_from_arrow_provider(&batch(index), &shape()).unwrap();
        for row in 0..3 {
            assert_eq!(scalar(&actual, row), scalar(&expected, row));
        }
        assert!(memory.snapshot().reserved_bytes <= 3 * 65536);
    }
    assert!(stream.next().is_none());
    assert!(stream.next().is_none());
    assert_eq!(memory.snapshot().reserved_bytes, 0);
    assert!(dropped.load(Ordering::Acquire));
}

#[test]
fn early_drop_drains_workers_and_releases_reader_and_credits() {
    let dropped = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let reader = ObservedReader {
        schema: batch(0).schema(),
        batches: (1..100).map(|index| Ok(batch(index))).collect(),
        pulls: Arc::new(AtomicUsize::new(0)),
        dropped: Arc::clone(&dropped),
    };
    let stream = iterator(Box::new(reader), 3, 3 * 65536).unwrap();
    let memory = stream.prefetch.as_ref().unwrap().pool.memory().clone();
    drop(stream);
    assert!(dropped.load(Ordering::Acquire));
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

#[test]
fn prefetch_failure_is_terminal_and_never_skips_bad_batches() {
    for oversize in [false, true] {
        let batches = if oversize {
            vec![Ok(batch(1))]
        } else {
            vec![Err(arrow_schema::ArrowError::ParseError(
                "injected read failure".to_string(),
            ))]
        };
        let reader = RecordBatchIterator::new(batches, batch(0).schema());
        let mut stream = iterator(Box::new(reader), 1, if oversize { 128 } else { 65536 }).unwrap();
        let memory = stream.prefetch.as_ref().unwrap().pool.memory().clone();
        assert!(stream.next().unwrap().is_ok());
        assert!(stream.next().unwrap().is_err());
        assert!(stream.next().is_none());
        assert_eq!(memory.snapshot().reserved_bytes, 0);
    }
}

#[test]
fn completed_out_of_order_arrays_remain_charged_until_ordered_handoff() {
    let reader = RecordBatchIterator::new(
        Vec::<std::result::Result<RecordBatch, arrow_schema::ArrowError>>::new(),
        batch(0).schema(),
    );
    let mut stream = iterator(Box::new(reader), 2, 131_072).unwrap();
    let prefetch = stream.prefetch.as_mut().unwrap();
    for task in prefetch.tasks.drain(..) {
        drop(task.join().unwrap());
    }
    prefetch.exhausted = true;
    let memory = prefetch.pool.memory().clone();
    let (second_ready, first_release) = mpsc::channel();
    for index in [2, 1] {
        let array = record_batch_to_vortex_from_arrow_provider(&batch(index), &shape()).unwrap();
        let ready = if index == 2 {
            Some(second_ready.clone())
        } else {
            None
        };
        let wait = if index == 1 {
            Some(&first_release)
        } else {
            None
        };
        // Enqueue the later source batch first to force the reorder path, independent
        // of physical worker assignment or OS scheduling.
        if let Some(wait) = wait {
            wait.recv_timeout(Duration::from_secs(5)).unwrap();
        }
        let task = prefetch
            .pool
            .submit(
                Budgeted::new(
                    move |_: &WorkerContext, _: &mut MemoryLease| {
                        if let Some(ready) = ready {
                            ready.send(()).unwrap();
                        }
                        Ok(Some((usize::try_from(index).unwrap(), array)))
                    },
                    memory.reserve(65536).unwrap(),
                ),
                prefetch.cancellation.clone(),
            )
            .unwrap();
        prefetch.tasks.push_back(task);
    }
    assert_eq!(memory.snapshot().reserved_bytes, 131_072);
    let first = prefetch.next_array(1).unwrap().unwrap();
    assert_eq!(prefetch.pending.len(), 1);
    assert_eq!(memory.snapshot().reserved_bytes, 65536);
    let second = prefetch.next_array(2).unwrap().unwrap();
    assert_ne!(scalar(&first, 0), scalar(&second, 0));
    assert_eq!(memory.snapshot().reserved_bytes, 0);
    assert!(prefetch.next_array(3).is_none());
}
