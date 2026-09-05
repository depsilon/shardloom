use super::*;
use std::sync::atomic::AtomicUsize;
use std::time::Duration;

fn schema() -> SchemaRef {
    Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]))
}

fn batch(id: usize) -> RecordBatch {
    RecordBatch::try_new(
        schema(),
        vec![Arc::new(Int64Array::from(vec![i64::try_from(id).unwrap()]))],
    )
    .unwrap()
}

fn tasks(count: usize) -> Vec<ParquetRowGroupReadTask> {
    (0..count)
        .map(|task_index| ParquetRowGroupReadTask {
            task_index,
            row_groups: vec![task_index],
        })
        .collect()
}

fn collect_ids(
    mut reader: ParquetRowGroupParallelRecordBatchReader,
) -> std::result::Result<Vec<i64>, ArrowError> {
    let mut ids = Vec::new();
    for result in reader.by_ref() {
        let batch = result?;
        ids.extend_from_slice(
            batch
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .values(),
        );
    }
    assert!(reader.next().is_none());
    assert!(reader.workers.is_empty());
    assert!(reader.receivers.is_empty());
    Ok(ids)
}

#[test]
fn slow_first_task_bounds_all_queued_and_blocked_batches_and_preserves_order() {
    const WORKERS: usize = 3;
    const TASKS: usize = 30;
    const BATCHES: usize = 8;
    let (release, blocked) = mpsc::sync_channel(1);
    let blocked = Mutex::new(blocked);
    let (created, created_batches) = mpsc::channel();
    let reader = ParquetRowGroupParallelRecordBatchReader::with_task_runner(
        schema(),
        tasks(TASKS),
        WORKERS,
        move |task, sender| {
            if task.task_index == 0 {
                let _ = blocked.lock().unwrap().recv();
            }
            for index in 0..BATCHES {
                let batch = batch(task.task_index * BATCHES + index);
                let _ = created.send((task.task_index, index, batch.get_array_memory_size()));
                if sender
                    .send(ParquetRowGroupReadResult::Batch(Ok(batch)))
                    .is_err()
                {
                    break;
                }
            }
            Ok(())
        },
    )
    .unwrap();
    let (finished, completion) = mpsc::channel();
    let consumer = thread::spawn(move || {
        let _ = finished.send(collect_ids(reader));
    });

    // The consumer is already pulling the first task. Later batches must remain
    // in their bounded task channels, not migrate into an unbounded reorder map.
    let max_retained = (WORKERS - 1) * (PARQUET_ROW_GROUP_RESULT_QUEUE_BATCHES_PER_WORKER + 1);
    let mut retained_bytes = 0;
    for _ in 0..max_retained {
        let (task, index, bytes) = created_batches
            .recv_timeout(Duration::from_secs(5))
            .unwrap();
        assert!((1..WORKERS).contains(&task));
        assert!(index <= PARQUET_ROW_GROUP_RESULT_QUEUE_BATCHES_PER_WORKER);
        retained_bytes += bytes;
    }
    assert_eq!(
        retained_bytes,
        max_retained * batch(0).get_array_memory_size()
    );
    assert!(matches!(
        created_batches.recv_timeout(Duration::from_millis(30)),
        Err(mpsc::RecvTimeoutError::Timeout)
    ));
    release.send(()).unwrap();
    let ids = completion
        .recv_timeout(Duration::from_secs(5))
        .unwrap()
        .unwrap();
    consumer.join().unwrap();
    assert_eq!(
        ids,
        (0..i64::try_from(TASKS * BATCHES).unwrap()).collect::<Vec<_>>()
    );
}

#[test]
fn completed_short_tasks_cannot_run_ahead_of_the_admitted_window() {
    const WORKERS: usize = 4;
    let (release, blocked) = mpsc::sync_channel(1);
    let blocked = Mutex::new(blocked);
    let (started, starts) = mpsc::channel();
    let reader = ParquetRowGroupParallelRecordBatchReader::with_task_runner(
        schema(),
        tasks(100),
        WORKERS,
        move |task, sender| {
            let _ = started.send(task.task_index);
            if task.task_index == 0 {
                let _ = blocked.lock().unwrap().recv();
            }
            let _ = sender.send(ParquetRowGroupReadResult::Batch(Ok(batch(task.task_index))));
            Ok(())
        },
    )
    .unwrap();
    let (finished, completion) = mpsc::channel();
    let consumer = thread::spawn(move || {
        let _ = finished.send(collect_ids(reader));
    });
    let mut admitted = Vec::new();
    for _ in 0..WORKERS {
        admitted.push(starts.recv_timeout(Duration::from_secs(5)).unwrap());
    }
    admitted.sort_unstable();
    assert_eq!(admitted, (0..WORKERS).collect::<Vec<_>>());
    assert!(matches!(
        starts.recv_timeout(Duration::from_millis(30)),
        Err(mpsc::RecvTimeoutError::Timeout)
    ));
    release.send(()).unwrap();
    let ids = completion
        .recv_timeout(Duration::from_secs(5))
        .unwrap()
        .unwrap();
    consumer.join().unwrap();
    assert_eq!(ids, (0..100).collect::<Vec<_>>());
}

#[test]
fn first_task_errors_disconnect_full_later_channels_and_join_every_worker() {
    for batch_error in [false, true] {
        let (later_ready, wait_for_later) = mpsc::sync_channel(1);
        let wait_for_later = Mutex::new(wait_for_later);
        let completed = Arc::new(AtomicUsize::new(0));
        let worker_completed = Arc::clone(&completed);
        let mut reader = ParquetRowGroupParallelRecordBatchReader::with_task_runner(
            schema(),
            tasks(40),
            2,
            move |task, sender| {
                if task.task_index == 0 {
                    wait_for_later
                        .lock()
                        .unwrap()
                        .recv_timeout(Duration::from_secs(5))
                        .unwrap();
                    let error = ArrowError::ComputeError("injected source failure".to_string());
                    if batch_error {
                        sender
                            .send(ParquetRowGroupReadResult::Batch(Err(error)))
                            .unwrap();
                        return Ok(());
                    }
                    return Err(error);
                }
                for index in 0..100 {
                    if index == PARQUET_ROW_GROUP_RESULT_QUEUE_BATCHES_PER_WORKER {
                        let _ = later_ready.send(());
                    }
                    if sender
                        .send(ParquetRowGroupReadResult::Batch(Ok(batch(index))))
                        .is_err()
                    {
                        break;
                    }
                }
                worker_completed.fetch_add(1, Ordering::SeqCst);
                Ok(())
            },
        )
        .unwrap();
        let (finished, completion) = mpsc::channel();
        let consumer = thread::spawn(move || {
            let error = reader.next().unwrap().unwrap_err();
            assert!(reader.next().is_none());
            assert!(reader.workers.is_empty());
            assert!(reader.receivers.is_empty());
            let _ = finished.send(error.to_string());
        });
        let error = completion.recv_timeout(Duration::from_secs(5)).unwrap();
        consumer.join().unwrap();
        assert!(error.contains("injected source failure"));
        assert_eq!(completed.load(Ordering::SeqCst), 1);
    }
}

#[test]
fn dropping_reader_unblocks_senders_and_does_not_dispatch_remaining_source() {
    const WORKERS: usize = 3;
    let (ready, blocked_senders) = mpsc::channel();
    let started = Arc::new(AtomicUsize::new(0));
    let stopped = Arc::new(AtomicUsize::new(0));
    let worker_started = Arc::clone(&started);
    let worker_stopped = Arc::clone(&stopped);
    let reader = ParquetRowGroupParallelRecordBatchReader::with_task_runner(
        schema(),
        tasks(100),
        WORKERS,
        move |_, sender| {
            worker_started.fetch_add(1, Ordering::SeqCst);
            for index in 0..100 {
                if index == PARQUET_ROW_GROUP_RESULT_QUEUE_BATCHES_PER_WORKER {
                    let _ = ready.send(());
                }
                if sender
                    .send(ParquetRowGroupReadResult::Batch(Ok(batch(index))))
                    .is_err()
                {
                    break;
                }
            }
            worker_stopped.fetch_add(1, Ordering::SeqCst);
            Ok(())
        },
    )
    .unwrap();
    for _ in 0..WORKERS {
        blocked_senders
            .recv_timeout(Duration::from_secs(5))
            .unwrap();
    }
    let (finished, completion) = mpsc::channel();
    let consumer = thread::spawn(move || {
        drop(reader);
        let _ = finished.send(());
    });
    completion.recv_timeout(Duration::from_secs(5)).unwrap();
    consumer.join().unwrap();
    assert_eq!(started.load(Ordering::SeqCst), WORKERS);
    assert_eq!(stopped.load(Ordering::SeqCst), WORKERS);
}

#[test]
fn empty_tasks_finish_without_workers_and_worker_panic_fails_closed() {
    let empty = ParquetRowGroupParallelRecordBatchReader::with_task_runner(
        schema(),
        Vec::new(),
        4,
        |_, _| panic!("empty source must not execute a task"),
    )
    .unwrap();
    assert!(collect_ids(empty).unwrap().is_empty());

    let mut reader = ParquetRowGroupParallelRecordBatchReader::with_task_runner(
        schema(),
        tasks(1),
        1,
        |_, _| panic!("injected worker panic"),
    )
    .unwrap();
    assert!(
        reader
            .next()
            .unwrap()
            .unwrap_err()
            .to_string()
            .contains("task 0")
    );
    assert!(reader.next().is_none());
    assert!(reader.workers.is_empty());
}

#[test]
fn bounded_public_parquet_source_matches_serial_nullable_renamed_columns() {
    const ROWS: usize = 4_097;
    let path = std::env::temp_dir().join(format!(
        "shardloom-parquet-bounded-source-{}-{}.parquet",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let schema = Arc::new(Schema::new(vec![
        Field::new("shipment_sequence", DataType::Int64, true),
        Field::new("destination", DataType::Utf8, true),
    ]));
    let values = (0..ROWS)
        .map(|index| (index % 7 != 0).then(|| i64::try_from(index).unwrap()))
        .collect::<Vec<_>>();
    let labels = (0..ROWS)
        .map(|index| (index % 11 != 0).then(|| format!("depot-{}-東京", index % 37)))
        .collect::<Vec<_>>();
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(values.clone())),
            Arc::new(StringArray::from(labels.clone())),
        ],
    )
    .unwrap();
    let properties = parquet::file::properties::WriterProperties::builder()
        .set_max_row_group_row_count(Some(17))
        .build();
    let mut writer = parquet::arrow::ArrowWriter::try_new(
        File::create(&path).unwrap(),
        schema,
        Some(properties),
    )
    .unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();

    for parallelism in [1, 2, 4, 12] {
        let mut source =
            stream_flat_parquet_columnar_source_with_parallelism(&path, ROWS, parallelism).unwrap();
        if parallelism > 1 {
            assert!(source.ingest_executor_unit_count_hint.unwrap() > 1);
            assert!(source.source_stream_policy.contains(&format!(
                "parquet_ordered_task_window={}",
                source.ingest_executor_applied_parallelism
            )));
            assert!(source.source_stream_policy.contains(&format!(
                "parquet_max_retained_decoded_batches={}",
                source.ingest_executor_applied_parallelism
                    * (PARQUET_ROW_GROUP_RESULT_QUEUE_BATCHES_PER_WORKER + 1)
            )));
            assert!(
                source
                    .source_stream_policy
                    .contains("parquet_source_byte_bound=not_enforced")
            );
        }
        let mut observed = 0;
        for result in source.reader.by_ref() {
            let batch = result.unwrap();
            let ids = batch
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            for index in 0..batch.num_rows() {
                assert_eq!(
                    ids.is_valid(index).then(|| ids.value(index)),
                    values[observed]
                );
                assert_eq!(
                    utf8_array_value(batch.column(1), index).unwrap(),
                    labels[observed].as_deref()
                );
                observed += 1;
            }
        }
        assert_eq!(observed, ROWS);
        assert!(source.reader.next().is_none());
    }
    std::fs::remove_file(path).unwrap();
}

struct DropObservedSource {
    produced: usize,
    failure_at: Option<usize>,
    reached_blocked_send: mpsc::Sender<()>,
    dropped: Arc<AtomicUsize>,
}

impl Iterator for DropObservedSource {
    type Item = std::result::Result<RecordBatch, ArrowError>;

    fn next(&mut self) -> Option<Self::Item> {
        let index = self.produced;
        self.produced += 1;
        if index == 1 {
            let _ = self.reached_blocked_send.send(());
        }
        if self.failure_at == Some(index) {
            return Some(Err(ArrowError::ComputeError(
                "injected prefetch error".into(),
            )));
        }
        assert!(self.failure_at.is_none_or(|failure| index < failure));
        Some(Ok(batch(index)))
    }
}

impl RecordBatchReader for DropObservedSource {
    fn schema(&self) -> SchemaRef {
        schema()
    }
}

impl Drop for DropObservedSource {
    fn drop(&mut self) {
        self.dropped.fetch_add(1, Ordering::SeqCst);
    }
}

#[test]
fn outer_prefetch_drop_joins_the_owned_source_with_a_full_channel() {
    let (reached_blocked_send, ready) = mpsc::channel();
    let dropped = Arc::new(AtomicUsize::new(0));
    let reader = CapillaryPrefetchRecordBatchReader::new(
        Box::new(DropObservedSource {
            produced: 0,
            failure_at: None,
            reached_blocked_send,
            dropped: Arc::clone(&dropped),
        }),
        1,
    );
    ready.recv_timeout(Duration::from_secs(5)).unwrap();
    let (finished, completion) = mpsc::channel();
    let consumer = thread::spawn(move || {
        drop(reader);
        let _ = finished.send(());
    });
    completion.recv_timeout(Duration::from_secs(5)).unwrap();
    consumer.join().unwrap();
    assert_eq!(dropped.load(Ordering::SeqCst), 1);
}

#[test]
fn outer_prefetch_error_stops_reading_and_joins_before_return() {
    let (reached_blocked_send, _) = mpsc::channel();
    let dropped = Arc::new(AtomicUsize::new(0));
    let mut reader = CapillaryPrefetchRecordBatchReader::new(
        Box::new(DropObservedSource {
            produced: 0,
            failure_at: Some(1),
            reached_blocked_send,
            dropped: Arc::clone(&dropped),
        }),
        1,
    );
    assert_eq!(reader.next().unwrap().unwrap().num_rows(), 1);
    assert!(
        reader
            .next()
            .unwrap()
            .unwrap_err()
            .to_string()
            .contains("injected prefetch error")
    );
    assert_eq!(dropped.load(Ordering::SeqCst), 1);
    assert!(reader.next().is_none());
    assert!(reader.worker.is_none());
    assert!(reader.receiver.is_none());
}
