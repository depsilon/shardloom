use super::*;
use arrow_array::{BooleanArray, Float64Array, Int64Array, RecordBatchIterator, StringArray};
use arrow_schema::{DataType, Field, Schema};
use std::{collections::VecDeque, sync::mpsc, thread, time::Duration};
use vortex::{
    VortexSessionDefault as _, array::VortexSessionExecute as _, file::OpenOptionsSessionExt as _,
    io::runtime::BlockingRuntime as _, io::session::RuntimeSessionExt as _,
};

fn batch(index: i64) -> RecordBatch {
    RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("renamed_id", DataType::Int64, true),
            Field::new("renamed_text", DataType::Utf8, true),
            Field::new("renamed_bool", DataType::Boolean, true),
            Field::new("renamed_metric", DataType::Float64, true),
        ])),
        vec![
            Arc::new(Int64Array::from(vec![
                Some((1_i64 << 60) + index),
                None,
                Some(-index),
            ])),
            Arc::new(StringArray::from(vec![
                Some("λ repeated dictionary value"),
                None,
                Some(""),
            ])),
            Arc::new(BooleanArray::from(vec![Some(true), None, Some(false)])),
            Arc::new(Float64Array::from(vec![
                Some(f64::from(i32::try_from(index).unwrap()) + 0.5),
                None,
                Some(-0.5),
            ])),
        ],
    )
    .unwrap()
}

fn source(batches: Vec<RecordBatch>, error: bool) -> FlatLocalColumnarStreamSource {
    let schema = batch(0).schema();
    let columns = schema
        .fields()
        .iter()
        .map(|field| field.name().clone())
        .collect::<Vec<_>>();
    let rows = batches.iter().map(RecordBatch::num_rows).sum();
    let count = batches.len();
    let mut batches = batches.into_iter().map(Ok).collect::<Vec<_>>();
    if error {
        batches.push(Err(arrow_schema::ArrowError::ParseError(
            "injected owned source failure".into(),
        )));
    }
    FlatLocalColumnarStreamSource {
        header: columns.clone(),
        column_dtypes: vec![None; columns.len()],
        column_arrow_dtypes: schema
            .fields()
            .iter()
            .map(|field| Some(field.data_type().clone()))
            .collect(),
        materialized_columns: columns.clone(),
        reader_projection_columns: columns,
        row_count_hint: Some(rows),
        record_batch_count_hint: Some(count),
        source_stream_batch_size: 3,
        source_stream_unit_count_hint: Some(count),
        source_stream_unit_row_ranges: None,
        source_stream_unit_hint_kind: "test_batches".into(),
        source_stream_policy: "test_source_batches".into(),
        source_dictionary_preservation_status: "source_flat_scalars".into(),
        ingest_executor_status: "serial_pull_reader".into(),
        ingest_executor_kind: "test_record_batch_reader".into(),
        ingest_executor_requested_parallelism: 1,
        ingest_executor_applied_parallelism: 1,
        ingest_executor_unit_count_hint: Some(count),
        source_identities: Vec::new(),
        embedded_derived_build_micros:
            crate::universal_format_io::new_embedded_derived_build_micros_counter(),
        reader: Box::new(RecordBatchIterator::new(batches, schema)),
    }
}

fn path(label: &str) -> PathBuf {
    std::env::temp_dir().join(format!(
        "shardloom-owned-ingest-{label}-{}-{}.vortex",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ))
}

#[test]
fn bounded_table_subtrees_round_trip_values_and_release_shared_memory() {
    // A timeout detects regressions in child EOF ordering. The source is tiny;
    // this is correctness/lifecycle verification, not a performance assertion.
    let (send, receive) = mpsc::channel();
    let worker = thread::spawn(move || {
        let path = path("roundtrip");
        let batches = (0..6).map(batch).collect::<Vec<_>>();
        let expected = batches
            .iter()
            .map(|batch| arrow_record_batch_to_vortex_array(batch.clone()).unwrap())
            .collect::<Vec<_>>();
        let report = write_flat_columnar_vortex_prepared_state_streaming(
            VortexPreparedStateColumnarStreamWriteRequest::new(&path, source(batches, false))
                .shared_native_memory_budget_bytes(32 << 20),
        )
        .unwrap();
        assert_eq!(report.row_count, 18);
        assert_eq!(
            report.vortex_encode_write_micros,
            report.vortex_segment_write_micros
        );
        assert!(
            report
                .writer_layout_strategy_applied
                .contains("bounded_source_batch_subtrees")
        );
        assert!(
            report
                .writer_coalescing_policy_status
                .contains("cross_batch_coalescing_disabled")
        );
        let ownership = report.shared_native_memory.unwrap();
        assert!(ownership.peak_reserved_bytes > 0);
        assert!(ownership.peak_reserved_bytes <= ownership.limit_bytes);
        assert_eq!(ownership.final_reserved_bytes, 0);
        let evidence = ownership
            .evidence_fields()
            .into_iter()
            .collect::<BTreeMap<_, _>>();
        assert_eq!(
            evidence["vortex_shared_native_memory_scope"],
            "copied_native_input_buffers;prefetch_admission;native_host_allocator;root_layout_references"
        );
        assert!(
            evidence["vortex_shared_native_memory_exclusions"]
                .contains("original_source_and_arrow_input_owners_until_conversion")
        );
        assert!(
            evidence["vortex_opaque_arrow_owner_policy"]
                .starts_with("all_imported_arrow_buffers_copy")
        );
        let runtime = vortex::io::runtime::current::CurrentThreadRuntime::new();
        let session = vortex::session::VortexSession::default().with_handle(runtime.handle());
        let file = runtime
            .block_on(session.open_options().open_path(&path))
            .unwrap();
        let mut offset = 0;
        for array in file
            .scan()
            .unwrap()
            .with_ordered(true)
            .into_array_iter(&runtime)
            .unwrap()
        {
            let array = array.unwrap();
            for row in 0..array.len() {
                assert_eq!(
                    array
                        .execute_scalar(row, &mut session.create_execution_ctx())
                        .unwrap(),
                    expected[offset / 3]
                        .execute_scalar(offset % 3, &mut session.create_execution_ctx())
                        .unwrap(),
                );
                offset += 1;
            }
        }
        assert_eq!(offset, 18);
        fs::remove_file(path).unwrap();
        send.send(()).unwrap();
    });
    receive
        .recv_timeout(Duration::from_secs(20))
        .expect("native child EOF/order must complete");
    worker.join().unwrap();
}

#[test]
fn owned_empty_stream_and_failures_preserve_output_atomicity() {
    let empty = path("empty");
    let report = write_flat_columnar_vortex_prepared_state_streaming(
        VortexPreparedStateColumnarStreamWriteRequest::new(&empty, source(Vec::new(), false))
            .shared_native_memory_budget_bytes(1 << 20),
    )
    .unwrap();
    assert_eq!(report.row_count, 0);
    assert_eq!(report.shared_native_memory.unwrap().final_reserved_bytes, 0);
    fs::remove_file(empty).unwrap();
    for (label, budget, error) in [("budget", 128, false), ("source", 32 << 20, true)] {
        let output = path(label);
        let result = write_flat_columnar_vortex_prepared_state_streaming(
            VortexPreparedStateColumnarStreamWriteRequest::new(
                &output,
                source(vec![batch(0)], error),
            )
            .shared_native_memory_budget_bytes(budget),
        );
        assert!(result.is_err());
        assert!(
            !output.exists(),
            "failed ingest must not publish an artifact"
        );
    }
}

#[test]
fn source_handoff_and_slice_lifetime_share_the_prefetch_pool() {
    for window in [0, 2] {
        let memory = NativeIngestMemory::new(16 << 20).unwrap();
        let first_batch = batch(0);
        let source_shape = FlatColumnarSourceShape {
            projected_columns: first_batch
                .schema()
                .fields()
                .iter()
                .enumerate()
                .map(|(index, field)| ColumnarProjectedColumn {
                    column: field.name().clone(),
                    reader_index: index,
                    dtype_hint: None,
                    arrow_dtype_hint: Some(field.data_type().clone()),
                })
                .collect(),
        };
        let timing = VortexStreamingIngestTiming::default();
        let mut lease = memory.reserve_input(1).unwrap();
        let first = record_batch_to_vortex_from_arrow_provider_profiled_with_memory(
            &first_batch,
            &source_shape,
            &timing.stages,
            Some((&memory, &mut lease)),
        )
        .unwrap();
        drop((first_batch, lease));
        let columns = source_shape
            .projected_columns
            .iter()
            .map(|field| field.column.clone())
            .collect();
        let mut iterator = StreamingColumnarVortexArrayIterator::new(
            first.dtype().clone(),
            first,
            Box::new(RecordBatchIterator::new(
                vec![Ok(batch(1)), Ok(batch(2))],
                batch(0).schema(),
            )),
            columns,
            source_shape,
            Arc::new(AtomicUsize::new(1)),
            timing,
            1,
            window,
            window,
            4 << 20,
            Some(memory.clone()),
        )
        .unwrap();
        drop(iterator.next().unwrap().unwrap());
        let retained = iterator.next().unwrap().unwrap().slice(1..2).unwrap();
        for array in &mut iterator {
            drop(array.unwrap());
        }
        drop(iterator);
        assert!(memory.pool.snapshot().reserved_bytes > 0);
        drop(retained);
        assert_eq!(memory.pool.snapshot().reserved_bytes, 0);
    }
}

fn check_owned_batch_hints(
    label: &str,
    batches: Vec<RecordBatch>,
    row_hint: Option<usize>,
    batch_hint: Option<usize>,
) {
    let output = path(label);
    let expected = batches
        .iter()
        .filter(|batch| batch.num_rows() != 0)
        .map(|batch| arrow_record_batch_to_vortex_array(batch.clone()).unwrap())
        .collect::<Vec<_>>();
    let expected_rows = expected
        .iter()
        .map(vortex::array::ArrayRef::len)
        .sum::<usize>();
    let mut input = source(batches, false);
    input.row_count_hint = row_hint;
    input.record_batch_count_hint = batch_hint;
    let report = write_flat_columnar_vortex_prepared_state_streaming(
        VortexPreparedStateColumnarStreamWriteRequest::new(&output, input)
            .shared_native_memory_budget_bytes(32 << 20),
    )
    .unwrap();
    assert_eq!(report.row_count, u64::try_from(expected_rows).unwrap());
    assert_eq!(report.reopen_row_count, report.row_count);
    let ownership = report.shared_native_memory.unwrap();
    assert_eq!(ownership.final_reserved_bytes, 0);
    assert_eq!(ownership.denied_reservations, 0);
    assert!(ownership.peak_reserved_bytes <= ownership.limit_bytes);
    assert_eq!(
        ownership.max_source_batches,
        (32 << 20) / std::mem::size_of::<vortex::layout::LayoutRef>()
    );
    let runtime = vortex::io::runtime::current::CurrentThreadRuntime::new();
    let session = vortex::session::VortexSession::default().with_handle(runtime.handle());
    let file = runtime
        .block_on(session.open_options().open_path(&output))
        .unwrap();
    let mut reference = expected
        .iter()
        .flat_map(|array| (0..array.len()).map(move |row| (array, row)));
    let mut seen = 0;
    for array in file
        .scan()
        .unwrap()
        .with_ordered(true)
        .into_array_iter(&runtime)
        .unwrap()
    {
        let array = array.unwrap();
        for row in 0..array.len() {
            let (expected, expected_row) = reference.next().expect("no extra output row");
            assert_eq!(
                array
                    .execute_scalar(row, &mut session.create_execution_ctx())
                    .unwrap(),
                expected
                    .execute_scalar(expected_row, &mut session.create_execution_ctx())
                    .unwrap(),
            );
            seen += 1;
        }
    }
    assert!(reference.next().is_none());
    assert_eq!(seen, expected_rows);
    drop(file);
    fs::remove_file(output).unwrap();
}

#[test]
fn owned_batch_admission_ignores_row_and_batch_count_hints() {
    let one = batch(19).slice(0, 1);
    let empty = RecordBatch::new_empty(one.schema());
    check_owned_batch_hints(
        "one-then-empty",
        vec![one.clone(), empty.clone()],
        Some(1),
        Some(2),
    );
    check_owned_batch_hints("empty-then-one", vec![empty, one], Some(1), Some(2));
    for (label, hint) in [
        ("no-row-hint", Some(3)),
        ("no-count-hints", None),
        ("underreported-batches", Some(0)),
        ("oversized-batch-hint", Some(usize::MAX)),
    ] {
        check_owned_batch_hints(label, vec![batch(1), batch(2), batch(3)], None, hint);
    }
}

#[test]
fn owned_all_empty_and_unhinted_empty_streams_reopen_without_rows() {
    let empty = RecordBatch::new_empty(batch(0).schema());
    check_owned_batch_hints("all-empty", vec![empty.clone(), empty], Some(0), Some(2));
    check_owned_batch_hints("unhinted-no-input", Vec::new(), None, None);
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum PipelineEnd {
    Complete,
    SourceError,
    ConversionError,
    Cancel,
    DestinationAppeared,
}

struct GatedPipelineReader {
    schema: arrow_schema::SchemaRef,
    initial: VecDeque<std::result::Result<RecordBatch, arrow_schema::ArrowError>>,
    final_batch: Option<std::result::Result<RecordBatch, arrow_schema::ArrowError>>,
    blocked: mpsc::Sender<()>,
    release: mpsc::Receiver<()>,
    dropped: Arc<AtomicUsize>,
}

impl Iterator for GatedPipelineReader {
    type Item = std::result::Result<RecordBatch, arrow_schema::ArrowError>;

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(batch) = self.initial.pop_front() {
            return Some(batch);
        }
        let final_batch = self.final_batch.take()?;
        self.blocked.send(()).unwrap();
        self.release
            .recv_timeout(Duration::from_secs(20))
            .expect("test must release the blocked source pull");
        Some(final_batch)
    }
}

impl arrow_array::RecordBatchReader for GatedPipelineReader {
    fn schema(&self) -> arrow_schema::SchemaRef {
        Arc::clone(&self.schema)
    }
}

impl Drop for GatedPipelineReader {
    fn drop(&mut self) {
        self.dropped.fetch_add(1, Ordering::SeqCst);
    }
}

struct ObservedPipelineIterator {
    inner: StreamingColumnarVortexArrayIterator,
    entered_writer: Option<mpsc::Sender<()>>,
}

impl Iterator for ObservedPipelineIterator {
    type Item = vortex::error::VortexResult<vortex::array::ArrayRef>;

    fn next(&mut self) -> Option<Self::Item> {
        let next = self.inner.next();
        if next.as_ref().is_some_and(std::result::Result::is_ok)
            && let Some(entered) = self.entered_writer.take()
        {
            entered.send(()).unwrap();
        }
        next
    }
}

impl vortex::array::iter::ArrayIterator for ObservedPipelineIterator {
    fn dtype(&self) -> &vortex::array::dtype::DType {
        &self.inner.dtype
    }
}

fn assert_pipeline_complete_values(output: &Path) {
    let expected = [batch(0), batch(1).slice(0, 1)]
        .into_iter()
        .map(|batch| arrow_record_batch_to_vortex_array(batch).unwrap())
        .collect::<Vec<_>>();
    let runtime = vortex::io::runtime::current::CurrentThreadRuntime::new();
    let session = vortex::session::VortexSession::default().with_handle(runtime.handle());
    let file = runtime
        .block_on(session.open_options().open_path(output))
        .unwrap();
    let mut reference = expected
        .iter()
        .flat_map(|array| (0..array.len()).map(move |row| (array, row)));
    let mut rows = 0;
    for array in file
        .scan()
        .unwrap()
        .with_ordered(true)
        .into_array_iter(&runtime)
        .unwrap()
    {
        let array = array.unwrap();
        for row in 0..array.len() {
            let (expected, expected_row) = reference.next().expect("no extra row");
            assert_eq!(
                array
                    .execute_scalar(row, &mut session.create_execution_ctx())
                    .unwrap(),
                expected
                    .execute_scalar(expected_row, &mut session.create_execution_ctx())
                    .unwrap()
            );
            rows += 1;
        }
    }
    assert_eq!(rows, 4);
    assert!(reference.next().is_none());
}

#[allow(clippy::too_many_lines)]
fn check_streaming_pipeline_end(grant: usize, end: PipelineEnd, parallel_codec: bool) {
    let directory =
        path(&format!("pipeline-{grant}-{end:?}-codec-{parallel_codec}")).with_extension("dir");
    fs::create_dir(&directory).unwrap();
    let output = directory.join("out.vortex");
    let worker_output = output.clone();
    let (blocked, blocked_rx) = mpsc::channel();
    let (release, released) = mpsc::channel();
    let (entered_writer, entered_rx) = mpsc::channel();
    let (ready, ready_rx) = mpsc::channel();
    let (finished, finished_rx) = mpsc::channel();
    let dropped = Arc::new(AtomicUsize::new(0));
    let worker_dropped = Arc::clone(&dropped);
    let worker = thread::spawn(move || {
        let final_batch = match end {
            PipelineEnd::SourceError => Err(arrow_schema::ArrowError::ParseError(
                "pipeline primary source failure".into(),
            )),
            PipelineEnd::ConversionError => {
                let valid = batch(1);
                let mut columns = valid.columns().to_vec();
                columns[3] = Arc::new(Float64Array::from(vec![Some(f64::NAN), None, Some(0.0)]));
                Ok(RecordBatch::try_new(valid.schema(), columns).unwrap())
            }
            _ => Ok(batch(1).slice(0, 1)),
        };
        let mut input = source(vec![batch(0), batch(1)], false);
        input.row_count_hint = Some(4);
        input.reader = Box::new(GatedPipelineReader {
            schema: batch(0).schema(),
            initial: VecDeque::from([Ok(batch(0)), Ok(RecordBatch::new_empty(batch(0).schema()))]),
            final_batch: Some(final_batch),
            blocked,
            release: released,
            dropped: worker_dropped,
        });
        let mut input = crate::universal_format_io::with_capillary_prefetch_columnar_stream_source(
            input, grant,
        );
        let shape = validate_flat_columnar_stream_source_shape(&input).unwrap();
        let advisor = parallel_codec.then(|| {
            // Select the existing large-source codec policy on this tiny
            // fixture. The actual streaming writer still verifies four rows.
            let mut advice = super::tests::layout_advisor_input(true, "none");
            advice.source_format = "parquet".to_string();
            advice.writer_provider_kind = "vortex_array_kernel".to_string();
            advice.writer_provider_surface =
                "ArrayRef::from_arrow(RecordBatch);streaming ArrayIterator;VortexSession::write_options().write(ArrayStream)".to_string();
            advice.row_count = VORTEX_PREPARED_OLAP_WRITER_LARGE_SOURCE_ROW_THRESHOLD;
            advice.writer_parallelism_budget = grant;
            advice.writer_compression_candidate_fields = vec!["renamed_text".to_string()];
            evaluate_vortex_layout_write_advisor(advice)
        });
        let decision = admit_layout_write_runtime_decision_for_source(
            advisor.as_ref(),
            "vortex_array_kernel",
            "ArrayRef::from_arrow(RecordBatch);streaming ArrayIterator",
            &worker_output,
            VortexIngestCertificationLevel::IngestCertified,
            VortexWriterPhysicalDesignSourceInput::streaming_columnar(&input).unwrap(),
        )
        .unwrap();
        if parallel_codec {
            assert_eq!(grant, 4);
            let design = &decision.writer_physical_design;
            assert_eq!(input.ingest_executor_applied_parallelism, 1);
            assert_eq!(design.array_build_worker_count, 1);
            assert_eq!(design.array_build_prefetch_window, 3);
            assert_eq!(design.writer_runtime_background_workers, 1);
            assert_eq!(design.writer_compression_concurrency, 4);
            assert!(!design.fallback_attempted);
            assert!(!design.external_engine_invoked);
        }
        let memory = NativeIngestMemory::new(32 << 20).unwrap();
        let timing = VortexStreamingIngestTiming::default();
        let first_batch = input.reader.next().unwrap().unwrap();
        let mut lease = memory.reserve_input(1).unwrap();
        let first = record_batch_to_vortex_from_arrow_provider_profiled_with_memory(
            &first_batch,
            &shape,
            &timing.stages,
            Some((&memory, &mut lease)),
        )
        .unwrap();
        drop((first_batch, lease));
        let iterator = StreamingColumnarVortexArrayIterator::new(
            first.dtype().clone(),
            first,
            input.reader,
            input.reader_projection_columns,
            shape,
            Arc::new(AtomicUsize::new(1)),
            timing,
            2,
            decision.writer_physical_design.array_build_prefetch_window,
            decision.writer_physical_design.array_build_worker_count,
            8 << 20,
            Some(memory.clone()),
        )
        .unwrap();
        let conversion_owner = iterator
            .prefetch
            .as_ref()
            .map(|prefetch| Arc::downgrade(&prefetch.context));
        ready
            .send(
                iterator
                    .prefetch
                    .as_ref()
                    .map(|prefetch| prefetch.cancellation.clone()),
            )
            .unwrap();
        // Use the production bounded subtree writer and validated staging
        // publication. Keeping the existing pool handle makes failure cleanup
        // observable without adding a runtime-only test hook.
        let result = write_vortex_array_iterator(
            &worker_output,
            ObservedPipelineIterator {
                inner: iterator,
                entered_writer: Some(entered_writer),
            },
            false,
            &decision,
            Some(4),
            Some(&memory),
            &[],
        );
        let snapshot = memory.pool.snapshot();
        assert!(snapshot.peak_reserved_bytes > 0);
        assert!(snapshot.peak_reserved_bytes <= snapshot.limit_bytes);
        assert_eq!(snapshot.reserved_bytes, 0);
        assert!(conversion_owner.is_none_or(|owner| owner.upgrade().is_none()));
        if end == PipelineEnd::Complete {
            if parallel_codec {
                let report = result.as_ref().unwrap();
                assert_eq!(report.writer_runtime_background_workers, 1);
                assert_eq!(report.writer_compression_concurrency, 4);
                assert_eq!(report.writer_row_count, 4);
            }
            assert_pipeline_complete_values(&worker_output);
        }
        finished
            .send(result.map(|_| ()).map_err(|error| error.to_string()))
            .unwrap();
    });
    let cancellation = ready_rx.recv_timeout(Duration::from_secs(20)).unwrap();
    entered_rx.recv_timeout(Duration::from_secs(20)).unwrap();
    blocked_rx.recv_timeout(Duration::from_secs(20)).unwrap();
    let unpublished = !output.exists();
    let staging_exists = fs::read_dir(&directory).unwrap().count() == 1;
    if end == PipelineEnd::Cancel {
        cancellation.expect("parallel conversion owner").cancel();
    } else if end == PipelineEnd::DestinationAppeared {
        fs::write(&output, b"foreign destination").unwrap();
    }
    // Release simulated blocking I/O before asserting. Cancellation is
    // cooperative once the source read returns; it does not interrupt I/O.
    release.send(()).unwrap();
    let result = finished_rx.recv_timeout(Duration::from_secs(20)).unwrap();
    worker.join().unwrap();
    assert!(unpublished, "publication must wait for the complete source");
    assert!(staging_exists, "the real writer must have entered staging");
    assert_eq!(dropped.load(Ordering::SeqCst), 1);
    match end {
        PipelineEnd::Complete => result.unwrap(),
        PipelineEnd::SourceError => assert!(
            result
                .unwrap_err()
                .contains("pipeline primary source failure")
        ),
        PipelineEnd::ConversionError => assert!(result.unwrap_err().contains("non-finite")),
        PipelineEnd::Cancel => assert!(result.unwrap_err().contains("execution cancelled")),
        PipelineEnd::DestinationAppeared => {
            assert!(result.unwrap_err().contains("appeared before commit"));
            assert_eq!(fs::read(&output).unwrap(), b"foreign destination");
        }
    }
    let remaining = fs::read_dir(&directory)
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .collect::<Vec<_>>();
    if matches!(
        end,
        PipelineEnd::Complete | PipelineEnd::DestinationAppeared
    ) {
        assert_eq!(remaining, vec![output]);
    } else {
        assert!(remaining.is_empty(), "failed pipeline must remove staging");
    }
    fs::remove_dir_all(directory).unwrap();
}

#[test]
fn streaming_pipeline_final_partial_and_empty_batches_wait_for_complete_publication() {
    for (grant, parallel_codec) in [(1, false), (4, false), (4, true)] {
        check_streaming_pipeline_end(grant, PipelineEnd::Complete, parallel_codec);
    }
}

#[test]
fn streaming_pipeline_primary_failures_join_sources_and_release_owned_memory() {
    for (grant, parallel_codec) in [(1, false), (4, false), (4, true)] {
        for end in [PipelineEnd::SourceError, PipelineEnd::ConversionError] {
            check_streaming_pipeline_end(grant, end, parallel_codec);
        }
    }
}

#[test]
fn streaming_pipeline_cancellation_drains_after_blocked_source_returns() {
    // Both retained P4 writer profiles have the existing conversion-prefetch
    // owner and token. Neither token interrupts a blocking source I/O call.
    for parallel_codec in [false, true] {
        check_streaming_pipeline_end(4, PipelineEnd::Cancel, parallel_codec);
    }
}

#[test]
fn streaming_pipeline_concurrent_destination_is_preserved_and_staging_released() {
    for parallel_codec in [false, true] {
        check_streaming_pipeline_end(4, PipelineEnd::DestinationAppeared, parallel_codec);
    }
}
