use super::*;
use arrow_array::{BooleanArray, Float64Array, Int64Array, RecordBatchIterator, StringArray};
use arrow_schema::{DataType, Field, Schema};
use std::{sync::mpsc, thread, time::Duration};
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
