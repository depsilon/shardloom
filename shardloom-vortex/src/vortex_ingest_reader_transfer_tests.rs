//! Actual reader provenance, complete writer output and refusal boundaries.
use super::*;
use arrow_array::{Int64Array, RecordBatchIterator, StringArray};
use arrow_schema::{DataType, Field, Schema};
use parquet::{arrow::ArrowWriter, file::properties::WriterProperties};
use vortex::{
    VortexSessionDefault as _,
    array::VortexSessionExecute as _,
    file::OpenOptionsSessionExt as _,
    io::{runtime::BlockingRuntime as _, session::RuntimeSessionExt as _},
};

struct Fixture(PathBuf);

impl Fixture {
    fn new(label: &str) -> Self {
        let root = std::env::temp_dir().join(format!(
            "shardloom-reader-transfer-{label}-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos(),
        ));
        fs::create_dir_all(&root).unwrap();
        let batch = input_batch();
        let mut writer = ArrowWriter::try_new(
            fs::File::create(root.join("source.parquet")).unwrap(),
            batch.schema(),
            Some(
                WriterProperties::builder()
                    .set_max_row_group_row_count(Some(256))
                    .build(),
            ),
        )
        .unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
        Self(root)
    }

    fn source(&self) -> PathBuf {
        self.0.join("source.parquet")
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        fs::remove_dir_all(&self.0).unwrap();
    }
}

fn input_batch() -> RecordBatch {
    RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("text", DataType::Utf8, true),
        ])),
        vec![
            Arc::new(Int64Array::from(
                (0..4096_i64)
                    .map(|row| (1_i64 << 60) + row)
                    .collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from_iter((0..4096).map(|row| {
                (row % 11 != 0).then(|| format!("row {row}: λ😀 text"))
            }))),
        ],
    )
    .unwrap()
}

fn resources(parallelism: usize) -> shardloom_core::ExecutionResources {
    shardloom_core::ExecutionResources::from_gib(
        1,
        parallelism,
        shardloom_core::ExecutionResourceOrigin::ExecutionCall,
    )
    .unwrap()
}

fn ownership(report: &VortexPreparedStateWriteReport, field: &str) -> u64 {
    report
        .stage_work
        .evidence_fields()
        .into_iter()
        .collect::<BTreeMap<_, _>>()[&format!("vortex_ingest_ownership_{field}")]
        .parse()
        .unwrap()
}

fn check_complete_values(path: &Path) {
    let runtime = vortex::io::runtime::current::CurrentThreadRuntime::new();
    let session = vortex::session::VortexSession::default().with_handle(runtime.handle());
    let expected = arrow_record_batch_to_vortex_array(input_batch()).unwrap();
    let file = runtime
        .block_on(session.open_options().open_path(path))
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
                expected
                    .execute_scalar(offset, &mut session.create_execution_ctx())
                    .unwrap(),
            );
            offset += 1;
        }
    }
    assert_eq!(offset, 4096);
}

#[test]
fn controlled_parquet_ingest_transfers_with_identical_complete_output() {
    for parallelism in [1, 4] {
        let fixture = Fixture::new(&format!("output-p{parallelism}"));
        let cancellation = CancellationToken::default();
        let source = crate::universal_format_io::stream_flat_parquet_columnar_source_with_batch_budget_controlled(
            &fixture.source(), 4096, parallelism, Some(16 << 10), &cancellation,
        ).unwrap();
        let source =
            crate::universal_format_io::with_capillary_prefetch_columnar_stream_source_controlled(
                source,
                parallelism,
                &cancellation,
            )
            .unwrap();
        let control = fixture.0.join("control.vortex");
        let control_report = write_flat_columnar_vortex_prepared_state_streaming(
            VortexPreparedStateColumnarStreamWriteRequest::new(
                &control,
                source,
                resources(parallelism),
            )
            .shared_native_memory_budget_bytes(32 << 20),
        )
        .unwrap();
        assert_eq!(ownership(&control_report, "transferred_buffer_bytes"), 0);
        assert!(ownership(&control_report, "copied_buffer_bytes") > 0);

        let source = IngestColumnarStreamSource::parquet(
            &fixture.source(),
            4096,
            parallelism,
            Some(16 << 10),
            &cancellation,
        )
        .unwrap()
        .with_prefetch(parallelism, &cancellation)
        .unwrap();
        let candidate = fixture.0.join("candidate.vortex");
        let memory = LiveMemoryPool::new(32 << 20).unwrap();
        let candidate_report = write_flat_columnar_vortex_prepared_state_streaming(
            VortexPreparedStateColumnarStreamWriteRequest::new(
                &candidate,
                source,
                resources(parallelism),
            )
            .shared_native_memory_pool(memory.clone()),
        )
        .unwrap();
        assert_eq!(candidate_report.row_count, 4096);
        assert!(candidate_report.array_build_record_batch_count > 1);
        assert!(ownership(&candidate_report, "transferred_buffer_bytes") > 0);
        assert!(
            ownership(&candidate_report, "copied_buffer_bytes")
                < ownership(&control_report, "copied_buffer_bytes")
        );
        assert_eq!(memory.snapshot().reserved_bytes, 0);
        assert_eq!(memory.snapshot().denied_reservations, 0);
        assert_eq!(fs::read(&control).unwrap(), fs::read(&candidate).unwrap());
        check_complete_values(&candidate);
    }
}

#[test]
fn replacement_reader_with_parquet_metadata_cannot_claim_transfer() {
    let fixture = Fixture::new("replacement-reader");
    let cancellation = CancellationToken::default();
    let mut source = crate::universal_format_io::stream_flat_parquet_columnar_source_with_batch_budget_controlled(
        &fixture.source(), 4096, 1, Some(16 << 10), &cancellation,
    ).unwrap();
    let batch = input_batch();
    // Leave all real-Parquet metadata labels and file identities intact while
    // replacing the publicly mutable reader with arbitrary external owners.
    source.reader = Box::new(RecordBatchIterator::new(
        vec![Ok(batch)],
        input_batch().schema(),
    ));
    let source = IngestColumnarStreamSource::from(source)
        .with_source_native_embedded_derived_columns()
        .with_prefetch(1, &cancellation)
        .unwrap();
    let output = fixture.0.join("external.vortex");
    let report = write_flat_columnar_vortex_prepared_state_streaming(
        VortexPreparedStateColumnarStreamWriteRequest::new(&output, source, resources(1))
            .shared_native_memory_budget_bytes(32 << 20),
    )
    .unwrap();
    assert_eq!(ownership(&report, "transferred_buffer_bytes"), 0);
    assert!(ownership(&report, "copied_buffer_bytes") > 0);
    check_complete_values(&output);
}

#[test]
fn controlled_reader_cancellation_and_source_change_preserve_existing_output() {
    use std::io::Write as _;
    for change_source in [false, true] {
        let fixture = Fixture::new(if change_source {
            "changed"
        } else {
            "cancelled"
        });
        let cancellation = CancellationToken::default();
        let source = IngestColumnarStreamSource::parquet(
            &fixture.source(),
            4096,
            4,
            Some(16 << 10),
            &cancellation,
        )
        .unwrap()
        .with_prefetch(4, &cancellation)
        .unwrap();
        let output = fixture.0.join("existing.vortex");
        fs::write(&output, b"existing complete output").unwrap();
        if change_source {
            fs::OpenOptions::new()
                .append(true)
                .open(fixture.source())
                .unwrap()
                .write_all(b"changed")
                .unwrap();
        } else {
            cancellation.cancel();
        }
        let memory = LiveMemoryPool::new(32 << 20).unwrap();
        let error = write_flat_columnar_vortex_prepared_state_streaming_controlled(
            VortexPreparedStateColumnarStreamWriteRequest::new(&output, source, resources(4))
                .shared_native_memory_pool(memory.clone())
                .allow_overwrite(true),
            &cancellation,
        )
        .unwrap_err();
        assert!(if change_source {
            error.to_string().contains("changed")
        } else {
            error.to_string().contains("cancel")
        });
        assert_eq!(fs::read(output).unwrap(), b"existing complete output");
        assert_eq!(memory.snapshot().reserved_bytes, 0);
    }
}
