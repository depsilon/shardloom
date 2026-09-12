//! Full source/conversion/writer admission and publication failure checks.
//! Original fixture/read buffers and provider allocations outside the native
//! allocator are not a process-memory claim. No timing here is a benchmark.

use super::*;
use arrow_array::{BooleanArray, Int64Array, StringArray};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use shardloom_exec::live_memory::LiveMemorySnapshot;
use std::{sync::mpsc, thread, time::Duration};
use vortex::{
    VortexSessionDefault as _, array::VortexSessionExecute as _, file::OpenOptionsSessionExt as _,
    io::runtime::BlockingRuntime as _, io::session::RuntimeSessionExt as _,
};

struct FixtureDirectory(PathBuf);

impl FixtureDirectory {
    fn new(label: &str) -> Self {
        let root = std::env::temp_dir().join(format!(
            "shardloom-ingest-pressure-{label}-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        fs::create_dir(&root).unwrap();
        Self(root)
    }
}

impl Drop for FixtureDirectory {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

fn bounded_completion(work: impl FnOnce() + Send + 'static) {
    let (done, completion) = mpsc::channel();
    let worker = thread::spawn(move || {
        work();
        done.send(()).unwrap();
    });
    completion
        .recv_timeout(Duration::from_secs(30))
        .expect("bounded fixture must drain its source/conversion/writer owners");
    worker.join().unwrap();
}

fn schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("renamed_extreme", DataType::Int64, true),
        Field::new("renamed_payload", DataType::Utf8, true),
        Field::new("renamed_flag", DataType::Boolean, true),
    ]))
}

fn batch(start: usize, rows: usize, text_bytes: usize) -> RecordBatch {
    let numbers = (start..start + rows)
        .map(|row| {
            if row % 11 == 0 {
                None
            } else if row % 2 == 0 {
                Some(i64::MAX - i64::try_from(row).unwrap())
            } else {
                Some(i64::MIN + i64::try_from(row).unwrap())
            }
        })
        .collect::<Vec<_>>();
    let strings = (start..start + rows)
        .map(|row| (row % 5 != 0).then(|| format!("{row}:{}", "λ".repeat(text_bytes / 2))))
        .collect::<Vec<_>>();
    let flags = (start..start + rows)
        .map(|row| (row % 7 != 0).then_some(row % 2 == 0))
        .collect::<Vec<_>>();
    RecordBatch::try_new(
        schema(),
        vec![
            Arc::new(Int64Array::from(numbers)),
            Arc::new(StringArray::from(strings)),
            Arc::new(BooleanArray::from(flags)),
        ],
    )
    .unwrap()
}

fn write_parquet(path: &Path, batches: &[RecordBatch]) {
    let properties = parquet::file::properties::WriterProperties::builder()
        .set_compression(parquet::basic::Compression::UNCOMPRESSED)
        .set_dictionary_enabled(false)
        .set_data_page_size_limit(1 << 20)
        .build();
    let mut writer = parquet::arrow::ArrowWriter::try_new(
        fs::File::create(path).unwrap(),
        schema(),
        Some(properties),
    )
    .unwrap();
    for batch in batches {
        writer.write(batch).unwrap();
        writer.flush().unwrap();
    }
    writer.close().unwrap();
}

fn write_ipc(path: &Path, batches: &[RecordBatch]) {
    let mut writer =
        arrow_ipc::writer::FileWriter::try_new(fs::File::create(path).unwrap(), schema().as_ref())
            .unwrap();
    for batch in batches {
        writer.write(batch).unwrap();
    }
    writer.finish().unwrap();
}

struct ObservedSource {
    inner: Box<dyn arrow_array::RecordBatchReader + Send>,
    batch_bytes: Arc<Mutex<Vec<usize>>>,
    dropped: Arc<AtomicUsize>,
}

impl Iterator for ObservedSource {
    type Item = std::result::Result<RecordBatch, arrow_schema::ArrowError>;

    fn next(&mut self) -> Option<Self::Item> {
        let item = self.inner.next();
        if let Some(Ok(batch)) = &item {
            self.batch_bytes
                .lock()
                .unwrap()
                .push(batch.get_array_memory_size());
        }
        item
    }
}

impl arrow_array::RecordBatchReader for ObservedSource {
    fn schema(&self) -> SchemaRef {
        self.inner.schema()
    }
}

impl Drop for ObservedSource {
    fn drop(&mut self) {
        self.dropped.fetch_add(1, Ordering::SeqCst);
    }
}

struct PressureIterator {
    inner: StreamingColumnarVortexArrayIterator,
    memory: LiveMemoryPool,
    exhaust_after_first: bool,
    pressure: Option<MemoryLease>,
    pressure_applied: Arc<AtomicUsize>,
}

impl Iterator for PressureIterator {
    type Item = vortex::error::VortexResult<vortex::array::ArrayRef>;

    fn next(&mut self) -> Option<Self::Item> {
        let item = self.inner.next();
        if self.exhaust_after_first && item.as_ref().is_some_and(std::result::Result::is_ok) {
            // This case runs at P1: no concurrent native owner can race the
            // snapshot. Inject contention in the existing shared pool only
            // after the actual writer consumes its first owned array.
            let snapshot = self.memory.snapshot();
            self.pressure = Some(
                self.memory
                    .reserve(snapshot.limit_bytes - snapshot.reserved_bytes)
                    .unwrap(),
            );
            self.pressure_applied.fetch_add(1, Ordering::SeqCst);
            self.exhaust_after_first = false;
        }
        item
    }
}

impl vortex::array::iter::ArrayIterator for PressureIterator {
    fn dtype(&self) -> &vortex::array::dtype::DType {
        &self.inner.dtype
    }
}

struct WriteOptions {
    grant: usize,
    expected_rows: u64,
    memory_bytes: u64,
    codec: bool,
    overwrite: bool,
    exhaust_after_first: bool,
}

impl WriteOptions {
    fn new(grant: usize, expected_rows: u64) -> Self {
        Self {
            grant,
            expected_rows,
            memory_bytes: 64 << 20,
            codec: false,
            overwrite: false,
            exhaust_after_first: false,
        }
    }
}

struct WriteObservation {
    result: Result<u64>,
    memory: LiveMemorySnapshot,
    batch_bytes: Vec<usize>,
    stages: BTreeMap<String, String>,
    pressure_applied: usize,
}

#[allow(clippy::too_many_lines)]
fn write_observed(
    mut input: FlatLocalColumnarStreamSource,
    output: &Path,
    options: &WriteOptions,
) -> WriteObservation {
    let batch_bytes = Arc::new(Mutex::new(Vec::new()));
    let dropped = Arc::new(AtomicUsize::new(0));
    input.reader = Box::new(ObservedSource {
        inner: input.reader,
        batch_bytes: Arc::clone(&batch_bytes),
        dropped: Arc::clone(&dropped),
    });
    let mut input = crate::universal_format_io::with_capillary_prefetch_columnar_stream_source(
        input,
        options.grant,
    );
    let source_identities = input.source_identities.clone();
    let shape = validate_flat_columnar_stream_source_shape(&input).unwrap();
    let advisor = options.codec.then(|| {
        let mut advice = super::tests::layout_advisor_input(true, "none");
        advice.writer_provider_kind = "vortex_array_kernel".into();
        advice.writer_provider_surface =
            "ArrayRef::from_arrow(RecordBatch);streaming ArrayIterator;VortexSession::write_options().write(ArrayStream)".into();
        advice.row_count = VORTEX_PREPARED_OLAP_WRITER_LARGE_SOURCE_ROW_THRESHOLD;
        advice.writer_parallelism_budget = options.grant;
        advice.writer_compression_candidate_fields = vec!["renamed_payload".into()];
        evaluate_vortex_layout_write_advisor(advice)
    });
    let decision = admit_layout_write_runtime_decision_for_source(
        advisor.as_ref(),
        "vortex_array_kernel",
        "ArrayRef::from_arrow(RecordBatch);streaming ArrayIterator",
        output,
        VortexIngestCertificationLevel::IngestCertified,
        VortexWriterPhysicalDesignSourceInput::streaming_columnar(&input).unwrap(),
    )
    .unwrap();
    let memory = NativeIngestMemory::new(options.memory_bytes).unwrap();
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
        timing.clone(),
        2,
        decision.writer_physical_design.array_build_prefetch_window,
        decision.writer_physical_design.array_build_worker_count,
        options.memory_bytes / 4,
        Some(memory.clone()),
    )
    .unwrap();
    let conversion_owner = iterator
        .prefetch
        .as_ref()
        .map(|prefetch| Arc::downgrade(&prefetch.context));
    if options.exhaust_after_first {
        assert_eq!(options.grant, 1);
        assert!(iterator.prefetch.is_none());
    }
    let pressure_applied = Arc::new(AtomicUsize::new(0));
    let result = write_vortex_array_iterator(
        output,
        PressureIterator {
            inner: iterator,
            memory: memory.pool.clone(),
            exhaust_after_first: options.exhaust_after_first,
            pressure: None,
            pressure_applied: Arc::clone(&pressure_applied),
        },
        options.overwrite,
        &decision,
        Some(options.expected_rows),
        Some(&memory),
        &source_identities,
    );
    let snapshot = memory.pool.snapshot();
    assert!(snapshot.peak_reserved_bytes > 0);
    assert!(snapshot.peak_reserved_bytes <= snapshot.limit_bytes);
    assert_eq!(snapshot.reserved_bytes, 0);
    assert_eq!(dropped.load(Ordering::SeqCst), 1);
    assert!(conversion_owner.is_none_or(|owner| owner.upgrade().is_none()));
    let mut stages = BTreeMap::new();
    let result = result.map(|result| {
        stages = result
            .stage_work
            .with_stream(&timing.stages.snapshot())
            .evidence_fields()
            .into_iter()
            .collect();
        result.writer_row_count
    });
    let batch_bytes = batch_bytes.lock().unwrap().clone();
    WriteObservation {
        result,
        memory: snapshot,
        batch_bytes,
        stages,
        pressure_applied: pressure_applied.load(Ordering::SeqCst),
    }
}

fn assert_complete_values(output: &Path, batches: &[RecordBatch]) {
    let expected = batches
        .iter()
        .cloned()
        .map(|batch| arrow_record_batch_to_vortex_array(batch).unwrap())
        .collect::<Vec<_>>();
    let runtime = vortex::io::runtime::current::CurrentThreadRuntime::new();
    let session = vortex::session::VortexSession::default().with_handle(runtime.handle());
    let file = runtime
        .block_on(session.open_options().open_path(output))
        .unwrap();
    let expected_schema =
        arrow_record_batch_to_vortex_array(RecordBatch::new_empty(schema())).unwrap();
    assert_eq!(file.dtype(), expected_schema.dtype());
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
            let (expected, expected_row) = reference.next().expect("no additional row");
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
    assert!(reference.next().is_none());
    assert_eq!(
        rows,
        batches.iter().map(RecordBatch::num_rows).sum::<usize>()
    );
}

fn assert_files(directory: &Path, expected: &[&Path]) {
    let mut actual = fs::read_dir(directory)
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .collect::<Vec<_>>();
    let mut expected = expected
        .iter()
        .map(|path| path.to_path_buf())
        .collect::<Vec<_>>();
    actual.sort();
    expected.sort();
    assert_eq!(
        actual, expected,
        "owned staging must not survive completion"
    );
}

#[test]
fn streaming_pressure_skewed_parquet_pages_preserve_complete_values_and_release_credits() {
    bounded_completion(|| {
        let directory = FixtureDirectory::new("parquet-skew");
        // Hold the independent original fixture while reader/native buffers are
        // live. Its memory is intentionally outside the native ownership pool.
        let batches = vec![batch(0, 3, 8), batch(3, 19, 65_536), batch(22, 2, 2)];
        let input = directory.0.join("input.parquet");
        write_parquet(&input, &batches);
        for grant in [1, 4] {
            for codec in [false, true] {
                let source = crate::universal_format_io::stream_flat_parquet_columnar_source_with_batch_budget(
                    &input, 24, grant, Some(256 << 10),
                ).unwrap();
                let output = directory.0.join(format!("skew-{grant}-{codec}.vortex"));
                let mut options = WriteOptions::new(grant, 24);
                options.codec = codec;
                let observed = write_observed(source, &output, &options);
                assert_eq!(observed.result.unwrap(), 24);
                assert_eq!(observed.memory.denied_reservations, 0);
                let minimum = *observed.batch_bytes.iter().min().unwrap();
                let maximum = *observed.batch_bytes.iter().max().unwrap();
                assert!(
                    maximum > minimum * 8,
                    "fixture must exercise actual decoded-batch skew"
                );
                assert!(
                    observed.stages["vortex_ingest_stream_arrow_conversion_input_bytes"]
                        .parse::<u64>()
                        .unwrap()
                        > 512 << 10
                );
                if codec {
                    assert!(
                        observed.stages["vortex_ingest_text_zstd_calls"]
                            .parse::<u64>()
                            .unwrap()
                            > 0
                    );
                }
                assert_complete_values(&output, &batches);
                fs::remove_file(output).unwrap();
                assert_files(&directory.0, &[&input]);
            }
        }
    });
}

#[test]
fn streaming_pressure_late_oversized_ipc_batch_fails_without_publishing_or_leaking() {
    bounded_completion(|| {
        let directory = FixtureDirectory::new("late-oversize");
        let input = directory.0.join("input.arrow");
        write_ipc(&input, &[batch(0, 3, 8), batch(3, 3, 2 << 20)]);
        for grant in [1, 4] {
            let source =
                crate::universal_format_io::stream_flat_arrow_ipc_columnar_source(&input, 6)
                    .unwrap();
            let output = directory.0.join(format!("denied-{grant}.vortex"));
            let mut options = WriteOptions::new(grant, 6);
            options.memory_bytes = 16 << 20;
            let observed = write_observed(source, &output, &options);
            assert!(
                observed
                    .result
                    .unwrap_err()
                    .to_string()
                    .contains("conversion headroom")
            );
            assert_eq!(observed.batch_bytes.len(), 2);
            assert!(observed.batch_bytes[1] > 4 << 20);
            assert!(!output.exists());
            assert_files(&directory.0, &[&input]);
        }
    });
}

#[test]
fn streaming_pressure_shared_budget_denial_after_writer_entry_releases_every_owned_credit() {
    bounded_completion(|| {
        let directory = FixtureDirectory::new("shared-denial");
        let input = directory.0.join("input.arrow");
        let batches = (0..8)
            .map(|index| batch(index * 3, 3, 64))
            .collect::<Vec<_>>();
        write_ipc(&input, &batches);
        let source =
            crate::universal_format_io::stream_flat_arrow_ipc_columnar_source(&input, 24).unwrap();
        let output = directory.0.join("denied.vortex");
        let mut options = WriteOptions::new(1, 24);
        options.memory_bytes = 8 << 20;
        options.exhaust_after_first = true;
        let observed = write_observed(source, &output, &options);
        assert_eq!(observed.pressure_applied, 1);
        assert!(observed.memory.denied_reservations > 0);
        assert!(
            observed
                .result
                .unwrap_err()
                .to_string()
                .contains("memory reservation denied")
        );
        assert!(!output.exists());
        assert_files(&directory.0, &[&input]);
    });
}

#[test]
fn streaming_pressure_full_writer_validation_failure_preserves_existing_destination() {
    bounded_completion(|| {
        let directory = FixtureDirectory::new("precommit-validation");
        let input = directory.0.join("input.parquet");
        let batches = vec![batch(0, 3, 128), batch(3, 7, 4096), batch(10, 1, 2)];
        write_parquet(&input, &batches);
        for grant in [1, 4] {
            let source =
                crate::universal_format_io::stream_flat_parquet_columnar_source_with_parallelism(
                    &input, 11, grant,
                )
                .unwrap();
            let output = directory.0.join(format!("existing-{grant}.vortex"));
            fs::write(&output, b"previous independently owned destination").unwrap();
            let mut options = WriteOptions::new(grant, 12);
            options.overwrite = true;
            options.codec = true;
            let observed = write_observed(source, &output, &options);
            assert!(
                observed
                    .result
                    .unwrap_err()
                    .to_string()
                    .contains("writer row count mismatch")
            );
            assert_eq!(
                fs::read(&output).unwrap(),
                b"previous independently owned destination"
            );
            assert_files(&directory.0, &[&input, &output]);
            fs::remove_file(output).unwrap();
        }
    });
}

#[cfg(unix)]
#[derive(Clone, Copy, Debug)]
enum SourceMutation {
    SameSizeWithRestoredMtime,
    Truncate,
    ReplacePath,
}

#[cfg(unix)]
impl SourceMutation {
    fn apply(self, path: &Path) {
        use std::io::{Read as _, Seek as _, SeekFrom, Write as _};

        let original = fs::metadata(path).unwrap();
        match self {
            Self::SameSizeWithRestoredMtime => {
                let mut file = fs::File::options()
                    .read(true)
                    .write(true)
                    .open(path)
                    .unwrap();
                file.seek(SeekFrom::Start(4)).unwrap();
                let mut byte = [0];
                file.read_exact(&mut byte).unwrap();
                file.seek(SeekFrom::Start(4)).unwrap();
                file.write_all(&[byte[0] ^ 0xff]).unwrap();
                file.set_times(fs::FileTimes::new().set_modified(original.modified().unwrap()))
                    .unwrap();
                let changed = file.metadata().unwrap();
                assert_eq!(changed.len(), original.len());
                assert_eq!(changed.modified().unwrap(), original.modified().unwrap());
            }
            Self::Truncate => {
                fs::File::options()
                    .write(true)
                    .open(path)
                    .unwrap()
                    .set_len(original.len() / 2)
                    .unwrap();
            }
            Self::ReplacePath => {
                // Identical bytes still represent a different held file
                // generation. No source-content difference is required.
                let replacement = path.with_extension("replacement.parquet");
                assert!(!replacement.exists());
                fs::copy(path, &replacement).unwrap();
                fs::rename(replacement, path).unwrap();
            }
        }
    }
}

#[cfg(unix)]
struct MutateAfterEofSource {
    inner: Box<dyn arrow_array::RecordBatchReader + Send>,
    path: PathBuf,
    mutation: Option<SourceMutation>,
    rows: Arc<AtomicUsize>,
    applied: Arc<AtomicUsize>,
}

#[cfg(unix)]
impl Iterator for MutateAfterEofSource {
    type Item = std::result::Result<RecordBatch, arrow_schema::ArrowError>;

    fn next(&mut self) -> Option<Self::Item> {
        let item = self.inner.next();
        if let Some(Ok(batch)) = &item {
            self.rows.fetch_add(batch.num_rows(), Ordering::SeqCst);
        }
        if item.is_none()
            && let Some(mutation) = self.mutation.take()
        {
            // The actual adapter has already accepted EOF. A reader-only
            // generation check cannot detect this change; publication must
            // retain and validate the same source identity independently.
            mutation.apply(&self.path);
            self.applied.fetch_add(1, Ordering::SeqCst);
        }
        item
    }
}

#[cfg(unix)]
impl arrow_array::RecordBatchReader for MutateAfterEofSource {
    fn schema(&self) -> SchemaRef {
        self.inner.schema()
    }
}

#[cfg(unix)]
fn mutate_after_eof(
    source: &mut FlatLocalColumnarStreamSource,
    path: &Path,
    mutation: SourceMutation,
) -> (Arc<AtomicUsize>, Arc<AtomicUsize>) {
    let rows = Arc::new(AtomicUsize::new(0));
    let applied = Arc::new(AtomicUsize::new(0));
    let placeholder = arrow_array::RecordBatchIterator::new(
        std::iter::empty::<std::result::Result<RecordBatch, arrow_schema::ArrowError>>(),
        source.reader.schema(),
    );
    let inner = std::mem::replace(&mut source.reader, Box::new(placeholder));
    source.reader = Box::new(MutateAfterEofSource {
        inner,
        path: path.to_path_buf(),
        mutation: Some(mutation),
        rows: Arc::clone(&rows),
        applied: Arc::clone(&applied),
    });
    (rows, applied)
}

#[cfg(unix)]
fn assert_generation_failure(error: &ShardLoomError) {
    let diagnostic = error.to_string();
    assert!(diagnostic.contains("prepared source"), "{diagnostic}");
    assert!(
        diagnostic.contains("changed") || diagnostic.contains("invalidated"),
        "{diagnostic}"
    );
    assert!(diagnostic.contains("no fallback execution was attempted"));
}

#[cfg(unix)]
#[test]
fn streaming_generation_mutation_before_first_pull_preserves_destination_and_drops_source() {
    bounded_completion(|| {
        let directory = FixtureDirectory::new("generation-before-pull");
        let input = directory.0.join("input.parquet");
        let output = directory.0.join("existing.vortex");
        for grant in [1, 4] {
            for mutation in [
                SourceMutation::SameSizeWithRestoredMtime,
                SourceMutation::Truncate,
                SourceMutation::ReplacePath,
            ] {
                write_parquet(&input, &[batch(0, 3, 128), batch(3, 7, 4096)]);
                let mut source = crate::universal_format_io::stream_flat_parquet_columnar_source_with_parallelism(
                    &input, 10, grant,
                ).unwrap();
                assert_eq!(source.source_identities.len(), 1);
                let identity = Arc::downgrade(&source.source_identities[0]);
                let dropped = Arc::new(AtomicUsize::new(0));
                let bytes = Arc::new(Mutex::new(Vec::new()));
                source.reader = Box::new(ObservedSource {
                    inner: source.reader,
                    batch_bytes: Arc::clone(&bytes),
                    dropped: Arc::clone(&dropped),
                });
                mutation.apply(&input);
                fs::write(&output, b"previous destination").unwrap();
                let error = write_flat_columnar_vortex_prepared_state_streaming(
                    VortexPreparedStateColumnarStreamWriteRequest::new(&output, source)
                        .allow_overwrite(true)
                        .shared_native_memory_budget_bytes(32 << 20),
                )
                .unwrap_err();
                assert_generation_failure(&error);
                assert!(bytes.lock().unwrap().is_empty());
                assert_eq!(dropped.load(Ordering::SeqCst), 1);
                assert!(identity.upgrade().is_none());
                assert_eq!(fs::read(&output).unwrap(), b"previous destination");
                assert_files(&directory.0, &[&input, &output]);
            }
        }
    });
}

#[cfg(unix)]
#[test]
fn streaming_generation_mutation_after_eof_rejects_publication_and_releases_credits() {
    bounded_completion(|| {
        let directory = FixtureDirectory::new("generation-after-eof");
        let input = directory.0.join("input.parquet");
        let output = directory.0.join("existing.vortex");
        for grant in [1, 4] {
            for mutation in [
                SourceMutation::SameSizeWithRestoredMtime,
                SourceMutation::Truncate,
                SourceMutation::ReplacePath,
            ] {
                write_parquet(&input, &[batch(0, 3, 128), batch(3, 7, 4096)]);
                let mut source = crate::universal_format_io::stream_flat_parquet_columnar_source_with_parallelism(
                    &input, 10, grant,
                ).unwrap();
                assert_eq!(source.source_identities.len(), 1);
                let identity = Arc::downgrade(&source.source_identities[0]);
                let (rows, applied) = mutate_after_eof(&mut source, &input, mutation);
                fs::write(&output, b"previous destination").unwrap();
                let mut options = WriteOptions::new(grant, 10);
                options.overwrite = true;
                options.codec = true;
                let observed = write_observed(source, &output, &options);
                assert_generation_failure(&observed.result.unwrap_err());
                assert_eq!(rows.load(Ordering::SeqCst), 10);
                assert_eq!(applied.load(Ordering::SeqCst), 1);
                assert!(identity.upgrade().is_none());
                assert_eq!(fs::read(&output).unwrap(), b"previous destination");
                assert_files(&directory.0, &[&input, &output]);
            }
        }
    });
}

#[cfg(unix)]
#[test]
fn streaming_generation_empty_source_mutation_after_eof_cannot_bypass_publication_guard() {
    bounded_completion(|| {
        let directory = FixtureDirectory::new("generation-empty-eof");
        let input = directory.0.join("input.parquet");
        let output = directory.0.join("existing.vortex");
        for grant in [1, 4] {
            for native_memory in [false, true] {
                write_parquet(&input, &[]);
                let mut source = crate::universal_format_io::stream_flat_parquet_columnar_source_with_parallelism(
                    &input, 1, grant,
                ).unwrap();
                assert_eq!(source.source_identities.len(), 1);
                let identity = Arc::downgrade(&source.source_identities[0]);
                let (rows, applied) = mutate_after_eof(
                    &mut source,
                    &input,
                    SourceMutation::SameSizeWithRestoredMtime,
                );
                fs::write(&output, b"previous destination").unwrap();
                let mut request =
                    VortexPreparedStateColumnarStreamWriteRequest::new(&output, source)
                        .allow_overwrite(true);
                if native_memory {
                    request = request.shared_native_memory_budget_bytes(32 << 20);
                }
                let error =
                    write_flat_columnar_vortex_prepared_state_streaming(request).unwrap_err();
                assert_generation_failure(&error);
                assert_eq!(rows.load(Ordering::SeqCst), 0);
                assert_eq!(applied.load(Ordering::SeqCst), 1);
                assert!(identity.upgrade().is_none());
                assert_eq!(fs::read(&output).unwrap(), b"previous destination");
                assert_files(&directory.0, &[&input, &output]);
            }
        }
    });
}
