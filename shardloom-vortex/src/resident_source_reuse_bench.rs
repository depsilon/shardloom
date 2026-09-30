//! Bounded release-only attribution screen for concurrent readers of one retained source.
//! This measures the existing path only; it adds no source cache or sharing runtime behavior.

#[path = "resident_source_fanout_bench.rs"]
#[cfg(feature = "vortex-local-primitives")]
mod fanout;

use super::*;
use futures::future::BoxFuture;
use serde_json::{Value, json};
use sha2::{Digest as _, Sha256};
use std::{
    fmt::Write as _,
    fs,
    io::Read as _,
    path::PathBuf,
    sync::{Mutex, mpsc},
    thread,
    time::{Duration, Instant},
};
use vortex::{
    array::{
        Canonical, Columnar, IntoArray as _,
        arrays::{PrimitiveArray, Struct, StructArray, VarBinArray, struct_::StructArrayExt as _},
        dtype::{DType, FieldNames, Nullability, PType},
        validity::Validity,
    },
    file::{
        WriteOptionsSessionExt as _,
        segments::{FileSegmentSource, RequestMetrics},
    },
    io::VortexReadAt,
    metrics::DefaultMetricsRegistry,
};

const ROWS: usize = 131_072;
const BATCH_ROWS: usize = 16_384;
const BATCHES: usize = ROWS / BATCH_ROWS;
const MAX_SOURCE_BYTES: u64 = 64 << 20;
const SESSION_BYTES: u64 = 256 << 20;
const MAX_OUTPUT_BYTES: u64 = 32 << 20;
const MAX_OUTPUT_ARRAYS: usize = 64;
const MAX_READ_EVENTS_PER_CALL: usize = 4096;
const DEADLINE: Duration = Duration::from_secs(20);
const BASE: i64 = 1_i64 << 60;

static NEXT_FIXTURE: AtomicU64 = AtomicU64::new(0);

struct Fixture {
    directory: PathBuf,
    path: PathBuf,
    len: u64,
    sha256: String,
}

impl Fixture {
    fn new() -> Self {
        let id = NEXT_FIXTURE.fetch_add(1, Ordering::Relaxed);
        let directory = std::env::temp_dir().join(format!(
            "shardloom-source-reuse-{}-{id}",
            std::process::id()
        ));
        fs::create_dir(&directory).unwrap();
        let path = directory.join("source.vortex");
        // Establish exact-path cleanup before any fallible writer work.
        let mut fixture = Self {
            directory,
            path,
            len: 0,
            sha256: String::new(),
        };
        let runtime = CurrentThreadRuntime::new();
        let session = VortexSession::default().with_handle(runtime.handle());
        let mut arrays = Vec::with_capacity(BATCHES);
        assert_eq!(arrays.capacity(), BATCHES);
        for batch in 0..BATCHES {
            let start = batch * BATCH_ROWS;
            let keys = PrimitiveArray::new(
                (start..start + BATCH_ROWS)
                    .map(|row| BASE + i64::try_from(row).unwrap())
                    .collect::<Vec<_>>(),
                Validity::NonNullable,
            )
            .into_array();
            let text = (start..start + BATCH_ROWS)
                .map(text_value)
                .collect::<Vec<_>>();
            let text = VarBinArray::from_iter(
                text.iter().map(|value| value.as_deref()),
                DType::Utf8(Nullability::Nullable),
            )
            .into_array();
            arrays.push(
                StructArray::try_new(
                    FieldNames::from(["renamed_exact_key", "renamed_text"]),
                    vec![keys, text],
                    BATCH_ROWS,
                    Validity::NonNullable,
                )
                .unwrap()
                .into_array(),
            );
        }
        let mut writer = session.write_options().blocking(&runtime).writer(
            fs::File::create(&fixture.path).unwrap(),
            arrays[0].dtype().clone(),
        );
        for array in arrays {
            writer.push(array).unwrap();
        }
        writer.finish().unwrap();
        drop(session);
        drop(runtime);

        let metadata = fs::metadata(&fixture.path).unwrap();
        let len = metadata.len();
        assert!(
            len > 0 && len <= MAX_SOURCE_BYTES,
            "fixture exceeded 64 MiB"
        );
        let mut input = fs::File::open(&fixture.path).unwrap();
        let mut digest = Sha256::new();
        let mut buffer = vec![0_u8; 64 * 1024];
        let mut hashed = 0_u64;
        loop {
            let count = input.read(&mut buffer).unwrap();
            if count == 0 {
                break;
            }
            hashed = hashed.checked_add(u64::try_from(count).unwrap()).unwrap();
            assert!(hashed <= MAX_SOURCE_BYTES);
            digest.update(&buffer[..count]);
        }
        assert_eq!(hashed, len);
        let mut sha256 = String::with_capacity(64);
        for byte in digest.finalize() {
            write!(&mut sha256, "{byte:02x}").unwrap();
        }
        fixture.len = len;
        fixture.sha256 = sha256;
        fixture
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        // This path is created uniquely by this fixture; cleanup never broadens to its parent.
        let _ = fs::remove_dir_all(&self.directory);
    }
}

fn text_value(row: usize) -> Option<String> {
    (!row.is_multiple_of(17)).then(|| format!("row-{row:06}-λ"))
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct ReadEvent {
    offset: u64,
    length: usize,
    start_nanos: u64,
    end_nanos: u64,
}

#[derive(Clone)]
struct TimedReadAt {
    inner: ResidentFileReadAt,
    events: Arc<Mutex<Vec<ReadEvent>>>,
    epoch: Instant,
}

impl VortexReadAt for TimedReadAt {
    fn coalesce_config(&self) -> Option<vortex::io::CoalesceConfig> {
        self.inner.coalesce_config()
    }

    fn concurrency(&self) -> usize {
        self.inner.concurrency()
    }

    fn size(&self) -> BoxFuture<'static, VortexResult<u64>> {
        self.inner.size()
    }

    fn read_at(
        &self,
        offset: u64,
        length: usize,
        alignment: Alignment,
    ) -> BoxFuture<'static, VortexResult<vortex::array::buffer::BufferHandle>> {
        let inner = self.inner.clone();
        let events = Arc::clone(&self.events);
        let epoch = self.epoch;
        async move {
            let start_nanos = nanos(epoch.elapsed());
            let buffer = inner.read_at(offset, length, alignment).await?;
            let end_nanos = nanos(epoch.elapsed());
            let mut events = events
                .lock()
                .map_err(|_| vortex_err!("read event lock poisoned"))?;
            if events.len() >= MAX_READ_EVENTS_PER_CALL {
                return Err(vortex_err!("bounded read event limit exceeded"));
            }
            events.push(ReadEvent {
                offset,
                length,
                start_nanos,
                end_nanos,
            });
            Ok(buffer)
        }
        .boxed()
    }
}

struct CallOutput {
    index: usize,
    columns: Vec<&'static str>,
    result: OwnedVortexResultBatch,
    start_nanos: u64,
    wall_nanos: u64,
    queue_nanos: u64,
    callback_service_nanos: u64,
    events: Vec<ReadEvent>,
    observer: Arc<Mutex<Vec<ReadEvent>>>,
    verify_nanos: u64,
}

fn nanos(duration: Duration) -> u64 {
    u64::try_from(duration.as_nanos()).unwrap()
}

fn scan_owned(
    file: &vortex::file::VortexFile,
    context: &NativeExecutionContext<'_>,
    columns: &[&'static str],
    resident: &ResidentVortexSession,
) -> Result<OwnedVortexResultBatch> {
    let projection = vortex::expr::select(columns, vortex::expr::root())
        .bind(file.dtype())
        .map_err(native_error)?;
    let dtype = projection.dtype().clone();
    let scan = file
        .scan()
        .map_err(native_error)?
        .with_ordered(true)
        .with_concurrency(context.cpu_lanes())
        .with_projection(projection);
    let ownership = resident
        .memory()
        .reserve(u64::try_from(MAX_OUTPUT_ARRAYS * size_of::<ArrayRef>()).unwrap())?;
    let mut arrays = Vec::with_capacity(MAX_OUTPUT_ARRAYS);
    assert_eq!(arrays.capacity(), MAX_OUTPUT_ARRAYS);
    let mut rows = 0_u64;
    let mut logical_buffer_bytes = 0_u64;
    for array in scan
        .into_array_iter(context.runtime())
        .map_err(native_error)?
    {
        context.check_cancelled()?;
        let array = array.map_err(native_error)?;
        rows = rows
            .checked_add(u64::try_from(array.len()).map_err(native_error)?)
            .ok_or_else(|| resident_error("source reuse output row count overflow"))?;
        logical_buffer_bytes = logical_buffer_bytes
            .checked_add(array.nbytes())
            .ok_or_else(|| resident_error("source reuse output byte count overflow"))?;
        if rows > ROWS as u64
            || logical_buffer_bytes > MAX_OUTPUT_BYTES
            || arrays.len() >= MAX_OUTPUT_ARRAYS
        {
            return Err(resident_error("bounded source reuse output limit exceeded"));
        }
        arrays.push(array);
    }
    if rows != ROWS as u64 {
        return Err(resident_error(
            "native projection returned an incomplete source",
        ));
    }
    Ok(OwnedVortexResultBatch {
        dtype,
        arrays: Budgeted::new(arrays, ownership),
        runtime: Arc::clone(&resident.0),
        rows,
        logical_buffer_bytes,
    })
}

fn call(
    source: &PreparedVortexSource,
    resident: &ResidentVortexSession,
    index: usize,
    columns: Vec<&'static str>,
    instrumented: bool,
    epoch: Instant,
) -> Result<CallOutput> {
    let events = Arc::new(Mutex::new(Vec::new()));
    let call_started = Instant::now();
    let start_nanos = nanos(call_started.duration_since(epoch));
    let (result, queue_nanos, callback_service_nanos) = source.with_native_execution_controlled(
        &CancellationToken::default(),
        |file, context| {
            let queue_nanos = u64::try_from(context.queue_time().as_nanos()).unwrap_or(u64::MAX);
            let run_started = Instant::now();
            let result = if instrumented {
                let identity = Arc::clone(source.0.identity.as_ref().ok_or_else(|| {
                    resident_error("attribution fixture requires a retained filesystem generation")
                })?);
                let scope = context.io_scope().ok_or_else(|| {
                    resident_error("instrumented serving call has no scoped I/O owner")
                })?;
                let reader = ResidentFileReadAt {
                    identity,
                    allocator: context.native_session().allocator(),
                    handle: context.runtime().handle(),
                    concurrency: context.cpu_lanes(),
                    _reader_owner: Some(scope.retain_reader(context.memory())?),
                    scope: Some(scope),
                };
                let metrics = RequestMetrics::new(&DefaultMetricsRegistry::default(), Vec::new());
                let segments = FileSegmentSource::open(
                    Arc::clone(file.footer().segment_map()),
                    TimedReadAt {
                        inner: reader,
                        events: Arc::clone(&events),
                        epoch,
                    },
                    context.runtime().handle(),
                    metrics,
                );
                let instrumented_file = file.clone().with_segment_source(Arc::new(segments));
                scan_owned(&instrumented_file, context, &columns, resident)?
            } else {
                scan_owned(file, context, &columns, resident)?
            };
            let callback_service_nanos =
                u64::try_from(run_started.elapsed().as_nanos()).unwrap_or(u64::MAX);
            Ok((result, queue_nanos, callback_service_nanos))
        },
    )?;
    let wall_nanos = nanos(call_started.elapsed());
    let completed = events
        .lock()
        .map_err(|_| resident_error("read event lock poisoned"))?
        .clone();
    Ok(CallOutput {
        index,
        columns,
        result,
        start_nanos,
        wall_nanos,
        queue_nanos,
        callback_service_nanos,
        events: completed,
        observer: events,
        verify_nanos: 0,
    })
}

fn verify(output: &CallOutput) {
    let result = &output.result;
    assert_eq!(result.row_count(), ROWS as u64);
    assert!(result.arrays().len() <= MAX_OUTPUT_ARRAYS);
    assert!(result.logical_buffer_bytes() <= MAX_OUTPUT_BYTES);
    let mut context = result.create_execution_ctx();
    let mut row_base = 0_usize;
    for array in result.arrays() {
        assert_eq!(array.dtype(), result.dtype());
        let array = array
            .clone()
            .execute::<Columnar>(&mut context)
            .unwrap()
            .into_array();
        let fields = array
            .as_opt::<Struct>()
            .expect("projection is a native struct");
        assert_eq!(
            fields.names().iter().map(AsRef::as_ref).collect::<Vec<_>>(),
            output.columns
        );
        for name in &output.columns {
            let field = fields
                .unmasked_field_by_name(name)
                .unwrap()
                .clone()
                .execute::<Canonical>(&mut context)
                .unwrap()
                .into_array();
            match *name {
                "renamed_exact_key" => {
                    assert_eq!(
                        field.dtype(),
                        &DType::Primitive(PType::I64, Nullability::NonNullable)
                    );
                    for local_row in 0..array.len() {
                        let row = row_base + local_row;
                        assert_eq!(
                            field.execute_scalar(local_row, &mut context).unwrap(),
                            (BASE + i64::try_from(row).unwrap()).into()
                        );
                    }
                }
                "renamed_text" => {
                    assert_eq!(field.dtype(), &DType::Utf8(Nullability::Nullable));
                    for local_row in 0..array.len() {
                        let row = row_base + local_row;
                        let scalar = field.execute_scalar(local_row, &mut context).unwrap();
                        let actual = scalar
                            .as_utf8()
                            .value()
                            .map(|value| value.as_str().to_owned());
                        assert_eq!(actual, text_value(row));
                    }
                }
                _ => panic!("unexpected projection column {name}"),
            }
        }
        row_base += array.len();
    }
    assert_eq!(row_base, ROWS);
}

fn event_json(event: ReadEvent) -> Value {
    json!([
        event.offset,
        event.length,
        event.start_nanos,
        event.end_nanos
    ])
}

fn ranges_summary(events: &[ReadEvent]) -> (u64, u64) {
    let total = events.iter().map(|event| event.length as u64).sum::<u64>();
    let mut ranges = events
        .iter()
        .map(|event| (event.offset, event.offset + event.length as u64))
        .collect::<Vec<_>>();
    ranges.sort_unstable();
    let mut union = 0_u64;
    let mut current: Option<(u64, u64)> = None;
    for (start, end) in ranges {
        current = match current {
            Some((left, right)) if start <= right => Some((left, right.max(end))),
            Some((left, right)) => {
                union += right - left;
                Some((start, end))
            }
            None => Some((start, end)),
        };
    }
    if let Some((left, right)) = current {
        union += right - left;
    }
    (total, union)
}

#[allow(
    clippy::too_many_lines,
    reason = "Keep release, native completion, oracle and ownership teardown in one visible cohort lifecycle"
)]
fn run_cohort(
    source: &PreparedVortexSource,
    resident: &ResidentVortexSession,
    callers: usize,
    disjoint: bool,
    instrumented: bool,
) -> Value {
    let ready_deadline = Instant::now() + DEADLINE;
    let (ready_tx, ready_rx) = mpsc::channel();
    let memory_before = resident.memory().snapshot();
    let (mut outputs, cohort_wall_nanos) = thread::scope(|scope| {
        let mut workers = Vec::with_capacity(callers);
        let mut releases = Vec::with_capacity(callers);
        for index in 0..callers {
            let ready = ready_tx.clone();
            let (release_tx, release) = mpsc::sync_channel(1);
            releases.push(release_tx);
            let source = source.clone();
            let resident = resident.clone();
            let columns = if disjoint {
                if index % 2 == 0 {
                    vec!["renamed_exact_key"]
                } else {
                    vec!["renamed_text"]
                }
            } else {
                vec!["renamed_exact_key", "renamed_text"]
            };
            workers.push(scope.spawn(move || {
                ready.send(()).expect("cohort controller remains alive");
                let epoch = release
                    .recv_timeout(DEADLINE)
                    .expect("bounded start gate released");
                call(&source, &resident, index, columns, instrumented, epoch)
            }));
        }
        drop(ready_tx);
        let mut ready_count = 0;
        while ready_count < callers && Instant::now() < ready_deadline {
            match ready_rx.recv_timeout(ready_deadline.saturating_duration_since(Instant::now())) {
                Ok(()) => ready_count += 1,
                Err(_) => break,
            }
        }
        let cohort_started = Instant::now();
        for release in releases {
            release
                .send(cohort_started)
                .expect("cohort workers remain alive");
        }
        let mut outputs = Vec::with_capacity(callers);
        let mut panic_seen = false;
        for worker in workers {
            match worker.join() {
                Ok(result) => outputs.push(result.expect("native source call succeeds")),
                Err(_) => panic_seen = true,
            }
        }
        assert_eq!(
            ready_count, callers,
            "bounded start gate did not collect every caller"
        );
        assert!(!panic_seen, "source-reuse cohort caller panicked");
        (outputs, nanos(cohort_started.elapsed()))
    });
    assert_eq!(resident.admission_snapshot().unwrap().active_calls, 0);
    assert_eq!(resident.io_snapshot().unwrap().active_requests, 0);
    let completed_read_events = outputs
        .iter()
        .map(|output| output.events.len())
        .sum::<usize>();
    let events = outputs
        .iter()
        .flat_map(|output| output.events.iter().copied())
        .collect::<Vec<_>>();
    let (requested_bytes, unique_interval_bytes) = ranges_summary(&events);
    let read_events = outputs.iter().map(|output| {
        json!({
            "caller": output.index,
            "successful_positional_ranges_offset_length_start_end_ns": output.events.iter().copied().map(event_json).collect::<Vec<_>>(),
            "successful_requested_bytes": ranges_summary(&output.events).0,
        })
    }).collect::<Vec<_>>();
    let mut verified = 0;
    for output in &mut outputs {
        let verify_started = Instant::now();
        verify(output);
        output.verify_nanos =
            u64::try_from(verify_started.elapsed().as_nanos()).unwrap_or(u64::MAX);
        assert_eq!(
            *output.observer.lock().unwrap(),
            output.events,
            "oracle caused source reads"
        );
        verified += 1;
    }
    let verify_nanos = outputs
        .iter()
        .map(|output| output.verify_nanos)
        .sum::<u64>();
    let output_rows = outputs
        .iter()
        .map(|output| output.result.row_count())
        .sum::<u64>();
    let output_bytes = outputs
        .iter()
        .map(|output| output.result.logical_buffer_bytes())
        .sum::<u64>();
    let starts = outputs
        .iter()
        .map(|output| output.start_nanos)
        .collect::<Vec<_>>();
    let walls = outputs
        .iter()
        .map(|output| output.wall_nanos)
        .collect::<Vec<_>>();
    let queues = outputs
        .iter()
        .map(|output| output.queue_nanos)
        .collect::<Vec<_>>();
    let callbacks = outputs
        .iter()
        .map(|output| output.callback_service_nanos)
        .collect::<Vec<_>>();
    let drop_started = Instant::now();
    for output in outputs {
        drop(output.result);
    }
    let output_drop_nanos = u64::try_from(drop_started.elapsed().as_nanos()).unwrap_or(u64::MAX);
    let memory_after = resident.memory().snapshot();
    // Completed reads can precede the provider driver's final buffer cleanup.
    // Observe this point; require zero credits only after joining session teardown.
    assert_eq!(memory_after.denied_reservations, 0);
    json!({
        "callers": callers,
        "projection_pattern": if !disjoint {"identical_key_text"} else if callers == 2 {"disjoint_key_text"} else {"two_repeated_projection_groups"},
        "instrumented_readat_wrapper": instrumented,
        "cohort_wall_nanos_from_release_through_join": cohort_wall_nanos,
        "call_start_nanos_from_cohort_release": starts,
        "call_wall_nanos_including_admission_drain_generation_validation": walls,
        "admission_queue_nanos": queues,
        "callback_scan_and_owned_output_nanos_excluding_outer_drain": callbacks,
        "successful_read_event_count": instrumented.then_some(completed_read_events),
        "successful_requested_bytes": instrumented.then_some(requested_bytes),
        "unique_interval_union_bytes": instrumented.then_some(unique_interval_bytes),
        "repeated_requested_bytes_by_interval_overlap": instrumented.then_some(requested_bytes.saturating_sub(unique_interval_bytes)),
        "successful_read_events": instrumented.then_some(read_events),
        "output_rows_total": output_rows,
        "output_logical_buffer_bytes_total": output_bytes,
        "outputs_fully_typed_value_and_null_verified": verified,
        "independent_oracle_nanos_total": verify_nanos,
        "output_drop_nanos_total": output_drop_nanos,
        "session_reserved_bytes_before": memory_before.reserved_bytes,
        "session_peak_reserved_bytes_cumulative": memory_after.peak_reserved_bytes,
        "session_reserved_bytes_after_output_drop": memory_after.reserved_bytes,
    })
}

#[test]
#[ignore = "bounded release-only concurrent native source attribution screen; run serially with --release --ignored --exact"]
#[allow(clippy::too_many_lines)]
#[allow(clippy::assertions_on_constants)]
fn concurrent_source_reuse_attribution() {
    assert!(
        !cfg!(debug_assertions),
        "this attribution screen requires --release"
    );
    let fixture = Fixture::new();
    let session = ResidentVortexSession::with_serving_policy(
        SESSION_BYTES,
        4,
        ResidentServingPolicy {
            general_cpu_lanes: 1,
            reserve_metadata_lane: false,
            max_queued_calls: 8,
            max_queued_call_bytes: 1 << 20,
            max_io_requests: 32,
            max_io_bytes: 128 << 20,
        },
    )
    .unwrap();
    assert_eq!(session.snapshot().provider_background_workers, 0);
    let source = session.prepare_file(&fixture.path).unwrap();
    assert_eq!(session.snapshot().prepared_source_opens, 1);
    source.validate_generation().unwrap();
    let generation = source.0.identity.as_ref().unwrap().generation.clone();
    assert_eq!(generation.len, fixture.len);
    let scenarios = [(1, false), (4, false), (2, true), (4, true)];
    let mut records = Vec::new();
    for (callers, disjoint) in scenarios {
        for iteration in 0..4 {
            // Each paired cohort uses a synchronized start; instrumentation order alternates.
            let instrumented_first = iteration % 2 == 1;
            let order = if instrumented_first {
                [true, false]
            } else {
                [false, true]
            };
            let first = run_cohort(&source, &session, callers, disjoint, order[0]);
            let second = run_cohort(&source, &session, callers, disjoint, order[1]);
            records.push(
                json!({"scenario_callers": callers, "disjoint": disjoint, "iteration": iteration,
                "warmup": iteration == 0, "instrumentation_order": order,
                "first": first, "second": second}),
            );
        }
        source.validate_generation().unwrap();
    }
    let executions = session.snapshot().completed_executions;
    let expected_calls = records
        .iter()
        .map(|record| {
            let callers = record["scenario_callers"].as_u64().unwrap();
            2 * callers
        })
        .sum::<u64>();
    assert_eq!(executions, expected_calls);
    assert_eq!(session.snapshot().prepared_source_opens, 1);
    assert_eq!(session.admission_snapshot().unwrap().active_cpu_lanes, 0);
    assert_eq!(session.io_snapshot().unwrap().active_bytes, 0);
    let snapshot = session.snapshot();
    source.validate_generation().unwrap();
    let mut report = json!({
        "schema": "shardloom.resident_source_reuse_attribution.v1",
        "test": "concurrent_source_reuse_attribution",
        "claim_scope": "bounded native source-read attribution only; owned projection may retain encoded children; scalar-oracle decoding is untimed; instrumentation wrapper adds overhead; no production speedup, exclusive CPU/decode time, or production tail-latency claim",
        "fixture": {"rows": ROWS, "batches": BATCHES, "batch_rows": BATCH_ROWS,
            "source_length_bytes": fixture.len, "source_sha256": fixture.sha256,
            "writer": "Vortex 0.85.0 default strategy",
            "cache_condition": "warm local filesystem cache; fixture just written and hashed",
            "generation": {"length": generation.len, "device": generation.device, "inode": generation.inode},
            "generation_debug": format!("{generation:?}"),
            "max_source_bytes": MAX_SOURCE_BYTES},
        "runtime": {"os": std::env::consts::OS, "arch": std::env::consts::ARCH,
            "release_build": !cfg!(debug_assertions), "vortex_version": "0.85.0",
            "release_user_surfaces_feature": cfg!(feature = "release-user-surfaces"),
            "available_parallelism": thread::available_parallelism().map_or(1, std::num::NonZero::get),
            "serving_session_cpu_lanes_per_call": 1, "serving_session_parallelism": 4,
            "persistent_provider_workers": snapshot.provider_background_workers,
            "shared_source_opens": snapshot.prepared_source_opens,
            "completed_native_calls": executions,
            "generation_still_valid": true},
        "timing_contract": "call wall begins immediately before native admission and ends after scoped I/O drain and generation validation; cohort wall begins at bounded release gate; callback timing excludes outer drain; oracle and output drop are separately recorded",
        "read_at_contract": "instrumented values are successful Vortex positional read_at ranges after segment-source coalescing; requested bytes and interval union are not physical device I/O; uninstrumented cohorts retain the ordinary reader path",
        "scenarios_and_all_samples": records,
        "final_session_reserved_bytes_before_teardown": snapshot.memory.reserved_bytes,
    });
    drop(source);
    let memory = session.memory().clone();
    drop(session);
    assert_eq!(
        memory.snapshot().reserved_bytes,
        0,
        "session owners leaked memory credits"
    );
    report["final_session_reserved_bytes_after_teardown"] = json!(memory.snapshot().reserved_bytes);
    assert_eq!(memory.snapshot().denied_reservations, 0);
    report["session_denied_reservations"] = json!(memory.snapshot().denied_reservations);
    drop(memory);
    let fixture_directory = fixture.directory.clone();
    drop(fixture);
    assert!(!fixture_directory.exists(), "owned fixture cleanup failed");
    report["owned_fixture_removed"] = json!(true);
    eprintln!("SHARDLOOM_R8_SOURCE_REUSE_ATTRIBUTION {report}");
}
