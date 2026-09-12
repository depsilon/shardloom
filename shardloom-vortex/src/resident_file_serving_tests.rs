//! File-backed shared-session contention acceptance. The deliberate gate below
//! measures fixture queue residence, not production latency or an ingest speedup.
//! Admission is currently serialized and has no FIFO or queued-cancellation API.

use super::*;
use shardloom_exec::compute_pool::CancellationToken;
use std::{
    path::PathBuf,
    sync::{atomic::AtomicUsize, mpsc},
    thread,
    time::{Duration, Instant},
};
use vortex::{
    array::{
        IntoArray as _,
        arrays::{PrimitiveArray, StructArray, VarBinViewArray},
        dtype::FieldNames,
        validity::Validity,
    },
    file::WriteOptionsSessionExt as _,
    layout::layouts::flat::writer::FlatLayoutStrategy,
};

const ROWS: usize = 4096;
const RANGE_ROWS: usize = 1024;
const COUNT_CALLERS: usize = 3;
const COUNTS_PER_CALLER: usize = 4;
const WATCHDOG: Duration = Duration::from_secs(30);
static NEXT_FIXTURE: AtomicUsize = AtomicUsize::new(0);

struct FileFixture {
    directory: PathBuf,
    expected: ArrayRef,
}

impl FileFixture {
    fn new() -> Self {
        let directory = std::env::temp_dir().join(format!(
            "shardloom-file-serving-{}-{}",
            std::process::id(),
            NEXT_FIXTURE.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir(&directory).unwrap();
        let keys = PrimitiveArray::new(
            (0..ROWS)
                .map(|row| {
                    if row % 2 == 0 {
                        i64::MAX - i64::try_from(row).unwrap()
                    } else {
                        i64::MIN + i64::try_from(row).unwrap()
                    }
                })
                .collect::<Vec<_>>(),
            Validity::NonNullable,
        )
        .into_array();
        let texts = (0..ROWS)
            .map(|row| (row % 5 != 0).then(|| format!("{row}:{}", "港λ".repeat(16))))
            .collect::<Vec<_>>();
        let labels = VarBinViewArray::from_iter_nullable_str(texts.iter().map(Option::as_deref))
            .into_array();
        let expected = StructArray::try_new(
            FieldNames::from(["renamed_boundary_key", "renamed_nullable_text"]),
            vec![keys, labels],
            ROWS,
            Validity::NonNullable,
        )
        .unwrap()
        .into_array();
        let runtime = CurrentThreadRuntime::new();
        let session = VortexSession::default().with_handle(runtime.handle());
        session
            .write_options()
            .with_strategy(Arc::new(FlatLayoutStrategy::default()))
            .blocking(&runtime)
            .write(
                std::fs::File::create(directory.join("source.vortex")).unwrap(),
                expected.to_array_iterator(),
            )
            .unwrap();
        Self {
            directory,
            expected,
        }
    }

    fn path(&self) -> PathBuf {
        self.directory.join("source.vortex")
    }
}

impl Drop for FileFixture {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.directory);
    }
}

fn recv<T>(receiver: &mpsc::Receiver<T>) -> T {
    receiver
        .recv_timeout(WATCHDOG)
        .expect("bounded native serving owners must complete or release their fixture gate")
}

struct ServingWorkers {
    release_scan: Option<mpsc::Sender<()>>,
    scan: Option<thread::JoinHandle<()>>,
    counts: Vec<thread::JoinHandle<()>>,
}

impl ServingWorkers {
    fn release(&mut self) {
        if let Some(release) = self.release_scan.take() {
            let _ = release.send(());
        }
    }

    fn finish(&mut self) {
        self.release();
        let mut failed = self
            .scan
            .take()
            .is_some_and(|worker| worker.join().is_err());
        for worker in self.counts.drain(..) {
            failed |= worker.join().is_err();
        }
        assert!(
            !failed,
            "all serving callers must complete without panicking"
        );
    }
}

impl Drop for ServingWorkers {
    fn drop(&mut self) {
        // An assertion in the controller must still release the paused native
        // scan and join every caller. The scan's checkpoint also has a watchdog.
        self.release();
        if let Some(worker) = self.scan.take() {
            let _ = worker.join();
        }
        for worker in self.counts.drain(..) {
            let _ = worker.join();
        }
    }
}

fn assert_values(array: &ArrayRef, expected: &ArrayRef, offset: usize, session: &VortexSession) {
    let mut context = session.create_execution_ctx();
    for row in 0..array.len() {
        assert_eq!(
            array.execute_scalar(row, &mut context).unwrap(),
            expected.execute_scalar(offset + row, &mut context).unwrap()
        );
    }
}

fn assert_complete(result: &OwnedVortexResultBatch, expected: &ArrayRef) {
    let mut rows = 0;
    for array in result.arrays() {
        assert_values(array, expected, rows, &result.runtime.session);
        rows += array.len();
    }
    assert_eq!(rows, ROWS);
    assert_eq!(result.row_count(), u64::try_from(ROWS).unwrap());
}

#[derive(Debug)]
struct CountObservation {
    // Arrival to entry of the native callback includes generation admission;
    // this is not a separately instrumented mutex-only wait duration.
    queue_residence: Duration,
    completed_latency: Duration,
    rows: u64,
}

#[allow(clippy::too_many_lines)]
fn shared_session_serving_case(workers: usize, cancel_scan: bool) {
    let fixture = FileFixture::new();
    let session = ResidentVortexSession::new(16 << 20, workers).unwrap();
    let source = session.prepare_file(fixture.path()).unwrap();
    let memory = session.memory().clone();
    let initial = session.snapshot();
    let owner = Arc::downgrade(&session.0);
    let active = Arc::new(AtomicUsize::new(0));
    let cancellation = CancellationToken::default();
    let (scan_held, held) = mpsc::channel();
    let (release_scan, released) = mpsc::channel();
    let (scan_done, scan_completion) = mpsc::channel();
    let scan_source = source.clone();
    let scan_expected = fixture.expected.clone();
    let scan_active = Arc::clone(&active);
    let scan_cancel = cancellation.clone();
    let scan_worker = thread::spawn(move || {
        let result = scan_source.with_native_execution(|file, native, runtime| {
            assert_eq!(scan_active.fetch_add(1, Ordering::SeqCst), 0);
            let result = (|| {
                let mut rows = 0;
                for start in (0..ROWS).step_by(RANGE_ROWS) {
                    // These are real ordered scans of one held file. Explicit
                    // row ranges keep the fixture's checkpoint deterministic;
                    // they do not create another artifact or scheduler.
                    let end = start + RANGE_ROWS;
                    for array in file
                        .scan()
                        .map_err(native_error)?
                        .with_row_range(u64::try_from(start).unwrap()..u64::try_from(end).unwrap())
                        .with_ordered(true)
                        .with_concurrency(scan_source.0.runtime.parallelism)
                        .into_array_iter(runtime)
                        .map_err(native_error)?
                    {
                        let array = array.map_err(native_error)?;
                        assert_values(&array, &scan_expected, rows, native);
                        if rows == 0 {
                            scan_held.send(array.len()).unwrap();
                            recv(&released);
                        }
                        // Cancellation is cooperative after a completed native
                        // read. This proves neither blocked-I/O interruption nor
                        // cancellation of callers queued on the session mutex.
                        scan_cancel.check()?;
                        rows += array.len();
                    }
                }
                Ok(rows)
            })();
            assert_eq!(scan_active.fetch_sub(1, Ordering::SeqCst), 1);
            result
        });
        scan_done.send(result).unwrap();
    });
    let mut owned_workers = ServingWorkers {
        release_scan: Some(release_scan),
        scan: Some(scan_worker),
        counts: Vec::new(),
    };
    let first_rows = recv(&held);
    assert!(first_rows > 0 && first_rows < ROWS);
    let held_memory = memory.snapshot();
    assert!(held_memory.reserved_bytes > initial.memory.reserved_bytes);
    let (arrived, arrivals) = mpsc::channel();
    let (finished, completions) = mpsc::channel();
    for caller in 0..COUNT_CALLERS {
        let count_source = source.clone();
        let count_session = session.clone();
        let count_active = Arc::clone(&active);
        let arrived = arrived.clone();
        let finished = finished.clone();
        owned_workers.counts.push(thread::spawn(move || {
            // Every caller observes actual admission contention before the
            // controller releases the scan; no sleep or latency inference.
            assert!(matches!(
                count_session.0.admission.try_lock(),
                Err(std::sync::TryLockError::WouldBlock)
            ));
            let first_arrival = Instant::now();
            arrived.send(caller).unwrap();
            let mut observations = Vec::new();
            for index in 0..COUNTS_PER_CALLER {
                let arrival = if index == 0 {
                    first_arrival
                } else {
                    Instant::now()
                };
                // The existing native boundary gives an exact admitted instant
                // without adding runtime telemetry or changing the scheduler.
                let (rows, admitted) = count_source
                    .with_native_execution(|file, _, _| {
                        let admitted = Instant::now();
                        assert_eq!(count_active.fetch_add(1, Ordering::SeqCst), 0);
                        let rows = file.row_count();
                        assert_eq!(count_active.fetch_sub(1, Ordering::SeqCst), 1);
                        Ok((rows, admitted))
                    })
                    .unwrap();
                observations.push(CountObservation {
                    queue_residence: admitted.duration_since(arrival),
                    completed_latency: arrival.elapsed(),
                    rows,
                });
            }
            // Cross-check the public prepared count on this same source; both
            // paths execute metadata and keep the same generation/worker owner.
            assert_eq!(
                count_source.prepare_count().execute().unwrap(),
                u64::try_from(ROWS).unwrap()
            );
            finished.send(observations).unwrap();
        }));
    }
    drop(arrived);
    drop(finished);
    for _ in 0..COUNT_CALLERS {
        recv(&arrivals);
    }
    assert_eq!(active.load(Ordering::SeqCst), 1);
    assert_eq!(session.snapshot().completed_executions, 0);
    assert_eq!(
        session.snapshot().provider_background_workers,
        initial.provider_background_workers
    );
    if cancel_scan {
        cancellation.cancel();
    }
    owned_workers.release();
    let scan_result = recv(&scan_completion);
    if cancel_scan {
        assert!(
            scan_result
                .unwrap_err()
                .to_string()
                .contains("execution cancelled")
        );
    } else {
        assert_eq!(scan_result.unwrap(), ROWS);
    }
    let mut samples = Vec::new();
    for _ in 0..COUNT_CALLERS {
        samples.extend(recv(&completions));
    }
    owned_workers.finish();
    assert_eq!(samples.len(), COUNT_CALLERS * COUNTS_PER_CALLER);
    for sample in &samples {
        assert_eq!(sample.rows, u64::try_from(ROWS).unwrap());
        assert!(sample.queue_residence <= sample.completed_latency);
    }
    samples.sort_by_key(|sample| sample.completed_latency);
    eprintln!(
        "file-serving fixture only: workers={workers} cancelled_scan={cancel_scan} calls={} p50={:?} max={:?} max_queue={:?}",
        samples.len(),
        samples[samples.len() / 2].completed_latency,
        samples.last().unwrap().completed_latency,
        samples
            .iter()
            .map(|sample| sample.queue_residence)
            .max()
            .unwrap(),
    );
    assert_eq!(active.load(Ordering::SeqCst), 0);
    let snapshot = session.snapshot();
    assert_eq!(snapshot.prepared_source_opens, 1);
    let count_completions = COUNT_CALLERS * (COUNTS_PER_CALLER + 1);
    assert_eq!(
        snapshot.completed_executions,
        u64::try_from(count_completions + usize::from(!cancel_scan)).unwrap()
    );
    assert_eq!(
        snapshot.provider_background_workers,
        initial.provider_background_workers
    );
    assert!(1 + snapshot.provider_background_workers <= workers);
    assert!(snapshot.memory.peak_reserved_bytes <= snapshot.memory.limit_bytes);
    // An interrupted scan must leave the public full-result path reusable.
    let result = source
        .prepare_projection(
            &["renamed_boundary_key", "renamed_nullable_text"],
            u64::try_from(ROWS).unwrap(),
            8 << 20,
        )
        .unwrap()
        .execute()
        .unwrap();
    assert_complete(&result, &fixture.expected);
    drop(result);
    drop(source);
    drop(session);
    assert!(owner.upgrade().is_none());
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

#[test]
fn resident_file_serving_short_counts_wait_for_complete_scan_and_then_all_finish() {
    for workers in [1, 4] {
        shared_session_serving_case(workers, false);
    }
}

#[test]
fn resident_file_serving_cooperative_scan_cancellation_releases_queued_counts() {
    for workers in [1, 4] {
        shared_session_serving_case(workers, true);
    }
}

#[test]
fn resident_file_serving_retained_result_pressure_denies_scan_but_metadata_progresses() {
    let fixture = FileFixture::new();
    let session = ResidentVortexSession::new(16 << 20, 1).unwrap();
    let source = session.prepare_file(fixture.path()).unwrap();
    let memory = session.memory().clone();
    let metadata_bytes = memory.snapshot().reserved_bytes;
    let projection = source
        .prepare_projection(
            &["renamed_boundary_key", "renamed_nullable_text"],
            u64::try_from(ROWS).unwrap(),
            8 << 20,
        )
        .unwrap();
    let retained = projection.execute().unwrap();
    assert_complete(&retained, &fixture.expected);
    assert!(memory.snapshot().reserved_bytes > metadata_bytes);
    let arrays = retained.arrays().to_vec();
    drop(retained);
    let with_arrays = memory.snapshot();
    assert!(with_arrays.reserved_bytes > metadata_bytes);
    // Single caller, no operation in flight: retain an explicit competing
    // owner's remaining credits while real result arrays are still live.
    let pressure = memory
        .reserve(with_arrays.limit_bytes - with_arrays.reserved_bytes)
        .unwrap();
    let denied_before = memory.snapshot().denied_reservations;
    let error = projection
        .execute()
        .err()
        .expect("native output admission must fail");
    assert!(error.to_string().contains("memory reservation denied"));
    assert!(memory.snapshot().denied_reservations > denied_before);
    assert_eq!(
        source.prepare_count().execute().unwrap(),
        u64::try_from(ROWS).unwrap()
    );
    assert_eq!(
        memory.snapshot().reserved_bytes,
        memory.snapshot().limit_bytes
    );
    let mut rows = 0;
    for array in &arrays {
        assert_values(array, &fixture.expected, rows, &session.0.session);
        rows += array.len();
    }
    assert_eq!(rows, ROWS);
    drop(pressure);
    let retried = projection.execute().unwrap();
    assert_complete(&retried, &fixture.expected);
    assert_eq!(session.snapshot().prepared_source_opens, 1);
    assert_eq!(session.snapshot().completed_executions, 3);
    drop((arrays, retried, projection, source, session));
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}
