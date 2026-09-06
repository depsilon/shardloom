//! Native serialized-segment and complete scan acceptance with actual completed
//! positional-read observations. OS page-cache/device reads are not measured.

#[path = "resident_segment_reuse_bench.rs"]
mod release_bench;

use super::*;
use crate::resident_session::{
    ResidentVortexSession, SourceIdentity,
    read_observer::{ObservedFileReadAt, ReadObservationLimits},
};
use std::{
    path::{Path, PathBuf},
    time::Duration,
};
use vortex::{
    VortexSessionDefault as _,
    array::{
        ArrayRef, IntoArray as _, VortexSessionExecute as _,
        arrays::{PrimitiveArray, StructArray, VarBinViewArray},
        dtype::FieldNames,
        memory::MemorySessionExt as _,
        validity::Validity,
    },
    file::{OpenOptionsSessionExt as _, WriteOptionsSessionExt as _},
    io::session::RuntimeSessionExt as _,
    layout::{
        LayoutStrategy,
        layouts::{flat::writer::FlatLayoutStrategy, table::TableStrategy},
    },
    session::VortexSession,
};

const ROWS: usize = 16_384;
const BASE: i64 = 1_i64 << 60;
static NEXT: AtomicUsize = AtomicUsize::new(0);

use crate::local_primitives::native_flat_layout as sequential_flat;

struct Fixture {
    directory: PathBuf,
    expected: ArrayRef,
    selected: ArrayRef,
}

impl Fixture {
    fn new() -> Self {
        Self::with_layout(false)
    }

    fn with_layout(columnar: bool) -> Self {
        let directory = std::env::temp_dir().join(format!(
            "shardloom-segment-reuse-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir(&directory).unwrap();
        let texts = (0..ROWS)
            .map(|row| match row % 3 {
                0 => None,
                1 => Some(format!("a-{row:08}-低-{}", "first-region".repeat(5))),
                _ => Some(format!("z-{row:08}-港-{}", "selected-region".repeat(5))),
            })
            .collect::<Vec<_>>();
        let ids = PrimitiveArray::new(
            (0..ROWS)
                .map(|row| BASE + i64::try_from(row).unwrap())
                .collect::<Vec<_>>(),
            Validity::NonNullable,
        )
        .into_array();
        let labels = VarBinViewArray::from_iter_nullable_str(texts.iter().map(Option::as_deref))
            .into_array();
        let expected = StructArray::try_new(
            FieldNames::from(["renamed_text", "exact_identifier"]),
            vec![labels, ids],
            ROWS,
            Validity::NonNullable,
        )
        .unwrap()
        .into_array();
        let selected_ids = (0..ROWS)
            .filter(|row| row % 3 == 2)
            .map(|row| BASE + i64::try_from(row).unwrap())
            .collect::<Vec<_>>();
        let selected = StructArray::try_new(
            FieldNames::from(["exact_identifier"]),
            vec![PrimitiveArray::new(selected_ids.clone(), Validity::NonNullable).into_array()],
            selected_ids.len(),
            Validity::NonNullable,
        )
        .unwrap()
        .into_array();
        let runtime = CurrentThreadRuntime::new();
        let session = VortexSession::default().with_handle(runtime.handle());
        // Both cache settings consume the identical artifact for each layout.
        // Whole-struct Flat is a no-gain control: projection pins the very same
        // physical segment across filter conjuncts. Table has separate fields.
        let flat: Arc<dyn LayoutStrategy> = Arc::new(FlatLayoutStrategy::default());
        let strategy = if columnar {
            Arc::new(TableStrategy::new(Arc::clone(&flat), flat)) as Arc<dyn LayoutStrategy>
        } else {
            flat
        };
        session
            .write_options()
            .with_strategy(strategy)
            .blocking(&runtime)
            .write(
                std::fs::File::create(directory.join("source.vortex")).unwrap(),
                expected.to_array_iterator(),
            )
            .unwrap();
        assert!(
            std::fs::metadata(directory.join("source.vortex"))
                .unwrap()
                .len()
                > 512 * 1024
        );
        Self {
            directory,
            expected,
            selected,
        }
    }

    fn path(&self) -> PathBuf {
        self.directory.join("source.vortex")
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        std::fs::remove_dir_all(&self.directory).unwrap();
    }
}

fn observer(
    path: &Path,
    session: &VortexSession,
    runtime: &CurrentThreadRuntime,
) -> ObservedFileReadAt {
    ObservedFileReadAt::new(
        path,
        session.allocator(),
        runtime.handle(),
        ReadObservationLimits {
            max_read_bytes: 8 << 20,
            max_attempted_bytes: 64 << 20,
            max_requests: 256,
            max_in_flight: 16,
        },
    )
    .unwrap()
}

fn io_policy() -> SegmentReusePolicy {
    SegmentReusePolicy {
        max_retained_bytes: 8 << 20,
        max_segment_bytes: 8 << 20,
        max_entries: 16,
    }
}

#[test]
fn repeated_native_segment_consumers_reduce_completed_read_bytes_including_copy_cost_evidence() {
    let fixture = Fixture::new();
    let mut observations = Vec::new();
    for cached in [false, true] {
        let resident = ResidentVortexSession::new(32 << 20, 2).unwrap();
        let measured = resident
            .with_native_session(|session, runtime| {
                let reader = observer(&fixture.path(), session, runtime);
                let file = runtime
                    .block_on(session.open_options().open_read(reader.clone()))
                    .unwrap();
                let opened = reader.snapshot().unwrap();
                let id = file
                    .footer()
                    .segment_map()
                    .iter()
                    .enumerate()
                    .max_by_key(|(_, spec)| spec.length)
                    .map(|(id, _)| SegmentId::try_from(id).unwrap())
                    .unwrap();
                let validator = reader.clone();
                let cache = cached.then(|| {
                    ScanSegmentReuse::new(
                        file.segment_source(),
                        resident.memory().clone(),
                        io_policy(),
                        move || validator.validate_generation(),
                    )
                    .unwrap()
                });
                let source: Arc<dyn SegmentSource> = cache.as_ref().map_or_else(
                    || file.segment_source(),
                    |cache| Arc::new(cache.clone()) as Arc<dyn SegmentSource>,
                );
                let started = Instant::now();
                let first = runtime.block_on(source.request(id)).unwrap();
                let first_bytes = first.as_host().as_slice().to_vec();
                drop(first);
                let second = runtime.block_on(source.request(id)).unwrap();
                assert_eq!(second.as_host().as_slice(), first_bytes);
                drop(second);
                let lifecycle_nanos = started.elapsed().as_nanos();
                let snapshot = cache.as_ref().map(|cache| cache.close().unwrap());
                drop(source);
                drop(cache);
                drop(file);
                let completed = reader.close_and_drain(Duration::from_secs(10)).unwrap();
                reader.validate_generation().unwrap();
                assert_eq!(
                    completed.failed_read_calls
                        + completed.failed_before_read
                        + completed.cancelled_before_read,
                    0
                );
                assert_eq!(completed.pending_jobs, 0);
                let post_open_bytes = completed.completed_read_bytes - opened.completed_read_bytes;
                Ok((
                    post_open_bytes,
                    lifecycle_nanos,
                    snapshot,
                    first_bytes.len(),
                ))
            })
            .unwrap();
        assert_eq!(resident.snapshot().memory.reserved_bytes, 0);
        observations.push(measured);
    }
    let (plain_bytes, _, _, logical_bytes) = observations[0];
    let (cached_bytes, _, snapshot, _) = observations[1];
    let snapshot = snapshot.unwrap();
    assert!(
        plain_bytes > cached_bytes,
        "completed positional-read bytes did not decline: {observations:?}"
    );
    assert!(plain_bytes - cached_bytes >= logical_bytes as u64);
    assert_eq!(snapshot.counters.hits, 1);
    assert_eq!(snapshot.counters.copied_bytes, logical_bytes as u64);
    assert_eq!(snapshot.retention.reserved_bytes, 0);
    eprintln!(
        "scan_segment_reuse_duplicate_bytes baseline={plain_bytes} candidate={cached_bytes} copied={} baseline_lifecycle_ns={} candidate_lifecycle_ns={} copy_ns={}",
        snapshot.counters.copied_bytes,
        observations[0].1,
        observations[1].1,
        snapshot.counters.copy_nanos
    );
}

#[test]
#[allow(clippy::too_many_lines)] // Paired physical-layout controls share one exact scan oracle.
fn duplicate_predicate_native_scan_preserves_flat_control_and_reduces_columnar_reads() {
    use shardloom_core::{ComparisonOp, StatValue};
    for columnar in [false, true] {
        let fixture = Fixture::with_layout(columnar);
        let mut observed = Vec::new();
        for cached in [false, true] {
            let resident = ResidentVortexSession::new(32 << 20, 2).unwrap();
            let read_bytes = resident
                .with_native_session(|session, runtime| {
                    let reader = observer(&fixture.path(), session, runtime);
                    let mut file = runtime
                        .block_on(session.open_options().open_read(reader.clone()))
                        .unwrap();
                    let opened = reader.snapshot().unwrap();
                    let column = ColumnRef::new("renamed_text").unwrap();
                    let predicate = PredicateExpr::And(vec![
                        PredicateExpr::IsNotNull {
                            column: column.clone(),
                        },
                        PredicateExpr::Compare {
                            column,
                            op: ComparisonOp::GtEq,
                            value: StatValue::Utf8("m".into()),
                        },
                    ]);
                    let projected = [ColumnRef::new("exact_identifier").unwrap()];
                    assert_eq!(
                        SegmentReusePolicy::for_scan(
                            &predicate,
                            &projected,
                            file.footer().layout().as_ref(),
                            32 << 20
                        )
                        .is_some(),
                        columnar
                    );
                    let projected_text = [ColumnRef::new("renamed_text").unwrap()];
                    assert!(
                        SegmentReusePolicy::for_scan(
                            &predicate,
                            &projected_text,
                            file.footer().layout().as_ref(),
                            32 << 20
                        )
                        .is_none()
                    );
                    let validator = reader.clone();
                    let cache = cached.then(|| {
                        ScanSegmentReuse::new(
                            file.segment_source(),
                            resident.memory().clone(),
                            io_policy(),
                            move || validator.validate_generation(),
                        )
                        .unwrap()
                    });
                    if let Some(cache) = &cache {
                        file = file.with_segment_source(Arc::new(cache.clone()));
                    }
                    let field = vortex::expr::get_item("renamed_text", vortex::expr::root());
                    let filter = vortex::expr::and(
                        vortex::expr::is_not_null(field.clone()),
                        vortex::expr::gt_eq(field, vortex::expr::lit("m")),
                    );
                    let scan = file
                        .scan()
                        .unwrap()
                        .with_ordered(true)
                        .with_projection(
                            vortex::expr::select(["exact_identifier"], vortex::expr::root())
                                .bind(file.dtype())
                                .unwrap(),
                        )
                        .with_filter(filter.bind(file.dtype()).unwrap());
                    let mut context = session.create_execution_ctx();
                    let mut rows = 0;
                    for array in scan.into_array_iter(runtime).unwrap() {
                        let array = array.unwrap();
                        for row in 0..array.len() {
                            assert_eq!(
                                array.execute_scalar(row, &mut context).unwrap(),
                                fixture.selected.execute_scalar(rows, &mut context).unwrap()
                            );
                            rows += 1;
                        }
                    }
                    assert_eq!(rows, fixture.selected.len());
                    if let Some(cache) = &cache {
                        let snapshot = cache.close().unwrap();
                        assert!(snapshot.counters.hits + snapshot.counters.shared_requests > 0);
                        assert!(snapshot.counters.copied_bytes > 0);
                    }
                    drop(file);
                    drop(cache);
                    let completed = reader.close_and_drain(Duration::from_secs(10)).unwrap();
                    reader.validate_generation().unwrap();
                    assert_eq!(
                        completed.failed_read_calls + completed.failed_before_read,
                        0
                    );
                    Ok(completed.completed_read_bytes - opened.completed_read_bytes)
                })
                .unwrap();
            assert_eq!(resident.snapshot().memory.reserved_bytes, 0);
            observed.push(read_bytes);
        }
        if columnar {
            assert!(
                observed[1] < observed[0],
                "columnar duplicate-consumer completed read bytes did not decline: {observed:?}"
            );
        } else {
            assert_eq!(
                observed[1], observed[0],
                "whole-struct Flat already shares its physical segment through projection"
            );
        }
        eprintln!(
            "scan_segment_reuse_native_predicate columnar={columnar} baseline_bytes={} candidate_bytes={}",
            observed[0], observed[1]
        );
    }
}

#[test]
fn prepared_reuse_admission_keeps_one_open_and_does_not_execute() {
    let column = ColumnRef::new("renamed_text").unwrap();
    let predicate = PredicateExpr::And(vec![
        PredicateExpr::IsNotNull {
            column: column.clone(),
        },
        PredicateExpr::IsNotNull { column },
    ]);
    let projected = [ColumnRef::new("exact_identifier").unwrap()];
    for columnar in [false, true] {
        let fixture = Fixture::with_layout(columnar);
        let resident = ResidentVortexSession::new(32 << 20, 1).unwrap();
        let prepared = resident.prepare_file(fixture.path()).unwrap();
        assert_eq!(prepared.has_segment_reuse_field_root(), columnar);
        for _ in 0..2 {
            assert_eq!(
                prepared
                    .segment_reuse_policy(&predicate, &projected)
                    .unwrap()
                    .is_some(),
                columnar
            );
        }
        assert_eq!(resident.snapshot().prepared_source_opens, 1);
        assert_eq!(resident.snapshot().completed_executions, 0);
        std::fs::rename(fixture.path(), fixture.directory.join("original.vortex")).unwrap();
        std::fs::copy(fixture.directory.join("original.vortex"), fixture.path()).unwrap();
        assert!(
            prepared
                .segment_reuse_policy(&predicate, &projected)
                .is_err()
        );
        drop(prepared);
        assert_eq!(resident.snapshot().memory.reserved_bytes, 0);
    }
}

#[test]
fn prepared_cached_execution_closes_per_call_and_rejects_path_replacement_after_scan() {
    let fixture = Fixture::new();
    let resident = ResidentVortexSession::new(32 << 20, 2).unwrap();
    let prepared = resident.prepare_file(fixture.path()).unwrap();
    for _ in 0..2 {
        let ((), snapshot) = prepared
            .with_native_execution_cached(io_policy(), |file, session, runtime| {
                let mut context = session.create_execution_ctx();
                let mut rows = 0;
                for array in file
                    .scan()
                    .unwrap()
                    .with_ordered(true)
                    .into_array_iter(runtime)
                    .unwrap()
                {
                    let array = array.unwrap();
                    for row in 0..array.len() {
                        assert_eq!(
                            array.execute_scalar(row, &mut context).unwrap(),
                            fixture.expected.execute_scalar(rows, &mut context).unwrap()
                        );
                        rows += 1;
                    }
                }
                assert_eq!(rows, ROWS);
                Ok(())
            })
            .unwrap();
        assert!(snapshot.closed);
        assert_eq!(snapshot.retained_entries, 0);
        assert_eq!(snapshot.retention.reserved_bytes, 0);
        assert!(snapshot.counters.downstream_requests > 0);
        let mut summary = "{}".to_string();
        snapshot.annotate(&mut summary).unwrap();
        let value: serde_json::Value = serde_json::from_str(&summary).unwrap();
        assert!(
            value["scan_segment_reuse"]["completed_segment_bytes_scope"]
                .as_str()
                .unwrap()
                .contains("not_filesystem")
        );
    }
    assert_eq!(resident.snapshot().completed_executions, 2);
    let replacement = fixture.directory.join("replacement.vortex");
    std::fs::copy(fixture.path(), &replacement).unwrap();
    assert!(
        prepared
            .with_native_execution_cached(io_policy(), |file, _, _| {
                let rows = file.row_count();
                std::fs::rename(&replacement, fixture.path()).unwrap();
                Ok(rows)
            })
            .is_err()
    );
    assert_eq!(resident.snapshot().completed_executions, 2);
    drop(prepared);
    assert_eq!(resident.snapshot().memory.reserved_bytes, 0);
}

#[test]
fn retained_generation_validator_rejects_a_cached_hit_after_in_place_mutation() {
    use std::io::{Seek as _, SeekFrom, Write as _};
    let fixture = Fixture::new();
    let identity = Arc::new(SourceIdentity::capture(&fixture.path()).unwrap());
    let memory = LiveMemoryPool::new(8192).unwrap();
    let source = source(128);
    let check = identity.clone();
    let cache = ScanSegmentReuse::new(source.clone(), memory.clone(), policy(1024, 2), move || {
        check.validate().map_err(|error| vortex_err!("{error}"))
    })
    .unwrap();
    let runtime = CurrentThreadRuntime::new();
    drop(runtime.block_on(cache.request(0.into())).unwrap());
    let mut writer = std::fs::OpenOptions::new()
        .write(true)
        .open(fixture.path())
        .unwrap();
    writer.seek(SeekFrom::Start(8)).unwrap();
    writer.write_all(&[0x93]).unwrap();
    writer.sync_all().unwrap();
    assert!(runtime.block_on(cache.request(0.into())).is_err());
    assert_eq!(cache.snapshot().unwrap().retention.reserved_bytes, 0);
    assert_eq!(source.calls.load(Ordering::SeqCst), 1);
    drop(cache);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

#[test]
fn optional_cache_table_refusal_still_executes_on_the_same_prepared_source() {
    let fixture = Fixture::new();
    let resident = ResidentVortexSession::new(8 << 20, 1).unwrap();
    let prepared = resident.prepare_file(fixture.path()).unwrap();
    let memory = resident.memory();
    let held = memory
        .reserve(memory.snapshot().limit_bytes - memory.snapshot().reserved_bytes)
        .unwrap();
    let mut calls = 0;
    let (rows, snapshot) = prepared
        .with_native_execution_cached_retry(io_policy(), |file, _, _, _| {
            calls += 1;
            Ok(file.row_count())
        })
        .unwrap();
    assert_eq!(rows, ROWS as u64);
    assert_eq!(calls, 1);
    assert!(snapshot.admission_skipped);
    assert_eq!(snapshot.uncached_replays, 0);
    assert_eq!(resident.snapshot().prepared_source_opens, 1);
    assert_eq!(resident.snapshot().completed_executions, 1);
    drop(held);
    drop(prepared);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

#[test]
fn cache_retry_gate_rejects_corruption_with_a_simultaneous_owned_denial() {
    let fixture = Fixture::new();
    let resident = ResidentVortexSession::new(8 << 20, 1).unwrap();
    let prepared = resident.prepare_file(fixture.path()).unwrap();
    let mut calls = 0;
    let result: Result<((), _)> =
        prepared.with_native_execution_cached_retry(io_policy(), |_, session, _, attempt| {
            calls += 1;
            let denied = session
                .allocator()
                .allocate(8 << 20, Alignment::DEFAULT_ALIGNMENT)
                .unwrap_err();
            assert!(is_owned_reservation_denial(&denied));
            let corruption =
                vortex_err!(InvalidArgument: "corrupt serialized segment with concurrent pressure");
            assert!(!attempt.request_uncached_retry(&corruption));
            Err(crate::resident_session::native_error(corruption))
        });
    assert!(
        result
            .unwrap_err()
            .to_string()
            .contains("corrupt serialized segment")
    );
    assert_eq!(calls, 1);
    assert_eq!(resident.snapshot().completed_executions, 0);
    drop(prepared);
    assert_eq!(resident.snapshot().memory.reserved_bytes, 0);
}

#[test]
fn temporary_provider_drivers_run_native_work_without_reopening_or_oversubscribing() {
    let fixture = Fixture::new();
    for permanent in [false, true] {
        let resident = if permanent {
            ResidentVortexSession::new(8 << 20, 2)
        } else {
            ResidentVortexSession::for_external_cpu_pool(8 << 20, 2)
        }
        .unwrap();
        let prepared = resident.prepare_file(fixture.path()).unwrap();
        let expected_drivers = 2_usize.min(std::thread::available_parallelism().unwrap().get()) - 1;
        let ((rows, ran_elsewhere), drivers) = prepared
            .with_native_execution_temporary_drivers(|file, _, runtime| {
                let ran_elsewhere = if expected_drivers == 0 {
                    false
                } else {
                    let caller = std::thread::current().id();
                    let (send, receive) = std::sync::mpsc::channel();
                    runtime
                        .handle()
                        .spawn(async move {
                            send.send(std::thread::current().id()).unwrap();
                        })
                        .detach();
                    receive.recv_timeout(Duration::from_secs(5)).unwrap() != caller
                };
                Ok((file.row_count(), ran_elsewhere))
            })
            .unwrap();
        assert_eq!(rows, ROWS as u64);
        assert_eq!(drivers, expected_drivers);
        assert_eq!(ran_elsewhere, expected_drivers != 0);
        let (_, snapshot) = prepared
            .with_native_execution_cached_retry_with_drivers(io_policy(), true, |file, _, _, _| {
                Ok(file.row_count())
            })
            .unwrap();
        assert_eq!(snapshot.provider_background_workers, expected_drivers);
        assert_eq!(resident.snapshot().prepared_source_opens, 1);
        assert_eq!(resident.snapshot().completed_executions, 2);
        assert_eq!(
            resident.snapshot().provider_background_workers,
            if permanent { expected_drivers } else { 0 }
        );
        drop(prepared);
        assert_eq!(resident.snapshot().memory.reserved_bytes, 0);
    }
}

#[test]
#[allow(clippy::too_many_lines)] // One native pressure/replay lifecycle is asserted end to end.
fn typed_source_pressure_replays_complete_two_range_native_query_once_without_cache() {
    let directory = std::env::temp_dir().join(format!(
        "shardloom-segment-reuse-pressure-{}-{}",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    ));
    std::fs::create_dir(&directory).unwrap();
    let path = directory.join("source.vortex");
    let row_counts = [32_768_usize, 196_608];
    let runtime = CurrentThreadRuntime::new();
    let session = VortexSession::default().with_handle(runtime.handle());
    let arrays = row_counts.map(|rows| {
        PrimitiveArray::new(
            (0..rows)
                .map(|row| BASE + i64::try_from(row).unwrap())
                .collect::<Vec<_>>(),
            Validity::NonNullable,
        )
        .into_array()
    });
    let mut output = std::fs::File::create(&path).unwrap();
    let mut writer = session
        .write_options()
        .with_strategy(sequential_flat::SequentialNativeFlatLayout::strategy(
            arrays.len(),
        ))
        .with_file_statistics(Vec::new())
        .blocking(&runtime)
        .writer(&mut output, arrays[0].dtype().clone());
    for array in &arrays {
        writer.push(array.clone()).unwrap();
    }
    writer.finish().unwrap();
    drop(output);
    let opened = runtime
        .block_on(session.open_options().open_path(&path))
        .unwrap();
    let specs = opened.footer().segment_map();
    let smallest = specs
        .iter()
        .map(|spec| u64::from(spec.length))
        .min()
        .unwrap();
    let largest = specs
        .iter()
        .map(|spec| u64::from(spec.length))
        .max()
        .unwrap();
    assert!(largest > smallest * 3);
    let budget = largest + smallest / 2 + (64 << 10);
    drop(opened);
    let resident = ResidentVortexSession::for_external_cpu_pool(budget, 1).unwrap();
    let prepared = resident.prepare_file(&path).unwrap();
    let execute = |file: &vortex::file::VortexFile,
                   session: &VortexSession,
                   runtime: &CurrentThreadRuntime,
                   attempt: &mut crate::resident_session::SegmentReuseAttempt|
     -> Result<usize> {
        let mut seen = 0;
        let mut start = 0;
        let mut context = session.create_execution_ctx();
        for (expected, rows) in arrays.iter().zip(row_counts) {
            let mut scan = file
                .scan()
                .unwrap()
                .with_ordered(true)
                .with_concurrency(1)
                .with_row_range(start..start + rows as u64)
                .into_array_iter(runtime)
                .unwrap();
            let mut range_rows = 0;
            for result in &mut scan {
                let array = match result {
                    Ok(array) => array,
                    Err(error) => {
                        attempt.request_uncached_retry(&error);
                        return Err(crate::resident_session::native_error(error));
                    }
                };
                for row in 0..array.len() {
                    assert_eq!(
                        array.execute_scalar(row, &mut context).unwrap(),
                        expected.execute_scalar(range_rows, &mut context).unwrap()
                    );
                    range_rows += 1;
                }
            }
            assert_eq!(range_rows, rows);
            seen += range_rows;
            start += rows as u64;
        }
        Ok(seen)
    };
    let mut calls = 0;
    let (rows, snapshot) = prepared
        .with_native_execution_cached_retry(
            SegmentReusePolicy {
                max_retained_bytes: budget,
                max_segment_bytes: usize::try_from(largest).unwrap(),
                max_entries: 8,
            },
            |file, session, runtime, attempt| {
                calls += 1;
                execute(file, session, runtime, attempt)
            },
        )
        .unwrap();
    assert_eq!(rows, row_counts.into_iter().sum::<usize>());
    assert_eq!(calls, 2);
    assert_eq!(snapshot.uncached_replays, 1);
    assert!(snapshot.counters.copied_bytes >= smallest);
    assert_eq!(snapshot.retention.reserved_bytes, 0);
    assert_eq!(resident.snapshot().prepared_source_opens, 1);
    assert_eq!(resident.snapshot().completed_executions, 1);
    // The same native query completes under the identical budget with retention
    // disabled from the start; no error text or counter delta requests replay.
    let (rows, _) = prepared
        .with_native_execution_cached_retry(
            SegmentReusePolicy {
                max_retained_bytes: 1,
                max_segment_bytes: 1,
                max_entries: 1,
            },
            execute,
        )
        .unwrap();
    assert_eq!(rows, row_counts.into_iter().sum::<usize>());
    drop(prepared);
    assert_eq!(resident.snapshot().memory.reserved_bytes, 0);
    std::fs::remove_file(path).unwrap();
    std::fs::remove_dir(directory).unwrap();
}
