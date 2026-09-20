//! Deterministic progress/ownership proofs, separate from load measurements.

use super::*;
use std::{
    sync::mpsc,
    thread,
    time::{Duration, Instant},
};
use vortex::{
    array::{
        IntoArray as _,
        arrays::{PrimitiveArray, StructArray},
        dtype::FieldNames,
        memory::HostAllocator as _,
        validity::Validity,
    },
    file::WriteOptionsSessionExt as _,
    layout::layouts::flat::writer::FlatLayoutStrategy,
};

const DEADLINE: Duration = Duration::from_secs(10);
static FIXTURE: AtomicU64 = AtomicU64::new(0);

struct Fixture {
    directory: std::path::PathBuf,
    expected: ArrayRef,
}
impl Fixture {
    fn new() -> Self {
        let directory = std::env::temp_dir().join(format!(
            "shardloom-concurrent-serving-{}-{}",
            std::process::id(),
            FIXTURE.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir(&directory).unwrap();
        let expected = StructArray::try_new(
            FieldNames::from(["renamed_exact_key"]),
            vec![
                PrimitiveArray::new(
                    (0..4096_i64).map(|v| i64::MAX - v).collect::<Vec<_>>(),
                    Validity::NonNullable,
                )
                .into_array(),
            ],
            4096,
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
    fn path(&self) -> std::path::PathBuf {
        self.directory.join("source.vortex")
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.directory);
    }
}

fn receive<T>(receiver: &mpsc::Receiver<T>) -> T {
    receiver
        .recv_timeout(DEADLINE)
        .expect("serving fixture must make progress")
}
fn wait_for(mut ready: impl FnMut() -> bool) {
    let start = Instant::now();
    while !ready() {
        assert!(
            start.elapsed() < DEADLINE,
            "bounded state transition did not occur"
        );
        thread::yield_now();
    }
}
fn exact(result: &OwnedVortexResultBatch, fixture: &Fixture) {
    let mut context = result.create_execution_ctx();
    let mut index = 0;
    for array in result.arrays() {
        for row in 0..array.len() {
            assert_eq!(
                array.execute_scalar(row, &mut context).unwrap(),
                fixture
                    .expected
                    .execute_scalar(index, &mut context)
                    .unwrap()
            );
            index += 1;
        }
    }
    assert_eq!(index, 4096);
}

#[test]
fn serving_general_projection_completes_while_another_real_read_is_held() {
    if thread::available_parallelism().unwrap().get() < 2 {
        return;
    }
    let fixture = Fixture::new();
    let session = ResidentVortexSession::with_serving_policy(
        32 << 20,
        4,
        ResidentServingPolicy {
            reserve_metadata_lane: false,
            ..Default::default()
        },
    )
    .unwrap();
    let source = session.prepare_file(fixture.path()).unwrap();
    let memory = session.memory().clone();
    let (entered, entry) = mpsc::channel();
    let (release, released) = mpsc::channel();
    thread::scope(|threads| {
        let source_ref = &source;
        let held = threads.spawn(move || {
            source_ref.with_native_execution_controlled(
                &CancellationToken::default(),
                |file, context| {
                    let array = file
                        .scan()
                        .map_err(native_error)?
                        .with_concurrency(1)
                        .into_array_iter(context.runtime())
                        .map_err(native_error)?
                        .next()
                        .unwrap()
                        .map_err(native_error)?;
                    assert!(!array.is_empty());
                    // Composed operators borrow the same context. Public recursive
                    // admission fails immediately instead of deadlocking on this caller.
                    source_ref.with_admitted_native_execution(context, |nested, _| {
                        assert_eq!(nested.row_count(), 4096);
                        Ok(())
                    })?;
                    assert!(
                        source_ref
                            .prepare_count()
                            .execute()
                            .unwrap_err()
                            .to_string()
                            .contains("nested serving admission")
                    );
                    entered.send(()).unwrap();
                    receive(&released);
                    Ok(())
                },
            )
        });
        receive(&entry);
        let result = source
            .prepare_projection(&["renamed_exact_key"], 4096, 1 << 20)
            .unwrap()
            .execute()
            .unwrap();
        exact(&result, &fixture);
        assert!(session.admission_snapshot().unwrap().peak_active_calls >= 2);
        release.send(()).unwrap();
        held.join().unwrap().unwrap();
        drop(result);
    });
    assert_eq!(session.snapshot().prepared_source_opens, 1);
    assert_eq!(session.io_snapshot().unwrap().active_requests, 0);
    assert_eq!(session.admission_snapshot().unwrap().active_cpu_lanes, 0);
    drop(source);
    drop(session);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

#[test]
fn serving_metadata_progresses_before_native_writer_commit_and_cancel() {
    if thread::available_parallelism().unwrap().get() < 2 {
        return;
    }
    for cancel in [false, true] {
        let fixture = Fixture::new();
        let session = ResidentVortexSession::with_serving_policy(
            32 << 20,
            2,
            ResidentServingPolicy::default(),
        )
        .unwrap();
        let source = session.prepare_file(fixture.path()).unwrap();
        let memory = session.memory().clone();
        let output = fixture.directory.join("output.vortex");
        let cancellation = CancellationToken::default();
        let (entered, entry) = mpsc::channel();
        let (release, released) = mpsc::channel();
        thread::scope(|threads| {
            let token = cancellation.clone();
            let source_ref = &source;
            let output_ref = &output;
            let writer = threads.spawn(move || {
                source_ref.with_native_execution_controlled(&token, |file, context| {
                    shardloom_core::write_workspace_safe_bytes_with_producer(
                        output_ref.parent().unwrap(),
                        output_ref,
                        false,
                        "concurrent native writer fixture",
                        |sink| {
                            let mut writer = context
                                .native_session()
                                .write_options()
                                .with_strategy(Arc::new(FlatLayoutStrategy::default()))
                                .blocking(context.runtime())
                                .writer(sink, file.dtype().clone());
                            let mut scan = file
                                .scan()
                                .map_err(native_error)?
                                .with_concurrency(1)
                                .into_array_iter(context.runtime())
                                .map_err(native_error)?;
                            let array = scan.next().unwrap().map_err(native_error)?;
                            writer.push(array).map_err(native_error)?;
                            entered.send(()).unwrap();
                            receive(&released);
                            context.check_cancelled()?;
                            for array in scan {
                                writer
                                    .push(array.map_err(native_error)?)
                                    .map_err(native_error)?;
                            }
                            writer.finish().map_err(native_error)?;
                            Ok(())
                        },
                    )?;
                    Ok(())
                })
            });
            receive(&entry);
            for _ in 0..8 {
                assert_eq!(source.prepare_count().execute().unwrap(), 4096);
            }
            assert_eq!(session.admission_snapshot().unwrap().active_calls, 1);
            if cancel {
                cancellation.cancel();
            }
            release.send(()).unwrap();
            let result = writer.join().unwrap();
            assert_eq!(result.is_err(), cancel);
        });
        assert_eq!(output.exists(), !cancel);
        if !cancel {
            let written = session.prepare_file(&output).unwrap();
            exact(
                &written
                    .prepare_projection(&["renamed_exact_key"], 4096, 1 << 20)
                    .unwrap()
                    .execute()
                    .unwrap(),
                &fixture,
            );
        }
        assert_eq!(
            std::fs::read_dir(&fixture.directory).unwrap().count(),
            if cancel { 1 } else { 2 }
        );
        assert!(session.admission_snapshot().unwrap().peak_active_cpu_lanes <= 2);
        assert_eq!(session.io_snapshot().unwrap().active_requests, 0);
        drop(source);
        drop(session);
        assert_eq!(memory.snapshot().reserved_bytes, 0);
    }
}

#[test]
fn serving_queue_cancellation_removes_waiter_preserves_fifo_and_rejects_overflow() {
    let memory = LiveMemoryPool::new(1 << 20).unwrap();
    let admission = serving_admission::Admission::new(
        ResidentServingPolicy {
            max_queued_calls: 2,
            ..Default::default()
        },
        1,
        &memory,
    )
    .unwrap();
    let held = admission
        .admit(CallClass::General, 0, &CancellationToken::default())
        .unwrap();
    let first_cancel = CancellationToken::default();
    let (done, results) = mpsc::channel();
    thread::scope(|threads| {
        let a = Arc::clone(&admission);
        let token = first_cancel.clone();
        let sent = done.clone();
        threads.spawn(move || {
            let r = a.admit(CallClass::General, 0, &token);
            sent.send((1, r.is_ok())).unwrap();
        });
        wait_for(|| admission.snapshot().queued_calls == 1);
        let a = Arc::clone(&admission);
        let sent = done.clone();
        threads.spawn(move || {
            let r = a.admit(CallClass::General, 0, &CancellationToken::default());
            sent.send((2, r.is_ok())).unwrap();
        });
        wait_for(|| admission.snapshot().queued_calls == 2);
        let a = Arc::clone(&admission);
        assert!(
            threads
                .spawn(move || a
                    .admit(CallClass::General, 0, &CancellationToken::default())
                    .is_err())
                .join()
                .unwrap()
        );
        first_cancel.cancel();
        assert_eq!(receive(&results), (1, false));
        assert_eq!(admission.snapshot().queued_calls, 1);
        drop(held);
        assert_eq!(receive(&results), (2, true));
    });
    assert_eq!(admission.snapshot().active_calls, 0);
    assert_eq!(admission.snapshot().queued_call_bytes, 0);
    assert_eq!(admission.snapshot().cancelled_queued_calls, 1);
    drop(admission);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

#[test]
#[allow(clippy::too_many_lines)] // Release and join every held caller before asserting outcomes.
fn full_general_waiter_queue_preserves_reserved_metadata_progress_and_p1_bounds() {
    let fixture = Fixture::new();
    for parallelism in [1, 2] {
        if thread::available_parallelism().unwrap().get() < parallelism {
            continue;
        }
        let session = ResidentVortexSession::with_serving_policy(
            32 << 20,
            parallelism,
            ResidentServingPolicy {
                max_queued_calls: 1,
                max_queued_call_bytes: serving_admission::TICKET_METADATA_BYTES,
                ..Default::default()
            },
        )
        .unwrap();
        let source = session.prepare_file(fixture.path()).unwrap();
        let memory = session.memory().clone();
        let queued_cancellation = CancellationToken::default();
        let (entered, entry) = mpsc::channel();
        let (release, released) = mpsc::channel();
        let (metadata_done, metadata_result) = mpsc::channel();
        let outcomes = thread::scope(|threads| {
            let held_session = session.clone();
            let held = threads.spawn(move || -> Result<()> {
                let _context = held_session
                    .0
                    .enter(CallClass::General, CancellationToken::default())?;
                entered.send(()).map_err(native_error)?;
                released.recv_timeout(DEADLINE).map_err(native_error)?;
                Ok(())
            });
            let observed_holder = entry.recv_timeout(DEADLINE).is_ok();
            let queued_source = &source;
            let cancellation = queued_cancellation.clone();
            let queued = threads.spawn(move || {
                queued_source
                    .with_native_execution_controlled(&cancellation, |file, _| Ok(file.row_count()))
            });
            let started = Instant::now();
            while session.admission_snapshot().unwrap().queued_calls != 1
                && started.elapsed() < DEADLINE
            {
                thread::yield_now();
            }
            let waiting = session.admission_snapshot().unwrap();
            let count_source = &source;
            let metadata = threads.spawn(move || {
                let result = count_source.prepare_count().execute();
                let _ = metadata_done.send(result);
            });
            let metadata_before_release = metadata_result.recv_timeout(Duration::from_secs(2));
            let overflow_rejected = waiting.queued_calls == 1
                && session
                    .0
                    .enter(CallClass::General, CancellationToken::default())
                    .is_err();
            // Cleanup precedes assertions, including timeout regressions. A
            // failed observation cannot strand a waiter behind this fixture.
            if !observed_holder || waiting.queued_calls != 1 {
                queued_cancellation.cancel();
            }
            let _ = release.send(());
            let held_result = held.join();
            let queued_result = queued.join();
            let metadata_joined = metadata.join();
            (
                observed_holder,
                waiting,
                metadata_before_release,
                overflow_rejected,
                held_result,
                queued_result,
                metadata_joined,
            )
        });
        let (observed_holder, waiting, metadata, overflow, held, queued, metadata_joined) =
            outcomes;
        assert!(observed_holder);
        assert_eq!(waiting.queued_calls, 1);
        assert_eq!(
            waiting.queued_call_bytes,
            serving_admission::TICKET_METADATA_BYTES
        );
        assert!(overflow);
        held.unwrap().unwrap();
        assert_eq!(queued.unwrap().unwrap(), 4096);
        metadata_joined.unwrap();
        let metadata = metadata.expect("metadata admission must finish before holder release");
        if parallelism == 2 {
            assert_eq!(metadata.unwrap(), 4096);
        } else {
            assert!(metadata.is_err(), "P1 has no independent metadata lane");
        }
        let after = session.admission_snapshot().unwrap();
        assert_eq!(after.queued_calls, 0);
        assert_eq!(after.queued_call_bytes, 0);
        assert_eq!(after.active_cpu_lanes, 0);
        assert!(after.peak_active_cpu_lanes <= parallelism);
        assert_eq!(after.peak_queued_calls, 1);
        assert_eq!(
            after.peak_queued_call_bytes,
            serving_admission::TICKET_METADATA_BYTES
        );
        drop(source);
        drop(session);
        assert_eq!(memory.snapshot().reserved_bytes, 0);
    }
}

#[test]
fn serving_queued_public_projection_cancels_before_provider_work_and_remains_reusable() {
    let fixture = Fixture::new();
    let session =
        ResidentVortexSession::with_serving_policy(32 << 20, 1, ResidentServingPolicy::default())
            .unwrap();
    let source = session.prepare_file(fixture.path()).unwrap();
    let memory = session.memory().clone();
    let held = session
        .0
        .enter(CallClass::General, CancellationToken::default())
        .unwrap();
    let cancellation = CancellationToken::default();
    let (done, result) = mpsc::channel();
    let before = session.io_snapshot().unwrap();
    thread::scope(|threads| {
        let token = cancellation.clone();
        let source = &source;
        threads.spawn(move || {
            let r = source
                .prepare_projection(&["renamed_exact_key"], 4096, 1 << 20)
                .unwrap()
                .execute_with_cancellation(&token);
            done.send(r.is_err()).unwrap();
        });
        wait_for(|| session.admission_snapshot().unwrap().queued_calls == 1);
        cancellation.cancel();
        assert!(receive(&result));
        assert_eq!(session.io_snapshot().unwrap(), before);
        drop(held);
    });
    exact(
        &source
            .prepare_projection(&["renamed_exact_key"], 4096, 1 << 20)
            .unwrap()
            .execute()
            .unwrap(),
        &fixture,
    );
    session.close_admission();
    assert!(source.prepare_count().execute().is_err());
    drop(source);
    drop(session);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

#[test]
fn serving_cancelled_blocking_completion_keeps_io_and_buffer_owners_until_return() {
    let memory = LiveMemoryPool::new(1 << 20).unwrap();
    let allocator = Arc::new(ReservedHostAllocator::new(memory.clone()));
    let budget = io_ownership::IoBudget::new(1, 4096);
    let token = CancellationToken::default();
    let scope = io_ownership::IoScope::new(Arc::clone(&budget), &memory, token.clone()).unwrap();
    let scope_metadata = memory.snapshot().reserved_bytes;
    let runtime = CurrentThreadRuntime::new();
    let job = scope.admit(4096).unwrap();
    assert!(scope.admit(1).is_err());
    let (entered, entry) = mpsc::channel();
    let (release, released) = mpsc::channel();
    let task = runtime.handle().spawn_blocking(move || {
        let buffer = allocator.allocate(4096, Alignment::none()).unwrap();
        entered.send(()).unwrap();
        receive(&released);
        io_ownership::ReadCompletion {
            result: buffer,
            _job: Some(job),
        }
    });
    receive(&entry);
    token.cancel();
    drop(task);
    assert_eq!(budget.snapshot().active_requests, 1);
    assert!(memory.snapshot().reserved_bytes >= 4096);
    release.send(()).unwrap();
    scope.close_and_drain(&runtime);
    assert_eq!(budget.snapshot().active_requests, 0);
    assert_eq!(budget.snapshot().active_bytes, 0);
    assert_eq!(memory.snapshot().reserved_bytes, scope_metadata);
    assert!(scope.admit(1).is_err());
    drop(scope);
    drop(runtime);
    drop(budget);
    // The job decrements pending and wakes the drain before Rust drops its
    // final Arc<IoScope> field. The read and buffer are already gone, but that
    // destructor epilogue can briefly retain the scope's metadata reservation.
    // Require eventual release without racing that final field destruction.
    wait_for(|| memory.snapshot().reserved_bytes == 0);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

#[test]
fn serving_retained_result_delivery_uses_cpu_admission_after_source_drop() {
    let fixture = Fixture::new();
    let session =
        ResidentVortexSession::with_serving_policy(32 << 20, 1, ResidentServingPolicy::default())
            .unwrap();
    let source = session.prepare_file(fixture.path()).unwrap();
    let result = source
        .prepare_projection(&["renamed_exact_key"], 4096, 1 << 20)
        .unwrap()
        .execute()
        .unwrap();
    let memory = session.memory().clone();
    drop(source);
    let owner = Arc::clone(&result.runtime);
    drop(session);
    let held = owner
        .enter(CallClass::General, CancellationToken::default())
        .unwrap();
    let (done, completed) = mpsc::channel();
    thread::scope(|threads| {
        let worker = threads.spawn(move || {
            let json = result
                .to_bounded_json(&["renamed_exact_key".to_owned()], 1 << 20)
                .unwrap();
            let rows: serde_json::Value = serde_json::from_str(json.value()).unwrap();
            assert_eq!(rows.as_array().unwrap().len(), 4096);
            assert_eq!(rows[0]["renamed_exact_key"], i64::MAX);
            assert_eq!(rows[4095]["renamed_exact_key"], i64::MAX - 4095);
            done.send(()).unwrap();
        });
        wait_for(|| owner.serving.as_ref().unwrap().snapshot().queued_calls == 1);
        assert!(completed.try_recv().is_err());
        drop(held);
        receive(&completed);
        worker.join().unwrap();
    });
    assert_eq!(
        owner.serving.as_ref().unwrap().snapshot().active_cpu_lanes,
        0
    );
    drop(owner);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

#[test]
fn serving_foreign_context_and_queued_source_generation_change_fail_before_reads() {
    let fixture = Fixture::new();
    let session =
        ResidentVortexSession::with_serving_policy(32 << 20, 1, ResidentServingPolicy::default())
            .unwrap();
    let source = session.prepare_file(fixture.path()).unwrap();
    let foreign =
        ResidentVortexSession::with_serving_policy(32 << 20, 1, ResidentServingPolicy::default())
            .unwrap();
    let context = foreign
        .0
        .enter(CallClass::General, CancellationToken::default())
        .unwrap();
    assert!(
        source
            .with_admitted_native_execution(&context, |_, _| -> Result<()> {
                panic!("foreign context must not execute")
            })
            .is_err()
    );
    drop(context);
    let held = session
        .0
        .enter(CallClass::General, CancellationToken::default())
        .unwrap();
    let before = session.io_snapshot().unwrap();
    thread::scope(|threads| {
        let source_ref = &source;
        let queued = threads.spawn(move || {
            source_ref
                .prepare_projection(&["renamed_exact_key"], 4096, 1 << 20)
                .unwrap()
                .execute()
        });
        wait_for(|| session.admission_snapshot().unwrap().queued_calls == 1);
        std::fs::OpenOptions::new()
            .write(true)
            .open(fixture.path())
            .unwrap()
            .set_len(1)
            .unwrap();
        drop(held);
        assert!(queued.join().unwrap().is_err());
    });
    assert_eq!(session.io_snapshot().unwrap(), before);
    assert!(source.prepare_count().execute().is_err());
}

#[path = "resident_serving_load_harness.rs"]
mod load_harness;
