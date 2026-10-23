//! Actual file reads, ordered handoff, cancellation and operation-boundary cleanup.

use super::*;
use crate::resident_session::ResidentVortexSession;
use std::sync::{Mutex, atomic::AtomicUsize, mpsc};
use std::time::Duration;
use vortex::layout::segments::{SegmentFuture, SegmentId, SegmentSource};

pub(super) fn admitted(
    file: &VortexFile,
    request: &VortexQueryPrimitiveRequest,
    session: &VortexSession,
    runtime: &CurrentThreadRuntime,
    memory: &LiveMemoryPool,
    parallelism: usize,
    token: Option<&CancellationToken>,
) -> Result<(Regions, SimpleAggregateStates)> {
    let policy = VortexLocalPrimitiveExecutionPolicy::new_with_memory_gb(parallelism, 1)?;
    let lowering = AggregateLowering::new(request, file.dtype(), policy.resource_envelope())?;
    let states = SimpleAggregateStates::new(
        &lowering.rewrite.aggregate,
        &lowering.plan.projected_columns,
    )?;
    let regions = Regions::admit(
        file,
        request.source_uri.as_ref().unwrap(),
        request,
        &lowering,
        &states,
        &lowering.plan.projected_columns,
        policy,
        session,
        runtime.clone(),
        memory,
        token,
    )?
    .expect("fixture must admit the source-region path");
    Ok((regions, states))
}

pub(super) fn run(regions: Regions, states: &mut SimpleAggregateStates) -> Result<Report> {
    regions.run(
        states,
        &mut NativeNumericAccessorWork::default(),
        &mut Vec::new(),
        &mut 0,
    )
}

enum SourceAction {
    Observe,
    Cancel(CancellationToken),
    Fail,
    Replace { path: PathBuf, replacement: PathBuf },
}

struct ObservedSource {
    inner: Arc<dyn SegmentSource>,
    requests: Arc<AtomicUsize>,
    action: SourceAction,
}

impl SegmentSource for ObservedSource {
    fn request(&self, id: SegmentId) -> SegmentFuture {
        let ordinal = self.requests.fetch_add(1, Ordering::AcqRel);
        if ordinal == 0 {
            match &self.action {
                SourceAction::Observe => {}
                SourceAction::Cancel(token) => token.cancel(),
                SourceAction::Fail => {
                    return Box::pin(async {
                        Err(vortex::error::vortex_err!("region-source-injected-error"))
                    });
                }
                SourceAction::Replace { path, replacement } => {
                    if let Err(error) = std::fs::rename(replacement, path) {
                        return Box::pin(async move { Err(vortex::error::vortex_err!("{error}")) });
                    }
                }
            }
        }
        self.inner.request(id)
    }
}

fn observed_file(
    file: &VortexFile,
    requests: &Arc<AtomicUsize>,
    action: SourceAction,
) -> VortexFile {
    file.clone().with_segment_source(Arc::new(ObservedSource {
        inner: file.segment_source(),
        requests: Arc::clone(requests),
        action,
    }))
}

#[test]
fn scalar_region_cancellation_and_source_error_drain_without_replay() {
    let source = fixture(&[65_536; 4], false, false);
    let request = request(&source, false);
    for parallelism in [1, 4] {
        for failure in ["cancel_before", "cancel_during", "source_error"] {
            let resident =
                ResidentVortexSession::for_external_cpu_pool(32 << 20, parallelism).unwrap();
            let memory = resident.memory().clone();
            let prepared = resident.prepare_file(&source.0).unwrap();
            crate::native_provider_memory::materialize_file_metadata_for_test(prepared.file());
            let retained = memory.snapshot().reserved_bytes;
            let token = CancellationToken::default();
            if failure == "cancel_before" {
                token.cancel();
            }
            let requests = Arc::new(AtomicUsize::new(0));
            let error = prepared
                .with_native_execution(|file, session, runtime| {
                    let action = match failure {
                        "cancel_during" => SourceAction::Cancel(token.clone()),
                        "source_error" => SourceAction::Fail,
                        _ => SourceAction::Observe,
                    };
                    let file = observed_file(file, &requests, action);
                    let (regions, mut states) = admitted(
                        &file,
                        &request,
                        session,
                        runtime,
                        &memory,
                        parallelism,
                        Some(&token),
                    )?;
                    run(regions, &mut states)
                })
                .err()
                .expect("the injected failure must reach the caller");
            let count = requests.load(Ordering::Acquire);
            assert!(
                error.to_string().contains(if failure == "source_error" {
                    "region-source-injected-error"
                } else {
                    "cancel"
                }),
                "{failure}: {error}"
            );
            if failure == "cancel_before" {
                assert_eq!(count, 0);
            } else {
                assert!((1..=4).contains(&count));
                if parallelism == 1 {
                    assert_eq!(count, 1, "no sequential source replay after failure");
                }
            }
            assert_eq!(resident.snapshot().completed_executions, 0);
            assert_eq!(memory.snapshot().reserved_bytes, retained);
            // An explicit new operation succeeds; the failed operation never
            // changes the parent's cancellation flag or replays itself.
            prepared
                .with_native_execution(|file, session, runtime| {
                    let (regions, mut states) =
                        admitted(file, &request, session, runtime, &memory, parallelism, None)?;
                    run(regions, &mut states)?;
                    assert_eq!(
                        payload(&states.result_summary(&[])?)["values"],
                        expected(262_144, false, false)
                    );
                    Ok(())
                })
                .unwrap();
            assert_eq!(memory.snapshot().reserved_bytes, retained);
            drop(prepared);
            drop(resident);
            assert_eq!(memory.snapshot().reserved_bytes, 0);
        }
    }
}

#[test]
fn scalar_region_initial_and_partial_growth_denials_release_every_component_credit() {
    let source = fixture(&[65_536; 2], false, false);
    let request = request(&source, false);
    let resident = ResidentVortexSession::for_external_cpu_pool(32 << 20, 1).unwrap();
    let provider_memory = resident.memory().clone();
    let prepared = resident.prepare_file(&source.0).unwrap();
    crate::native_provider_memory::materialize_file_metadata_for_test(prepared.file());
    let retained = provider_memory.snapshot().reserved_bytes;
    prepared
        .with_native_execution(|file, session, runtime| {
            // Isolate only the region component's pool to deterministically deny its
            // partial growth. The real provider retains its resident memory owner.
            let memory = LiveMemoryPool::new(1 << 20)?;
            let requests = Arc::new(AtomicUsize::new(0));
            let file = observed_file(file, &requests, SourceAction::Observe);
            let policy = VortexLocalPrimitiveExecutionPolicy::new_with_memory_gb(1, 1)?;
            let lowering =
                AggregateLowering::new(&request, file.dtype(), policy.resource_envelope())?;
            let mut states = SimpleAggregateStates::new(
                &lowering.rewrite.aggregate,
                &lowering.plan.projected_columns,
            )?;
            let tiny = LiveMemoryPool::new(1)?;
            assert!(
                Regions::admit(
                    &file,
                    request.source_uri.as_ref().unwrap(),
                    &request,
                    &lowering,
                    &states,
                    &lowering.plan.projected_columns,
                    policy,
                    session,
                    runtime.clone(),
                    &tiny,
                    None,
                )?
                .is_none()
            );
            assert_eq!(requests.load(Ordering::Acquire), 0);
            assert_eq!(tiny.snapshot().reserved_bytes, 0);
            let (regions, _) = admitted(&file, &request, session, runtime, &memory, 1, None)?;
            let available = memory.snapshot().limit_bytes - memory.snapshot().reserved_bytes;
            let pressure = memory.reserve(available - regions.recipe.partial_bytes)?;
            let error = run(regions, &mut states)
                .err()
                .expect("metadata growth must exceed the remaining partial credit");
            assert!(error.to_string().contains("memory"), "{error}");
            assert_eq!(requests.load(Ordering::Acquire), 1);
            assert_eq!(memory.snapshot().denied_reservations, 1);
            assert_eq!(memory.snapshot().reserved_bytes, pressure.bytes());
            drop(pressure);
            assert_eq!(memory.snapshot().reserved_bytes, 0);
            Ok(())
        })
        .unwrap();
    assert_eq!(provider_memory.snapshot().reserved_bytes, retained);
    drop(prepared);
    drop(resident);
    assert_eq!(provider_memory.snapshot().reserved_bytes, 0);
}

struct SkewSource {
    inner: Arc<dyn SegmentSource>,
    requests: AtomicUsize,
    entered: mpsc::Sender<()>,
    release: Mutex<mpsc::Receiver<()>>,
    release_first: mpsc::Sender<()>,
    completion_order: Arc<Mutex<Vec<usize>>>,
}

impl SegmentSource for SkewSource {
    fn request(&self, id: SegmentId) -> SegmentFuture {
        let ordinal = self.requests.fetch_add(1, Ordering::AcqRel);
        let future = self.inner.request(id);
        if ordinal == 0 {
            self.entered.send(()).unwrap();
            self.release
                .lock()
                .unwrap()
                .recv_timeout(Duration::from_secs(10))
                .expect("a second source region must progress while the first is blocked");
        }
        let release = self.release_first.clone();
        let order = Arc::clone(&self.completion_order);
        Box::pin(async move {
            let result = future.await;
            order.lock().unwrap().push(ordinal);
            if ordinal == 1 {
                release.send(()).unwrap();
            }
            result
        })
    }
}

#[test]
fn scalar_region_actual_reads_overlap_under_skew_and_merge_in_source_order() {
    let source = fixture(&[65_536; 2], false, false);
    let request = request(&source, false);
    let resident = ResidentVortexSession::for_external_cpu_pool(32 << 20, 4).unwrap();
    let memory = resident.memory().clone();
    let prepared = resident.prepare_file(&source.0).unwrap();
    crate::native_provider_memory::materialize_file_metadata_for_test(prepared.file());
    let retained = memory.snapshot().reserved_bytes;
    prepared
        .with_native_execution(|file, session, runtime| {
            let (entered_tx, entered_rx) = mpsc::channel();
            let (release_tx, release_rx) = mpsc::channel();
            let completion_order = Arc::new(Mutex::new(Vec::new()));
            let file = file.clone().with_segment_source(Arc::new(SkewSource {
                inner: file.segment_source(),
                requests: AtomicUsize::new(0),
                entered: entered_tx,
                release: Mutex::new(release_rx),
                release_first: release_tx,
                completion_order: Arc::clone(&completion_order),
            }));
            let (mut regions, mut states) =
                admitted(&file, &request, session, runtime, &memory, 4, None)?;
            assert_eq!(resident.snapshot().provider_background_workers, 0);
            for index in 0..2 {
                let recipe = Arc::clone(&regions.recipe);
                let scan = Arc::clone(&regions.scan);
                let reader = Arc::clone(&regions.reader);
                let runtime = regions.runtime.clone();
                let range = regions.boundaries[index]..regions.boundaries[index + 1];
                assert_eq!(
                    regions
                        .jobs
                        .submit(recipe.partial_bytes, move |context, lease| {
                            execute_region(
                                context, lease, &recipe, &scan, &reader, None, &runtime, range,
                            )
                        })?,
                    index as u64
                );
                if index == 0 {
                    entered_rx.recv_timeout(Duration::from_secs(10)).unwrap();
                }
            }
            let mut report = Report::default();
            let mut next_row = 0;
            let mut splits = Vec::new();
            let mut work = NativeNumericAccessorWork::default();
            let mut max_rows = 0;
            for expected_end in [65_536, 131_072] {
                regions.merge_next(
                    &mut states,
                    &mut work,
                    &mut splits,
                    &mut max_rows,
                    &mut report,
                    &mut next_row,
                )?;
                assert_eq!(next_row, expected_end);
            }
            assert_eq!(*completion_order.lock().unwrap(), vec![1, 0]);
            assert_eq!(regions.jobs.outstanding(), 0);
            assert!(regions.jobs.pool_snapshot().unwrap().peak_active_workers >= 2);
            assert_eq!(splits.len(), 2);
            assert_eq!(
                payload(&states.result_summary(&[])?)["values"],
                expected(131_072, false, false)
            );
            regions.jobs.retire()?;
            Ok(())
        })
        .unwrap();
    assert_eq!(memory.snapshot().reserved_bytes, retained);
    drop(prepared);
    drop(resident);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

#[test]
fn scalar_region_generation_replacement_before_and_during_execution_is_rejected() {
    for during in [false, true] {
        let source = fixture(&[65_536; 2], false, false);
        let replacement = fixture(&[65_536; 2], true, false);
        let request = request(&source, false);
        let resident = ResidentVortexSession::for_external_cpu_pool(32 << 20, 1).unwrap();
        let memory = resident.memory().clone();
        let prepared = resident.prepare_file(&source.0).unwrap();
        crate::native_provider_memory::materialize_file_metadata_for_test(prepared.file());
        let retained = memory.snapshot().reserved_bytes;
        let requests = Arc::new(AtomicUsize::new(0));
        if !during {
            std::fs::rename(&replacement.0, &source.0).unwrap();
        }
        let error = prepared
            .with_native_execution(|file, session, runtime| {
                let file = observed_file(
                    file,
                    &requests,
                    SourceAction::Replace {
                        path: source.0.clone(),
                        replacement: replacement.0.clone(),
                    },
                );
                let (regions, mut states) =
                    admitted(&file, &request, session, runtime, &memory, 1, None)?;
                run(regions, &mut states)
            })
            .err()
            .expect("a changed file generation must never publish a result");
        assert!(error.to_string().contains("changed"), "{error}");
        assert_eq!(requests.load(Ordering::Acquire), usize::from(during));
        assert_eq!(resident.snapshot().completed_executions, 0);
        assert_eq!(memory.snapshot().reserved_bytes, retained);
        drop(prepared);
        drop(resident);
        assert_eq!(memory.snapshot().reserved_bytes, 0);
    }
}
