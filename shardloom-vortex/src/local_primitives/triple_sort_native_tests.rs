//! Real file/prepared/owned-result and failure evidence for complete triple keys.
use super::*;
use crate::{
    VortexLocalPrimitiveRowExportFormat, local_primitives as runtime,
    resident_session::ResidentVortexSession,
};
use runtime::prepared_aggregate::prepare_aggregate_in_session;
use shardloom_exec::compute_pool::CancellationToken;
use std::{
    fs,
    path::PathBuf,
    sync::atomic::{AtomicU64, Ordering},
};
use vortex::{
    VortexSessionDefault as _,
    file::WriteOptionsSessionExt as _,
    io::{
        runtime::{BlockingRuntime as _, single::SingleThreadRuntime},
        session::RuntimeSessionExt as _,
    },
};

struct Fixture {
    dir: PathBuf,
    path: PathBuf,
}
impl Fixture {
    fn new() -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let dir = std::env::temp_dir().join(format!(
            "shardloom-triple-sort-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir(&dir).unwrap();
        let path = dir.join("source.vortex");
        let runtime = SingleThreadRuntime::default();
        let session = VortexSession::default().with_handle(runtime.handle());
        let chunks = (0..4)
            .map(|index| {
                let codes = if index % 2 == 0 {
                    vec![0, 1, 2, 2]
                } else {
                    vec![2, 1, 0, 0]
                };
                let texts = if index % 2 == 0 {
                    ["zeta", "zz", "alpha"]
                } else {
                    ["alpha", "zz", "zeta"]
                };
                chunk(
                    false,
                    PrimitiveArray::new(
                        vec![u64::MAX, 0, u64::MAX - 1, u64::MAX],
                        Validity::NonNullable,
                    )
                    .into_array(),
                    dictionary(&codes, &texts),
                    integers(&[-1, 0, 61, -1]),
                )
            })
            .collect::<Vec<_>>();
        let mut file = fs::File::create_new(&path).unwrap();
        let mut writer = session
            .write_options()
            .with_strategy(
                runtime::native_flat_layout::SequentialNativeFlatLayout::strategy(chunks.len()),
            )
            .with_file_statistics(Vec::new())
            .blocking(&runtime)
            .writer(&mut file, chunks[0].dtype().clone());
        for chunk in chunks {
            writer.push(chunk).unwrap();
        }
        assert_eq!(writer.finish().unwrap().row_count(), 16);
        Self { dir, path }
    }
    fn query(&self) -> crate::VortexQueryPrimitiveRequest {
        crate::VortexQueryPrimitiveRequest::simple_aggregate(
            shardloom_core::DatasetUri::new(self.path.display().to_string()).unwrap(),
            request(false).with_offset(1),
        )
        .with_source_order_limit(2)
    }
    fn expected() -> serde_json::Value {
        serde_json::json!([
            {"subject_code":u64::MAX-1,"phrase_label":"alpha","minute_slot":1,"frequency":4},
            {"subject_code":u64::MAX,"phrase_label":"alpha","minute_slot":59,"frequency":4}
        ])
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.dir);
    }
}
fn policy() -> VortexLocalPrimitiveExecutionPolicy {
    let mut policy = VortexLocalPrimitiveExecutionPolicy::new(2).unwrap();
    policy.resource_envelope.memory_budget_bytes = 16 << 20;
    policy
}
fn payload(report: &runtime::VortexLocalPrimitiveExecutionReport) -> serde_json::Value {
    assert!(!report.fallback_execution_allowed);
    assert!(!report.has_errors());
    serde_json::from_str(
        report
            .result_summary
            .as_ref()
            .unwrap()
            .rsplit_once(" values=")
            .unwrap()
            .1,
    )
    .unwrap()
}
fn assert_route(value: &serde_json::Value) {
    assert_eq!(
        value["aggregate_workers_family"],
        "complete_numeric_minute_string_partition_sort_reduce"
    );
    assert_eq!(value["aggregate_workers_triple_rows"], 16);
    assert!(
        value["aggregate_workers_triple_source_chunks"]
            .as_u64()
            .unwrap()
            > 0
    );
    assert_eq!(
        value["aggregate_workers_triple_submitted_partition_tasks"],
        64
    );
    assert_eq!(value["aggregate_workers_triple_joined_partition_tasks"], 64);
}
fn rendered(result: &crate::resident_session::OwnedVortexResultBatch) -> serde_json::Value {
    serde_json::from_str(
        result
            .to_bounded_json(
                &["subject_code", "phrase_label", "minute_slot", "frequency"].map(str::to_string),
                4096,
            )
            .unwrap()
            .value(),
    )
    .unwrap()
}

#[test]
fn triple_sort_native_ordinary_prepared_owned_result_and_export_preserve_unsigned_keys() {
    let fixture = Fixture::new();
    let query = fixture.query();
    let ordinary =
        payload(&runtime::execute_vortex_local_primitive_with_policy(&query, policy()).unwrap());
    assert_eq!(ordinary["values"], Fixture::expected());
    assert_route(&ordinary);
    let session = ResidentVortexSession::for_external_cpu_pool(16 << 20, 2).unwrap();
    let memory = session.memory().clone();
    let prepared = prepare_aggregate_in_session(&query, policy(), &session).unwrap();
    for completed in 1..=2 {
        let result = prepared.execute().unwrap();
        assert!(result.native_io_certificate.is_certified());
        assert!(!result.native_io_certificate.fallback_attempted);
        let work = payload(&result.report);
        assert_eq!(work["values"], Fixture::expected());
        assert_route(&work);
        assert_eq!(result.runtime.completed_executions, completed);
    }
    let completed = prepared.execute_owned().unwrap();
    assert_eq!(rendered(&completed.result), Fixture::expected());
    drop(prepared);
    fs::remove_file(&fixture.path).unwrap();
    let target = fixture.dir.join("result.vortex");
    assert_eq!(
        completed
            .write(&target, VortexLocalPrimitiveRowExportFormat::Vortex, false)
            .unwrap()
            .rows_written,
        2
    );
    let reader = ResidentVortexSession::new(16 << 20, 1).unwrap();
    let source = reader.prepare_file(&target).unwrap();
    let exported = source
        .prepare_projection(
            &["subject_code", "phrase_label", "minute_slot", "frequency"],
            10,
            4096,
        )
        .unwrap()
        .execute()
        .unwrap();
    assert_eq!(rendered(&exported), Fixture::expected());
    drop(session);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

#[test]
fn triple_sort_native_source_errors_do_not_replay_and_prepared_recovers() {
    use runtime::aggregate_count_workers::{SOURCE_SCAN_TEST_FAULT, SourceScanTestFault};
    struct ClearFault;
    impl Drop for ClearFault {
        fn drop(&mut self) {
            SOURCE_SCAN_TEST_FAULT.with(|fault| fault.set(None));
        }
    }
    let _clear = ClearFault;
    let fixture = Fixture::new();
    let session = ResidentVortexSession::for_external_cpu_pool(16 << 20, 2).unwrap();
    let memory = session.memory().clone();
    let query = fixture.query();
    let prepared = prepare_aggregate_in_session(&query, policy(), &session).unwrap();
    let retained = memory.snapshot().reserved_bytes;
    for (fault, message) in [
        (
            SourceScanTestFault::OwnedDenial,
            "memory reservation denied",
        ),
        (
            SourceScanTestFault::CorruptionWithConcurrentDenial,
            "injected source corruption",
        ),
    ] {
        let before = session.snapshot().completed_executions;
        SOURCE_SCAN_TEST_FAULT.with(|hook| hook.set(Some(fault)));
        let error = prepared
            .execute_owned()
            .err()
            .expect("committed triple input cannot replay");
        assert!(error.to_string().contains(message), "{error}");
        assert!(SOURCE_SCAN_TEST_FAULT.with(std::cell::Cell::get).is_none());
        assert_eq!(session.snapshot().completed_executions, before);
        assert_eq!(memory.snapshot().reserved_bytes, retained);
        let fresh = prepared.execute().unwrap();
        let work = payload(&fresh.report);
        assert_eq!(work["values"], Fixture::expected());
        assert_route(&work);
    }
    drop(prepared);
    drop(session);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

#[test]
fn triple_sort_native_running_worker_observes_operation_cancel_and_refunds() {
    use runtime::triple_sort_workers::WORKER_START_TEST_HOOK;
    use std::sync::atomic::AtomicBool;
    struct ClearHook;
    impl Drop for ClearHook {
        fn drop(&mut self) {
            WORKER_START_TEST_HOOK.with(|hook| drop(hook.borrow_mut().take()));
        }
    }
    let _clear = ClearHook;
    let fixture = Fixture::new();
    let session = ResidentVortexSession::for_external_cpu_pool(16 << 20, 2).unwrap();
    let memory = session.memory().clone();
    let query = fixture.query();
    let prepared = prepare_aggregate_in_session(&query, policy(), &session).unwrap();
    let retained = memory.snapshot().reserved_bytes;
    let cancellation = CancellationToken::default();
    let operation = cancellation.clone();
    let observed = Arc::new(AtomicBool::new(false));
    let worker_observed = Arc::clone(&observed);
    WORKER_START_TEST_HOOK.with(|hook| {
        *hook.borrow_mut() = Some(Box::new(move |worker| {
            operation.cancel();
            worker_observed.store(worker.check_cancelled().is_err(), Ordering::Release);
        }));
    });
    let error = prepared
        .execute_cancellable(&cancellation)
        .err()
        .expect("caller cancellation reaches active reducer");
    assert!(error.to_string().contains("cancel"), "{error}");
    assert!(observed.load(Ordering::Acquire));
    assert!(WORKER_START_TEST_HOOK.with(|hook| hook.borrow().is_none()));
    assert_eq!(session.snapshot().completed_executions, 0);
    assert_eq!(memory.snapshot().reserved_bytes, retained);
    let fresh_token = CancellationToken::default();
    let result = prepared.execute_cancellable(&fresh_token).unwrap();
    assert_route(&payload(&result.report));
    assert_eq!(payload(&result.report)["values"], Fixture::expected());
    assert!(!fresh_token.is_cancelled());
    drop(prepared);
    drop(session);
    assert!(!fresh_token.is_cancelled());
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}
