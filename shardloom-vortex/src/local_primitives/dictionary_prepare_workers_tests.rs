//! Exact values, ordered pressure retirement and held-file worker ownership.
use super::*;
use crate::{
    VortexAggregateExpression, VortexAggregateHavingExpr, VortexAggregateOrderExpr,
    VortexSimpleAggregateMeasure, VortexSimpleAggregateRequest,
    resident_session::ResidentVortexSession,
};
use dictionary_prepare_workers::{DictionaryPrepareWorkers, WORKER_START_TEST_HOOK};
use prepared_aggregate::prepare_aggregate_in_session;
use shardloom_core::{ColumnRef, ComparisonOp, DatasetUri, PredicateExpr, StatValue};
use shardloom_exec::{compute_pool::CancellationToken, live_memory::LiveMemoryPool};
use std::{
    collections::BTreeMap,
    fs,
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
};
use vortex::{
    VortexSessionDefault as _,
    array::{
        IntoArray as _,
        arrays::{StructArray, VarBinViewArray},
        validity::Validity,
    },
    file::WriteOptionsSessionExt as _,
    io::{
        runtime::{BlockingRuntime as _, single::SingleThreadRuntime},
        session::RuntimeSessionExt as _,
    },
    session::VortexSession,
};

const COLS: [&str; 4] = ["k", "c", "l", "m"];
const MEMORY: u64 = 64 << 20;

fn request() -> VortexSimpleAggregateRequest {
    VortexSimpleAggregateRequest::grouped(
        Vec::new(),
        vec![
            VortexSimpleAggregateMeasure::new("count", None, "c".into()),
            VortexSimpleAggregateMeasure::new(
                "avg",
                Some(ColumnRef::new("Referer").unwrap()),
                "l".into(),
            )
            .with_value_transform("length"),
            VortexSimpleAggregateMeasure::new(
                "min",
                Some(ColumnRef::new("Referer").unwrap()),
                "m".into(),
            ),
        ],
    )
    .with_group_expressions(vec![VortexAggregateExpression::new(
        "k".into(),
        ColumnRef::new("Referer").unwrap(),
        "url_domain",
    )])
    .with_having(vec![VortexAggregateHavingExpr::new(
        "c",
        ComparisonOp::GtEq,
        "2",
    )])
    .with_order_by(vec![
        VortexAggregateOrderExpr::new("l", true),
        VortexAggregateOrderExpr::new("k", false),
    ])
}
fn query(path: &Path) -> VortexQueryPrimitiveRequest {
    let mut query = VortexQueryPrimitiveRequest::simple_aggregate(
        DatasetUri::new(path.display().to_string()).unwrap(),
        request(),
    )
    .with_source_order_limit(2);
    query.predicate = Some(PredicateExpr::Compare {
        column: ColumnRef::new("Referer").unwrap(),
        op: ComparisonOp::NotEq,
        value: StatValue::Utf8(String::new()),
    });
    query
}
fn policy(parallelism: usize) -> VortexLocalPrimitiveExecutionPolicy {
    let mut policy = VortexLocalPrimitiveExecutionPolicy::new(parallelism).unwrap();
    policy.resource_envelope.memory_budget_bytes = MEMORY;
    policy
}
fn chunk(values: &[String]) -> vortex::array::ArrayRef {
    StructArray::new(
        ["Referer"].into(),
        vec![VarBinViewArray::from_iter_str(values.iter()).into_array()],
        values.len(),
        Validity::NonNullable,
    )
    .into_array()
}
fn payload(report: &VortexLocalPrimitiveExecutionReport) -> serde_json::Value {
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
fn state_values(states: &mut GroupedAggregateStates<'_>) -> serde_json::Value {
    let (_, summary) = states.result_row_count_and_summary(Some(2)).unwrap();
    serde_json::from_str::<serde_json::Value>(&summary).unwrap()["values"].clone()
}
fn provider_lane(parallelism: usize) -> bool {
    parallelism >= 3 && std::thread::available_parallelism().unwrap().get() >= 3
}

fn assert_native_io_credits_return(memory: &LiveMemoryPool) {
    // Ordinary sessions do not synchronously join upstream blocking I/O. A
    // cancelled read keeps its buffer charged until its closure/result drops;
    // ResidentWorkerGroup joins CPU drivers, not that separate I/O pool. The
    // immediate post-error assertion still verifies dictionary-job refunds.
    let started = std::time::Instant::now();
    let initial = memory.snapshot().reserved_bytes;
    while memory.snapshot().reserved_bytes != 0 {
        assert!(
            started.elapsed() < std::time::Duration::from_secs(5),
            "native I/O credits did not return: initial={initial}, current={:?}",
            memory.snapshot()
        );
        std::thread::yield_now();
    }
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

fn assert_workers(work: &serde_json::Value, parallelism: usize) {
    let jobs = &work["aggregate_dictionary_preparation_workers"];
    assert!(jobs["submitted_chunks"].as_u64().unwrap() > 0);
    assert_eq!(jobs["submitted_chunks"], jobs["completed_chunks"]);
    assert!(jobs["peak_outstanding_chunks"].as_u64().unwrap() <= 2);
    assert_eq!(
        jobs["cpu_ceiling"],
        2 + usize::from(provider_lane(parallelism))
    );
    assert_eq!(
        jobs["provider_background_worker_grant"],
        usize::from(provider_lane(parallelism))
    );
    if provider_lane(parallelism) {
        assert_eq!(work["aggregate_provider_background_workers"], 1);
        assert!(
            work["aggregate_provider_cpu_scope"]
                .as_str()
                .unwrap()
                .contains("shared_three_lane_CPU_grant")
        );
    }
    assert_eq!(jobs["retired_to_same_serial_consumer"], false);
}

#[test]
fn dictionary_preparation_preserves_chunk_order_and_drains_before_pressure_retirement() {
    let columns = vec!["Referer".to_owned()];
    let request = request();
    let inputs = (0..4)
        .map(|batch| {
            chunk(
                &(0..(512 * (batch + 1)))
                    .map(|row| format!("http://{}.test/{:04}-{}", row % 2, row % 97, batch))
                    .collect::<Vec<_>>(),
            )
        })
        .collect::<Vec<_>>();
    for pressure_after in [None, Some(0), Some(1)] {
        let memory = LiveMemoryPool::new(MEMORY).unwrap();
        let mut serial =
            GroupedAggregateStates::new(&request, Some(2), &columns, false, false).unwrap();
        let mut state =
            GroupedAggregateStates::new(&request, Some(2), &columns, false, false).unwrap();
        let operation = CancellationToken::default();
        let mut workers = DictionaryPrepareWorkers::admit(
            &state,
            inputs[0].dtype(),
            &columns,
            policy(2),
            &memory,
            Some(&operation),
        )
        .unwrap()
        .unwrap();
        let mut pressure = None;
        for (index, input) in inputs.iter().enumerate() {
            if pressure_after == Some(index) {
                // The next chunk needs more than draining the older job frees.
                let snapshot = memory.snapshot();
                pressure = Some(
                    memory
                        .reserve(snapshot.limit_bytes - snapshot.reserved_bytes - 1024)
                        .unwrap(),
                );
            }
            workers.before_next(&mut state).unwrap();
            if !workers.submit(input, &mut state).unwrap() {
                assert!(
                    state
                        .update_compact_direct_from_chunk(input, &columns, None)
                        .unwrap()
                );
            }
            assert!(
                serial
                    .update_compact_direct_from_chunk(input, &columns, None)
                    .unwrap()
            );
        }
        workers.finish(&mut state).unwrap();
        assert_eq!(state_values(&mut state), state_values(&mut serial));
        assert_eq!(workers.retired(), pressure_after.is_some());
        drop(workers);
        drop(pressure);
        assert!(!operation.is_cancelled());
        assert_eq!(memory.snapshot().reserved_bytes, 0);
    }
}

#[test]
fn dictionary_preparation_declines_nullable_unordered_and_single_cpu_before_allocation() {
    use vortex::array::dtype::{DType, Nullability, StructFields};
    let columns = vec!["Referer".to_owned()];
    let mut request = request();
    let memory = LiveMemoryPool::new(MEMORY).unwrap();
    let dtype =
        |field, root| DType::Struct(StructFields::new(["Referer"].into(), vec![field]), root);
    let native = dtype(
        DType::Utf8(Nullability::NonNullable),
        Nullability::NonNullable,
    );
    let state = GroupedAggregateStates::new(&request, Some(2), &columns, false, false).unwrap();
    for (dtype, parallelism) in [
        (native.clone(), 1),
        (
            dtype(DType::Utf8(Nullability::Nullable), Nullability::NonNullable),
            2,
        ),
        (
            dtype(DType::Utf8(Nullability::NonNullable), Nullability::Nullable),
            2,
        ),
    ] {
        assert!(
            DictionaryPrepareWorkers::admit(
                &state,
                &dtype,
                &columns,
                policy(parallelism),
                &memory,
                None
            )
            .unwrap()
            .is_none()
        );
        assert_eq!(memory.snapshot().reserved_bytes, 0);
    }
    let pressure = memory.reserve(MEMORY).unwrap();
    assert!(
        DictionaryPrepareWorkers::admit(&state, &native, &columns, policy(2), &memory, None)
            .unwrap()
            .is_none()
    );
    drop(pressure);
    request.order_by.clear();
    let state = GroupedAggregateStates::new(&request, Some(2), &columns, false, false).unwrap();
    assert!(
        DictionaryPrepareWorkers::admit(&state, &native, &columns, policy(2), &memory, None)
            .unwrap()
            .is_none()
    );
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

#[test]
fn dictionary_preparation_drains_before_native_dictionary_and_resumes_in_source_order() {
    use vortex::array::arrays::{DictArray, PrimitiveArray};
    let values = ["http://é.test/z", "http://東京.test/é", "http://é.test/a"];
    let first = chunk(&values.map(str::to_owned));
    let codes = PrimitiveArray::new(vec![2_u32, 1, 0, 1], Validity::NonNullable).into_array();
    let dict = DictArray::try_new(codes, VarBinViewArray::from_iter_str(values).into_array())
        .unwrap()
        .into_array();
    let middle =
        StructArray::new(["Referer"].into(), vec![dict], 4, Validity::NonNullable).into_array();
    let inputs = [first.clone(), middle, first];
    let request = request();
    let columns = vec!["Referer".to_owned()];
    let memory = LiveMemoryPool::new(MEMORY).unwrap();
    let mut state = GroupedAggregateStates::new(&request, Some(2), &columns, false, false).unwrap();
    let mut serial =
        GroupedAggregateStates::new(&request, Some(2), &columns, false, false).unwrap();
    let mut workers = DictionaryPrepareWorkers::admit(
        &state,
        inputs[0].dtype(),
        &columns,
        policy(2),
        &memory,
        None,
    )
    .unwrap()
    .unwrap();
    for (index, input) in inputs.iter().enumerate() {
        workers.before_next(&mut state).unwrap();
        let consumed = workers.submit(input, &mut state).unwrap();
        assert_eq!(consumed, index != 1);
        if !consumed {
            assert!(
                state
                    .update_compact_direct_from_chunk(input, &columns, None)
                    .unwrap()
            );
        }
        assert!(
            serial
                .update_compact_direct_from_chunk(input, &columns, None)
                .unwrap()
        );
    }
    workers.finish(&mut state).unwrap();
    assert_eq!(state_values(&mut state), state_values(&mut serial));
    assert!(!workers.retired());
    drop(workers);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

struct Fixture {
    dir: PathBuf,
    path: PathBuf,
    expected: serde_json::Value,
}
impl Fixture {
    fn new() -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let dir = std::env::temp_dir().join(format!(
            "shardloom-dictionary-preparation-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir(&dir).unwrap();
        let path = dir.join("source.vortex");
        let runtime = SingleThreadRuntime::default();
        let session = VortexSession::default().with_handle(runtime.handle());
        let mut oracle = BTreeMap::<String, (u32, u32, String)>::new();
        let mut chunks = Vec::new();
        for batch in 0..16 {
            let mut values = vec![String::new()];
            for row in 0..256 {
                let domain = if row % 2 == 0 {
                    "one.test"
                } else {
                    "longer-two.test"
                };
                let value = format!("http://{domain}/{batch:02}/{:04}", (255 - row) % 113);
                let entry = oracle.entry(domain.into()).or_insert((0, 0, value.clone()));
                entry.0 += 1;
                entry.1 += u32::try_from(value.len()).unwrap();
                if value < entry.2 {
                    entry.2.clone_from(&value);
                }
                values.push(value);
            }
            chunks.push(chunk(&values));
        }
        let mut file = fs::File::create_new(&path).unwrap();
        let mut writer = session
            .write_options()
            .with_strategy(native_flat_layout::SequentialNativeFlatLayout::strategy(
                chunks.len(),
            ))
            .with_file_statistics(Vec::new())
            .blocking(&runtime)
            .writer(&mut file, chunks[0].dtype().clone());
        for input in chunks {
            writer.push(input).unwrap();
        }
        writer.finish().unwrap();
        let mut expected = oracle.into_iter().map(|(key, (count, sum, min))| serde_json::json!({"k":key,"c":count,"l":f64::from(sum)/f64::from(count),"m":min})).collect::<Vec<_>>();
        expected.sort_by(|a, b| {
            b["l"]
                .as_f64()
                .unwrap()
                .total_cmp(&a["l"].as_f64().unwrap())
                .then_with(|| a["k"].as_str().unwrap().cmp(b["k"].as_str().unwrap()))
        });
        Self {
            dir,
            path,
            expected: expected.into(),
        }
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.dir);
    }
}
struct ClearHooks;
impl Drop for ClearHooks {
    fn drop(&mut self) {
        WORKER_START_TEST_HOOK.with(|hook| drop(hook.borrow_mut().take()));
        dictionary_prepare_workers::SUBMIT_TEST_PRESSURE.with(|flag| flag.set(false));
        aggregate_count_workers::SOURCE_SCAN_TEST_FAULT.with(|fault| fault.set(None));
    }
}

#[test]
fn dictionary_preparation_native_ordinary_prepared_owned_values_and_owner_lifetime() {
    let fixture = Fixture::new();
    let query = query(&fixture.path);
    for parallelism in [1, 2, 3] {
        let report =
            execute_vortex_local_primitive_with_policy(&query, policy(parallelism)).unwrap();
        let work = payload(&report);
        assert_eq!(work["values"], fixture.expected);
        if parallelism > 1 {
            assert_workers(&work, parallelism);
        }
    }
    for (external, parallelism) in [(false, 2), (true, 2), (false, 3), (true, 3)] {
        let session = if external {
            ResidentVortexSession::for_external_cpu_pool(MEMORY, parallelism)
        } else {
            ResidentVortexSession::new(MEMORY, parallelism)
        }
        .unwrap();
        let memory = session.memory().clone();
        let prepared = prepare_aggregate_in_session(&query, policy(parallelism), &session).unwrap();
        let executed = prepared.execute().unwrap();
        let work = payload(&executed.report);
        assert_eq!(work["values"], fixture.expected);
        if external {
            assert_workers(&work, parallelism);
            assert_eq!(
                executed.runtime.provider_background_workers,
                usize::from(provider_lane(parallelism))
            );
        } else {
            assert!(
                work.get("aggregate_dictionary_preparation_workers")
                    .is_none()
            );
        }
        let owned = prepared.execute_owned().unwrap();
        let json = owned
            .result
            .to_bounded_json(&COLS.map(str::to_owned), 16 * 1024)
            .unwrap();
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(json.value()).unwrap(),
            fixture.expected
        );
        assert_eq!(prepared.snapshot().prepared_source_opens, 1);
        drop(prepared);
        drop(session);
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(
                owned
                    .result
                    .to_bounded_json(&COLS.map(str::to_owned), 16 * 1024)
                    .unwrap()
                    .value()
            )
            .unwrap(),
            fixture.expected
        );
        drop(owned);
        drop(json);
        drop(executed);
        assert_eq!(memory.snapshot().reserved_bytes, 0);
    }
}

#[test]
fn dictionary_preparation_native_pressure_restores_provider_only_after_pool_retirement() {
    for parallelism in [2, 3] {
        native_pressure(parallelism);
    }
}

fn native_pressure(parallelism: usize) {
    let _clear = ClearHooks;
    let fixture = Fixture::new();
    let session = ResidentVortexSession::for_external_cpu_pool(MEMORY, parallelism).unwrap();
    let memory = session.memory().clone();
    let prepared =
        prepare_aggregate_in_session(&query(&fixture.path), policy(parallelism), &session).unwrap();
    dictionary_prepare_workers::SUBMIT_TEST_PRESSURE.with(|flag| flag.set(true));
    let result = prepared.execute().unwrap();
    let work = payload(&result.report);
    assert_eq!(work["values"], fixture.expected);
    assert_eq!(
        work["aggregate_dictionary_preparation_workers"]["retired_to_same_serial_consumer"],
        true
    );
    assert!(
        work["aggregate_provider_background_workers"]
            .as_u64()
            .unwrap()
            > 0
    );
    assert!(
        work["aggregate_provider_cpu_scope"]
            .as_str()
            .unwrap()
            .contains(if provider_lane(parallelism) {
                "dictionary_worker_joined_before_serial_consumer;existing_provider_driver_retained"
            } else {
                "dictionary_preparation_workers_retired_before_provider_resume"
            })
    );
    assert!(work["aggregate_workers_partition_source_replays"].is_null());
    assert_eq!(prepared.snapshot().prepared_source_opens, 1);
    drop(prepared);
    drop(session);
    drop(result);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

#[test]
fn dictionary_preparation_native_active_worker_observes_operation_cancel_and_recovers() {
    for parallelism in [2, 3] {
        native_cancel(parallelism);
    }
}

fn native_cancel(parallelism: usize) {
    let _clear = ClearHooks;
    let fixture = Fixture::new();
    let session = ResidentVortexSession::for_external_cpu_pool(MEMORY, parallelism).unwrap();
    let memory = session.memory().clone();
    let prepared =
        prepare_aggregate_in_session(&query(&fixture.path), policy(parallelism), &session).unwrap();
    let baseline = memory.snapshot().reserved_bytes;
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
        .expect("active job cancels");
    assert!(error.to_string().contains("cancel"), "{error}");
    assert!(observed.load(Ordering::Acquire));
    assert_eq!(session.snapshot().completed_executions, 0);
    assert_eq!(memory.snapshot().reserved_bytes, baseline);
    let fresh = CancellationToken::default();
    let result = prepared.execute_cancellable(&fresh).unwrap();
    assert_eq!(payload(&result.report)["values"], fixture.expected);
    assert!(!fresh.is_cancelled());
    drop(result);
    drop(prepared);
    drop(session);
    assert_native_io_credits_return(&memory);
}

#[test]
fn dictionary_preparation_native_committed_source_failures_do_not_replay() {
    use aggregate_count_workers::{SOURCE_SCAN_TEST_FAULT, SourceScanTestFault};
    let _clear = ClearHooks;
    let fixture = Fixture::new();
    for (parallelism, fault) in [
        (2, SourceScanTestFault::OwnedDenial),
        (2, SourceScanTestFault::CorruptionWithConcurrentDenial),
        (3, SourceScanTestFault::OwnedDenial),
        (3, SourceScanTestFault::CorruptionWithConcurrentDenial),
    ] {
        let session = ResidentVortexSession::for_external_cpu_pool(MEMORY, parallelism).unwrap();
        let memory = session.memory().clone();
        let prepared =
            prepare_aggregate_in_session(&query(&fixture.path), policy(parallelism), &session)
                .unwrap();
        let baseline = memory.snapshot().reserved_bytes;
        SOURCE_SCAN_TEST_FAULT.with(|value| value.set(Some(fault)));
        let error = prepared
            .execute_owned()
            .err()
            .expect("committed source error propagates");
        assert!(
            error.to_string().contains("memory reservation denied")
                || error.to_string().contains("source corruption"),
            "{error}"
        );
        assert!(SOURCE_SCAN_TEST_FAULT.with(std::cell::Cell::get).is_none());
        assert_eq!(session.snapshot().completed_executions, 0);
        assert_eq!(memory.snapshot().reserved_bytes, baseline);
        assert_eq!(
            payload(&prepared.execute().unwrap().report)["values"],
            fixture.expected
        );
        drop(prepared);
        drop(session);
        assert_native_io_credits_return(&memory);
    }
}
