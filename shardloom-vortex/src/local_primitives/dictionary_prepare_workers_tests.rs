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
        expr::stats::Stat,
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
const NUMERIC_COLS: [&str; 7] = ["k", "c", "l", "m", "x", "s", "n"];
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
fn numeric_request() -> VortexSimpleAggregateRequest {
    let mut request = request();
    request.measures[2].value_transform = Some("length".into());
    for (function, alias) in [("max", "x"), ("sum", "s"), ("count", "n")] {
        request.measures.push(
            VortexSimpleAggregateMeasure::new(
                function,
                Some(ColumnRef::new("Referer").unwrap()),
                alias.into(),
            )
            .with_value_transform("length"),
        );
    }
    request
}
fn numeric_query(path: &Path) -> VortexQueryPrimitiveRequest {
    let mut query = query(path);
    query.simple_aggregate = Some(numeric_request());
    query
}
fn policy(parallelism: usize) -> VortexLocalPrimitiveExecutionPolicy {
    let mut policy =
        VortexLocalPrimitiveExecutionPolicy::new_with_memory_gb(parallelism, 4).unwrap();
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

fn assert_native_io_credits_return(memory: &LiveMemoryPool, expected: u64) {
    // The ordinary native operation now drives its I/O drain before returning,
    // including failure and cancellation. No result owner remains in these
    // fixtures, so waiting outside the runtime would conceal a lifetime defect.
    assert_eq!(memory.snapshot().reserved_bytes, expected);
}

#[test]
fn aggregate_utf8_context_dictionary_preparation_refuses_and_retries() {
    use vortex::{array::VortexSessionExecute as _, encodings::zstd::Zstd, mask::Mask};

    let values = (0..128)
        .map(|row| format!("http://{}.test/{row}:{}", row % 2, "x".repeat(1024)))
        .collect::<Vec<_>>();
    let source = VarBinViewArray::from_iter_str(values.iter().map(String::as_str));
    let encoded = Zstd::from_var_bin_view_without_dict(
        &source,
        0,
        8,
        &mut VortexSession::default().create_execution_ctx(),
    )
    .unwrap()
    .into_array();
    let filtered = encoded
        .filter(Mask::from_indices(128, [1, 3, 120, 124]))
        .unwrap();
    let input =
        StructArray::new(["Referer"].into(), vec![filtered], 4, Validity::NonNullable).into_array();
    let request = request();
    let columns = vec!["Referer".to_owned()];
    let owner = ResidentVortexSession::for_external_cpu_pool(MEMORY, 2).unwrap();
    let memory = owner.memory().clone();
    owner
        .with_native_session(|session, _| {
            let mut state = GroupedAggregateStates::new(&request, Some(2), &columns, false, false)?;
            state.native_execution_ctx = Some(native_numeric_execution_ctx(session));
            let mut workers = DictionaryPrepareWorkers::admit(
                &state,
                input.dtype(),
                &columns,
                policy(2),
                &memory,
                None,
            )?
            .unwrap();
            let before = memory.snapshot().reserved_bytes;
            let pressure = memory.reserve(MEMORY - before - 1)?;
            let error = workers
                .submit(&input, &mut state)
                .expect_err("dictionary preparation must retain query allocation policy");
            assert!(
                error.to_string().contains("memory reservation denied"),
                "{error}"
            );
            assert!(memory.snapshot().denied_reservations > 0);
            drop(pressure);
            assert_eq!(memory.snapshot().reserved_bytes, before);
            assert!(workers.submit(&input, &mut state)?);
            workers.finish(&mut state)?;
            assert_eq!(
                state_values(&mut state),
                serde_json::json!([
                    {"k":"0.test","c":2,"l":f64::from(u32::try_from(values[120].len()).unwrap()),"m":values[120]},
                    {"k":"1.test","c":2,"l":f64::from(u32::try_from(values[1].len()).unwrap()),"m":values[1]},
                ])
            );
            drop((workers, state));
            Ok(())
        })
        .unwrap();
    drop(owner);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

#[test]
fn dictionary_preparation_active_worker_cancel_refunds_immediately_without_native_io() {
    let _clear = ClearHooks;
    let columns = vec!["Referer".to_owned()];
    let request = request();
    let input = chunk(
        &(0..1024)
            .map(|row| format!("http://{}.test/{row}", row % 2))
            .collect::<Vec<_>>(),
    );
    let memory = LiveMemoryPool::new(MEMORY).unwrap();
    let mut state = GroupedAggregateStates::new(&request, Some(2), &columns, false, false).unwrap();
    let operation = CancellationToken::default();
    let cancellation = operation.clone();
    let observed = Arc::new(AtomicBool::new(false));
    let worker_observed = Arc::clone(&observed);
    WORKER_START_TEST_HOOK.with(|hook| {
        *hook.borrow_mut() = Some(Box::new(move |worker| {
            cancellation.cancel();
            worker_observed.store(worker.check_cancelled().is_err(), Ordering::Release);
        }));
    });
    let mut workers = DictionaryPrepareWorkers::admit(
        &state,
        input.dtype(),
        &columns,
        policy(2),
        &memory,
        Some(&operation),
    )
    .unwrap()
    .unwrap();
    assert!(workers.submit(&input, &mut state).unwrap());
    let error = workers.finish(&mut state).unwrap_err();
    assert!(error.to_string().contains("cancel"), "{error}");
    assert!(operation.is_cancelled());
    assert!(observed.load(Ordering::Acquire));
    drop(workers);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

fn assert_workers(work: &serde_json::Value, parallelism: usize) {
    let partials = parallelism.min(std::thread::available_parallelism().unwrap().get()) >= 4;
    let workers = if partials { 2 } else { 1 };
    let jobs = &work["aggregate_dictionary_preparation_workers"];
    assert!(jobs["submitted_chunks"].as_u64().unwrap() > 0);
    assert_eq!(jobs["submitted_chunks"], jobs["completed_chunks"]);
    assert!(jobs["peak_outstanding_chunks"].as_u64().unwrap() <= workers + 1);
    assert_eq!(
        jobs["cpu_ceiling"],
        1 + workers + u64::from(provider_lane(parallelism))
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
                .contains(if partials {
                    "shared_four_lane_CPU_grant"
                } else {
                    "shared_three_lane_CPU_grant"
                })
        );
    }
    assert_eq!(jobs["retired_to_same_serial_consumer"], false);
    assert_eq!(jobs["dictionary_partial_recipe_admitted"], partials);
    assert_eq!(jobs["dictionary_background_workers"], workers);
    assert_eq!(
        jobs["worker_dense_partial_chunks"].as_u64().unwrap() > 0,
        partials
    );
}

#[test]
fn regex_dictionary_workers_preserve_nonmatching_urls_and_complete_measures() {
    if std::thread::available_parallelism().unwrap().get() < 4 {
        return;
    }
    let mut request = request();
    request.group_expressions[0].function = "regex_domain".into();
    let columns = vec!["Referer".to_owned()];
    let texts = (0..256)
        .map(|row| {
            if row % 2 == 0 {
                "http://example.test".to_owned()
            } else {
                format!("http://example.test/{row:04}")
            }
        })
        .collect::<Vec<_>>();
    let input = chunk(&texts);
    let memory = LiveMemoryPool::new(MEMORY).unwrap();
    let mut states =
        GroupedAggregateStates::new(&request, Some(2), &columns, false, false).unwrap();
    let mut serial =
        GroupedAggregateStates::new(&request, Some(2), &columns, false, false).unwrap();
    let mut workers =
        DictionaryPrepareWorkers::admit(&states, input.dtype(), &columns, policy(4), &memory, None)
            .unwrap()
            .unwrap();
    assert!(workers.submit(&input, &mut states).unwrap());
    workers.finish(&mut states).unwrap();
    assert!(
        serial
            .update_compact_direct_from_chunk(&input, &columns, None)
            .unwrap()
    );
    let expected = serde_json::json!([
        {"k":"example.test","c":128,"l":24.0,"m":"http://example.test/0001"},
        {"k":"http://example.test","c":128,"l":19.0,"m":"http://example.test"},
    ]);
    assert_eq!(state_values(&mut states), expected);
    assert_eq!(state_values(&mut serial), expected);
    let mut summary = "{}".to_owned();
    workers.annotate_summary(&mut summary).unwrap();
    let summary: serde_json::Value = serde_json::from_str(&summary).unwrap();
    assert_eq!(
        summary["aggregate_dictionary_preparation_workers"]["worker_dense_partial_groups"],
        2
    );
    drop(workers);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

#[test]
fn dictionary_preparation_dense_out_of_order_completion_keeps_ordered_merge() {
    if std::thread::available_parallelism().unwrap().get() < 4 {
        return;
    }
    let _clear = ClearHooks;
    let mut request = request();
    request.measures.push(VortexSimpleAggregateMeasure::new(
        "max",
        Some(ColumnRef::new("Referer").unwrap()),
        "mx".into(),
    ));
    let columns = vec!["Referer".to_owned()];
    let memory = LiveMemoryPool::new(MEMORY).unwrap();
    let inputs = (0..3)
        .map(|batch| {
            chunk(
                &(0..128)
                    .map(|row| format!("https://{}.test/{row}/{}", row % 2, "x".repeat(batch)))
                    .collect::<Vec<_>>(),
            )
        })
        .collect::<Vec<_>>();
    let mut state = GroupedAggregateStates::new(&request, Some(2), &columns, false, false).unwrap();
    let mut serial =
        GroupedAggregateStates::new(&request, Some(2), &columns, false, false).unwrap();
    let mut workers = DictionaryPrepareWorkers::admit(
        &state,
        inputs[0].dtype(),
        &columns,
        policy(4),
        &memory,
        None,
    )
    .unwrap()
    .unwrap();
    let (send, receive) = std::sync::mpsc::channel();
    // With two workers, the second finishes and starts the third while the
    // first is held. The third releases the first; caller merge stays ordered.
    WORKER_START_TEST_HOOK.with(|hook| {
        *hook.borrow_mut() = Some(Box::new(move |_| {
            receive
                .recv_timeout(std::time::Duration::from_secs(10))
                .unwrap();
        }));
    });
    for (index, input) in inputs.iter().enumerate() {
        if index == 2 {
            let send = send.clone();
            WORKER_START_TEST_HOOK
                .with(|hook| *hook.borrow_mut() = Some(Box::new(move |_| send.send(()).unwrap())));
        }
        workers.before_next(&mut state).unwrap();
        assert!(workers.submit(input, &mut state).unwrap());
        assert!(
            serial
                .update_compact_direct_from_chunk(input, &columns, None)
                .unwrap()
        );
    }
    workers.finish(&mut state).unwrap();
    assert_eq!(state_values(&mut state), state_values(&mut serial));
    let mut summary = "{}".to_owned();
    workers.annotate_summary(&mut summary).unwrap();
    let summary: serde_json::Value = serde_json::from_str(&summary).unwrap();
    let jobs = &summary["aggregate_dictionary_preparation_workers"];
    assert_eq!(jobs["worker_dense_partial_chunks"], 3);
    assert_eq!(jobs["worker_dense_partial_groups"], 6);
    assert_eq!(jobs["cpu_ceiling"], 4);
    assert_eq!(jobs["peak_outstanding_chunks"], 3);
    drop(workers);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
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
        Self::with_statistics(Vec::new())
    }
    fn numeric_expected(&self) -> serde_json::Value {
        self.expected.as_array().unwrap().iter().map(|row| {
            let length = row["m"].as_str().unwrap().len();
            // This fixture uses fixed-width paths, so every string for a domain
            // has this same byte length. Count and sum came from the row oracle.
            serde_json::json!({"k":row["k"],"c":row["c"],"l":row["l"],"m":length,"x":length,"s":row["l"].as_f64().unwrap() * f64::from(u32::try_from(row["c"].as_u64().unwrap()).unwrap()),"n":row["c"]})
        }).collect::<Vec<_>>().into()
    }

    fn with_statistics(statistics: Vec<Stat>) -> Self {
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
            .with_file_statistics(statistics)
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
    for parallelism in [1, 2, 3, 4, 12] {
        let report =
            execute_vortex_local_primitive_with_policy(&query, policy(parallelism)).unwrap();
        let work = payload(&report);
        assert_eq!(work["values"], fixture.expected);
        if parallelism > 1 {
            assert_workers(&work, parallelism);
        }
    }
    for (external, parallelism) in [
        (false, 2),
        (true, 2),
        (false, 3),
        (true, 3),
        (false, 4),
        (true, 4),
        (false, 12),
        (true, 12),
    ] {
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
fn dictionary_preparation_numeric_native_ordinary_prepared_owned_and_pressure() {
    let _clear = ClearHooks;
    let fixture = Fixture::new();
    let query = numeric_query(&fixture.path);
    let expected = fixture.numeric_expected();
    let host = std::thread::available_parallelism().unwrap().get();
    let serial_session = ResidentVortexSession::new(MEMORY, 1).unwrap();
    let serial_prepared = prepare_aggregate_in_session(&query, policy(1), &serial_session).unwrap();
    let serial_owned = serial_prepared.execute_owned().unwrap();
    let expected_dtype = serial_owned.result.dtype().clone();
    let expected_schema = serial_owned.result.schema_json().unwrap().value().clone();
    drop((serial_owned, serial_prepared, serial_session));
    for parallelism in [1, 2, 3, 4, 12] {
        let report =
            execute_vortex_local_primitive_with_policy(&query, policy(parallelism)).unwrap();
        let work = payload(&report);
        assert_eq!(work["values"], expected);
        if parallelism.min(host) >= 4 {
            let jobs = &work["aggregate_dictionary_preparation_workers"];
            assert_eq!(jobs["dictionary_partial_recipe_admitted"], true);
            assert_eq!(jobs["dictionary_background_workers"], 2);
            assert_eq!(jobs["cpu_ceiling"], 4);
            assert_eq!(jobs["window_limit_chunks"], 3);
            assert!(jobs["worker_dense_partial_chunks"].as_u64().unwrap() > 0);
            assert_eq!(jobs["submitted_chunks"], jobs["completed_chunks"]);
            assert!(
                work["aggregate_provider_cpu_scope"]
                    .as_str()
                    .unwrap()
                    .contains("shared_four_lane_CPU_grant")
            );
        }
    }
    for external in [false, true] {
        let session = if external {
            ResidentVortexSession::for_external_cpu_pool(MEMORY, 4)
        } else {
            ResidentVortexSession::new(MEMORY, 4)
        }
        .unwrap();
        let memory = session.memory().clone();
        let prepared = prepare_aggregate_in_session(&query, policy(4), &session).unwrap();
        let executed = prepared.execute().unwrap();
        assert!(executed.native_io_certificate.is_certified());
        assert_eq!(payload(&executed.report)["values"], expected);
        let owned = prepared.execute_owned().unwrap();
        assert_eq!(owned.result.dtype(), &expected_dtype);
        assert_eq!(
            owned.result.schema_json().unwrap().value(),
            &expected_schema
        );
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(
                owned
                    .result
                    .to_bounded_json(&NUMERIC_COLS.map(str::to_owned), 16 * 1024)
                    .unwrap()
                    .value()
            )
            .unwrap(),
            expected
        );
        if external {
            dictionary_prepare_workers::SUBMIT_TEST_PRESSURE.with(|flag| flag.set(true));
            let retired = prepared.execute().unwrap();
            let work = payload(&retired.report);
            assert_eq!(work["values"], expected);
            assert_eq!(
                work["aggregate_dictionary_preparation_workers"]["retired_to_same_serial_consumer"],
                true
            );
            assert!(work["aggregate_workers_partition_source_replays"].is_null());
        }
        assert_eq!(prepared.snapshot().prepared_source_opens, 1);
        drop((prepared, session, executed));
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(
                owned
                    .result
                    .to_bounded_json(&NUMERIC_COLS.map(str::to_owned), 16 * 1024)
                    .unwrap()
                    .value()
            )
            .unwrap(),
            expected
        );
        drop(owned);
        assert_eq!(memory.snapshot().reserved_bytes, 0);
    }
}

#[test]
fn dictionary_preparation_native_pruned_input_starts_no_workers() {
    let fixture = Fixture::with_statistics(vec![Stat::Min, Stat::Max, Stat::NullCount]);
    let mut query = query(&fixture.path);
    query.predicate = Some(PredicateExpr::Compare {
        column: ColumnRef::new("Referer").unwrap(),
        op: ComparisonOp::Lt,
        value: StatValue::Utf8(String::new()),
    });
    let assert_pruned = |report: &VortexLocalPrimitiveExecutionReport| {
        assert!(report.embedded_layout.metadata_pruned_entire_input);
        assert!(!report.upstream_scan_called && !report.data_read);
        assert_eq!(report.arrays_read_count, 0);
        let work = payload(report);
        assert!(work["aggregate_dictionary_preparation_workers"].is_null());
        assert!(work["aggregate_provider_background_workers"].is_null());
    };
    for parallelism in [2, 3] {
        let report =
            execute_vortex_local_primitive_with_policy(&query, policy(parallelism)).unwrap();
        assert_pruned(&report);
        assert_eq!(payload(&report)["values"], serde_json::json!([]));
        let session = ResidentVortexSession::for_external_cpu_pool(MEMORY, parallelism).unwrap();
        let prepared = prepare_aggregate_in_session(&query, policy(parallelism), &session).unwrap();
        let memory = session.memory().clone();
        let baseline = memory.snapshot().reserved_bytes;
        for pressure in [false, true] {
            let held = pressure.then(|| {
                memory
                    .reserve(memory.snapshot().limit_bytes - baseline)
                    .unwrap()
            });
            let executed = prepared.execute().unwrap();
            assert!(executed.native_io_certificate.is_certified());
            assert_pruned(&executed.report);
            assert_eq!(payload(&executed.report)["values"], serde_json::json!([]));
            assert_eq!(executed.runtime.provider_background_workers, 0);
            drop(executed);
            drop(held);
            assert_eq!(memory.snapshot().reserved_bytes, baseline);
        }
        let owned = prepared.execute_owned().unwrap();
        assert_pruned(&owned.execution.report);
        assert_eq!(owned.execution.runtime.provider_background_workers, 0);
        assert_eq!(owned.result.row_count(), 0);
        drop((owned, prepared, session));
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
    for (parallelism, numeric) in [
        (2, false),
        (3, false),
        (4, false),
        (12, false),
        (4, true),
        (12, true),
    ] {
        native_cancel(parallelism, numeric);
    }
}

fn native_cancel(parallelism: usize, numeric: bool) {
    let _clear = ClearHooks;
    let fixture = Fixture::new();
    let session = ResidentVortexSession::for_external_cpu_pool(MEMORY, parallelism).unwrap();
    let memory = session.memory().clone();
    let query = if numeric {
        numeric_query(&fixture.path)
    } else {
        query(&fixture.path)
    };
    let expected = if numeric {
        fixture.numeric_expected()
    } else {
        fixture.expected.clone()
    };
    let prepared = prepare_aggregate_in_session(&query, policy(parallelism), &session).unwrap();
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
    assert_native_io_credits_return(&memory, baseline);
    let fresh = CancellationToken::default();
    let result = prepared.execute_cancellable(&fresh).unwrap();
    assert_eq!(payload(&result.report)["values"], expected);
    assert!(!fresh.is_cancelled());
    drop(result);
    drop(prepared);
    drop(session);
    assert_native_io_credits_return(&memory, 0);
}

#[test]
fn dictionary_preparation_native_committed_source_failures_do_not_replay() {
    use aggregate_count_workers::{SOURCE_SCAN_TEST_FAULT, SourceScanTestFault};
    let _clear = ClearHooks;
    let fixture = Fixture::new();
    for (parallelism, numeric, fault) in [
        (2, false, SourceScanTestFault::OwnedDenial),
        (
            2,
            false,
            SourceScanTestFault::CorruptionWithConcurrentDenial,
        ),
        (3, false, SourceScanTestFault::OwnedDenial),
        (
            3,
            false,
            SourceScanTestFault::CorruptionWithConcurrentDenial,
        ),
        (4, true, SourceScanTestFault::OwnedDenial),
        (4, true, SourceScanTestFault::CorruptionWithConcurrentDenial),
        (4, false, SourceScanTestFault::OwnedDenial),
        (
            4,
            false,
            SourceScanTestFault::CorruptionWithConcurrentDenial,
        ),
    ] {
        let session = ResidentVortexSession::for_external_cpu_pool(MEMORY, parallelism).unwrap();
        let memory = session.memory().clone();
        let query = if numeric {
            numeric_query(&fixture.path)
        } else {
            query(&fixture.path)
        };
        let expected = if numeric {
            fixture.numeric_expected()
        } else {
            fixture.expected.clone()
        };
        let prepared = prepare_aggregate_in_session(&query, policy(parallelism), &session).unwrap();
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
        assert_native_io_credits_return(&memory, baseline);
        assert_eq!(prepared.snapshot().prepared_source_opens, 1);
        assert_eq!(
            payload(&prepared.execute().unwrap().report)["values"],
            expected
        );
        drop(prepared);
        drop(session);
        assert_native_io_credits_return(&memory, 0);
    }
}

#[test]
fn dictionary_preparation_numeric_native_dict_interleave_and_capacity_retirement() {
    use vortex::array::arrays::{DictArray, PrimitiveArray};
    if std::thread::available_parallelism().unwrap().get() < 4 {
        return;
    }
    let texts = (0..256)
        .map(|row| format!("https://{}.test/{row}/{}", row % 2, "x".repeat(row % 9)))
        .collect::<Vec<_>>();
    let first = chunk(&texts);
    let dict = DictArray::try_new(
        PrimitiveArray::new(vec![2_u32, 1, 0, 1], Validity::NonNullable).into_array(),
        VarBinViewArray::from_iter_str(texts.iter()).into_array(),
    )
    .unwrap()
    .into_array();
    let middle =
        StructArray::new(["Referer"].into(), vec![dict], 4, Validity::NonNullable).into_array();
    let inputs = [first.clone(), middle, first];
    let request = numeric_request();
    let columns = vec!["Referer".to_owned()];
    for pressure_after in [None, Some(0), Some(2)] {
        let memory = LiveMemoryPool::new(MEMORY).unwrap();
        let mut state =
            GroupedAggregateStates::new(&request, Some(2), &columns, false, false).unwrap();
        let mut serial =
            GroupedAggregateStates::new(&request, Some(2), &columns, false, false).unwrap();
        let operation = CancellationToken::default();
        let mut workers = DictionaryPrepareWorkers::admit(
            &state,
            inputs[0].dtype(),
            &columns,
            policy(4),
            &memory,
            Some(&operation),
        )
        .unwrap()
        .unwrap();
        let mut pressure = None;
        for (index, input) in inputs.iter().enumerate() {
            if pressure_after == Some(index) {
                let snapshot = memory.snapshot();
                pressure = Some(
                    memory
                        .reserve(snapshot.limit_bytes - snapshot.reserved_bytes - 1)
                        .unwrap(),
                );
            }
            workers.before_next(&mut state).unwrap();
            let consumed = workers.submit(input, &mut state).unwrap();
            if index == 1 {
                assert!(
                    !consumed,
                    "native dictionaries retain their existing consumer"
                );
            }
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
        assert_eq!(workers.retired(), pressure_after.is_some());
        drop((workers, pressure));
        assert!(!operation.is_cancelled());
        assert_eq!(memory.snapshot().reserved_bytes, 0);
    }
}

#[test]
fn dictionary_preparation_numeric_ties_and_offset_use_complete_keys() {
    if std::thread::available_parallelism().unwrap().get() < 4 {
        return;
    }
    let mut request = numeric_request();
    request.order_by = vec![VortexAggregateOrderExpr::new("c", true)];
    request.offset = 1;
    let columns = vec!["Referer".to_owned()];
    let mut oracle = BTreeMap::<String, (u32, u32, usize, usize)>::new();
    let inputs = (0..3)
        .map(|batch| {
            chunk(
                &(0..128)
                    .map(|row| {
                        let domain = ["z.test", "a.test", "c.test", "b.test"][(row + batch) % 4];
                        let text =
                            format!("https://{domain}/{row}/{}", "x".repeat(batch + row % 7));
                        let entry = oracle.entry(domain.into()).or_insert((0, 0, usize::MAX, 0));
                        entry.0 += 1;
                        entry.1 += u32::try_from(text.len()).unwrap();
                        entry.2 = entry.2.min(text.len());
                        entry.3 = entry.3.max(text.len());
                        text
                    })
                    .collect::<Vec<_>>(),
            )
        })
        .collect::<Vec<_>>();
    let expected: serde_json::Value = oracle.into_iter().skip(1).take(2).map(|(key, (count, sum, min, max))| {
        serde_json::json!({"k":key,"c":count,"l":f64::from(sum)/f64::from(count),"m":min,"x":max,"s":f64::from(sum),"n":count})
    }).collect::<Vec<_>>().into();
    let memory = LiveMemoryPool::new(MEMORY).unwrap();
    let mut state = GroupedAggregateStates::new(&request, Some(2), &columns, false, false).unwrap();
    let mut serial =
        GroupedAggregateStates::new(&request, Some(2), &columns, false, false).unwrap();
    let mut workers = DictionaryPrepareWorkers::admit(
        &state,
        inputs[0].dtype(),
        &columns,
        policy(4),
        &memory,
        None,
    )
    .unwrap()
    .unwrap();
    for input in inputs {
        workers.before_next(&mut state).unwrap();
        assert!(workers.submit(&input, &mut state).unwrap());
        assert!(
            serial
                .update_compact_direct_from_chunk(&input, &columns, None)
                .unwrap()
        );
    }
    workers.finish(&mut state).unwrap();
    assert_eq!(state_values(&mut state), expected);
    assert_eq!(state_values(&mut serial), expected);
    drop(workers);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}
