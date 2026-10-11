use super::*;
use crate::{
    VortexAggregateHavingExpr, VortexSimpleAggregateMeasure, VortexSimpleAggregateRequest,
};
use shardloom_core::{ColumnRef, ComparisonOp, DatasetUri};
use std::collections::BTreeSet;
use vortex::{
    VortexSessionDefault as _,
    array::{
        IntoArray as _,
        arrays::{DictArray, PrimitiveArray, StructArray},
        validity::Validity,
    },
};

const MEMORY: u64 = 64 << 20;

fn request() -> VortexSimpleAggregateRequest {
    VortexSimpleAggregateRequest::new(vec![VortexSimpleAggregateMeasure::new(
        "count_distinct",
        Some(ColumnRef::new("text").unwrap()),
        "n".into(),
    )])
}
fn state() -> SimpleAggregateStates {
    SimpleAggregateStates::new(&request(), &["text".into()]).unwrap()
}
fn policy(parallelism: usize) -> VortexLocalPrimitiveExecutionPolicy {
    let mut policy =
        VortexLocalPrimitiveExecutionPolicy::new_with_memory_gb(parallelism, 4).unwrap();
    policy.resource_envelope.memory_budget_bytes = MEMORY;
    policy
}
fn wrap(values: ArrayRef) -> ArrayRef {
    let rows = values.len();
    StructArray::new(["text"].into(), vec![values], rows, Validity::NonNullable).into_array()
}
fn strings(values: &[Option<&str>]) -> ArrayRef {
    wrap(VarBinViewArray::from_iter_nullable_str(values.iter().copied()).into_array())
}
fn admit(
    input: &ArrayRef,
    state: &SimpleAggregateStates,
    memory: &LiveMemoryPool,
    parallelism: usize,
    token: Option<&CancellationToken>,
) -> ScalarDistinctWorkers {
    ScalarDistinctWorkers::admit(
        state,
        input.dtype(),
        &["text".into()],
        policy(parallelism),
        &VortexSession::default(),
        memory,
        token,
    )
    .unwrap()
    .unwrap()
}
fn n(state: &SimpleAggregateStates) -> u64 {
    state.result_values().unwrap()["n"].as_u64().unwrap()
}

#[test]
fn scalar_distinct_workers_exact_across_chunks_nulls_slices_and_source_release() {
    for parallelism in [2, 4, 12] {
        let inputs = [
            vec![
                None,
                Some(""),
                Some("東京🙂"),
                Some("long external string shared across chunks"),
                Some("é"),
            ],
            vec![
                Some("東京🙂"),
                None,
                Some(""),
                Some("e\u{301}"),
                Some("embedded\0nul"),
            ],
            vec![None, None, None, None, None],
        ];
        let oracle = inputs
            .iter()
            .flatten()
            .filter_map(|v| *v)
            .collect::<BTreeSet<_>>();
        let memory = LiveMemoryPool::new(MEMORY).unwrap();
        let mut states = state();
        let mut workers = admit(&strings(&inputs[0]), &states, &memory, parallelism, None);
        for _ in 0..3 {
            for values in &inputs {
                let mut padded = vec![Some("excluded prefix")];
                padded.extend(values);
                padded.push(Some("excluded suffix"));
                let source = VarBinViewArray::from_iter_nullable_str(padded).into_array();
                let selected = source.slice(1..6).unwrap();
                let chunk = wrap(selected);
                workers.before_next().unwrap();
                workers.submit(&chunk).unwrap();
                drop((chunk, source));
            }
        }
        workers.finish(&mut states).unwrap();
        assert_eq!(n(&states), oracle.len() as u64);
        assert!(states.states[0].distinct_values.is_empty());
        assert_eq!(
            states.count_distinct_state_entries().unwrap(),
            oracle.len() as u64
        );
        assert_eq!(workers.work.rows, 45);
        assert_eq!(
            workers.work.copied_bytes,
            oracle.iter().map(|v| v.len() as u64).sum::<u64>()
        );
        assert_eq!(workers.jobs.outstanding(), 0);
        assert!(!workers.jobs.is_full());
        assert!(workers.jobs.peak_outstanding() <= parallelism * 2);
        let having = [VortexAggregateHavingExpr::new("n", ComparisonOp::Gt, "100")];
        assert_eq!(states.result_row_count(&having).unwrap(), 0);
        assert_eq!(
            states.result_payload(&having).unwrap()["values"],
            serde_json::json!({})
        );
        drop(workers);
        assert_eq!(memory.snapshot().reserved_bytes, 0);
        assert_eq!(n(&states), oracle.len() as u64);
    }
}

#[test]
fn scalar_distinct_workers_native_dictionary_null_codes_values_unused_and_reordered() {
    let memory = LiveMemoryPool::new(MEMORY).unwrap();
    let mut states = state();
    let mut workers = admit(&strings(&[Some("x")]), &states, &memory, 2, None);
    for values in [
        vec![Some("tea"), None, Some(""), Some("unused")],
        vec![Some(""), None, Some("tea"), Some("unused")],
    ] {
        let codes =
            PrimitiveArray::from_option_iter([Some(0_u32), None, Some(1), Some(2), Some(0)])
                .into_array();
        let dictionary = DictArray::try_new(
            codes,
            VarBinViewArray::from_iter_nullable_str(values).into_array(),
        )
        .unwrap()
        .into_array();
        workers.before_next().unwrap();
        workers.submit(&wrap(dictionary)).unwrap();
    }
    // A sparse values domain must not materialize unused values into the union.
    let values = (0..200).map(|i| format!("sparse-{i}")).collect::<Vec<_>>();
    let dictionary = DictArray::try_new(
        PrimitiveArray::new(vec![199_u32, 199, 7], Validity::NonNullable).into_array(),
        VarBinViewArray::from_iter_str(values).into_array(),
    )
    .unwrap()
    .into_array();
    workers.before_next().unwrap();
    workers.submit(&wrap(dictionary)).unwrap();
    workers.finish(&mut states).unwrap();
    assert_eq!(n(&states), 4);
    assert_eq!(workers.work.dictionary_chunks, 3);
    assert_eq!(
        workers.work.copied_bytes,
        ("tea".len() + "sparse-199".len() + "sparse-7".len()) as u64
    );
    drop(workers);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

#[test]
fn scalar_distinct_partitions_full_hash_collisions_growth_and_denial_preserve_exact_union() {
    let memory = LiveMemoryPool::new(MEMORY).unwrap();
    let parts = Partitions::new(&memory).unwrap();
    let worker = ChunkWorkerContext::Inline(CancellationToken::default());
    let values = (0..2200)
        .map(|i| format!("common-prefix-東京-{i}"))
        .collect::<Vec<_>>();
    let pressure = memory
        .reserve(MEMORY - memory.snapshot().reserved_bytes)
        .unwrap();
    assert!(
        parts
            .union(&mut [(0, 0)], |i| values[i].as_bytes(), &worker)
            .is_err()
    );
    assert_eq!(parts.cardinality().unwrap(), 0);
    drop(pressure);
    let mut entries = (0..values.len()).map(|i| (0, i)).collect::<Vec<_>>();
    assert_eq!(
        parts
            .union(&mut entries, |i| values[i].as_bytes(), &worker)
            .unwrap(),
        values.iter().map(|v| v.len() as u64).sum::<u64>()
    );
    assert_eq!(parts.cardinality().unwrap(), values.len() as u64);
    entries.reverse();
    assert_eq!(
        parts
            .union(&mut entries, |i| values[i].as_bytes(), &worker)
            .unwrap(),
        0
    );
    assert_eq!(parts.cardinality().unwrap(), values.len() as u64);
    worker.check_cancelled().unwrap();
    drop(parts);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

#[test]
fn scalar_distinct_workers_admission_cancellation_and_capacity_failure_publish_no_result() {
    let input = strings(&[Some("key"), None]);
    let small = LiveMemoryPool::new(64).unwrap();
    assert!(
        ScalarDistinctWorkers::admit(
            &state(),
            input.dtype(),
            &["text".into()],
            policy(2),
            &VortexSession::default(),
            &small,
            None
        )
        .unwrap()
        .is_none()
    );
    assert_eq!(small.snapshot().reserved_bytes, 0);
    let memory = LiveMemoryPool::new(MEMORY).unwrap();
    let mut states = state();
    let token = CancellationToken::default();
    let mut workers = admit(&input, &states, &memory, 2, Some(&token));
    workers.submit(&input).unwrap();
    token.cancel();
    assert!(
        workers
            .finish(&mut states)
            .unwrap_err()
            .to_string()
            .contains("cancel")
    );
    assert!(!states.partition_distinct_completed);
    drop(workers);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
    let mut workers = admit(&input, &states, &memory, 2, None);
    let pressure = memory
        .reserve(MEMORY - memory.snapshot().reserved_bytes)
        .unwrap();
    assert!(workers.submit(&input).is_err());
    assert_eq!(workers.jobs.submitted(), 0);
    assert!(!states.partition_distinct_completed);
    drop((workers, pressure));
    assert_eq!(memory.snapshot().reserved_bytes, 0);
    let mut empty = admit(&input, &states, &memory, 2, None);
    empty.finish(&mut states).unwrap();
    assert_eq!(n(&states), 0);
    drop(empty);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

fn malformed() -> ArrayRef {
    let binary = VarBinViewArray::from_iter_bin([
        b"valid".as_slice(),
        b"external invalid UTF8\xff".as_slice(),
    ]);
    VarBinViewArray::new_handle(
        binary.views_handle().clone(),
        binary.data_buffers().to_vec().into(),
        DType::Utf8(Nullability::NonNullable),
        Validity::NonNullable,
    )
    .into_array()
}

#[test]
fn scalar_distinct_workers_invalid_used_utf8_fails_but_unused_dictionary_value_is_ignored() {
    let memory = LiveMemoryPool::new(MEMORY).unwrap();
    let mut states = state();
    let input = wrap(malformed());
    let mut workers = admit(&input, &states, &memory, 2, None);
    workers.submit(&input).unwrap();
    assert!(workers.finish(&mut states).is_err());
    assert!(!states.partition_distinct_completed);
    drop(workers);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
    let used_invalid = DictArray::try_new(
        PrimitiveArray::new(vec![1_u32, 0], Validity::NonNullable).into_array(),
        malformed(),
    )
    .unwrap()
    .into_array();
    let input = wrap(used_invalid);
    let mut workers = admit(&input, &states, &memory, 2, None);
    workers.submit(&input).unwrap();
    assert!(
        workers
            .finish(&mut states)
            .unwrap_err()
            .to_string()
            .contains("invalid UTF8")
    );
    assert!(!states.partition_distinct_completed);
    drop(workers);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
    let dict = DictArray::try_new(
        PrimitiveArray::new(vec![0_u32, 0], Validity::NonNullable).into_array(),
        malformed(),
    )
    .unwrap()
    .into_array();
    let input = wrap(dict);
    let mut workers = admit(&input, &states, &memory, 2, None);
    workers.submit(&input).unwrap();
    workers.finish(&mut states).unwrap();
    assert_eq!(n(&states), 1);
    drop(workers);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

#[test]
fn scalar_distinct_workers_decline_other_shapes_before_allocating() {
    let memory = LiveMemoryPool::new(MEMORY).unwrap();
    let session = VortexSession::default();
    let nullable_parent = StructArray::new(
        ["text"].into(),
        vec![VarBinViewArray::from_iter_str(["x"]).into_array()],
        1,
        Validity::AllInvalid,
    )
    .into_array();
    let numeric = wrap(PrimitiveArray::new(vec![1_u32], Validity::NonNullable).into_array());
    for input in [nullable_parent, numeric] {
        assert!(
            ScalarDistinctWorkers::admit(
                &state(),
                input.dtype(),
                &["text".into()],
                policy(2),
                &session,
                &memory,
                None,
            )
            .unwrap()
            .is_none()
        );
        assert_eq!(memory.snapshot().reserved_bytes, 0);
    }
    let input = strings(&[Some("x")]);
    assert!(
        ScalarDistinctWorkers::admit(
            &state(),
            input.dtype(),
            &["text".into()],
            policy(1),
            &session,
            &memory,
            None,
        )
        .unwrap()
        .is_none()
    );
    assert_eq!(memory.snapshot().reserved_bytes, 0);
    let uri = DatasetUri::new("unused.vortex").unwrap();
    let base = VortexQueryPrimitiveRequest::simple_aggregate(uri.clone(), request());
    assert!(request_may_be_admitted(&base));
    assert!(!request_may_be_admitted(&base.with_source_order_limit(1)));
    let grouped = VortexSimpleAggregateRequest::grouped(
        vec![ColumnRef::new("text").unwrap()],
        request().measures,
    );
    assert!(!request_may_be_admitted(
        &VortexQueryPrimitiveRequest::simple_aggregate(uri.clone(), grouped)
    ));
    let mut multiple = request();
    multiple.measures.push(VortexSimpleAggregateMeasure::new(
        "count",
        None,
        "rows".into(),
    ));
    assert!(!request_may_be_admitted(
        &VortexQueryPrimitiveRequest::simple_aggregate(uri, multiple)
    ));
}

#[cfg(all(feature = "vortex-write", unix))]
#[test]
fn scalar_distinct_workers_file_prepared_owned_and_serial_values_agree() {
    use crate::{
        local_primitives::{
            execute_vortex_local_primitive_with_policy, native_flat_layout,
            prepared_aggregate::prepare_aggregate_in_session,
        },
        resident_session::ResidentVortexSession,
    };
    use vortex::{
        file::WriteOptionsSessionExt as _,
        io::{
            runtime::{BlockingRuntime as _, single::SingleThreadRuntime},
            session::RuntimeSessionExt as _,
        },
    };
    let directory =
        std::env::temp_dir().join(format!("shardloom-scalar-distinct-{}", std::process::id()));
    std::fs::create_dir(&directory).unwrap();
    let path = directory.join("source.vortex");
    let input = strings(&[
        Some("a long external UTF8 東京 value"),
        None,
        Some(""),
        Some("é"),
        Some("é"),
    ]);
    let runtime = SingleThreadRuntime::default();
    let session = VortexSession::default().with_handle(runtime.handle());
    let mut file = std::fs::File::create_new(&path).unwrap();
    let mut writer = session
        .write_options()
        .with_strategy(native_flat_layout::SequentialNativeFlatLayout::strategy(3))
        .with_file_statistics(Vec::new())
        .blocking(&runtime)
        .writer(&mut file, input.dtype().clone());
    for _ in 0..3 {
        writer.push(input.clone()).unwrap();
    }
    writer.finish().unwrap();
    let query = VortexQueryPrimitiveRequest::simple_aggregate(
        DatasetUri::new(path.display().to_string()).unwrap(),
        request(),
    );
    let payload = |report: &crate::VortexLocalPrimitiveExecutionReport| {
        assert!(!report.has_errors());
        assert!(!report.fallback_execution_allowed);
        serde_json::from_str::<serde_json::Value>(
            report
                .result_summary
                .as_ref()
                .unwrap()
                .split_once(" values=")
                .unwrap()
                .1,
        )
        .unwrap()
    };
    for parallelism in [1, 2, 4] {
        let report =
            execute_vortex_local_primitive_with_policy(&query, policy(parallelism)).unwrap();
        let value = payload(&report);
        assert_eq!(value["values"]["n"], 3);
        assert_eq!(
            value.get("aggregate_scalar_distinct_workers").is_some(),
            parallelism > 1
        );
    }
    for external in [false, true] {
        let session = if external {
            ResidentVortexSession::for_external_cpu_pool(MEMORY, 2)
        } else {
            ResidentVortexSession::new(MEMORY, 2)
        }
        .unwrap();
        let memory = session.memory().clone();
        let prepared = prepare_aggregate_in_session(&query, policy(2), &session).unwrap();
        let result = prepared.execute().unwrap();
        let value = payload(&result.report);
        assert_eq!(value["values"]["n"], 3);
        assert_eq!(
            value.get("aggregate_scalar_distinct_workers").is_some(),
            external
        );
        let owned = prepared.execute_owned().unwrap();
        drop((result, prepared, session));
        let json = owned.result.to_bounded_json(&["n".into()], 1024).unwrap();
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(json.value()).unwrap(),
            serde_json::json!([{"n":3}])
        );
        drop((json, owned));
        assert_eq!(memory.snapshot().reserved_bytes, 0);
    }
    drop(file);
    std::fs::remove_dir_all(directory).unwrap();
}
