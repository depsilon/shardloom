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

#[test]
fn scalar_distinct_workers_reject_malformed_text_before_admission_and_ignore_unused_values() {
    use vortex::array::dtype::Nullability;

    let binary = VarBinViewArray::from_iter_bin([
        b"valid".as_slice(),
        b"external invalid UTF8\xff".as_slice(),
    ]);
    let error = VarBinViewArray::try_new_handle_in(
        binary.views_handle().clone(),
        binary.data_buffers().iter().cloned(),
        DType::Utf8(Nullability::NonNullable),
        Validity::NonNullable,
        &vortex::session::VortexSession::empty()
            .create_execution_ctx()
            .host_allocator(),
    )
    .unwrap_err();
    assert!(error.to_string().contains("utf-8 validation"));
    let memory = LiveMemoryPool::new(MEMORY).unwrap();
    // Every dictionary value must be valid text at construction; only referenced
    // values contribute to the exact distinct count.
    for (codes, expected) in [(vec![0_u32, 0], 1), (vec![1_u32, 0], 2)] {
        let dict = DictArray::try_new(
            PrimitiveArray::new(codes, Validity::NonNullable).into_array(),
            VarBinViewArray::from_iter_str(["valid", "outlined dictionary value 東京"])
                .into_array(),
        )
        .unwrap()
        .into_array();
        let mut states = state();
        let input = wrap(dict);
        let mut workers = admit(&input, &states, &memory, 2, None);
        workers.submit(&input).unwrap();
        workers.finish(&mut states).unwrap();
        assert_eq!(n(&states), expected);
        drop(workers);
        assert_eq!(memory.snapshot().reserved_bytes, 0);
    }
}

#[test]
fn scalar_distinct_workers_decline_other_shapes_before_allocating() {
    use vortex::array::{arrays::DecimalArray, dtype::DecimalDType};
    let memory = LiveMemoryPool::new(MEMORY).unwrap();
    let session = VortexSession::default();
    let binary = wrap(VarBinViewArray::from_iter_bin([b"a".as_slice()]).into_array());
    let decimal = wrap(
        DecimalArray::try_new(
            vortex::buffer::buffer![100_i64],
            DecimalDType::new(10, 2),
            Validity::NonNullable,
        )
        .unwrap()
        .into_array(),
    );
    for input in [binary, decimal] {
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

#[test]
fn scalar_distinct_workers_numeric_float_zero_nan_bool_dict_constant_and_nullable_struct() {
    use vortex::array::arrays::{BoolArray, ConstantArray};
    use vortex::array::dtype::Nullability;
    use vortex::array::scalar::Scalar;

    let memory = LiveMemoryPool::new(MEMORY).unwrap();

    // 1. i32 across multiple chunks with zeros, negatives, and nulls.
    let mut states = state();
    let c1 = wrap(
        PrimitiveArray::from_option_iter([Some(0_i32), Some(-1), None, Some(42)]).into_array(),
    );
    let c2 = wrap(PrimitiveArray::from_option_iter([Some(42_i32), Some(0), Some(7)]).into_array());
    let mut workers = admit(&c1, &states, &memory, 2, None);
    workers.before_next().unwrap();
    workers.submit(&c1).unwrap();
    workers.before_next().unwrap();
    workers.submit(&c2).unwrap();
    workers.finish(&mut states).unwrap();
    assert_eq!(n(&states), 4);
    assert_eq!(
        states.result_payload(&[]).unwrap()["distinct_state_strategy"],
        "bounded_native_numeric_partition_exact"
    );
    drop(workers);
    assert_eq!(memory.snapshot().reserved_bytes, 0);

    // 2. f64: -0.0 and +0.0 merge to 1 distinct key; distinct NaN bit patterns stay distinct.
    let nan1 = f64::from_bits(0x7ff8_0000_0000_0001);
    let nan2 = f64::from_bits(0x7ff8_0000_0000_0002);
    let floats = wrap(
        PrimitiveArray::from_option_iter([
            Some(0.0_f64),
            Some(-0.0_f64),
            Some(nan1),
            Some(nan2),
            Some(nan1),
            None,
        ])
        .into_array(),
    );
    let mut states = state();
    let mut workers = admit(&floats, &states, &memory, 2, None);
    workers.submit(&floats).unwrap();
    workers.finish(&mut states).unwrap();
    assert_eq!(n(&states), 3);
    drop(workers);
    assert_eq!(memory.snapshot().reserved_bytes, 0);

    // 3. Bool: canonical + Dictionary + Constant.
    let bools =
        wrap(BoolArray::from_iter([Some(true), None, Some(false), Some(true)]).into_array());
    let bool_dict = wrap(
        DictArray::try_new(
            PrimitiveArray::from_option_iter([Some(0_u32), None, Some(0)]).into_array(),
            BoolArray::from_iter([Some(true), Some(false)]).into_array(),
        )
        .unwrap()
        .into_array(),
    );
    let mut states = state();
    let mut workers = admit(&bools, &states, &memory, 2, None);
    workers.submit(&bools).unwrap();
    workers.submit(&bool_dict).unwrap();
    workers.finish(&mut states).unwrap();
    assert_eq!(n(&states), 2);
    drop(workers);
    assert_eq!(memory.snapshot().reserved_bytes, 0);

    // 4. Numeric Dict + Constant + Nullable parent Struct.
    let num_dict = DictArray::try_new(
        PrimitiveArray::from_option_iter([Some(0_u32), None, Some(2)]).into_array(),
        PrimitiveArray::from_option_iter([Some(100_i64), Some(200), Some(300)]).into_array(),
    )
    .unwrap()
    .into_array();
    let nullable_dict_struct = StructArray::new(
        ["text"].into(),
        vec![num_dict],
        3,
        Validity::from_iter([true, true, false]),
    )
    .into_array();
    let all_invalid_struct = StructArray::new(
        ["text"].into(),
        vec![PrimitiveArray::new(vec![777_i64, 888], Validity::NonNullable).into_array()],
        2,
        Validity::AllInvalid,
    )
    .into_array();
    let const_valid =
        wrap(ConstantArray::new(Scalar::primitive(555_i64, Nullability::Nullable), 4).into_array());
    let const_null = wrap(
        ConstantArray::new(
            Scalar::null(DType::Primitive(PType::I64, Nullability::Nullable)),
            4,
        )
        .into_array(),
    );
    let mut states = state();
    let mut workers = admit(&nullable_dict_struct, &states, &memory, 2, None);
    workers.submit(&nullable_dict_struct).unwrap();
    workers.submit(&all_invalid_struct).unwrap();
    workers.submit(&const_valid).unwrap();
    workers.submit(&const_null).unwrap();
    workers.finish(&mut states).unwrap();
    // Only row 0 of nullable_dict_struct (100) and const_valid (555) are non-null.
    assert_eq!(n(&states), 2);
    drop(workers);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

#[cfg(all(feature = "vortex-write", unix))]
fn write_filtered_numeric_and_binary_fixture() -> (
    std::path::PathBuf,
    std::path::PathBuf,
    vortex::array::dtype::DType,
    std::fs::File,
) {
    use crate::local_primitives::native_flat_layout;
    use vortex::file::WriteOptionsSessionExt as _;
    use vortex::io::{
        runtime::{BlockingRuntime as _, single::SingleThreadRuntime},
        session::RuntimeSessionExt as _,
    };

    static SEQ: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let directory = std::env::temp_dir().join(format!(
        "shardloom-scalar-distinct-num-{}",
        SEQ.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
    ));
    std::fs::create_dir(&directory).unwrap();
    let path = directory.join("source.vortex");
    let col_i64 =
        PrimitiveArray::from_option_iter([Some(10_i64), Some(20), Some(10), None, Some(30)])
            .into_array();
    let col_bin =
        VarBinViewArray::from_iter_bin([b"a".as_slice(), b"b", b"c", b"d", b"e"]).into_array();
    let batch = StructArray::new(
        ["text", "bin"].into(),
        vec![col_i64, col_bin],
        5,
        Validity::NonNullable,
    )
    .into_array();
    let runtime = SingleThreadRuntime::default();
    let session = VortexSession::default().with_handle(runtime.handle());
    let mut file = std::fs::File::create_new(&path).unwrap();
    let mut writer = session
        .write_options()
        .with_strategy(native_flat_layout::SequentialNativeFlatLayout::strategy(3))
        .with_file_statistics(Vec::new())
        .blocking(&runtime)
        .writer(&mut file, batch.dtype().clone());
    for _ in 0..3 {
        writer.push(batch.clone()).unwrap();
    }
    writer.finish().unwrap();
    (directory, path, batch.dtype().clone(), file)
}

#[cfg(all(feature = "vortex-write", unix))]
#[test]
fn scalar_distinct_workers_filtered_numeric_and_provider_restore() {
    use crate::local_primitives::{
        aggregate_count_workers, execute_vortex_local_primitive_with_policy,
        prepared_aggregate::prepare_aggregate_in_session,
    };
    use crate::resident_session::ResidentVortexSession;
    use shardloom_core::{ComparisonOp, PredicateExpr, StatValue};

    let (directory, path, dtype, file) = write_filtered_numeric_and_binary_fixture();

    let mut filtered = VortexQueryPrimitiveRequest::simple_aggregate(
        DatasetUri::new(path.display().to_string()).unwrap(),
        request(),
    );
    filtered.predicate = Some(PredicateExpr::Compare {
        column: ColumnRef::new("text").unwrap(),
        op: ComparisonOp::Gt,
        value: StatValue::Int64(10),
    });

    assert!(request_may_be_admitted(&filtered));
    assert!(request_schema_may_be_admitted(
        &filtered,
        &dtype,
        policy(2).resource_envelope()
    ));
    assert!(!aggregate_count_workers::restore_provider_drivers(
        &filtered,
        &dtype,
        policy(2).resource_envelope(),
    ));

    let binary_req = VortexQueryPrimitiveRequest::simple_aggregate(
        DatasetUri::new(path.display().to_string()).unwrap(),
        VortexSimpleAggregateRequest::new(vec![VortexSimpleAggregateMeasure::new(
            "count_distinct",
            Some(ColumnRef::new("bin").unwrap()),
            "n".into(),
        )]),
    );
    assert!(!request_schema_may_be_admitted(
        &binary_req,
        &dtype,
        policy(2).resource_envelope()
    ));
    assert!(aggregate_count_workers::restore_provider_drivers(
        &binary_req,
        &dtype,
        policy(2).resource_envelope(),
    ));

    let p1 = execute_vortex_local_primitive_with_policy(&filtered, policy(1)).unwrap();
    let p2 = execute_vortex_local_primitive_with_policy(&filtered, policy(2)).unwrap();
    let extract_payload = |report: &crate::VortexLocalPrimitiveExecutionReport| {
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
    let v1 = extract_payload(&p1);
    let v2 = extract_payload(&p2);
    assert_eq!(v1["values"]["n"], 2);
    assert_eq!(v2["values"]["n"], 2);
    assert_eq!(
        v2["aggregate_scalar_distinct_workers"]["strategy"],
        "bounded_native_numeric_partition_exact"
    );

    let resident = ResidentVortexSession::for_external_cpu_pool(MEMORY, 2).unwrap();
    let mem = resident.memory().clone();
    let prepared = prepare_aggregate_in_session(&filtered, policy(2), &resident).unwrap();
    let exec = prepared.execute().unwrap();
    let ev = extract_payload(&exec.report);
    assert_eq!(ev["values"]["n"], 2);
    assert_eq!(
        ev["aggregate_scalar_distinct_workers"]["strategy"],
        "bounded_native_numeric_partition_exact"
    );
    drop((exec, prepared, resident));
    assert_eq!(mem.snapshot().reserved_bytes, 0);
    drop(file);
    std::fs::remove_dir_all(directory).unwrap();
}
