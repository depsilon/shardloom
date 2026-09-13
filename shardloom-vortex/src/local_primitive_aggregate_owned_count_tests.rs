use super::*;
use crate::local_primitives as runtime;
use shardloom_core::{ComparisonOp, PredicateExpr, StatValue};

fn count_request(
    path: &Path,
    offset: usize,
    limit: usize,
    explicit_tie: bool,
) -> VortexQueryPrimitiveRequest {
    let mut request = request(path, offset, limit);
    let aggregate = request.simple_aggregate.as_mut().unwrap();
    aggregate.measures[0] = VortexSimpleAggregateMeasure::new("count", None, COUNT.into());
    if !explicit_tie {
        aggregate.order_by.truncate(1);
    }
    request.projection = shardloom_plan::ProjectionRequest::columns(aggregate.projected_columns());
    request
}

fn count_oracle(
    keys: impl IntoIterator<Item = i64>,
    offset: usize,
    limit: usize,
) -> serde_json::Value {
    let mut counts = BTreeMap::<i64, u64>::new();
    for key in keys {
        *counts.entry(key).or_default() += 1;
    }
    let mut counts = counts.into_iter().collect::<Vec<_>>();
    counts.sort_by(|left, right| right.1.cmp(&left.1).then(left.0.cmp(&right.0)));
    serde_json::Value::Array(
        counts
            .into_iter()
            .skip(offset)
            .take(limit)
            .map(|(key, count)| serde_json::json!({KEY: key, COUNT: count}))
            .collect(),
    )
}

fn work(completed: &ExecutedOwnedVortexAggregate) -> serde_json::Value {
    let (_, text) = completed
        .execution
        .report
        .result_summary
        .as_ref()
        .unwrap()
        .rsplit_once(" values=")
        .unwrap();
    let work: serde_json::Value = serde_json::from_str(text).unwrap();
    assert_eq!(work["materialized_group_value_count"], 0);
    assert!(work["values"].is_null());
    assert_eq!(
        work["group_output_strategy"],
        "bounded_heap_after_complete_count_group_reduction"
    );
    assert!(completed.execution.native_io_certificate.is_certified());
    assert!(!completed.execution.report.fallback_execution_allowed);
    work
}

#[test]
fn owned_count_complete_file_workers_admission_pressure_ties_offsets_and_fresh_state() {
    let fixture = Fixture::new();
    let keys = [
        i64::MAX,
        i64::MIN,
        0,
        i64::MAX,
        i64::MIN,
        0,
        i64::MAX,
        i64::MIN,
        0,
        0,
    ];
    let path = fixture.physical_pair_batches(&keys.map(|key| (key, 7)));
    for workers in [1, 2, 4] {
        for (offset, limit) in [(0, 10), (1, 1), (2, 3), (8, 2)] {
            for explicit_tie in [false, true] {
                let prepared = prepare_aggregate(
                    &count_request(&path, offset, limit, explicit_tie),
                    VortexLocalPrimitiveExecutionPolicy::new(workers).unwrap(),
                )
                .unwrap();
                let memory = prepared.session.memory().clone();
                let prepared_bytes = memory.snapshot().reserved_bytes;
                for (index, pressure) in [false, true, false].into_iter().enumerate() {
                    // Apply pressure to the admitted primary-count ordering.
                    // The explicit tie route
                    // deliberately exercises the existing caller/compact path.
                    runtime::aggregate_count_workers::ADMISSION_TEST_PRESSURE
                        .with(|flag| flag.set(pressure && !explicit_tie));
                    let completed = prepared.execute_owned().unwrap();
                    assert!(
                        !runtime::aggregate_count_workers::ADMISSION_TEST_PRESSURE
                            .with(std::cell::Cell::get)
                    );
                    let work = work(&completed);
                    assert_eq!(
                        rendered(&completed.result),
                        count_oracle(keys, offset, limit)
                    );
                    assert_eq!(completed.execution.report.rows_scanned, keys.len() as u64);
                    assert_eq!(work["candidate_groups"], 3);
                    if !explicit_tie && !pressure {
                        assert_eq!(work["aggregate_workers_rows"], keys.len() as u64);
                        assert_eq!(work["aggregate_workers_outstanding_chunks"], 0);
                        assert_eq!(
                            work["aggregate_workers_submitted_chunks"],
                            work["aggregate_workers_completed_chunks"]
                        );
                    }
                    assert_eq!(prepared.snapshot().completed_executions, index as u64 + 1);
                    assert_eq!(prepared.snapshot().prepared_source_opens, 1);
                    drop(completed);
                    assert_eq!(memory.snapshot().reserved_bytes, prepared_bytes);
                }
                let ordinary = prepared.execute().unwrap();
                let (_, payload) = ordinary
                    .report
                    .result_summary
                    .as_ref()
                    .unwrap()
                    .rsplit_once(" values=")
                    .unwrap();
                let payload: serde_json::Value = serde_json::from_str(payload).unwrap();
                assert_eq!(payload["values"], count_oracle(keys, offset, limit));
                drop(ordinary);
                assert_eq!(memory.snapshot().reserved_bytes, prepared_bytes);
                drop(prepared);
                assert_eq!(memory.snapshot().reserved_bytes, 0);
            }
        }
    }
}

#[test]
fn owned_count_filtered_values_and_source_generation_failure_release_ownership() {
    let fixture = Fixture::new();
    let keys = [
        i64::MIN,
        0,
        9,
        i64::MAX,
        0,
        9,
        i64::MIN,
        0,
        i64::MAX,
        9,
        0,
        0,
    ];
    let path = fixture.physical_pair_batches(&keys.map(|key| (key, 1)));
    for workers in [1, 4] {
        let mut request = count_request(&path, 1, 2, false);
        request.predicate = Some(PredicateExpr::Compare {
            column: ColumnRef::new(KEY).unwrap(),
            op: ComparisonOp::NotEq,
            value: StatValue::Int64(9),
        });
        let prepared = prepare_aggregate(
            &request,
            VortexLocalPrimitiveExecutionPolicy::new(workers).unwrap(),
        )
        .unwrap();
        let memory = prepared.session.memory().clone();
        let prepared_bytes = memory.snapshot().reserved_bytes;
        let completed = prepared.execute_owned().unwrap();
        let work = work(&completed);
        assert!(work["aggregate_workers_partition_source_replays"].is_null());
        assert_eq!(work["aggregate_workers_rows"], 9);
        assert_eq!(
            rendered(&completed.result),
            count_oracle(keys.into_iter().filter(|key| *key != 9), 1, 2)
        );
        drop(completed);
        assert_eq!(memory.snapshot().reserved_bytes, prepared_bytes);
        // Identical file bytes at a replacement inode are a new generation.
        let replacement = fixture.0.join("replacement.vortex");
        fs::copy(&path, &replacement).unwrap();
        fs::rename(&replacement, &path).unwrap();
        assert!(prepared.execute_owned().is_err());
        assert_eq!(prepared.snapshot().completed_executions, 1);
        assert_eq!(memory.snapshot().reserved_bytes, prepared_bytes);
        drop(prepared);
        assert_eq!(memory.snapshot().reserved_bytes, 0);
    }
}

#[test]
fn owned_count_finalizer_visits_actual_compact_and_general_states_without_json() {
    let fixture = Fixture::new();
    let path = standard(&fixture);
    let request = count_request(&path, 1, 2, true);
    let aggregate = request.simple_aggregate.as_ref().unwrap();
    let columns = vec![KEY.to_owned()];
    let input = vec![vec![
        StatValue::Int64(i64::MAX),
        StatValue::Int64(0),
        StatValue::Int64(i64::MIN),
        StatValue::Int64(0),
        StatValue::Int64(i64::MIN),
        StatValue::Int64(i64::MAX),
        StatValue::Int64(0),
    ]];
    let dtype = DType::struct_(
        [(KEY, DType::Primitive(PType::I64, Nullability::NonNullable))],
        Nullability::NonNullable,
    );
    for compact in [false, true] {
        let mut states =
            runtime::GroupedAggregateStates::new(aggregate, Some(2), &columns, false, false)
                .unwrap();
        if compact {
            for row in 0..input[0].len() {
                assert!(
                    states
                        .update_count_star_direct_from_materialized_columns(&input, row)
                        .unwrap()
                );
            }
            assert!(states.groups.values().all(|group| matches!(
                group,
                runtime::GroupedAggregateState::CompactCountStar { .. }
            )));
        } else {
            states.update(&input, input[0].len()).unwrap();
            assert!(
                states
                    .groups
                    .values()
                    .all(|group| matches!(group, runtime::GroupedAggregateState::General { .. }))
            );
        }
        let session = ResidentVortexSession::new(1 << 20, 1).unwrap();
        let mut output =
            runtime::aggregate_owned::OwnedAggregateFinalizer::new(&request, &dtype, &session)
                .unwrap();
        let (rows, summary) = output.finish(&states).unwrap();
        assert_eq!(rows, 2);
        assert!(serde_json::from_str::<serde_json::Value>(&summary).unwrap()["values"].is_null());
        let (array, ownership) = output.into_array().unwrap();
        assert_eq!(
            runtime::row_export_columns_from_chunk(&array, &[KEY.into(), COUNT.into()]).unwrap(),
            vec![
                vec![StatValue::Int64(i64::MIN), StatValue::Int64(i64::MAX)],
                vec![StatValue::UInt64(2), StatValue::UInt64(2)]
            ]
        );
        drop((array, ownership));
        assert_eq!(session.memory().snapshot().reserved_bytes, 0);
    }
}

#[test]
fn owned_count_preserves_every_integer_width_and_extreme_value() {
    macro_rules! check {
        ($t:ty, $ptype:expr) => {{
            let fixture = Fixture::new();
            let path = fixture.source(PrimitiveArray::new(vec![<$t>::MAX, <$t>::MIN, <$t>::MAX, <$t>::MIN], Validity::NonNullable).into_array(), vec![1; 4]);
            for explicit_tie in [false, true] {
                let prepared = prepare_aggregate(&count_request(&path, 0, 5, explicit_tie), VortexLocalPrimitiveExecutionPolicy::new(2).unwrap()).unwrap();
                let completed = prepared.execute_owned().unwrap();
                assert_eq!(completed.result.arrays()[0].dtype().as_struct_fields_opt().unwrap().field(KEY), Some(DType::Primitive($ptype, Nullability::NonNullable)));
                assert_eq!(completed.result.arrays()[0].dtype().as_struct_fields_opt().unwrap().field(COUNT), Some(DType::Primitive(PType::U64, Nullability::NonNullable)));
                assert_eq!(rendered(&completed.result), serde_json::json!([{KEY: <$t>::MIN, COUNT: 2}, {KEY: <$t>::MAX, COUNT: 2}]));
            }
        }};
    }
    check!(i8, PType::I8);
    check!(i16, PType::I16);
    check!(i32, PType::I32);
    check!(i64, PType::I64);
    check!(u8, PType::U8);
    check!(u16, PType::U16);
    check!(u32, PType::U32);
    check!(u64, PType::U64);
}

#[test]
fn owned_count_empty_pruned_and_offset_past_end_keep_schema_and_native_sink_lifetime() {
    for (keys, pruned, offset) in [
        (vec![], false, 0),
        (vec![1_i16, 2], true, 0),
        (vec![1, 2], false, 5),
    ] {
        let fixture = Fixture::new();
        let path = fixture.source(
            PrimitiveArray::new(keys.clone(), Validity::NonNullable).into_array(),
            vec![1; keys.len()],
        );
        let mut request = count_request(&path, offset, 4, false);
        if pruned {
            request.predicate = Some(PredicateExpr::Compare {
                column: ColumnRef::new(KEY).unwrap(),
                op: ComparisonOp::Lt,
                value: StatValue::Int64(-10),
            });
        }
        let prepared = prepare_aggregate(
            &request,
            VortexLocalPrimitiveExecutionPolicy::new(2).unwrap(),
        )
        .unwrap();
        let completed = prepared.execute_owned().unwrap();
        assert_eq!(rendered(&completed.result), serde_json::json!([]));
        let session = prepared.session.clone();
        drop(prepared);
        fs::remove_file(path).unwrap();
        let target = fixture.0.join("result.vortex");
        completed
            .write(
                &target,
                runtime::VortexLocalPrimitiveRowExportFormat::Vortex,
                false,
            )
            .unwrap();
        assert_eq!(session.snapshot().completed_executions, 1);
        assert_eq!(session.snapshot().memory.reserved_bytes, 0);
        let reader = ResidentVortexSession::new(1 << 20, 1).unwrap();
        let source = reader.prepare_file(&target).unwrap();
        assert_eq!(source.prepare_count().execute().unwrap(), 0);
        assert_eq!(
            source.dtype().as_struct_fields_opt().unwrap().field(KEY),
            Some(DType::Primitive(PType::I16, Nullability::NonNullable))
        );
        assert_eq!(
            source.dtype().as_struct_fields_opt().unwrap().field(COUNT),
            Some(DType::Primitive(PType::U64, Nullability::NonNullable))
        );
    }
}

#[test]
fn owned_count_rejects_column_counts_wrong_order_and_memory_before_execution() {
    let fixture = Fixture::new();
    let path = standard(&fixture);
    let mut requests = vec![count_request(&path, 65_536, 1, false)];
    let mut column = count_request(&path, 0, 10, false);
    column.simple_aggregate.as_mut().unwrap().measures[0].column =
        Some(ColumnRef::new(VALUE).unwrap());
    column.projection = shardloom_plan::ProjectionRequest::columns(
        column
            .simple_aggregate
            .as_ref()
            .unwrap()
            .projected_columns(),
    );
    requests.push(column);
    for term in [0, 1] {
        let mut reverse = count_request(&path, 0, 10, true);
        reverse.simple_aggregate.as_mut().unwrap().order_by[term].descending ^= true;
        requests.push(reverse);
    }
    for request in requests {
        let prepared = prepare_aggregate(
            &request,
            VortexLocalPrimitiveExecutionPolicy::new(2).unwrap(),
        )
        .unwrap();
        let memory = prepared.session.memory().clone();
        let prepared_bytes = memory.snapshot().reserved_bytes;
        assert!(prepared.execute_owned().is_err());
        assert_eq!(prepared.snapshot().completed_executions, 0);
        assert_eq!(memory.snapshot().reserved_bytes, prepared_bytes);
        drop(prepared);
        assert_eq!(memory.snapshot().reserved_bytes, 0);
    }
    let prepared = prepare_aggregate(
        &count_request(&path, 0, 10, false),
        VortexLocalPrimitiveExecutionPolicy::new(2).unwrap(),
    )
    .unwrap();
    let memory = prepared.session.memory().clone();
    let snapshot = memory.snapshot();
    let prepared_bytes = snapshot.reserved_bytes;
    let grant = memory
        .reserve(snapshot.limit_bytes - snapshot.reserved_bytes - 1024)
        .unwrap();
    assert!(prepared.execute_owned().is_err());
    assert_eq!(prepared.snapshot().completed_executions, 0);
    drop(grant);
    let completed = prepared.execute_owned().unwrap();
    let snapshot = memory.snapshot();
    let grant = memory
        .reserve(snapshot.limit_bytes - snapshot.reserved_bytes - 1024)
        .unwrap();
    let target = fixture.0.join("denied.vortex");
    assert!(
        completed
            .write(
                &target,
                runtime::VortexLocalPrimitiveRowExportFormat::Vortex,
                false
            )
            .is_err()
    );
    assert!(!target.exists());
    drop(grant);
    assert_eq!(memory.snapshot().reserved_bytes, prepared_bytes);
    drop(prepared);
    assert_eq!(memory.snapshot().reserved_bytes, 0);

    let nullable = Fixture::new();
    let path = nullable.source(
        PrimitiveArray::new(vec![1_i64, 2], Validity::from_iter([true, false])).into_array(),
        vec![1, 2],
    );
    let prepared = prepare_aggregate(
        &count_request(&path, 0, 10, false),
        VortexLocalPrimitiveExecutionPolicy::new(2).unwrap(),
    )
    .unwrap();
    assert!(prepared.execute_owned().is_err());
    assert_eq!(prepared.snapshot().completed_executions, 0);
}

#[test]
fn owned_count_and_distinct_decline_nullable_parent_before_native_allocation() {
    let path = Path::new("/not-opened-nullable-parent.vortex");
    let session = ResidentVortexSession::new(64 << 10, 1).unwrap();
    let dtype = DType::struct_(
        [
            (KEY, DType::Primitive(PType::I64, Nullability::NonNullable)),
            (
                VALUE,
                DType::Primitive(PType::U64, Nullability::NonNullable),
            ),
        ],
        Nullability::Nullable,
    );
    for query in [count_request(path, 0, 3, false), request(path, 0, 3)] {
        let result =
            runtime::aggregate_owned::OwnedAggregateFinalizer::new(&query, &dtype, &session);
        assert!(
            result
                .err()
                .unwrap()
                .to_string()
                .contains("nonnullable Struct")
        );
        assert_eq!(session.memory().snapshot().reserved_bytes, 0);
        assert_eq!(session.snapshot().prepared_source_opens, 0);
        assert_eq!(session.snapshot().completed_executions, 0);
    }
}

#[test]
fn owned_count_native_sink_preserves_all_values_after_source_drop_and_existing_destination() {
    let fixture = Fixture::new();
    let keys = [i64::MIN, 0, i64::MAX, 0, i64::MIN, i64::MAX, 0];
    let path = fixture.physical_pair_batches(&keys.map(|key| (key, 1)));
    let prepared = prepare_aggregate(
        &count_request(&path, 0, 10, false),
        VortexLocalPrimitiveExecutionPolicy::new(2).unwrap(),
    )
    .unwrap();
    let target = fixture.0.join("existing.vortex");
    fs::write(&target, b"foreign destination").unwrap();
    let completed = prepared.execute_owned().unwrap();
    assert!(
        completed
            .write(
                &target,
                runtime::VortexLocalPrimitiveRowExportFormat::Vortex,
                true
            )
            .is_err()
    );
    assert_eq!(fs::read(&target).unwrap(), b"foreign destination");
    let completed = prepared.execute_owned().unwrap();
    let session = prepared.session.clone();
    drop(prepared);
    fs::remove_file(path).unwrap();
    assert_eq!(rendered(&completed.result), count_oracle(keys, 0, 10));
    let target = fixture.0.join("result.vortex");
    let report = completed
        .write(
            &target,
            runtime::VortexLocalPrimitiveRowExportFormat::Vortex,
            false,
        )
        .unwrap();
    assert_eq!(
        report
            .evidence
            .native_array_sink
            .unwrap()
            .scalar_values_materialized,
        0
    );
    assert_eq!(session.snapshot().completed_executions, 2);
    assert_eq!(session.snapshot().prepared_source_opens, 1);
    assert_eq!(session.snapshot().memory.reserved_bytes, 0);
    let reader = ResidentVortexSession::new(1 << 20, 1).unwrap();
    let source = reader.prepare_file(&target).unwrap();
    let result = source
        .prepare_projection(&[KEY, COUNT], 10, 1024)
        .unwrap()
        .execute()
        .unwrap();
    assert_eq!(rendered(&result), count_oracle(keys, 0, 10));
}

#[cfg(feature = "universal-format-io")]
#[test]
fn owned_count_compatibility_sinks_keep_complete_values_after_source_drop() {
    use arrow_array::{Array as _, Int64Array, UInt64Array};
    use runtime::VortexLocalPrimitiveRowExportFormat as Format;
    let keys = [i64::MIN, 0, i64::MAX, 0, i64::MIN, i64::MAX, 0];
    for (format, offset, limit) in [
        (Format::ArrowIpc, 0, 10),
        (Format::Parquet, 0, 10),
        (Format::ArrowIpc, 1, 1),
        (Format::Parquet, 1, 1),
        (Format::ArrowIpc, 8, 2),
        (Format::Parquet, 8, 2),
    ] {
        let fixture = Fixture::new();
        let path = fixture.physical_pair_batches(&keys.map(|key| (key, 1)));
        let prepared = prepare_aggregate(
            &count_request(&path, offset, limit, false),
            VortexLocalPrimitiveExecutionPolicy::new(2).unwrap(),
        )
        .unwrap();
        let result = prepared.execute_owned().unwrap();
        let session = prepared.session.clone();
        drop(prepared);
        fs::remove_file(path).unwrap();
        let target = fixture.0.join(format.as_str());
        let report = result.write(&target, format, false).unwrap();
        let expected = count_oracle(keys, offset, limit);
        assert_eq!(
            report.rows_written,
            expected.as_array().unwrap().len() as u64
        );
        assert_eq!(report.projected_columns, vec![KEY, COUNT]);
        assert_eq!(session.snapshot().completed_executions, 1);
        assert_eq!(session.snapshot().memory.reserved_bytes, 0);
        let batches = if format == Format::ArrowIpc {
            arrow_ipc::reader::FileReader::try_new(fs::File::open(&target).unwrap(), None)
                .unwrap()
                .collect::<std::result::Result<Vec<_>, _>>()
                .unwrap()
        } else {
            parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(
                fs::File::open(&target).unwrap(),
            )
            .unwrap()
            .build()
            .unwrap()
            .collect::<std::result::Result<Vec<_>, _>>()
            .unwrap()
        };
        let mut rows = Vec::new();
        for batch in batches {
            assert_eq!(batch.num_columns(), 2);
            assert_eq!(batch.schema().field(0).name(), KEY);
            assert_eq!(batch.schema().field(1).name(), COUNT);
            let keys = batch
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            let counts = batch
                .column(1)
                .as_any()
                .downcast_ref::<UInt64Array>()
                .unwrap();
            assert_eq!(keys.null_count(), 0);
            assert_eq!(counts.null_count(), 0);
            for row in 0..batch.num_rows() {
                rows.push(serde_json::json!({KEY:keys.value(row), COUNT:counts.value(row)}));
            }
        }
        assert_eq!(serde_json::json!(rows), expected);
    }
}
