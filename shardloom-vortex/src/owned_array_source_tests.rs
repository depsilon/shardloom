use super::*;
use crate::{
    VortexAggregateOrderExpr, VortexSimpleAggregateMeasure, VortexSimpleAggregateRequest,
    resident_memory_source::{
        MemoryColumn, MemoryColumnValues, MemorySourceBounds, ResidentMemorySource,
    },
};
use serde_json::{Value, json};
use shardloom_core::{ColumnRef, ComparisonOp, PredicateExpr, StatValue};

fn values(
    executed: &crate::local_primitives::prepared_aggregate::ExecutedVortexAggregate,
) -> Value {
    let summary = executed.report.result_summary.as_deref().unwrap();
    serde_json::from_str::<Value>(summary.rsplit_once(" values=").unwrap().1).unwrap()["values"]
        .clone()
}

#[test]
#[allow(clippy::too_many_lines)] // Keep the complete mixed-type oracle and ownership release together.
fn nullable_filtered_multi_measure_uses_shared_exact_semantics() {
    let session = ResidentVortexSession::new(32 << 20, 1).unwrap();
    let memory = session.memory().clone();
    let input = ResidentMemorySource::from_columns(
        &session,
        &[
            MemoryColumn {
                name: "account",
                values: MemoryColumnValues::Int64(&[
                    Some(7),
                    Some(7),
                    None,
                    Some(9),
                    Some(9),
                    Some(7),
                ]),
            },
            MemoryColumn {
                name: "amount",
                values: MemoryColumnValues::Int64(&[
                    Some(4),
                    None,
                    Some(90),
                    Some(6),
                    Some(-1),
                    Some(8),
                ]),
            },
            MemoryColumn {
                name: "label",
                values: MemoryColumnValues::Utf8(&[
                    Some("λ"),
                    Some("λ"),
                    Some("hidden"),
                    Some("東京"),
                    None,
                    Some(""),
                ]),
            },
        ],
        MemorySourceBounds::default(),
    )
    .unwrap();
    let projection = input
        .prepare_projection(&["account", "amount", "label"], None, None)
        .unwrap();
    let source = OwnedArraySource::from_owned(
        projection.execute_arrays().unwrap(),
        OwnedArraySourceBounds::default(),
        &CancellationToken::default(),
    )
    .unwrap();
    let mut request = VortexQueryPrimitiveRequest::simple_aggregate(
        source.source_uri().clone(),
        VortexSimpleAggregateRequest::grouped(
            vec![ColumnRef::new("account").unwrap()],
            vec![
                VortexSimpleAggregateMeasure::new("count", None, "rows".into()),
                VortexSimpleAggregateMeasure::new(
                    "count",
                    Some(ColumnRef::new("amount").unwrap()),
                    "present".into(),
                ),
                VortexSimpleAggregateMeasure::new(
                    "sum",
                    Some(ColumnRef::new("amount").unwrap()),
                    "total".into(),
                ),
                VortexSimpleAggregateMeasure::new(
                    "min",
                    Some(ColumnRef::new("label").unwrap()),
                    "first".into(),
                ),
            ],
        )
        .with_order_by(vec![VortexAggregateOrderExpr::new("account", false)]),
    );
    request.predicate = Some(PredicateExpr::Compare {
        column: ColumnRef::new("account").unwrap(),
        op: ComparisonOp::GtEq,
        value: StatValue::Int64(7),
    });
    let prepared = source
        .prepare_aggregate(
            &request,
            VortexLocalPrimitiveExecutionPolicy::new_with_memory_gb(1, 4)
                .expect("explicit fixture allocation"),
        )
        .unwrap();
    let result = prepared.execute().unwrap();
    assert_eq!(
        values(&result),
        json!([
            {"account": 7, "rows": 3, "present": 2, "total": 12.0, "first": ""},
            {"account": 9, "rows": 2, "present": 2, "total": 5.0, "first": "東京"},
        ])
    );
    assert!(!result.report.embedded_layout.metadata_persisted_in_artifact);
    assert!(
        !result
            .report
            .embedded_layout
            .metadata_first_pruning_consulted
    );
    drop((result, prepared, source, projection, input, session));
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

#[test]
fn owned_numeric_exact_distinct_crosses_scan_splits_and_reuses_worker_policy() {
    let session = ResidentVortexSession::for_external_cpu_pool(64 << 20, 3).unwrap();
    let memory = session.memory().clone();
    let keys = (0..65_536_i64).map(|row| row % 17).collect::<Vec<_>>();
    let items = (0..65_536_i64).map(|row| row % 11).collect::<Vec<_>>();
    let input = ResidentMemorySource::from_columns(
        &session,
        &[
            MemoryColumn {
                name: "cohort",
                values: MemoryColumnValues::Int64NonNullable(&keys),
            },
            MemoryColumn {
                name: "item",
                values: MemoryColumnValues::Int64NonNullable(&items),
            },
        ],
        MemorySourceBounds::default(),
    )
    .unwrap();
    let projection = input
        .prepare_projection(&["cohort", "item"], None, None)
        .unwrap();
    let source = OwnedArraySource::from_owned(
        projection.execute_arrays().unwrap(),
        OwnedArraySourceBounds::default(),
        &CancellationToken::default(),
    )
    .unwrap();
    let request = VortexQueryPrimitiveRequest::simple_aggregate(
        source.source_uri().clone(),
        VortexSimpleAggregateRequest::grouped(
            vec![ColumnRef::new("cohort").unwrap()],
            vec![VortexSimpleAggregateMeasure::new(
                "count_distinct",
                Some(ColumnRef::new("item").unwrap()),
                "unique_items".into(),
            )],
        )
        .with_order_by(vec![VortexAggregateOrderExpr::new("cohort", false)]),
    );
    let prepared = source
        .prepare_aggregate(
            &request,
            VortexLocalPrimitiveExecutionPolicy::new_with_memory_gb(3, 4).unwrap(),
        )
        .unwrap();
    drop((source, projection, input, session));
    let expected = Value::Array(
        (0..17)
            .map(|key| json!({"cohort": key, "unique_items": 11}))
            .collect(),
    );
    for _ in 0..2 {
        let result = prepared.execute().unwrap();
        assert_eq!(values(&result), expected);
        assert!(result.native_io_certificate.is_certified());
        assert!(result.report.arrays_read_count > 1);
    }
    drop(prepared);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

fn kv_memory_source(
    session: &ResidentVortexSession,
    rows: i64,
    v_fn: impl Fn(i64) -> i64,
) -> ResidentMemorySource {
    let keys: Vec<i64> = (0..rows).collect();
    let vals: Vec<i64> = keys.iter().copied().map(v_fn).collect();
    ResidentMemorySource::from_columns(
        session,
        &[
            MemoryColumn {
                name: "k",
                values: MemoryColumnValues::Int64NonNullable(&keys),
            },
            MemoryColumn {
                name: "v",
                values: MemoryColumnValues::Int64NonNullable(&vals),
            },
        ],
        MemorySourceBounds::default(),
    )
    .unwrap()
}

fn owned_rel_policy(bytes: u64) -> VortexLocalPrimitiveExecutionPolicy {
    use shardloom_core::{ExecutionResourceOrigin, ExecutionResources};
    VortexLocalPrimitiveExecutionPolicy::from_resources(
        ExecutionResources::from_bytes(bytes, 1, ExecutionResourceOrigin::Context).unwrap(),
    )
    .unwrap()
}

#[test]
fn owned_array_relational_scan_prunes_splits_with_zone_maps_and_preserves_values() {
    use crate::{
        local_primitives::prepared_relational::prepare_relational_with_schema_in_session,
        relational_query::{VortexRelationalPlan, VortexRelationalScan},
    };
    use shardloom_core::{ColumnRef, ComparisonOp, DatasetUri, PredicateExpr, StatValue};
    use shardloom_plan::ProjectionRequest;

    let bytes = 8 << 20;
    let session = ResidentVortexSession::new(bytes, 1).unwrap();
    let memory = session.memory().clone();
    let input = kv_memory_source(&session, 20_000, |k| k * 10);
    let batch = input
        .prepare_projection(&["k", "v"], None, None)
        .unwrap()
        .execute_arrays()
        .unwrap();
    let source = batch
        .into_owned_array_source(
            OwnedArraySourceBounds::default(),
            &CancellationToken::default(),
        )
        .unwrap();
    assert!(source.has_zone_maps());
    drop(input);

    let policy = owned_rel_policy(bytes);
    let uri = DatasetUri::new("memory://owned-pruned-relational").unwrap();
    let run_with_threshold = |threshold: i64| {
        let prepared = prepare_relational_with_schema_in_session(policy, &session, |schemas| {
            schemas.register_owned_array_source(uri.clone(), source.clone())?;
            Ok(VortexRelationalPlan::Scan(VortexRelationalScan {
                source_uri: uri.clone(),
                projection: ProjectionRequest::Columns(vec![
                    ColumnRef::new("k").unwrap(),
                    ColumnRef::new("v").unwrap(),
                ]),
                predicate: Some(PredicateExpr::Compare {
                    column: ColumnRef::new("k").unwrap(),
                    op: ComparisonOp::GtEq,
                    value: StatValue::Int64(threshold),
                }),
            }))
        })
        .unwrap();
        let executed = prepared
            .execute_native(&CancellationToken::default())
            .unwrap();
        (prepared, executed)
    };

    let (prepared, executed) = run_with_threshold(17_000);
    assert_eq!(executed.execution.output_rows, 3_000);
    assert_eq!(source.pruned_splits(), 2);
    assert_eq!(source.pruned_rows(), 16_384);
    assert!(executed.execution.native_io_certificate.is_certified());
    let rendered = executed
        .result
        .to_bounded_json(&["k".into(), "v".into()], 512 * 1024)
        .unwrap();
    let rows: Vec<Value> = serde_json::from_str(rendered.value()).unwrap();
    assert_eq!(rows.len(), 3_000);
    assert_eq!(rows[0], json!({"k": 17_000, "v": 170_000}));
    assert_eq!(rows[2_999], json!({"k": 19_999, "v": 199_990}));

    let (prepared_empty, executed_empty) = run_with_threshold(25_000);
    assert_eq!(executed_empty.execution.output_rows, 0);
    assert_eq!(source.pruned_splits(), 5);
    assert_eq!(source.pruned_rows(), 36_384);
    assert!(
        executed_empty
            .execution
            .native_io_certificate
            .is_certified()
    );

    drop((
        executed_empty,
        prepared_empty,
        rendered,
        executed,
        prepared,
        source,
        session,
    ));
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

#[test]
fn prepared_collect_owned_array_multi_step_relational_chain_zero_serialization() {
    use crate::{
        local_primitives::prepared_relational::prepare_relational_with_schema_in_session,
        relational_query::{VortexRelationalPlan, VortexRelationalProject, VortexRelationalScan},
    };
    use shardloom_core::{
        ColumnRef, ComparisonOp, DatasetUri, ExprId, Expression, PredicateExpr, StatValue,
    };
    use shardloom_plan::ProjectionRequest;

    let bytes = 8 << 20;
    let session = ResidentVortexSession::new(bytes, 1).unwrap();
    let memory = session.memory().clone();
    let input = kv_memory_source(&session, 20_000, |k| k + 1);
    let policy = owned_rel_policy(bytes);
    let uri1 = DatasetUri::new("memory://chain-stage-1").unwrap();
    let prep1 = prepare_relational_with_schema_in_session(policy, &session, |schemas| {
        schemas.register_memory_source(uri1.clone(), |_| Ok(input))?;
        Ok(VortexRelationalPlan::Scan(VortexRelationalScan {
            source_uri: uri1,
            projection: ProjectionRequest::All,
            predicate: Some(PredicateExpr::Compare {
                column: ColumnRef::new("k").unwrap(),
                op: ComparisonOp::GtEq,
                value: StatValue::Int64(1_000),
            }),
        }))
    })
    .unwrap();
    let stage1 = prep1.execute_native(&CancellationToken::default()).unwrap();
    assert_eq!(stage1.execution.output_rows, 19_000);
    drop(prep1);

    let owned_source = stage1
        .result
        .into_owned_array_source(
            OwnedArraySourceBounds::default(),
            &CancellationToken::default(),
        )
        .unwrap();
    assert!(owned_source.has_zone_maps());

    let uri2 = DatasetUri::new("memory://chain-stage-2").unwrap();
    let prep2 = prepare_relational_with_schema_in_session(policy, &session, |schemas| {
        schemas.register_owned_array_source(uri2.clone(), owned_source.clone())?;
        Ok(VortexRelationalPlan::Project(Box::new(
            VortexRelationalProject {
                input: VortexRelationalPlan::Scan(VortexRelationalScan {
                    source_uri: uri2,
                    projection: ProjectionRequest::All,
                    predicate: Some(PredicateExpr::Compare {
                        column: ColumnRef::new("k").unwrap(),
                        op: ComparisonOp::GtEq,
                        value: StatValue::Int64(18_000),
                    }),
                }),
                expressions: vec![(
                    "k".into(),
                    Expression::column(ExprId::new("k").unwrap(), ColumnRef::new("k").unwrap()),
                )],
            },
        )))
    })
    .unwrap();
    let stage2 = prep2.execute_native(&CancellationToken::default()).unwrap();
    assert_eq!(stage2.execution.output_rows, 2_000);
    assert!(owned_source.pruned_splits() >= 2);
    assert!(stage2.execution.native_io_certificate.is_certified());
    let rendered = stage2
        .result
        .to_bounded_json(&["k".into()], 256 * 1024)
        .unwrap();
    let rows: Vec<Value> = serde_json::from_str(rendered.value()).unwrap();
    assert_eq!(rows.len(), 2_000);
    assert_eq!(rows[0], json!({"k": 18_000}));
    assert_eq!(rows[1_999], json!({"k": 19_999}));

    drop((
        rendered,
        stage2,
        prep2,
        owned_source,
        stage1.execution,
        session,
    ));
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

#[test]
fn owned_scan_sub_zone_batches_and_concurrent_scans_isolate_pruning_certificates() {
    use crate::{
        local_primitives::prepared_relational::prepare_relational_with_schema_in_session,
        relational_query::{VortexRelationalPlan, VortexRelationalScan},
    };
    use shardloom_core::{
        ColumnRef, ComparisonOp, DatasetUri, PredicateExpr, StatValue,
        native_io_decisions::NativeIoCapability,
    };
    use shardloom_plan::ProjectionRequest;

    let bytes = 16 << 20;
    let session = ResidentVortexSession::new(bytes, 2).unwrap();
    let memory = session.memory().clone();
    let input = kv_memory_source(&session, 8_192, |k| k);
    let batch = input
        .prepare_projection(&["k", "v"], None, None)
        .unwrap()
        .execute_arrays()
        .unwrap();
    let source = batch
        .into_owned_array_source(
            OwnedArraySourceBounds::default(),
            &CancellationToken::default(),
        )
        .unwrap();
    drop(input);

    // Concurrent scans with disjoint pruning filters must report exact per-scan
    // excluded_regions <= regions without cross-scan accumulation or sub-8192 overcounting.
    let policy = VortexLocalPrimitiveExecutionPolicy::from_resources(
        shardloom_core::ExecutionResources::from_bytes(
            bytes,
            2,
            shardloom_core::ExecutionResourceOrigin::Context,
        )
        .unwrap(),
    )
    .unwrap();
    let uri = DatasetUri::new("memory://owned-concurrent-prune").unwrap();
    let run_scan = |threshold: i64| {
        let prep = prepare_relational_with_schema_in_session(policy, &session, |schemas| {
            schemas.register_owned_array_source(uri.clone(), source.clone())?;
            Ok(VortexRelationalPlan::Scan(VortexRelationalScan {
                source_uri: uri.clone(),
                projection: ProjectionRequest::Columns(vec![ColumnRef::new("k").unwrap()]),
                predicate: Some(PredicateExpr::Compare {
                    column: ColumnRef::new("k").unwrap(),
                    op: ComparisonOp::GtEq,
                    value: StatValue::Int64(threshold),
                }),
            }))
        })
        .unwrap();
        let out = prep.execute_native(&CancellationToken::default()).unwrap();
        let entry = *out
            .execution
            .native_io_certificate
            .capability_decisions
            .get(NativeIoCapability::RegionExclusion)
            .expect("RegionExclusion decision");
        (out.execution.output_rows, entry)
    };
    let (res_pruned, res_kept) = std::thread::scope(|s| {
        let h1 = s.spawn(|| run_scan(99_999));
        let h2 = s.spawn(|| run_scan(0));
        (h1.join().unwrap(), h2.join().unwrap())
    });
    assert_eq!(res_pruned.0, 0);
    assert_eq!(res_pruned.1.observed.regions, Some(1));
    assert_eq!(res_pruned.1.observed.excluded_regions, Some(1));
    assert_eq!(res_kept.0, 8_192);
    assert_eq!(res_kept.1.observed.regions, Some(1));
    assert_eq!(res_kept.1.observed.excluded_regions, Some(0));

    // Also verify sub-zone SplitBy::RowCount(2048) never overcounts per-scan excluded_zones.
    let (chunks, excluded_zones) = session
        .with_native_session(|vortex_session, runtime| {
            let (scan, stats) = source.scan_with_stats(vortex_session);
            let expr = vortex::array::expr::lt(
                vortex::array::expr::get_item("k", vortex::array::expr::root()),
                vortex::array::expr::lit(0_i64),
            )
            .bind(source.dtype())
            .map_err(|e| shardloom_core::ShardLoomError::InvalidOperation(e.to_string()))?;
            let chunks: Vec<_> = scan
                .with_split_by(vortex::layout::scan::split_by::SplitBy::RowCount(2048))
                .with_filter(expr)
                .into_array_iter(runtime)
                .map_err(|e| shardloom_core::ShardLoomError::InvalidOperation(e.to_string()))?
                .collect::<std::result::Result<Vec<_>, _>>()
                .map_err(|e| shardloom_core::ShardLoomError::InvalidOperation(e.to_string()))?;
            Ok((chunks, stats.excluded_zones()))
        })
        .unwrap();
    assert!(chunks.iter().all(vortex::array::ArrayRef::is_empty));
    assert_eq!(excluded_zones, 1);

    drop((chunks, source, session));
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

#[test]
fn owned_zone_index_respects_max_metadata_bytes_and_skips_non_prunable_only_schemas() {
    use arrow_array::types::Int64Type;
    use arrow_array::{ArrayRef as ArrowArrayRef, ListArray, StringArray};

    let session = ResidentVortexSession::new(8 << 20, 1).unwrap();
    let memory = session.memory().clone();
    // Schema with only a List column (non-prunable): ZoneIndex reserves 0 bytes.
    let list_col: ArrowArrayRef =
        Arc::new(ListArray::from_iter_primitive::<Int64Type, _, _>(vec![
            Some(vec![Some(1), Some(2)]),
            Some(vec![Some(3)]),
        ]));
    let list_batch = arrow_array::RecordBatch::try_from_iter(vec![("items", list_col)]).unwrap();
    let list_src = ResidentMemorySource::from_arrow_record_batch(
        &session,
        list_batch,
        &CancellationToken::default(),
    )
    .unwrap();
    let list_out = list_src
        .prepare_projection(&["items"], None, None)
        .unwrap()
        .execute_arrays()
        .unwrap();
    let list_owned = list_out
        .into_owned_array_source(
            OwnedArraySourceBounds::default(),
            &CancellationToken::default(),
        )
        .unwrap();
    assert!(!list_owned.has_zone_maps());

    // Tight max_metadata_bytes fails deterministically before retaining string extrema.
    let strings: ArrowArrayRef = Arc::new(StringArray::from(vec![Some("a".repeat(200))]));
    let str_batch = arrow_array::RecordBatch::try_from_iter(vec![("s", strings)]).unwrap();
    let str_src = ResidentMemorySource::from_arrow_record_batch(
        &session,
        str_batch,
        &CancellationToken::default(),
    )
    .unwrap();
    let str_out = str_src
        .prepare_projection(&["s"], None, None)
        .unwrap()
        .execute_arrays()
        .unwrap();
    let tight = OwnedArraySourceBounds {
        max_metadata_bytes: 512,
        ..OwnedArraySourceBounds::default()
    };
    assert!(
        str_out
            .into_owned_array_source(tight, &CancellationToken::default())
            .is_err()
    );

    drop((list_owned, list_src, str_src, session));
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}
