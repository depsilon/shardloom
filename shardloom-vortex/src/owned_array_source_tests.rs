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
            VortexLocalPrimitiveExecutionPolicy::single_threaded(),
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
            VortexLocalPrimitiveExecutionPolicy::new(3).unwrap(),
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
