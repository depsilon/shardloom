//! Produce real helper fields, persist them, and observe the actual native scan.

use super::*;
use crate::{
    VortexAggregateExpression, VortexAggregateOrderExpr, VortexLocalPrimitiveExecutionPolicy,
    VortexPreparedStateColumnarWriteRequest, VortexQueryPrimitiveRequest,
    VortexSimpleAggregateMeasure, VortexSimpleAggregateRequest,
    execute_vortex_local_primitive_with_policy, write_flat_columnar_vortex_prepared_state,
};
use shardloom_core::{ColumnRef, DatasetUri, ExecutionResourceOrigin, ExecutionResources};
use std::path::PathBuf;
use vortex::{
    VortexSessionDefault as _,
    array::dtype::{DType, Nullability, PType},
    file::OpenOptionsSessionExt as _,
    io::{
        runtime::{BlockingRuntime as _, current::CurrentThreadRuntime},
        session::RuntimeSessionExt as _,
    },
    session::VortexSession,
};

const MINUTE: &str = "__shardloom_derived_extract_minute_EventTime";
const BUCKET: &str = "__shardloom_derived_date_trunc_minute_EventTime";

struct Fixture(PathBuf);

impl Drop for Fixture {
    fn drop(&mut self) {
        std::fs::remove_dir_all(&self.0).unwrap();
    }
}

fn persist(time: ArrayRef, legacy_nullable_helpers: bool, label: &str) -> Fixture {
    let root = std::env::temp_dir().join(format!(
        "shardloom_persisted_helper_{label}_{}",
        std::process::id(),
    ));
    std::fs::create_dir(&root).unwrap();
    let fixture = Fixture(root);
    let produced = append(time, false).unwrap();
    let mut fields: Vec<_> = produced
        .schema()
        .fields()
        .iter()
        .map(|field| {
            if legacy_nullable_helpers && field.name() != "EventTime" {
                Arc::new(field.as_ref().clone().with_nullable(true))
            } else {
                Arc::clone(field)
            }
        })
        .collect();
    fields.extend([
        Arc::new(Field::new("UserID", DataType::Int64, false)),
        Arc::new(Field::new("SearchPhrase", DataType::Utf8, false)),
    ]);
    let schema = Arc::new(Schema::new(fields));
    let mut columns = produced.columns().to_vec();
    columns.push(Arc::new(Int64Array::from(vec![42, 42, 7, 7, 42])));
    columns.push(Arc::new(StringArray::from(vec![
        "alpha", "alpha", "beta", "beta", "gamma",
    ])));
    let batch = RecordBatch::try_new(Arc::clone(&schema), columns).unwrap();
    let names: Vec<_> = schema
        .fields()
        .iter()
        .map(|field| field.name().clone())
        .collect();
    let source = FlatLocalColumnarSource {
        header: names.clone(),
        column_dtypes: vec![None; names.len()],
        column_arrow_dtypes: schema
            .fields()
            .iter()
            .map(|field| Some(field.data_type().clone()))
            .collect(),
        materialized_columns: names.clone(),
        reader_projection_columns: names,
        batches: vec![batch],
        row_count: 5,
    };
    let report =
        write_flat_columnar_vortex_prepared_state(VortexPreparedStateColumnarWriteRequest::new(
            fixture.0.join("prepared.vortex"),
            source,
            ExecutionResources::from_bytes(16 << 20, 2, ExecutionResourceOrigin::ExecutionCall)
                .unwrap(),
        ))
        .unwrap();
    assert_eq!(report.row_count, 5);
    assert_eq!(report.reopen_row_count, 5);
    fixture
}

fn assert_execution(fixture: &Fixture, helper_nullable: bool) {
    let path = fixture.0.join("prepared.vortex");
    let runtime = CurrentThreadRuntime::new();
    let session = VortexSession::default().with_handle(runtime.handle());
    let file = runtime
        .block_on(session.open_options().open_path(&path))
        .unwrap();
    let DType::Struct(fields, _) = file.dtype() else {
        panic!("persisted struct required");
    };
    let nullability = if helper_nullable {
        Nullability::Nullable
    } else {
        Nullability::NonNullable
    };
    assert_eq!(
        fields.field(MINUTE),
        Some(DType::Primitive(PType::U8, nullability))
    );
    assert_eq!(
        fields.field(BUCKET),
        Some(DType::Primitive(PType::I64, nullability))
    );
    drop(file);
    let aggregate = VortexSimpleAggregateRequest::grouped(
        vec![
            ColumnRef::new("UserID").unwrap(),
            ColumnRef::new("SearchPhrase").unwrap(),
        ],
        vec![VortexSimpleAggregateMeasure::new(
            "count",
            None,
            "rows".into(),
        )],
    )
    .with_group_expressions(vec![VortexAggregateExpression::new(
        "EventMinute".into(),
        ColumnRef::new("EventTime").unwrap(),
        "extract_minute",
    )])
    .with_order_by(vec![VortexAggregateOrderExpr::new("rows", true)]);
    let request = VortexQueryPrimitiveRequest::simple_aggregate(
        DatasetUri::new(path.display().to_string()).unwrap(),
        aggregate,
    )
    .with_source_order_limit(3);
    let mut policy = VortexLocalPrimitiveExecutionPolicy::new_with_memory_gb(2, 1).unwrap();
    policy.resource_envelope.memory_budget_bytes = 16 << 20;
    let report = execute_vortex_local_primitive_with_policy(&request, policy).unwrap();
    assert!(!report.has_errors());
    assert!(!report.fallback_execution_allowed);
    let scan = report.scan_columns.unwrap();
    assert_eq!(
        scan.projection_columns,
        vec![
            "UserID",
            "SearchPhrase",
            if helper_nullable { "EventTime" } else { MINUTE }
        ]
    );
    assert_eq!(scan.predicate_columns, [] as [String; 0]);
    let summary = report.result_summary.unwrap();
    let (_, payload) = summary.rsplit_once(" values=").unwrap();
    let value: serde_json::Value = serde_json::from_str(payload).unwrap();
    assert_eq!(
        value["aggregate_workers_family"],
        "complete_numeric_minute_string_partitions"
    );
    assert_eq!(value["aggregate_workers_triple_rows"], 5);
    assert!(
        value["aggregate_workers_submitted_chunks"]
            .as_u64()
            .unwrap()
            > 0
    );
    assert_eq!(
        value["values"],
        serde_json::json!([
            {"UserID":7,"SearchPhrase":"beta","EventMinute":2,"rows":2},
            {"UserID":42,"SearchPhrase":"alpha","EventMinute":1,"rows":2},
            {"UserID":42,"SearchPhrase":"gamma","EventMinute":1,"rows":1},
        ])
    );
}

#[test]
fn persisted_typed_helpers_replace_raw_time_and_keep_specialized_workers() {
    let sources: Vec<(ArrayRef, &str)> = vec![
        (
            Arc::new(Int64Array::from(vec![60, 61, 120, 121, 62])),
            "integer",
        ),
        (
            Arc::new(
                TimestampNanosecondArray::from(vec![
                    60_000_000_000,
                    61_000_000_000,
                    120_000_000_000,
                    121_000_000_000,
                    62_000_000_000,
                ])
                .with_timezone("UTC"),
            ),
            "timestamp",
        ),
    ];
    for (source, label) in sources {
        assert_execution(&persist(source, false, label), false);
    }
}

#[test]
fn legacy_nullable_helpers_keep_raw_time_admission_and_complete_values() {
    let source = Arc::new(Int64Array::from(vec![60, 61, 120, 121, 62]));
    assert_execution(&persist(source, true, "legacy"), true);
}
