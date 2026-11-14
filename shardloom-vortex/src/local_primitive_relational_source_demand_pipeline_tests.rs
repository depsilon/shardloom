//! Equivalent flat programs retain source demand and evaluation barriers.

use super::*;
use crate::relational_query::{
    VortexRelationalLimit, VortexRelationalPipeline, VortexRelationalStage as Stage,
};
use serde_json::json;

fn predicate(name: &str, op: ComparisonOp, value: i64) -> Expression {
    Expression::new(
        ExprId::new("predicate").unwrap(),
        ExpressionKind::Compare {
            left: Box::new(column(name)),
            op,
            right: Box::new(Expression::literal(
                ExprId::new("literal").unwrap(),
                ScalarValue::Int64(value),
            )),
        },
    )
}

#[test]
fn parquet_demand_pipeline_projects_through_ranges_without_reading_unused_columns() {
    let fixture = ParquetFixture::new();
    for group in 0..4 {
        for column in [1, 2] {
            fixture.corrupt_chunk(group, column);
        }
    }
    let input = project(
        fixture.scan(&[], None),
        &[("text", "payload"), ("n", "key"), ("other", "unused")],
    );
    let ranged = VortexRelationalPlan::Limit(Box::new(VortexRelationalLimit {
        input,
        offset: 2,
        count: 4,
    }));
    let plan = project(ranged, &[("key", "result")]);
    for equivalent in [plan.clone(), plan.compact_streaming()] {
        let prepared = fixture.prepare(&equivalent);
        let initial = prepared.snapshot().memory.reserved_bytes;
        let result = prepared
            .collect_jsonl(&CancellationToken::default())
            .unwrap();
        assert_eq!(
            json_rows(&result),
            (2..6).map(|n| json!({"result":n})).collect::<Vec<_>>()
        );
        let report = result.execution.input.as_ref().unwrap();
        assert_eq!(report.columnar_projected_columns, 1);
        assert_eq!(report.columnar_physical_leaf_columns, 1);
        assert_eq!(report.columnar_requested_prefix_rows, 6);
        assert_eq!(report.columnar_rows_decoded, 6);
        drop(result);
        assert_eq!(prepared.snapshot().memory.reserved_bytes, initial);
    }
}

#[test]
fn parquet_demand_pipeline_retains_unused_computation_before_a_rejecting_filter() {
    let fixture = ParquetFixture::with_batch(
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("n", DataType::Int64, false),
                Field::new("text", DataType::Utf8, false),
            ])),
            vec![
                Arc::new(Int64Array::from(vec![1, 2, 3])),
                Arc::new(StringArray::from(vec!["1", "2", "invalid"])),
            ],
        )
        .unwrap(),
        EnabledStatistics::Chunk,
    );
    let computed = VortexRelationalPlan::Project(Box::new(VortexRelationalProject {
        input: fixture.scan(&[], None),
        expressions: vec![
            ("key".into(), column("n")),
            (
                "unused".into(),
                Expression::cast(
                    ExprId::new("fallible").unwrap(),
                    column("text"),
                    shardloom_core::LogicalDType::Int64,
                ),
            ),
        ],
    }));
    let filtered = VortexRelationalPlan::Filter(Box::new(VortexRelationalFilter {
        input: computed,
        predicate: predicate("key", ComparisonOp::Eq, 1),
    }));
    let plan = project(filtered, &[("key", "result")]);
    for equivalent in [plan.clone(), plan.compact_streaming()] {
        let prepared = fixture.prepare(&equivalent);
        let initial = prepared.snapshot().memory.reserved_bytes;
        assert!(
            prepared
                .collect_jsonl(&CancellationToken::default())
                .is_err()
        );
        assert_eq!(prepared.snapshot().completed_executions, 0);
        assert_eq!(prepared.snapshot().memory.reserved_bytes, initial);
    }
}

#[test]
fn parquet_demand_pipeline_many_filters_keep_balanced_source_predicates() {
    let fixture = ParquetFixture::new();
    fixture.corrupt_chunk(0, 0);
    let mut stages = vec![Stage::Project(vec![("key".into(), column("n"))])];
    stages.extend((0..128).map(|_| Stage::Filter(predicate("key", ComparisonOp::Gt, -1))));
    stages.push(Stage::Filter(predicate("key", ComparisonOp::Eq, 5)));
    let plan = VortexRelationalPlan::Pipeline(Box::new(VortexRelationalPipeline {
        input: fixture.scan(&[], None),
        stages,
    }));
    let prepared = fixture.prepare(&plan);
    let result = prepared
        .collect_jsonl(&CancellationToken::default())
        .unwrap();
    assert_eq!(json_rows(&result), [json!({"key":5})]);
    let report = result.execution.input.as_ref().unwrap();
    assert_eq!(report.columnar_projected_columns, 1);
    assert_eq!(report.columnar_row_groups_read, 1);
    assert_eq!(report.columnar_rows_decoded, 4);
}

#[test]
fn parquet_demand_pipeline_long_alias_chain_does_not_recharge_unchanged_demand() {
    let fixture = ParquetFixture::new();
    for group in 0..4 {
        fixture.corrupt_chunk(group, 2);
    }
    let mut name = "n";
    let stages = (0..512)
        .map(|_| {
            let alias = if name == "n" { "key" } else { "n" };
            let stage = Stage::Project(vec![(alias.into(), column(name))]);
            name = alias;
            stage
        })
        .collect();
    let plan = VortexRelationalPlan::Pipeline(Box::new(VortexRelationalPipeline {
        input: fixture.scan(&[], None),
        stages,
    }));
    let prepared = fixture.prepare(&plan);
    let initial = prepared.snapshot().memory.reserved_bytes;
    let result = prepared
        .collect_jsonl(&CancellationToken::default())
        .unwrap();
    assert_eq!(
        json_rows(&result),
        (0..16).map(|n| json!({"n":n})).collect::<Vec<_>>()
    );
    assert_eq!(
        result
            .execution
            .input
            .as_ref()
            .unwrap()
            .columnar_projected_columns,
        1
    );
    drop(result);
    assert_eq!(prepared.snapshot().memory.reserved_bytes, initial);
}
