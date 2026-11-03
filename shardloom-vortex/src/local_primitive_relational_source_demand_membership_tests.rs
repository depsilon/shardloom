//! Subquery demand keeps only membership keys and explicitly hidden guards.

use super::*;
use crate::relational_query::{
    VortexRelationalFilter, VortexRelationalSubquery, VortexRelationalSubqueryKind as SubqueryKind,
};
use arrow_array::{Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use parquet::file::properties::EnabledStatistics;
use serde_json::json;
use shardloom_core::{ComparisonOp, ScalarValue};
use std::sync::Arc;

fn fixture(rows: &[(i64, Option<&'static str>)]) -> ParquetFixture {
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("n", DataType::Int64, false),
            Field::new("other", DataType::Int64, false),
            Field::new("text", DataType::Utf8, true),
        ])),
        vec![
            Arc::new(Int64Array::from_iter_values(rows.iter().map(|(n, _)| *n))),
            Arc::new(Int64Array::from_iter_values(
                rows.iter().map(|(n, _)| *n + 100),
            )),
            Arc::new(StringArray::from(
                rows.iter().map(|(_, text)| *text).collect::<Vec<_>>(),
            )),
        ],
    )
    .unwrap();
    ParquetFixture::with_batch_rows(batch, EnabledStatistics::Chunk, 2)
}

fn key(left: &str, right: &str) -> VortexRelationalJoinKey {
    VortexRelationalJoinKey {
        left: ColumnRef::new(left).unwrap(),
        right: ColumnRef::new(right).unwrap(),
    }
}

fn compare(name: &str, op: ComparisonOp, value: i64) -> Expression {
    Expression::new(
        ExprId::new("membership-demand-comparison").unwrap(),
        ExpressionKind::Compare {
            left: Box::new(column(name)),
            op,
            right: Box::new(Expression::literal(
                ExprId::new("membership-demand-literal").unwrap(),
                ScalarValue::Int64(value),
            )),
        },
    )
}

fn membership(
    outer: &ParquetFixture,
    relation: VortexRelationalPlan,
    negated: bool,
    evaluation_guard: Option<Expression>,
) -> VortexRelationalPlan {
    let subquery = VortexRelationalPlan::Subquery(Box::new(VortexRelationalSubquery {
        input: outer.scan(&[], None),
        relation,
        kind: SubqueryKind::In {
            columns: vec![key("text", "text")],
        },
        evaluation_guard,
        correlation: vec![],
        output_column: "answer".into(),
        negated,
    }));
    project(subquery, &[("answer", "marker")])
}

fn prepare(fixtures: &[&ParquetFixture], plan: &VortexRelationalPlan) -> PreparedVortexRelational {
    prepare_relational_with_schema(policy(), |preparation| {
        for fixture in fixtures {
            let source = LocalColumnarSource::admit(
                &fixture.path(),
                "parquet",
                &preparation.shared_memory_pool(),
                &preparation.cancellation_token(),
            )?;
            preparation.register_local_source(
                DatasetUri::new(fixture.path().display().to_string())?,
                source,
            )?;
        }
        Ok(plan.clone())
    })
    .unwrap()
}

#[test]
fn parquet_membership_keeps_fallible_inner_projection_and_lazy_guard() {
    let outer = fixture(&[(1, Some("hit"))]);
    let inner = fixture(&[(1, Some("hit"))]);
    let division = Expression::new(
        ExprId::new("division").unwrap(),
        ExpressionKind::Binary {
            left: Box::new(column("other")),
            op: shardloom_core::BinaryOp::Divide,
            right: Box::new(Expression::literal(
                ExprId::new("zero").unwrap(),
                ScalarValue::Int64(0),
            )),
        },
    );
    let relation = VortexRelationalPlan::Project(Box::new(VortexRelationalProject {
        input: inner.scan(&[], None),
        expressions: vec![("text".into(), column("text")), ("unused".into(), division)],
    }));
    for enabled in [false, true] {
        let guard = compare("n", ComparisonOp::Eq, i64::from(enabled));
        let plan = membership(&outer, relation.clone(), false, Some(guard));
        let prepared = prepare(&[&outer, &inner], &plan);
        let initial = prepared.snapshot().memory.reserved_bytes;
        let result = prepared.collect_jsonl(&CancellationToken::default());
        if enabled {
            let error = result.err().expect("guarded computation must execute");
            assert!(error.to_string().contains("division by zero"), "{error}");
        } else {
            assert_eq!(json_rows(&result.unwrap()), [json!({"marker":null})]);
        }
        assert_eq!(prepared.snapshot().memory.reserved_bytes, initial);
    }
}

#[test]
fn parquet_membership_parent_projection_keeps_only_nullable_keys_for_in_and_not_in() {
    let outer_rows = [
        (0, None),
        (1, Some("hit")),
        (2, Some("miss")),
        (3, None),
        (4, Some("hit")),
        (5, Some("other")),
        (6, None),
    ];
    let inner_rows = [(10, Some("hit")), (11, Some("other"))];

    for (negated, expected) in [
        (
            false,
            vec![
                json!({"marker":null}),
                json!({"marker":true}),
                json!({"marker":false}),
                json!({"marker":null}),
                json!({"marker":true}),
                json!({"marker":true}),
                json!({"marker":null}),
            ],
        ),
        (
            true,
            vec![
                json!({"marker":null}),
                json!({"marker":false}),
                json!({"marker":true}),
                json!({"marker":null}),
                json!({"marker":false}),
                json!({"marker":false}),
                json!({"marker":null}),
            ],
        ),
    ] {
        let outer = fixture(&outer_rows);
        let inner = fixture(&inner_rows);
        // The result projection depends only on the membership value. Neither
        // side's unrelated `other` payload should be opened by the reader.
        outer.corrupt_chunk(0, 1);
        inner.corrupt_chunk(0, 1);
        let plan = membership(&outer, inner.scan(&[], None), negated, None);
        let prepared = prepare(&[&outer, &inner], &plan);
        let initial = prepared.snapshot().memory.reserved_bytes;
        let result = prepared
            .collect_jsonl(&CancellationToken::default())
            .unwrap();
        assert_eq!(json_rows(&result), expected, "negated={negated}");
        let input = result.execution.input.as_ref().unwrap();
        assert_eq!(
            (
                input.columnar_projected_columns,
                input.columnar_available_columns
            ),
            (2, 6),
            "negated={negated}"
        );
        assert!(input.end_of_input_observed && input.output_ownership_detached);
        assert!(result.execution.native_io_certificate.is_certified());
        assert!(
            !result
                .execution
                .native_io_certificate
                .source_capability_report
                .fallback_attempted
        );
        drop(result);
        assert_eq!(prepared.snapshot().memory.reserved_bytes, initial);
    }
}

#[test]
fn parquet_membership_demand_retains_hidden_guard_and_inner_filter_columns() {
    let outer_rows = [
        (0, Some("not-used")),
        (1, Some("hit")),
        (2, Some("miss")),
        (3, None),
        (4, Some("hit")),
        (5, Some("other")),
        (6, None),
    ];
    let inner_rows = [
        (0, Some("not-used")),
        (1, Some("hit")),
        (2, Some("late-hit")),
    ];
    let outer = fixture(&outer_rows);
    let inner = fixture(&inner_rows);
    outer.corrupt_chunk(0, 1);
    inner.corrupt_chunk(0, 1);

    let filtered_inner = VortexRelationalPlan::Filter(Box::new(VortexRelationalFilter {
        input: inner.scan(&[], None),
        predicate: compare("n", ComparisonOp::Lt, 2),
    }));
    let plan = membership(
        &outer,
        filtered_inner,
        false,
        Some(compare("n", ComparisonOp::Gt, 0)),
    );
    let prepared = prepare(&[&outer, &inner], &plan);
    let initial = prepared.snapshot().memory.reserved_bytes;
    let result = prepared
        .collect_jsonl(&CancellationToken::default())
        .unwrap();
    assert_eq!(
        json_rows(&result),
        vec![
            json!({"marker":null}), // Guard false; key would otherwise match.
            json!({"marker":true}),
            json!({"marker":false}),
            json!({"marker":null}),
            json!({"marker":true}),
            json!({"marker":false}),
            json!({"marker":null}),
        ]
    );
    let input = result.execution.input.as_ref().unwrap();
    assert_eq!(
        (
            input.columnar_projected_columns,
            input.columnar_available_columns
        ),
        (4, 6)
    );
    assert!(input.end_of_input_observed && input.output_ownership_detached);
    assert!(result.execution.native_io_certificate.is_certified());
    assert!(
        !result
            .execution
            .native_io_certificate
            .source_capability_report
            .fallback_attempted
    );
    drop(result);
    assert_eq!(prepared.snapshot().memory.reserved_bytes, initial);
}
