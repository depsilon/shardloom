//! Plain projections forward ordering demand without changing observation domains.

use super::*;
use crate::relational_query::{VortexRelationalPipeline, VortexRelationalStage};

fn projected(input: VortexRelationalPlan, compact: bool) -> VortexRelationalPlan {
    let first = vec![
        ("ordinal".into(), col("id")),
        ("label".into(), col("s")),
        ("duplicate".into(), col("id")),
    ];
    let second = vec![
        ("label".into(), col("label")),
        ("renamed".into(), col("ordinal")),
        ("duplicate".into(), col("duplicate")),
    ];
    if compact {
        VortexRelationalPlan::Pipeline(Box::new(VortexRelationalPipeline {
            input,
            stages: vec![
                VortexRelationalStage::Project(first),
                VortexRelationalStage::Project(second),
            ],
        }))
    } else {
        let input = VortexRelationalPlan::Project(Box::new(VortexRelationalProject {
            input,
            expressions: first,
        }));
        VortexRelationalPlan::Project(Box::new(VortexRelationalProject {
            input,
            expressions: second,
        }))
    }
}

fn projected_json(row: &Row) -> serde_json::Value {
    serde_json::json!({"label": row.s, "renamed": row.id, "duplicate": row.id})
}

fn check_projected_prefix(compact: bool, offset: usize, count: usize) {
    let rows = fixture_rows();
    let keys = vec![
        key("n", true, NullOrder::Last),
        key("s", false, NullOrder::First),
    ];
    let mut expected = rows.clone();
    reference_sort(&mut expected, &keys);
    let plan = limited(projected(sorted(scan(), keys), compact), offset, count);
    let prepared = prepare_rows(&if compact {
        plan.compact_streaming()
    } else {
        plan
    });
    let dtype = prepared.output_dtype().unwrap();
    let fields = dtype.as_struct_fields_opt().unwrap();
    assert_eq!(
        fields.names(),
        &FieldNames::from(["label", "renamed", "duplicate"])
    );
    assert_eq!(
        fields.field("label").unwrap(),
        DType::Utf8(Nullability::Nullable)
    );
    assert_eq!(
        fields.field("renamed").unwrap(),
        DType::Primitive(PType::I64, Nullability::Nullable)
    );
    let memory = prepared.session.memory().clone();
    let baseline = memory.snapshot().reserved_bytes;
    let mut chunks = std::iter::once(&rows[..0]).chain(rows.chunks(17));
    let mut prior: Option<Weak<MemoryLease>> = None;
    let mut calls = 0;
    let mut input = |session: &ResidentVortexSession| {
        assert!(
            prior
                .as_ref()
                .is_none_or(|witness| witness.strong_count() == 0)
        );
        calls += 1;
        chunks
            .next()
            .map(|rows| {
                let source = rows_source(session, rows)?;
                prior = Some(source.batch_release_witness()?);
                Ok(source)
            })
            .transpose()
    };
    let result = prepared
        .with_batch_input(&mut input)
        .unwrap()
        .collect_jsonl(&CancellationToken::default())
        .unwrap();
    assert_eq!(
        json_rows(&result),
        expected
            .iter()
            .skip(offset)
            .take(count)
            .map(projected_json)
            .collect::<Vec<_>>()
    );
    assert_eq!(calls, rows.len().div_ceil(17) + 2);
    let report = result.execution.input.as_ref().unwrap();
    assert_eq!(report.rows, rows.len() as u64);
    assert!(report.end_of_input_observed && report.output_ownership_detached);
    if count == usize::MAX {
        assert_eq!(report.ordering_prefix_batches, 0);
    } else {
        assert_eq!(report.ordering_prefix_rows_validated, rows.len() as u64);
        assert!(report.ordering_prefix_batches > 0);
        if offset + count <= 10 {
            assert!(report.ordering_rows_detached < rows.len() as u64);
        }
    }
    drop(result);
    assert_eq!(memory.snapshot().reserved_bytes, baseline);
    drop(prepared);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

#[test]
fn streamed_ordering_column_projection_prefix_keeps_schema_ties_ranges_and_ownership() {
    for compact in [false, true] {
        for (offset, count) in [(3, 7), (9, 0), (200, 3), (0, usize::MAX)] {
            check_projected_prefix(compact, offset, count);
        }
    }
}

#[test]
fn streamed_ordering_column_projection_prefix_keeps_intervening_ranges() {
    let rows = fixture_rows();
    let keys = vec![key("n", false, NullOrder::First)];
    let mut expected = rows.clone();
    reference_sort(&mut expected, &keys);
    for compact in [false, true] {
        let plan = limited(
            projected(limited(sorted(scan(), keys.clone()), 3, 25), compact),
            2,
            7,
        );
        let prepared = prepare_rows(&if compact {
            plan.compact_streaming()
        } else {
            plan
        });
        let baseline = prepared.snapshot().memory.reserved_bytes;
        let mut chunks = rows.chunks(17);
        let mut input = |session: &ResidentVortexSession| {
            chunks
                .next()
                .map(|rows| rows_source(session, rows))
                .transpose()
        };
        let result = prepared
            .with_batch_input(&mut input)
            .unwrap()
            .collect_jsonl(&CancellationToken::default())
            .unwrap();
        assert_eq!(
            json_rows(&result),
            expected[5..12]
                .iter()
                .map(projected_json)
                .collect::<Vec<_>>()
        );
        assert_eq!(
            result
                .execution
                .input
                .as_ref()
                .unwrap()
                .ordering_prefix_rows_validated,
            rows.len() as u64
        );
        drop(result);
        assert_eq!(prepared.snapshot().memory.reserved_bytes, baseline);
    }
}

#[test]
fn streamed_ordering_column_projection_prefix_preserves_pipeline_expression_errors() {
    let increment = Expression::new(
        ExprId::new("increment").unwrap(),
        ExpressionKind::Binary {
            left: Box::new(col("n")),
            op: shardloom_core::BinaryOp::Add,
            right: Box::new(Expression::new(
                ExprId::new("one").unwrap(),
                ExpressionKind::Literal(shardloom_core::ScalarValue::Int64(1)),
            )),
        },
    );
    for count in [0, 1] {
        let plan = VortexRelationalPlan::Pipeline(Box::new(VortexRelationalPipeline {
            input: sorted(scan(), vec![key("n", false, NullOrder::Last)]),
            stages: vec![
                VortexRelationalStage::Project(vec![("computed".into(), increment.clone())]),
                VortexRelationalStage::Project(vec![("renamed".into(), col("computed"))]),
                VortexRelationalStage::Limit { offset: 0, count },
            ],
        }));
        let prepared = prepare(&plan, 2 << 20).unwrap();
        let baseline = prepared.snapshot().memory.reserved_bytes;
        let mut chunks = [0, i64::MAX].into_iter();
        let mut input = |session: &ResidentVortexSession| {
            chunks
                .next()
                .map(|n| source(session, &[Some(n)], &[Some("value")]))
                .transpose()
        };
        let error = prepared
            .with_batch_input(&mut input)
            .unwrap()
            .collect_jsonl(&CancellationToken::default())
            .err()
            .unwrap();
        assert!(error.to_string().contains("overflow"), "{error}");
        assert_eq!(prepared.snapshot().completed_executions, 0);
        assert_eq!(prepared.snapshot().memory.reserved_bytes, baseline);
    }
}

#[test]
fn streamed_ordering_column_projection_prefix_preserves_complete_observations() {
    let rows = fixture_rows();
    let keys = vec![key("n", false, NullOrder::First)];
    let mut expected = rows.clone();
    reference_sort(&mut expected, &keys);
    for count in [0, 7] {
        let observed = VortexRelationalPlan::Pipeline(Box::new(VortexRelationalPipeline {
            input: sorted(scan(), keys.clone()),
            stages: vec![VortexRelationalStage::Observe(
                crate::relational_query::VortexRelationalObservation {
                    name: "complete_sorted_rows".into(),
                    projection: vec![],
                    measures: vec![crate::query_primitive::VortexSimpleAggregateMeasure::new(
                        "count",
                        None,
                        "rows".into(),
                    )],
                },
            )],
        }));
        let prepared =
            prepare_rows(&limited(projected(observed, true), 3, count).compact_streaming());
        let baseline = prepared.snapshot().memory.reserved_bytes;
        let mut chunks = rows.chunks(17);
        let mut input = |session: &ResidentVortexSession| {
            chunks
                .next()
                .map(|rows| rows_source(session, rows))
                .transpose()
        };
        let result = prepared
            .with_batch_input(&mut input)
            .unwrap()
            .collect_jsonl(&CancellationToken::default())
            .unwrap();
        assert_eq!(
            json_rows(&result),
            expected
                .iter()
                .skip(3)
                .take(count)
                .map(projected_json)
                .collect::<Vec<_>>()
        );
        assert_eq!(result.execution.observations.len(), 1);
        let observed = serde_json::from_str::<serde_json::Value>(
            result.execution.observations[0].result_jsonl.value(),
        )
        .unwrap();
        assert_eq!(observed, serde_json::json!({"rows": rows.len()}));
        assert_eq!(
            result
                .execution
                .input
                .as_ref()
                .unwrap()
                .ordering_prefix_batches,
            0
        );
        drop(result);
        assert_eq!(prepared.snapshot().memory.reserved_bytes, baseline);
    }
}
