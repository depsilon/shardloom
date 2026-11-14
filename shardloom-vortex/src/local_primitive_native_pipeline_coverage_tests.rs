use super::*;
use crate::relational_query::{
    VortexRelationalFilter, VortexRelationalLimit, VortexRelationalObservation,
    VortexRelationalPipeline, VortexRelationalProject, VortexRelationalStage as Stage,
};
use shardloom_core::{ComparisonOp, ExprId, Expression, ExpressionKind, ScalarValue};

#[path = "local_primitive_native_file_scan_evidence_tests.rs"]
mod file_scan_evidence_tests;

fn column(name: &str) -> Expression {
    Expression::column(ExprId::new(name).unwrap(), ColumnRef::new(name).unwrap())
}

fn greater(name: &str, value: i64) -> Expression {
    Expression::new(
        ExprId::new("greater").unwrap(),
        ExpressionKind::Compare {
            left: Box::new(column(name)),
            op: ComparisonOp::Gt,
            right: Box::new(Expression::literal(
                ExprId::new("threshold").unwrap(),
                ScalarValue::Int64(value),
            )),
        },
    )
}

fn transforms(input: VortexRelationalPlan, stages: Vec<Stage>, flat: bool) -> VortexRelationalPlan {
    if flat {
        return VortexRelationalPlan::Pipeline(Box::new(VortexRelationalPipeline {
            input,
            stages,
        }));
    }
    stages.into_iter().fold(input, |input, stage| match stage {
        Stage::Project(expressions) => {
            VortexRelationalPlan::Project(Box::new(VortexRelationalProject { input, expressions }))
        }
        Stage::Filter(predicate) => {
            VortexRelationalPlan::Filter(Box::new(VortexRelationalFilter { input, predicate }))
        }
        Stage::Limit { offset, count } => {
            VortexRelationalPlan::Limit(Box::new(VortexRelationalLimit {
                input,
                offset,
                count,
            }))
        }
        _ => panic!("fixture uses only equivalent pure transforms"),
    })
}

#[test]
fn native_pipeline_flat_and_nested_filters_use_the_same_count_kernel() {
    for keys in [
        vec![],
        vec![None, None],
        vec![Some(2), None, Some(1), Some(3)],
    ] {
        let amounts = vec![10; keys.len()];
        let fixture = Fixture::new(keyed(&keys, &amounts), 1);
        let expected = keys.iter().flatten().filter(|key| **key > 1).count();
        for flat in [false, true] {
            let input = transforms(
                fixture.scan(),
                vec![
                    Stage::Project(vec![("key".into(), column("entity"))]),
                    Stage::Filter(greater("key", 1)),
                ],
                flat,
            );
            let prepared = prepare_relational(&count(input), policy()).unwrap();
            assert!(prepared.root_has_file_kernel());
            let initial = prepared.snapshot().reserved_without_source_segment_cache();
            let result = prepared
                .collect_jsonl(&CancellationToken::default())
                .unwrap();
            assert_eq!(json_rows(&result), [json!({"rows":expected})]);
            pipeline(&result.execution, 1);
            drop(result);
            assert_eq!(
                prepared.snapshot().reserved_without_source_segment_cache(),
                initial
            );
        }
    }
}

#[test]
fn native_pipeline_empty_count_requires_authoritative_completed_scan_evidence() {
    use crate::local_primitives::{
        local_primitive_native_io_certificate, prepared_count::prepare_count_where_in_session,
    };
    use crate::query_primitive::VortexQueryPrimitiveRequest;
    use crate::resident_session::ResidentVortexSession;
    use shardloom_core::{DatasetUri, PredicateExpr, StatValue};

    let fixture = Fixture::new(keyed(&[], &[]), 1);
    let request = VortexQueryPrimitiveRequest::count_where(
        DatasetUri::new(fixture.path().display().to_string()).unwrap(),
        PredicateExpr::Compare {
            column: ColumnRef::new("entity").unwrap(),
            op: ComparisonOp::Gt,
            value: StatValue::UInt64(1),
        },
    );
    let session = ResidentVortexSession::new(32 << 20, 1).unwrap();
    let result = prepare_count_where_in_session(&request, policy(), &session)
        .unwrap()
        .execute()
        .unwrap();
    assert_eq!(result.count, 0);
    assert_eq!(result.report.rows_scanned, 0);
    assert_eq!(result.report.arrays_read_count, 0);
    assert!(!result.report.data_read);
    assert!(result.native_io_certificate.is_certified());

    for mutation in 0..6 {
        let mut report = result.report.clone();
        match mutation {
            0 => report.embedded_layout.footer_row_count = 1,
            1 => report.upstream_scan_called = false,
            2 => report.rows_selected = Some(1),
            3 => report.embedded_layout.metadata_persisted_in_artifact = false,
            4 => report.filter_pushdown_applied = false,
            5 => report.streaming_scan_used = false,
            _ => unreachable!(),
        }
        assert!(
            !local_primitive_native_io_certificate(&request, &report)
                .unwrap()
                .is_certified(),
            "empty count accepted invalid evidence mutation {mutation}"
        );
    }
}

#[test]
fn native_pipeline_flat_projection_filter_and_limit_admit_one_file_kernel() {
    let fixture = Fixture::new(
        keyed(&[Some(2), None, Some(1), Some(3)], &[10, 20, 30, 40]),
        1,
    );
    for flat in [false, true] {
        let plan = transforms(
            fixture.scan(),
            vec![
                Stage::Project(vec![("key".into(), column("entity"))]),
                Stage::Filter(greater("key", 1)),
                Stage::Limit {
                    offset: 0,
                    count: 1,
                },
            ],
            flat,
        );
        let prepared = prepare_relational(&plan, policy()).unwrap();
        assert!(prepared.root_has_file_kernel());
        let result = prepared
            .collect_jsonl(&CancellationToken::default())
            .unwrap();
        assert_eq!(json_rows(&result), [json!({"key":2})]);
        pipeline(&result.execution, 1);
    }
}

#[test]
fn native_pipeline_long_flat_alias_chain_is_lowered_without_rebuilding_a_tree() {
    let fixture = Fixture::new(keyed(&[Some(2), None, Some(1)], &[10, 20, 30]), 1);
    let mut name = "entity";
    let stages = (0..1024)
        .map(|_| {
            let alias = if name == "entity" { "key" } else { "entity" };
            let stage = Stage::Project(vec![(alias.into(), column(name))]);
            name = alias;
            stage
        })
        .collect();
    let plan = transforms(fixture.scan(), stages, true);
    let prepared = prepare_relational(&plan, policy()).unwrap();
    assert!(prepared.root_has_file_kernel());
    let initial = prepared.snapshot().reserved_without_source_segment_cache();
    let result = prepared
        .collect_jsonl(&CancellationToken::default())
        .unwrap();
    assert_eq!(
        json_rows(&result),
        [
            json!({"entity":2}),
            json!({"entity":null}),
            json!({"entity":1})
        ]
    );
    pipeline(&result.execution, 1);
    drop(result);
    assert_eq!(
        prepared.snapshot().reserved_without_source_segment_cache(),
        initial
    );
}

#[test]
fn native_pipeline_flat_kernel_cancellation_and_generation_errors_are_terminal() {
    let fixture = Fixture::new(keyed(&[Some(2), None, Some(1)], &[10, 20, 30]), 1);
    let plan = transforms(
        fixture.scan(),
        vec![Stage::Project(vec![("key".into(), column("entity"))])],
        true,
    );
    let prepared = prepare_relational(&plan, policy()).unwrap();
    assert!(prepared.root_has_file_kernel());
    let initial = prepared.snapshot().reserved_without_source_segment_cache();
    let token = CancellationToken::default();
    assert!(
        prepared
            .for_each_batch(&token, |_, _| {
                token.cancel();
                Ok(())
            })
            .is_err()
    );
    assert_eq!(
        prepared.snapshot().reserved_without_source_segment_cache(),
        initial
    );
    assert_eq!(prepared.snapshot().completed_executions, 0);
    assert!(
        prepared
            .for_each_batch(&CancellationToken::default(), |_, _| {
                fixture.replace();
                Ok(())
            })
            .is_err()
    );
    assert_eq!(
        prepared.snapshot().reserved_without_source_segment_cache(),
        initial
    );
    assert_eq!(prepared.snapshot().completed_executions, 0);
}

#[test]
fn native_pipeline_flat_observation_remains_a_completion_barrier() {
    let fixture = Fixture::new(
        keyed(&[Some(2), None, Some(1), Some(3)], &[10, 20, 30, 40]),
        1,
    );
    let plan = VortexRelationalPlan::Pipeline(Box::new(VortexRelationalPipeline {
        input: fixture.scan(),
        stages: vec![
            Stage::Observe(VortexRelationalObservation {
                name: "complete".into(),
                projection: vec![],
                measures: vec![VortexSimpleAggregateMeasure::new(
                    "count",
                    None,
                    "rows".into(),
                )],
            }),
            Stage::Limit {
                offset: 0,
                count: 1,
            },
        ],
    }));
    let prepared = prepare_relational(&plan, policy()).unwrap();
    assert!(!prepared.root_has_file_kernel());
    let result = prepared
        .collect_jsonl(&CancellationToken::default())
        .unwrap();
    assert_eq!(json_rows(&result), [json!({"entity":2,"amount":10})]);
    assert_eq!(result.execution.observations.len(), 1);
    let observed: serde_json::Value =
        serde_json::from_str(result.execution.observations[0].result_jsonl.value()).unwrap();
    assert_eq!(observed, json!({"rows":4}));
}
