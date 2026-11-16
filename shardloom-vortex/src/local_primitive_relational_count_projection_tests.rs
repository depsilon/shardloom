//! Total group ordering remains valid through bound column-only projections.

use super::*;
use crate::relational_query::{
    VortexRelationalLimit, VortexRelationalPipeline, VortexRelationalProject, VortexRelationalStage,
};
use shardloom_core::{BinaryOp, ExprId, Expression, ExpressionKind, ScalarValue};

fn col(name: &str) -> Expression {
    Expression::new(
        ExprId::new(name).unwrap(),
        ExpressionKind::Column(ColumnRef::new(name).unwrap()),
    )
}

fn lit(value: ScalarValue) -> Expression {
    Expression::new(
        ExprId::new("literal").unwrap(),
        ExpressionKind::Literal(value),
    )
}

fn binary(left: Expression, op: BinaryOp, right: Expression) -> Expression {
    Expression::new(
        ExprId::new("computed").unwrap(),
        ExpressionKind::Binary {
            left: Box::new(left),
            op,
            right: Box::new(right),
        },
    )
}

fn projected(compact: bool, mode: &str, descending: bool) -> PreparedVortexRelational {
    let uri = DatasetUri::new("memory://count-projection").unwrap();
    let input = aggregate(
        VortexRelationalPlan::Scan(VortexRelationalScan {
            source_uri: uri.clone(),
            projection: shardloom_plan::ProjectionRequest::All,
            predicate: None,
        }),
        &["g"],
        vec![measure("count", Some("v"), "n")],
    );
    let mut first = vec![
        ("total".into(), col("n")),
        ("group".into(), col("g")),
        ("duplicate".into(), col("g")),
    ];
    if mode == "computed" {
        first[0].1 = binary(col("n"), BinaryOp::Add, lit(ScalarValue::UInt64(0)));
    }
    if mode == "error" {
        first[0].1 = binary(col("n"), BinaryOp::Divide, lit(ScalarValue::UInt64(0)));
    }
    let second = vec![
        ("key".into(), col("duplicate")),
        ("total".into(), col("total")),
        ("copy".into(), col("group")),
    ];
    let mut stages = vec![VortexRelationalStage::Project(first)];
    match mode {
        "filter" => stages.push(VortexRelationalStage::Filter(lit(ScalarValue::Boolean(
            true,
        )))),
        "range" => stages.push(VortexRelationalStage::Limit {
            offset: 0,
            count: 3,
        }),
        _ => {}
    }
    stages.push(VortexRelationalStage::Project(second));
    let input = if compact {
        VortexRelationalPlan::Pipeline(Box::new(VortexRelationalPipeline { input, stages }))
    } else {
        stages.into_iter().fold(input, |input, stage| match stage {
            VortexRelationalStage::Project(expressions) => {
                VortexRelationalPlan::Project(Box::new(VortexRelationalProject {
                    input,
                    expressions,
                }))
            }
            VortexRelationalStage::Filter(predicate) => {
                VortexRelationalPlan::Filter(Box::new(VortexRelationalFilter { input, predicate }))
            }
            VortexRelationalStage::Limit { offset, count } => {
                VortexRelationalPlan::Limit(Box::new(VortexRelationalLimit {
                    input,
                    offset,
                    count,
                }))
            }
            _ => unreachable!(),
        })
    };
    let key = |name: &str| VortexRelationalOrderKey {
        column: ColumnRef::new(name).unwrap(),
        descending,
        nulls: Some(VortexRelationalNullOrder::Last),
    };
    let keys = match mode {
        "count-only" => vec![key("total")],
        "secondary" => vec![key("total"), key("key")],
        _ => vec![key("key")],
    };
    let plan = VortexRelationalPlan::Sort(Box::new(VortexRelationalSort { input, keys }));
    prepare_relational_with_schema(policy(), |preparation| {
        preparation.register_batch_source(uri, |session| source(session, 0, false))?;
        Ok(plan)
    })
    .unwrap()
}

fn execute(prepared: &PreparedVortexRelational) -> CollectedVortexRelational {
    let mut calls = 0;
    let mut producer = |session: &ResidentVortexSession| {
        calls += 1;
        match calls {
            1 | 2 => source(session, 2048, false).map(Some),
            3 => Ok(None),
            _ => panic!("projection COUNT replay"),
        }
    };
    let result = prepared
        .with_batch_input(&mut producer)
        .unwrap()
        .collect_jsonl(&CancellationToken::default())
        .unwrap();
    assert_eq!(calls, 3);
    let report = result.execution.input.as_ref().unwrap();
    assert_eq!(report.rows, 4096);
    assert!(report.end_of_input_observed);
    assert!(result.execution.native_io_certificate.is_certified());
    assert!(
        !result
            .execution
            .native_io_certificate
            .side_effects
            .fallback_attempted
    );
    result
}

#[test]
fn count_partial_crosses_only_column_projects_and_keeps_complete_group_order() {
    for compact in [false, true] {
        for mode in [
            "alias",
            "secondary",
            "count-only",
            "computed",
            "filter",
            "range",
        ] {
            for descending in [false, true] {
                let prepared = projected(compact, mode, descending);
                let memory = prepared.session.memory().clone();
                let initial = memory.snapshot().reserved_bytes;
                let result = execute(&prepared);
                let mut expected = vec![
                    json!({"key":3,"total":1366,"copy":3}),
                    json!({"key":1,"total":1366,"copy":1}),
                    json!({"key":-1,"total":1364,"copy":-1}),
                ];
                expected.sort_by(|left, right| {
                    let primary = if matches!(mode, "count-only" | "secondary") {
                        "total"
                    } else {
                        "key"
                    };
                    let order = left[primary].as_i64().cmp(&right[primary].as_i64());
                    let order = if mode == "secondary" {
                        order.then(left["key"].as_i64().cmp(&right["key"].as_i64()))
                    } else {
                        order
                    };
                    if descending { order.reverse() } else { order }
                });
                assert_eq!(
                    json_rows(&result),
                    expected,
                    "{compact}/{mode}/{descending}"
                );
                let work = result.execution.native_io_certificate.count_partial_work;
                let admitted = matches!(mode, "alias" | "secondary");
                assert_eq!(
                    work.admitted_batches,
                    if admitted { 2 } else { 0 },
                    "{compact}/{mode}"
                );
                assert_eq!(work.input_rows, if admitted { 4096 } else { 0 });
                assert_eq!(work.reduced_groups, if admitted { 6 } else { 0 });
                drop(result);
                assert_eq!(memory.snapshot().reserved_bytes, initial);
                drop(prepared);
                assert_eq!(memory.snapshot().reserved_bytes, 0);
            }
        }
    }
}

#[test]
fn count_partial_projected_spill_and_late_failure_keep_existing_owners() {
    let fixture = Fixture::new(keyed(&[], &[]), 1);
    let workspace = fixture.0.join("projected-count-spill");
    fs::create_dir(&workspace).unwrap();
    let prepared = projected(true, "alias", false)
        .with_spill(VortexRelationalSpillPolicy::new(workspace.clone(), 64 << 20, 1 << 20).unwrap())
        .unwrap();
    let result = execute(&prepared);
    assert_eq!(
        json_rows(&result),
        [
            json!({"key":-1,"total":1364,"copy":-1}),
            json!({"key":1,"total":1366,"copy":1}),
            json!({"key":3,"total":1366,"copy":3}),
        ]
    );
    assert_eq!(
        result
            .execution
            .native_io_certificate
            .count_partial_work
            .admitted_batches,
        0
    );
    assert_eq!(result.execution.ordered_aggregate_input_rows, 4096);
    assert!(
        result
            .execution
            .spill
            .as_ref()
            .unwrap()
            .owned_cleanup_completed
    );
    assert_eq!(fs::read_dir(&workspace).unwrap().count(), 0);
    drop((result, prepared));
    let prepared = projected(true, "alias", false);
    let memory = prepared.session.memory().clone();
    let initial = memory.snapshot().reserved_bytes;
    let mut calls = 0;
    let mut producer = |session: &ResidentVortexSession| {
        calls += 1;
        if calls == 1 {
            source(session, 2048, false).map(Some)
        } else {
            Err(shardloom_core::ShardLoomError::InvalidOperation(
                "late projected COUNT input".into(),
            ))
        }
    };
    let error = prepared
        .with_batch_input(&mut producer)
        .unwrap()
        .collect_jsonl(&CancellationToken::default())
        .err()
        .unwrap();
    assert!(error.to_string().contains("late projected COUNT input"));
    assert_eq!(calls, 2);
    assert_eq!(memory.snapshot().reserved_bytes, initial);
    drop(prepared);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

#[test]
fn count_partial_projection_does_not_hide_computed_errors() {
    for compact in [false, true] {
        let prepared = projected(compact, "error", false);
        let memory = prepared.session.memory().clone();
        let initial = memory.snapshot().reserved_bytes;
        let mut calls = 0;
        let mut producer = |session: &ResidentVortexSession| {
            calls += 1;
            match calls {
                1 => source(session, 2048, false).map(Some),
                2 => Ok(None),
                _ => panic!("computed failure replay"),
            }
        };
        let error = prepared
            .with_batch_input(&mut producer)
            .unwrap()
            .collect_jsonl(&CancellationToken::default())
            .err()
            .unwrap();
        assert!(error.to_string().contains("division by zero"), "{error}");
        assert_eq!(calls, 2);
        assert_eq!(memory.snapshot().reserved_bytes, initial);
        drop(prepared);
        assert_eq!(memory.snapshot().reserved_bytes, 0);
    }
}
