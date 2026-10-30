//! Source admission, completion and failure controls for parent join demand.

use super::*;
use crate::relational_query::{
    VortexRelationalAsOfJoin, VortexRelationalAsOfOptions, VortexRelationalFrameFunction,
    VortexRelationalWindow, VortexRelationalWindowExpression, VortexRelationalWindowFunction,
};

#[test]
fn parquet_join_demand_window_row_population_preserves_source_scope() {
    let left = ParquetFixture::new();
    let right = ParquetFixture::new();
    right.corrupt_chunk(3, 2);
    for project_input in [false, true] {
        let VortexRelationalPlan::Join(mut join) = joined(&left, &right, JoinKind::Cross) else {
            unreachable!()
        };
        join.right = VortexRelationalPlan::Window(Box::new(VortexRelationalWindow {
            input: if project_input {
                project(
                    right.scan(&[], None),
                    &[("text", "text"), ("n", "n"), ("other", "other")],
                )
            } else {
                right.scan(&[], None)
            },
            columns: ["n", "other", "text"]
                .map(|name| ColumnRef::new(name).unwrap())
                .to_vec(),
            expressions: vec![VortexRelationalWindowExpression {
                output_column: "ordinal".into(),
                function: VortexRelationalWindowFunction::Framed(
                    VortexRelationalFrameFunction::Aggregate(Box::new(
                        VortexSimpleAggregateMeasure::new("count", None, "ordinal".into()),
                    )),
                ),
                partition_by: vec![],
                order_by: vec![],
                frame: None,
            }],
        }));
        let prepared = prepare(
            &[&left, &right],
            &aggregate(VortexRelationalPlan::Join(join), false),
        );
        let initial = prepared.snapshot().memory.reserved_bytes;
        let result = prepared
            .collect_jsonl(&CancellationToken::default())
            .unwrap();
        assert_eq!(json_rows(&result), vec![json!({"rows":256})]);
        let input = result.execution.input.as_ref().unwrap();
        assert_eq!(input.columnar_projected_columns, 2);
        assert!(input.end_of_input_observed && input.output_ownership_detached);
        drop(result);
        assert_eq!(prepared.snapshot().memory.reserved_bytes, initial);
    }
}

#[test]
fn parquet_join_demand_checks_each_source_generation_after_final_consumer() {
    for replace_right in [false, true] {
        let left = ParquetFixture::new();
        let right = ParquetFixture::new();
        let plan = project(
            joined(&left, &right, JoinKind::Inner),
            &[("left_other", "value")],
        );
        let prepared = prepare(&[&left, &right], &plan);
        let initial = prepared.snapshot().memory.reserved_bytes;
        let fixture = if replace_right { &right } else { &left };
        let mut replaced = false;
        let result = prepared.for_each_batch(&CancellationToken::default(), |_, _| {
            if !replaced {
                let replacement = fixture.0.join("replacement.parquet");
                fs::copy(fixture.path(), &replacement).unwrap();
                fs::rename(replacement, fixture.path()).unwrap();
                replaced = true;
            }
            Ok(())
        });
        assert!(replaced && result.is_err());
        assert_eq!(prepared.snapshot().memory.reserved_bytes, initial);
        assert_eq!(prepared.snapshot().completed_executions, 0);
        assert!(
            prepared
                .collect_jsonl(&CancellationToken::default())
                .is_err()
        );
        let result = prepare(&[&left, &right], &plan)
            .collect_jsonl(&CancellationToken::default())
            .unwrap();
        assert_eq!(
            json_rows(&result),
            (100..116)
                .map(|value| json!({"value":value}))
                .collect::<Vec<_>>()
        );
    }
}

#[test]
fn parquet_join_demand_keeps_computed_projection_first_error_order() {
    for overflow_first in [false, true] {
        let left = ParquetFixture::new();
        let right = ParquetFixture::new();
        let computed = |op, value| {
            Expression::new(
                ExprId::new("failure").unwrap(),
                ExpressionKind::Binary {
                    left: Box::new(column("n")),
                    op,
                    right: Box::new(Expression::literal(
                        ExprId::new("constant").unwrap(),
                        ScalarValue::Int64(value),
                    )),
                },
            )
        };
        let mut failures = vec![
            (
                "division".into(),
                computed(shardloom_core::BinaryOp::Divide, 0),
            ),
            (
                "overflow".into(),
                computed(shardloom_core::BinaryOp::Add, i64::MAX),
            ),
        ];
        if overflow_first {
            failures.reverse();
        }
        let VortexRelationalPlan::Join(mut join) = joined(&left, &right, JoinKind::Inner) else {
            unreachable!()
        };
        let mut expressions = ["n", "other", "text"]
            .map(|name| (name.into(), column(name)))
            .to_vec();
        expressions.extend(failures);
        join.right = VortexRelationalPlan::Project(Box::new(VortexRelationalProject {
            input: right.scan(&[], None),
            expressions,
        }));
        let prepared = prepare(
            &[&left, &right],
            &aggregate(VortexRelationalPlan::Join(join), false),
        );
        let initial = prepared.snapshot().memory.reserved_bytes;
        let error = prepared
            .collect_jsonl(&CancellationToken::default())
            .err()
            .unwrap()
            .to_string();
        let expected = if overflow_first {
            "overflow"
        } else {
            "division by zero"
        };
        assert!(error.contains(expected), "{error}");
        assert_eq!(prepared.snapshot().completed_executions, 0);
        assert_eq!(prepared.snapshot().memory.reserved_bytes, initial);
    }
}

#[test]
fn parquet_join_demand_keeps_unrelated_count_projection_validation_scope() {
    let fixture = ParquetFixture::new();
    fixture.corrupt_chunk(3, 2);
    let plan = aggregate(
        project(fixture.scan(&[], None), &[("text", "text"), ("n", "n")]),
        false,
    );
    let prepared = fixture.prepare(&plan);
    let result = prepared
        .collect_jsonl(&CancellationToken::default())
        .unwrap();
    assert_eq!(json_rows(&result), vec![json!({"rows":16})]);
    assert_eq!(
        result
            .execution
            .input
            .as_ref()
            .unwrap()
            .columnar_projected_columns,
        1
    );
}

#[test]
fn parquet_join_demand_row_field_requirement_stops_at_computed_projection() {
    let left = ParquetFixture::new();
    let right = ParquetFixture::new();
    right.corrupt_chunk(3, 2);
    for kind in [JoinKind::Inner, JoinKind::Cross] {
        let VortexRelationalPlan::Join(mut join) = joined(&left, &right, kind) else {
            unreachable!()
        };
        join.right = VortexRelationalPlan::Project(Box::new(VortexRelationalProject {
            input: project(right.scan(&[], None), &[("text", "text"), ("n", "n")]),
            expressions: [("n", 0), ("other", 100), ("text", 0)]
                .into_iter()
                .map(|(name, value)| {
                    (
                        name.to_owned(),
                        Expression::literal(ExprId::new(name).unwrap(), ScalarValue::Int64(value)),
                    )
                })
                .collect(),
        }));
        let prepared = prepare(
            &[&left, &right],
            &aggregate(VortexRelationalPlan::Join(join), false),
        );
        let result = prepared
            .collect_jsonl(&CancellationToken::default())
            .unwrap();
        assert_eq!(
            json_rows(&result),
            vec![json!({"rows":if kind == JoinKind::Cross {256} else {16}})]
        );
        assert_eq!(
            result
                .execution
                .input
                .as_ref()
                .unwrap()
                .columnar_projected_columns,
            2
        );
    }
}

#[test]
fn parquet_join_asof_preserves_ordering_dependencies_with_one_requested_payload() {
    let left = ParquetFixture::new();
    let right = ParquetFixture::new();
    let VortexRelationalPlan::Join(join) = joined(&left, &right, JoinKind::Left) else {
        unreachable!()
    };
    let asof = VortexRelationalPlan::AsOfJoin(Box::new(VortexRelationalAsOfJoin {
        left: join.left,
        right: join.right,
        by: vec![],
        on: join.keys[0].clone(),
        options: VortexRelationalAsOfOptions::default(),
        columns: join.columns,
    }));
    let plan = project(asof, &[("right_other", "value")]);
    let prepared = prepare(&[&left, &right], &plan);
    let result = prepared
        .collect_jsonl(&CancellationToken::default())
        .unwrap();
    assert_eq!(
        json_rows(&result),
        (100..116)
            .map(|value| json!({"value":value}))
            .collect::<Vec<_>>()
    );
    assert_eq!(
        result
            .execution
            .input
            .as_ref()
            .unwrap()
            .columnar_projected_columns,
        3
    );
}

#[test]
fn parquet_join_cancellation_consumer_failure_pressure_and_fresh_retry_release_owners() {
    let left = ParquetFixture::new();
    let right = ParquetFixture::new();
    let plan = project(
        joined(&left, &right, JoinKind::Left),
        &[("right_text", "value")],
    );
    let prepared = prepare(&[&left, &right], &plan);
    let initial = prepared.snapshot().memory.reserved_bytes;
    let cancellation = CancellationToken::default();
    let error = prepared
        .for_each_batch(&cancellation, |_, _| {
            cancellation.cancel();
            Ok(())
        })
        .err()
        .unwrap();
    assert!(error.to_string().contains("cancelled"));
    assert_eq!(prepared.snapshot().memory.reserved_bytes, initial);
    let error = prepared
        .for_each_batch(&CancellationToken::default(), |_, _| {
            Err(failed("join demand consumer sentinel"))
        })
        .err()
        .unwrap();
    assert!(error.to_string().contains("join demand consumer sentinel"));
    assert_eq!(prepared.snapshot().memory.reserved_bytes, initial);
    let pressure = prepared
        .session
        .memory()
        .reserve(prepared.snapshot().memory.limit_bytes - initial - 4096)
        .unwrap();
    assert!(
        prepared
            .collect_jsonl(&CancellationToken::default())
            .is_err()
    );
    drop(pressure);
    assert_eq!(prepared.snapshot().memory.reserved_bytes, initial);
    assert_eq!(prepared.snapshot().completed_executions, 0);
    let result = prepared
        .collect_jsonl(&CancellationToken::default())
        .unwrap();
    assert_eq!(json_rows(&result).len(), 16);
    drop(result);
    assert_eq!(prepared.snapshot().memory.reserved_bytes, initial);
}

#[test]
fn parquet_join_demand_preserves_prepared_vortex_payload_validation_barrier() {
    let left = ParquetFixture::new();
    let right = Fixture::new(
        StructArray::new(
            FieldNames::from(["n", "other", "text"]),
            vec![
                PrimitiveArray::from_iter([0_i64, 1]).into_array(),
                PrimitiveArray::from_iter([100_i64, 101]).into_array(),
                PrimitiveArray::from_iter([1.0_f64, f64::NAN]).into_array(),
            ],
            2,
            Validity::NonNullable,
        )
        .into_array(),
        1,
    );
    let VortexRelationalPlan::Join(mut join) = joined(&left, &left, JoinKind::Inner) else {
        unreachable!()
    };
    join.right = right.scan();
    let plan = aggregate(VortexRelationalPlan::Join(join), false);
    let prepared = prepare(&[&left], &plan);
    let initial = prepared.snapshot().memory.reserved_bytes;
    let error = prepared
        .collect_jsonl(&CancellationToken::default())
        .err()
        .unwrap();
    assert!(
        error.to_string().contains("finite declared dtype"),
        "{error}"
    );
    assert_eq!(prepared.snapshot().completed_executions, 0);
    assert_eq!(prepared.snapshot().memory.reserved_bytes, initial);
}

#[test]
fn parquet_join_demand_keeps_caller_owned_arrow_outputs_as_a_barrier() {
    let left = ParquetFixture::new();
    let uri = DatasetUri::new("memory://caller-arrow").unwrap();
    let VortexRelationalPlan::Join(mut join) = joined(&left, &left, JoinKind::Inner) else {
        unreachable!()
    };
    join.right = VortexRelationalPlan::Scan(VortexRelationalScan {
        source_uri: uri.clone(),
        projection: ProjectionRequest::All,
        predicate: None,
    });
    let plan = aggregate(VortexRelationalPlan::Join(join), false);
    let prepared = prepare_relational_with_schema(policy(), |preparation| {
        let source = LocalColumnarSource::admit(
            &left.path(),
            "parquet",
            &preparation.shared_memory_pool(),
            &preparation.cancellation_token(),
        )?;
        preparation
            .register_local_source(DatasetUri::new(left.path().display().to_string())?, source)?;
        preparation.register_memory_source(uri.clone(), |session| {
            ResidentMemorySource::from_arrow_record_batch(
                session,
                ParquetFixture::batch(),
                &CancellationToken::default(),
            )
        })?;
        Ok(plan.clone())
    })
    .unwrap();
    #[cfg(debug_assertions)]
    let observation = Observation::start();
    let result = prepared
        .collect_jsonl(&CancellationToken::default())
        .unwrap();
    assert_eq!(json_rows(&result), vec![json!({"rows":16})]);
    #[cfg(debug_assertions)]
    {
        let counts = observation.finish();
        assert_eq!(
            (counts.join_output_rows, counts.join_output_cells),
            (16, 96)
        );
    }
    assert_eq!(
        result
            .execution
            .input
            .as_ref()
            .unwrap()
            .columnar_projected_columns,
        3
    );
}
