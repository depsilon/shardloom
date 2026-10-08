use super::*;
use crate::{
    relational_query::{VortexRelationalFilter, VortexRelationalProject},
    resident_memory_source::{MemoryColumn, MemoryColumnValues, MemorySourceBounds},
};
use shardloom_core::{ExprId, Expression, ExpressionKind};
use std::sync::Weak;

#[path = "local_primitive_relational_batch_order_tests.rs"]
mod order_tests;

#[path = "local_primitive_relational_batch_aggregate_tests.rs"]
mod aggregate_tests;

#[path = "local_primitive_relational_batch_join_tests.rs"]
mod join_tests;

fn scan() -> VortexRelationalPlan {
    VortexRelationalPlan::Scan(VortexRelationalScan {
        source_uri: DatasetUri::new("memory://stream").unwrap(),
        projection: shardloom_plan::ProjectionRequest::All,
        predicate: None,
    })
}

fn col(name: &str) -> Expression {
    Expression::new(
        ExprId::new(name).unwrap(),
        ExpressionKind::Column(ColumnRef::new(name).unwrap()),
    )
}

fn source(
    session: &ResidentVortexSession,
    values: &[Option<i64>],
    text: &[Option<&str>],
) -> Result<ResidentMemorySource> {
    ResidentMemorySource::from_batch_columns(
        session,
        &[
            MemoryColumn {
                name: "n",
                values: MemoryColumnValues::Int64(values),
            },
            MemoryColumn {
                name: "s",
                values: MemoryColumnValues::Utf8(text),
            },
        ],
    )
}

fn prepare(plan: &VortexRelationalPlan, bytes: u64) -> Result<PreparedVortexRelational> {
    let mut policy = policy();
    policy.resource_envelope.memory_budget_bytes = bytes;
    prepare_relational_with_schema(policy, |preparation| {
        preparation.register_batch_source(DatasetUri::new("memory://stream")?, |session| {
            source(session, &[], &[])
        })?;
        Ok(plan.clone())
    })
}

fn values(
    array: &ArrayRef,
    context: &NativeExecutionContext<'_>,
) -> Result<Vec<serde_json::Value>> {
    let mut sink =
        crate::local_primitives::collect::JsonRows::new(context.memory(), 8 << 20, false)?;
    sink.append_native(array, context)?;
    Ok(serde_json::from_str(sink.finish()?.value()).unwrap())
}

#[test]
fn completion_input_exact_values_schema_empty_batches_and_owned_output() {
    let plan = VortexRelationalPlan::Project(Box::new(VortexRelationalProject {
        input: scan(),
        expressions: vec![("value".into(), col("n")), ("text".into(), col("s"))],
    }));
    let prepared = prepare(&plan, 4 << 20).unwrap();
    let initial = prepared.snapshot().memory.reserved_bytes;
    let mut next = 0;
    let mut prior: Option<Weak<MemoryLease>> = None;
    let mut input = |session: &ResidentVortexSession| {
        assert!(
            prior
                .as_ref()
                .is_none_or(|witness| witness.strong_count() == 0)
        );
        let batch = match next {
            0 | 2 => source(session, &[], &[])?,
            1 => source(session, &[Some(i64::MIN), None], &[Some("λ\"\n"), None])?,
            3 => source(
                session,
                &[Some(i64::MAX), Some(0)],
                &[Some(""), Some("東京")],
            )?,
            4 => return Ok(None),
            _ => unreachable!(),
        };
        next += 1;
        prior = Some(batch.batch_release_witness()?);
        Ok(Some(batch))
    };
    let mut retained = Vec::new();
    let mut values = Vec::new();
    let result = prepared
        .with_batch_input(&mut input)
        .unwrap()
        .for_each_batch(&CancellationToken::default(), |array, context| {
            let mut sink =
                super::super::super::collect::JsonRows::new(context.memory(), 1 << 20, false)?;
            sink.append_native(&array, context)?;
            values.extend(
                serde_json::from_str::<Vec<serde_json::Value>>(sink.finish()?.value()).unwrap(),
            );
            retained.push(array.clone());
            Ok(())
        })
        .unwrap();
    assert_eq!(next, 4);
    assert_eq!(
        values,
        serde_json::json!([
            {"value":i64::MIN,"text":"λ\"\n"}, {"value":null,"text":null},
            {"value":i64::MAX,"text":""}, {"value":0,"text":"東京"},
        ])
        .as_array()
        .unwrap()
        .clone()
    );
    let input_report = result.input.as_ref().unwrap();
    assert_eq!(input_report.payload_batches, 4);
    assert_eq!(input_report.rows, 4);
    assert_eq!(input_report.max_retained_input_batches, 1);
    assert!(input_report.end_of_input_observed && input_report.output_ownership_detached);
    assert_eq!(result.runtime.completed_executions, 1);
    assert_eq!(result.prepared_sources, 1);
    assert!(result.native_io_certificate.is_certified());
    assert!(!result.native_io_certificate.side_effects.fallback_attempted);
    assert_eq!(retained[0].dtype(), &prepared.output_dtype().unwrap());
    drop(result);
    assert!(prepared.snapshot().memory.reserved_bytes > initial);
    drop(retained);
    assert_eq!(prepared.snapshot().memory.reserved_bytes, initial);
}

#[test]
fn completion_input_cumulative_payload_exceeds_grant_without_retention() {
    let prepared = prepare(&scan(), 2 << 20).unwrap();
    let initial = prepared.snapshot().memory.reserved_bytes;
    let text = "λ".repeat(128);
    let mut next = 0_i64;
    let mut input = |session: &ResidentVortexSession| {
        if next == 41 {
            return Ok(None);
        }
        let values = (0..1024)
            .map(|row| Some(next * 1024 + row))
            .collect::<Vec<_>>();
        let batch = source(session, &values, &vec![Some(text.as_str()); 1024])?;
        next += 1;
        Ok(Some(batch))
    };
    let mut expected = 0_i64;
    let result = prepared
        .with_batch_input(&mut input)
        .unwrap()
        .for_each_json_batch(&CancellationToken::default(), 511, 1 << 20, |batch| {
            let rows: Vec<serde_json::Value> =
                serde_json::from_str(batch.values_json.value()).unwrap();
            assert_eq!(rows.len(), batch.rows);
            for row in rows {
                assert_eq!(row, serde_json::json!({"n":expected,"s":text}));
                expected += 1;
            }
            Ok(())
        })
        .unwrap();
    assert_eq!(expected, 41 * 1024);
    assert_eq!(result.output_rows, u64::try_from(expected).unwrap());
    let input = result.input.as_ref().unwrap();
    assert!(input.input_logical_bytes > 4 * (2 << 20));
    assert_eq!(input.payload_batches, 41);
    assert_eq!(input.max_retained_input_batches, 1);
    assert!(input.max_retained_input_logical_bytes < 300_000);
    assert!(result.runtime.memory.peak_reserved_bytes <= 2 << 20);
    assert_eq!(result.runtime.completed_executions, 1);
    drop(result);
    assert_eq!(prepared.snapshot().memory.reserved_bytes, initial);
}

#[test]
fn completion_input_end_is_required_even_for_empty_or_filtered_results() {
    for empty in [true, false] {
        let plan = VortexRelationalPlan::Filter(Box::new(VortexRelationalFilter {
            input: scan(),
            predicate: Expression::new(
                ExprId::new("false").unwrap(),
                ExpressionKind::Literal(shardloom_core::ScalarValue::Boolean(false)),
            ),
        }));
        let prepared = prepare(&plan, 2 << 20).unwrap();
        let mut calls = 0;
        let mut input = |session: &ResidentVortexSession| {
            calls += 1;
            if empty || calls == 4 {
                return Ok(None);
            }
            Ok(Some(source(session, &[Some(calls)], &[Some("hidden")])?))
        };
        let result = prepared
            .with_batch_input(&mut input)
            .unwrap()
            .collect_jsonl(&CancellationToken::default())
            .unwrap();
        assert_eq!(calls, if empty { 1 } else { 4 });
        assert_eq!(result.execution.output_rows, 0);
        assert_eq!(result.execution.output_batches, 1);
        assert!(
            result
                .execution
                .input
                .as_ref()
                .unwrap()
                .end_of_input_observed
        );
        assert_eq!(json_rows(&result), [] as [serde_json::Value; 0]);
        assert_eq!(result.execution.output_columns, vec!["n", "s"]);
    }
}

#[test]
fn completion_input_late_failure_cancellation_and_consumer_error_never_complete() {
    for failure in ["producer", "schema", "cancel", "consumer"] {
        let prepared = prepare(&scan(), 2 << 20).unwrap();
        let initial = prepared.snapshot().memory.reserved_bytes;
        let token = CancellationToken::default();
        let mut calls = 0;
        let mut emitted = 0;
        let mut input = |session: &ResidentVortexSession| {
            calls += 1;
            if calls > 1 {
                if failure == "producer" {
                    return Err(batch_input::failed("late producer error"));
                }
                return Ok(Some(ResidentMemorySource::from_batch_columns(
                    session,
                    &[MemoryColumn {
                        name: "wrong",
                        values: MemoryColumnValues::Int64(&[Some(2)]),
                    }],
                )?));
            }
            Ok(Some(source(session, &[Some(1)], &[Some("provisional")])?))
        };
        let result = prepared
            .with_batch_input(&mut input)
            .unwrap()
            .for_each_batch(&token, |_, _| {
                emitted += 1;
                match failure {
                    "cancel" => token.cancel(),
                    "consumer" => return Err(batch_input::failed("consumer refused")),
                    _ => {}
                }
                Ok(())
            });
        assert!(result.is_err());
        assert_eq!(emitted, 1);
        assert_eq!(
            calls,
            if matches!(failure, "producer" | "schema") {
                2
            } else {
                1
            }
        );
        assert_eq!(prepared.snapshot().completed_executions, 0);
        assert_eq!(prepared.snapshot().memory.reserved_bytes, initial);
    }
}

#[test]
fn completion_input_foreign_shared_and_untracked_owners_are_denied() {
    for invalid in ["foreign", "shared", "untracked", "buffer_alias"] {
        let prepared = prepare(&scan(), 4 << 20).unwrap();
        let initial = prepared.snapshot().memory.reserved_bytes;
        let foreign = ResidentVortexSession::new(1 << 20, 1).unwrap();
        let session = if invalid == "foreign" {
            &foreign
        } else {
            &prepared.session
        };
        let batch = if invalid == "untracked" {
            ResidentMemorySource::from_columns(
                session,
                &[
                    MemoryColumn {
                        name: "n",
                        values: MemoryColumnValues::Int64(&[Some(1)]),
                    },
                    MemoryColumn {
                        name: "s",
                        values: MemoryColumnValues::Utf8(&[Some("held")]),
                    },
                ],
                MemorySourceBounds::default(),
            )
            .unwrap()
        } else {
            source(session, &[Some(1)], &[Some("held")]).unwrap()
        };
        let alias = (invalid == "shared").then(|| batch.clone());
        let buffer_alias = (invalid == "buffer_alias").then(|| {
            batch
                .prepare_projection(&["n", "s"], None, None)
                .unwrap()
                .execute_arrays()
                .unwrap()
        });
        let completed_before = prepared.snapshot().completed_executions;
        let mut batch = Some(batch);
        let mut calls = 0;
        let mut input = |_: &ResidentVortexSession| {
            calls += 1;
            Ok(batch.take())
        };
        let result = prepared
            .with_batch_input(&mut input)
            .unwrap()
            .for_each_batch(&CancellationToken::default(), |_, _| Ok(()));
        assert!(result.is_err(), "{invalid}");
        assert_eq!(calls, 1, "must deny before another demand: {invalid}");
        assert_eq!(prepared.snapshot().completed_executions, completed_before);
        drop(alias);
        drop(buffer_alias);
        assert_eq!(prepared.snapshot().memory.reserved_bytes, initial);
        assert_eq!(foreign.snapshot().memory.reserved_bytes, 0);
    }
}

#[test]
fn completion_input_plan_and_provider_admission_precede_payload() {
    let union = VortexRelationalPlan::Set(Box::new(VortexRelationalSet {
        left: scan(),
        right: scan(),
        kind: crate::relational_query::VortexRelationalSetKind::UnionAll,
    }));
    let joined = VortexRelationalPlan::Join(Box::new(VortexRelationalJoin {
        left: scan(),
        right: scan(),
        kind: JoinKind::Cross,
        keys: vec![],
        condition: None,
        columns: vec![],
    }));
    for (plan, expected) in [
        (union, "set operation/repeated source"),
        (joined, "repeated batch source"),
    ] {
        let error = prepare(&plan, 2 << 20).err().unwrap().to_string();
        assert!(error.contains("SL-NATIVE-BATCH") && error.contains(expected));
    }
    let prepared = prepare(&scan(), 2 << 20).unwrap();
    assert!(
        prepared
            .for_each_batch(&CancellationToken::default(), |_, _| panic!("no provider"))
            .is_err()
    );
    let mut calls = 0;
    let mut input = |_: &ResidentVortexSession| {
        calls += 1;
        Ok(None)
    };
    let path = std::env::temp_dir().join(format!(
        "shardloom-stream-rejected-{}.json",
        std::process::id()
    ));
    assert!(
        prepared
            .with_batch_input(&mut input)
            .unwrap()
            .write_controlled(
                &path,
                super::super::super::VortexLocalPrimitiveRowExportFormat::Json,
                false,
                &CancellationToken::default(),
            )
            .is_err()
    );
    assert_eq!(calls, 0);
    assert!(!path.exists());
    let fixture = Fixture::new(keyed(&[Some(0)], &[0]), 1);
    let parent = fixture.0.join("no-provider-parent");
    let error = prepared
        .write_many(
            &[(
                parent.join("output.vortex"),
                super::super::super::VortexLocalPrimitiveRowExportFormat::Vortex,
            )],
            false,
        )
        .err()
        .expect("fanout must reject a missing provider before creating directories");
    assert!(error.to_string().contains("requires with_batch_input"));
    assert!(!parent.exists());
    let dynamic = prepare_relational_with_dynamic_inputs(
        &[DatasetUri::new("memory://stream").unwrap()],
        policy(),
        4096,
        |preparation| {
            preparation.register_batch_source(DatasetUri::new("memory://stream")?, |session| {
                source(session, &[], &[])
            })
        },
        |_| panic!("dynamic input must be rejected before lowering"),
    );
    assert!(
        dynamic
            .err()
            .unwrap()
            .to_string()
            .contains("dynamic schemas")
    );
}

#[test]
fn completion_input_native_writer_reopens_complete_values_and_cleans_late_failure() {
    let fixture = Fixture::new(keyed(&[Some(0)], &[0]), 1);
    for fail in [false, true] {
        let prepared = prepare(&scan(), 8 << 20).unwrap();
        let initial = prepared.snapshot().memory.reserved_bytes;
        let output = fixture.0.join(format!("stream-{fail}.vortex"));
        let mut calls = 0;
        let mut input = |session: &ResidentVortexSession| {
            calls += 1;
            if calls == 4 {
                return if fail {
                    Err(batch_input::failed("late input refusal"))
                } else {
                    Ok(None)
                };
            }
            Ok(Some(source(
                session,
                &[Some(calls), None],
                &[Some("東京"), None],
            )?))
        };
        let result = prepared
            .with_batch_input(&mut input)
            .unwrap()
            .write_controlled(
                &output,
                super::super::super::VortexLocalPrimitiveRowExportFormat::Vortex,
                false,
                &CancellationToken::default(),
            );
        assert_eq!(calls, 4);
        if fail {
            assert!(
                result
                    .err()
                    .unwrap()
                    .to_string()
                    .contains("late input refusal")
            );
            assert!(!output.exists());
            assert_eq!(prepared.snapshot().completed_executions, 0);
        } else {
            let result = result.unwrap();
            assert!(
                result
                    .execution
                    .input
                    .as_ref()
                    .unwrap()
                    .end_of_input_observed
            );
            assert_eq!(result.execution.output_rows, 6);
            assert_eq!(result.execution.runtime.completed_executions, 1);
            let reopened = prepare_relational(
                &VortexRelationalPlan::Scan(VortexRelationalScan {
                    source_uri: DatasetUri::new(output.display().to_string()).unwrap(),
                    projection: shardloom_plan::ProjectionRequest::All,
                    predicate: None,
                }),
                policy(),
            )
            .unwrap()
            .collect_jsonl(&CancellationToken::default())
            .unwrap();
            assert_eq!(
                json_rows(&reopened),
                serde_json::json!([
                    {"n":1,"s":"東京"},{"n":null,"s":null}, {"n":2,"s":"東京"},{"n":null,"s":null},
                    {"n":3,"s":"東京"},{"n":null,"s":null},
                ])
                .as_array()
                .unwrap()
                .clone()
            );
            drop(result);
        }
        assert_eq!(prepared.snapshot().memory.reserved_bytes, initial);
        let mut names = fs::read_dir(&fixture.0)
            .unwrap()
            .map(|entry| entry.unwrap().file_name())
            .collect::<Vec<_>>();
        names.sort();
        assert_eq!(names, ["input.vortex", "stream-false.vortex"]);
    }
}

#[test]
fn completion_input_empty_batches_still_obey_the_finite_source_limit() {
    for extra in [false, true] {
        let prepared = prepare(&scan(), 2 << 20).unwrap();
        let initial = prepared.snapshot().memory.reserved_bytes;
        let mut batches = 0;
        let mut input = |session: &ResidentVortexSession| {
            if !extra && batches == 4096 {
                return Ok(None);
            }
            batches += 1;
            Ok(Some(source(session, &[], &[])?))
        };
        let result = prepared
            .with_batch_input(&mut input)
            .unwrap()
            .collect_jsonl(&CancellationToken::default());
        if extra {
            assert!(result.err().unwrap().to_string().contains("4,096"));
            assert_eq!(batches, 4097);
            assert_eq!(prepared.snapshot().completed_executions, 0);
        } else {
            let result = result.unwrap();
            assert_eq!(
                result.execution.input.as_ref().unwrap().payload_batches,
                4096
            );
            assert_eq!(result.execution.output_rows, 0);
            assert_eq!(prepared.snapshot().completed_executions, 1);
        }
        assert_eq!(prepared.snapshot().memory.reserved_bytes, initial);
    }
}

#[test]
fn completion_input_late_expression_failure_cannot_certify_an_emitted_prefix() {
    let expression = Expression::new(
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
    let plan = VortexRelationalPlan::Project(Box::new(VortexRelationalProject {
        input: scan(),
        expressions: vec![("increment".into(), expression)],
    }));
    let prepared = prepare(&plan, 2 << 20).unwrap();
    let initial = prepared.snapshot().memory.reserved_bytes;
    let mut calls = 0;
    let mut input = |session: &ResidentVortexSession| {
        calls += 1;
        Ok(Some(source(
            session,
            &[Some(if calls == 1 { 3 } else { i64::MAX })],
            &[None],
        )?))
    };
    let mut rows = Vec::new();
    let result = prepared
        .with_batch_input(&mut input)
        .unwrap()
        .for_each_json_batch(&CancellationToken::default(), 10, 4096, |batch| {
            rows.extend(
                serde_json::from_str::<Vec<serde_json::Value>>(batch.values_json.value()).unwrap(),
            );
            Ok(())
        });
    assert!(result.err().unwrap().to_string().contains("overflow"));
    assert_eq!(rows, vec![serde_json::json!({"increment":4})]);
    assert_eq!(calls, 2);
    assert_eq!(prepared.snapshot().completed_executions, 0);
    assert_eq!(prepared.snapshot().memory.reserved_bytes, initial);
}
