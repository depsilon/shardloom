//! Scalar SUM/AVG consume transformed native batches through the shared kernel.

use super::*;
use shardloom_core::{BinaryOp, ComparisonOp, ScalarValue};

fn literal(value: ScalarValue) -> Expression {
    Expression::new(
        ExprId::new("literal").unwrap(),
        ExpressionKind::Literal(value),
    )
}

fn plan() -> VortexRelationalPlan {
    let filtered = VortexRelationalPlan::Filter(Box::new(VortexRelationalFilter {
        input: scan(),
        predicate: Expression::new(
            ExprId::new("keep").unwrap(),
            ExpressionKind::Compare {
                left: Box::new(col("s")),
                op: ComparisonOp::Eq,
                right: Box::new(literal(ScalarValue::Utf8("keep".into()))),
            },
        ),
    }));
    let projected = VortexRelationalPlan::Project(Box::new(VortexRelationalProject {
        input: filtered,
        expressions: vec![(
            "computed".into(),
            Expression::new(
                ExprId::new("computed").unwrap(),
                ExpressionKind::Binary {
                    left: Box::new(col("n")),
                    op: BinaryOp::Multiply,
                    right: Box::new(literal(ScalarValue::Float64(1.0))),
                },
            ),
        )],
    }));
    aggregate(
        projected,
        &[],
        vec![
            measure("sum", Some("computed"), "sum"),
            measure("avg", Some("computed"), "avg"),
            measure("count", Some("computed"), "present"),
        ],
    )
}

fn float_source(
    session: &ResidentVortexSession,
    numbers: &[Option<f64>],
    text: &[Option<&str>],
) -> Result<ResidentMemorySource> {
    ResidentMemorySource::from_batch_columns(
        session,
        &[
            MemoryColumn {
                name: "n",
                values: MemoryColumnValues::Float64(numbers),
            },
            MemoryColumn {
                name: "s",
                values: MemoryColumnValues::Utf8(text),
            },
        ],
    )
}

fn prepared_float() -> PreparedVortexRelational {
    let mut policy = policy();
    policy.resource_envelope.memory_budget_bytes = 4 << 20;
    prepare_relational_with_schema(policy, |preparation| {
        preparation.register_batch_source(DatasetUri::new("memory://stream")?, |session| {
            float_source(session, &[], &[])
        })?;
        Ok(plan())
    })
    .unwrap()
}

#[test]
fn composed_numeric_consumer_streams_after_filter_projection_and_reuses_no_prior_execution() {
    let prepared = prepared_float();
    let memory = prepared.session.memory().clone();
    let initial = memory.snapshot().reserved_bytes;
    let mut retained = Vec::new();
    for iteration in 1..=2 {
        let numbers = [
            Some(1e16),
            None,
            Some(-1e16),
            Some(99.0),
            Some(f64::from(iteration)),
        ];
        let strings = [
            Some("keep"),
            Some("keep"),
            Some("keep"),
            Some("skip"),
            Some("keep"),
        ];
        let mut ranges = [0..0, 0..2, 2..2, 2..4, 4..5, 5..5].into_iter();
        let mut prior: Option<Weak<MemoryLease>> = None;
        let ended = Cell::new(false);
        let mut input = |session: &ResidentVortexSession| {
            assert!(
                prior
                    .as_ref()
                    .is_none_or(|witness| witness.strong_count() == 0)
            );
            let Some(range) = ranges.next() else {
                ended.set(true);
                return Ok(None);
            };
            let source = float_source(session, &numbers[range.clone()], &strings[range])?;
            prior = Some(source.batch_release_witness()?);
            Ok(Some(source))
        };
        #[cfg(debug_assertions)]
        let observation = crate::local_primitives::native_relational_observation::Session::start();
        let mut actual = Vec::new();
        let result = prepared
            .with_batch_input(&mut input)
            .unwrap()
            .for_each_batch(&CancellationToken::default(), |array, context| {
                assert!(ended.get(), "aggregate emitted before complete input");
                assert_eq!(array.dtype(), &prepared.output_dtype().unwrap());
                actual.extend(values(&array, context)?);
                retained.push(array);
                Ok(())
            })
            .unwrap();
        #[cfg(debug_assertions)]
        {
            let counts = observation.finish();
            assert_eq!(counts.aggregate_numeric_bound_batches, 6);
            assert_eq!(counts.aggregate_numeric_bound_rows, 8);
            // COUNT observes only validity; none of these measures dispatches
            // through the generic scalar-value update path.
            assert_eq!(counts.aggregate_row_updates, 0);
        }
        assert_eq!(
            actual,
            vec![
                json!({"sum":f64::from(iteration), "avg":f64::from(iteration) / 3.0, "present":3})
            ]
        );
        let input = result.input.as_ref().unwrap();
        assert!(input.end_of_input_observed && input.output_ownership_detached);
        assert_eq!(input.rows, 5);
        assert_eq!(input.max_retained_input_batches, 1);
        assert_eq!(
            result.runtime.completed_executions,
            u64::try_from(iteration).unwrap()
        );
        assert!(result.native_io_certificate.is_certified());
        assert!(!result.native_io_certificate.side_effects.fallback_attempted);
        drop(result);
        assert!(memory.snapshot().reserved_bytes > initial);
        if iteration == 1 {
            retained.clear();
            assert_eq!(memory.snapshot().reserved_bytes, initial);
        }
    }
    drop(prepared);
    assert!(memory.snapshot().reserved_bytes > 0);
    let clone = retained[0].slice(0..1).unwrap();
    drop(retained);
    assert!(memory.snapshot().reserved_bytes > 0);
    drop(clone);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

#[test]
fn composed_numeric_consumer_pipeline_faults_refund_credits_without_completion() {
    for fault in ["producer", "sum", "cancel", "consumer", "denied"] {
        let prepared = prepared_float();
        let memory = prepared.session.memory().clone();
        let initial = memory.snapshot().reserved_bytes;
        let token = CancellationToken::default();
        let mut calls = 0;
        let mut emitted = 0;
        let mut input = |session: &ResidentVortexSession| {
            calls += 1;
            if calls == 3 {
                if fault == "producer" {
                    return Err(failed("numeric producer failed"));
                }
                if fault == "cancel" {
                    token.cancel();
                }
                return Ok(None);
            }
            float_source(
                session,
                &[Some(if fault == "sum" { f64::MAX } else { 1.0 })],
                &[Some("keep")],
            )
            .map(Some)
        };
        let occupied = (fault == "denied").then(|| {
            memory
                .reserve(memory.snapshot().limit_bytes - initial)
                .unwrap()
        });
        let error = prepared
            .with_batch_input(&mut input)
            .unwrap()
            .for_each_batch(&token, |_, _| {
                emitted += 1;
                if fault == "consumer" {
                    return Err(failed("numeric consumer failed"));
                }
                Ok(())
            })
            .err()
            .unwrap()
            .to_string();
        assert_eq!(emitted, usize::from(fault == "consumer"));
        assert!(
            error.contains(match fault {
                "producer" => "numeric producer failed",
                "sum" => "aggregate sum became nonfinite",
                "cancel" => "cancel",
                "consumer" => "numeric consumer failed",
                _ => "reservation denied",
            }),
            "{fault}: {error}"
        );
        assert_eq!(prepared.snapshot().completed_executions, 0);
        drop(occupied);
        assert_eq!(memory.snapshot().reserved_bytes, initial);
        drop(prepared);
        assert_eq!(memory.snapshot().reserved_bytes, 0);
    }
}
