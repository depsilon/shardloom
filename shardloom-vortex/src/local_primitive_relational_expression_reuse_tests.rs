//! Prepared expression decisions and output reuse through real streaming plans.

use super::*;
use shardloom_core::{BinaryOp, ComparisonOp, ScalarValue};

fn binary(left: Expression, op: BinaryOp, right: Expression) -> Expression {
    Expression::new(
        ExprId::new("binary").unwrap(),
        ExpressionKind::Binary {
            left: Box::new(left),
            op,
            right: Box::new(right),
        },
    )
}

fn literal(value: i64) -> Expression {
    Expression::new(
        ExprId::new("literal").unwrap(),
        ExpressionKind::Literal(ScalarValue::Int64(value)),
    )
}

fn compare(op: ComparisonOp, value: i64) -> Expression {
    Expression::new(
        ExprId::new("compare").unwrap(),
        ExpressionKind::Compare {
            left: Box::new(col("n")),
            op,
            right: Box::new(literal(value)),
        },
    )
}

fn product() -> Expression {
    binary(col("n"), BinaryOp::Multiply, literal(2))
}

fn project(expressions: Vec<(&str, Expression)>) -> VortexRelationalPlan {
    VortexRelationalPlan::Project(Box::new(VortexRelationalProject {
        input: scan(),
        expressions: expressions
            .into_iter()
            .map(|(name, expression)| (name.into(), expression))
            .collect(),
    }))
}

#[test]
fn expression_reuse_prepared_decisions_and_results_are_local_to_each_batch_and_execution() {
    let predicate = binary(
        compare(ComparisonOp::GtEq, 0),
        BinaryOp::And,
        compare(ComparisonOp::Lt, 5),
    );
    let plan = project(vec![
        ("gross", product()),
        ("again", product()),
        ("net", binary(product(), BinaryOp::Subtract, col("n"))),
        ("selected", predicate),
    ]);
    #[cfg(debug_assertions)]
    let observation = crate::local_primitives::native_relational_observation::Session::start();
    let prepared = prepare(&plan, 2 << 20).unwrap();
    #[cfg(debug_assertions)]
    assert!(observation.finish().predicate_compile_attempts > 0);
    let initial = prepared.snapshot().memory.reserved_bytes;
    let memory = prepared.session.memory().clone();
    let mut retained = Vec::new();
    for iteration in 0..2 {
        let shift = iteration * 7;
        let numbers = [
            Some(-2 + shift),
            None,
            Some(shift),
            Some(4 + shift),
            Some(5 + shift),
        ];
        let mut ranges = [0..0, 0..2, 2..2, 2..5, 5..5].into_iter();
        let mut prior: Option<Weak<MemoryLease>> = None;
        let mut input = |session: &ResidentVortexSession| {
            assert!(
                prior
                    .as_ref()
                    .is_none_or(|witness| witness.strong_count() == 0)
            );
            ranges
                .next()
                .map(|range| {
                    let input = source(session, &numbers[range.clone()], &vec![None; range.len()])?;
                    prior = Some(input.batch_release_witness()?);
                    Ok(input)
                })
                .transpose()
        };
        #[cfg(debug_assertions)]
        let observation = crate::local_primitives::native_relational_observation::Session::start();
        let mut actual = Vec::new();
        let result = prepared
            .with_batch_input(&mut input)
            .unwrap()
            .for_each_batch(&CancellationToken::default(), |array, context| {
                actual.extend(values(&array, context)?);
                assert_eq!(array.dtype(), &prepared.output_dtype().unwrap());
                retained.push(array);
                Ok(())
            })
            .unwrap();
        #[cfg(debug_assertions)]
        {
            let counts = observation.finish();
            assert_eq!(counts.predicate_compile_attempts, 0);
            assert_eq!(counts.predicate_boolean_rows, numbers.len() as u64);
            assert!(counts.expression_output_reuses >= 4);
        }
        let expected = numbers
            .into_iter()
            .map(|number| {
                serde_json::json!({
                    "gross": number.map(|n| n * 2), "again": number.map(|n| n * 2),
                    "net": number, "selected": number.map(|n| (0..5).contains(&n)),
                })
            })
            .collect::<Vec<_>>();
        assert_eq!(actual, expected);
        assert!(result.input.as_ref().unwrap().end_of_input_observed);
        assert!(result.native_io_certificate.is_certified());
        assert!(!result.native_io_certificate.side_effects.fallback_attempted);
        assert_eq!(
            result.runtime.completed_executions,
            u64::try_from(iteration + 1).unwrap()
        );
        drop(result);
        assert!(memory.snapshot().reserved_bytes > initial);
        retained.clear();
        assert_eq!(memory.snapshot().reserved_bytes, initial);
    }
    drop(prepared);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

#[test]
fn expression_reuse_preserves_lazy_case_coalesce_and_nullif_results() {
    let call = |name: &str, args| {
        Expression::new(
            ExprId::new(name).unwrap(),
            ExpressionKind::FunctionCall {
                name: name.into(),
                args,
            },
        )
    };
    let unsafe_division = binary(literal(8), BinaryOp::Divide, col("n"));
    let plan = project(vec![
        ("gross", product()),
        (
            "case",
            call(
                "case_when",
                vec![compare(ComparisonOp::Eq, 0), literal(99), unsafe_division],
            ),
        ),
        (
            "coalesced",
            call("coalesce", vec![col("n"), product(), literal(3)]),
        ),
        ("nullified", call("nullif", vec![product(), literal(4)])),
        ("after", binary(product(), BinaryOp::Add, literal(1))),
    ]);
    let prepared = prepare(&plan, 2 << 20).unwrap();
    let initial = prepared.snapshot().memory.reserved_bytes;
    let mut next = false;
    let mut input = |session: &ResidentVortexSession| {
        if std::mem::replace(&mut next, true) {
            return Ok(None);
        }
        source(session, &[Some(0), Some(2), None, Some(-2)], &[None; 4]).map(Some)
    };
    let mut actual = Vec::new();
    prepared
        .with_batch_input(&mut input)
        .unwrap()
        .for_each_batch(&CancellationToken::default(), |array, context| {
            actual.extend(values(&array, context)?);
            Ok(())
        })
        .unwrap();
    assert_eq!(
        actual,
        serde_json::json!([
            {"gross":0,"case":99,"coalesced":0,"nullified":0,"after":1},
            {"gross":4,"case":4,"coalesced":2,"nullified":null,"after":5},
            {"gross":null,"case":null,"coalesced":3,"nullified":null,"after":null},
            {"gross":-4,"case":-4,"coalesced":-2,"nullified":-4,"after":-3},
        ])
        .as_array()
        .unwrap()
        .clone()
    );
    assert_eq!(prepared.snapshot().memory.reserved_bytes, initial);
}

#[test]
fn expression_reuse_preserves_late_errors_consumer_failure_and_denied_credits() {
    let plan = project(vec![
        ("gross", product()),
        ("again", product()),
        ("after", binary(product(), BinaryOp::Add, literal(1))),
    ]);
    for fault in 0..4 {
        let prepared = prepare(&plan, 2 << 20).unwrap();
        let initial = prepared.snapshot().memory.reserved_bytes;
        let cancellation = CancellationToken::default();
        let mut calls = 0;
        let mut emitted = 0;
        let mut input = |session: &ResidentVortexSession| {
            calls += 1;
            if calls == 3 {
                return Ok(None);
            }
            source(
                session,
                &[Some(if calls == 1 { 2 } else { i64::MAX })],
                &[None],
            )
            .map(Some)
        };
        let memory = prepared.session.memory().clone();
        let occupied = if fault == 3 {
            Some(memory.reserve((2 << 20) - initial).unwrap())
        } else {
            None
        };
        let error = prepared
            .with_batch_input(&mut input)
            .unwrap()
            .for_each_batch(&cancellation, |_, _| {
                emitted += 1;
                if fault == 1 {
                    return Err(failed("expression consumer failed"));
                }
                if fault == 2 {
                    cancellation.cancel();
                }
                Ok(())
            })
            .err()
            .unwrap()
            .to_string();
        match fault {
            0 => {
                assert_eq!(emitted, 1);
                assert!(error.contains("overflow"));
            }
            1 => assert!(error.contains("expression consumer failed")),
            2 => assert!(error.to_ascii_lowercase().contains("cancel")),
            _ => {
                assert_eq!(emitted, 0);
                assert!(memory.snapshot().denied_reservations > 0);
            }
        }
        drop(occupied);
        assert_eq!(prepared.snapshot().completed_executions, 0);
        assert_eq!(memory.snapshot().reserved_bytes, initial);
        drop(prepared);
        assert_eq!(memory.snapshot().reserved_bytes, 0);
    }
}

#[test]
fn expression_reuse_preparation_programs_remain_under_shared_metadata_admission() {
    let fields = (0..8)
        .map(|index| format!("predicate{index}"))
        .collect::<Vec<_>>();
    let plan = project(
        fields
            .iter()
            .map(|name| {
                (
                    name.as_str(),
                    binary(
                        compare(ComparisonOp::Gt, 0),
                        BinaryOp::And,
                        compare(ComparisonOp::Lt, 5),
                    ),
                )
            })
            .collect(),
    );
    let session = ResidentVortexSession::new(128 << 10, 1).unwrap();
    let retained = session.memory().reserve(2048).unwrap();
    let mut binder = super::super::super::bind::Binder::new(&session).unwrap();
    binder
        .register_batch_source(DatasetUri::new("memory://stream").unwrap(), |session| {
            source(session, &[], &[])
        })
        .unwrap();
    let error = binder.bind(&plan, 0).err().unwrap().to_string();
    assert!(error.contains("memory reservation denied"), "{error}");
    assert!(session.memory().snapshot().denied_reservations > 0);
    drop(binder);
    assert_eq!(session.memory().snapshot().reserved_bytes, retained.bytes());
    drop(retained);
    assert_eq!(session.memory().snapshot().reserved_bytes, 0);
}
