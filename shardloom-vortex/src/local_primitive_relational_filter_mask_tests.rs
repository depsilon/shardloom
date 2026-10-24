use super::*;
use shardloom_core::{BinaryOp, ComparisonOp, ScalarValue};

fn comparison(op: ComparisonOp, literal: i64) -> Expression {
    Expression::new(
        ExprId::new("compare").unwrap(),
        ExpressionKind::Compare {
            left: Box::new(col("n")),
            op,
            right: Box::new(Expression::new(
                ExprId::new("literal").unwrap(),
                ExpressionKind::Literal(ScalarValue::Int64(literal)),
            )),
        },
    )
}

#[test]
fn predicate_filter_mask_compiles_once_per_execution_across_nullable_and_empty_batches() {
    let plan = VortexRelationalPlan::Filter(Box::new(VortexRelationalFilter {
        input: scan(),
        predicate: Expression::new(
            ExprId::new("range").unwrap(),
            ExpressionKind::Binary {
                left: Box::new(comparison(ComparisonOp::GtEq, 0)),
                op: BinaryOp::And,
                right: Box::new(comparison(ComparisonOp::Lt, 5)),
            },
        ),
    }));
    let prepared = prepare(&plan, 2 << 20).unwrap();
    let initial = prepared.snapshot().memory.reserved_bytes;
    let numbers = [
        Some(-1),
        None,
        Some(0),
        Some(4),
        Some(5),
        Some(i64::MAX),
        Some(-2),
        Some(3),
        None,
    ];
    let text = [Some("λ"); 9];
    for completed in 1..=2 {
        let mut ranges = [0..0, 0..2, 2..6, 6..6, 6..9, 9..9].into_iter();
        let mut ended = false;
        let mut input = |session: &ResidentVortexSession| {
            if let Some(range) = ranges.next() {
                source(session, &numbers[range.clone()], &text[range]).map(Some)
            } else {
                ended = true;
                Ok(None)
            }
        };
        #[cfg(debug_assertions)]
        let observation = crate::local_primitives::native_relational_observation::Session::start();
        let mut actual = Vec::new();
        let result = prepared
            .with_batch_input(&mut input)
            .unwrap()
            .for_each_batch(&CancellationToken::default(), |array, context| {
                actual.extend(values(&array, context)?);
                Ok(())
            })
            .unwrap();
        #[cfg(debug_assertions)]
        {
            let counts = observation.finish();
            assert_eq!(counts.predicate_compile_attempts, 1);
            assert_eq!(counts.predicate_boolean_rows, 0);
        }
        assert!(ended);
        assert_eq!(
            actual,
            serde_json::json!([
                {"n": 0, "s": "λ"}, {"n": 4, "s": "λ"}, {"n": 3, "s": "λ"}
            ])
            .as_array()
            .unwrap()
            .clone()
        );
        assert!(result.input.as_ref().unwrap().end_of_input_observed);
        assert!(result.native_io_certificate.is_certified());
        assert!(!result.native_io_certificate.side_effects.fallback_attempted);
        assert_eq!(result.runtime.completed_executions, completed);
        drop(result);
        assert_eq!(prepared.snapshot().memory.reserved_bytes, initial);
    }
}
