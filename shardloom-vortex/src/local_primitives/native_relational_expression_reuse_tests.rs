use super::*;
use crate::resident_session::ResidentVortexSession;
use shardloom_core::{BinaryOp, ComparisonOp, UnaryOp};
use shardloom_exec::compute_pool::CancellationToken;
use vortex::array::{
    ArrayRef, IntoArray as _, VortexSessionExecute as _,
    arrays::{PrimitiveArray, StructArray},
    dtype::{DType, Nullability, PType},
};

fn column(name: &str) -> Expression {
    Expression::new(
        DType::Primitive(PType::I64, Nullability::Nullable),
        Kind::Column(name.into()),
    )
}

fn literal(scalar: impl Into<Scalar>) -> Expression {
    let scalar = scalar.into();
    Expression::new(scalar.dtype().clone(), Kind::Literal(scalar))
}

fn binary(left: Expression, op: BinaryOp, right: Expression) -> Expression {
    Expression::new(
        left.dtype.clone(),
        Kind::Binary(Box::new(left), op, Box::new(right)),
    )
}

fn product() -> Expression {
    binary(column("a"), BinaryOp::Multiply, literal(2_i64))
}

#[test]
fn expression_reuse_rebinds_after_a_prior_column_output_is_removed() {
    let mut expressions = vec![column("unused"), column("a"), product(), product()];
    Expression::bind_projection_reuse(&mut expressions);
    assert_eq!(expressions[3].reuse_output, Some(2));
    expressions.remove(0);
    Expression::bind_projection_reuse(&mut expressions);
    assert_eq!(expressions[2].reuse_output, Some(1));
    expressions.remove(1);
    Expression::bind_projection_reuse(&mut expressions);
    assert_eq!(expressions[1].reuse_output, None);
    let Kind::Binary(left, _, _) = &expressions[1].kind else {
        panic!("product changed");
    };
    assert_eq!(left.reuse_output, Some(0));
}

fn assert_identical(left: &Expression, right: &Expression, expected: bool) {
    let mut remaining = MAX_NODES;
    assert_eq!(same(left, right, &mut remaining), expected);
}

#[test]
fn expression_reuse_preserves_order_type_and_literal_bits_with_bounded_search() {
    assert_identical(&product(), &product(), true);
    assert_identical(
        &product(),
        &binary(literal(2_i64), BinaryOp::Multiply, column("a")),
        false,
    );
    assert_identical(
        &product(),
        &binary(column("a"), BinaryOp::Add, literal(2_i64)),
        false,
    );
    assert_identical(&column("a"), &column("b"), false);
    assert_identical(
        &column("a"),
        &Expression::new(
            DType::Primitive(PType::I64, Nullability::NonNullable),
            Kind::Column("a".into()),
        ),
        false,
    );
    assert_identical(&literal(-0.0_f64), &literal(0.0_f64), false);
    assert_identical(&literal(-0.0_f32), &literal(0.0_f32), false);
    assert_identical(&literal(-0.0_f64), &literal(-0.0_f64), true);
    assert_identical(&literal(f64::NAN), &literal(f64::NAN), true);
    assert_identical(
        &literal(f64::from_bits(0x7ff8_0000_0000_0001)),
        &literal(f64::from_bits(0x7ff8_0000_0000_0002)),
        false,
    );
    let chain = |nodes: usize| {
        let mut expression = column("a");
        for _ in 1..nodes {
            expression = Expression::new(
                expression.dtype.clone(),
                Kind::Unary(UnaryOp::Negate, Box::new(expression)),
            );
        }
        expression
    };
    assert_identical(&chain(64), &chain(64), true);
    assert_identical(&chain(65), &chain(65), false);
    for gap in [15, 16] {
        let mut expressions = vec![product()];
        expressions.extend((0..gap).map(|_| column("unrelated")));
        expressions.push(product());
        Expression::bind_projection_reuse(&mut expressions);
        assert_eq!(
            expressions.last().unwrap().reuse_output,
            (gap == 15).then_some(0)
        );
    }
}

#[test]
fn expression_reuse_keeps_lazy_branches_and_selected_inputs_out_of_scope() {
    let wrappers = [
        Kind::Conditional(
            Box::new(literal(true)),
            Box::new(product()),
            Box::new(product()),
        ),
        Kind::Coalesce(vec![product(), product()]),
        Kind::NullIf(Box::new(product()), Box::new(product())),
        Kind::Cast {
            input: Box::new(product()),
            tolerant: false,
        },
        Kind::List(vec![product()]),
        Kind::Struct(vec![product()]),
        Kind::Function {
            function: super::super::scalar::Function::Abs,
            args: vec![product()],
        },
    ];
    for wrapper in wrappers {
        let mut expressions = vec![product(), Expression::new(column("a").dtype, wrapper)];
        Expression::bind_projection_reuse(&mut expressions);
        let expression = &expressions[1];
        assert!(expression.reuse_output.is_none());
        let children: Vec<&Expression> = match &expression.kind {
            Kind::Conditional(c, y, n) => vec![c, y, n],
            Kind::Coalesce(children)
            | Kind::List(children)
            | Kind::Struct(children)
            | Kind::Function { args: children, .. } => children.iter().collect(),
            Kind::NullIf(l, r) => vec![l, r],
            Kind::Cast { input, .. } => vec![input],
            _ => unreachable!(),
        };
        assert!(children.iter().all(|child| child.reuse_output.is_none()));
    }
}

#[test]
fn expression_reuse_shares_complete_coalesce_outputs_without_reusing_selected_branches() {
    let coalesce = || {
        Expression::new(
            column("a").dtype,
            Kind::Coalesce(vec![product(), literal(7_i64)]),
        )
    };
    let mut expressions = vec![product(), coalesce(), coalesce()];
    Expression::bind_projection_reuse(&mut expressions);
    assert!(expressions[1].reuse_output.is_none());
    assert_eq!(expressions[2].reuse_output, Some(1));
    let Kind::Coalesce(children) = &expressions[1].kind else {
        unreachable!();
    };
    assert!(children.iter().all(|child| child.reuse_output.is_none()));
    let input = StructArray::from_fields(&[(
        "a",
        PrimitiveArray::from_option_iter([None, Some(-3_i64), None, Some(2)]).into_array(),
    )])
    .unwrap()
    .into_array();
    let session = ResidentVortexSession::new(1 << 20, 1).unwrap();
    let memory = session.memory().clone();
    session
        .with_native_execution_context(&CancellationToken::default(), |context| {
            let mut outputs = Vec::new();
            for expression in &expressions {
                outputs.push(expression.evaluate_reusing(&input, context, &outputs)?);
            }
            assert!(ArrayRef::ptr_eq(&outputs[1], &outputs[2]));
            let mut execution = context.native_session().create_execution_ctx();
            for (row, value) in [7_i64, -6, 7, 4].into_iter().enumerate() {
                assert_eq!(
                    outputs[2].execute_scalar(row, &mut execution).unwrap(),
                    Scalar::primitive(value, Nullability::Nullable),
                );
            }
            Ok(())
        })
        .unwrap();
    drop(session);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

#[test]
fn expression_reuse_complete_lazy_results_preserve_branch_errors_and_cast_policy() {
    let guarded = || {
        Expression::new(
            column("a").dtype,
            Kind::Conditional(
                Box::new(literal(false)),
                Box::new(binary(column("a"), BinaryOp::Divide, literal(0_i64))),
                Box::new(column("a")),
            ),
        )
    };
    let cast = |tolerant| {
        Expression::new(
            column("a").dtype,
            Kind::Cast {
                input: Box::new(column("a")),
                tolerant,
            },
        )
    };
    assert_identical(&guarded(), &guarded(), true);
    assert_identical(&cast(true), &cast(false), false);
    let mut expressions = vec![guarded(), guarded()];
    Expression::bind_projection_reuse(&mut expressions);
    let input = StructArray::from_fields(&[(
        "a",
        PrimitiveArray::from_option_iter([Some(3_i64), None]).into_array(),
    )])
    .unwrap()
    .into_array();
    let session = ResidentVortexSession::new(1 << 20, 1).unwrap();
    session
        .with_native_execution_context(&CancellationToken::default(), |context| {
            let first = expressions[0].evaluate(&input, context)?;
            let second =
                expressions[1].evaluate_reusing(&input, context, std::slice::from_ref(&first))?;
            assert!(ArrayRef::ptr_eq(&first, &second));
            let unguarded = binary(column("a"), BinaryOp::Divide, literal(0_i64));
            assert!(unguarded.evaluate(&input, context).is_err());
            Ok(())
        })
        .unwrap();
}

#[test]
fn expression_reuse_returns_owned_values_and_preserves_checks_and_cancellation() {
    let mut expressions = vec![
        product(),
        product(),
        binary(product(), BinaryOp::Subtract, column("a")),
        Expression::new(
            DType::Bool(Nullability::Nullable),
            Kind::Compare(Box::new(product()), ComparisonOp::Eq, Box::new(product())),
        ),
    ];
    Expression::bind_projection_reuse(&mut expressions);
    let input = StructArray::from_fields(&[(
        "a",
        PrimitiveArray::from_option_iter([Some(2_i64), None, Some(-3)]).into_array(),
    )])
    .unwrap()
    .into_array();
    let session = ResidentVortexSession::new(1 << 20, 1).unwrap();
    let memory = session.memory().clone();
    let mut retained = Vec::<ArrayRef>::new();
    session
        .with_native_execution_context(&CancellationToken::default(), |context| {
            #[cfg(debug_assertions)]
            let observation =
                crate::local_primitives::native_relational_observation::Session::start();
            for expression in &expressions {
                retained.push(expression.evaluate_reusing(&input, context, &retained)?);
            }
            assert!(ArrayRef::ptr_eq(&retained[0], &retained[1]));
            #[cfg(debug_assertions)]
            {
                let counts = observation.finish();
                assert_eq!(counts.predicate_compile_attempts, 0);
                assert_eq!(counts.expression_output_reuses, 4);
            }
            let mut execution = context.native_session().create_execution_ctx();
            for (row, (gross, net, equal)) in [
                (Some(4_i64), Some(2_i64), Some(true)),
                (None, None, None),
                (Some(-6), Some(-3), Some(true)),
            ]
            .into_iter()
            .enumerate()
            {
                for (index, expected) in [gross, gross, net].into_iter().enumerate() {
                    assert_eq!(
                        retained[index].execute_scalar(row, &mut execution).unwrap(),
                        expected.map_or_else(
                            || Scalar::null(expressions[index].dtype.clone()),
                            |value| Scalar::primitive(value, Nullability::Nullable),
                        )
                    );
                }
                assert_eq!(
                    retained[3].execute_scalar(row, &mut execution).unwrap(),
                    equal.map_or_else(
                        || Scalar::null(expressions[3].dtype.clone()),
                        |value| Scalar::bool(value, Nullability::Nullable),
                    )
                );
            }
            let wrong_size = retained[0].slice(0..1).unwrap();
            assert!(
                expressions[1]
                    .evaluate_reusing(&input, context, &[wrong_size])
                    .is_err()
            );
            assert!(
                expressions[1]
                    .evaluate_reusing(&input, context, &[retained[3].clone()])
                    .is_err()
            );
            assert!(
                expressions[1]
                    .evaluate_reusing(&input, context, &[])
                    .is_err()
            );
            Ok(())
        })
        .unwrap();
    let bytes = memory.snapshot().reserved_bytes;
    assert!(bytes > 0);
    let cancel = CancellationToken::default();
    assert!(
        session
            .with_native_execution_context(&cancel, |context| {
                cancel.cancel();
                assert!(
                    expressions[1]
                        .evaluate_reusing(&input, context, &retained)
                        .is_err()
                );
                Ok(())
            })
            .is_err()
    );
    assert_eq!(memory.snapshot().reserved_bytes, bytes);
    drop((input, session, expressions));
    assert_eq!(memory.snapshot().reserved_bytes, bytes);
    drop(retained);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}
