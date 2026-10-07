use super::*;
use crate::resident_session::ResidentVortexSession;
use shardloom_exec::compute_pool::CancellationToken;
use vortex::{
    VortexSessionDefault as _,
    array::{
        IntoArray as _, VortexSessionExecute as _,
        arrays::{
            BoolArray, ChunkedArray, ConstantArray, DictArray, PrimitiveArray, StructArray,
            bool::BoolArrayExt as _,
        },
        dtype::{FieldNames, Nullability},
        memory::MemorySessionExt as _,
        scalar::Scalar,
        validity::Validity,
    },
};

fn col(name: &str, nullable: bool) -> Expression {
    Expression {
        dtype: DType::Primitive(
            PType::I64,
            if nullable {
                Nullability::Nullable
            } else {
                Nullability::NonNullable
            },
        ),
        kind: Kind::Column(name.into()),
    }
}

fn lit(value: i64) -> Expression {
    let scalar = Scalar::from(value);
    Expression {
        dtype: scalar.dtype().clone(),
        kind: Kind::Literal(scalar),
    }
}

fn comparison(left: Expression, op: ComparisonOp, right: Expression) -> Expression {
    Expression {
        dtype: DType::Bool(if left.dtype.is_nullable() || right.dtype.is_nullable() {
            Nullability::Nullable
        } else {
            Nullability::NonNullable
        }),
        kind: Kind::Compare(Box::new(left), op, Box::new(right)),
    }
}

fn cmp(name: &str, op: ComparisonOp, value: i64, nullable: bool, reversed: bool) -> Expression {
    if reversed {
        comparison(lit(value), op, col(name, nullable))
    } else {
        comparison(col(name, nullable), op, lit(value))
    }
}

fn binary(left: Expression, op: BinaryOp, right: Expression) -> Expression {
    Expression {
        dtype: DType::Bool(if left.dtype.is_nullable() || right.dtype.is_nullable() {
            Nullability::Nullable
        } else {
            Nullability::NonNullable
        }),
        kind: Kind::Binary(Box::new(left), op, Box::new(right)),
    }
}

fn not(child: Expression) -> Expression {
    Expression {
        dtype: child.dtype.clone(),
        kind: Kind::Unary(UnaryOp::Not, Box::new(child)),
    }
}

fn batch(columns: &[(&str, ArrayRef)]) -> ArrayRef {
    StructArray::try_new(
        columns
            .iter()
            .map(|(name, _)| *name)
            .collect::<FieldNames>(),
        columns.iter().map(|(_, array)| array.clone()),
        columns[0].1.len(),
        Validity::NonNullable,
    )
    .unwrap()
    .into_array()
}

fn check(
    expression: &Expression,
    input: &ArrayRef,
    expected: &[Option<bool>],
    session: &ResidentVortexSession,
) {
    assert!(Recipe::compile(expression).is_some());
    let output = session
        .with_native_execution_context(&CancellationToken::default(), |context| {
            expression.evaluate(input, context)
        })
        .unwrap();
    assert_eq!(output.dtype(), &expression.dtype);
    assert_eq!(output.len(), expected.len());
    let mut execution = vortex::session::VortexSession::default().create_execution_ctx();
    let values = output.execute::<BoolArray>(&mut execution).unwrap();
    let valid = values
        .validity()
        .unwrap()
        .execute_mask(values.len(), &mut execution)
        .unwrap();
    for (row, expected) in expected.iter().enumerate() {
        assert_eq!(
            valid
                .value(row)
                .then(|| values.bit_buffer_view().value(row)),
            *expected,
            "row {row}"
        );
    }
}

fn and(left: Option<bool>, right: Option<bool>) -> Option<bool> {
    match (left, right) {
        (Some(false), _) | (_, Some(false)) => Some(false),
        (Some(true), Some(true)) => Some(true),
        _ => None,
    }
}

fn or(left: Option<bool>, right: Option<bool>) -> Option<bool> {
    match (left, right) {
        (Some(true), _) | (_, Some(true)) => Some(true),
        (Some(false), Some(false)) => Some(false),
        _ => None,
    }
}

#[test]
fn pure_predicate_blocks_preserve_nullable_truth_tables_slices_and_tails() {
    let session = ResidentVortexSession::new(8 << 20, 1).unwrap();
    for rows in [0, 1, 9, 63, 64, 65, 2047, 2048, 2049] {
        let domain = [Some(-1_i64), Some(1), None];
        let a: Vec<_> = (0..rows + 3).map(|row| domain[(row / 3) % 3]).collect();
        let b: Vec<_> = (0..rows + 3).map(|row| domain[row % 3]).collect();
        let input = batch(&[
            (
                "a",
                PrimitiveArray::from_option_iter(a.iter().copied()).into_array(),
            ),
            (
                "b",
                PrimitiveArray::from_option_iter(b.iter().copied()).into_array(),
            ),
        ])
        .slice(3..rows + 3)
        .unwrap();
        for op in [BinaryOp::And, BinaryOp::Or] {
            for negate in [false, true] {
                let expr = binary(
                    cmp("a", ComparisonOp::Gt, 0, true, false),
                    op,
                    cmp("b", ComparisonOp::Gt, 0, true, false),
                );
                let expr = if negate { not(expr) } else { expr };
                let expected: Vec<_> = (3..rows + 3)
                    .map(|row| {
                        let a = a[row].map(|x| x > 0);
                        let b = b[row].map(|x| x > 0);
                        let result = if op == BinaryOp::And {
                            and(a, b)
                        } else {
                            or(a, b)
                        };
                        if negate { result.map(|x| !x) } else { result }
                    })
                    .collect();
                check(&expr, &input, &expected, &session);
            }
        }
        assert_eq!(session.memory().snapshot().reserved_bytes, 0);
    }
}

#[test]
fn pure_predicate_blocks_compare_i64_edges_in_both_operand_orders() {
    let session = ResidentVortexSession::new(8 << 20, 1).unwrap();
    let values = [i64::MIN, i64::MIN + 1, -1, 0, 1, i64::MAX - 1, i64::MAX];
    for nullable in [false, true] {
        let mut optional: Vec<_> = values.into_iter().map(Some).collect();
        let array = if nullable {
            optional.push(None);
            PrimitiveArray::from_option_iter(optional.iter().copied()).into_array()
        } else {
            PrimitiveArray::from_iter(values).into_array()
        };
        let input = batch(&[("a", array)]);
        for op in [
            ComparisonOp::Eq,
            ComparisonOp::NotEq,
            ComparisonOp::Lt,
            ComparisonOp::LtEq,
            ComparisonOp::Gt,
            ComparisonOp::GtEq,
        ] {
            for literal in [i64::MIN, -1, 0, 1, i64::MAX] {
                for reversed in [false, true] {
                    let expr = binary(
                        cmp("a", op, literal, nullable, reversed),
                        BinaryOp::Or,
                        cmp("a", op, literal, nullable, reversed),
                    );
                    let expected: Vec<_> = optional
                        .iter()
                        .map(|value| {
                            value.map(|value| {
                                let (a, b) = if reversed {
                                    (literal, value)
                                } else {
                                    (value, literal)
                                };
                                match op {
                                    ComparisonOp::Eq => a == b,
                                    ComparisonOp::NotEq => a != b,
                                    ComparisonOp::Lt => a < b,
                                    ComparisonOp::LtEq => a <= b,
                                    ComparisonOp::Gt => a > b,
                                    ComparisonOp::GtEq => a >= b,
                                }
                            })
                        })
                        .collect();
                    let recipe = Recipe::compile(&expr).unwrap();
                    assert_eq!(recipe.column_count, 1);
                    assert_eq!(recipe.instruction_count, 2);
                    check(&expr, &input, &expected, &session);
                }
            }
        }
    }
    assert_eq!(session.memory().snapshot().reserved_bytes, 0);
}

fn dictionary(rows: usize) -> ArrayRef {
    DictArray::try_new(
        PrimitiveArray::from_iter((0..rows).map(|row| u8::try_from(row % 5).unwrap())).into_array(),
        PrimitiveArray::from_option_iter([
            Some(i64::MIN),
            Some(-1_i64),
            Some(0),
            Some(i64::MAX),
            None,
        ])
        .into_array(),
    )
    .unwrap()
    .into_array()
}

#[test]
fn pure_predicate_blocks_use_existing_constant_dictionary_and_chunk_owners() {
    let session = ResidentVortexSession::new(8 << 20, 1).unwrap();
    let rows = 137;
    let dict = dictionary(rows + 3).slice(3..rows + 3).unwrap();
    let chunked = ChunkedArray::try_new(
        vec![dict.slice(0..31).unwrap(), dict.slice(31..rows).unwrap()],
        dict.dtype().clone(),
    )
    .unwrap()
    .into_array();
    let choices = [Some(i64::MIN), Some(-1), Some(0), Some(i64::MAX), None];
    let expr = binary(
        cmp("a", ComparisonOp::Gt, -1, true, false),
        BinaryOp::And,
        not(cmp("a", ComparisonOp::Eq, i64::MAX, true, false)),
    );
    let expected: Vec<_> = (3..rows + 3)
        .map(|row| choices[row % 5].map(|v| v > -1 && v != i64::MAX))
        .collect();
    for array in [dict, chunked] {
        check(&expr, &batch(&[("a", array)]), &expected, &session);
    }
    for value in [Some(-1), Some(0), Some(i64::MAX), None] {
        let scalar = value.map_or_else(
            || Scalar::null(DType::Primitive(PType::I64, Nullability::Nullable)),
            |value| Scalar::primitive(value, Nullability::Nullable),
        );
        check(
            &expr,
            &batch(&[("a", ConstantArray::new(scalar, rows).into_array())]),
            &vec![value.map(|v| v > -1 && v != i64::MAX); rows],
            &session,
        );
    }
    assert_eq!(session.memory().snapshot().reserved_bytes, 0);
}

fn tree(depth: usize) -> Expression {
    if depth == 0 {
        cmp("a", ComparisonOp::Eq, 0, true, false)
    } else {
        binary(tree(depth - 1), BinaryOp::Or, tree(depth - 1))
    }
}

fn chain(count: usize, distinct_columns: bool) -> Expression {
    let mut expr = cmp("a0", ComparisonOp::Eq, 0, true, false);
    for index in 1..count {
        let name = if distinct_columns {
            format!("a{index}")
        } else {
            "a0".into()
        };
        expr = binary(
            expr,
            BinaryOp::And,
            cmp(
                &name,
                ComparisonOp::Eq,
                i64::try_from(index).unwrap(),
                true,
                false,
            ),
        );
    }
    expr
}

#[test]
fn pure_predicate_blocks_bound_recipe_work_and_reuse_only_identical_nodes() {
    assert!(Recipe::compile(&chain(16, false)).is_some());
    assert!(Recipe::compile(&chain(17, false)).is_none());
    assert!(Recipe::compile(&chain(8, true)).is_some());
    assert!(Recipe::compile(&chain(9, true)).is_none());
    let within = tree(5);
    let recipe = Recipe::compile(&within).unwrap();
    assert_eq!(recipe.visits, 63);
    assert_eq!(recipe.instruction_count, 6);
    assert!(Recipe::compile(&tree(6)).is_none());
    assert!(Recipe::compile(&cmp("a", ComparisonOp::Eq, 0, true, false)).is_none());
    let ordered = binary(
        binary(
            cmp("a", ComparisonOp::Lt, 0, true, false),
            BinaryOp::And,
            cmp("a", ComparisonOp::Gt, 0, true, false),
        ),
        BinaryOp::Or,
        binary(
            cmp("a", ComparisonOp::Gt, 0, true, false),
            BinaryOp::And,
            cmp("a", ComparisonOp::Lt, 0, true, false),
        ),
    );
    assert_eq!(Recipe::compile(&ordered).unwrap().instruction_count, 5);
}

fn divided_compare() -> Expression {
    comparison(
        Expression {
            dtype: col("a", true).dtype,
            kind: Kind::Binary(Box::new(col("a", true)), BinaryOp::Divide, Box::new(lit(0))),
        },
        ComparisonOp::Gt,
        lit(0),
    )
}

#[test]
fn pure_predicate_blocks_reject_outside_typed_column_literal_contract() {
    let nullable_i64 = DType::Primitive(PType::I64, Nullability::Nullable);
    let null_literal = Expression {
        dtype: nullable_i64.clone(),
        kind: Kind::Literal(Scalar::null(nullable_i64)),
    };
    let null_comparison = comparison(col("a", true), ComparisonOp::Eq, null_literal);
    let column_comparison = comparison(col("a", true), ComparisonOp::Eq, col("b", true));
    let i32_column = Expression {
        dtype: DType::Primitive(PType::I32, Nullability::NonNullable),
        kind: Kind::Column("c".into()),
    };
    let i32_literal = Expression {
        dtype: DType::Primitive(PType::I32, Nullability::NonNullable),
        kind: Kind::Literal(Scalar::from(1_i32)),
    };
    let i32_comparison = comparison(i32_column, ComparisonOp::Eq, i32_literal);
    let bool_column = Expression {
        dtype: DType::Bool(Nullability::Nullable),
        kind: Kind::Column("flag".into()),
    };
    let mismatched_literal = Expression {
        dtype: DType::Primitive(PType::I64, Nullability::NonNullable),
        kind: Kind::Literal(Scalar::primitive(1_i64, Nullability::Nullable)),
    };
    let mismatched_comparison = comparison(col("a", true), ComparisonOp::Eq, mismatched_literal);
    for other in [
        null_comparison,
        column_comparison,
        i32_comparison,
        bool_column,
        mismatched_comparison,
    ] {
        assert!(
            Recipe::compile(&binary(
                cmp("a", ComparisonOp::Eq, 1, true, false),
                BinaryOp::Or,
                other,
            ))
            .is_none()
        );
    }
    assert!(Recipe::compile(&not(cmp("a", ComparisonOp::Eq, 1, true, false))).is_none());
}

#[test]
fn pure_predicate_blocks_leave_fallible_and_lazy_branches_in_existing_evaluation() {
    let session = ResidentVortexSession::new(8 << 20, 1).unwrap();
    let input = batch(&[(
        "a",
        PrimitiveArray::from_option_iter([Some(1_i64), Some(-1), None]).into_array(),
    )]);
    let rejected = binary(
        cmp("a", ComparisonOp::Eq, 1, true, false),
        BinaryOp::And,
        divided_compare(),
    );
    assert!(Recipe::compile(&rejected).is_none());
    let safe = binary(
        cmp("a", ComparisonOp::Eq, 1, true, false),
        BinaryOp::Or,
        cmp("a", ComparisonOp::Gt, 0, true, false),
    );
    let condition = Expression {
        dtype: DType::Bool(Nullability::NonNullable),
        kind: Kind::Literal(Scalar::from(true)),
    };
    let selected = Expression {
        dtype: safe.dtype.clone(),
        kind: Kind::Conditional(
            Box::new(condition),
            Box::new(safe),
            Box::new(divided_compare()),
        ),
    };
    assert!(Recipe::compile(&selected).is_none());
    let output = session
        .with_native_execution_context(&CancellationToken::default(), |context| {
            selected.evaluate(&input, context)
        })
        .unwrap();
    let reference = binary(
        cmp("a", ComparisonOp::Gt, 0, true, false),
        BinaryOp::Or,
        cmp("a", ComparisonOp::Gt, 0, true, false),
    );
    check(
        &reference,
        &input,
        &[Some(true), Some(false), None],
        &session,
    );
    let mut execution = vortex::session::VortexSession::default().create_execution_ctx();
    let actual = output.clone().execute::<BoolArray>(&mut execution).unwrap();
    assert!(actual.bit_buffer_view().value(0));
    assert!(!actual.bit_buffer_view().value(1));
    assert!(
        !actual
            .validity()
            .unwrap()
            .execute_mask(3, &mut execution)
            .unwrap()
            .value(2)
    );
    assert!(
        session
            .with_native_execution_context(&CancellationToken::default(), |context| rejected
                .evaluate(&input, context))
            .is_err()
    );
    drop((actual, output));
    assert_eq!(session.memory().snapshot().reserved_bytes, 0);
}

#[test]
fn pure_predicate_blocks_keep_output_credits_through_clones_and_release_partial_failures() {
    let session = ResidentVortexSession::new(8 << 20, 1).unwrap();
    let memory = session.memory().clone();
    let input = batch(&[("a", dictionary(4097))]);
    let expr = binary(
        cmp("a", ComparisonOp::Gt, -1, true, false),
        BinaryOp::Or,
        cmp("a", ComparisonOp::Eq, i64::MIN, true, false),
    );
    let output = session
        .with_native_execution_context(&CancellationToken::default(), |context| {
            expr.evaluate(&input, context)
        })
        .unwrap();
    let slice = output.slice(3..4096).unwrap();
    let clone = slice.clone();
    drop((slice, output, input, session));
    assert!(memory.snapshot().reserved_bytes > 0);
    drop(clone);
    assert_eq!(memory.snapshot().reserved_bytes, 0);

    let session = ResidentVortexSession::new(8 << 20, 1).unwrap();
    let source = session
        .with_native_execution_context(&CancellationToken::default(), |context| {
            crate::local_primitives::result_batch::build_column(
                &DType::Primitive(PType::I64, Nullability::Nullable),
                4097,
                &context.native_session().allocator(),
                |row| Ok(Value::Int(i64::try_from(row).unwrap())),
            )
        })
        .unwrap();
    let input = batch(&[("a", source)]);
    let retained = session.memory().snapshot().reserved_bytes;
    assert!(retained > 0);
    let missing = binary(
        cmp("a", ComparisonOp::Gt, 0, true, false),
        BinaryOp::And,
        cmp("missing", ComparisonOp::Gt, 0, true, false),
    );
    assert!(
        session
            .with_native_execution_context(&CancellationToken::default(), |context| missing
                .evaluate(&input, context))
            .is_err()
    );
    assert_eq!(session.memory().snapshot().reserved_bytes, retained);
    drop(input);
    assert_eq!(session.memory().snapshot().reserved_bytes, 0);
}

#[test]
fn pure_predicate_blocks_deny_before_output_growth_and_honor_cancellation() {
    let rows = 4096_usize;
    let session = ResidentVortexSession::new(1 << 20, 1).unwrap();
    let memory = session.memory().clone();
    let input = batch(&[(
        "a",
        PrimitiveArray::from_option_iter((0..rows).map(|row| Some(i64::try_from(row).unwrap())))
            .into_array(),
    )]);
    let expr = binary(
        cmp("a", ComparisonOp::Gt, 0, true, false),
        BinaryOp::Or,
        cmp("a", ComparisonOp::Eq, 0, true, false),
    );
    for available in [0, 576] {
        let block = memory.reserve((1 << 20) - available).unwrap();
        let prior = memory.snapshot();
        assert!(
            session
                .with_native_execution_context(&CancellationToken::default(), |context| expr
                    .evaluate(&input, context))
                .is_err()
        );
        assert_eq!(memory.snapshot().reserved_bytes, prior.reserved_bytes);
        assert!(memory.snapshot().denied_reservations > prior.denied_reservations);
        drop(block);
        assert_eq!(memory.snapshot().reserved_bytes, 0);
    }
    let cancellation = CancellationToken::default();
    let result = session.with_native_execution_context(&cancellation, |context| {
        cancellation.cancel();
        expr.evaluate(&input, context)
    });
    assert!(result.is_err());
    assert_eq!(memory.snapshot().reserved_bytes, 0);
    check(&expr, &input, &vec![Some(true); rows], &session);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}
