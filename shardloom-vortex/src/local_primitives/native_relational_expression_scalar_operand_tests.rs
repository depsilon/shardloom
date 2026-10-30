use super::*;
use shardloom_core::FloatPolicy;

fn original(
    expression: &Expression,
    input: &ArrayRef,
    context: &NativeExecutionContext<'_>,
) -> Result<ArrayRef> {
    let Kind::Binary(left, op, right) = &expression.kind else {
        unreachable!()
    };
    let left_values = keys(&left.evaluate(input, context)?, context)?;
    let right_values = keys(&right.evaluate(input, context)?, context)?;
    if let Some(output) = binary(&left_values, *op, &right_values, &expression.dtype, context)? {
        return Ok(output);
    }
    expression.build(input.len(), context, |row| {
        scalar_binary(
            left_values.raw_cell(row)?,
            *op,
            right_values.raw_cell(row)?,
            &left.dtype,
            &right.dtype,
            &expression.dtype,
            context.float_policy(),
        )
    })
}

fn agrees(
    source: &ArrayRef,
    value: &Scalar,
    op: BinaryOp,
    output: PType,
    policy: FloatPolicy,
    admitted: bool,
) {
    let dtype = DType::Primitive(
        output,
        if source.dtype().is_nullable() || value.dtype().is_nullable() {
            Nullability::Nullable
        } else {
            Nullability::NonNullable
        },
    );
    for literal_left in [false, true] {
        let source_expression = column("value", source.dtype());
        let literal_expression = literal(value.clone());
        let (left, right) = if literal_left {
            (literal_expression, source_expression)
        } else {
            (source_expression, literal_expression)
        };
        let expression = Expression::new(
            dtype.clone(),
            Kind::Binary(Box::new(left), op, Box::new(right)),
        );
        let input = StructArray::from_fields(&[("value", source.clone())])
            .unwrap()
            .into_array();
        let session = ResidentVortexSession::new_with_float_policy(8 << 20, 1, policy).unwrap();
        let memory = session.memory().clone();
        session
            .with_native_execution_context(&CancellationToken::default(), |context| {
                let before = memory.snapshot().reserved_bytes;
                let expected = original(&expression, &input, context);
                #[cfg(debug_assertions)]
                let observation =
                    crate::local_primitives::native_relational_observation::Session::start();
                let actual = expression.evaluate(&input, context);
                #[cfg(debug_assertions)]
                {
                    let counts = observation.finish();
                    assert_eq!(
                        counts.expression_numeric_scalar_batches,
                        u64::from(admitted)
                    );
                    if admitted && actual.is_ok() {
                        assert_eq!(counts.expression_numeric_scalar_rows, input.len() as u64);
                        assert_eq!(counts.expression_numeric_binary_batches, 0);
                    }
                }
                assert_eq!(
                    actual.as_ref().err().map(ToString::to_string),
                    expected.as_ref().err().map(ToString::to_string)
                );
                if let (Ok(actual), Ok(expected)) = (actual, expected) {
                    assert_same(&actual, &expected, context);
                }
                assert_eq!(memory.snapshot().reserved_bytes, before);
                Ok(())
            })
            .unwrap();
        drop((input, session));
        assert_eq!(memory.snapshot().reserved_bytes, 0);
    }
}

const OPERATIONS: [BinaryOp; 5] = [
    BinaryOp::Add,
    BinaryOp::Subtract,
    BinaryOp::Multiply,
    BinaryOp::Divide,
    BinaryOp::Remainder,
];

#[test]
fn prepared_scalar_operands_own_results_after_input_and_session_close() {
    for rows in [0, 3] {
        let producer = ResidentVortexSession::new(1 << 20, 1).unwrap();
        let receiver = ResidentVortexSession::new(1 << 20, 1).unwrap();
        let producer_memory = producer.memory().clone();
        let receiver_memory = receiver.memory().clone();
        let source = producer
            .with_native_execution_context(&CancellationToken::default(), |context| {
                result_batch::build_column(
                    &DType::Primitive(PType::I16, Nullability::Nullable),
                    rows,
                    &context.native_session().host_allocator(),
                    |row| {
                        Ok(match row {
                            0 => Value::Int(9),
                            1 => Value::Null,
                            _ => Value::Int(-1),
                        })
                    },
                )
            })
            .unwrap();
        let expression = Expression::new(
            DType::Primitive(PType::I64, Nullability::Nullable),
            Kind::Binary(
                Box::new(column("value", source.dtype())),
                BinaryOp::Multiply,
                Box::new(literal(Scalar::from(2i64))),
            ),
        );
        let input = StructArray::from_fields(&[("value", source)])
            .unwrap()
            .into_array();
        let output = receiver
            .with_native_execution_context(&CancellationToken::default(), |context| {
                expression.evaluate(&input, context)
            })
            .unwrap();
        drop((input, producer));
        assert_eq!(producer_memory.snapshot().reserved_bytes, 0);
        let retained = output.slice(0..rows).unwrap();
        drop((output, receiver));
        assert!(receiver_memory.snapshot().reserved_bytes > 0);
        let mut execution = vortex::array::array_session().create_execution_ctx();
        for (row, value) in [Some(18i64), None, Some(-2)]
            .into_iter()
            .take(rows)
            .enumerate()
        {
            assert_eq!(
                result_batch::scalar(&retained, row, &mut execution).unwrap(),
                value.map_or_else(
                    || Scalar::null(retained.dtype().clone()),
                    |value| Scalar::primitive(value, Nullability::Nullable)
                )
            );
        }
        drop((retained, execution));
        assert_eq!(receiver_memory.snapshot().reserved_bytes, 0);
    }
}

#[test]
fn prepared_scalar_operands_match_every_integer_width_operation_and_operand_side() {
    let sources = [
        PrimitiveArray::from_option_iter([Some(-3i8), None, Some(0), Some(4)]).into_array(),
        PrimitiveArray::from_option_iter([Some(-3i16), None, Some(0), Some(4)]).into_array(),
        PrimitiveArray::from_option_iter([Some(-3i32), None, Some(0), Some(4)]).into_array(),
        PrimitiveArray::from_option_iter([Some(-3i64), None, Some(0), Some(4)]).into_array(),
        PrimitiveArray::from_option_iter([Some(3u8), None, Some(0), Some(4)]).into_array(),
        PrimitiveArray::from_option_iter([Some(3u16), None, Some(0), Some(4)]).into_array(),
        PrimitiveArray::from_option_iter([Some(3u32), None, Some(0), Some(4)]).into_array(),
        PrimitiveArray::from_option_iter([Some(3u64), None, Some(0), Some(4)]).into_array(),
    ];
    for source in sources {
        for op in OPERATIONS {
            agrees(
                &source,
                &Scalar::from(-2i64),
                op,
                PType::I64,
                FloatPolicy::Strict,
                true,
            );
        }
    }
    for value in [
        Scalar::from(-2i8),
        Scalar::from(-2i16),
        Scalar::from(-2i32),
        Scalar::from(-2i64),
        Scalar::from(2u8),
        Scalar::from(2u16),
        Scalar::from(2u32),
        Scalar::from(2u64),
    ] {
        for op in OPERATIONS {
            agrees(
                &PrimitiveArray::from_option_iter([Some(-3i64), None, Some(0), Some(4)])
                    .into_array(),
                &value,
                op,
                PType::I64,
                FloatPolicy::Strict,
                true,
            );
        }
    }
    for op in OPERATIONS {
        agrees(
            &PrimitiveArray::from_iter([u64::MAX, 0, 3]).into_array(),
            &Scalar::from(2u64),
            op,
            PType::U64,
            FloatPolicy::Strict,
            true,
        );
        agrees(
            &PrimitiveArray::from_iter([i64::MIN, 0, i64::MAX]).into_array(),
            &Scalar::from(-1i64),
            op,
            PType::I64,
            FloatPolicy::Strict,
            true,
        );
    }
}

#[test]
fn prepared_scalar_operands_preserve_float_bits_policy_nulls_and_first_errors() {
    for policy in [FloatPolicy::Strict, FloatPolicy::Extended] {
        for op in OPERATIONS {
            for literal in [0.0, -0.0, 2.0, -3.5] {
                for values in [
                    vec![-0.0, 0.0, 3.5, -1.25, f64::MAX],
                    vec![0.0, f64::NAN, 1.0],
                    vec![f64::NAN, 0.0, 1.0],
                    vec![
                        f64::from_bits(0xfff8_0000_0000_1234),
                        f64::INFINITY,
                        f64::NEG_INFINITY,
                    ],
                ] {
                    agrees(
                        &PrimitiveArray::from_iter(values).into_array(),
                        &Scalar::from(literal),
                        op,
                        PType::F64,
                        policy,
                        true,
                    );
                }
                let source = PrimitiveArray::new(
                    Buffer::from(vec![f64::NAN, -0.0, 3.5]),
                    Validity::from_iter([false, true, true]),
                )
                .into_array();
                agrees(
                    &source,
                    &Scalar::from(literal),
                    op,
                    PType::F64,
                    policy,
                    true,
                );
            }
        }
        for value in [
            Scalar::from(f64::NAN),
            Scalar::from(f64::INFINITY),
            Scalar::null(DType::Primitive(PType::F64, Nullability::Nullable)),
        ] {
            agrees(
                &PrimitiveArray::from_option_iter([Some(1.0f64), None, Some(f64::NAN)])
                    .into_array(),
                &value,
                BinaryOp::Divide,
                PType::F64,
                policy,
                false,
            );
        }
    }
}

#[test]
fn prepared_scalar_operands_handle_empty_dictionary_selected_and_constant_sources() {
    let source = DictArray::try_new(
        PrimitiveArray::from_iter([2u8, 1, 0, 2, 0]).into_array(),
        PrimitiveArray::from_option_iter([Some(7u16), None, Some(u16::MAX)]).into_array(),
    )
    .unwrap()
    .into_array();
    for source in [
        source.clone(),
        source.slice(1..5).unwrap(),
        source.slice(0..0).unwrap(),
        ConstantArray::new(Scalar::from(-9i64), 2053).into_array(),
    ] {
        agrees(
            &source,
            &Scalar::from(-2i64),
            BinaryOp::Multiply,
            PType::I64,
            FloatPolicy::Strict,
            true,
        );
    }
    agrees(
        &PrimitiveArray::from_iter([1.0f32, -0.0]).into_array(),
        &Scalar::from(2.0f32),
        BinaryOp::Add,
        PType::F64,
        FloatPolicy::Strict,
        false,
    );
}

#[test]
fn prepared_scalar_operands_decline_before_evaluation_and_preserve_completed_child_reuse() {
    let dtype = DType::Primitive(PType::I64, Nullability::NonNullable);
    let input =
        StructArray::from_fields(&[("value", PrimitiveArray::from_iter([3i64, 4]).into_array())])
            .unwrap()
            .into_array();
    let session = ResidentVortexSession::new(1 << 20, 1).unwrap();
    session
        .with_native_execution_context(&CancellationToken::default(), |context| {
            for (value, op, reused) in [
                (
                    Scalar::null(DType::Primitive(PType::I64, Nullability::Nullable)),
                    BinaryOp::Add,
                    false,
                ),
                (Scalar::from(2i64), BinaryOp::And, false),
                (Scalar::from(2i64), BinaryOp::Add, true),
            ] {
                let missing = column("absent", &dtype);
                let mut value = literal(value);
                value.reuse_output = reused.then_some(999);
                let expression = Expression::new(dtype.clone(), Kind::Column("unused".into()));
                assert!(
                    scalar_operand(
                        &expression,
                        &input,
                        context,
                        Some(&[]),
                        &missing,
                        op,
                        &value
                    )?
                    .is_none()
                );
            }
            let mut source = column("absent", &dtype);
            source.reuse_output = Some(0);
            let expression = Expression::new(
                dtype.clone(),
                Kind::Binary(
                    Box::new(source),
                    BinaryOp::Multiply,
                    Box::new(literal(Scalar::from(2i64))),
                ),
            );
            let completed = [PrimitiveArray::from_iter([5i64, 7]).into_array()];
            let actual = expression.evaluate_reusing(&input, context, &completed)?;
            assert_same(
                &actual,
                &PrimitiveArray::from_iter([10i64, 14]).into_array(),
                context,
            );
            assert!(expression.evaluate_reusing(&input, context, &[]).is_err());
            Ok(())
        })
        .unwrap();
    assert_eq!(session.memory().snapshot().reserved_bytes, 0);
}

#[test]
fn prepared_scalar_operands_keep_lazy_branches_and_release_denied_or_cancelled_outputs() {
    let source = PrimitiveArray::from_option_iter([Some(0i64), Some(2), None]).into_array();
    let dtype = source.dtype().clone();
    let condition = Expression::new(
        DType::Bool(Nullability::NonNullable),
        Kind::Column("condition".into()),
    );
    let divide = Expression::new(
        dtype.clone(),
        Kind::Binary(
            Box::new(literal(Scalar::from(8i64))),
            BinaryOp::Divide,
            Box::new(column("value", &dtype)),
        ),
    );
    let expression = Expression::new(
        dtype.clone(),
        Kind::Conditional(
            Box::new(condition),
            Box::new(divide),
            Box::new(literal(Scalar::from(9i64))),
        ),
    );
    let input = StructArray::from_fields(&[
        ("value", source),
        (
            "condition",
            vortex::array::arrays::BoolArray::from_iter([false, true, true]).into_array(),
        ),
    ])
    .unwrap()
    .into_array();
    let session = ResidentVortexSession::new(1 << 20, 1).unwrap();
    let memory = session.memory().clone();
    session
        .with_native_execution_context(&CancellationToken::default(), |context| {
            let expected =
                PrimitiveArray::from_option_iter([Some(9i64), Some(4), None]).into_array();
            let actual = expression.evaluate(&input, context)?;
            assert_same(&actual, &expected, context);
            drop(actual);
            let before = memory.snapshot();
            let hold = memory.reserve(before.limit_bytes - before.reserved_bytes)?;
            let held = memory.snapshot().reserved_bytes;
            assert!(expression.evaluate(&input, context).is_err());
            assert_eq!(memory.snapshot().reserved_bytes, held);
            drop(hold);
            drop(expression.evaluate(&input, context)?);
            assert_eq!(memory.snapshot().reserved_bytes, before.reserved_bytes);
            Ok(())
        })
        .unwrap();
    let cancellation = CancellationToken::default();
    cancellation.cancel();
    assert!(
        session
            .with_native_execution_context(&cancellation, |context| expression
                .evaluate(&input, context))
            .is_err()
    );
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}
