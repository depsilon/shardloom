use super::*;
use crate::local_primitives::{
    native_relational_expression::binary_with_float_policy as scalar_binary, result_batch::Value,
};
use crate::resident_session::ResidentVortexSession;
use shardloom_exec::compute_pool::CancellationToken;
use vortex::array::{
    IntoArray as _, VortexSessionExecute as _,
    arrays::{ConstantArray, DecimalArray, DictArray, ExtensionArray, PrimitiveArray, StructArray},
    dtype::{DecimalDType, Nullability},
    extension::datetime::{Date, TimeUnit},
    scalar::Scalar,
    validity::Validity,
};
use vortex::buffer::Buffer;

#[path = "native_relational_expression_scalar_operand_tests.rs"]
mod scalar_operand_tests;

fn column(name: &str, dtype: &DType) -> Expression {
    Expression::new(dtype.clone(), Kind::Column(name.into()))
}

fn literal(value: Scalar) -> Expression {
    Expression::new(value.dtype().clone(), Kind::Literal(value))
}

fn assert_same(actual: &ArrayRef, expected: &ArrayRef, context: &NativeExecutionContext<'_>) {
    assert_eq!(actual.dtype(), expected.dtype());
    assert_eq!(actual.len(), expected.len());
    let mut execution = context.native_session().create_execution_ctx();
    if !matches!(actual.dtype(), DType::Primitive(..)) {
        for row in 0..actual.len() {
            assert_eq!(
                actual.execute_scalar(row, &mut execution).unwrap(),
                expected.execute_scalar(row, &mut execution).unwrap()
            );
        }
        return;
    }
    let actual = actual
        .clone()
        .execute::<PrimitiveArray>(&mut execution)
        .unwrap();
    let expected = expected
        .clone()
        .execute::<PrimitiveArray>(&mut execution)
        .unwrap();
    // Include signed zero, exact integer bits and initialized NULL slots.
    assert_eq!(
        actual.buffer_handle().as_host(),
        expected.buffer_handle().as_host()
    );
    let actual_valid = actual
        .validity()
        .unwrap()
        .execute_mask(actual.len(), &mut execution)
        .unwrap();
    let expected_valid = expected
        .validity()
        .unwrap()
        .execute_mask(expected.len(), &mut execution)
        .unwrap();
    for row in 0..actual.len() {
        assert_eq!(actual_valid.value(row), expected_valid.value(row));
    }
}

fn binary_agrees(left: ArrayRef, op: BinaryOp, right: ArrayRef, dtype: DType, admitted: bool) {
    let session = ResidentVortexSession::new(8 << 20, 1).unwrap();
    let memory = session.memory().clone();
    session
        .with_native_execution_context(&CancellationToken::default(), |context| {
            let left_values = keys(&left, context)?;
            let right_values = keys(&right, context)?;
            let before = memory.snapshot().reserved_bytes;
            let expected = result_batch::build_column(
                &dtype,
                left.len(),
                &context.native_session().host_allocator(),
                |row| {
                    if row.is_multiple_of(1024) {
                        context.check_cancelled()?;
                    }
                    scalar_binary(
                        left_values.raw_cell(row)?,
                        op,
                        right_values.raw_cell(row)?,
                        left.dtype(),
                        right.dtype(),
                        &dtype,
                        context.float_policy(),
                    )
                },
            );
            #[cfg(debug_assertions)]
            let observation =
                crate::local_primitives::native_relational_observation::Session::start();
            let actual = binary(&left_values, op, &right_values, &dtype, context);
            #[cfg(debug_assertions)]
            {
                let counts = observation.finish();
                assert_eq!(
                    counts.expression_numeric_binary_batches,
                    u64::from(admitted)
                );
                if admitted && actual.is_ok() {
                    assert_eq!(counts.expression_numeric_binary_rows, left.len() as u64);
                }
            }
            if admitted {
                assert_eq!(
                    actual.as_ref().err().map(ToString::to_string),
                    expected.as_ref().err().map(ToString::to_string)
                );
                if let (Ok(Some(actual)), Ok(expected)) = (actual, expected) {
                    assert_same(&actual, &expected, context);
                }
            } else {
                assert!(actual.unwrap().is_none());
                drop(expected);
            }
            assert_eq!(memory.snapshot().reserved_bytes, before);
            Ok(())
        })
        .unwrap();
    drop((left, right, dtype));
    drop(session);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

#[test]
fn numeric_expression_binary_matches_original_operations_nulls_and_exact_bits() {
    for op in [
        BinaryOp::Add,
        BinaryOp::Subtract,
        BinaryOp::Multiply,
        BinaryOp::Divide,
        BinaryOp::Remainder,
    ] {
        for (left, right) in [
            (
                PrimitiveArray::from_option_iter([Some(-12_i64), None, Some(0), Some(24), None])
                    .into_array(),
                PrimitiveArray::from_option_iter([Some(3_i64), Some(0), None, Some(-2), None])
                    .into_array(),
            ),
            (
                PrimitiveArray::from_option_iter([
                    Some(12_u64),
                    None,
                    Some(0),
                    Some(u64::MAX - 3),
                    None,
                ])
                .into_array(),
                PrimitiveArray::from_option_iter([Some(3_u64), Some(0), None, Some(2), None])
                    .into_array(),
            ),
            (
                PrimitiveArray::from_option_iter([
                    Some(-0.0_f64),
                    None,
                    Some(0.0),
                    Some(f64::from_bits(1)),
                    Some(-12.5),
                ])
                .into_array(),
                PrimitiveArray::from_option_iter([
                    Some(2.0_f64),
                    Some(0.0),
                    Some(-2.0),
                    Some(0.5),
                    Some(2.5),
                ])
                .into_array(),
            ),
        ] {
            let dtype = left.dtype().clone();
            binary_agrees(left, op, right, dtype, true);
        }
    }
    for rows in [0_usize, 1, 2053] {
        let left = PrimitiveArray::from_option_iter(
            (0..rows).map(|row| (row % 3 != 1).then_some(i64::try_from(row).unwrap() - 19)),
        )
        .into_array();
        let right = ConstantArray::new(Scalar::from(7_i64), rows).into_array();
        let dtype = left.dtype().clone();
        binary_agrees(left, BinaryOp::Add, right, dtype, true);
    }
    let domain = PrimitiveArray::from_option_iter([Some(7_i64), None, Some(-9)]).into_array();
    let dictionary = DictArray::try_new(
        PrimitiveArray::from_iter([2_u8, 1, 0, 2, 0]).into_array(),
        domain,
    )
    .unwrap()
    .into_array();
    let selected = dictionary.slice(1..5).unwrap();
    let dtype = selected.dtype().clone();
    binary_agrees(
        selected,
        BinaryOp::Multiply,
        ConstantArray::new(Scalar::from(-2_i64), 4).into_array(),
        dtype,
        true,
    );
}

#[test]
fn numeric_expression_binary_preserves_overflow_nonfinite_and_first_error() {
    for (left, op, right) in [
        (i64::MAX, BinaryOp::Add, 1),
        (i64::MIN, BinaryOp::Divide, -1),
        (i64::MIN, BinaryOp::Remainder, -1),
        (1, BinaryOp::Divide, 0),
        (i64::MIN, BinaryOp::Multiply, -1),
    ] {
        let left = PrimitiveArray::from_iter([12_i64, left]).into_array();
        let dtype = left.dtype().clone();
        binary_agrees(
            left,
            op,
            PrimitiveArray::from_iter([2_i64, right]).into_array(),
            dtype,
            true,
        );
    }
    for (left, op, right) in [
        (u64::MAX, BinaryOp::Add, 1),
        (0, BinaryOp::Subtract, 1),
        (u64::MAX, BinaryOp::Multiply, u64::MAX),
    ] {
        let left = PrimitiveArray::from_iter([left]).into_array();
        let dtype = left.dtype().clone();
        binary_agrees(
            left,
            op,
            PrimitiveArray::from_iter([right]).into_array(),
            dtype,
            true,
        );
    }
    for invalid in [f64::NAN, f64::INFINITY, f64::NEG_INFINITY] {
        for (left, right) in [(Some(invalid), None), (None, Some(invalid))] {
            let left = PrimitiveArray::from_option_iter([left, Some(1.0)]).into_array();
            let dtype = left.dtype().clone();
            binary_agrees(
                left,
                BinaryOp::Divide,
                PrimitiveArray::from_option_iter([right, Some(0.0)]).into_array(),
                dtype,
                true,
            );
        }
        for (left, right) in [([1.0, invalid], [0.0, 1.0]), ([invalid, 1.0], [1.0, 0.0])] {
            let left = PrimitiveArray::from_iter(left).into_array();
            let dtype = left.dtype().clone();
            binary_agrees(
                left,
                BinaryOp::Divide,
                PrimitiveArray::from_iter(right).into_array(),
                dtype,
                true,
            );
        }
    }
    let left = PrimitiveArray::from_iter([f64::MAX]).into_array();
    let dtype = left.dtype().clone();
    binary_agrees(
        left,
        BinaryOp::Multiply,
        PrimitiveArray::from_iter([2.0]).into_array(),
        dtype,
        true,
    );
    // Invalid physical payload under a NULL operand remains unobserved.
    let left = PrimitiveArray::new(
        Buffer::from(vec![f64::NAN, -0.0]),
        Validity::from_iter([false, true]),
    )
    .into_array();
    let dtype = left.dtype().clone();
    binary_agrees(
        left,
        BinaryOp::Add,
        PrimitiveArray::from_iter([3.0, -0.0]).into_array(),
        dtype,
        true,
    );
}

#[test]
fn numeric_expression_binary_admits_integer_widths_and_preserves_public_evaluation() {
    for left in [
        PrimitiveArray::from_iter([1_i32, 2]).into_array(),
        PrimitiveArray::from_iter([1.0_f32, 2.0]).into_array(),
    ] {
        let right = PrimitiveArray::from_iter([3_i64, 4]).into_array();
        let dtype = DType::Primitive(
            if matches!(left.dtype(), DType::Primitive(PType::F32, _)) {
                PType::F64
            } else {
                PType::I64
            },
            Nullability::NonNullable,
        );
        binary_agrees(
            left.clone(),
            BinaryOp::Add,
            right.clone(),
            dtype.clone(),
            !matches!(left.dtype(), DType::Primitive(PType::F32, _)),
        );
        let expression = Expression::new(
            dtype,
            Kind::Binary(
                Box::new(column("left", left.dtype())),
                BinaryOp::Add,
                Box::new(column("right", right.dtype())),
            ),
        );
        let input = StructArray::from_fields(&[("left", left), ("right", right)])
            .unwrap()
            .into_array();
        let session = ResidentVortexSession::new(1 << 20, 1).unwrap();
        session
            .with_native_execution_context(&CancellationToken::default(), |context| {
                let output = expression.evaluate(&input, context)?;
                let expected = expression.build(2, context, |row| {
                    Ok(
                        if matches!(expression.dtype, DType::Primitive(PType::F64, _)) {
                            Value::Float(if row == 0 { 4.0 } else { 6.0 })
                        } else {
                            Value::Int(if row == 0 { 4 } else { 6 })
                        },
                    )
                })?;
                assert_same(&output, &expected, context);
                Ok(())
            })
            .unwrap();
    }
}

fn integer_column(ptype: PType, values: &[Option<i64>]) -> ArrayRef {
    match_each_integer_ptype!(ptype, |T| {
        PrimitiveArray::from_option_iter(
            values
                .iter()
                .map(|value| value.map(|value| T::try_from(value).unwrap())),
        )
        .into_array()
    })
}

#[test]
fn numeric_expression_mixed_integer_widths_match_every_pair_operation_and_null_domain() {
    let widths = [
        PType::I8,
        PType::I16,
        PType::I32,
        PType::I64,
        PType::U8,
        PType::U16,
        PType::U32,
        PType::U64,
    ];
    for left_width in widths {
        for right_width in widths {
            for (left, right) in [
                (
                    &[Some(12), Some(0), Some(24)][..],
                    &[Some(3), Some(2), Some(4)][..],
                ),
                (
                    &[Some(12), None, Some(0), Some(24), None][..],
                    &[Some(3), Some(0), None, Some(2), None][..],
                ),
                (&[None, None][..], &[Some(0), None][..]),
                (&[][..], &[][..]),
            ] {
                for output in [PType::I64, PType::U64] {
                    for op in [
                        BinaryOp::Add,
                        BinaryOp::Subtract,
                        BinaryOp::Multiply,
                        BinaryOp::Divide,
                        BinaryOp::Remainder,
                    ] {
                        binary_agrees(
                            integer_column(left_width, left),
                            op,
                            integer_column(right_width, right),
                            DType::Primitive(output, Nullability::Nullable),
                            true,
                        );
                    }
                }
            }
        }
    }
}

#[test]
fn numeric_expression_mixed_integer_widths_preserve_overflow_and_first_error() {
    let signed = DType::Primitive(PType::I64, Nullability::Nullable);
    let unsigned = DType::Primitive(PType::U64, Nullability::Nullable);
    for op in [
        BinaryOp::Add,
        BinaryOp::Subtract,
        BinaryOp::Multiply,
        BinaryOp::Divide,
        BinaryOp::Remainder,
    ] {
        for dtype in [&signed, &unsigned] {
            for (left, right) in [
                (
                    PrimitiveArray::from_iter([i64::MIN, i64::MAX]).into_array(),
                    PrimitiveArray::from_iter([-1_i8, 1]).into_array(),
                ),
                (
                    PrimitiveArray::from_iter([u64::MAX, 0]).into_array(),
                    PrimitiveArray::from_iter([1_i16, -1]).into_array(),
                ),
                (
                    PrimitiveArray::from_iter([0_i32, -1]).into_array(),
                    PrimitiveArray::from_iter([u64::MAX, 0]).into_array(),
                ),
                (
                    PrimitiveArray::from_iter([u64::MAX]).into_array(),
                    PrimitiveArray::from_iter([u64::MAX]).into_array(),
                ),
                (
                    PrimitiveArray::from_option_iter([Some(u64::MAX), None, Some(1)]).into_array(),
                    PrimitiveArray::from_option_iter([None, Some(0_i8), Some(0)]).into_array(),
                ),
            ] {
                binary_agrees(left, op, right, dtype.clone(), true);
            }
        }
    }
    // Distinguish result-width overflow from division by zero in both orders.
    for (left, right) in [([u64::MAX, 1], [1_i32, 0]), ([1, u64::MAX], [0_i32, 1])] {
        binary_agrees(
            PrimitiveArray::from_iter(left).into_array(),
            BinaryOp::Divide,
            PrimitiveArray::from_iter(right).into_array(),
            signed.clone(),
            true,
        );
    }
    // NULL output refusal occurs at the same first row as the scalar builder.
    binary_agrees(
        PrimitiveArray::from_option_iter([None, Some(u16::MAX)]).into_array(),
        BinaryOp::Add,
        PrimitiveArray::from_iter([0_i64, i64::MAX]).into_array(),
        DType::Primitive(PType::I64, Nullability::NonNullable),
        true,
    );
}

#[test]
fn numeric_expression_mixed_integer_widths_keep_encoded_selected_values_and_cancellation_batches() {
    let source = DictArray::try_new(
        PrimitiveArray::from_iter([2_u8, 1, 0, 2, 0]).into_array(),
        PrimitiveArray::from_option_iter([Some(7_u16), None, Some(u16::MAX)]).into_array(),
    )
    .unwrap()
    .into_array();
    binary_agrees(
        source.slice(1..5).unwrap(),
        BinaryOp::Add,
        ConstantArray::new(Scalar::from(-2_i64), 4).into_array(),
        DType::Primitive(PType::I64, Nullability::Nullable),
        true,
    );
    for rows in [0_usize, 1, 2053] {
        let left = PrimitiveArray::from_option_iter(
            (0..rows).map(|row| (row % 3 != 1).then_some(u16::try_from(row).unwrap())),
        )
        .into_array();
        binary_agrees(
            left,
            BinaryOp::Multiply,
            ConstantArray::new(Scalar::from(-3_i64), rows).into_array(),
            DType::Primitive(PType::I64, Nullability::Nullable),
            true,
        );
    }
}

fn coalesce_agrees(source: ArrayRef, value: Scalar, dtype: DType, admitted: bool) {
    coalesce_literal_agrees(source, literal(value), dtype, admitted);
}

fn coalesce_literal_agrees(source: ArrayRef, value: Expression, dtype: DType, admitted: bool) {
    let expression = Expression::new(
        dtype,
        Kind::Coalesce(vec![column("value", source.dtype()), value]),
    );
    let input = StructArray::from_fields(&[("value", source)])
        .unwrap()
        .into_array();
    let Kind::Coalesce(children) = &expression.kind else {
        unreachable!()
    };
    let session = ResidentVortexSession::new(8 << 20, 1).unwrap();
    let memory = session.memory().clone();
    session
        .with_native_execution_context(&CancellationToken::default(), |context| {
            let before = memory.snapshot().reserved_bytes;
            let expected = expression.coalesce(&input, context, children);
            #[cfg(debug_assertions)]
            let observation =
                crate::local_primitives::native_relational_observation::Session::start();
            let actual = expression.evaluate(&input, context);
            #[cfg(debug_assertions)]
            {
                let counts = observation.finish();
                assert_eq!(
                    counts.expression_numeric_coalesce_batches,
                    u64::from(admitted)
                );
                if admitted && actual.is_ok() {
                    assert_eq!(counts.expression_numeric_coalesce_rows, input.len() as u64);
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
    drop(session);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

#[test]
fn numeric_expression_literal_coalesce_preserves_values_masks_and_output_bits() {
    for rows in [0, 1, 2053] {
        for present in [0, 1, 3] {
            let values = (0..rows)
                .map(|row| (present != 0 && row % present != 1).then_some(i64::from(row) - 31));
            for nullable in [Nullability::Nullable, Nullability::NonNullable] {
                coalesce_agrees(
                    PrimitiveArray::from_option_iter(values.clone()).into_array(),
                    Scalar::from(-17_i64),
                    DType::Primitive(PType::I64, nullable),
                    rows > 0,
                );
            }
        }
    }
    coalesce_agrees(
        PrimitiveArray::from_option_iter([Some(u64::MAX), None, Some(0)]).into_array(),
        Scalar::from(u64::MAX - 1),
        DType::Primitive(PType::U64, Nullability::NonNullable),
        true,
    );
    for values in [
        vec![Some(-0.0), None, Some(0.0), Some(f64::from_bits(1))],
        vec![None; 4],
        vec![Some(-0.0); 4],
    ] {
        coalesce_agrees(
            PrimitiveArray::from_option_iter(values).into_array(),
            Scalar::from(-0.0),
            DType::Primitive(PType::F64, Nullability::NonNullable),
            true,
        );
    }
    let source = PrimitiveArray::new(
        Buffer::from(vec![f64::NAN, -0.0, f64::INFINITY]),
        Validity::from_iter([false, true, false]),
    )
    .into_array();
    coalesce_agrees(
        source,
        Scalar::from(1.5_f64),
        DType::Primitive(PType::F64, Nullability::NonNullable),
        true,
    );
    let dictionary = DictArray::try_new(
        PrimitiveArray::from_iter([1_u8, 0, 2, 1]).into_array(),
        PrimitiveArray::from_option_iter([Some(8_i64), None, Some(-9)]).into_array(),
    )
    .unwrap()
    .into_array();
    coalesce_agrees(
        dictionary.slice(1..4).unwrap(),
        Scalar::from(5_i64),
        DType::Primitive(PType::I64, Nullability::NonNullable),
        true,
    );
}

#[test]
fn numeric_expression_literal_coalesce_preserves_nonfinite_errors_and_declines_lazy_literals() {
    for invalid in [f64::NAN, f64::INFINITY, f64::NEG_INFINITY] {
        coalesce_agrees(
            PrimitiveArray::from_option_iter([None, Some(invalid)]).into_array(),
            Scalar::from(1.0),
            DType::Primitive(PType::F64, Nullability::NonNullable),
            true,
        );
        // An unused nonfinite literal is not observed by the general owner.
        for values in [vec![Some(1.0), Some(-0.0)], vec![Some(1.0), None]] {
            coalesce_agrees(
                PrimitiveArray::from_option_iter(values).into_array(),
                Scalar::from(invalid),
                DType::Primitive(PType::F64, Nullability::NonNullable),
                false,
            );
        }
    }
    coalesce_agrees(
        PrimitiveArray::from_option_iter([Some(1_i64), None]).into_array(),
        Scalar::null(DType::Primitive(PType::I64, Nullability::Nullable)),
        DType::Primitive(PType::I64, Nullability::Nullable),
        false,
    );
    coalesce_agrees(
        PrimitiveArray::from_option_iter([Some(1_i32), None]).into_array(),
        Scalar::from(7_i64),
        DType::Primitive(PType::I64, Nullability::NonNullable),
        false,
    );
}

#[test]
fn numeric_expression_allocation_refusal_cancellation_and_output_lifetime() {
    binary_lifetime(PType::I64);
    binary_lifetime(PType::U16);
}

fn binary_lifetime(source_type: PType) {
    let producer = ResidentVortexSession::new(1 << 20, 1).unwrap();
    let producer_memory = producer.memory().clone();
    let receiver = ResidentVortexSession::new(1 << 20, 1).unwrap();
    let receiver_memory = receiver.memory().clone();
    let dtype = DType::Primitive(PType::I64, Nullability::Nullable);
    let source = producer
        .with_native_execution_context(&CancellationToken::default(), |context| {
            result_batch::build_column(
                &DType::Primitive(source_type, Nullability::Nullable),
                2053,
                &context.native_session().host_allocator(),
                |row| {
                    Ok(if row % 3 == 1 {
                        Value::Null
                    } else if source_type == PType::U16 {
                        Value::UInt(u64::try_from(row).unwrap())
                    } else {
                        Value::Int(i64::try_from(row).unwrap())
                    })
                },
            )
        })
        .unwrap();
    let token = CancellationToken::default();
    let output = receiver
        .with_native_execution_context(&token, |context| {
            let left = keys(&source, context)?;
            let right = keys(
                &ConstantArray::new(Scalar::from(1_i64), source.len()).into_array(),
                context,
            )?;
            let before = receiver_memory.snapshot();
            let occupied = receiver_memory.reserve(before.limit_bytes - before.reserved_bytes)?;
            assert!(
                binary(&left, BinaryOp::Add, &right, &dtype, context)
                    .unwrap_err()
                    .to_string()
                    .contains("memory")
            );
            assert_eq!(
                receiver_memory.snapshot().reserved_bytes,
                before.limit_bytes
            );
            drop(occupied);
            Ok(binary(&left, BinaryOp::Add, &right, &dtype, context)?.unwrap())
        })
        .unwrap();
    let reserved = receiver_memory.snapshot().reserved_bytes;
    let cancelled = receiver.with_native_execution_context(&token, |context| {
        let left = keys(&source, context)?;
        token.cancel();
        assert!(binary(&left, BinaryOp::Add, &left, &dtype, context).is_err());
        Ok(())
    });
    assert!(cancelled.unwrap_err().to_string().contains("cancelled"));
    assert_eq!(receiver_memory.snapshot().reserved_bytes, reserved);
    drop(source);
    drop(producer);
    assert_eq!(producer_memory.snapshot().reserved_bytes, 0);
    let clone = output.clone();
    let slice = output.slice(2051..2053).unwrap();
    drop(output);
    drop(receiver);
    assert!(receiver_memory.snapshot().reserved_bytes > 0);
    drop(clone);
    assert!(receiver_memory.snapshot().reserved_bytes > 0);
    let mut execution = vortex::array::array_session().create_execution_ctx();
    assert_eq!(
        slice.execute_scalar(0, &mut execution).unwrap(),
        Scalar::primitive(2052_i64, Nullability::Nullable)
    );
    assert_eq!(
        slice.execute_scalar(1, &mut execution).unwrap(),
        Scalar::primitive(2053_i64, Nullability::Nullable)
    );
    drop(slice);
    assert_eq!(receiver_memory.snapshot().reserved_bytes, 0);
}

#[test]
fn numeric_expression_coalesce_refusal_cancellation_and_independent_output_lifetime() {
    let producer = ResidentVortexSession::new(1 << 20, 1).unwrap();
    let producer_memory = producer.memory().clone();
    let receiver = ResidentVortexSession::new(1 << 20, 1).unwrap();
    let receiver_memory = receiver.memory().clone();
    let source = producer
        .with_native_execution_context(&CancellationToken::default(), |context| {
            result_batch::build_column(
                &DType::Primitive(PType::F64, Nullability::Nullable),
                2053,
                &context.native_session().host_allocator(),
                |row| {
                    Ok(if row % 2 == 0 {
                        Value::Null
                    } else {
                        Value::Float(-0.0)
                    })
                },
            )
        })
        .unwrap();
    let expression = Expression::new(
        DType::Primitive(PType::F64, Nullability::NonNullable),
        Kind::Coalesce(vec![
            column("value", source.dtype()),
            literal(Scalar::from(1.5)),
        ]),
    );
    let Kind::Coalesce(children) = &expression.kind else {
        unreachable!()
    };
    let input = StructArray::from_fields(&[("value", source)])
        .unwrap()
        .into_array();
    let token = CancellationToken::default();
    let output = receiver
        .with_native_execution_context(&token, |context| {
            let before = receiver_memory.snapshot();
            let occupied = receiver_memory.reserve(before.limit_bytes - before.reserved_bytes)?;
            assert!(
                coalesce(&expression, &input, context, children)
                    .unwrap_err()
                    .to_string()
                    .contains("memory")
            );
            assert_eq!(
                receiver_memory.snapshot().reserved_bytes,
                before.limit_bytes
            );
            drop(occupied);
            Ok(coalesce(&expression, &input, context, children)?.unwrap())
        })
        .unwrap();
    let reserved = receiver_memory.snapshot().reserved_bytes;
    let cancelled = receiver.with_native_execution_context(&token, |context| {
        token.cancel();
        assert!(coalesce(&expression, &input, context, children).is_err());
        Ok(())
    });
    assert!(cancelled.unwrap_err().to_string().contains("cancelled"));
    assert_eq!(receiver_memory.snapshot().reserved_bytes, reserved);
    drop((input, producer));
    assert_eq!(producer_memory.snapshot().reserved_bytes, 0);
    let slice = output.slice(2051..2053).unwrap();
    drop((output, receiver));
    assert!(receiver_memory.snapshot().reserved_bytes > 0);
    let mut execution = vortex::array::array_session().create_execution_ctx();
    let selected = slice
        .clone()
        .execute::<PrimitiveArray>(&mut execution)
        .unwrap();
    assert_eq!(
        selected.as_slice::<f64>()[0].to_bits(),
        (-0.0_f64).to_bits()
    );
    assert_eq!(selected.as_slice::<f64>()[1], 1.5);
    drop((slice, selected));
    assert_eq!(receiver_memory.snapshot().reserved_bytes, 0);
}

#[test]
fn numeric_expression_decimal_and_temporal_stay_with_original_owners() {
    let decimal =
        DecimalArray::from_option_iter([Some(123_i128), None, Some(-9)], DecimalDType::new(8, 2))
            .into_array();
    let date = ExtensionArray::try_new(
        Date::new(TimeUnit::Days, Nullability::Nullable).erased(),
        PrimitiveArray::from_option_iter([Some(-1_i32), None, Some(20_000)]).into_array(),
    )
    .unwrap()
    .into_array();
    for source in [decimal, date] {
        let value = if matches!(source.dtype(), DType::Extension(_)) {
            // The binder keeps temporal literal ticks in primitive scalar
            // storage while the expression carries the logical date dtype.
            Expression::new(source.dtype().clone(), Kind::Literal(Scalar::from(-1_i32)))
        } else {
            let mut execution = vortex::array::array_session().create_execution_ctx();
            literal(source.execute_scalar(0, &mut execution).unwrap())
        };
        coalesce_literal_agrees(source.clone(), value, source.dtype().clone(), false);
        let session = ResidentVortexSession::new(1 << 20, 1).unwrap();
        session
            .with_native_execution_context(&CancellationToken::default(), |context| {
                let values = keys(&source, context)?;
                assert!(
                    binary(&values, BinaryOp::Add, &values, source.dtype(), context)?.is_none()
                );
                Ok(())
            })
            .unwrap();
    }
}

#[test]
fn numeric_expression_left_preparation_precedes_right_evaluation_failure() {
    // The existing key owner rejects F16 during preparation, before evaluation
    // can discover the missing right-hand input column.
    let left = PrimitiveArray::from_byte_buffer(
        Buffer::from(vec![0x3c00_u16]).into_byte_buffer(),
        PType::F16,
        Validity::NonNullable,
    )
    .into_array();
    let dtype = left.dtype().clone();
    let expression = Expression::new(
        dtype.clone(),
        Kind::Binary(
            Box::new(column("left", &dtype)),
            BinaryOp::Add,
            Box::new(column("right_absent", &dtype)),
        ),
    );
    let input = StructArray::from_fields(&[("left", left.clone())])
        .unwrap()
        .into_array();
    let session = ResidentVortexSession::new(1 << 20, 1).unwrap();
    let memory = session.memory().clone();
    session
        .with_native_execution_context(&CancellationToken::default(), |context| {
            let expected = keys(&left, context)
                .err()
                .expect("left preparation must reject unsupported F16 keys");
            assert!(expected.to_string().contains("key requires"));
            let actual = expression.evaluate(&input, context).unwrap_err();
            assert_eq!(actual.to_string(), expected.to_string());
            Ok(())
        })
        .unwrap();
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}
