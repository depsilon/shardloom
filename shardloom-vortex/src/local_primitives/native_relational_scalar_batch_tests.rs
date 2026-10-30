use super::*;
use crate::{
    local_primitives::{
        native_relational_expression::{Kind, keys},
        result_batch,
    },
    resident_session::ResidentVortexSession,
};
use shardloom_exec::compute_pool::CancellationToken;
use vortex::array::{
    ArrayParts, IntoArray as _, VortexSessionExecute as _,
    arrays::{
        ConstantArray, DictArray, PrimitiveArray, StructArray, VarBin, VarBinArray, VarBinViewArray,
    },
    dtype::{DType, Nullability, PType},
    memory::HostMemorySessionExt as _,
    scalar::Scalar,
};

fn functions() -> Vec<Function> {
    vec![
        Function::Lower,
        Function::Upper,
        Function::Trim,
        Function::Length,
        Function::ByteLength,
        Function::Regex(regex::Regex::new(r"(?i)λ|東京|\s$").unwrap()),
    ]
}

fn expression(function: Function, dtype: &DType) -> Expression {
    let output = match &function {
        Function::Length | Function::ByteLength => {
            DType::Primitive(PType::I64, dtype.nullability())
        }
        Function::Regex(_) => DType::Bool(dtype.nullability()),
        _ => DType::Utf8(dtype.nullability()),
    };
    let mut args = vec![Expression::new(dtype.clone(), Kind::Column("value".into()))];
    if let Function::Regex(regex) = &function {
        let pattern = Scalar::from(regex.as_str());
        args.push(Expression::new(
            pattern.dtype().clone(),
            Kind::Literal(pattern),
        ));
    }
    Expression::new(output, Kind::Function { function, args })
}

fn original(
    expression: &Expression,
    input: &ArrayRef,
    context: &NativeExecutionContext<'_>,
) -> Result<ArrayRef> {
    let Kind::Function { function, args } = &expression.kind else {
        unreachable!()
    };
    let columns = args
        .iter()
        .map(|arg| keys(&arg.evaluate(input, context)?, context))
        .collect::<Result<Vec<_>>>()?;
    let mut scratch = context.memory().reserve(0)?;
    expression.build(input.len(), context, |row| {
        function.evaluate(&columns, row, &expression.dtype, &mut scratch)
    })
}

fn agrees(source: ArrayRef, function: Function, admitted: bool) {
    let expression = expression(function, source.dtype());
    let input = StructArray::from_fields(&[("value", source)])
        .unwrap()
        .into_array();
    let session = ResidentVortexSession::new(8 << 20, 1).unwrap();
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
                assert_eq!(counts.expression_prepared_utf8_batches, u64::from(admitted));
                if admitted && actual.is_ok() {
                    assert_eq!(counts.expression_prepared_utf8_rows, input.len() as u64);
                }
            }
            assert_eq!(
                actual.as_ref().err().map(ToString::to_string),
                expected.as_ref().err().map(ToString::to_string)
            );
            if let (Ok(actual), Ok(expected)) = (actual, expected) {
                assert_eq!(actual.dtype(), expected.dtype());
                assert_eq!(actual.len(), expected.len());
                let mut execution = context.native_session().create_execution_ctx();
                for row in 0..input.len() {
                    assert_eq!(
                        result_batch::scalar(&actual, row, &mut execution)?,
                        result_batch::scalar(&expected, row, &mut execution)?
                    );
                }
            }
            assert_eq!(memory.snapshot().reserved_bytes, before);
            Ok(())
        })
        .unwrap();
    drop((input, session));
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

#[test]
fn prepared_utf8_functions_match_scalar_owner_for_unicode_dictionary_slices_and_constants() {
    let long = "  λ東京İß\0ΣΟΣe\u{301}  ".repeat(119);
    let domain = VarBinViewArray::from_iter_nullable_str([
        Some(""),
        None,
        Some("  short\0 "),
        Some(long.as_str()),
        Some("\u{2003}İßΣΟΣ\u{2003}"),
    ])
    .into_array();
    let dictionary = DictArray::try_new(
        PrimitiveArray::from_option_iter([
            Some(3u8),
            None,
            Some(1),
            Some(4),
            Some(0),
            Some(3),
            Some(2),
        ])
        .into_array(),
        domain.clone(),
    )
    .unwrap()
    .into_array();
    for source in [
        domain.clone(),
        dictionary.clone(),
        dictionary.slice(1..6).unwrap(),
        domain.slice(0..0).unwrap(),
        domain.slice(1..2).unwrap(),
        ConstantArray::new(Scalar::from("  İßΣΟΣ\0  "), 2053).into_array(),
        VarBinArray::from(vec!["  outlined text beyond inline width  ", "\n東京\t"]).into_array(),
    ] {
        for function in functions() {
            agrees(source.clone(), function, true);
        }
    }
    // Binary byte length is still supported by its existing scalar owner.
    agrees(
        VarBinArray::from(vec![Some(&b"\xff\0"[..]), None]).into_array(),
        Function::ByteLength,
        false,
    );
}

#[test]
fn prepared_utf8_preserves_unreferenced_domain_and_observed_code_errors() {
    let binary = VarBinArray::from(vec![&b"valid"[..], &b"\xff"[..]]);
    let slots = binary.slots().iter().cloned().collect();
    let domain = VarBinArray::try_from_parts(ArrayParts::new(
        VarBin,
        DType::Utf8(Nullability::NonNullable),
        2,
        binary.into_data(),
        slots,
    ))
    .unwrap()
    .into_array();
    let unreferenced = DictArray::try_new(PrimitiveArray::from_iter([0u8]).into_array(), domain)
        .unwrap()
        .into_array();
    for function in functions() {
        agrees(unreferenced.clone(), function, false);
    }
    for codes in [
        PrimitiveArray::from_iter([0u64, u64::MAX]).into_array(),
        PrimitiveArray::from_iter([0i64, -1]).into_array(),
        PrimitiveArray::from_iter([0u8, 2]).into_array(),
    ] {
        let source = DictArray::try_new(
            codes,
            VarBinViewArray::from_iter_str(["good", "unused"]).into_array(),
        )
        .unwrap()
        .into_array();
        for function in functions() {
            agrees(source.clone(), function, true);
        }
    }
}

#[test]
fn prepared_utf8_output_does_not_retain_source_credits_after_session_close() {
    for function in functions() {
        let producer = ResidentVortexSession::new(1 << 20, 1).unwrap();
        let receiver = ResidentVortexSession::new(1 << 20, 1).unwrap();
        let producer_memory = producer.memory().clone();
        let receiver_memory = receiver.memory().clone();
        let source = producer
            .with_native_execution_context(&CancellationToken::default(), |context| {
                result_batch::build_column(
                    &DType::Utf8(Nullability::Nullable),
                    3,
                    &context.native_session().host_allocator(),
                    |row| {
                        Ok(if row == 1 {
                            Value::Null
                        } else {
                            Value::Text("  λ東京İß\0ΣΟΣ  ".into())
                        })
                    },
                )
            })
            .unwrap();
        let expression = expression(function, source.dtype());
        let input = StructArray::from_fields(&[("value", source)])
            .unwrap()
            .into_array();
        let (output, expected) = receiver
            .with_native_execution_context(&CancellationToken::default(), |context| {
                let expected = original(&expression, &input, context)?;
                let mut execution = context.native_session().create_execution_ctx();
                let expected = (0..3)
                    .map(|row| result_batch::scalar(&expected, row, &mut execution))
                    .collect::<Result<Vec<_>>>()?;
                Ok((expression.evaluate(&input, context)?, expected))
            })
            .unwrap();
        drop((input, producer));
        assert_eq!(producer_memory.snapshot().reserved_bytes, 0);
        let retained = output.slice(0..3).unwrap();
        drop(output);
        drop(receiver);
        assert!(receiver_memory.snapshot().reserved_bytes > 0);
        let mut execution = vortex::array::array_session().create_execution_ctx();
        for (row, expected) in expected.into_iter().enumerate() {
            assert_eq!(
                result_batch::scalar(&retained, row, &mut execution).unwrap(),
                expected
            );
        }
        drop((retained, execution));
        assert_eq!(receiver_memory.snapshot().reserved_bytes, 0);
    }
}

#[test]
fn prepared_utf8_regex_preserves_the_null_pattern_argument() {
    let source =
        VarBinViewArray::from_iter_nullable_str([Some("λ"), None, Some("東京")]).into_array();
    let mut expression = expression(
        Function::Regex(regex::Regex::new("").unwrap()),
        source.dtype(),
    );
    let Kind::Function { args, .. } = &mut expression.kind else {
        unreachable!()
    };
    let pattern = Scalar::null(DType::Utf8(Nullability::Nullable));
    args[1] = Expression::new(pattern.dtype().clone(), Kind::Literal(pattern));
    let input = StructArray::from_fields(&[("value", source)])
        .unwrap()
        .into_array();
    let session = ResidentVortexSession::new(1 << 20, 1).unwrap();
    session
        .with_native_execution_context(&CancellationToken::default(), |context| {
            let expected = original(&expression, &input, context)?;
            let actual = expression.evaluate(&input, context)?;
            let mut execution = context.native_session().create_execution_ctx();
            for row in 0..3 {
                let value = result_batch::scalar(&actual, row, &mut execution)?;
                assert!(value.is_null());
                assert_eq!(value, result_batch::scalar(&expected, row, &mut execution)?);
            }
            Ok(())
        })
        .unwrap();
    assert_eq!(session.memory().snapshot().reserved_bytes, 0);
}

#[test]
fn prepared_utf8_memory_refusal_cancellation_and_fresh_retry_release_credits() {
    let source = VarBinViewArray::from_iter_str(["  retained λ  "]).into_array();
    let expression = expression(Function::Trim, source.dtype());
    let input = StructArray::from_fields(&[("value", source.clone())])
        .unwrap()
        .into_array();
    let session = ResidentVortexSession::new(1 << 20, 1).unwrap();
    let memory = session.memory().clone();
    session
        .with_native_execution_context(&CancellationToken::default(), |context| {
            let columns = [keys(&source, context)?];
            let Kind::Function { function, .. } = &expression.kind else {
                unreachable!()
            };
            let before = memory.snapshot();
            let hold = memory.reserve(before.limit_bytes - before.reserved_bytes)?;
            let held = memory.snapshot().reserved_bytes;
            assert!(
                function
                    .evaluate_prepared(&expression, &columns, 1, context)
                    .is_err()
            );
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
