use super::*;

fn record(source: ArrayRef) -> ArrayRef {
    StructArray::from_fields(&[("value", source)])
        .unwrap()
        .into_array()
}

#[test]
fn dictionary_text_unused_values_do_not_run_kernels_or_inflate_output_memory() {
    let large_unused = "UNREFERENCED İΣΟΣ ".repeat(20_000);
    let source = DictArray::try_new(
        PrimitiveArray::from_iter([0u8; 1024]).into_array(),
        VarBinViewArray::from_iter_str(["  ABC  ", large_unused.as_str()]).into_array(),
    )
    .unwrap()
    .into_array();
    let expression = expression(Function::Lower, source.dtype());
    let input = record(source);
    let session = ResidentVortexSession::new(32 << 10, 1).unwrap();
    let memory = session.memory().clone();
    session
        .with_native_execution_context(&CancellationToken::default(), |context| {
            let actual = expression.evaluate(&input, context)?;
            let dictionary = actual.as_::<Dict>();
            assert_eq!(dictionary.values().len(), 2);
            assert_eq!(
                dictionary.codes().dtype(),
                &DType::Primitive(PType::U8, Nullability::NonNullable)
            );
            let mut execution = context.native_session().create_execution_ctx();
            for row in 0..1024 {
                assert_eq!(
                    result_batch::scalar(&actual, row, &mut execution)?,
                    Scalar::from("  abc  ")
                );
            }
            assert_eq!(
                result_batch::scalar(dictionary.values(), 1, &mut execution)?,
                Scalar::from("")
            );
            Ok(())
        })
        .unwrap();
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

#[test]
fn dictionary_text_outputs_and_child_aliases_release_both_producer_and_receiver_credits() {
    for function in functions() {
        let producer = ResidentVortexSession::new(1 << 20, 1).unwrap();
        let receiver = ResidentVortexSession::new(1 << 20, 1).unwrap();
        let producer_memory = producer.memory().clone();
        let receiver_memory = receiver.memory().clone();
        let input = producer
            .with_native_execution_context(&CancellationToken::default(), |context| {
                let allocator = context.native_session().host_allocator();
                let codes = result_batch::build_column(
                    &DType::Primitive(PType::U16, Nullability::Nullable),
                    128,
                    &allocator,
                    |row| {
                        Ok(if row % 7 == 0 {
                            Value::Null
                        } else {
                            Value::UInt((row % 4) as u64)
                        })
                    },
                )?;
                let values = result_batch::build_column(
                    &DType::Utf8(Nullability::Nullable),
                    4,
                    &allocator,
                    |row| {
                        Ok(match row {
                            0 => Value::Text("  İßΣΟΣ\0  ".into()),
                            1 => Value::Text("\u{2003}東京\u{2003}".into()),
                            2 => Value::Text("".into()),
                            _ => Value::Null,
                        })
                    },
                )?;
                Ok(record(
                    DictArray::try_new(codes, values).unwrap().into_array(),
                ))
            })
            .unwrap();
        let dtype = crate::local_primitives::logical_field_from_native_array(&input, "value")
            .unwrap()
            .dtype()
            .clone();
        let expression = expression(function, &dtype);
        let (output, expected) = receiver
            .with_native_execution_context(&CancellationToken::default(), |context| {
                let expected = original(&expression, &input, context)?;
                let mut execution = context.native_session().create_execution_ctx();
                let expected = (0..128)
                    .map(|row| result_batch::scalar(&expected, row, &mut execution))
                    .collect::<Result<Vec<_>>>()?;
                Ok((expression.evaluate(&input, context)?, expected))
            })
            .unwrap();
        let dictionary = output.as_::<Dict>();
        let codes = dictionary.codes().clone();
        let values = dictionary.values().clone();
        assert_eq!(
            codes.dtype(),
            &DType::Primitive(PType::U16, Nullability::Nullable)
        );
        drop((input, producer));
        assert_eq!(producer_memory.snapshot().reserved_bytes, 0);
        let retained = output.slice(1..127).unwrap();
        drop((output, receiver));
        assert!(receiver_memory.snapshot().reserved_bytes > 0);
        let mut execution = vortex::session::VortexSession::empty().create_execution_ctx();
        for (row, expected) in expected.into_iter().enumerate() {
            if (1..127).contains(&row) {
                assert_eq!(
                    result_batch::scalar(&retained, row - 1, &mut execution).unwrap(),
                    expected
                );
            }
        }
        drop((retained, execution));
        assert!(receiver_memory.snapshot().reserved_bytes > 0);
        drop((codes, values));
        assert_eq!(receiver_memory.snapshot().reserved_bytes, 0);
    }
}

#[test]
fn dictionary_text_optional_bookkeeping_refusal_keeps_the_admitted_row_path() {
    // First refuse metadata; then admit metadata but refuse representatives.
    // In both cases the unchanged row output still fits the remaining grant.
    for (domain_len, rows, remaining) in [(128, 129, 1600), (300, 301, 3000)] {
        let domain = (0..domain_len).map(|_| "é").collect::<Vec<_>>();
        let source = DictArray::try_new(
            PrimitiveArray::from_iter(
                (0..rows).map(|row| u16::try_from(row % domain_len).unwrap()),
            )
            .into_array(),
            VarBinViewArray::from_iter_str(domain).into_array(),
        )
        .unwrap()
        .into_array();
        let expression = expression(Function::Length, source.dtype());
        let session = ResidentVortexSession::new(1 << 20, 1).unwrap();
        let memory = session.memory().clone();
        session
            .with_native_execution_context(&CancellationToken::default(), |context| {
                let columns = [keys(&source, context)?];
                let Kind::Function { function, .. } = &expression.kind else {
                    unreachable!()
                };
                let before = memory.snapshot();
                let hold =
                    memory.reserve(before.limit_bytes - before.reserved_bytes - remaining)?;
                let held = memory.snapshot().reserved_bytes;
                let output = function
                    .evaluate_prepared(&expression, &columns, rows, context)?
                    .unwrap();
                assert!(!output.is::<Dict>());
                let mut execution = context.native_session().create_execution_ctx();
                for row in 0..rows {
                    assert_eq!(
                        result_batch::scalar(&output, row, &mut execution)?,
                        Scalar::from(1_i64)
                    );
                }
                drop(output);
                assert_eq!(memory.snapshot().reserved_bytes, held);
                drop(hold);
                Ok(())
            })
            .unwrap();
        assert_eq!(memory.snapshot().reserved_bytes, 0);
    }
}

#[test]
fn dictionary_text_fallible_functions_do_not_observe_unused_valid_utf8_values() {
    let source = DictArray::try_new(
        PrimitiveArray::from_iter([0u8; 32]).into_array(),
        VarBinViewArray::from_iter_str(["aa", "invalid hex"]).into_array(),
    )
    .unwrap()
    .into_array();
    let dtype = source.dtype().clone();
    let expression = Expression::new(
        DType::Binary(Nullability::NonNullable),
        Kind::Function {
            function: Function::Unhex,
            args: vec![Expression::new(dtype, Kind::Column("value".into()))],
        },
    );
    let input = record(source);
    let session = ResidentVortexSession::new(1 << 20, 1).unwrap();
    session
        .with_native_execution_context(&CancellationToken::default(), |context| {
            let actual = expression.evaluate(&input, context)?;
            let expected = original(&expression, &input, context)?;
            assert!(!actual.is::<Dict>());
            let mut execution = context.native_session().create_execution_ctx();
            for row in 0..32 {
                assert_eq!(
                    result_batch::scalar(&actual, row, &mut execution)?,
                    result_batch::scalar(&expected, row, &mut execution)?
                );
            }
            Ok(())
        })
        .unwrap();
    assert_eq!(session.memory().snapshot().reserved_bytes, 0);
}

#[test]
fn dictionary_text_empty_all_null_sparse_and_all_integer_code_widths_match_rows() {
    for ptype in [
        PType::I8,
        PType::I16,
        PType::I32,
        PType::I64,
        PType::U8,
        PType::U16,
        PType::U32,
        PType::U64,
    ] {
        let producer = ResidentVortexSession::new(1 << 20, 1).unwrap();
        let codes = producer
            .with_native_execution_context(&CancellationToken::default(), |context| {
                result_batch::build_column(
                    &DType::Primitive(ptype, Nullability::Nullable),
                    67,
                    &context.native_session().host_allocator(),
                    |row| {
                        Ok(if row % 5 == 0 {
                            Value::Null
                        } else if matches!(ptype, PType::I8 | PType::I16 | PType::I32 | PType::I64)
                        {
                            Value::Int(i64::try_from(row % 3).unwrap())
                        } else {
                            Value::UInt((row % 3) as u64)
                        })
                    },
                )
            })
            .unwrap();
        let domain = VarBinViewArray::from_iter_nullable_str([
            Some(" İßΣΟΣ "),
            None,
            Some("é"),
            Some("unused"),
        ])
        .into_array();
        let source = DictArray::try_new(codes, domain).unwrap().into_array();
        for function in functions() {
            agrees(source.slice(1..66).unwrap(), function, true);
        }
        drop((source, producer));
    }
    for codes in [
        PrimitiveArray::from_iter([0u8; 0]).into_array(),
        PrimitiveArray::from_option_iter([None::<u8>; 16]).into_array(),
        PrimitiveArray::from_iter([1u8; 16]).into_array(),
    ] {
        let source = DictArray::try_new(
            codes,
            VarBinViewArray::from_iter_nullable_str([Some("unused"), None]).into_array(),
        )
        .unwrap()
        .into_array();
        for function in functions() {
            agrees(source.clone(), function, true);
        }
    }
    let source = DictArray::try_new(
        PrimitiveArray::from_option_iter([None::<u8>; 16]).into_array(),
        VarBinViewArray::from_iter_nullable_str(std::iter::empty::<Option<&str>>()).into_array(),
    )
    .unwrap()
    .into_array();
    for function in functions() {
        agrees(source.clone(), function, true);
    }
}

#[test]
fn dictionary_text_invalid_codes_and_unused_invalid_utf8_preserve_errors() {
    for codes in [
        PrimitiveArray::from_iter([0u64, 0, 0, u64::MAX]).into_array(),
        PrimitiveArray::from_iter([0i64, 0, 0, -1]).into_array(),
        PrimitiveArray::from_iter([0u8, 0, 0, 2]).into_array(),
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
    let binary = VarBinArray::from(vec![&b"good"[..], &b"\xff"[..]]);
    let slots = binary.slots().iter().cloned().collect();
    let invalid = VarBinArray::try_from_parts(ArrayParts::new(
        VarBin,
        DType::Utf8(Nullability::NonNullable),
        2,
        binary.into_data(),
        slots,
    ))
    .unwrap()
    .into_array();
    let source = DictArray::try_new(PrimitiveArray::from_iter([0u8; 16]).into_array(), invalid)
        .unwrap()
        .into_array();
    for function in functions() {
        agrees(source.clone(), function, false);
    }
}

#[test]
fn dictionary_text_varying_regex_sibling_keeps_row_null_masks() {
    let source = DictArray::try_new(
        PrimitiveArray::from_iter([0u8; 32]).into_array(),
        VarBinViewArray::from_iter_str(["λ"]).into_array(),
    )
    .unwrap()
    .into_array();
    let pattern =
        VarBinViewArray::from_iter_nullable_str((0..32).map(|row| (row % 2 == 0).then_some("λ")))
            .into_array();
    let mut expression = expression(
        Function::Regex(regex::Regex::new("λ").unwrap()),
        source.dtype(),
    );
    expression.dtype = DType::Bool(Nullability::Nullable);
    let Kind::Function { args, .. } = &mut expression.kind else {
        unreachable!()
    };
    args[1] = Expression::new(pattern.dtype().clone(), Kind::Column("pattern".into()));
    let input = StructArray::from_fields(&[("value", source), ("pattern", pattern)])
        .unwrap()
        .into_array();
    let session = ResidentVortexSession::new(1 << 20, 1).unwrap();
    session
        .with_native_execution_context(&CancellationToken::default(), |context| {
            let actual = expression.evaluate(&input, context)?;
            let expected = original(&expression, &input, context)?;
            assert!(!actual.is::<Dict>());
            let mut execution = context.native_session().create_execution_ctx();
            for row in 0..32 {
                let value = result_batch::scalar(&actual, row, &mut execution)?;
                assert_eq!(value.is_null(), row % 2 != 0);
                assert_eq!(value, result_batch::scalar(&expected, row, &mut execution)?);
            }
            Ok(())
        })
        .unwrap();
    assert_eq!(session.memory().snapshot().reserved_bytes, 0);
}

#[test]
fn dictionary_text_cancelled_domain_build_refunds_and_fresh_retry_succeeds() {
    let source = DictArray::try_new(
        PrimitiveArray::from_iter([0u8; 1024]).into_array(),
        VarBinViewArray::from_iter_str([" ABC ", "unused"]).into_array(),
    )
    .unwrap()
    .into_array();
    let expression = expression(Function::Lower, source.dtype());
    let session = ResidentVortexSession::new(1 << 20, 1).unwrap();
    let memory = session.memory().clone();
    let cancellation = CancellationToken::default();
    let result = session.with_native_execution_context(&cancellation, |context| {
        let KeyColumn::Variable(values) = keys(&source, context)? else {
            unreachable!()
        };
        let before = memory.snapshot().reserved_bytes;
        let builder = super::super::dictionary::Output::prepare(&values, source.len(), context)?;
        let result = builder.build(&expression, context, |_| {
            cancellation.cancel();
            Ok(Value::Text("abc".into()))
        });
        assert!(result.is_err());
        assert_eq!(memory.snapshot().reserved_bytes, before);
        Ok(())
    });
    assert!(result.is_err());
    assert_eq!(memory.snapshot().reserved_bytes, 0);
    agrees(source, Function::Lower, true);
}

#[test]
fn dictionary_text_required_code_copy_refusal_is_an_error_and_refunds_temporary_work() {
    let source = DictArray::try_new(
        PrimitiveArray::from_iter([0u16; 4096]).into_array(),
        VarBinViewArray::from_iter_str(["abc", "unused"]).into_array(),
    )
    .unwrap()
    .into_array();
    let expression = expression(Function::Lower, source.dtype());
    let session = ResidentVortexSession::new(1 << 20, 1).unwrap();
    let memory = session.memory().clone();
    session
        .with_native_execution_context(&CancellationToken::default(), |context| {
            let KeyColumn::Variable(values) = keys(&source, context)? else {
                unreachable!()
            };
            let before = memory.snapshot().reserved_bytes;
            let builder =
                super::super::dictionary::Output::prepare(&values, source.len(), context)?;
            let snapshot = memory.snapshot();
            let hold = memory.reserve(snapshot.limit_bytes - snapshot.reserved_bytes - 1024)?;
            let mut calls = 0;
            let result = builder.build(&expression, context, |_| {
                calls += 1;
                Ok(Value::Text("abc".into()))
            });
            assert_eq!(calls, 1);
            assert!(result.unwrap_err().to_string().contains("memory"));
            assert_eq!(memory.snapshot().reserved_bytes, before + hold.bytes());
            drop(hold);
            drop(expression.evaluate(&record(source.clone()), context)?);
            assert_eq!(memory.snapshot().reserved_bytes, before);
            Ok(())
        })
        .unwrap();
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

#[test]
fn dictionary_text_null_literal_regex_pattern_preserves_dictionary_null_results() {
    let source = DictArray::try_new(
        PrimitiveArray::from_option_iter((0..32).map(|row| (row % 5 != 0).then_some(0u8)))
            .into_array(),
        VarBinViewArray::from_iter_str(["λ"]).into_array(),
    )
    .unwrap()
    .into_array();
    let mut expression = expression(
        Function::Regex(regex::Regex::new("").unwrap()),
        source.dtype(),
    );
    expression.dtype = DType::Bool(Nullability::Nullable);
    let Kind::Function { args, .. } = &mut expression.kind else {
        unreachable!()
    };
    let pattern = Scalar::null(DType::Utf8(Nullability::Nullable));
    args[1] = Expression::new(pattern.dtype().clone(), Kind::Literal(pattern));
    let input = record(source);
    let session = ResidentVortexSession::new(1 << 20, 1).unwrap();
    session
        .with_native_execution_context(&CancellationToken::default(), |context| {
            let actual = expression.evaluate(&input, context)?;
            assert!(actual.is::<Dict>());
            let expected = original(&expression, &input, context)?;
            let mut execution = context.native_session().create_execution_ctx();
            for row in 0..32 {
                let value = result_batch::scalar(&actual, row, &mut execution)?;
                assert!(value.is_null());
                assert_eq!(value, result_batch::scalar(&expected, row, &mut execution)?);
            }
            Ok(())
        })
        .unwrap();
    assert_eq!(session.memory().snapshot().reserved_bytes, 0);
}
