//! Dictionary memoization preserves complete preparation, ordered errors and credits.

use super::*;

fn dictionary(codes: ArrayRef) -> ArrayRef {
    DictArray::try_new(
        codes,
        VarBinViewArray::from_iter_nullable_str([
            Some("unreferenced"),
            Some("  λ東京İß\0ΣΟΣ  "),
            None,
            Some("\u{2003}e\u{301}X\u{2003}"),
        ])
        .into_array(),
    )
    .unwrap()
    .into_array()
}

fn pure() -> [Function; 4] {
    [
        Function::Lower,
        Function::Upper,
        Function::Trim,
        Function::Length,
    ]
}

#[test]
fn dictionary_unary_computes_first_references_once_and_preserves_nulls_and_complete_values() {
    let codes = [Some(3u8), Some(1), None, Some(2), Some(3), Some(1)];
    let source = dictionary(
        PrimitiveArray::from_option_iter(codes.into_iter().cycle().take(258)).into_array(),
    );
    for function in pure() {
        let expression = expression(function, source.dtype());
        let input = StructArray::from_fields(&[("value", source.clone())])
            .unwrap()
            .into_array();
        let session = ResidentVortexSession::new(8 << 20, 1).unwrap();
        let memory = session.memory().clone();
        session
            .with_native_execution_context(&CancellationToken::default(), |context| {
                let before = memory.snapshot().reserved_bytes;
                let expected = original(&expression, &input, context)?;
                let actual = expression.evaluate(&input, context)?;
                let mut execution = context.native_session().create_execution_ctx();
                for row in 0..input.len() {
                    assert_eq!(
                        result_batch::scalar(&actual, row, &mut execution)?,
                        result_batch::scalar(&expected, row, &mut execution)?
                    );
                }
                let mut proof = String::new();
                context.append_payload_work_certificate(&mut proof)?;
                for token in [
                    "expression_dictionary_completed_batches=1",
                    "expression_dictionary_completed_rows=258",
                    "expression_dictionary_computed_values=2",
                    "expression_dictionary_cache_hits=170",
                    "expression_dictionary_resource_declines=0",
                ] {
                    assert!(
                        proof.split_ascii_whitespace().any(|part| part == token),
                        "{proof}"
                    );
                }
                drop((actual, expected, execution));
                assert_eq!(memory.snapshot().reserved_bytes, before);
                Ok(())
            })
            .unwrap();
        drop(session);
        assert_eq!(memory.snapshot().reserved_bytes, 0);
    }
}

#[test]
fn dictionary_unary_bad_late_codes_keep_scalar_errors_after_cache_hits() {
    for codes in [
        PrimitiveArray::from_iter([1u64, 1, 3, 1, 3, 1, u64::MAX]).into_array(),
        PrimitiveArray::from_iter([1i64, 1, 3, 1, 3, 1, -1]).into_array(),
        PrimitiveArray::from_iter([1u8, 1, 3, 1, 3, 1, 4]).into_array(),
    ] {
        for function in pure() {
            agrees(dictionary(codes.clone()), function, true);
        }
    }
}

#[test]
fn dictionary_cache_memory_decline_reuses_prepared_arguments_and_refunds_partial_output() {
    let source = DictArray::try_new(
        PrimitiveArray::from_iter((0..256u16).map(|row| row % 64)).into_array(),
        VarBinViewArray::from_iter_str((0..64).map(|row| format!("value λ {row}"))).into_array(),
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
            let hold = memory.reserve(before.limit_bytes - before.reserved_bytes - 3072)?;
            let held = memory.snapshot().reserved_bytes;
            let expected = {
                let mut scratch = memory.reserve(0)?;
                let output = expression.build(256, context, |row| {
                    function.evaluate(&columns, row, &expression.dtype, &mut scratch)
                })?;
                let mut execution = context.native_session().create_execution_ctx();
                (0..256)
                    .map(|row| result_batch::scalar(&output, row, &mut execution))
                    .collect::<Result<Vec<_>>>()?
            };
            assert_eq!(memory.snapshot().reserved_bytes, held);
            let actual = function
                .evaluate_prepared(&expression, &columns, 256, context)?
                .unwrap();
            let mut execution = context.native_session().create_execution_ctx();
            for (row, expected) in expected.into_iter().enumerate() {
                assert_eq!(
                    result_batch::scalar(&actual, row, &mut execution)?,
                    expected
                );
            }
            let mut proof = String::new();
            context.append_payload_work_certificate(&mut proof)?;
            assert!(
                proof.contains("expression_dictionary_resource_declines=1"),
                "{proof}"
            );
            assert!(memory.snapshot().denied_reservations > before.denied_reservations);
            drop((actual, execution));
            assert_eq!(memory.snapshot().reserved_bytes, held);
            drop(hold);
            assert_eq!(memory.snapshot().reserved_bytes, before.reserved_bytes);
            Ok(())
        })
        .unwrap();
    drop(session);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}
