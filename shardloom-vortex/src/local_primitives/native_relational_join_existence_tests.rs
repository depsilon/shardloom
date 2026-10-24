//! Exact existence state reuses row membership without retaining duplicate payloads.

use super::*;
use vortex::array::{
    ArrayParts,
    arrays::{ChunkedArray, DictArray, VarBin, VarBinArray, VarBinViewArray},
};

fn keyed(keys: ArrayRef, ids: impl IntoIterator<Item = u32>) -> ArrayRef {
    let len = keys.len();
    StructArray::new(
        FieldNames::from(["key", "id"]),
        vec![keys, PrimitiveArray::from_iter(ids).into_array()],
        len,
        Validity::NonNullable,
    )
    .into_array()
}

fn check_case(left: &ArrayRef, right: &[ArrayRef], expected_semi: &[u64], unique: usize) {
    for kind in [Kind::LeftSemi, Kind::LeftAnti] {
        let ids = logical_field_from_native_array(left, "id").unwrap();
        let mut execution = vortex::array::array_session().create_execution_ctx();
        let expected = (0..left.len())
            .map(
                |row| match result_batch::scalar_value(&ids, row, &mut execution).unwrap() {
                    result_batch::Value::UInt(id) => id,
                    _ => panic!("unexpected ID dtype"),
                },
            )
            .filter(|id| expected_semi.contains(id) == (kind == Kind::LeftSemi))
            .map(|id| (Some(id), None))
            .collect::<Vec<_>>();
        for ordinary in [false, true] {
            let session = ResidentVortexSession::new(8 << 20, 1).unwrap();
            let mut binding = spec(kind);
            binding.right_key_fields[0].1 = logical_field_from_native_array(&right[0], "key")
                .unwrap()
                .dtype()
                .clone();
            let mut actual = Vec::new();
            session
                .with_native_execution_context(&CancellationToken::default(), |context| {
                    let mut join = Join::new(&binding, context.memory())?;
                    if ordinary {
                        join.membership = None; // Preserve the original consumer as a test oracle.
                    }
                    for batch in right {
                        join.build(batch.clone(), context, 2)?;
                    }
                    if !ordinary {
                        assert_eq!(join.right.rows(), 0);
                        assert_eq!(join.index.unique_rows(), 0);
                        let membership = join.membership.as_ref().unwrap();
                        assert_eq!(membership.rows(), unique);
                        let rows = (0..unique).map(Some).collect::<Vec<_>>();
                        let gathered = membership.gather(&rows, context)?;
                        if unique > 0 {
                            assert!(
                                gathered
                                    .column(
                                        "id",
                                        &DType::Primitive(PType::U32, Nullability::NonNullable),
                                        context
                                    )
                                    .is_err()
                            );
                        }
                    }
                    join.consume(left.clone(), context, 2, &mut |array| {
                        assert_eq!(
                            array.dtype(),
                            &DType::struct_(binding.fields.clone(), Nullability::NonNullable)
                        );
                        actual.extend(pairs(&array));
                        Ok(())
                    })?;
                    Ok(())
                })
                .unwrap();
            assert_eq!(actual, expected, "{kind:?}, ordinary={ordinary}");
            assert_eq!(session.memory().snapshot().reserved_bytes, 0);
        }
    }
}

#[test]
fn existence_membership_retains_unique_keys_across_batches_and_preserves_left_multiplicity() {
    let left = input(
        &[Some(2), Some(1), None, Some(2), Some(3)],
        &[10, 11, 12, 13, 14],
    );
    let right = [
        input(&[Some(1), Some(1), None], &[20, 21, 22]),
        input(&[], &[]),
        input(&[Some(2), Some(1), Some(2), None], &[23, 24, 25, 26]),
    ];
    check_case(&left, &right, &[10, 11, 13], 2);
    check_case(&left, &[input(&[], &[])], &[], 0);
    check_case(&left, &[input(&[None, None], &[20, 21])], &[], 0);
}

#[test]
fn existence_membership_preserves_dictionary_domains_slices_and_chunked_keys() {
    let dictionary = |codes: Vec<u32>, domain: Vec<&str>| {
        DictArray::try_new(
            PrimitiveArray::from_iter(codes).into_array(),
            VarBinViewArray::from_iter_str(domain).into_array(),
        )
        .unwrap()
        .into_array()
    };
    let first = dictionary(vec![2, 0, 0, 1, 2], vec!["λ", "東京", "ignored"]);
    let second = dictionary(vec![0, 1, 0], vec!["東京", "λ"]);
    let chunked = ChunkedArray::try_new(
        vec![first.slice(1..4).unwrap(), second],
        DType::Utf8(Nullability::NonNullable),
    )
    .unwrap()
    .into_array();
    let left = keyed(
        VarBinViewArray::from_iter_str(["東京", "λ", "", "λ"]).into_array(),
        10..14,
    );
    check_case(&left, &[keyed(chunked, 20..26)], &[10, 11, 13], 2);
}

#[test]
fn existence_membership_preserves_float_zero_and_nested_child_null_equality() {
    let left = keyed(
        PrimitiveArray::from_option_iter([Some(-0.0f64), None, Some(1.0), Some(2.0), Some(0.0)])
            .into_array(),
        10..15,
    );
    let right = keyed(
        PrimitiveArray::from_option_iter([Some(0.0f64), Some(-0.0), None, Some(1.0), Some(1.0)])
            .into_array(),
        20..25,
    );
    check_case(&left, &[right], &[10, 12, 14], 2);
    let nested = |values: Vec<Option<i64>>, valid: Vec<bool>| {
        let len = values.len();
        StructArray::new(
            FieldNames::from(["child"]),
            vec![PrimitiveArray::from_option_iter(values).into_array()],
            len,
            Validity::from_iter(valid),
        )
        .into_array()
    };
    let left = keyed(
        nested(
            vec![None, Some(999), Some(1), None, Some(2)],
            vec![true, false, true, true, true],
        ),
        10..15,
    );
    let right = keyed(
        nested(
            vec![None, Some(999), Some(1), None],
            vec![true, false, true, true],
        ),
        20..24,
    );
    check_case(&left, &[right], &[10, 12, 13], 2);
}

#[test]
fn existence_membership_keeps_compound_null_short_circuit_and_late_nonfinite_failure() {
    for ordinary in [false, true] {
        for null_first in [false, true] {
            let session = ResidentVortexSession::new(8 << 20, 1).unwrap();
            let mut binding = spec(Kind::LeftSemi);
            binding.right_keys.push("second".into());
            binding.left_keys.push("second".into());
            binding.right_key_fields.push((
                "second".into(),
                DType::Primitive(PType::F64, Nullability::NonNullable),
            ));
            let array = StructArray::new(
                FieldNames::from(["key", "second", "id"]),
                vec![
                    PrimitiveArray::from_option_iter([Some(1u64), (!null_first).then_some(2)])
                        .into_array(),
                    PrimitiveArray::from_iter([1.0f64, f64::NAN]).into_array(),
                    PrimitiveArray::from_iter([10u32, 11]).into_array(),
                ],
                2,
                Validity::NonNullable,
            )
            .into_array();
            let result =
                session.with_native_execution_context(&CancellationToken::default(), |context| {
                    let mut join = Join::new(&binding, context.memory())?;
                    if ordinary {
                        join.membership = None;
                    }
                    join.build(array.clone(), context, 1)?;
                    let mut actual = Vec::new();
                    join.consume(array, context, 1, &mut |batch| {
                        actual.extend(pairs(&batch));
                        Ok(())
                    })?;
                    assert_eq!(actual, [(Some(10), None)]);
                    Ok(())
                });
            if null_first {
                result.unwrap();
            } else {
                assert!(result.unwrap_err().to_string().contains("nonfinite"));
            }
            assert_eq!(session.memory().snapshot().reserved_bytes, 0);
        }
    }
}

#[test]
fn existence_membership_denial_and_cancellation_release_state() {
    for cancelled in [false, true] {
        let session = ResidentVortexSession::new(8 << 20, 1).unwrap();
        let memory = session.memory().clone();
        let token = CancellationToken::default();
        let result = session.with_native_execution_context(&token, |context| {
            let binding = spec(Kind::LeftAnti);
            let mut join = Join::new(&binding, context.memory())?;
            join.build(input(&[Some(1)], &[20]), context, 1)?;
            let _pressure = if cancelled {
                token.cancel();
                None
            } else {
                Some(
                    context
                        .memory()
                        .reserve((8 << 20) - context.memory().snapshot().reserved_bytes)?,
                )
            };
            join.build(input(&[Some(2)], &[21]), context, 1)
        });
        assert!(result.is_err());
        assert_eq!(memory.snapshot().reserved_bytes, 0);
    }
}

#[test]
fn existence_membership_rejects_malformed_domains_and_observed_dictionary_codes() {
    let binary = VarBinArray::from(vec![&b"good"[..], &b"\xff"[..]]);
    let slots = binary.slots().iter().cloned().collect();
    let malformed = VarBinArray::try_from_parts(ArrayParts::new(
        VarBin,
        DType::Utf8(Nullability::NonNullable),
        2,
        binary.into_data(),
        slots,
    ))
    .unwrap()
    .into_array();
    for (codes, domain) in [
        (vec![0u8, 0], malformed),
        (
            vec![0u8, 2],
            VarBinViewArray::from_iter_str(["first", "second"]).into_array(),
        ),
    ] {
        let keys = DictArray::try_new(PrimitiveArray::from_iter(codes).into_array(), domain)
            .unwrap()
            .into_array();
        let mut errors = Vec::new();
        for ordinary in [false, true] {
            let session = ResidentVortexSession::new(8 << 20, 1).unwrap();
            let mut binding = spec(Kind::LeftSemi);
            binding.right_key_fields[0].1 = keys.dtype().clone();
            let error = session
                .with_native_execution_context(&CancellationToken::default(), |context| {
                    let mut join = Join::new(&binding, context.memory())?;
                    if ordinary {
                        join.membership = None;
                    }
                    join.build(keyed(keys.clone(), 20..22), context, 1)
                })
                .unwrap_err()
                .to_string();
            errors.push(error);
            assert_eq!(session.memory().snapshot().reserved_bytes, 0);
        }
        assert_eq!(errors[0], errors[1]);
    }
}
