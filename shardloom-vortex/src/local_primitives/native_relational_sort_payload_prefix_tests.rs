//! Prefix construction keeps the full Sort payload validation domain and order.

use super::*;
use shardloom_core::FloatPolicy;
use vortex::{
    array::{
        VortexSessionExecute as _,
        arrays::{BoolArray, DecimalArray, DictArray, ListArray, VarBinViewArray},
        dtype::{DecimalDType, Nullability, PType, half::f16},
    },
    buffer::Buffer,
};

#[path = "native_relational_sort_payload_prefix_rich_tests.rs"]
mod rich_tests;

fn record(fields: Vec<(&str, ArrayRef)>) -> ArrayRef {
    let rows = fields[0].1.len();
    StructArray::try_new(
        FieldNames::from(fields.iter().map(|(name, _)| *name).collect::<Vec<_>>()),
        fields.into_iter().map(|(_, array)| array),
        rows,
        Validity::NonNullable,
    )
    .unwrap()
    .into_array()
}

fn decimal(values: &[i128]) -> ArrayRef {
    DecimalArray::try_new(
        Buffer::from(values.to_vec()),
        DecimalDType::new(2, 0),
        Validity::NonNullable,
    )
    .unwrap()
    .into_array()
}

fn list(values: ArrayRef, offsets: &[u64], validity: Validity) -> ArrayRef {
    ListArray::try_new(
        values,
        PrimitiveArray::from_iter(offsets.iter().copied()).into_array(),
        validity,
        &mut vortex::session::VortexSession::empty().create_execution_ctx(),
    )
    .unwrap()
    .into_array()
}

#[test]
fn native_relational_sort_prefix_keeps_rich_error_order_across_sorted_batches() {
    let items = list(
        PrimitiveArray::from_iter([1.0_f64, 2.0, f64::NAN]).into_array(),
        &[0, 1, 2, 3],
        Validity::NonNullable,
    );
    let amounts = decimal(&[7, 999, 8]);
    for reversed_fields in [false, true] {
        let mut fields = vec![("items", items.clone()), ("amount", amounts.clone())];
        if reversed_fields {
            fields.reverse();
        }
        fields.insert(
            0,
            ("key", PrimitiveArray::from_iter([1_i64, 2, 0]).into_array()),
        );
        let array = record(fields);
        let binding = spec(&array, &["key"]);
        for batch_rows in [1, 2, 3] {
            let session = ResidentVortexSession::new(2 << 20, 1).unwrap();
            let memory = session.memory().clone();
            let mut expected = None;
            for cap in [None, Some(0), Some(1), Some(2), Some(3), Some(7)] {
                session
                    .with_native_execution_context(&CancellationToken::default(), |context| {
                        let mut sort = Sort::new(&binding, context.memory())?;
                        // Gather spans separate native source chunks.
                        for range in [0..1, 1..3] {
                            sort.build(array.slice(range).map_err(vortex_error)?, context)?;
                        }
                        let error = sort
                            .finish_prefix(context, batch_rows, cap, false, &mut |_| Ok(()))
                            .unwrap_err()
                            .to_string();
                        assert_eq!(expected.get_or_insert(error.clone()), &error);
                        let first_error = if reversed_fields && batch_rows == 3 {
                            "precision"
                        } else {
                            "finite declared dtype"
                        };
                        assert!(error.contains(first_error), "{error}");
                        Ok(())
                    })
                    .unwrap();
                assert_eq!(memory.snapshot().reserved_bytes, 0);
            }
        }
    }
}

#[test]
fn native_relational_sort_prefix_ignores_masked_and_unreferenced_invalid_children() {
    let dictionary = DictArray::try_new(
        PrimitiveArray::from_iter([1_u8, 1, 1]).into_array(),
        decimal(&[999, 7]),
    )
    .unwrap()
    .into_array();
    let masked = list(
        PrimitiveArray::from_iter([f64::NAN, 2.0, 3.0]).into_array(),
        &[0, 1, 2, 3],
        Validity::from_iter([false, true, true]),
    );
    let array = record(vec![
        ("key", PrimitiveArray::from_iter([2_i64, 0, 1]).into_array()),
        ("amount", dictionary),
        ("items", masked),
    ]);
    let binding = spec(&array, &["key"]);
    for cap in [0, 1, 2, 3, 7] {
        let session = ResidentVortexSession::new(2 << 20, 1).unwrap();
        let memory = session.memory().clone();
        session
            .with_native_execution_context(&CancellationToken::default(), |context| {
                let mut sort = Sort::new(&binding, context.memory())?;
                for range in [0..1, 1..3] {
                    sort.build(array.slice(range).map_err(vortex_error)?, context)?;
                }
                let mut actual = Vec::new();
                sort.finish_prefix(context, 2, Some(cap), false, &mut |batch| {
                    let mut sink = super::super::super::collect::JsonRows::new(
                        context.memory(),
                        1 << 20,
                        false,
                    )?;
                    sink.append_native(&batch, context)?;
                    actual.extend(
                        serde_json::from_str::<Vec<serde_json::Value>>(sink.finish()?.value())
                            .unwrap(),
                    );
                    Ok(())
                })?;
                let expected = serde_json::json!([
                    {"key":0, "amount":"decimal128(2,0):7", "items":[2.0]},
                    {"key":1, "amount":"decimal128(2,0):7", "items":[3.0]},
                    {"key":2, "amount":"decimal128(2,0):7", "items":null}
                ]);
                assert_eq!(actual, expected.as_array().unwrap()[..cap.min(3)]);
                Ok(())
            })
            .unwrap();
        drop(session);
        assert_eq!(memory.snapshot().reserved_bytes, 0);
    }
}

#[test]
fn native_relational_sort_prefix_keeps_consumer_failure_before_later_rich_error() {
    let array = record(vec![
        ("key", PrimitiveArray::from_iter([0_i64, 1]).into_array()),
        ("amount", decimal(&[7, 999])),
    ]);
    let binding = spec(&array, &["key"]);
    for cap in [None, Some(1)] {
        let session = ResidentVortexSession::new(1 << 20, 1).unwrap();
        let memory = session.memory().clone();
        let error = session
            .with_native_execution_context(&CancellationToken::default(), |context| {
                let mut sort = Sort::new(&binding, context.memory())?;
                sort.build(array.clone(), context)?;
                sort.finish_prefix(context, 1, cap, false, &mut |_| {
                    Err(failed("original consumer failure"))
                })
            })
            .unwrap_err();
        assert!(error.to_string().contains("original consumer failure"));
        assert_eq!(memory.snapshot().reserved_bytes, 0);
    }
}

#[test]
fn native_relational_sort_prefix_reuses_complete_typed_validation_for_flat_payloads() {
    let array = record(vec![
        ("key", PrimitiveArray::from_iter(0_i64..257).into_array()),
        (
            "float",
            PrimitiveArray::from_option_iter(
                (0..257).map(|row| (row % 7 != 0).then_some(f64::from(row) - 128.0)),
            )
            .into_array(),
        ),
        (
            "flag",
            BoolArray::from_iter((0..257).map(|row| (row % 3 != 0).then_some(row % 2 == 0)))
                .into_array(),
        ),
        (
            "text",
            VarBinViewArray::from_iter_nullable_str(
                (0..257).map(|row| (row % 5 != 0).then_some("λ\0longer than twelve bytes")),
            )
            .into_array(),
        ),
    ]);
    let binding = spec(&array, &["key"]);
    let session = ResidentVortexSession::new(2 << 20, 1).unwrap();
    let memory = session.memory().clone();
    session
        .with_native_execution_context(&CancellationToken::default(), |context| {
            let mut sort = Sort::new(&binding, context.memory())?;
            for range in [0..65, 65..129, 129..257] {
                sort.build(array.slice(range).map_err(vortex_error)?, context)?;
            }
            #[cfg(debug_assertions)]
            let observation = super::super::super::native_relational_observation::Session::start();
            let mut emitted = 0;
            sort.finish_prefix(context, 31, Some(1), false, &mut |batch| {
                emitted += batch.len();
                Ok(())
            })?;
            assert_eq!(emitted, 1);
            #[cfg(debug_assertions)]
            {
                let counts = observation.finish();
                assert_eq!(counts.sort_output_rows_constructed, 1);
                assert_eq!(counts.sort_payload_rows_validated, 257);
                assert_eq!(counts.payload_numeric_validation_rows, 257 * 2);
                assert_eq!(counts.payload_boolean_validation_rows, 257);
                assert_eq!(counts.payload_variable_validation_rows, 257);
                assert_eq!(counts.payload_validation_scalar_calls, 0);
                assert_eq!(counts.numeric_output_copy_rows, 2);
            }
            Ok(())
        })
        .unwrap();
    drop(session);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

#[test]
fn native_relational_sort_prefix_preserves_float16_bits_and_common_type_promotion() {
    for policy in [FloatPolicy::Strict, FloatPolicy::Extended] {
        let bits = if policy == FloatPolicy::Strict {
            [0x8000, 0x0001, 0x3c00, 0xbc00]
        } else {
            [0x7c01, 0xfc2a, 0x7c00, 0x8000]
        };
        let array = record(vec![
            (
                "key",
                PrimitiveArray::from_iter([3_i64, 1, 2, 0]).into_array(),
            ),
            (
                "half",
                PrimitiveArray::from_iter(bits.map(f16::from_bits)).into_array(),
            ),
        ]);
        for promote in [false, true] {
            let mut binding = spec(&array, &["key"]);
            if promote {
                binding.fields[1].1 = DType::Primitive(PType::F64, Nullability::NonNullable);
            }
            for cap in [0, 1, 3, 4, 7] {
                let session =
                    ResidentVortexSession::new_with_float_policy(2 << 20, 1, policy).unwrap();
                let memory = session.memory().clone();
                let outputs = session
                    .with_native_execution_context(&CancellationToken::default(), |context| {
                        let mut sort = Sort::new(&binding, context.memory())?;
                        for range in [0..1, 1..4] {
                            sort.build(array.slice(range).map_err(vortex_error)?, context)?;
                        }
                        let mut outputs = Vec::new();
                        sort.finish_prefix(context, 4, Some(cap), false, &mut |batch| {
                            outputs.push(super::super::super::logical_field_from_native_array(
                                &batch, "half",
                            )?);
                            Ok(())
                        })?;
                        Ok(outputs)
                    })
                    .unwrap();
                // The retained primitive owner remains valid after Sort and its table drop.
                session
                    .with_native_execution_context(&CancellationToken::default(), |context| {
                        let mut execution = context.native_session().create_execution_ctx();
                        let mut actual = Vec::new();
                        for output in &outputs {
                            let output = output
                                .clone()
                                .execute::<PrimitiveArray>(&mut execution)
                                .map_err(vortex_error)?;
                            if promote {
                                actual.extend(
                                    output.as_slice::<f64>().iter().map(|value| value.to_bits()),
                                );
                            } else {
                                actual.extend(
                                    output
                                        .as_slice::<f16>()
                                        .iter()
                                        .map(|value| u64::from(value.to_bits())),
                                );
                            }
                        }
                        let expected = [3, 1, 2, 0]
                            .into_iter()
                            .take(cap)
                            .map(|row| {
                                if promote {
                                    shardloom_core::float_policy::widen_f16_bits(bits[row])
                                        .to_bits()
                                } else {
                                    u64::from(bits[row])
                                }
                            })
                            .collect::<Vec<_>>();
                        assert_eq!(actual, expected, "{policy:?}, promote={promote}, cap={cap}");
                        Ok(())
                    })
                    .unwrap();
                drop(outputs);
                drop(session);
                assert_eq!(memory.snapshot().reserved_bytes, 0);
            }
        }
    }
}

#[test]
fn native_relational_sort_prefix_rich_late_denial_and_cancellation_refund_credits() {
    let array = record(vec![
        ("key", PrimitiveArray::from_iter([0_i64, 1, 2]).into_array()),
        ("amount", decimal(&[7, 8, 9])),
    ]);
    let binding = spec(&array, &["key"]);
    for cancel in [false, true] {
        let session = ResidentVortexSession::new(2 << 20, 1).unwrap();
        let memory = session.memory().clone();
        let token = CancellationToken::default();
        let mut hold = None;
        let mut emitted = 0;
        let error = session
            .with_native_execution_context(&token, |context| {
                let mut sort = Sort::new(&binding, context.memory())?;
                sort.build(array.clone(), context)?;
                sort.finish_prefix(context, 1, Some(1), false, &mut |_| {
                    emitted += 1;
                    if cancel {
                        token.cancel();
                    } else {
                        let snapshot = memory.snapshot();
                        hold =
                            Some(memory.reserve(snapshot.limit_bytes - snapshot.reserved_bytes)?);
                    }
                    Ok(())
                })
            })
            .unwrap_err()
            .to_string();
        assert_eq!(emitted, 1);
        assert!(
            error.contains(if cancel {
                "cancel"
            } else {
                "reservation denied"
            }),
            "{error}"
        );
        drop(hold);
        assert_eq!(memory.snapshot().reserved_bytes, 0);
    }
}

#[test]
fn native_relational_sort_prefix_keeps_discarded_utf8_failure_coordinates() {
    use vortex::array::{
        ArrayParts,
        arrays::{VarBin, VarBinArray},
    };

    let binary = VarBinArray::from(vec![b"good".as_slice(), b"\xff".as_slice()]);
    let slots = binary.slots().iter().cloned().collect();
    let text = VarBinArray::try_from_parts(ArrayParts::new(
        VarBin,
        DType::Utf8(Nullability::NonNullable),
        2,
        binary.into_data(),
        slots,
    ))
    .unwrap()
    .into_array();
    let array = record(vec![
        ("key", PrimitiveArray::from_iter([0_i64, 1]).into_array()),
        ("text", text),
    ]);
    let binding = spec(&array, &["key"]);
    for batch_rows in [1, 2] {
        let session = ResidentVortexSession::new(1 << 20, 1).unwrap();
        let memory = session.memory().clone();
        let mut expected = None;
        for cap in [None, Some(0), Some(1)] {
            session
                .with_native_execution_context(&CancellationToken::default(), |context| {
                    let mut sort = Sort::new(&binding, context.memory())?;
                    sort.build(array.clone(), context)?;
                    let error = sort
                        .finish_prefix(context, batch_rows, cap, false, &mut |_| Ok(()))
                        .unwrap_err()
                        .to_string();
                    assert!(error.contains("utf-8"), "{error}");
                    assert_eq!(expected.get_or_insert(error.clone()), &error);
                    Ok(())
                })
                .unwrap();
            assert_eq!(memory.snapshot().reserved_bytes, 0);
        }
    }
}

#[test]
fn native_relational_sort_prefix_keeps_unobserved_payload_policy_barriers() {
    let payload = record(vec![(
        "float",
        PrimitiveArray::from_iter([f64::NAN, f64::INFINITY, -0.0]).into_array(),
    )]);
    let array = record(vec![
        ("key", PrimitiveArray::from_iter([2_i64, 0, 1]).into_array()),
        ("payload", payload),
    ]);
    for column_policy in [false, true] {
        let mut binding = spec(&array, &["key"]);
        if column_policy {
            binding.unobserved_fields.push(1);
        } else {
            binding.copy_policy = CopyPolicy::PreserveUnobserved;
        }
        let session = ResidentVortexSession::new(2 << 20, 1).unwrap();
        let memory = session.memory().clone();
        session
            .with_native_execution_context(&CancellationToken::default(), |context| {
                let mut sort = Sort::new(&binding, context.memory())?;
                sort.build(array.clone(), context)?;
                let mut emitted = 0;
                sort.finish_prefix(context, 2, Some(1), false, &mut |batch| {
                    emitted += batch.len();
                    Ok(())
                })?;
                assert_eq!(emitted, 3);
                Ok(())
            })
            .unwrap();
        assert_eq!(memory.snapshot().reserved_bytes, 0);
    }
}

#[test]
fn native_relational_sort_prefix_keeps_complete_cast_and_nullability_validation() {
    let cases = [
        (
            PrimitiveArray::from_iter([7_i16, 8]).into_array(),
            DType::Primitive(PType::I64, Nullability::NonNullable),
        ),
        (
            PrimitiveArray::from_option_iter([Some(7_i16), None]).into_array(),
            DType::Primitive(PType::I64, Nullability::NonNullable),
        ),
        (
            PrimitiveArray::from_option_iter([Some(7_i16), None]).into_array(),
            DType::Primitive(PType::I64, Nullability::Nullable),
        ),
        (
            PrimitiveArray::from_iter([7_i64, 128]).into_array(),
            DType::Primitive(PType::I8, Nullability::NonNullable),
        ),
    ];
    for (payload, target) in cases {
        let array = record(vec![
            ("key", PrimitiveArray::from_iter([0_i64, 1]).into_array()),
            ("payload", payload),
        ]);
        let mut binding = spec(&array, &["key"]);
        binding.fields[1].1 = target;
        let session = ResidentVortexSession::new(2 << 20, 1).unwrap();
        let memory = session.memory().clone();
        let mut expected: Option<std::result::Result<Vec<Option<i64>>, String>> = None;
        for cap in [None, Some(0), Some(1), Some(2), Some(7)] {
            let result = session
                .with_native_execution_context(&CancellationToken::default(), |context| {
                    let mut sort = Sort::new(&binding, context.memory())?;
                    sort.build(array.clone(), context)?;
                    let mut actual = Vec::new();
                    sort.finish_prefix(context, 2, cap, false, &mut |batch| {
                        let column = super::super::super::logical_field_from_native_array(
                            &batch, "payload",
                        )?;
                        let mut execution = context.native_session().create_execution_ctx();
                        for row in 0..column.len() {
                            actual.push(
                                column
                                    .execute_scalar(row, &mut execution)
                                    .map_err(vortex_error)?
                                    .as_primitive()
                                    .as_::<i64>(),
                            );
                        }
                        Ok(())
                    })?;
                    Ok(actual)
                })
                .map_err(|error| error.to_string());
            if let Some(expected) = &expected {
                match expected {
                    Ok(rows) => assert_eq!(result.unwrap(), rows[..cap.unwrap().min(rows.len())]),
                    Err(error) => assert_eq!(&result.unwrap_err(), error),
                }
            } else {
                expected = Some(result);
            }
            assert_eq!(memory.snapshot().reserved_bytes, 0);
        }
    }
}
