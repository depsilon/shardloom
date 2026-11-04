use super::*;
use crate::resident_session::ResidentVortexSession;
use shardloom_core::ColumnRef;
use shardloom_exec::compute_pool::CancellationToken;
use vortex::array::{IntoArray as _, arrays::PrimitiveArray};

#[path = "native_relational_sort_numeric_tests.rs"]
mod numeric_tests;

fn spec(array: &ArrayRef, names: &[&str]) -> Spec {
    let DType::Struct(fields, _) = array.dtype() else {
        panic!("expected struct")
    };
    Spec {
        fields: fields
            .names()
            .iter()
            .zip(fields.fields())
            .map(|(name, dtype)| (name.to_string(), dtype))
            .collect(),
        keys: names
            .iter()
            .map(|name| VortexRelationalOrderKey {
                column: ColumnRef::new(*name).unwrap(),
                descending: false,
                nulls: Some(NullOrder::First),
            })
            .collect(),
        names: names.iter().map(|name| (*name).to_owned()).collect(),
        copy_policy: CopyPolicy::ValidateValues,
    }
}

#[test]
fn native_relational_sort_resolves_each_row_once_for_tied_multikey_comparisons() {
    let values: Vec<_> = (0..257)
        .map(|row| {
            (
                (row % 19 != 0).then_some(i64::from(row % 3)),
                (row % 13 != 0).then_some(i64::from(row % 7)),
            )
        })
        .collect();
    let array = StructArray::try_new(
        FieldNames::from(["a", "b"]),
        vec![
            PrimitiveArray::from_option_iter(values.iter().map(|value| value.0)).into_array(),
            PrimitiveArray::from_option_iter(values.iter().map(|value| value.1)).into_array(),
        ],
        values.len(),
        Validity::NonNullable,
    )
    .unwrap()
    .into_array();
    let binding = spec(&array, &["a", "b"]);
    let mut expected: Vec<_> = (0..values.len()).collect();
    expected.sort_by_key(|row| (values[*row], *row));
    let mut comparison_count = 0;
    let mut reference: Vec<_> = (0..values.len()).collect();
    native_relational_order::sort(&mut reference, &CancellationToken::default(), |a, b| {
        comparison_count += 1;
        Ok((values[a], a).cmp(&(values[b], b)))
    })
    .unwrap();
    assert_eq!(reference, expected);
    let session = ResidentVortexSession::new(1 << 20, 1).unwrap();
    let memory = session.memory().clone();
    session
        .with_native_execution_context(&CancellationToken::default(), |context| {
            let mut sort = Sort::new(&binding, context.memory())?;
            for range in [0..0, 0..1, 1..64, 64..64, 64..256, 256..257, 257..257] {
                sort.build(array.slice(range).map_err(vortex_error)?, context)?;
            }
            #[cfg(debug_assertions)]
            let observation = super::super::native_relational_observation::Session::start();
            let actual = sort.ordered_rows(context)?;
            #[cfg(debug_assertions)]
            {
                let counts = observation.finish();
                let adjacent = values
                    .windows(2)
                    .position(|pair| pair[0] > pair[1])
                    .unwrap()
                    + 1;
                assert_eq!(counts.order_check_comparisons, adjacent as u64);
                assert_eq!(counts.order_heap_sorts, 1);
                assert_eq!(
                    counts.row_location_searches,
                    (comparison_count + adjacent as u64) * 2
                );
            }
            assert_eq!(actual.values, expected);
            Ok(())
        })
        .unwrap();
    drop(session);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

#[test]
fn native_relational_sort_validates_payload_after_the_consumer_has_enough_rows() {
    let array = StructArray::try_new(
        FieldNames::from(["key", "payload"]),
        vec![
            PrimitiveArray::from_iter([0i64, 1]).into_array(),
            PrimitiveArray::from_iter([1.0f64, f64::NAN]).into_array(),
        ],
        2,
        Validity::NonNullable,
    )
    .unwrap()
    .into_array();
    let binding = spec(&array, &["key"]);
    let session = ResidentVortexSession::new(1 << 20, 1).unwrap();
    let memory = session.memory().clone();
    session
        .with_native_execution_context(&CancellationToken::default(), |context| {
            let mut sort = Sort::new(&binding, context.memory())?;
            sort.build(array, context)?;
            let mut emitted = 0;
            #[cfg(debug_assertions)]
            let observation = super::super::native_relational_observation::Session::start();
            let error = sort
                .finish(context, 1, &mut |batch| {
                    // A downstream LIMIT has no remaining output demand after
                    // this row, but the next payload must still be validated.
                    assert_eq!(batch.len(), 1);
                    emitted += batch.len();
                    Ok(())
                })
                .unwrap_err();
            #[cfg(debug_assertions)]
            {
                let counts = observation.finish();
                assert_eq!(counts.ordered_input_sorts, 1);
                assert_eq!(counts.order_heap_sorts, 0);
            }
            assert_eq!(emitted, 1);
            assert!(
                error.to_string().contains("finite declared dtype"),
                "{error}"
            );
            Ok(())
        })
        .unwrap();
    drop(session);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

#[test]
fn native_relational_sort_validates_every_key_before_ordering() {
    let array = StructArray::try_new(
        FieldNames::from(["first", "later"]),
        vec![
            PrimitiveArray::from_iter([0i64, 1]).into_array(),
            PrimitiveArray::from_option_iter([Some(0.0f64), Some(f64::NAN)]).into_array(),
        ],
        2,
        Validity::NonNullable,
    )
    .unwrap()
    .into_array();
    let binding = spec(&array, &["first", "later"]);
    let session = ResidentVortexSession::new(1 << 20, 1).unwrap();
    session
        .with_native_execution_context(&CancellationToken::default(), |context| {
            let mut sort = Sort::new(&binding, context.memory())?;
            #[cfg(debug_assertions)]
            let observation = super::super::native_relational_observation::Session::start();
            let error = sort.build(array, context).unwrap_err();
            #[cfg(debug_assertions)]
            assert_eq!(observation.finish().order_checks, 0);
            assert!(error.to_string().contains("nonfinite"), "{error}");
            assert_eq!(sort.table.rows(), 0);
            Ok(())
        })
        .unwrap();
}

#[test]
fn ordered_input_full_sort_declines_cross_batch_types_before_the_unchanged_heap_error() {
    use vortex::array::arrays::VarBinViewArray;
    let array = |key: ArrayRef| {
        let length = key.len();
        StructArray::new(
            FieldNames::from(["key"]),
            vec![key],
            length,
            Validity::NonNullable,
        )
        .into_array()
    };
    let first = array(PrimitiveArray::from_iter([0i64, 1]).into_array());
    let later = array(VarBinViewArray::from_iter_str(["x", "y"]).into_array());
    let binding = spec(&first, &["key"]);
    let session = ResidentVortexSession::new(1 << 20, 1).unwrap();
    let memory = session.memory().clone();
    session
        .with_native_execution_context(&CancellationToken::default(), |context| {
            let mut sorter = Sort::new(&binding, context.memory())?;
            sorter.build(first, context)?;
            sorter.build(later, context)?;
            let mut original = (0..4).collect::<Vec<_>>();
            let expected =
                native_relational_order::sort(&mut original, context.cancellation(), |a, b| {
                    sorter.compare_rows(a, b)
                })
                .unwrap_err();
            #[cfg(debug_assertions)]
            let observation = super::super::native_relational_observation::Session::start();
            let Err(actual) = sorter.ordered_rows(context) else {
                panic!("mixed comparison domains must retain their original error");
            };
            assert_eq!(actual.to_string(), expected.to_string());
            #[cfg(debug_assertions)]
            {
                let counts = observation.finish();
                assert_eq!(counts.order_checks, 0);
                assert_eq!(counts.order_heap_sorts, 1);
            }
            Ok(())
        })
        .unwrap();
    drop(session);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

#[test]
fn ordered_input_full_sort_keeps_memory_denial_before_comparison() {
    let array = StructArray::new(
        FieldNames::from(["key"]),
        vec![PrimitiveArray::from_iter(0_i64..33).into_array()],
        33,
        Validity::NonNullable,
    )
    .into_array();
    let binding = spec(&array, &["key"]);
    let session = ResidentVortexSession::new(1 << 20, 1).unwrap();
    let memory = session.memory().clone();
    session
        .with_native_execution_context(&CancellationToken::default(), |context| {
            let mut sorter = Sort::new(&binding, context.memory())?;
            sorter.build(array, context)?;
            let snapshot = memory.snapshot();
            let _occupied = memory.reserve(snapshot.limit_bytes - snapshot.reserved_bytes)?;
            #[cfg(debug_assertions)]
            let observation = super::super::native_relational_observation::Session::start();
            assert!(sorter.ordered_rows(context).is_err());
            #[cfg(debug_assertions)]
            assert_eq!(observation.finish().order_checks, 0);
            assert!(memory.snapshot().denied_reservations > 0);
            Ok(())
        })
        .unwrap();
    drop(session);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

#[test]
#[allow(clippy::too_many_lines)] // Keep the independent ordering oracle beside both ownership modes.
fn native_relational_sort_prefix_preserves_nullable_multikey_ties_and_payloads() {
    use vortex::array::{VortexSessionExecute as _, arrays::VarBinViewArray};

    let values: Vec<_> = (0..257)
        .map(|row| {
            (
                (row % 19 != 0).then_some(i64::from(row % 3)),
                (row % 13 != 0).then_some(i64::from(row % 7)),
            )
        })
        .collect();
    let payloads: Vec<_> = (0..values.len())
        .map(|row| (row % 11 != 0).then(|| format!("λ\0{row:04}")))
        .collect();
    let array = StructArray::try_new(
        FieldNames::from(["a", "b", "id", "payload"]),
        vec![
            PrimitiveArray::from_option_iter(values.iter().map(|value| value.0)).into_array(),
            PrimitiveArray::from_option_iter(values.iter().map(|value| value.1)).into_array(),
            PrimitiveArray::from_iter(0_i64..257).into_array(),
            VarBinViewArray::from_iter(
                payloads.iter().map(|value| value.as_deref()),
                DType::Utf8(vortex::dtype::Nullability::Nullable),
            )
            .into_array(),
        ],
        values.len(),
        Validity::NonNullable,
    )
    .unwrap()
    .into_array();
    let mut binding = spec(&array, &["a", "b"]);
    binding.keys[1].descending = true;
    binding.keys[1].nulls = Some(NullOrder::Last);
    let mut expected: Vec<_> = (0..values.len()).collect();
    expected.sort_by(|&left, &right| {
        values[left]
            .0
            .cmp(&values[right].0)
            .then_with(|| match (values[left].1, values[right].1) {
                (None, None) => Ordering::Equal,
                (None, Some(_)) => Ordering::Greater,
                (Some(_), None) => Ordering::Less,
                (Some(left), Some(right)) => right.cmp(&left),
            })
            .then(left.cmp(&right))
    });
    for validated in [false, true] {
        for cap in [0, 1, 7, 64, 65, 256, 257, 1000] {
            let session = ResidentVortexSession::new(2 << 20, 1).unwrap();
            let memory = session.memory().clone();
            let mut outputs = Vec::new();
            session
                .with_native_execution_context(&CancellationToken::default(), |context| {
                    let mut sort = Sort::new(&binding, context.memory())?;
                    for range in [0..0, 0..1, 1..64, 64..64, 64..256, 256..257, 257..257] {
                        let source = array.slice(range).map_err(vortex_error)?;
                        let source = if validated {
                            super::super::native_payload::detach(&source, context)?
                        } else {
                            source
                        };
                        sort.build(source, context)?;
                    }
                    #[cfg(debug_assertions)]
                    let observation = super::super::native_relational_observation::Session::start();
                    sort.finish_prefix(context, 31, Some(cap), validated, &mut |batch| {
                        outputs.push(batch);
                        Ok(())
                    })?;
                    #[cfg(debug_assertions)]
                    {
                        let counts = observation.finish();
                        assert_eq!(
                            counts.sort_output_rows_constructed,
                            cap.min(values.len()) as u64
                        );
                        assert_eq!(
                            counts.sort_retained_ordinals,
                            if validated && cap <= values.len() / 4 {
                                cap
                            } else {
                                values.len()
                            } as u64
                        );
                        assert_eq!(
                            counts.sort_payload_rows_validated,
                            if validated {
                                cap.min(values.len())
                            } else {
                                values.len()
                            } as u64
                        );
                    }
                    Ok(())
                })
                .unwrap();
            // Native output owners survive the sort and its source table.
            session
                .with_native_execution_context(&CancellationToken::default(), |context| {
                    let mut actual = Vec::new();
                    let mut execution = context.native_session().create_execution_ctx();
                    for batch in &outputs {
                        let ids = super::super::logical_field_from_native_array(batch, "id")?;
                        let text = super::super::logical_field_from_native_array(batch, "payload")?;
                        for row in 0..batch.len() {
                            let id = ids
                                .execute_scalar(row, &mut execution)
                                .map_err(vortex_error)?
                                .as_primitive()
                                .as_::<usize>()
                                .expect("nonnegative fixture id");
                            let value = super::super::result_batch::scalar_value(
                                &text,
                                row,
                                &mut execution,
                            )?
                            .into_json()?;
                            assert_eq!(value, serde_json::json!(payloads[id]));
                            actual.push(id);
                        }
                    }
                    assert_eq!(actual, expected[..cap.min(values.len())]);
                    Ok(())
                })
                .unwrap();
            drop(outputs);
            drop(session);
            assert_eq!(memory.snapshot().reserved_bytes, 0);
        }
    }
}

#[test]
fn native_relational_sort_prefix_validates_discarded_payload_in_original_output_order() {
    let array = StructArray::try_new(
        FieldNames::from(["key", "payload"]),
        vec![
            PrimitiveArray::from_iter([1_i64, 0]).into_array(),
            PrimitiveArray::from_iter([f64::NAN, 1.0]).into_array(),
        ],
        2,
        Validity::NonNullable,
    )
    .unwrap()
    .into_array();
    let binding = spec(&array, &["key"]);
    let session = ResidentVortexSession::new(1 << 20, 1).unwrap();
    let memory = session.memory().clone();
    let mut expected_error = None;
    for cap in [None, Some(0), Some(1)] {
        session
            .with_native_execution_context(&CancellationToken::default(), |context| {
                let mut sort = Sort::new(&binding, context.memory())?;
                sort.build(array.clone(), context)?;
                let mut emitted = 0;
                let error = sort
                    .finish_prefix(context, 1, cap, false, &mut |batch| {
                        emitted += batch.len();
                        Ok(())
                    })
                    .unwrap_err()
                    .to_string();
                assert_eq!(emitted, usize::from(cap != Some(0)));
                assert!(error.contains("finite declared dtype"), "{error}");
                assert_eq!(expected_error.get_or_insert(error.clone()), &error);
                Ok(())
            })
            .unwrap();
    }
    drop(session);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

#[test]
fn native_relational_sort_prefix_retains_full_gather_for_resident_nested_and_decimal_payloads() {
    use vortex::array::{arrays::DecimalArray, dtype::DecimalDType};

    let payloads = [
        StructArray::try_new(
            FieldNames::from(["nested"]),
            vec![PrimitiveArray::from_iter(0_i64..33).into_array()],
            33,
            Validity::NonNullable,
        )
        .unwrap()
        .into_array(),
        DecimalArray::from_option_iter((0_i128..33).map(Some), DecimalDType::new(3, 0))
            .into_array(),
    ];
    for payload in payloads {
        let array = StructArray::try_new(
            FieldNames::from(["key", "payload"]),
            vec![PrimitiveArray::from_iter(0_i64..33).into_array(), payload],
            33,
            Validity::NonNullable,
        )
        .unwrap()
        .into_array();
        let binding = spec(&array, &["key"]);
        let session = ResidentVortexSession::new(1 << 20, 1).unwrap();
        let memory = session.memory().clone();
        session
            .with_native_execution_context(&CancellationToken::default(), |context| {
                let mut sort = Sort::new(&binding, context.memory())?;
                sort.build(array, context)?;
                let mut emitted = 0;
                sort.finish_prefix(context, 7, Some(1), false, &mut |batch| {
                    emitted += batch.len();
                    Ok(())
                })?;
                assert_eq!(emitted, 33);
                Ok(())
            })
            .unwrap();
        drop(session);
        assert_eq!(memory.snapshot().reserved_bytes, 0);
    }
}

#[test]
fn native_relational_sort_prefix_consumer_refusal_cancellation_and_denial_release_ownership() {
    let array = StructArray::try_new(
        FieldNames::from(["key"]),
        vec![PrimitiveArray::from_iter(0_i64..257).into_array()],
        257,
        Validity::NonNullable,
    )
    .unwrap()
    .into_array();
    let binding = spec(&array, &["key"]);
    for validated in [false, true] {
        for failure in ["consumer", "cancel", "grant"] {
            let session = ResidentVortexSession::new(1 << 20, 1).unwrap();
            let memory = session.memory().clone();
            let token = CancellationToken::default();
            let error = session
                .with_native_execution_context(&token, |context| {
                    let mut sort = Sort::new(&binding, context.memory())?;
                    let input = if validated {
                        super::super::native_payload::detach(&array, context)?
                    } else {
                        array.clone()
                    };
                    sort.build(input, context)?;
                    let _occupied = if failure == "grant" {
                        let snapshot = context.memory().snapshot();
                        Some(
                            context
                                .memory()
                                .reserve(snapshot.limit_bytes - snapshot.reserved_bytes)?,
                        )
                    } else {
                        None
                    };
                    sort.finish_prefix(context, 1, Some(7), validated, &mut |_| {
                        if failure == "cancel" {
                            token.cancel();
                            Ok(())
                        } else {
                            Err(failed("prefix consumer refused"))
                        }
                    })
                })
                .unwrap_err();
            if failure == "consumer" {
                assert!(error.to_string().contains("prefix consumer refused"));
            }
            if failure == "grant" {
                assert!(memory.snapshot().denied_reservations > 0);
            }
            drop(session);
            assert_eq!(memory.snapshot().reserved_bytes, 0);
        }
    }
}
