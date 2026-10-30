use super::*;
use crate::local_primitives::native_relational_sort::numeric;
use shardloom_core::FloatPolicy;
use vortex::array::{
    arrays::{BoolArray, ChunkedArray, ConstantArray, DictArray, VarBinArray},
    scalar::Scalar,
};

fn record(column: &ArrayRef) -> ArrayRef {
    StructArray::try_new(
        FieldNames::from(["key", "tie"]),
        vec![
            column.clone(),
            PrimitiveArray::from_option_iter(
                (0..column.len())
                    .map(|row| (row % 11 != 0).then(|| u16::try_from(row % 5).unwrap())),
            )
            .into_array(),
        ],
        column.len(),
        Validity::NonNullable,
    )
    .unwrap()
    .into_array()
}

fn independent_order<T: PartialOrd>(
    left: Option<T>,
    right: Option<T>,
    descending: bool,
    first: bool,
) -> Ordering {
    match (left, right) {
        (None, None) => Ordering::Equal,
        (None, Some(_)) => {
            if first {
                Ordering::Less
            } else {
                Ordering::Greater
            }
        }
        (Some(_), None) => {
            if first {
                Ordering::Greater
            } else {
                Ordering::Less
            }
        }
        (Some(left), Some(right)) => {
            let order = left.partial_cmp(&right).unwrap();
            if descending { order.reverse() } else { order }
        }
    }
}

fn check(
    column: &ArrayRef,
    policy: FloatPolicy,
    compare: impl Fn(usize, usize, bool, bool) -> Ordering,
) {
    let input = record(column);
    for descending in [false, true] {
        for first in [false, true] {
            for multikey in [false, true] {
                let mut binding = spec(&input, if multikey { &["key", "tie"] } else { &["key"] });
                binding.keys[0].descending = descending;
                binding.keys[0].nulls = Some(if first {
                    NullOrder::First
                } else {
                    NullOrder::Last
                });
                if multikey {
                    binding.keys[1].descending = !descending;
                }
                let mut expected: Vec<_> = (0..input.len()).collect();
                expected.sort_by(|&left, &right| {
                    compare(left, right, descending, first)
                        .then_with(|| {
                            if multikey {
                                independent_order(
                                    (left % 11 != 0).then_some(left % 5),
                                    (right % 11 != 0).then_some(right % 5),
                                    !descending,
                                    true,
                                )
                            } else {
                                Ordering::Equal
                            }
                        })
                        .then(left.cmp(&right))
                });
                let session =
                    ResidentVortexSession::new_with_float_policy(8 << 20, 1, policy).unwrap();
                let memory = session.memory().clone();
                session
                    .with_native_execution_context(&CancellationToken::default(), |context| {
                        let mut sort = Sort::new(&binding, &memory)?;
                        sort.build(input.slice(0..0).unwrap(), context)?;
                        for start in (0..input.len()).step_by(17) {
                            sort.build(input.slice(start..(start + 17).min(input.len())).unwrap(), context)?;
                        }
                        for cap in [0, 1, 7, 64, input.len(), input.len() + 7] {
                            #[cfg(debug_assertions)]
                            let observation = crate::local_primitives::native_relational_observation::Session::start();
                            let baseline = memory.snapshot().reserved_bytes;
                            let actual = sort.ordered_prefix(context, Some(cap))?;
                            assert_eq!(actual.values, expected[..cap.min(input.len())]);
                            #[cfg(debug_assertions)]
                            {
                                let counts = observation.finish();
                                assert_eq!(counts.row_location_searches, 0);
                                assert_eq!(counts.sort_numeric_prefixes, u64::from(cap != 0 && !input.is_empty()));
                                if counts.sort_numeric_prefixes != 0 {
                                    assert_eq!(counts.sort_numeric_prefix_rows, input.len() as u64);
                                    assert_eq!(counts.sort_numeric_prefix_key_views,
                                        input.len().div_ceil(17) as u64 * binding.keys.len() as u64);
                                }
                            }
                            drop(actual);
                            assert_eq!(memory.snapshot().reserved_bytes, baseline);
                        }
                        Ok(())
                    })
                    .unwrap();
                drop(session);
                assert_eq!(memory.snapshot().reserved_bytes, 0);
            }
        }
    }
}

#[test]
fn numeric_prefix_preserves_original_integer_widths_nulls_ties_and_endpoints() {
    macro_rules! integer_cases { ($($ty:ty),+) => { $(
        for length in [0, 3, 131] {
            let values: Vec<Option<$ty>> = (0..length).map(|row| match row % 7 {
                0 => None, 1 | 5 => Some(<$ty>::MIN), 2 => Some(<$ty>::MAX),
                3 => Some(0), _ => Some(1),
            }).collect();
            let column = PrimitiveArray::from_option_iter(values.iter().copied()).into_array();
            check(&column, FloatPolicy::Strict, |a, b, desc, first| {
                independent_order(values[a], values[b], desc, first)
            });
        }
    )+ }; }
    integer_cases!(i8, i16, i32, i64, u8, u16, u32, u64);
    let values = [
        Some(u64::MAX),
        Some((1 << 53) + 1),
        Some(1 << 53),
        Some(i64::MAX as u64),
    ];
    check(
        &PrimitiveArray::from_option_iter(values).into_array(),
        FloatPolicy::Strict,
        |a, b, desc, first| independent_order(values[a], values[b], desc, first),
    );
    check(
        &PrimitiveArray::from_option_iter([None::<i16>; 131]).into_array(),
        FloatPolicy::Strict,
        |_, _, _, _| Ordering::Equal,
    );
}

fn float_reference(left: f64, right: f64) -> Ordering {
    match (left.is_nan(), right.is_nan()) {
        (true, true) => Ordering::Equal,
        (true, false) => Ordering::Greater,
        (false, true) => Ordering::Less,
        (false, false) => left.partial_cmp(&right).unwrap(),
    }
}

#[test]
fn numeric_prefix_preserves_analytical_float_identity_and_both_policies() {
    for policy in [FloatPolicy::Strict, FloatPolicy::Extended] {
        let mut values = vec![
            Some(-0.0),
            Some(0.0),
            None,
            Some(-7.5),
            Some(7.5),
            Some(f64::MAX),
            Some(-f64::MAX),
        ];
        if policy == FloatPolicy::Extended {
            values.extend([
                Some(f64::INFINITY),
                Some(f64::NEG_INFINITY),
                Some(f64::from_bits(0xfff8_0000_0000_4321)),
                Some(f64::from_bits(0x7ff8_0000_0000_1234)),
            ]);
        }
        let values: Vec<_> = values.iter().cycle().take(131).copied().collect();
        let column = PrimitiveArray::from_option_iter(values.iter().copied()).into_array();
        check(&column, policy, |a, b, desc, first| {
            match (values[a], values[b]) {
                (Some(a), Some(b)) => {
                    let order = float_reference(a, b);
                    if desc { order.reverse() } else { order }
                }
                _ => independent_order(values[a].map(|_| 0), values[b].map(|_| 0), desc, first),
            }
        });
        let values: Vec<_> = values
            .iter()
            .map(|value| {
                value.map(|value| {
                    #[allow(clippy::cast_possible_truncation)]
                    // Include exact f32 conversion as a separate declared domain.
                    {
                        if value.is_finite() {
                            value.clamp(-f64::from(f32::MAX), f64::from(f32::MAX)) as f32
                        } else {
                            value as f32
                        }
                    }
                })
            })
            .collect();
        let column = PrimitiveArray::from_option_iter(values.iter().copied()).into_array();
        check(&column, policy, |a, b, desc, first| {
            match (values[a], values[b]) {
                (Some(a), Some(b)) => {
                    let order = float_reference(f64::from(a), f64::from(b));
                    if desc { order.reverse() } else { order }
                }
                _ => independent_order(values[a].map(|_| 0), values[b].map(|_| 0), desc, first),
            }
        });
    }
}

#[test]
fn numeric_prefix_reuses_prepared_encoded_numeric_owners() {
    let constant = ConstantArray::new(Scalar::from(-7i32), 131).into_array();
    check(&constant, FloatPolicy::Strict, |_, _, _, _| Ordering::Equal);
    let domain = PrimitiveArray::from_option_iter([Some(i64::MAX), None, Some(i64::MIN), Some(7)])
        .into_array();
    let codes: Vec<_> = (0..131)
        .map(|row| (row % 13 != 0).then(|| u8::try_from(row % 4).unwrap()))
        .collect();
    let dictionary = DictArray::try_new(
        PrimitiveArray::from_option_iter(codes.iter().copied()).into_array(),
        domain,
    )
    .unwrap()
    .into_array();
    let values: Vec<_> = codes
        .iter()
        .map(|code| {
            code.and_then(|code| [Some(i64::MAX), None, Some(i64::MIN), Some(7)][usize::from(code)])
        })
        .collect();
    check(&dictionary, FloatPolicy::Strict, |a, b, desc, first| {
        independent_order(values[a], values[b], desc, first)
    });
    let chunked = ChunkedArray::try_new(
        vec![
            dictionary.slice(0..31).unwrap(),
            dictionary.slice(31..131).unwrap(),
        ],
        dictionary.dtype().clone(),
    )
    .unwrap()
    .into_array();
    check(&chunked, FloatPolicy::Strict, |a, b, desc, first| {
        independent_order(values[a], values[b], desc, first)
    });
}

#[test]
fn numeric_prefix_declines_mixed_domains_and_text_without_changing_general_errors() {
    let session = ResidentVortexSession::new(2 << 20, 1).unwrap();
    let memory = session.memory().clone();
    session
        .with_native_execution_context(&CancellationToken::default(), |context| {
            for columns in [
                vec![
                    PrimitiveArray::from_iter([u64::MAX, 3]).into_array(),
                    PrimitiveArray::from_iter([-1i64, i64::MAX]).into_array(),
                ],
                vec![
                    PrimitiveArray::from_iter([1i32, 0]).into_array(),
                    PrimitiveArray::from_iter([2i64, -7]).into_array(),
                ],
                vec![
                    PrimitiveArray::from_iter([0i64, 3]).into_array(),
                    VarBinArray::from(vec!["a", "b"]).into_array(),
                ],
                vec![
                    VarBinArray::from(vec!["z", "a"]).into_array(),
                    VarBinArray::from(vec!["a", "y"]).into_array(),
                ],
            ] {
                let binding = spec(&record(&columns[0]), &["key"]);
                let mut sort = Sort::new(&binding, &memory)?;
                for column in columns {
                    sort.build(record(&column), context)?;
                }
                let before = memory.snapshot().reserved_bytes;
                let mut ordinals = vec![0, 1];
                assert!(!numeric::try_prefix(&sort, &mut ordinals, context)?);
                assert_eq!(ordinals, [0, 1]);
                assert_eq!(memory.snapshot().reserved_bytes, before);
                let mut expected = vec![0, 1];
                let reference = native_relational_order::select_prefix(
                    &mut expected,
                    sort.table.rows(),
                    context.cancellation(),
                    |a, b| sort.compare_rows(a, b),
                );
                let actual = sort.ordered_prefix(context, Some(2));
                assert_eq!(
                    actual.as_ref().err().map(ToString::to_string),
                    reference.as_ref().err().map(ToString::to_string)
                );
                if let Ok(actual) = actual {
                    assert_eq!(actual.values, expected);
                }
            }
            Ok(())
        })
        .unwrap();
    drop(session);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

#[test]
fn numeric_prefix_optional_memory_decline_and_cancel_preserve_state_and_credits() {
    let session = ResidentVortexSession::new(2 << 20, 1).unwrap();
    let memory = session.memory().clone();
    let token = CancellationToken::default();
    let error = session
        .with_native_execution_context(&token, |context| {
            let array = record(&PrimitiveArray::from_iter((0_i64..131).rev()).into_array());
            let binding = spec(&array, &["key"]);
            let mut sort = Sort::new(&binding, &memory)?;
            sort.build(array, context)?;
            let before = memory.snapshot().reserved_bytes;
            let deny = memory.reserve((2 << 20) - before - 128)?;
            #[cfg(debug_assertions)]
            let observation =
                crate::local_primitives::native_relational_observation::Session::start();
            let actual = sort.ordered_prefix(context, Some(7))?;
            assert_eq!(actual.values, (124..131).rev().collect::<Vec<_>>());
            #[cfg(debug_assertions)]
            {
                let counts = observation.finish();
                assert_eq!(counts.sort_numeric_prefixes, 0);
                assert!(counts.row_location_searches > 0);
            }
            drop((actual, deny));
            assert_eq!(memory.snapshot().reserved_bytes, before);
            let mut ordinals: Vec<_> = (0..7).collect();
            assert!(numeric::try_prefix(&sort, &mut ordinals, context)?);
            assert_eq!(ordinals, (124..131).rev().collect::<Vec<_>>());
            assert_eq!(memory.snapshot().reserved_bytes, before);
            token.cancel();
            let old = ordinals.clone();
            assert!(numeric::try_prefix(&sort, &mut ordinals, context).is_err());
            assert_eq!(ordinals, old);
            assert_eq!(memory.snapshot().reserved_bytes, before);
            Ok(())
        })
        .unwrap_err();
    assert!(error.to_string().contains("cancel"));
    drop(session);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

#[test]
fn numeric_prefix_uses_parent_validity_and_avoids_generic_row_locations() {
    let input = StructArray::try_new(
        FieldNames::from(["key"]),
        vec![
            PrimitiveArray::from_option_iter([
                Some(f64::NAN),
                Some(-3.0),
                Some(f64::INFINITY),
                None,
                Some(-f64::MAX),
            ])
            .into_array(),
        ],
        5,
        Validity::Array(BoolArray::from_iter([false, true, false, true, true]).into_array()),
    )
    .unwrap()
    .into_array();
    let session = ResidentVortexSession::new(2 << 20, 1).unwrap();
    session
        .with_native_execution_context(&CancellationToken::default(), |context| {
            let binding = spec(&input, &["key"]);
            let mut sort = Sort::new(&binding, context.memory())?;
            sort.build(input, context)?;
            #[cfg(debug_assertions)]
            let observation =
                crate::local_primitives::native_relational_observation::Session::start();
            let actual = sort.ordered_prefix(context, Some(4))?;
            assert_eq!(actual.values, [0, 2, 3, 4]);
            #[cfg(debug_assertions)]
            {
                let counts = observation.finish();
                assert_eq!(counts.sort_numeric_prefixes, 1);
                assert!(counts.sort_numeric_prefix_comparisons > 0);
                assert_eq!(counts.row_location_searches, 0);
            }
            #[cfg(debug_assertions)]
            let observation =
                crate::local_primitives::native_relational_observation::Session::start();
            let mut original = vec![0, 1, 2, 3];
            native_relational_order::select_prefix(
                &mut original,
                5,
                context.cancellation(),
                |a, b| sort.compare_rows(a, b),
            )?;
            assert_eq!(actual.values, original);
            #[cfg(debug_assertions)]
            assert!(observation.finish().row_location_searches > 0);
            Ok(())
        })
        .unwrap();
}

#[test]
fn numeric_prefix_declines_large_prefix_without_allocating_or_mutating_ordinals() {
    let column = PrimitiveArray::from_iter((0_i64..16_385).rev()).into_array();
    let input = record(&column);
    let binding = spec(&input, &["key"]);
    let session = ResidentVortexSession::new(8 << 20, 1).unwrap();
    session
        .with_native_execution_context(&CancellationToken::default(), |context| {
            let mut sort = Sort::new(&binding, context.memory())?;
            sort.build(input, context)?;
            let before = context.memory().snapshot().reserved_bytes;
            let mut ordinals: Vec<_> = (0..16_385).collect();
            assert!(!numeric::try_prefix(&sort, &mut ordinals, context)?);
            assert_eq!(ordinals, (0..16_385).collect::<Vec<_>>());
            assert_eq!(context.memory().snapshot().reserved_bytes, before);
            Ok(())
        })
        .unwrap();
}
