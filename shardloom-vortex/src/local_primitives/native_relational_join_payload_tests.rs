//! A payload mask preserves the selected-value checks of complete join output.

use super::*;
use vortex::{array::arrays::DecimalArray, array::dtype::DecimalDType, buffer::Buffer};

fn right_payload(parent_valid: bool) -> ArrayRef {
    let nested = StructArray::new(
        FieldNames::from(["value"]),
        vec![PrimitiveArray::from_iter([1.0, f64::NAN]).into_array()],
        2,
        Validity::from_iter([true, parent_valid]),
    )
    .into_array();
    let amounts = DecimalArray::try_new(
        Buffer::from(vec![7_i128, 999]),
        DecimalDType::new(2, 0),
        Validity::NonNullable,
    )
    .unwrap()
    .into_array();
    StructArray::new(
        FieldNames::from(["key", "id", "nested", "amount"]),
        vec![
            PrimitiveArray::from_option_iter([Some(1_u64), Some(99)]).into_array(),
            PrimitiveArray::from_iter([20_u32, 21]).into_array(),
            nested,
            amounts,
        ],
        2,
        Validity::NonNullable,
    )
    .into_array()
}

#[test]
fn prepared_join_payload_mask_matches_nested_decimal_errors_and_outer_selections() {
    for kind in [Kind::Inner, Kind::Left, Kind::Right, Kind::Full] {
        for selected in [false, true] {
            for parent_valid in [false, true] {
                for decimal_first in [false, true] {
                    let right = right_payload(parent_valid);
                    let mut binding = spec(kind);
                    let names = if decimal_first {
                        ["amount", "nested"]
                    } else {
                        ["nested", "amount"]
                    };
                    for name in names {
                        let column = logical_field_from_native_array(&right, name).unwrap();
                        let dtype = if matches!(kind, Kind::Left | Kind::Full) {
                            column.dtype().as_nullable()
                        } else {
                            column.dtype().clone()
                        };
                        binding.fields.push((name.into(), dtype));
                        binding.columns.push((Side::Right, name.into()));
                    }
                    let mut outcomes = Vec::new();
                    for masked in [false, true] {
                        binding.retained = masked.then_some(vec![true, false, false, false]);
                        let session = ResidentVortexSession::new(8 << 20, 1).unwrap();
                        let mut delivered = Vec::new();
                        let result = session.with_native_execution_context(
                            &CancellationToken::default(),
                            |context| {
                                let mut join = Join::new(&binding, context.memory())?;
                                join.build(right.clone(), context, 2)?;
                                let mut consume = |array: ArrayRef| {
                                    delivered.extend(pairs(&array).into_iter().map(|row| row.0));
                                    Ok(())
                                };
                                join.consume(
                                    input(
                                        &[Some(1), Some(if selected { 99 } else { 2 })],
                                        &[10, 11],
                                    ),
                                    context,
                                    2,
                                    &mut consume,
                                )?;
                                join.finish(context, 2, &mut consume)?;
                                Ok(())
                            },
                        );
                        assert_eq!(session.memory().snapshot().reserved_bytes, 0);
                        outcomes.push((result.map_err(|error| error.to_string()), delivered));
                    }
                    assert_eq!(outcomes[0], outcomes[1]);
                    if selected || matches!(kind, Kind::Right | Kind::Full) {
                        let expected = if decimal_first || !parent_valid {
                            "precision"
                        } else {
                            "finite declared dtype"
                        };
                        assert!(outcomes[1].0.as_ref().unwrap_err().contains(expected));
                    } else {
                        assert!(outcomes[1].0.is_ok());
                    }
                }
            }
        }
    }
}

#[test]
fn prepared_join_payload_mask_releases_state_on_cancel_pressure_and_retry() {
    for mode in ["cancel", "pressure", "consumer", "retry"] {
        let session = ResidentVortexSession::new(8 << 20, 1).unwrap();
        let cancellation = CancellationToken::default();
        let mut binding = spec(Kind::Inner);
        binding.retained = Some(vec![true, false]);
        let mut delivered = 0;
        let result = session.with_native_execution_context(&cancellation, |context| {
            let mut join = Join::new(&binding, context.memory())?;
            join.build(input(&[Some(1); 4], &[20, 21, 22, 23]), context, 2)?;
            let _pressure = if mode == "pressure" {
                let snapshot = context.memory().snapshot();
                Some(
                    context
                        .memory()
                        .reserve(snapshot.limit_bytes - snapshot.reserved_bytes)?,
                )
            } else {
                None
            };
            let mut consume = |_: ArrayRef| {
                delivered += 1;
                if mode == "cancel" {
                    cancellation.cancel();
                }
                if mode == "consumer" {
                    return Err(failed("test consumer failure"));
                }
                Ok(())
            };
            join.consume(input(&[Some(1)], &[10]), context, 1, &mut consume)?;
            join.finish(context, 1, &mut consume)?;
            Ok(())
        });
        if mode == "retry" {
            result.unwrap();
            assert_eq!(delivered, 4);
        } else {
            let error = result.unwrap_err().to_string();
            let expected = match mode {
                "cancel" => "cancelled",
                "pressure" => "memory reservation denied",
                "consumer" => "test consumer failure",
                _ => unreachable!("successful retry is checked above"),
            };
            assert!(error.contains(expected), "{error}");
            assert_eq!(delivered, usize::from(mode != "pressure"));
        }
        assert_eq!(session.memory().snapshot().reserved_bytes, 0);
    }
}
