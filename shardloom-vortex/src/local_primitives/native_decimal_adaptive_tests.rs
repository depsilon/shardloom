use super::*;
use vortex::array::dtype::i256;

#[cfg(all(feature = "vortex-write", unix))]
#[test]
fn adaptive_decimal_spill_keeps_the_existing_wide_wire_and_restored_state() {
    let dtype = DecimalDType::new(38, 6);
    for values in [vec![], vec![0], vec![3, -8, 5], vec![100, 200, 300]] {
        let mut total = Total::default();
        for value in values {
            total.add(value, dtype).unwrap();
        }
        assert!(matches!(total.sum, DecimalValue::I128(_)));
        let (bytes, count) = total.spill_parts();
        assert_eq!(bytes, total.sum.as_i256().to_le_bytes());
        let mut restored = Total::from_spill_parts(bytes, count);
        assert_wide(&restored);
        assert_eq!(restored.count(), total.count());
        for average in [false, true] {
            assert_eq!(
                restored.finish(dtype, average).unwrap(),
                total.finish(dtype, average).unwrap()
            );
        }
        restored.add(7, dtype).unwrap();
        restored.remove(7, dtype).unwrap();
        assert_wide(&restored);
        assert_eq!(restored.spill_parts(), (bytes, count));
        let mut narrow = Total::default();
        narrow.add(7, dtype).unwrap();
        narrow.merge(&restored).unwrap();
        assert_wide(&narrow);
        assert_eq!(narrow.count(), count + 1);
    }
}

fn assert_wide(total: &Total) {
    assert!(matches!(total.sum, DecimalValue::I256(_)), "{total:?}");
}

#[test]
fn native_decimal_reduction_promotes_both_signs_without_demoting() {
    let dtype = DecimalDType::new(38, 6);
    let maximum = 10i128.pow(38) - 1;
    for sign in [-1, 1] {
        let value = sign * maximum;
        let mut total = Total::default();
        assert!(matches!(total.sum, DecimalValue::I128(0)));
        total.add(value, dtype).unwrap();
        assert!(matches!(total.sum, DecimalValue::I128(v) if v == value));
        total.add(value, dtype).unwrap();
        assert_wide(&total);
        assert_eq!(total.finish(dtype, true).unwrap(), Some(value));
        total.remove(value, dtype).unwrap();
        assert_wide(&total);
        assert_eq!(total.finish(dtype, false).unwrap(), Some(value));
        total.remove(value, dtype).unwrap();
        assert_wide(&total);
        assert_eq!(total.finish(dtype, true).unwrap(), None);
        total.add(1, dtype).unwrap();
        assert_wide(&total);
        assert_eq!(total.finish(dtype, false).unwrap(), Some(1));
    }
}

#[test]
fn native_decimal_reduction_removal_can_require_promotion() {
    let dtype = DecimalDType::new(38, 6);
    let maximum = 10i128.pow(38) - 1;
    for sign in [-1, 1] {
        let mut total = Total::default();
        for value in [sign * maximum, -sign * maximum, sign * maximum] {
            total.add(value, dtype).unwrap();
        }
        assert!(matches!(total.sum, DecimalValue::I128(_)));
        // The opposite-signed observation really is in the live multiset.
        total.remove(-sign * maximum, dtype).unwrap();
        assert_wide(&total);
        assert_eq!(total.count(), 2);
        assert_eq!(total.finish(dtype, true).unwrap(), Some(sign * maximum));
    }
}

#[test]
fn native_decimal_reduction_merges_every_width_pair_and_weighted_count() {
    let dtype = DecimalDType::new(38, 6);
    let maximum = 10i128.pow(38) - 1;
    for left_wide in [false, true] {
        for right_wide in [false, true] {
            let mut left = Total::default();
            let mut right = Total::default();
            // Cancel through zero while retaining the promoted width.
            for (total, wide) in [(&mut left, left_wide), (&mut right, right_wide)] {
                if wide {
                    for value in [maximum, maximum, -maximum, -maximum] {
                        total.add(value, dtype).unwrap();
                    }
                    for value in [maximum, maximum, -maximum, -maximum] {
                        total.remove(value, dtype).unwrap();
                    }
                }
            }
            left.add(100, dtype).unwrap();
            for value in [200, 300, 400] {
                right.add(value, dtype).unwrap();
            }
            left.merge(&right).unwrap();
            assert_eq!(left.count(), 4);
            assert_eq!(left.finish(dtype, false).unwrap(), Some(1000));
            assert_eq!(left.finish(dtype, true).unwrap(), Some(250));
            assert_eq!(
                matches!(left.sum, DecimalValue::I256(_)),
                left_wide || right_wide
            );
        }
    }
    for sign in [-1, 1] {
        let mut left = Total::default();
        left.add(sign * maximum, dtype).unwrap();
        let right = left;
        left.merge(&right).unwrap();
        assert_wide(&left);
        assert_eq!(left.finish(dtype, true).unwrap(), Some(sign * maximum));
    }
}

#[test]
fn native_decimal_reduction_narrow_average_widens_before_scale_multiply() {
    let dtype = DecimalDType::new(32, 0);
    let value = 10i128.pow(32) - 1;
    for sign in [-1, 1] {
        let mut total = Total::default();
        total.add(sign * value, dtype).unwrap();
        total.add(sign * value, dtype).unwrap();
        assert!(matches!(total.sum, DecimalValue::I128(_)));
        assert_eq!(
            total.finish(dtype, true).unwrap(),
            Some(sign * value * 1_000_000)
        );
        assert_eq!(output_dtype(dtype, true).unwrap(), DecimalDType::new(38, 6));
    }
}

fn reference_result(
    sum: DecimalValue,
    count: usize,
    scale: i8,
    average: bool,
) -> std::result::Result<Option<i128>, &'static str> {
    if count == 0 {
        return Ok(None);
    }
    let value = if average {
        let numerator = sum
            .checked_mul(&DecimalValue::I128(
                10i128.pow(u32::try_from(6 - scale.min(6)).unwrap()),
            ))
            .unwrap();
        let divisor = DecimalValue::I128(i128::try_from(count).unwrap());
        let quotient = numerator.checked_div(&divisor).unwrap();
        if quotient.checked_mul(&divisor) != Some(numerator) {
            return Err("nonzero fractional digits");
        }
        quotient
    } else {
        sum
    };
    let value = value.cast::<i128>().ok_or("final precision overflow")?;
    if value.unsigned_abs() >= 10u128.pow(38) {
        return Err("final precision overflow");
    }
    Ok(Some(value))
}

#[test]
fn native_decimal_reduction_generated_live_multisets_match_wide_reference() {
    let maximum = 10i128.pow(38) - 1;
    let choices = [
        maximum,
        -maximum,
        maximum / 4,
        -maximum / 4,
        0,
        1,
        -1,
        123_456,
    ];
    for scale in [0, 6, 38] {
        let dtype = DecimalDType::new(38, scale);
        for seed in 1..=8u64 {
            let mut random = seed;
            let mut next = || {
                random = random
                    .wrapping_mul(6_364_136_223_846_793_005)
                    .wrapping_add(1);
                random >> 32
            };
            let mut live = Vec::new();
            let mut total = Total::default();
            for step in 0..128 {
                let decision = next();
                if decision % 4 == 0 && !live.is_empty() {
                    let index =
                        usize::try_from(next() % u64::try_from(live.len()).unwrap()).unwrap();
                    total.remove(live.remove(index), dtype).unwrap();
                } else if decision % 8 == 1 {
                    let mut partial = Total::default();
                    for _ in 0..=(next() % 4) {
                        let value = choices[usize::try_from(next() % 8).unwrap()];
                        partial.add(value, dtype).unwrap();
                        live.push(value);
                    }
                    total.merge(&partial).unwrap();
                } else {
                    let value = choices[usize::try_from(next() % 8).unwrap()];
                    total.add(value, dtype).unwrap();
                    live.push(value);
                }
                // Recompute from the actual multiset, independently of the
                // incremental updates, removals and merge implementation.
                let expected = live
                    .iter()
                    .fold(DecimalValue::I256(i256::ZERO), |sum, value| {
                        sum.checked_add(&DecimalValue::I128(*value)).unwrap()
                    });
                assert_eq!(total.sum, expected, "seed={seed}/step={step}/scale={scale}");
                assert_eq!(total.count(), u64::try_from(live.len()).unwrap());
                for average in [false, true] {
                    match reference_result(expected, live.len(), scale, average) {
                        Ok(value) => assert_eq!(total.finish(dtype, average).unwrap(), value),
                        Err(message) => assert!(
                            total
                                .finish(dtype, average)
                                .unwrap_err()
                                .to_string()
                                .contains(message)
                        ),
                    }
                }
            }
            for value in live {
                total.remove(value, dtype).unwrap();
            }
            assert_eq!(total.count(), 0);
            assert_eq!(total.finish(dtype, true).unwrap(), None);
        }
    }
}

#[test]
fn native_decimal_reduction_wide_overflow_and_invalid_metadata_are_atomic() {
    let dtype = DecimalDType::new(38, 6);
    // Vortex rejects this shape before a reduction can receive its dtype.
    assert!(DecimalDType::try_new(2, 3).is_err());
    for (sum, add, remove) in [(i256::MAX, 1, -1), (i256::MIN, -1, 1)] {
        let mut total = Total {
            sum: DecimalValue::I256(sum),
            count: 2,
        };
        let before = total;
        assert!(
            total
                .add(add, dtype)
                .unwrap_err()
                .to_string()
                .contains("wide total overflow")
        );
        assert_eq!(total, before);
        assert!(
            total
                .remove(remove, dtype)
                .unwrap_err()
                .to_string()
                .contains("wide total overflow")
        );
        assert_eq!(total, before);
        let other = Total {
            sum: DecimalValue::I128(add),
            count: 1,
        };
        assert!(
            total
                .merge(&other)
                .unwrap_err()
                .to_string()
                .contains("wide total overflow")
        );
        assert_eq!(total, before);
    }
    for wide in [false, true] {
        let mut total = Total {
            sum: if wide {
                DecimalValue::I256(i256::ZERO)
            } else {
                DecimalValue::I128(0)
            },
            count: 1,
        };
        for invalid in [DecimalDType::new(39, 0), DecimalDType::new(2, -1)] {
            let before = total;
            assert!(total.add(0, invalid).is_err());
            assert!(total.remove(0, invalid).is_err());
            assert!(total.finish(invalid, false).is_err());
            assert_eq!(total, before);
        }
    }
}
