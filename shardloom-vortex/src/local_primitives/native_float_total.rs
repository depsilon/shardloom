//! Reversible exact totals for moving F64 frames. Each finite IEEE value is an
//! integer coefficient in units of 2^-1074; round only the completed result.

use shardloom_core::{Result, ShardLoomError};
use std::cmp::Ordering;

// A finite F64 coefficient uses at most 2098 bits. Multiplication by a u64
// observation count needs at most 2162, covered by 34 limbs (2176 bits).
const LIMBS: usize = 34;
const FRACTION_MASK: u64 = (1 << 52) - 1;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct Total {
    magnitude: [u64; LIMBS],
    negative: bool,
    count: u64,
}

impl Default for Total {
    fn default() -> Self {
        Self {
            magnitude: [0; LIMBS],
            negative: false,
            count: 0,
        }
    }
}

impl Total {
    #[cfg(test)]
    fn count(&self) -> u64 {
        self.count
    }

    pub(super) fn add(&mut self, value: f64) -> Result<()> {
        let count = self
            .count
            .checked_add(1)
            .ok_or_else(|| failed("count overflow"))?;
        let mut next = *self;
        next.adjust(value, false)?;
        next.count = count;
        *self = next;
        Ok(())
    }

    pub(super) fn remove(&mut self, value: f64) -> Result<()> {
        let count = self
            .count
            .checked_sub(1)
            .ok_or_else(|| failed("empty frame removal"))?;
        let mut next = *self;
        next.adjust(value, true)?;
        if count == 0 && next.magnitude.iter().any(|&limb| limb != 0) {
            return Err(failed("empty frame retained a nonzero total"));
        }
        next.count = count;
        *self = next;
        Ok(())
    }

    pub(super) fn merge(&mut self, other: &Self) -> Result<()> {
        let count = self
            .count
            .checked_add(other.count)
            .ok_or_else(|| failed("count overflow"))?;
        let mut next = *self;
        next.signed_add(&other.magnitude, other.negative)?;
        next.count = count;
        *self = next;
        Ok(())
    }

    fn adjust(&mut self, value: f64, subtract: bool) -> Result<()> {
        if !value.is_finite() {
            return Err(failed("requires finite observations"));
        }
        let bits = value.to_bits();
        let exponent = ((bits >> 52) & 0x7ff) as usize;
        let mantissa = (bits & FRACTION_MASK) | if exponent == 0 { 0 } else { 1 << 52 };
        let shift = exponent.saturating_sub(1);
        let mut coefficient = [0; LIMBS];
        coefficient[shift / 64] = mantissa << (shift % 64);
        if !shift.is_multiple_of(64) {
            coefficient[shift / 64 + 1] = mantissa >> (64 - shift % 64);
        }
        self.signed_add(&coefficient, (bits >> 63 != 0) ^ subtract)
    }

    fn signed_add(&mut self, other: &[u64; LIMBS], negative: bool) -> Result<()> {
        if self.negative == negative {
            let mut carry = false;
            for (left, right) in self.magnitude.iter_mut().zip(other) {
                let (sum, first) = left.overflowing_add(*right);
                let (sum, second) = sum.overflowing_add(u64::from(carry));
                *left = sum;
                carry = first || second;
            }
            if carry {
                return Err(failed("wide total overflow"));
            }
        } else {
            match self.magnitude.iter().rev().cmp(other.iter().rev()) {
                Ordering::Greater => subtract(&mut self.magnitude, other),
                Ordering::Less => {
                    let mut magnitude = *other;
                    subtract(&mut magnitude, &self.magnitude);
                    self.magnitude = magnitude;
                    self.negative = negative;
                }
                Ordering::Equal => {
                    self.magnitude = [0; LIMBS];
                    self.negative = false;
                }
            }
        }
        Ok(())
    }

    pub(super) fn finish(&self, average: bool) -> Result<Option<f64>> {
        if self.count == 0 {
            return Ok(None);
        }
        let divisor = if average { self.count } else { 1 };
        let mut quotient = self.magnitude;
        let mut remainder = 0u64;
        for limb in quotient.iter_mut().rev() {
            let numerator = (u128::from(remainder) << 64) | u128::from(*limb);
            // remainder < divisor, so both results fit their destination limb.
            *limb = u64::try_from(numerator / u128::from(divisor))
                .map_err(|_| failed("wide division overflow"))?;
            remainder = u64::try_from(numerator % u128::from(divisor))
                .map_err(|_| failed("wide division remainder overflow"))?;
        }
        let top = quotient
            .iter()
            .rposition(|&limb| limb != 0)
            .map(|index| index * 64 + (63 - quotient[index].leading_zeros() as usize));
        let mut shift = top.unwrap_or(0).saturating_sub(52);
        let mut mantissa = quotient[shift / 64] >> (shift % 64);
        if shift % 64 != 0 && shift / 64 + 1 < LIMBS {
            mantissa |= quotient[shift / 64 + 1] << (64 - shift % 64);
        }
        let round_up = if shift == 0 {
            let half = (u128::from(remainder) * 2).cmp(&u128::from(divisor));
            half == Ordering::Greater || (half == Ordering::Equal && mantissa & 1 != 0)
        } else {
            let guard = bit(&quotient, shift - 1);
            guard && (any_below(&quotient, shift - 1) || remainder != 0 || mantissa & 1 != 0)
        };
        if round_up {
            mantissa += 1;
            if mantissa == 1 << 53 {
                mantissa >>= 1;
                shift += 1;
            }
        }
        let magnitude = if shift == 0 && mantissa < 1 << 52 {
            mantissa
        } else {
            let exponent =
                u64::try_from(shift + 1).map_err(|_| failed("result exponent overflow"))?;
            if exponent >= 0x7ff {
                return Err(failed("final result is not finite"));
            }
            (exponent << 52) | (mantissa & FRACTION_MASK)
        };
        // Exact cancellation is +0. Tiny negative means may round to -0.
        Ok(Some(f64::from_bits(
            magnitude | (u64::from(self.negative) << 63),
        )))
    }
}

fn subtract(left: &mut [u64; LIMBS], right: &[u64; LIMBS]) {
    let mut borrow = false;
    for (left, right) in left.iter_mut().zip(right) {
        let (difference, first) = left.overflowing_sub(*right);
        let (difference, second) = difference.overflowing_sub(u64::from(borrow));
        *left = difference;
        borrow = first || second;
    }
    debug_assert!(!borrow, "magnitude comparison precedes subtraction");
}

fn bit(value: &[u64; LIMBS], position: usize) -> bool {
    value[position / 64] & (1 << (position % 64)) != 0
}

fn any_below(value: &[u64; LIMBS], end: usize) -> bool {
    value[..end / 64].iter().any(|&limb| limb != 0)
        || value[end / 64] & ((1u64 << (end % 64)) - 1) != 0
}

fn failed(message: &str) -> ShardLoomError {
    ShardLoomError::InvalidOperation(format!(
        "native floating frame {message}; no fallback execution was attempted"
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn native_float_total_matches_independent_exact_rational_bits() {
        let cases: serde_json::Value = serde_json::from_str(include_str!(
            "../../tests/fixtures/native_window_float_reference.json"
        ))
        .unwrap();
        let cases = cases["cases"].as_array().unwrap();
        assert_eq!(cases.len(), 120);
        let bits =
            |value: &serde_json::Value| u64::from_str_radix(value.as_str().unwrap(), 16).unwrap();
        for (index, case) in cases.iter().enumerate() {
            let input = case["inputs"]
                .as_array()
                .unwrap()
                .iter()
                .map(|value| f64::from_bits(bits(value)))
                .collect::<Vec<_>>();
            let mut total = Total::default();
            for &value in &input {
                total.add(value).unwrap();
            }
            if case["sum_bits"].is_null() {
                assert!(
                    total
                        .finish(false)
                        .unwrap_err()
                        .to_string()
                        .contains("final result is not finite"),
                    "case {index}"
                );
            } else {
                assert_eq!(
                    total.finish(false).unwrap().unwrap().to_bits(),
                    bits(&case["sum_bits"]),
                    "sum case {index}"
                );
            }
            assert_eq!(
                total.finish(true).unwrap().unwrap().to_bits(),
                bits(&case["avg_bits"]),
                "mean case {index}"
            );
            let mut reverse = Total::default();
            for &value in input.iter().rev() {
                reverse.add(value).unwrap();
            }
            assert_eq!(total, reverse, "order case {index}");
            let mut halves = [Total::default(); 2];
            for (position, &value) in input.iter().enumerate() {
                halves[usize::from(position >= input.len() / 2)]
                    .add(value)
                    .unwrap();
            }
            let second = halves[1];
            halves[0].merge(&second).unwrap();
            assert_eq!(total, halves[0], "merge case {index}");
            for (position, &value) in input.iter().enumerate() {
                total.remove(value).unwrap();
                let mut retained = Total::default();
                for &value in &input[position + 1..] {
                    retained.add(value).unwrap();
                }
                assert_eq!(total, retained, "removal case {index}, position {position}");
            }
            assert_eq!(total, Total::default());
        }
    }

    #[test]
    fn native_float_total_reversible_cancellation_and_mean() {
        let mut total = Total::default();
        for value in [1e300, 1.0, -1e300] {
            total.add(value).unwrap();
        }
        assert_eq!(total.finish(false).unwrap(), Some(1.0));
        total.remove(1e300).unwrap();
        total.remove(-1e300).unwrap();
        assert_eq!(total.finish(false).unwrap(), Some(1.0));
        total.remove(1.0).unwrap();
        assert_eq!(total.finish(false).unwrap(), None);
        for _ in 0..2 {
            total.add(f64::MAX).unwrap();
        }
        assert!(total.finish(false).is_err());
        assert_eq!(total.finish(true).unwrap(), Some(f64::MAX));
        total.remove(f64::MAX).unwrap();
        assert_eq!(total.finish(false).unwrap(), Some(f64::MAX));
    }

    #[test]
    fn native_float_total_rounds_ties_subnormal_and_signed_underflow() {
        for (first, second, expected, average) in [
            (1.0, 2f64.powi(-53), 1.0, false),
            (
                f64::from_bits(1.0f64.to_bits() + 1),
                2f64.powi(-53),
                f64::from_bits(1.0f64.to_bits() + 2),
                false,
            ),
            (f64::from_bits(1), 0.0, 0.0, true),
            (f64::from_bits(3), 0.0, f64::from_bits(2), true),
            (-f64::from_bits(1), 0.0, -0.0, true),
        ] {
            let mut total = Total::default();
            total.add(first).unwrap();
            total.add(second).unwrap();
            assert_eq!(
                total.finish(average).unwrap().unwrap().to_bits(),
                expected.to_bits()
            );
        }
    }

    #[test]
    fn native_float_total_denials_are_atomic_and_merges_are_weighted() {
        let mut total = Total::default();
        assert!(total.remove(1.0).is_err());
        total.add(1.0).unwrap();
        for invalid in [f64::NAN, f64::INFINITY, f64::NEG_INFINITY] {
            let before = total;
            assert!(total.add(invalid).is_err());
            assert!(total.remove(invalid).is_err());
            assert_eq!(before, total);
        }
        let before = total;
        assert!(total.remove(2.0).is_err());
        assert_eq!(before, total);
        let mut other = Total::default();
        for value in [2.0, 3.0, 4.0] {
            other.add(value).unwrap();
        }
        total.merge(&other).unwrap();
        assert_eq!(total.count(), 4);
        assert_eq!(total.finish(true).unwrap(), Some(2.5));
    }
}
