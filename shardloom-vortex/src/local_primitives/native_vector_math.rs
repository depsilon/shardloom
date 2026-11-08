//! Bounded exact finite vector reductions, independent of provider representation.
//!
//! A binary64 product is an integer at quantum 2^-2148. Two 68-word banks
//! cover every product and a 64-bit component count, including doubled cross
//! products in squared distance. The caller admits this fixed scratch before
//! construction. No general big-number runtime or external execution is used.

use super::{Result, failed};
use std::cmp::Ordering;

const WORDS: usize = 68;
const QUANTUM: i32 = 2148;
const FRACTION: u64 = (1_u64 << 52) - 1;
const SIGN: u64 = 1_u64 << 63;
const INFINITY: u64 = 0x7ff0_0000_0000_0000;
const _: () = assert!(usize::BITS <= 64);
type Bank = [u64; WORDS];

pub(super) struct Accumulator {
    positive: Bank,
    negative: Bank,
}

impl Accumulator {
    pub(super) fn new() -> Self {
        Self {
            positive: [0; WORDS],
            negative: [0; WORDS],
        }
    }

    /// Add exactly `left * right`, optionally negated and/or doubled.
    pub(super) fn product(
        &mut self,
        left: f64,
        right: f64,
        negate: bool,
        double: bool,
    ) -> Result<()> {
        if !left.is_finite() || !right.is_finite() {
            return Err(failed(
                "exact vector accumulation requires finite components",
            ));
        }
        let (ls, lm, le) = decompose(left);
        let (rs, rm, re) = decompose(right);
        let product = u128::from(lm) * u128::from(rm);
        if product == 0 {
            return Ok(());
        }
        let shift = usize::try_from(le + re + QUANTUM + i32::from(double))
            .map_err(|_| failed("exact vector product exponent is outside its finite domain"))?;
        let bank = if ls ^ rs ^ negate {
            &mut self.negative
        } else {
            &mut self.positive
        };
        add_shifted(bank, product, shift)
    }

    pub(super) fn finish(mut self) -> Magnitude {
        match self.positive.iter().rev().cmp(self.negative.iter().rev()) {
            Ordering::Less => {
                subtract(&mut self.negative, &self.positive);
                Magnitude {
                    words: self.negative,
                    negative: true,
                }
            }
            Ordering::Equal => Magnitude {
                words: [0; WORDS],
                negative: false,
            },
            Ordering::Greater => {
                subtract(&mut self.positive, &self.negative);
                Magnitude {
                    words: self.positive,
                    negative: false,
                }
            }
        }
    }
}

pub(super) struct Magnitude {
    words: Bank,
    negative: bool,
}

impl Magnitude {
    fn highest(&self) -> Option<usize> {
        self.words
            .iter()
            .rposition(|word| *word != 0)
            .map(|index| index * 64 + 63 - self.words[index].leading_zeros() as usize)
    }

    #[cfg(test)]
    pub(super) fn is_zero(&self) -> bool {
        self.highest().is_none()
    }

    pub(super) fn rounded(&self) -> f64 {
        let Some(high) = self.highest() else {
            return 0.0;
        };
        let shift = high.saturating_sub(52).max(1074);
        signed(
            encode_rounded(
                self.rounded_integer(shift),
                i32::try_from(shift).expect("fixed accumulator width") - QUANTUM,
            ),
            self.negative,
        )
    }

    /// Round sqrt(M * 2^-2148) directly, including exact midpoint ties.
    pub(super) fn sqrt(&self) -> Result<f64> {
        if self.negative {
            return Err(failed("exact vector squared magnitude became negative"));
        }
        let Some(high) = self.highest() else {
            return Ok(0.0);
        };
        let exponent =
            (i32::try_from(high).expect("fixed accumulator width") - QUANTUM).div_euclid(2);
        let quantum = (exponent - 52).max(-1074);
        let shift =
            usize::try_from(QUANTUM + 2 * quantum).expect("square-root quantum is at least -1074");
        let mut integer = self.shifted_u128(shift).isqrt();
        let boundary = (2 * integer + 1).pow(2);
        let comparison = if shift == 0 {
            (self.shifted_u128(0) << 2).cmp(&boundary)
        } else {
            // shift is even. The retained value and boundary fit in 108 bits;
            // low discarded bits distinguish an exact tie from a larger value.
            let comparison = self.shifted_u128(shift - 2).cmp(&boundary);
            if comparison == Ordering::Equal && self.any_below(shift - 2) {
                Ordering::Greater
            } else {
                comparison
            }
        };
        if comparison == Ordering::Greater || (comparison == Ordering::Equal && integer & 1 != 0) {
            integer += 1;
        }
        Ok(encode_rounded(
            u64::try_from(integer).expect("rounded square root has at most 54 bits"),
            quantum,
        ))
    }

    /// Rounded normal mantissa plus an unbounded-by-F64 exponent. This does not
    /// round the full value to subnormal or infinity before a later ratio.
    fn normal(&self) -> Option<(f64, i32)> {
        let high = self.highest()?;
        let shift = high.saturating_sub(52);
        Some(normal_integer(
            self.rounded_integer(shift),
            i32::try_from(shift).expect("fixed accumulator width") - QUANTUM,
        ))
    }

    fn rounded_integer(&self, shift: usize) -> u64 {
        let mut integer =
            u64::try_from(self.shifted_u128(shift)).expect("rounding retains at most 53 bits");
        if shift != 0 && self.bit(shift - 1) && (self.any_below(shift - 1) || integer & 1 != 0) {
            integer += 1;
        }
        integer
    }

    fn bit(&self, index: usize) -> bool {
        self.words
            .get(index / 64)
            .is_some_and(|word| word & (1_u64 << (index % 64)) != 0)
    }

    fn any_below(&self, bits: usize) -> bool {
        let full = bits / 64;
        self.words.iter().take(full).any(|word| *word != 0)
            || (!bits.is_multiple_of(64)
                && self
                    .words
                    .get(full)
                    .is_some_and(|word| word & ((1_u64 << (bits % 64)) - 1) != 0))
    }

    /// Callers select a shift leaving at most 108 significant bits.
    fn shifted_u128(&self, shift: usize) -> u128 {
        let index = shift / 64;
        let bits = shift % 64;
        let word = |index| self.words.get(index).copied().unwrap_or(0);
        let shifted = |index| {
            if bits == 0 {
                word(index)
            } else {
                (word(index) >> bits) | (word(index + 1) << (64 - bits))
            }
        };
        u128::from(shifted(index)) | (u128::from(shifted(index + 1)) << 64)
    }
}

pub(super) struct Normalizer {
    root: f64,
    exponent: i32,
}

impl Normalizer {
    pub(super) fn new(square: &Magnitude) -> Result<Option<Self>> {
        if square.negative {
            return Err(failed("exact vector norm square became negative"));
        }
        Ok(square.normal().map(|(mantissa, exponent)| Self {
            root: (mantissa * parity(exponent)).sqrt(),
            exponent: exponent.div_euclid(2),
        }))
    }

    pub(super) fn component(&self, value: f64) -> f64 {
        if value == 0.0 {
            return value;
        }
        let (negative, integer, exponent) = decompose(value);
        let (mantissa, exponent) = normal_integer(integer, exponent);
        signed(
            scale_normal(mantissa / self.root, exponent - self.exponent),
            negative,
        )
    }
}

pub(super) fn cosine(
    dot: &Magnitude,
    left_square: &Magnitude,
    right_square: &Magnitude,
) -> Result<Option<f64>> {
    if left_square.negative || right_square.negative {
        return Err(failed("exact vector norm square became negative"));
    }
    let Some((left, le)) = left_square.normal() else {
        return Ok(None);
    };
    let Some((right, re)) = right_square.normal() else {
        return Ok(None);
    };
    let Some((numerator, ne)) = dot.normal() else {
        return Ok(Some(0.0));
    };
    let exponent = le + re;
    let denominator = ((left * right) * parity(exponent)).sqrt();
    Ok(Some(
        signed(
            scale_normal(numerator / denominator, ne - exponent.div_euclid(2)),
            dot.negative,
        )
        .clamp(-1.0, 1.0),
    ))
}

fn parity(exponent: i32) -> f64 {
    if exponent.rem_euclid(2) == 0 {
        1.0
    } else {
        2.0
    }
}

fn signed(value: f64, negative: bool) -> f64 {
    f64::from_bits(value.to_bits() | if negative { SIGN } else { 0 })
}

fn decompose(value: f64) -> (bool, u64, i32) {
    let bits = value.to_bits();
    let exponent = ((bits >> 52) & 0x7ff) as i32;
    (
        bits & SIGN != 0,
        (bits & FRACTION) | if exponent == 0 { 0 } else { 1_u64 << 52 },
        if exponent == 0 {
            -1074
        } else {
            exponent - 1075
        },
    )
}

fn add_shifted(bank: &mut Bank, value: u128, shift: usize) -> Result<()> {
    let index = shift / 64;
    let bits = shift % 64;
    let low = u64::try_from(value & u128::from(u64::MAX)).expect("masked low accumulator word");
    let high = (value >> 64) as u64;
    if bits == 0 {
        add_word(bank, index, low)?;
        add_word(bank, index + 1, high)
    } else {
        add_word(bank, index, low << bits)?;
        add_word(bank, index + 1, (high << bits) | (low >> (64 - bits)))?;
        add_word(bank, index + 2, high >> (64 - bits))
    }
}

fn add_word(bank: &mut Bank, mut index: usize, mut value: u64) -> Result<()> {
    while value != 0 {
        let word = bank
            .get_mut(index)
            .ok_or_else(|| failed("exact vector accumulator exceeded its proven capacity"))?;
        let (sum, carry) = word.overflowing_add(value);
        *word = sum;
        value = u64::from(carry);
        index += 1;
    }
    Ok(())
}

fn subtract(larger: &mut Bank, smaller: &Bank) {
    let mut borrow = false;
    for (left, right) in larger.iter_mut().zip(smaller) {
        let (value, first) = left.overflowing_sub(*right);
        let (value, second) = value.overflowing_sub(u64::from(borrow));
        *left = value;
        borrow = first || second;
    }
    debug_assert!(!borrow, "magnitude comparison precedes subtraction");
}

fn normal_integer(integer: u64, power: i32) -> (f64, i32) {
    debug_assert!(integer != 0 && integer <= 1_u64 << 53);
    let high = integer.ilog2();
    let significand = if high > 52 {
        debug_assert_eq!(integer & 1, 0);
        integer >> (high - 52)
    } else {
        integer << (52 - high)
    };
    (
        f64::from_bits((1023_u64 << 52) | (significand & FRACTION)),
        power + i32::try_from(high).expect("highest u64 bit index"),
    )
}

/// Encode a pre-rounded integer * 2^power without a second rounding.
fn encode_rounded(integer: u64, power: i32) -> f64 {
    if integer == 0 {
        return 0.0;
    }
    let (mantissa, exponent) = normal_integer(integer, power);
    if exponent > 1023 {
        return f64::INFINITY;
    }
    if exponent < -1022 {
        debug_assert!(power >= -1074);
        return f64::from_bits(integer << (power + 1074));
    }
    f64::from_bits(
        (u64::try_from(exponent + 1023).expect("normal exponent is nonnegative") << 52)
            | (mantissa.to_bits() & FRACTION),
    )
}

/// Scale a normal finite mantissa. Only the final subnormal conversion rounds.
fn scale_normal(value: f64, power: i32) -> f64 {
    debug_assert!(value.is_normal());
    let bits = value.to_bits();
    let exponent = ((bits >> 52) & 0x7ff) as i32 - 1023 + power;
    if exponent > 1023 {
        return f64::from_bits((bits & SIGN) | INFINITY);
    }
    if exponent >= -1022 {
        return f64::from_bits(
            (bits & (SIGN | FRACTION))
                | (u64::try_from(exponent + 1023).expect("normal exponent is nonnegative") << 52),
        );
    }
    let shift = u32::try_from(-1022 - exponent).expect("subnormal shift is positive");
    if shift >= 54 {
        return f64::from_bits(bits & SIGN);
    }
    let integer = (1_u64 << 52) | (bits & FRACTION);
    let mut rounded = integer >> shift;
    let remainder = integer & ((1_u64 << shift) - 1);
    let midpoint = 1_u64 << (shift - 1);
    if remainder > midpoint || (remainder == midpoint && rounded & 1 != 0) {
        rounded += 1;
    }
    f64::from_bits((bits & SIGN) | rounded)
}

#[cfg(test)]
#[path = "native_vector_math_tests.rs"]
mod tests;
