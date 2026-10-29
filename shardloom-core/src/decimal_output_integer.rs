//! Bounded unsigned arithmetic for explicit Decimal128 result rounding.
//!
//! The largest admitted numerator is a 38-digit coefficient, shifted by a
//! Float64 probability's at-most 1074 denominator bits, then multiplied by
//! 10^38: fewer than 1328 bits. Twenty-four limbs leave checked headroom while
//! keeping every intermediate on the stack. This is not a general `BigInt` API.

use super::{Result, failed};
use std::cmp::Ordering;

const LIMBS: usize = 24;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct Unsigned([u64; LIMBS]);

impl Unsigned {
    pub(super) const ZERO: Self = Self([0; LIMBS]);

    pub(super) fn from_u128(value: u128) -> Self {
        let bytes = value.to_le_bytes();
        let mut limbs = [0; LIMBS];
        limbs[0] = u64::from_le_bytes(bytes[..8].try_into().expect("eight bytes"));
        limbs[1] = u64::from_le_bytes(bytes[8..].try_into().expect("eight bytes"));
        Self(limbs)
    }

    pub(super) fn is_zero(self) -> bool {
        self.0.iter().all(|&limb| limb == 0)
    }

    fn len(self) -> usize {
        self.0
            .iter()
            .rposition(|&limb| limb != 0)
            .map_or(0, |i| i + 1)
    }

    fn bits(self) -> usize {
        let len = self.len();
        if len == 0 {
            0
        } else {
            (len - 1) * 64 + (64 - self.0[len - 1].leading_zeros() as usize)
        }
    }

    pub(super) fn cmp(self, other: Self) -> Ordering {
        self.0.iter().rev().cmp(other.0.iter().rev())
    }

    pub(super) fn add(self, other: Self) -> Result<Self> {
        let mut result = Self::ZERO;
        let mut carry = false;
        for (index, output) in result.0.iter_mut().enumerate() {
            let (sum, first) = self.0[index].overflowing_add(other.0[index]);
            let (sum, second) = sum.overflowing_add(u64::from(carry));
            *output = sum;
            carry = first || second;
        }
        if carry {
            return Err(failed("intermediate exceeds the admitted integer domain"));
        }
        Ok(result)
    }

    pub(super) fn subtract(self, other: Self) -> Result<Self> {
        let mut result = Self::ZERO;
        let mut borrow = false;
        for (index, output) in result.0.iter_mut().enumerate() {
            let (difference, first) = self.0[index].overflowing_sub(other.0[index]);
            let (difference, second) = difference.overflowing_sub(u64::from(borrow));
            *output = difference;
            borrow = first || second;
        }
        if borrow {
            return Err(failed("unsigned intermediate subtraction underflow"));
        }
        Ok(result)
    }

    pub(super) fn multiply(self, other: Self) -> Result<Self> {
        if let (Some(left), Some(right)) = (self.to_u128(), other.to_u128())
            && let Some(value) = left.checked_mul(right)
        {
            return Ok(Self::from_u128(value));
        }
        let mut result = Self::ZERO;
        let (left_len, right_len) = (self.len(), other.len());
        for left in 0..left_len {
            let mut carry = 0_u128;
            for right in 0..right_len {
                let index = left + right;
                if index >= LIMBS {
                    return Err(failed("intermediate exceeds the admitted integer domain"));
                }
                // A full-width product plus one limb and the previous carry
                // fits exactly in u128; no unchecked arithmetic wraps here.
                let value = u128::from(self.0[left]) * u128::from(other.0[right])
                    + u128::from(result.0[index])
                    + carry;
                let bytes = value.to_le_bytes();
                result.0[index] = u64::from_le_bytes(bytes[..8].try_into().expect("eight bytes"));
                carry = value >> 64;
            }
            let index = left + right_len;
            if carry != 0 {
                let Some(output) = result.0.get_mut(index) else {
                    return Err(failed("intermediate exceeds the admitted integer domain"));
                };
                // This slot has not been visited by an earlier outer iteration.
                debug_assert_eq!(*output, 0);
                *output = u64::try_from(carry).expect("one-limb carry");
            }
        }
        Ok(result)
    }

    pub(super) fn power10(exponent: u32) -> Result<Self> {
        if exponent > 76 {
            return Err(failed("decimal scale exceeds the admitted domain"));
        }
        let mut value = Self::from_u128(1);
        for _ in 0..exponent {
            value = value.multiply(Self::from_u128(10))?;
        }
        Ok(value)
    }

    pub(super) fn shift_left(self, shift: usize) -> Result<Self> {
        if self.is_zero() {
            return Ok(self);
        }
        if shift >= LIMBS * 64 || self.bits() + shift > LIMBS * 64 {
            return Err(failed(
                "intermediate shift exceeds the admitted integer domain",
            ));
        }
        let mut result = Self::ZERO;
        let (words, bits) = (shift / 64, shift % 64);
        for index in 0..self.len() {
            result.0[index + words] |= self.0[index] << bits;
            if bits != 0 && index + words + 1 < LIMBS {
                result.0[index + words + 1] |= self.0[index] >> (64 - bits);
            }
        }
        Ok(result)
    }

    pub(super) fn divide(self, denominator: Self) -> Result<(Self, Self)> {
        if denominator.is_zero() {
            return Err(failed("division by zero"));
        }
        if let (Some(numerator), Some(denominator)) = (self.to_u128(), denominator.to_u128()) {
            return Ok((
                Self::from_u128(numerator / denominator),
                Self::from_u128(numerator % denominator),
            ));
        }
        if self.cmp(denominator).is_lt() {
            return Ok((Self::ZERO, self));
        }
        let mut quotient = Self::ZERO;
        let mut remainder = self;
        let difference = self.bits() - denominator.bits();
        for shift in (0..=difference).rev() {
            let shifted = denominator.shift_left(shift)?;
            if !remainder.cmp(shifted).is_lt() {
                remainder = remainder.subtract(shifted)?;
                quotient.0[shift / 64] |= 1_u64 << (shift % 64);
            }
        }
        Ok((quotient, remainder))
    }

    pub(super) fn to_u128(self) -> Option<u128> {
        self.0[2..]
            .iter()
            .all(|&limb| limb == 0)
            .then(|| u128::from(self.0[0]) | (u128::from(self.0[1]) << 64))
    }
}

#[derive(Clone, Copy, Debug)]
pub(super) struct Signed {
    pub(super) negative: bool,
    pub(super) magnitude: Unsigned,
}

impl Signed {
    pub(super) fn from_i128(value: i128) -> Self {
        Self {
            negative: value.is_negative(),
            magnitude: Unsigned::from_u128(value.unsigned_abs()),
        }
    }

    pub(super) fn from_i256_bytes(mut bytes: [u8; 32]) -> Self {
        let negative = bytes[31] & 0x80 != 0;
        if negative {
            let mut carry = true;
            for byte in &mut bytes {
                let (value, overflow) = (!*byte).overflowing_add(u8::from(carry));
                *byte = value;
                carry = overflow;
            }
        }
        let mut magnitude = Unsigned::ZERO;
        for (index, chunk) in bytes.as_chunks::<8>().0.iter().enumerate() {
            magnitude.0[index] = u64::from_le_bytes(*chunk);
        }
        Self {
            negative,
            magnitude,
        }
    }

    pub(super) fn negated(self) -> Self {
        Self {
            negative: !self.negative,
            ..self
        }
    }

    pub(super) fn add(self, other: Self) -> Result<Self> {
        if self.negative == other.negative {
            Ok(Self {
                magnitude: self.magnitude.add(other.magnitude)?,
                ..self
            })
        } else if self.magnitude.cmp(other.magnitude).is_lt() {
            Ok(Self {
                magnitude: other.magnitude.subtract(self.magnitude)?,
                ..other
            })
        } else {
            Ok(Self {
                magnitude: self.magnitude.subtract(other.magnitude)?,
                ..self
            })
        }
    }

    pub(super) fn multiply_unsigned(self, value: Unsigned) -> Result<Self> {
        Ok(Self {
            magnitude: self.magnitude.multiply(value)?,
            ..self
        })
    }

    pub(super) fn multiply(self, other: Self) -> Result<Self> {
        Ok(Self {
            negative: self.negative ^ other.negative,
            magnitude: self.magnitude.multiply(other.magnitude)?,
        })
    }
}
