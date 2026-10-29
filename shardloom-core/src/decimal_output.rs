//! Explicit Decimal128 result domains and exact, single-step rounding.
//!
//! Existing casts and arithmetic keep their defaults. These declarations are
//! opt-in and carry no process-global policy or external execution provider.

use crate::{
    Result, ShardLoomError,
    expression::{BinaryOp, Decimal128Operand},
};
use std::cmp::Ordering;

#[path = "decimal_output_integer.rs"]
mod integer;
use integer::{Signed, Unsigned};

/// How an explicit decimal result treats a nonzero discarded fraction.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
pub enum DecimalRounding {
    /// Refuse any nonzero discarded fraction.
    #[default]
    Exact,
    /// Nearest coefficient; exact ties select an even coefficient.
    HalfEven,
    /// Nearest coefficient; exact ties go away from zero.
    HalfAway,
    /// Truncate toward zero.
    TowardZero,
    /// Round toward negative infinity.
    Floor,
    /// Round toward positive infinity.
    Ceil,
}

impl DecimalRounding {
    /// Parse the stable, case-sensitive public policy name.
    ///
    /// # Errors
    /// Rejects an unknown rounding policy.
    pub fn parse(value: &str) -> Result<Self> {
        match value {
            "exact" => Ok(Self::Exact),
            "half_even" => Ok(Self::HalfEven),
            "half_away" => Ok(Self::HalfAway),
            "toward_zero" => Ok(Self::TowardZero),
            "floor" => Ok(Self::Floor),
            "ceil" => Ok(Self::Ceil),
            _ => Err(failed(
                "rounding must be exact, half_even, half_away, toward_zero, floor or ceil",
            )),
        }
    }

    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Exact => "exact",
            Self::HalfEven => "half_even",
            Self::HalfAway => "half_away",
            Self::TowardZero => "toward_zero",
            Self::Floor => "floor",
            Self::Ceil => "ceil",
        }
    }
}

/// Validated fixed precision, scale and rounding for one decimal operation.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct DecimalOutput {
    precision: u8,
    scale: u8,
    rounding: DecimalRounding,
}

impl DecimalOutput {
    /// Declare a fixed Decimal128 output domain.
    ///
    /// # Errors
    /// Rejects precision outside 1..=38 or scale greater than precision.
    pub fn new(precision: u8, scale: u8, rounding: DecimalRounding) -> Result<Self> {
        if !(1..=38).contains(&precision) || scale > precision {
            return Err(failed("requires precision 1..38 and scale 0..precision"));
        }
        Ok(Self {
            precision,
            scale,
            rounding,
        })
    }

    #[must_use]
    pub const fn precision(self) -> u8 {
        self.precision
    }

    #[must_use]
    pub const fn scale(self) -> u8 {
        self.scale
    }

    #[must_use]
    pub const fn rounding(self) -> DecimalRounding {
        self.rounding
    }

    /// Rescale a validated exact integer or decimal, rounding once.
    ///
    /// # Errors
    /// Rejects inexact results under `exact` and final precision overflow.
    pub fn rescale(self, value: Decimal128Operand) -> Result<Decimal128Operand> {
        self.finish(
            Signed::from_i128(value.value()),
            Unsigned::from_u128(1),
            u32::from(value.precision_scale().1),
        )
    }

    /// Evaluate decimal arithmetic with the caller-selected output domain.
    ///
    /// # Errors
    /// Rejects non-arithmetic operators, division by zero, inexact `exact`
    /// results and final precision overflow. Input scales may differ.
    pub fn binary(
        self,
        left: Decimal128Operand,
        operation: BinaryOp,
        right: Decimal128Operand,
    ) -> Result<Decimal128Operand> {
        let (left_scale, right_scale) = (
            u32::from(left.precision_scale().1),
            u32::from(right.precision_scale().1),
        );
        let (left, right) = (
            Signed::from_i128(left.value()),
            Signed::from_i128(right.value()),
        );
        match operation {
            BinaryOp::Add | BinaryOp::Subtract => {
                let scale = left_scale.max(right_scale);
                let left = left.multiply_unsigned(Unsigned::power10(scale - left_scale)?)?;
                let mut right = right.multiply_unsigned(Unsigned::power10(scale - right_scale)?)?;
                if operation == BinaryOp::Subtract {
                    right = right.negated();
                }
                self.finish(left.add(right)?, Unsigned::from_u128(1), scale)
            }
            BinaryOp::Multiply => self.finish(
                left.multiply(right)?,
                Unsigned::from_u128(1),
                left_scale + right_scale,
            ),
            BinaryOp::Divide => {
                if right.magnitude.is_zero() {
                    return Err(failed("division by zero"));
                }
                let numerator = Signed {
                    negative: left.negative ^ right.negative,
                    ..left
                }
                .multiply_unsigned(Unsigned::power10(right_scale)?)?;
                let denominator = right.magnitude.multiply(Unsigned::power10(left_scale)?)?;
                self.finish(numerator, denominator, 0)
            }
            _ => Err(failed(
                "admits addition, subtraction, multiplication and division",
            )),
        }
    }

    /// Finalize a native exact total, supplied as signed little-endian i256.
    /// The caller handles empty reductions and supplies 1 for SUM or count for AVG.
    ///
    /// # Errors
    /// Rejects an invalid source scale, zero divisor, inexact `exact` result
    /// or final precision overflow. Intermediate totals may exceed Decimal128.
    pub fn total(
        self,
        coefficient: [u8; 32],
        source_scale: u8,
        divisor: u64,
    ) -> Result<Decimal128Operand> {
        if source_scale > 38 {
            return Err(failed("source scale exceeds 38"));
        }
        self.finish(
            Signed::from_i256_bytes(coefficient),
            Unsigned::from_u128(u128::from(divisor)),
            u32::from(source_scale),
        )
    }

    /// Interpolate exact decimal endpoints with weight `numerator / 2^shift`.
    /// The native rank owner derives these integers from the original Float64 bits.
    ///
    /// # Errors
    /// Rejects differing endpoint scales, a weight outside [0, 1), a denominator
    /// exponent greater than 1074, inexact `exact` results or precision overflow.
    pub fn interpolate(
        self,
        left: Decimal128Operand,
        right: Decimal128Operand,
        numerator: u128,
        shift: u32,
    ) -> Result<Decimal128Operand> {
        if left.precision_scale().1 != right.precision_scale().1 {
            return Err(failed("interpolation endpoints require the same scale"));
        }
        if shift > 1074 || (shift < 128 && numerator >= (1_u128 << shift)) {
            return Err(failed(
                "interpolation weight is outside the admitted binary-ratio domain",
            ));
        }
        let denominator = Unsigned::from_u128(1).shift_left(shift as usize)?;
        let base = Signed::from_i128(left.value()).multiply_unsigned(denominator)?;
        let difference =
            Signed::from_i128(right.value()).add(Signed::from_i128(left.value()).negated())?;
        let step = difference.multiply_unsigned(Unsigned::from_u128(numerator))?;
        self.finish(
            base.add(step)?,
            denominator,
            u32::from(left.precision_scale().1),
        )
    }

    fn finish(
        self,
        numerator: Signed,
        denominator: Unsigned,
        source_scale: u32,
    ) -> Result<Decimal128Operand> {
        let numerator = numerator.multiply_unsigned(Unsigned::power10(u32::from(self.scale))?)?;
        let denominator = denominator.multiply(Unsigned::power10(source_scale)?)?;
        let (quotient, remainder) = numerator.magnitude.divide(denominator)?;
        if !remainder.is_zero() && self.rounding == DecimalRounding::Exact {
            return Err(failed(
                "would discard nonzero fractional digits under exact rounding",
            ));
        }
        let mut coefficient = quotient
            .to_u128()
            .ok_or_else(|| failed("final precision overflow"))?;
        if !remainder.is_zero() {
            let half = remainder.cmp(denominator.subtract(remainder)?);
            let increment = match self.rounding {
                DecimalRounding::Exact => {
                    return Err(failed(
                        "would discard nonzero fractional digits under exact rounding",
                    ));
                }
                DecimalRounding::HalfEven => {
                    half.is_gt() || (half == Ordering::Equal && coefficient % 2 == 1)
                }
                DecimalRounding::HalfAway => !half.is_lt(),
                DecimalRounding::TowardZero => false,
                DecimalRounding::Floor => numerator.negative,
                DecimalRounding::Ceil => !numerator.negative,
            };
            coefficient = coefficient
                .checked_add(u128::from(increment))
                .ok_or_else(|| failed("final precision overflow"))?;
        }
        if coefficient >= 10_u128.pow(u32::from(self.precision)) {
            return Err(failed("final precision overflow"));
        }
        let coefficient =
            i128::try_from(coefficient).map_err(|_| failed("final precision overflow"))?;
        Decimal128Operand::decimal(
            if numerator.negative {
                -coefficient
            } else {
                coefficient
            },
            self.precision,
            self.scale,
        )
    }
}

fn failed(reason: &str) -> ShardLoomError {
    ShardLoomError::InvalidOperation(format!(
        "decimal output {reason}; no fallback execution was attempted"
    ))
}

#[cfg(test)]
#[path = "decimal_output_tests.rs"]
mod tests;
