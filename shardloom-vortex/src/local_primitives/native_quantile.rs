//! Exact quantile ranks and explicit numeric interpolation. Value ordering is
//! owned by the native resident index or existing native ordered spill path.

use super::{
    native_relational_aggregate::number,
    native_relational_batch::failed,
    native_relational_keys::{Cell, validate_finite_scalar},
    result_batch::Value,
    vortex_error,
};
use crate::query_primitive::VortexQuantileLevel;
use shardloom_core::{Result, expression::Decimal128Operand};
use vortex::array::{
    dtype::{DType, DecimalDType},
    scalar::DecimalValue,
};

#[derive(Debug, Clone, Copy)]
pub(super) struct Position {
    pub(super) lower: u64,
    pub(super) upper: u64,
    numerator: u128,
    shift: u32,
}

impl Position {
    pub(super) fn new(level: VortexQuantileLevel, count: u64, discrete: bool) -> Result<Self> {
        if count == 0 {
            return Err(failed("quantile rank requires a nonempty observed domain"));
        }
        let bits = level.value().to_bits();
        let exponent = u32::try_from(bits >> 52).map_err(vortex_error)?;
        let fraction = bits & ((1_u64 << 52) - 1);
        let (mantissa, shift) = if exponent == 0 {
            (fraction, 1074)
        } else {
            (fraction | (1_u64 << 52), 1075 - exponent)
        };
        let product = u128::from(mantissa) * u128::from(if discrete { count } else { count - 1 });
        let (whole, remainder) = if shift >= 128 {
            (0, product)
        } else {
            (product >> shift, product & ((1_u128 << shift) - 1))
        };
        let whole = u64::try_from(whole).map_err(vortex_error)?;
        let lower = if discrete {
            // ceil(q*N)-1, including both endpoints without underflow.
            if remainder == 0 {
                whole.saturating_sub(1)
            } else {
                whole
            }
        } else {
            whole
        };
        let upper = if !discrete && remainder != 0 {
            lower
                .checked_add(1)
                .ok_or_else(|| failed("quantile rank overflow"))?
        } else {
            lower
        };
        if upper >= count {
            return Err(failed("quantile rank exceeds its observed domain"));
        }
        Ok(Self {
            lower,
            upper,
            numerator: if discrete { 0 } else { remainder },
            shift,
        })
    }

    #[allow(clippy::cast_precision_loss)] // Only the interpolation weight becomes approximate, never the selected ranks.
    fn weight(self) -> f64 {
        if self.numerator == 0 {
            return 0.0;
        }
        let numerator = self.numerator as f64;
        if self.shift <= 1022 {
            numerator * f64::from_bits(u64::from(1023 - self.shift) << 52)
        } else {
            (numerator * f64::from_bits(u64::from(2045 - self.shift) << 52)) * f64::MIN_POSITIVE
        }
    }

    pub(super) fn interpolate(
        self,
        lower: &Cell,
        upper: &Cell,
        output: &DType,
    ) -> Result<Value<'static>> {
        if let DType::Decimal(dtype, _) = output {
            let (Cell::Decimal(left, left_type), Cell::Decimal(right, right_type)) = (lower, upper)
            else {
                return Err(failed("decimal quantile endpoints changed type"));
            };
            if left_type != right_type {
                return Err(failed("decimal quantile endpoints changed metadata"));
            }
            return decimal(self, *left, *right, *left_type, *dtype)
                .map(|value| Value::Decimal(value, *dtype));
        }
        let left = number(lower)?;
        let right = number(upper)?;
        validate_finite_scalar(left)?;
        validate_finite_scalar(right)?;
        let value = if self.numerator == 0 || left.to_bits() == right.to_bits() {
            left
        } else {
            let weight = self.weight();
            if left.is_sign_negative() == right.is_sign_negative() {
                left + (right - left) * weight
            } else {
                left * (1.0 - weight) + right * weight
            }
        };
        validate_finite_scalar(value)?;
        Ok(Value::Float(value))
    }
}

pub(super) fn numeric_value(value: &Cell, dtype: &DType) -> Result<Value<'static>> {
    Ok(match value {
        Cell::NegativeInteger(value) => Value::Int(*value),
        Cell::NonnegativeInteger(value) if matches!(dtype, DType::Primitive(ptype, _) if ptype.is_signed_int()) => {
            Value::Int(i64::try_from(*value).map_err(vortex_error)?)
        }
        Cell::NonnegativeInteger(value) => Value::UInt(*value),
        Cell::Float(bits) => {
            let value = f64::from_bits(*bits);
            validate_finite_scalar(value)?;
            Value::Float(value)
        }
        Cell::Decimal(value, actual) if matches!(dtype, DType::Decimal(expected, _) if expected == actual) => {
            Value::Decimal(*value, *actual)
        }
        _ => return Err(failed("quantile result changed its numeric input domain")),
    })
}

pub(super) fn validate(value: &Cell) -> Result<()> {
    match value {
        Cell::NegativeInteger(_) | Cell::NonnegativeInteger(_) => Ok(()),
        Cell::Float(bits) => validate_finite_scalar(f64::from_bits(*bits)),
        Cell::Decimal(value, dtype) => Decimal128Operand::decimal(
            *value,
            dtype.precision(),
            u8::try_from(dtype.scale()).map_err(vortex_error)?,
        )
        .map(|_| ()),
        _ => Err(failed(
            "quantile requires a nonnull primitive numeric or Decimal128 observation",
        )),
    }
}

fn decimal(
    position: Position,
    left: i128,
    right: i128,
    source: DecimalDType,
    output: DecimalDType,
) -> Result<i128> {
    let scale = u32::try_from(output.scale() - source.scale()).map_err(vortex_error)?;
    let factor = 10_i128
        .checked_pow(scale)
        .ok_or_else(|| failed("decimal quantile scale overflow"))?;
    let left = DecimalValue::I256(DecimalValue::I128(left).as_i256());
    let right = DecimalValue::I256(DecimalValue::I128(right).as_i256());
    let factor = DecimalValue::I128(factor);
    let base = left
        .checked_mul(&factor)
        .ok_or_else(|| failed("decimal quantile numerator overflow"))?;
    let mut step = right
        .checked_sub(&left)
        .and_then(|step| step.checked_mul(&factor))
        .ok_or_else(|| failed("decimal quantile difference overflow"))?;
    let value = if position.numerator == 0 || step.is_zero() {
        base
    } else {
        let common = position.numerator.trailing_zeros().min(position.shift);
        let numerator = position.numerator >> common;
        let divisor = DecimalValue::I128(2);
        // Cancel powers of two before multiplying the wide decimal. An exact
        // decimal result requires every denominator factor to divide this step.
        for _ in 0..position.shift - common {
            let divided = step
                .checked_div(&divisor)
                .ok_or_else(|| failed("decimal quantile division failed"))?;
            if divided.checked_mul(&divisor) != Some(step) {
                return Err(failed(
                    "decimal quantile would discard nonzero fractional digits; select an explicit decimal rounding policy",
                ));
            }
            step = divided;
        }
        let numerator = DecimalValue::I128(i128::try_from(numerator).map_err(vortex_error)?);
        step.checked_mul(&numerator)
            .and_then(|step| base.checked_add(&step))
            .ok_or_else(|| failed("decimal quantile interpolation overflow"))?
    };
    let value = value
        .cast::<i128>()
        .ok_or_else(|| failed("decimal quantile final precision overflow"))?;
    Decimal128Operand::decimal(
        value,
        output.precision(),
        u8::try_from(output.scale()).map_err(vortex_error)?,
    )?;
    Ok(value)
}

#[cfg(test)]
mod tests {
    use super::*;
    use vortex::array::dtype::{Nullability, PType};

    #[test]
    fn exact_quantile_ranks_keep_large_counts_endpoints_and_subnormal_levels() {
        for count in [1, 2, 3, (1_u64 << 53) + 3, u64::MAX] {
            for discrete in [false, true] {
                let start =
                    Position::new(VortexQuantileLevel::new(0.0).unwrap(), count, discrete).unwrap();
                let end =
                    Position::new(VortexQuantileLevel::new(1.0).unwrap(), count, discrete).unwrap();
                assert_eq!((start.lower, start.upper), (0, 0));
                assert_eq!((end.lower, end.upper), (count - 1, count - 1));
                let middle =
                    Position::new(VortexQuantileLevel::new(0.5).unwrap(), count, discrete).unwrap();
                assert_eq!(middle.lower, (count - 1) / 2);
                assert_eq!(
                    middle.upper,
                    if discrete { (count - 1) / 2 } else { count / 2 }
                );
            }
            let tiny = Position::new(
                VortexQuantileLevel::new(f64::from_bits(1)).unwrap(),
                count,
                false,
            )
            .unwrap();
            assert_eq!((tiny.lower, tiny.upper), (0, u64::from(count > 1)));
        }
        for invalid in [-0.1, 1.1, f64::NAN, f64::INFINITY] {
            assert!(VortexQuantileLevel::new(invalid).is_err());
        }
        assert_eq!(
            VortexQuantileLevel::new(-0.0).unwrap(),
            VortexQuantileLevel::new(0.0).unwrap()
        );
    }

    #[test]
    fn exact_quantile_interpolation_preserves_finite_extremes_and_decimal_digits() {
        let midpoint = Position::new(VortexQuantileLevel::new(0.5).unwrap(), 2, false).unwrap();
        let dtype = DType::Primitive(PType::F64, Nullability::Nullable);
        for (left, right, expected) in [
            (-f64::MAX, f64::MAX, 0.0),
            (f64::MAX, f64::MAX, f64::MAX),
            (2.0, 8.0, 5.0),
        ] {
            let Value::Float(actual) = midpoint
                .interpolate(
                    &Cell::Float(left.to_bits()),
                    &Cell::Float(right.to_bits()),
                    &dtype,
                )
                .unwrap()
            else {
                panic!("floating quantile type changed")
            };
            assert_eq!(actual, expected);
        }
        let source = DecimalDType::new(10, 2);
        let dtype = DType::Decimal(DecimalDType::new(38, 6), Nullability::Nullable);
        let Value::Decimal(actual, _) = midpoint
            .interpolate(
                &Cell::Decimal(-301, source),
                &Cell::Decimal(100, source),
                &dtype,
            )
            .unwrap()
        else {
            panic!("decimal quantile type changed")
        };
        assert_eq!(actual, -1_005_000);
        let level = Position::new(VortexQuantileLevel::new(0.1).unwrap(), 2, false).unwrap();
        assert!(
            level
                .interpolate(
                    &Cell::Decimal(0, source),
                    &Cell::Decimal(100, source),
                    &dtype
                )
                .err()
                .unwrap()
                .to_string()
                .contains("fractional digits")
        );
    }
}
