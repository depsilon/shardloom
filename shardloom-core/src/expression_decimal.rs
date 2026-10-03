//! Checked decimal value operations shared by reference and native kernels.

use super::{
    BinaryOp, Decimal128Operand, Decimal128OperandSource, EvalFailure, EvalResult, Result,
    ShardLoomError, decimal128_binary, decimal128_checked_operand, decimal128_power10,
    decimal128_precision_scale_is_valid, validate_decimal128_arithmetic_scale_boundary,
};

impl Decimal128Operand {
    /// Validates a decimal value and its precision/scale without allocation.
    ///
    /// # Errors
    /// Rejects invalid metadata and values outside the declared precision.
    pub fn decimal(value: i128, precision: u8, scale: u8) -> Result<Self> {
        decimal128_checked_operand(value, precision, scale).map_err(|error| value_error(*error))
    }

    /// Constructs an integer operand using precision derived from its schema.
    ///
    /// # Errors
    /// Rejects invalid precision and values outside that precision.
    pub fn integer(value: i128, precision: u8) -> Result<Self> {
        let mut operand = Self::decimal(value, precision, 0)?;
        operand.source = Decimal128OperandSource::Integer;
        Ok(operand)
    }

    #[must_use]
    pub const fn value(self) -> i128 {
        self.value
    }

    #[must_use]
    pub const fn precision_scale(self) -> (u8, u8) {
        (self.precision, self.scale)
    }

    /// Derives output metadata solely from operand metadata, including at zero rows.
    ///
    /// # Errors
    /// Rejects incompatible scales, non-arithmetic operators and precision overflow.
    pub fn arithmetic_type(self, op: BinaryOp, right: Self) -> Result<(u8, u8)> {
        decimal128_binary_metadata(self, op, right).map_err(|error| value_error(*error))
    }

    /// Evaluates checked exact arithmetic without a reference evaluator or row map.
    ///
    /// # Errors
    /// Rejects invalid metadata, overflow, division by zero and inexact division.
    pub fn checked_binary(self, op: BinaryOp, right: Self) -> Result<Self> {
        decimal128_binary(self, op, right).map_err(|error| value_error(*error))
    }

    /// Converts to another decimal type, requiring discarded digits to be zero.
    ///
    /// # Errors
    /// Rejects invalid metadata, precision overflow and inexact downscaling.
    pub fn rescale(self, precision: u8, scale: u8) -> Result<Self> {
        Self::decimal(0, precision, scale)?;
        let factor = decimal128_power10(scale.abs_diff(self.scale)).ok_or_else(|| {
            ShardLoomError::InvalidOperation(
                "decimal128 scale difference exceeds precision 38".to_owned(),
            )
        })?;
        let value = if scale >= self.scale {
            self.value.checked_mul(factor)
        } else {
            if self.value % factor != 0 {
                return Err(ShardLoomError::InvalidOperation(
                    "decimal128 conversion would discard nonzero fractional digits".to_owned(),
                ));
            }
            Some(self.value / factor)
        }
        .ok_or_else(|| {
            ShardLoomError::InvalidOperation("decimal128 rescale overflow".to_owned())
        })?;
        Self::decimal(value, precision, scale)
    }

    #[must_use]
    pub fn negate(self) -> Self {
        // A valid decimal has at most 38 digits; i128::MIN has 39.
        Self {
            value: -self.value,
            ..self
        }
    }

    #[must_use]
    pub fn abs(self) -> Self {
        Self {
            value: self.value.abs(),
            ..self
        }
    }

    #[must_use]
    pub fn floor(self) -> Self {
        self.integral(|value, factor| value.div_euclid(factor))
    }

    #[must_use]
    pub fn ceil(self) -> Self {
        self.integral(|value, factor| -(-value).div_euclid(factor))
    }

    #[must_use]
    pub fn round(self) -> Self {
        self.integral(|value, factor| {
            let integer = value / factor;
            let remainder = value % factor;
            // factor <= 10^38, so comparing against half never doubles a value.
            integer
                + if remainder.abs() >= factor / 2 {
                    value.signum()
                } else {
                    0
                }
        })
    }

    fn integral(self, round: impl FnOnce(i128, i128) -> i128) -> Self {
        if self.scale == 0 {
            return self;
        }
        Self {
            value: round(
                self.value,
                decimal128_power10(self.scale).expect("valid scale"),
            ),
            precision: self.precision - self.scale + 1,
            scale: 0,
            source: Decimal128OperandSource::Decimal,
        }
    }
}

fn value_error(failure: EvalFailure) -> ShardLoomError {
    ShardLoomError::InvalidOperation(
        failure
            .diagnostic
            .reason
            .unwrap_or_else(|| "invalid decimal128 operation".to_owned()),
    )
}

pub(super) fn decimal128_binary_metadata(
    left: Decimal128Operand,
    op: BinaryOp,
    right: Decimal128Operand,
) -> EvalResult<(u8, u8)> {
    validate_decimal128_arithmetic_scale_boundary(left, right)?;
    let (precision, scale) = match op {
        BinaryOp::Add | BinaryOp::Subtract => {
            let scale = left.scale.max(right.scale);
            (
                left.precision
                    .saturating_add(scale - left.scale)
                    .max(right.precision.saturating_add(scale - right.scale))
                    .saturating_add(1),
                scale,
            )
        }
        BinaryOp::Multiply => (
            left.precision.saturating_add(right.precision),
            left.scale.saturating_add(right.scale),
        ),
        BinaryOp::Divide => (38, left.scale.max(right.scale).max(6)),
        BinaryOp::And | BinaryOp::Or => {
            return Err(EvalFailure::unsupported(
                "decimal128_arithmetic",
                "decimal128 requires an arithmetic operator",
            ));
        }
    };
    if !decimal128_precision_scale_is_valid(precision, scale) {
        return Err(EvalFailure::unsupported(
            "decimal128_arithmetic",
            "decimal128 arithmetic output precision/scale exceeds decimal128(38,s)",
        ));
    }
    Ok((precision, scale))
}
