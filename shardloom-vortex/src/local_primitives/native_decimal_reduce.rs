//! Exact decimal totals shared by the existing native reduction state owners.
//! Widen before accumulation; only final results must fit Decimal128 precision.

use shardloom_core::{Result, ShardLoomError, expression::Decimal128Operand};
use vortex::array::{dtype::DecimalDType, scalar::DecimalValue};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct Total {
    sum: DecimalValue,
    count: u64,
}

impl Default for Total {
    fn default() -> Self {
        Self {
            sum: DecimalValue::I256(DecimalValue::I128(0).as_i256()),
            count: 0,
        }
    }
}

pub(super) fn output_dtype(source: DecimalDType, average: bool) -> Result<DecimalDType> {
    validate(0, source)?;
    Ok(DecimalDType::new(
        38,
        if average {
            source.scale().max(6)
        } else {
            source.scale()
        },
    ))
}

impl Total {
    pub(super) fn count(&self) -> u64 {
        self.count
    }

    pub(super) fn add(&mut self, value: i128, source: DecimalDType) -> Result<()> {
        validate(value, source)?;
        let count = self
            .count
            .checked_add(1)
            .ok_or_else(|| failed("count overflow"))?;
        let sum = self
            .sum
            .checked_add(&DecimalValue::I128(value))
            .ok_or_else(|| failed("wide total overflow"))?;
        *self = Self { sum, count };
        Ok(())
    }

    pub(super) fn remove(&mut self, value: i128, source: DecimalDType) -> Result<()> {
        validate(value, source)?;
        let count = self
            .count
            .checked_sub(1)
            .ok_or_else(|| failed("empty window removal"))?;
        let sum = self
            .sum
            .checked_sub(&DecimalValue::I128(value))
            .ok_or_else(|| failed("wide total overflow"))?;
        if count == 0 && !sum.is_zero() {
            return Err(failed("empty window retained a nonzero total"));
        }
        *self = Self { sum, count };
        Ok(())
    }

    /// Callers merge cells from the same bound decimal input domain.
    pub(super) fn merge(&mut self, other: &Self) -> Result<()> {
        let count = self
            .count
            .checked_add(other.count)
            .ok_or_else(|| failed("count overflow"))?;
        let sum = self
            .sum
            .checked_add(&other.sum)
            .ok_or_else(|| failed("wide total overflow"))?;
        *self = Self { sum, count };
        Ok(())
    }

    pub(super) fn finish(&self, source: DecimalDType, average: bool) -> Result<Option<i128>> {
        let output = output_dtype(source, average)?;
        if self.count == 0 {
            return Ok(None);
        }
        let value = if average {
            let scale = u32::try_from(output.scale() - source.scale())
                .map_err(|_| failed("invalid average scale"))?;
            let numerator = self
                .sum
                .checked_mul(&DecimalValue::I128(10i128.pow(scale)))
                .ok_or_else(|| failed("wide average numerator overflow"))?;
            let divisor = DecimalValue::I128(i128::from(self.count));
            let quotient = numerator
                .checked_div(&divisor)
                .ok_or_else(|| failed("average division failed"))?;
            if quotient.checked_mul(&divisor) != Some(numerator) {
                return Err(failed("average would discard nonzero fractional digits"));
            }
            quotient
        } else {
            self.sum
        };
        let value = value
            .cast::<i128>()
            .ok_or_else(|| failed("final precision overflow"))?;
        validate(value, output).map_err(|_| failed("final precision overflow"))?;
        Ok(Some(value))
    }
}

fn validate(value: i128, dtype: DecimalDType) -> Result<()> {
    let scale = u8::try_from(dtype.scale()).map_err(|_| failed("negative decimal scale"))?;
    Decimal128Operand::decimal(value, dtype.precision(), scale).map(|_| ())
}

fn failed(message: &str) -> ShardLoomError {
    ShardLoomError::InvalidOperation(format!(
        "native decimal reduction {message}; no fallback execution was attempted"
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn native_decimal_reduction_preserves_cancellation_and_oversized_average() {
        let dtype = DecimalDType::new(38, 6);
        let maximum = 10i128.pow(38) - 1;
        let mut total = Total::default();
        total.add(maximum, dtype).unwrap();
        total.add(maximum, dtype).unwrap();
        assert!(
            total
                .finish(dtype, false)
                .unwrap_err()
                .to_string()
                .contains("precision overflow")
        );
        assert_eq!(total.finish(dtype, true).unwrap(), Some(maximum));
        total.add(-maximum, dtype).unwrap();
        assert_eq!(total.finish(dtype, false).unwrap(), Some(maximum));
        total.remove(-maximum, dtype).unwrap();
        total.remove(maximum, dtype).unwrap();
        total.remove(maximum, dtype).unwrap();
        assert_eq!(total.finish(dtype, false).unwrap(), None);
        assert_eq!(total.count(), 0);
    }

    #[test]
    fn native_decimal_reduction_exact_average_scale_and_weighted_merge() {
        let dtype = DecimalDType::new(10, 2);
        let mut left = Total::default();
        left.add(100, dtype).unwrap();
        let mut right = Total::default();
        for value in [200, 300, 400] {
            right.add(value, dtype).unwrap();
        }
        left.merge(&right).unwrap();
        assert_eq!(left.finish(dtype, true).unwrap(), Some(2_500_000));
        assert_eq!(output_dtype(dtype, true).unwrap(), DecimalDType::new(38, 6));
        assert_eq!(
            output_dtype(DecimalDType::new(38, 38), true).unwrap(),
            DecimalDType::new(38, 38)
        );
        let mut repeating = Total::default();
        for value in [1, 0, 0] {
            repeating.add(value, dtype).unwrap();
        }
        assert!(
            repeating
                .finish(dtype, true)
                .unwrap_err()
                .to_string()
                .contains("nonzero fractional digits")
        );
        let mut negative = Total::default();
        for value in [-100, -201] {
            negative.add(value, dtype).unwrap();
        }
        assert_eq!(negative.finish(dtype, true).unwrap(), Some(-1_505_000));
    }

    #[test]
    fn native_decimal_reduction_denied_changes_are_atomic_and_metadata_checked() {
        let dtype = DecimalDType::new(2, 0);
        let mut total = Total::default();
        assert!(total.remove(1, dtype).is_err());
        assert!(total.add(100, dtype).is_err());
        assert_eq!(total, Total::default());
        total.add(99, dtype).unwrap();
        let before = total;
        assert!(total.remove(98, dtype).is_err());
        assert_eq!(total, before);
        let mut full = Total {
            count: u64::MAX,
            ..Total::default()
        };
        let before = full;
        assert!(full.add(0, dtype).is_err());
        assert_eq!(full, before);
        assert!(full.merge(&total).is_err());
        assert_eq!(full, before);
        for invalid in [DecimalDType::new(39, 0), DecimalDType::new(2, -1)] {
            assert!(output_dtype(invalid, false).is_err());
        }
    }
}
