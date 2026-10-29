//! Centered online moments with an original-width integer origin. Subtracting
//! integer origins before conversion preserves small differences above 2^53.
//! Power-of-two scales keep centered products away from overflow/underflow;
//! units are restored only when the requested statistic is finalized.
//! The centered update/merge formulas follow Pebay, SAND2008-6212 (2008).
//! This implementation is original and shares no external engine execution.

use super::{NumericAggregateSum, native_relational_batch::failed, native_relational_keys::Cell};
use shardloom_core::{FloatPolicy, Result, float_policy::CANONICAL_F64_NAN_BITS};

fn special(value: &Cell, policy: FloatPolicy) -> Result<bool> {
    match value {
        Cell::Float(bits) => {
            let value = f64::from_bits(*bits);
            if !policy.admits(value) {
                return Err(failed("statistical input is nonfinite"));
            }
            Ok(!value.is_finite())
        }
        Cell::Null | Cell::NegativeInteger(_) | Cell::NonnegativeInteger(_) => Ok(false),
        _ => Err(failed(
            "statistical input requires primitive numeric values",
        )),
    }
}

fn count_with_specials(count: u64, specials: u64) -> Result<u64> {
    count
        .checked_add(specials)
        .ok_or_else(|| failed("statistical count overflow"))
}

#[derive(Debug, Clone, Copy)]
enum Number {
    Integer(i128),
    Float(f64),
}

impl Number {
    fn from_cell(value: &Cell) -> Result<Option<Self>> {
        Ok(match value {
            Cell::Null => None,
            Cell::NegativeInteger(value) => Some(Self::Integer(i128::from(*value))),
            Cell::NonnegativeInteger(value) => Some(Self::Integer(i128::from(*value))),
            Cell::Float(bits) if f64::from_bits(*bits).is_finite() => {
                Some(Self::Float(f64::from_bits(*bits)))
            }
            Cell::Float(_) => return Err(failed("statistical input is nonfinite")),
            _ => {
                return Err(failed(
                    "statistical input requires primitive numeric values",
                ));
            }
        })
    }

    #[allow(clippy::cast_precision_loss)] // Output moments are explicitly Float64; integer subtraction occurs first.
    fn difference(self, other: Self) -> f64 {
        match (self, other) {
            (Self::Integer(left), Self::Integer(right)) => (left - right) as f64,
            (Self::Float(left), Self::Float(right)) => left - right,
            (Self::Integer(left), Self::Float(right)) => left as f64 - right,
            (Self::Float(left), Self::Integer(right)) => left - right as f64,
        }
    }

    fn difference_scale(self, other: Self) -> f64 {
        let difference = self.difference(other).abs();
        if difference.is_infinite() {
            return f64::from_bits(2046_u64 << 52);
        }
        binary_scale(difference)
    }

    #[allow(clippy::cast_precision_loss)] // Integer differences are converted before normalization, never individual integer operands.
    fn offset(self, origin: Self, scale: f64) -> f64 {
        if scale == 0.0 {
            return 0.0;
        }
        let difference = self.difference(origin);
        if difference.is_finite() {
            return difference / scale;
        }
        // Two finite floating observations can differ by more than f64::MAX.
        // Their normalized difference is still finite. Mixed numeric variants
        // are also handled here for independently merged summaries.
        let value = |number| match number {
            Self::Integer(number) => number as f64 / scale,
            Self::Float(number) => number / scale,
        };
        value(self) - value(origin)
    }
}

fn binary_scale(value: f64) -> f64 {
    let bits = value.abs().to_bits();
    let exponent = bits & 0x7ff0_0000_0000_0000;
    if exponent != 0 {
        f64::from_bits(exponent)
    } else if bits != 0 {
        f64::from_bits(1_u64 << bits.ilog2())
    } else {
        0.0
    }
}

fn scale_ratio(before: f64, after: f64) -> f64 {
    if before == 0.0 { 0.0 } else { before / after }
}

fn scaled_sum(value: NumericAggregateSum, factor: f64) -> NumericAggregateSum {
    match value {
        NumericAggregateSum::Ordered(value) => NumericAggregateSum::Ordered(value * factor),
        NumericAggregateSum::Integer { sum, correction } => NumericAggregateSum::Integer {
            sum: sum * factor,
            correction: correction * factor,
        },
    }
}

fn binary_exponent(value: f64) -> i32 {
    let bits = value.abs().to_bits();
    let exponent = i32::try_from(bits >> 52).expect("finite exponent fits i32");
    if exponent == 0 {
        -1074 + i32::try_from(bits.ilog2()).expect("mantissa width fits i32")
    } else {
        exponent - 1023
    }
}

/// Restore two power-of-two units without multiplying the units first. The
/// final subnormal multiplication performs the only underflow rounding.
fn restore_product(value: f64, x: f64, y: f64) -> f64 {
    if value == 0.0 || x == 0.0 || y == 0.0 {
        return value * 0.0;
    }
    let exponent = binary_exponent(value) + binary_exponent(x) + binary_exponent(y);
    let mantissa = value / binary_scale(value);
    if exponent > 1023 {
        return f64::INFINITY.copysign(value);
    }
    if exponent < -1075 {
        return 0.0_f64.copysign(value);
    }
    if exponent >= -1022 {
        mantissa * f64::from_bits(u64::try_from(exponent + 1023).expect("positive exponent") << 52)
    } else {
        let factor =
            f64::from_bits(u64::try_from(exponent + 2045).expect("subnormal exponent") << 52);
        (mantissa * factor) * f64::MIN_POSITIVE
    }
}

#[derive(Debug, Clone, Copy, Default)]
pub(super) struct Moments {
    count: u64,
    nonfinite_count: u64,
    origin: Option<Number>,
    scale: f64,
    mean_offset: NumericAggregateSum,
    centered_sum: NumericAggregateSum,
}

impl Moments {
    #[cfg(test)]
    pub(super) fn observe(&mut self, value: &Cell) -> Result<()> {
        self.observe_with_policy(value, FloatPolicy::Strict)
    }

    pub(super) fn observe_with_policy(&mut self, value: &Cell, policy: FloatPolicy) -> Result<()> {
        if matches!(value, Cell::Null) {
            return Ok(());
        }
        count_with_specials(count_with_specials(self.count, self.nonfinite_count)?, 1)?;
        if special(value, policy)? {
            self.nonfinite_count += 1; // Total cardinality was checked above.
            return Ok(());
        }
        let Some(value) = Number::from_cell(value)? else {
            return Ok(());
        };
        self.observe_number(value)
    }

    #[allow(clippy::cast_precision_loss)] // Count normalization is explicitly Float64.
    fn observe_number(&mut self, value: Number) -> Result<()> {
        let count = self
            .count
            .checked_add(1)
            .ok_or_else(|| failed("statistical count overflow"))?;
        let Some(origin) = self.origin else {
            self.origin = Some(value);
            self.count = count;
            return Ok(());
        };
        let scale = self.scale.max(value.difference_scale(origin));
        let mut next = self.rescaled(scale);
        let offset = value.offset(origin, scale);
        let delta = offset - next.mean_offset.total();
        let mut mean = next.mean_offset;
        mean.add(delta / count as f64, true);
        let mut centered = next.centered_sum;
        centered.add(delta * (offset - mean.total()), true);
        if !mean.is_finite() || !centered.is_finite() {
            return Err(failed("statistical accumulator became nonfinite"));
        }
        next.mean_offset = mean;
        next.centered_sum = centered;
        next.count = count;
        *self = next;
        Ok(())
    }

    fn rescaled(mut self, scale: f64) -> Self {
        if self.scale.to_bits() != scale.to_bits() {
            let ratio = scale_ratio(self.scale, scale);
            self.mean_offset = scaled_sum(self.mean_offset, ratio);
            // Apply each scale separately; forming ratio squared can underflow
            // before it is multiplied by a large count-weighted centered sum.
            self.centered_sum = scaled_sum(scaled_sum(self.centered_sum, ratio), ratio);
            self.scale = scale;
        }
        self
    }

    pub(super) fn merge(&mut self, other: Self) -> Result<()> {
        let nonfinite_count = count_with_specials(self.nonfinite_count, other.nonfinite_count)?;
        count_with_specials(
            count_with_specials(self.count, other.count)?,
            nonfinite_count,
        )?;
        let mut next = *self;
        next.merge_finite(other)?;
        next.nonfinite_count = nonfinite_count;
        *self = next;
        Ok(())
    }

    #[allow(clippy::cast_precision_loss)] // Pairwise normalization uses Float64 after checked exact counts.
    fn merge_finite(&mut self, other: Self) -> Result<()> {
        if other.count == 0 {
            return Ok(());
        }
        if self.count == 0 {
            *self = other;
            return Ok(());
        }
        let count = self
            .count
            .checked_add(other.count)
            .ok_or_else(|| failed("statistical count overflow"))?;
        let scale = self.scale.max(other.scale).max(
            other
                .origin
                .ok_or_else(|| failed("statistical origin is absent"))?
                .difference_scale(
                    self.origin
                        .ok_or_else(|| failed("statistical origin is absent"))?,
                ),
        );
        let mut next = self.rescaled(scale);
        let other = other.rescaled(scale);
        let delta = next.center_difference(&other, scale)?;
        let mut mean = next.mean_offset;
        mean.add(delta * (other.count as f64 / count as f64), true);
        let mut centered = next.centered_sum;
        centered.merge(other.centered_sum);
        centered.add(
            delta * (delta * (self.count as f64 / count as f64)) * other.count as f64,
            true,
        );
        if !mean.is_finite() || !centered.is_finite() {
            return Err(failed("statistical accumulator became nonfinite"));
        }
        next.mean_offset = mean;
        next.centered_sum = centered;
        next.count = count;
        *self = next;
        Ok(())
    }

    fn center_difference(&self, other: &Self, scale: f64) -> Result<f64> {
        Ok(other
            .origin
            .ok_or_else(|| failed("statistical origin is absent"))?
            .offset(
                self.origin
                    .ok_or_else(|| failed("statistical origin is absent"))?,
                scale,
            )
            + other.mean_offset.total() * scale_ratio(other.scale, scale)
            - self.mean_offset.total() * scale_ratio(self.scale, scale))
    }

    #[allow(clippy::cast_precision_loss)] // Variance is an explicitly approximate Float64 result.
    pub(super) fn finish(&self, sample: bool, standard_deviation: bool) -> Result<Option<f64>> {
        let correction = u64::from(sample);
        if count_with_specials(self.count, self.nonfinite_count)? <= correction {
            return Ok(None);
        }
        if self.nonfinite_count != 0 {
            return Ok(Some(f64::from_bits(CANONICAL_F64_NAN_BITS)));
        }
        let variance = self.centered_sum.total() / (self.count - correction) as f64;
        if !variance.is_finite() || variance < 0.0 {
            return Err(failed("statistical variance is outside its finite domain"));
        }
        let result = if standard_deviation {
            variance.sqrt() * self.scale
        } else {
            restore_product(variance, self.scale, self.scale)
        };
        if !result.is_finite() {
            return Err(failed("statistical result is outside its finite domain"));
        }
        Ok(Some(result))
    }
}

#[derive(Debug, Clone, Copy, Default)]
pub(super) struct PairedMoments {
    x: Moments,
    y: Moments,
    cross: NumericAggregateSum,
    nonfinite_count: u64,
}

impl PairedMoments {
    #[cfg(test)]
    pub(super) fn observe(&mut self, x: &Cell, y: &Cell) -> Result<()> {
        self.observe_with_policy(x, y, FloatPolicy::Strict)
    }

    pub(super) fn observe_with_policy(
        &mut self,
        x: &Cell,
        y: &Cell,
        policy: FloatPolicy,
    ) -> Result<()> {
        // Pair selection precedes domain observation of either companion.
        if matches!(x, Cell::Null) || matches!(y, Cell::Null) {
            return Ok(());
        }
        count_with_specials(count_with_specials(self.x.count, self.nonfinite_count)?, 1)?;
        let x_special = special(x, policy)?;
        let y_special = special(y, policy)?;
        if x_special || y_special {
            self.nonfinite_count += 1;
            return Ok(());
        }
        let x = Number::from_cell(x)?.ok_or_else(|| failed("nonnull pair became null"))?;
        let y = Number::from_cell(y)?.ok_or_else(|| failed("nonnull pair became null"))?;
        let mut next = *self;
        next.x.observe_number(x)?;
        next.y.observe_number(y)?;
        if let (Some(x_origin), Some(y_origin)) = (self.x.origin, self.y.origin) {
            next.cross = scaled_sum(
                scaled_sum(self.cross, scale_ratio(self.x.scale, next.x.scale)),
                scale_ratio(self.y.scale, next.y.scale),
            );
            let delta_x = x.offset(x_origin, next.x.scale)
                - self.x.mean_offset.total() * scale_ratio(self.x.scale, next.x.scale);
            let centered_y = y.offset(y_origin, next.y.scale) - next.y.mean_offset.total();
            next.cross.add(delta_x * centered_y, true);
            if !next.cross.is_finite() {
                return Err(failed("statistical cross-product became nonfinite"));
            }
        }
        *self = next;
        Ok(())
    }

    pub(super) fn merge(&mut self, other: Self) -> Result<()> {
        let nonfinite_count = count_with_specials(self.nonfinite_count, other.nonfinite_count)?;
        count_with_specials(
            count_with_specials(self.x.count, other.x.count)?,
            nonfinite_count,
        )?;
        let mut next = *self;
        next.merge_finite(other)?;
        next.nonfinite_count = nonfinite_count;
        *self = next;
        Ok(())
    }

    #[allow(clippy::cast_precision_loss)] // Centered products are explicitly Float64 after exact count checks.
    fn merge_finite(&mut self, other: Self) -> Result<()> {
        if other.x.count == 0 {
            return Ok(());
        }
        if self.x.count == 0 {
            *self = other;
            return Ok(());
        }
        let mut next = *self;
        next.x.merge(other.x)?;
        next.y.merge(other.y)?;
        let delta_x = self.x.center_difference(&other.x, next.x.scale)?;
        let delta_y = self.y.center_difference(&other.y, next.y.scale)?;
        next.cross = scaled_sum(
            scaled_sum(self.cross, scale_ratio(self.x.scale, next.x.scale)),
            scale_ratio(self.y.scale, next.y.scale),
        );
        next.cross.merge(scaled_sum(
            scaled_sum(other.cross, scale_ratio(other.x.scale, next.x.scale)),
            scale_ratio(other.y.scale, next.y.scale),
        ));
        next.cross.add(
            delta_x
                * (delta_y * (self.x.count as f64 / next.x.count as f64))
                * other.x.count as f64,
            true,
        );
        if !next.cross.is_finite() {
            return Err(failed("statistical cross-product became nonfinite"));
        }
        *self = next;
        Ok(())
    }

    #[allow(clippy::cast_precision_loss)] // Covariance is an explicitly approximate Float64 result.
    pub(super) fn finish(&self, sample: bool, correlation: bool) -> Result<Option<f64>> {
        let correction = u64::from(sample || correlation);
        if count_with_specials(self.x.count, self.nonfinite_count)? <= correction {
            return Ok(None);
        }
        if self.nonfinite_count != 0 {
            return Ok(Some(f64::from_bits(CANONICAL_F64_NAN_BITS)));
        }
        let result = if correlation {
            let x = self.x.centered_sum.total();
            let y = self.y.centered_sum.total();
            if x == 0.0 || y == 0.0 {
                return Ok(None);
            }
            // Do not form x*y: it can overflow for a perfectly valid correlation.
            let correlation = (self.cross.total() / x.sqrt()) / y.sqrt();
            if !correlation.is_finite() || correlation.abs() > 1.0 + 64.0 * f64::EPSILON {
                return Err(failed(
                    "statistical correlation is outside its finite domain",
                ));
            }
            correlation.clamp(-1.0, 1.0)
        } else {
            restore_product(
                self.cross.total() / (self.x.count - correction) as f64,
                self.x.scale,
                self.y.scale,
            )
        };
        if !result.is_finite() {
            return Err(failed(
                "statistical covariance is outside its finite domain",
            ));
        }
        Ok(Some(result))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn statistical_extended_specials_preserve_cardinality_null_pairs_and_finite_merges() {
        let extended = FloatPolicy::Extended;
        let scalar = |value: f64| Cell::Float(value.to_bits());
        for value in [
            f64::INFINITY,
            f64::NEG_INFINITY,
            f64::from_bits(0xfff0_0000_0000_0001),
        ] {
            let mut special = Moments::default();
            special
                .observe_with_policy(&scalar(value), extended)
                .unwrap();
            assert_eq!(special.finish(true, false).unwrap(), None);
            assert_eq!(
                special.finish(false, false).unwrap().unwrap().to_bits(),
                CANONICAL_F64_NAN_BITS
            );
            let mut finite = Moments::default();
            finite.observe(&scalar(2.0)).unwrap();
            finite.observe(&scalar(4.0)).unwrap();
            for (mut left, right) in [(finite, special), (special, finite)] {
                left.merge(right).unwrap();
                assert_eq!(left.count, 2);
                assert_eq!(left.nonfinite_count, 1);
                for sample in [false, true] {
                    assert_eq!(
                        left.finish(sample, false).unwrap().unwrap().to_bits(),
                        CANONICAL_F64_NAN_BITS
                    );
                }
            }
            assert_eq!(finite.finish(false, false).unwrap(), Some(1.0));

            let mut pair = PairedMoments::default();
            pair.observe_with_policy(&scalar(value), &Cell::Null, extended)
                .unwrap();
            pair.observe_with_policy(&Cell::Null, &scalar(value), FloatPolicy::Strict)
                .unwrap();
            assert_eq!(pair.nonfinite_count, 0);
            pair.observe_with_policy(&scalar(value), &scalar(2.0), extended)
                .unwrap();
            assert_eq!(pair.finish(true, false).unwrap(), None);
            assert_eq!(
                pair.finish(false, false).unwrap().unwrap().to_bits(),
                CANONICAL_F64_NAN_BITS
            );
            let mut finite = PairedMoments::default();
            finite.observe(&scalar(1.0), &scalar(3.0)).unwrap();
            finite.observe(&scalar(2.0), &scalar(5.0)).unwrap();
            for (mut left, right) in [(finite, pair), (pair, finite)] {
                left.merge(right).unwrap();
                for correlation in [false, true] {
                    assert_eq!(
                        left.finish(false, correlation).unwrap().unwrap().to_bits(),
                        CANONICAL_F64_NAN_BITS
                    );
                }
            }
        }
    }

    #[test]
    fn statistical_scaled_moments_preserve_tiny_large_and_subnormal_results_across_merges() {
        let float = |value: f64| Cell::Float(value.to_bits());
        for value in [1e-200, 1e200, f64::from_bits(2)] {
            for split in 0..=2 {
                let mut left = Moments::default();
                let mut right = Moments::default();
                let mut paired_left = PairedMoments::default();
                let mut paired_right = PairedMoments::default();
                for (row, number) in [0.0, value].into_iter().enumerate() {
                    let (single, pair) = if row < split {
                        (&mut left, &mut paired_left)
                    } else {
                        (&mut right, &mut paired_right)
                    };
                    single.observe(&float(number)).unwrap();
                    pair.observe(&float(number), &float(-number)).unwrap();
                }
                left.merge(right).unwrap();
                paired_left.merge(paired_right).unwrap();
                assert!(
                    (left.finish(false, true).unwrap().unwrap() / (value * 0.5) - 1.0).abs()
                        < 1e-14
                );
                assert!((paired_left.finish(false, true).unwrap().unwrap() + 1.0).abs() < 1e-14);
            }
        }
        for (x, y, covariance) in [
            (2.0f64.powi(1000), 2.0f64.powi(-1000), 0.25),
            (2.0f64.powi(-500), 2.0f64.powi(-572), f64::from_bits(1)),
        ] {
            for split in 0..=2 {
                let mut left = PairedMoments::default();
                let mut right = PairedMoments::default();
                for (row, (x, y)) in [(0.0, 0.0), (x, y)].into_iter().enumerate() {
                    if row < split { &mut left } else { &mut right }
                        .observe(&float(x), &float(y))
                        .unwrap();
                }
                left.merge(right).unwrap();
                assert_eq!(left.finish(false, false).unwrap(), Some(covariance));
                assert!((left.finish(false, true).unwrap().unwrap() - 1.0).abs() < 1e-14);
            }
        }
        let mut extreme = Moments::default();
        let mut pair = PairedMoments::default();
        for value in [-f64::MAX, f64::MAX] {
            extreme.observe(&float(value)).unwrap();
            pair.observe(&float(value), &float(-value)).unwrap();
        }
        assert_eq!(extreme.finish(false, true).unwrap(), Some(f64::MAX));
        assert!(extreme.finish(false, false).is_err());
        assert!(extreme.finish(true, true).is_err());
        assert!((pair.finish(false, true).unwrap().unwrap() + 1.0).abs() < 1e-14);
        assert!(pair.finish(false, false).is_err());
    }

    #[test]
    fn statistical_merge_scales_before_multiplying_to_preserve_finite_large_moments() {
        let low = Cell::Float(0.0f64.to_bits());
        let high = Cell::Float(1.5e154f64.to_bits());
        let mut left = Moments::default();
        let mut right = Moments::default();
        left.observe(&low).unwrap();
        right.observe(&high).unwrap();
        left.merge(right).unwrap();
        let expected = 5.625e307;
        assert!((left.finish(false, false).unwrap().unwrap() / expected - 1.0).abs() < 1e-14);
        let mut left = PairedMoments::default();
        let mut right = PairedMoments::default();
        left.observe(&low, &low).unwrap();
        right.observe(&high, &high).unwrap();
        left.merge(right).unwrap();
        assert!((left.finish(false, false).unwrap().unwrap() / expected - 1.0).abs() < 1e-14);
        assert!((left.finish(false, true).unwrap().unwrap() - 1.0).abs() < 1e-14);
    }

    fn values(input: &[i64]) -> Moments {
        let mut value = Moments::default();
        for &number in input {
            value
                .observe(&if number < 0 {
                    Cell::NegativeInteger(number)
                } else {
                    Cell::NonnegativeInteger(number.unsigned_abs())
                })
                .unwrap();
        }
        value
    }

    #[test]
    fn statistical_moments_match_centered_reference_and_small_sample_semantics() {
        let data = values(&[2, 4, 4, 4, 5, 5, 7, 9]);
        assert!((data.finish(false, false).unwrap().unwrap() - 4.0).abs() < 1e-14);
        assert!((data.finish(true, false).unwrap().unwrap() - 32.0 / 7.0).abs() < 1e-14);
        assert!((data.finish(false, true).unwrap().unwrap() - 2.0).abs() < 1e-14);
        let mut empty = Moments::default();
        empty.observe(&Cell::Null).unwrap();
        assert_eq!(empty.finish(false, false).unwrap(), None);
        let single = values(&[19]);
        assert_eq!(single.finish(false, false).unwrap(), Some(0.0));
        assert_eq!(single.finish(true, false).unwrap(), None);
    }

    #[test]
    fn statistical_moments_preserve_small_integer_differences_and_partition_merges() {
        for origin in [i64::MIN, -10, 0, 1_i64 << 60, i64::MAX - 3] {
            let data = [origin, origin + 1, origin + 2, origin + 3];
            let full = values(&data);
            assert_eq!(full.finish(false, false).unwrap(), Some(1.25));
            for split in 0..=data.len() {
                let mut left = values(&data[..split]);
                left.merge(values(&data[split..])).unwrap();
                assert_eq!(left.finish(false, false).unwrap(), Some(1.25));
            }
        }
        let mut unsigned = Moments::default();
        for value in [u64::MAX - 1, u64::MAX] {
            unsigned.observe(&Cell::NonnegativeInteger(value)).unwrap();
        }
        assert_eq!(unsigned.finish(false, false).unwrap(), Some(0.25));
    }

    #[test]
    fn statistical_moments_refuse_nonfinite_and_overflow_without_changing_state() {
        let mut value = values(&[3, 5]);
        for invalid in [f64::NAN, f64::INFINITY, f64::NEG_INFINITY] {
            assert!(value.observe(&Cell::Float(invalid.to_bits())).is_err());
            assert_eq!(value.finish(false, false).unwrap(), Some(1.0));
        }
        value.count = u64::MAX;
        assert!(value.observe(&Cell::NonnegativeInteger(7)).is_err());
        assert_eq!(value.count, u64::MAX);
        assert!(value.merge(values(&[7])).is_err());
        assert_eq!(value.count, u64::MAX);
    }

    #[test]
    fn statistical_paired_moments_preserve_null_pairs_original_width_and_merges() {
        let input = [1u64, 2, 3, 4].map(|x| Cell::NonnegativeInteger(u64::MAX - 4 + x));
        let output = [2u64, 1, 4, 3].map(|y| Cell::NonnegativeInteger(u64::MAX - 4 + y));
        let part = |start, end| {
            let mut moments = PairedMoments::default();
            for row in start..end {
                moments.observe(&input[row], &output[row]).unwrap();
            }
            moments
        };
        for split in 0..=4 {
            let mut moments = part(0, split);
            moments.merge(part(split, 4)).unwrap();
            moments
                .observe(&Cell::Null, &Cell::Float(f64::NAN.to_bits()))
                .unwrap();
            moments
                .observe(&Cell::Float(f64::INFINITY.to_bits()), &Cell::Null)
                .unwrap();
            for (sample, correlation, expected) in
                [(false, false, 0.75), (true, false, 1.0), (false, true, 0.6)]
            {
                assert!(
                    (moments.finish(sample, correlation).unwrap().unwrap() - expected).abs()
                        < 1e-14
                );
            }
            assert!(
                moments
                    .observe(&Cell::Float(f64::NAN.to_bits()), &input[0])
                    .is_err()
            );
            assert!((moments.finish(false, true).unwrap().unwrap() - 0.6).abs() < 1e-14);
        }
        let mut singleton = part(0, 1);
        assert_eq!(singleton.finish(false, false).unwrap(), Some(0.0));
        assert_eq!(singleton.finish(true, false).unwrap(), None);
        assert_eq!(singleton.finish(false, true).unwrap(), None);
        singleton.observe(&input[0], &output[1]).unwrap();
        assert_eq!(singleton.finish(false, true).unwrap(), None);
    }

    #[test]
    fn statistical_correlation_normalization_does_not_overflow_variance_product() {
        let mut moments = PairedMoments::default();
        for value in [1e100f64, 2e100, 3e100] {
            moments
                .observe(
                    &Cell::Float(value.to_bits()),
                    &Cell::Float((-value).to_bits()),
                )
                .unwrap();
        }
        assert!((moments.finish(false, true).unwrap().unwrap() + 1.0).abs() < 1e-14);
    }
}
