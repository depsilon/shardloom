//! Primitive SUM/AVG totals. Integer inputs retain their existing conversion to
//! f64, but compensate addition error instead of losing small contributions in a
//! long accumulation. Floating inputs retain the original ordered additions.

/// Little-endian 128-bit integer accumulator stored as two `u64` words so
/// `NumericAggregateSum` remains 8-byte aligned and 24 bytes wide.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(super) struct CompactI128([u64; 2]);

impl CompactI128 {
    pub(super) const fn from_i128(value: i128) -> Self {
        let unsigned = value.cast_unsigned();
        #[allow(clippy::cast_possible_truncation)]
        Self([unsigned as u64, (unsigned >> 64) as u64])
    }

    pub(super) const fn into_i128(self) -> i128 {
        let unsigned = (self.0[0] as u128) | ((self.0[1] as u128) << 64);
        unsigned.cast_signed()
    }
}

#[derive(Clone, Copy, Debug)]
pub(super) enum NumericAggregateSum {
    Ordered(f64),
    Integer { sum: f64, correction: f64 },
    ExactInteger(CompactI128),
}

impl Default for NumericAggregateSum {
    fn default() -> Self {
        Self::Ordered(0.0)
    }
}

impl NumericAggregateSum {
    pub(super) const fn exact_integer(value: i128) -> Self {
        Self::ExactInteger(CompactI128::from_i128(value))
    }

    pub(super) fn add(&mut self, value: f64, integer_input: bool) {
        if !integer_input {
            *self = Self::Ordered(self.total() + value);
            return;
        }
        if let Self::ExactInteger(acc) = *self {
            let mut sum = 0.0;
            let mut correction = 0.0;
            Self::add_i128_to_parts(&mut sum, &mut correction, acc.into_i128());
            *self = Self::Integer { sum, correction };
        }
        if let Self::Ordered(sum) = *self {
            *self = Self::Integer {
                sum,
                correction: 0.0,
            };
        }
        let Self::Integer { sum, correction } = self else {
            unreachable!("integer accumulation mode was selected above")
        };
        Self::add_integer_parts(sum, correction, value);
    }

    pub(super) fn add_i128(&mut self, value: i128) {
        match self {
            Self::ExactInteger(acc) => {
                let current = acc.into_i128();
                if let Some(next) = current.checked_add(value) {
                    *acc = CompactI128::from_i128(next);
                    return;
                }
                let mut sum = 0.0;
                let mut correction = 0.0;
                Self::add_i128_to_parts(&mut sum, &mut correction, current);
                Self::add_i128_to_parts(&mut sum, &mut correction, value);
                *self = Self::Integer { sum, correction };
            }
            Self::Ordered(acc) if acc.to_bits() == 0.0_f64.to_bits() => {
                *self = Self::exact_integer(value);
            }
            Self::Ordered(acc) => {
                let mut sum = *acc;
                let mut correction = 0.0;
                Self::add_i128_to_parts(&mut sum, &mut correction, value);
                *self = Self::Integer { sum, correction };
            }
            Self::Integer { sum, correction } => {
                Self::add_i128_to_parts(sum, correction, value);
            }
        }
    }

    /// Exact 128-bit integer addition decomposed into four 32-bit exact f64 limbs
    /// when a mixed float or `i128` overflow forces compensated float representation.
    #[allow(clippy::cast_precision_loss)]
    pub(super) fn add_i128_to_parts(sum: &mut f64, correction: &mut f64, value: i128) {
        if value == 0 {
            return;
        }
        if value.unsigned_abs() <= (1_u128 << 53) {
            Self::add_integer_parts(sum, correction, value as f64);
            return;
        }
        let sign = if value < 0 { -1.0 } else { 1.0 };
        let abs = value.unsigned_abs();
        let limb3 = ((abs >> 96) & 0xFFFF_FFFF) as u32;
        let limb2 = ((abs >> 64) & 0xFFFF_FFFF) as u32;
        let limb1 = ((abs >> 32) & 0xFFFF_FFFF) as u32;
        let limb0 = (abs & 0xFFFF_FFFF) as u32;
        if limb3 != 0 {
            Self::add_integer_parts(
                sum,
                correction,
                sign * f64::from(limb3) * f64::from_bits((1023_u64 + 96) << 52),
            );
        }
        if limb2 != 0 {
            Self::add_integer_parts(
                sum,
                correction,
                sign * f64::from(limb2) * f64::from_bits((1023_u64 + 64) << 52),
            );
        }
        if limb1 != 0 {
            Self::add_integer_parts(
                sum,
                correction,
                sign * f64::from(limb1) * f64::from_bits((1023_u64 + 32) << 52),
            );
        }
        if limb0 != 0 {
            Self::add_integer_parts(sum, correction, sign * f64::from(limb0));
        }
    }

    pub(super) fn add_i128_repeated(&mut self, value: i128, count: u64) {
        if count == 0 {
            return;
        }
        if count == 1 {
            self.add_i128(value);
            return;
        }
        if let Some(product) = i128::from(count).checked_mul(value) {
            self.add_i128(product);
            return;
        }
        #[allow(clippy::cast_precision_loss)]
        let value_f64 = value as f64;
        self.add_integer_repeated(value_f64, count);
    }

    pub(super) fn with_i128_offset(mut self, offset: i128) -> Self {
        self.add_i128(offset);
        self
    }

    #[allow(clippy::cast_precision_loss)]
    pub(super) fn average(self, count: u64) -> f64 {
        const MAX_EXACT_F64_INT: u128 = 1_u128 << 53;
        match self {
            Self::ExactInteger(acc) if count > 0 => {
                let total = acc.into_i128();
                if total.unsigned_abs() <= MAX_EXACT_F64_INT
                    && u128::from(count) <= MAX_EXACT_F64_INT
                {
                    return (total as f64) / (count as f64);
                }
                let divisor = i128::from(count);
                let quotient = total / divisor;
                let remainder = total % divisor;
                (quotient as f64) + (remainder as f64) / (count as f64)
            }
            _ => self.total() / (count as f64),
        }
    }

    /// The same ordered compensation with a representation fixed by binding.
    /// Callers retain the count that distinguishes an untouched integer slot
    /// from one whose first integer addition has selected compensation.
    pub(super) fn add_integer_parts(sum: &mut f64, correction: &mut f64, value: f64) {
        let next = *sum + value;
        // Recover the rounding residue from the addition, using the larger
        // operand first so cancellation does not discard the smaller operand.
        *correction += if sum.abs() >= value.abs() {
            (*sum - next) + value
        } else {
            (value - next) + *sum
        };
        *sum = next;
    }

    pub(super) fn add_integer_repeated(&mut self, value: f64, count: u64) {
        fn add_scaled(target: &mut NumericAggregateSum, value: f64, factor: f64) {
            let hi = value * factor;
            if !hi.is_finite() {
                target.add(hi, true);
                return;
            }
            let lo = value.mul_add(factor, -hi);
            target.add(hi, true);
            if lo != 0.0 {
                target.add(lo, true);
            }
        }

        if count == 0 {
            return;
        }
        if count == 1 {
            self.add(value, true);
            return;
        }
        let high = u32::try_from(count >> 32).expect("upper 32 bits fit in u32");
        let low = u32::try_from(count & u64::from(u32::MAX)).expect("lower 32 bits fit in u32");
        if high != 0 {
            add_scaled(self, value, f64::from(high) * 4_294_967_296.0);
        }
        if low != 0 {
            add_scaled(self, value, f64::from(low));
        }
    }

    pub(super) fn merge(&mut self, other: Self) {
        match other {
            Self::Ordered(value)
                if value.to_bits() == 0.0_f64.to_bits()
                    && matches!(self, Self::ExactInteger(_)) => {}
            Self::Ordered(value) => self.add(
                value,
                matches!(self, Self::Integer { .. } | Self::ExactInteger(_)),
            ),
            Self::Integer { sum, correction } => {
                self.add(sum, true);
                self.add(correction, true);
            }
            Self::ExactInteger(value) => self.add_i128(value.into_i128()),
        }
    }

    pub(super) fn with_offset(mut self, offset: f64) -> Self {
        self.add(
            offset,
            matches!(self, Self::Integer { .. } | Self::ExactInteger(_)),
        );
        self
    }

    #[allow(clippy::cast_precision_loss)]
    pub(super) fn total(self) -> f64 {
        match self {
            Self::Ordered(sum) => sum,
            Self::Integer { sum, correction } => sum + correction,
            Self::ExactInteger(sum) => sum.into_i128() as f64,
        }
    }

    pub(super) fn is_finite(self) -> bool {
        matches!(self, Self::ExactInteger(_)) || self.total().is_finite()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn integer_total_recovers_small_terms_and_cancellation_across_partials() {
        let mut total = NumericAggregateSum::default();
        total.add(1_000_000_000_000_000_000.0, true);
        for _ in 0..2_000_000 {
            total.add(1.0, true);
        }
        assert_eq!(total.total(), 1_000_000_000_002_000_000.0);
        let mut tail = NumericAggregateSum::default();
        tail.add(-1_000_000_000_000_000_000.0, true);
        total.merge(tail);
        assert_eq!(total.total(), 2_000_000.0);
    }

    #[test]
    fn floating_total_preserves_order_and_existing_partial_merge() {
        let mut total = NumericAggregateSum::default();
        for value in [1e18, 1.0, -1e18] {
            total.add(value, false);
        }
        assert_eq!(total.total().to_bits(), 0.0_f64.to_bits());
        let mut left = NumericAggregateSum::default();
        left.add(1e18, false);
        let mut right = NumericAggregateSum::default();
        right.add(-1e18, false);
        right.add(1.0, false);
        left.merge(right);
        assert_eq!(left.total().to_bits(), 0.0_f64.to_bits());
    }

    #[test]
    fn totals_expose_nonfinite_results_and_empty_identity() {
        assert_eq!(NumericAggregateSum::default().total(), 0.0);
        for integer in [false, true] {
            let mut total = NumericAggregateSum::default();
            total.add(f64::MAX, integer);
            total.add(f64::MAX, integer);
            assert!(!total.is_finite());
        }
    }

    #[test]
    fn integer_repeated_matches_compensated_loop_and_handles_split_counts() {
        for count in [0_u64, 1, 2, 11, 2_000_000] {
            let mut expected = NumericAggregateSum::default();
            expected.add(1e18, true);
            for _ in 0..count {
                expected.add(1.0, true);
            }
            let mut neg = NumericAggregateSum::default();
            neg.add(-1e18, true);
            expected.merge(neg);

            let mut actual = NumericAggregateSum::default();
            actual.add(1e18, true);
            actual.add_integer_repeated(1.0, count);
            let mut neg_actual = NumericAggregateSum::default();
            neg_actual.add(-1e18, true);
            actual.merge(neg_actual);

            assert_eq!(actual.total().to_bits(), expected.total().to_bits());
        }

        let mut split = NumericAggregateSum::default();
        split.add_integer_repeated(3.0, (1_u64 << 32) + 17);
        assert_eq!(split.total(), 12_884_901_939.0);

        let mut overflow = NumericAggregateSum::default();
        overflow.add_integer_repeated(f64::MAX, 2);
        assert!(!overflow.is_finite());
    }

    #[test]
    fn compact_exact_integer_keeps_24_byte_layout_and_preserves_wide_cancellation() {
        assert_eq!(std::mem::size_of::<NumericAggregateSum>(), 24);
        assert_eq!(std::mem::align_of::<NumericAggregateSum>(), 8);

        let mut cancelled = NumericAggregateSum::default();
        cancelled.add_i128(i128::from(i64::MAX));
        cancelled.add_i128(7);
        cancelled.add_i128(-i128::from(i64::MAX));
        assert!(matches!(cancelled, NumericAggregateSum::ExactInteger(_)));
        assert_eq!(cancelled.total(), 7.0);

        let mut wide_pair = NumericAggregateSum::default();
        wide_pair.add_i128((1_i128 << 54) + 3);
        wide_pair.add_i128(-(1_i128 << 54));
        assert_eq!(wide_pair.total(), 3.0);

        let mut empty = NumericAggregateSum::default();
        let mut exact = NumericAggregateSum::exact_integer((1_i128 << 62) + 3);
        exact.merge(empty);
        assert!(matches!(exact, NumericAggregateSum::ExactInteger(_)));
        exact.add_i128(-(1_i128 << 62));
        assert_eq!(exact.total(), 3.0);

        empty.merge(NumericAggregateSum::exact_integer(19));
        assert!(matches!(empty, NumericAggregateSum::ExactInteger(_)));
        assert_eq!(empty.total(), 19.0);
    }

    #[test]
    fn exact_integer_average_repeated_offset_and_overflow_promotion_preserve_accuracy() {
        // (2^54 + 1) / 3 rounds toward 6004799503160662.0 rather than truncating to 2^54 first.
        let unaligned = NumericAggregateSum::exact_integer((1_i128 << 54) + 1);
        assert_eq!(unaligned.average(3), 6_004_799_503_160_662.0);

        #[allow(clippy::cast_precision_loss)]
        let shifted_avg = ((1_i64 << 53) + 3) as f64;
        let pair = NumericAggregateSum::exact_integer((1_i128 << 54) + 6);
        assert_eq!(pair.average(2), shifted_avg);

        let mut large_cancel = NumericAggregateSum::default();
        large_cancel.add_i128((1_i128 << 55) + 10);
        large_cancel.add_i128(-(1_i128 << 55));
        assert_eq!(large_cancel.average(2), 5.0);

        let mut repeated = NumericAggregateSum::default();
        repeated.add_i128_repeated((1_i128 << 53) + 1, 3);
        let adjusted = repeated.with_i128_offset(-3 * (1_i128 << 53));
        assert_eq!(adjusted.total(), 3.0);

        let mut overflow = NumericAggregateSum::exact_integer(i128::MAX);
        overflow.add_i128(1);
        assert!(matches!(overflow, NumericAggregateSum::Integer { .. }));
        assert!(overflow.is_finite());
        overflow.add_i128(-i128::MAX);
        assert!(overflow.is_finite());
        assert_eq!(overflow.total(), 1.0);
    }
}
