//! Primitive SUM/AVG totals. Integer inputs retain their existing conversion to
//! f64, but compensate addition error instead of losing small contributions in a
//! long accumulation. Floating inputs retain the original ordered additions.

#[derive(Clone, Copy, Debug)]
pub(super) enum NumericAggregateSum {
    Ordered(f64),
    Integer { sum: f64, correction: f64 },
}

impl Default for NumericAggregateSum {
    fn default() -> Self {
        Self::Ordered(0.0)
    }
}

impl NumericAggregateSum {
    pub(super) fn add(&mut self, value: f64, integer_input: bool) {
        if !integer_input {
            *self = Self::Ordered(self.total() + value);
            return;
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

    pub(super) fn merge(&mut self, other: Self) {
        match other {
            Self::Ordered(value) => self.add(value, matches!(self, Self::Integer { .. })),
            Self::Integer { sum, correction } => {
                self.add(sum, true);
                self.add(correction, true);
            }
        }
    }

    pub(super) fn with_offset(mut self, offset: f64) -> Self {
        self.add(offset, matches!(self, Self::Integer { .. }));
        self
    }

    pub(super) fn total(self) -> f64 {
        match self {
            Self::Ordered(sum) => sum,
            Self::Integer { sum, correction } => sum + correction,
        }
    }

    pub(super) fn is_finite(self) -> bool {
        self.total().is_finite()
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
}
