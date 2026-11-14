//! Completed grouped COUNT batch partial-work observations.
//!
//! Input rows, reduced groups, and partial-capacity bytes sum only admitted
//! batches. Capacity is summed across partials, not peak or live memory. Declined
//! counts describe attempts before state mutation; these counters do not claim
//! file I/O avoidance.

/// Completed grouped COUNT batch partial work, aggregated without a trace.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct NativeCountPartialWork {
    pub considered_batches: u64,
    pub admitted_batches: u64,
    pub input_rows: u64,
    pub reduced_groups: u64,
    pub constant_batches: u64,
    pub declined_unique_batches: u64,
    pub declined_capacity_batches: u64,
    pub partial_capacity_bytes: u64,
}

impl NativeCountPartialWork {
    /// Atomically add completed grouped COUNT batch partial observations.
    ///
    /// # Errors
    /// Returns a stable error on overflow without changing this value.
    pub fn merge(&mut self, other: Self) -> crate::Result<()> {
        let mut merged = *self;
        merged.considered_batches = merged
            .considered_batches
            .checked_add(other.considered_batches)
            .ok_or_else(overflow)?;
        merged.admitted_batches = merged
            .admitted_batches
            .checked_add(other.admitted_batches)
            .ok_or_else(overflow)?;
        merged.input_rows = merged
            .input_rows
            .checked_add(other.input_rows)
            .ok_or_else(overflow)?;
        merged.reduced_groups = merged
            .reduced_groups
            .checked_add(other.reduced_groups)
            .ok_or_else(overflow)?;
        merged.constant_batches = merged
            .constant_batches
            .checked_add(other.constant_batches)
            .ok_or_else(overflow)?;
        merged.declined_unique_batches = merged
            .declined_unique_batches
            .checked_add(other.declined_unique_batches)
            .ok_or_else(overflow)?;
        merged.declined_capacity_batches = merged
            .declined_capacity_batches
            .checked_add(other.declined_capacity_batches)
            .ok_or_else(overflow)?;
        merged.partial_capacity_bytes = merged
            .partial_capacity_bytes
            .checked_add(other.partial_capacity_bytes)
            .ok_or_else(overflow)?;
        *self = merged;
        Ok(())
    }
}

fn overflow() -> crate::ShardLoomError {
    crate::ShardLoomError::InvalidOperation("native grouped COUNT partial counter overflow".into())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn merge_overflow_in_last_counter_is_atomic() {
        let mut current = NativeCountPartialWork {
            considered_batches: 2,
            admitted_batches: 1,
            partial_capacity_bytes: u64::MAX,
            ..NativeCountPartialWork::default()
        };
        let before = current;
        let incoming = NativeCountPartialWork {
            considered_batches: 3,
            admitted_batches: 4,
            partial_capacity_bytes: 1,
            ..NativeCountPartialWork::default()
        };

        assert!(matches!(
            current.merge(incoming),
            Err(crate::ShardLoomError::InvalidOperation(message))
                if message == "native grouped COUNT partial counter overflow"
        ));
        assert_eq!(current, before);
    }
}
