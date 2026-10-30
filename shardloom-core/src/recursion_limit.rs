//! Caller-selected work bound for native recursive relation execution.

use crate::{Result, ShardLoomError};
use std::num::NonZeroU64;

/// Maximum recursive-term evaluations in one recursive relation.
/// The anchor is not an iteration. Reaching this bound never returns a truncated
/// result, changes UNION identity, or grants additional memory or spill space.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct RecursionLimit(NonZeroU64);

impl Default for RecursionLimit {
    fn default() -> Self {
        Self(NonZeroU64::new(1_000).expect("positive default recursion limit"))
    }
}

impl RecursionLimit {
    /// Admit an explicit positive work bound before source effects.
    /// # Errors
    /// Rejects zero, which does not mean unlimited recursion.
    pub fn new(value: u64) -> Result<Self> {
        NonZeroU64::new(value).map(Self).ok_or_else(|| {
            ShardLoomError::InvalidOperation(
                "recursion_limit must be a positive integer; no fallback execution was attempted"
                    .into(),
            )
        })
    }

    /// Parse a positive decimal integer without accepting signs or whitespace.
    /// # Errors
    /// Rejects zero, overflow and malformed policy values.
    pub fn parse(value: &str) -> Result<Self> {
        if value.is_empty() || !value.bytes().all(|byte| byte.is_ascii_digit()) {
            return Err(ShardLoomError::InvalidOperation(
                "recursion_limit requires a positive decimal integer; no fallback execution was attempted"
                    .into(),
            ));
        }
        Self::new(value.parse().map_err(|_| {
            ShardLoomError::InvalidOperation(
                "recursion_limit exceeds the supported integer range; no fallback execution was attempted"
                    .into(),
            )
        })?)
    }

    #[must_use]
    pub const fn get(self) -> u64 {
        self.0.get()
    }

    /// Admit another recursive-term evaluation after `completed` evaluations.
    /// Call only when the current frontier is nonempty. An empty frontier has
    /// already terminated and does not need another evaluation or budget unit.
    /// # Errors
    /// Rejects a required evaluation beyond the exact bound, without overflow.
    pub fn next(self, completed: u64) -> Result<u64> {
        if completed >= self.get() {
            return Err(ShardLoomError::InvalidOperation(format!(
                "SL_RECURSION_LIMIT_EXCEEDED: recursive relation requires another iteration after {} evaluations; increase recursion_limit or provide a terminating recursive term; no fallback execution was attempted",
                self.get()
            )));
        }
        Ok(completed + 1)
    }
}

#[cfg(test)]
mod tests {
    use super::RecursionLimit;

    #[test]
    fn positive_limits_reject_implicit_unbounded_and_malformed_values() {
        assert_eq!(RecursionLimit::default().get(), 1_000);
        for invalid in [
            "",
            "0",
            "+1",
            "-1",
            " 1",
            "1 ",
            "1.0",
            "1e3",
            "18446744073709551616",
        ] {
            assert!(RecursionLimit::parse(invalid).is_err(), "{invalid}");
        }
        assert!(RecursionLimit::new(0).is_err());
        assert_eq!(RecursionLimit::parse("1").unwrap().get(), 1);
        assert_eq!(RecursionLimit::new(u64::MAX).unwrap().get(), u64::MAX);
    }

    #[test]
    fn exact_work_boundary_never_truncates_or_overflows() {
        let limit = RecursionLimit::new(2).unwrap();
        assert_eq!(limit.next(0).unwrap(), 1);
        assert_eq!(limit.next(1).unwrap(), 2);
        assert!(
            limit
                .next(2)
                .unwrap_err()
                .message()
                .starts_with("SL_RECURSION_LIMIT_EXCEEDED:")
        );
        assert!(limit.next(u64::MAX).is_err());
        let largest = RecursionLimit::new(u64::MAX).unwrap();
        assert_eq!(largest.next(u64::MAX - 1).unwrap(), u64::MAX);
        assert!(largest.next(u64::MAX).is_err());
    }
}
