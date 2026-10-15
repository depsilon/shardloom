//! Per-call wall clocks at the prepared native execution/result boundary.

use std::time::Duration;

/// Disjoint elapsed durations for one completed prepared query.
/// Execution includes source validation, query-time I/O, native operators,
/// native result retention and execution certification. It excludes preparation,
/// result serialization, CLI/report rendering and process startup/shutdown.
/// These are wall times, not CPU time or sums of parallel worker durations.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct QueryTiming {
    pub execution: Duration,
    pub result_serialization: Duration,
}

impl QueryTiming {
    /// Remove synchronous result-sink work from the enclosing query clock.
    #[must_use]
    pub fn from_elapsed(elapsed: Duration, result_serialization: Duration) -> Self {
        debug_assert!(result_serialization <= elapsed);
        Self {
            execution: elapsed.saturating_sub(result_serialization),
            result_serialization,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn synchronous_sink_is_excluded_from_execution() {
        let timing =
            QueryTiming::from_elapsed(Duration::from_nanos(1_000), Duration::from_nanos(350));
        assert_eq!(timing.execution, Duration::from_nanos(650));
        assert_eq!(timing.result_serialization, Duration::from_nanos(350));
    }
}
