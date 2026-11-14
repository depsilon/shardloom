//! Completed native join work observations.
//!
//! These counters describe work completed by native join owners. They do not
//! imply source-scan I/O avoidance. Runtime-filter rejection is counted only
//! after key validation; callers preserve outer and anti join semantics.

/// Actual runtime-filter work completed by a native join.
///
/// Rejected rows and avoided index lookups describe post-key-validation probe
/// work, not rows or source I/O avoided by the scan.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct NativeRuntimeFilterWork {
    pub considered: u64,
    pub admitted: u64,
    pub declined_small: u64,
    pub declined_memory: u64,
    /// Distinct exact keys in completed build indices, including declined builds.
    pub build_keys: u64,
    pub retained_bytes: u64,
    /// Validated non-NULL key hashes tested by an admitted filter.
    pub probe_rows: u64,
    pub rejected_rows: u64,
    pub index_lookups_avoided: u64,
}

/// Actual hot-group work completed by a native skew path, not an estimate.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct NativeSkewWork {
    pub considered: u64,
    pub hot_groups: u64,
    pub hot_rows: u64,
    pub admitted_groups: u64,
    pub admitted_rows: u64,
    pub declined_memory: u64,
    pub range_visits: u64,
    pub rows_visited: u64,
}

/// Actual native cyclic-existence operator work completed by a query.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct NativeCyclicExistenceWork {
    pub executions: u64,
    pub last_build_rows: u64,
    pub middle_build_rows: u64,
    pub outer_rows: u64,
    pub middle_visits: u64,
    pub last_probes: u64,
    pub witness_exits: u64,
    pub gathered_rows: u64,
    /// Distinct middle rows whose non-key inner payload was checked on a match.
    pub middle_payload_rows_validated: u64,
    /// Distinct last rows whose non-key inner payload was checked on a match.
    pub last_payload_rows_validated: u64,
    /// Selected non-key cells checked without constructing inner product rows.
    pub payload_cells_validated: u64,
}

/// Actual completed-query native join owner work, aggregated without a trace.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct NativeJoinWork {
    pub runtime_filter: NativeRuntimeFilterWork,
    pub skew: NativeSkewWork,
    pub cyclic: NativeCyclicExistenceWork,
}

macro_rules! merge_counters {
    ($this:ident, $other:ident, $($field:ident),+ $(,)?) => {{
        let mut merged = *$this;
        $(
            merged.$field = merged.$field.checked_add($other.$field).ok_or_else(|| {
                crate::ShardLoomError::InvalidOperation(
                    "native join work counter overflow".into(),
                )
            })?;
        )+
        *$this = merged;
        Ok(())
    }};
}

impl NativeRuntimeFilterWork {
    /// Atomically add observations from another completed runtime-filter owner.
    ///
    /// # Errors
    /// Returns a stable error on overflow without changing this value.
    pub fn merge(&mut self, other: Self) -> crate::Result<()> {
        merge_counters!(
            self,
            other,
            considered,
            admitted,
            declined_small,
            declined_memory,
            build_keys,
            retained_bytes,
            probe_rows,
            rejected_rows,
            index_lookups_avoided,
        )
    }
}

impl NativeSkewWork {
    /// Atomically add observations from another completed skew owner.
    ///
    /// # Errors
    /// Returns a stable error on overflow without changing this value.
    pub fn merge(&mut self, other: Self) -> crate::Result<()> {
        merge_counters!(
            self,
            other,
            considered,
            hot_groups,
            hot_rows,
            admitted_groups,
            admitted_rows,
            declined_memory,
            range_visits,
            rows_visited,
        )
    }
}

impl NativeCyclicExistenceWork {
    /// Atomically add observations from another completed cyclic-existence owner.
    ///
    /// # Errors
    /// Returns a stable error on overflow without changing this value.
    pub fn merge(&mut self, other: Self) -> crate::Result<()> {
        merge_counters!(
            self,
            other,
            executions,
            last_build_rows,
            middle_build_rows,
            outer_rows,
            middle_visits,
            last_probes,
            witness_exits,
            gathered_rows,
            middle_payload_rows_validated,
            last_payload_rows_validated,
            payload_cells_validated,
        )
    }
}

impl NativeJoinWork {
    /// Atomically add all observations from another completed query.
    ///
    /// # Errors
    /// Returns a stable error on overflow without changing any child.
    pub fn merge(&mut self, other: Self) -> crate::Result<()> {
        let mut merged = *self;
        merged.runtime_filter.merge(other.runtime_filter)?;
        merged.skew.merge(other.skew)?;
        merged.cyclic.merge(other.cyclic)?;
        *self = merged;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const OVERFLOW: &str = "native join work counter overflow";

    #[test]
    fn child_merge_overflow_in_last_counter_is_atomic() {
        let mut current = NativeRuntimeFilterWork {
            considered: 4,
            admitted: 2,
            index_lookups_avoided: u64::MAX,
            ..NativeRuntimeFilterWork::default()
        };
        let before = current;
        let incoming = NativeRuntimeFilterWork {
            considered: 7,
            admitted: 3,
            index_lookups_avoided: 1,
            ..NativeRuntimeFilterWork::default()
        };

        assert!(matches!(
            current.merge(incoming),
            Err(crate::ShardLoomError::InvalidOperation(message)) if message == OVERFLOW
        ));
        assert_eq!(current, before);
    }

    #[test]
    fn container_merge_overflow_in_last_child_is_atomic() {
        let mut current = NativeJoinWork {
            runtime_filter: NativeRuntimeFilterWork {
                build_keys: 5,
                ..Default::default()
            },
            cyclic: NativeCyclicExistenceWork {
                payload_cells_validated: u64::MAX,
                ..Default::default()
            },
            ..NativeJoinWork::default()
        };
        let before = current;
        let incoming = NativeJoinWork {
            runtime_filter: NativeRuntimeFilterWork {
                build_keys: 9,
                ..Default::default()
            },
            cyclic: NativeCyclicExistenceWork {
                payload_cells_validated: 1,
                ..Default::default()
            },
            ..NativeJoinWork::default()
        };

        assert!(matches!(
            current.merge(incoming),
            Err(crate::ShardLoomError::InvalidOperation(message)) if message == OVERFLOW
        ));
        assert_eq!(current, before);
    }
}
