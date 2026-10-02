//! One ingest CPU grant across the admitted source, conversion and writer owners.
//!
//! This module creates no workers. Source admission and the streaming writer
//! construct their owned workers from the same policy. Blocking I/O and source
//! library internal threads remain separately scoped.

#[cfg(any(test, feature = "vortex-write"))]
use shardloom_core::{Result, ShardLoomError};

/// Measured policy inputs, not independent grants. Unused lanes go to the native
/// writer runtime, which also retains the caller as its mandatory progress lane.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct IngestCpuDemand {
    pub(crate) source_workers: usize,
    pub(crate) conversion_workers: usize,
    pub(crate) prefetch_slots: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct IngestCpuLanes {
    requested: usize,
    source_workers: usize,
    conversion_workers: usize,
    provider_drivers: usize,
    prefetch_slots: usize,
    shared_runtime: bool,
}

/// Actual ownership observations required before replacing a live plan. A zero
/// queued-work count alone does not prove background threads have been joined.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
#[cfg(test)]
pub(crate) struct IngestCpuActivity {
    pub(crate) source_threads: usize,
    pub(crate) conversion_threads: usize,
    pub(crate) provider_threads: usize,
    pub(crate) queued_or_active_jobs: usize,
    pub(crate) retained_unpublished_batches: usize,
}

// A compatibility-reader-only build uses the source grant; remaining accessors
// are consumed when the optional native writer is enabled.
#[cfg_attr(not(feature = "vortex-write"), allow(dead_code))]
impl IngestCpuLanes {
    pub(crate) const CALLER_LANES: usize = 1;

    /// Allocate a positive grant once. `source_task_capacity` is the number of
    /// independent source tasks supported by the actual reader; zero selects
    /// synchronous source pull on the conversion owner or caller.
    #[cfg(any(test, feature = "vortex-write"))]
    pub(crate) fn allocate(
        requested: usize,
        source_task_capacity: usize,
        demand: IngestCpuDemand,
    ) -> Result<Self> {
        if requested == 0 {
            return Err(lane_error("requested CPU grant must be positive"));
        }
        if demand.conversion_workers > 0 && demand.prefetch_slots == 0 {
            return Err(lane_error(
                "conversion workers require a bounded prefetch slot",
            ));
        }
        Ok(Self::partition(requested, source_task_capacity, demand))
    }

    /// Initial measured-candidate recipe. Source work has one ordered producer,
    /// conversion has one worker when there is room, and the native writer gets
    /// the remaining grant. This is a policy choice, not a claim of optimality.
    #[cfg(any(test, not(feature = "vortex-write")))]
    pub(crate) fn pipeline(requested: usize, source_task_capacity: usize) -> Self {
        Self::partition(
            requested.max(1),
            source_task_capacity,
            Self::pipeline_demand(requested, 1),
        )
    }

    /// Reconcile a source already admitted by `ShardLoom` before starting any
    /// conversion/provider owner. Unknown external reader threads are excluded.
    #[cfg(any(test, feature = "vortex-write"))]
    pub(crate) fn with_admitted_source(requested: usize, source_workers: usize) -> Result<Self> {
        if requested == 0 {
            return Err(lane_error("requested CPU grant must be positive"));
        }
        let applied = shardloom_exec::compute_pool::bounded_cpu_parallelism(requested);
        if source_workers > applied.saturating_sub(Self::CALLER_LANES) {
            return Err(lane_error(
                "existing source workers exceed the requested grant",
            ));
        }
        let mut lanes = Self::allocate(
            applied,
            source_workers,
            Self::pipeline_demand(applied, source_workers),
        )?;
        lanes.requested = requested;
        Ok(lanes)
    }

    /// Ready source/conversion/provider tasks share every native driver. Task
    /// window sizes are not additional thread grants.
    #[cfg(feature = "vortex-write")]
    pub(crate) fn shared(requested: usize, applied: usize) -> Result<Self> {
        if applied == 0 || applied > requested {
            return Err(lane_error(
                "applied CPU grant must be positive and within the requested maximum",
            ));
        }
        Ok(Self {
            requested,
            source_workers: 0,
            conversion_workers: 0,
            provider_drivers: applied - Self::CALLER_LANES,
            prefetch_slots: if applied == 1 { 0 } else { applied },
            shared_runtime: true,
        })
    }

    #[cfg(feature = "vortex-write")]
    pub(crate) const fn shares_runtime(self) -> bool {
        self.shared_runtime
    }

    #[cfg(feature = "vortex-write")]
    pub(crate) const fn conversion_task_limit(self) -> usize {
        if self.shared_runtime {
            self.prefetch_slots
        } else {
            self.conversion_workers
        }
    }

    /// Bound unfinished conversions by both CPU demand and observed input size.
    /// Each slot allows the existing two-input conversion headroom plus a 2x
    /// batch-size margin. Later skew still passes the per-batch admission check;
    /// this estimate does not authorize unbounded growth or silently retry work.
    #[cfg(all(feature = "vortex-write", feature = "universal-format-io"))]
    pub(crate) fn admit_conversion_memory(&mut self, budget_bytes: u64, batch_bytes: u64) {
        if self.shared_runtime {
            let Some(slot_bytes) = batch_bytes.checked_mul(4) else {
                self.prefetch_slots = 0;
                return;
            };
            let slot_bytes = slot_bytes.max(1);
            let admitted = usize::try_from(budget_bytes / slot_bytes).unwrap_or(usize::MAX);
            self.prefetch_slots = self.prefetch_slots.min(admitted);
        }
    }

    fn pipeline_demand(requested: usize, source_workers: usize) -> IngestCpuDemand {
        let prefetch_slots = requested.saturating_sub(1).min(4);
        IngestCpuDemand {
            source_workers,
            conversion_workers: usize::from(prefetch_slots > 0),
            prefetch_slots,
        }
    }

    fn partition(requested: usize, source_task_capacity: usize, demand: IngestCpuDemand) -> Self {
        // Source delivery has priority so a waiting converter never consumes
        // the last lane needed by its producer. Both stages can run inline.
        let mut remaining = requested - Self::CALLER_LANES;
        let source_workers = demand
            .source_workers
            .min(source_task_capacity)
            .min(remaining);
        remaining -= source_workers;
        let conversion_workers = demand
            .conversion_workers
            .min(demand.prefetch_slots)
            .min(remaining);
        remaining -= conversion_workers;
        Self {
            requested,
            source_workers,
            conversion_workers,
            provider_drivers: remaining,
            // Zero means bypass ComputePool entirely, never construct a
            // zero-worker pool or leave a queue without a consumer.
            prefetch_slots: if conversion_workers == 0 {
                0
            } else {
                demand.prefetch_slots
            },
            shared_runtime: false,
        }
    }

    pub(crate) const fn requested(self) -> usize {
        self.requested
    }

    pub(crate) const fn source_workers(self) -> usize {
        self.source_workers
    }

    pub(crate) const fn conversion_workers(self) -> usize {
        self.conversion_workers
    }

    pub(crate) const fn provider_drivers(self) -> usize {
        self.provider_drivers
    }

    pub(crate) const fn prefetch_slots(self) -> usize {
        self.prefetch_slots
    }

    pub(crate) const fn configured_cpu_lanes(self) -> usize {
        // Construction partitions requested by subtraction, so this cannot
        // overflow, including a usize::MAX grant. No allocations happen here.
        Self::CALLER_LANES + self.source_workers + self.conversion_workers + self.provider_drivers
    }

    #[cfg(test)]
    pub(crate) const fn source_runs_on_caller(self) -> bool {
        self.source_workers == 0 && self.conversion_workers == 0
    }

    /// A no-op needs no teardown. Any real adjustment requires drained tasks,
    /// released unpublished batches and joined background owners. This validates
    /// observations; it neither joins owners nor certifies unobserved threads.
    #[cfg(test)]
    pub(crate) fn validate_replacement(
        self,
        next: Self,
        activity: IngestCpuActivity,
    ) -> Result<()> {
        if self == next {
            return Ok(());
        }
        if activity != IngestCpuActivity::default() {
            return Err(lane_error(
                "CPU plan replacement requires a drained and joined ownership boundary",
            ));
        }
        Ok(())
    }
}

#[cfg(any(test, feature = "vortex-write"))]
fn lane_error(reason: &str) -> ShardLoomError {
    ShardLoomError::InvalidOperation(format!(
        "ingest CPU lane admission: {reason}; no fallback execution was attempted"
    ))
}

#[cfg(test)]
#[path = "ingest_cpu_lanes_tests.rs"]
mod tests;
