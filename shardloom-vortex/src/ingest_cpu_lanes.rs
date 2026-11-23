//! One ingest CPU grant across the admitted source, conversion and writer owners.
//!
//! This module creates no workers. Source admission and the streaming writer
//! construct their owned workers from the same policy. Blocking I/O and source
//! library internal threads remain separately scoped.

#[cfg(any(test, feature = "vortex-write"))]
use shardloom_core::{Result, ShardLoomError};
#[cfg_attr(not(feature = "vortex-write"), allow(unused_imports))]
use shardloom_exec::live_memory::LiveMemoryPool;

/// Measured policy inputs, not independent grants. Unused lanes go to the native
/// writer runtime, which also retains the caller as its mandatory progress lane.
#[cfg_attr(not(feature = "universal-format-io"), allow(dead_code))]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct IngestCpuDemand {
    pub(crate) source_workers: usize,
    pub(crate) conversion_workers: usize,
    pub(crate) prefetch_slots: usize,
}

#[cfg_attr(not(feature = "universal-format-io"), allow(dead_code))]
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
#[cfg_attr(
    not(all(feature = "vortex-write", feature = "universal-format-io")),
    allow(dead_code)
)]
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

    /// Balanced pipeline lane allocation. Source workers scale with both the
    /// CPU grant and `source_task_capacity` while preserving caller/conversion
    /// progress lanes and remaining provider drivers.
    #[cfg(any(test, not(feature = "vortex-write")))]
    pub(crate) fn pipeline(requested: usize, source_task_capacity: usize) -> Self {
        let requested = requested.max(1);
        let source_workers = if source_task_capacity == 0 || requested <= 1 {
            0
        } else if requested == 2 {
            1
        } else {
            (requested / 2)
                .min(requested.saturating_sub(2).max(1))
                .min(source_task_capacity)
        };
        Self::partition(
            requested,
            source_task_capacity,
            Self::pipeline_demand(requested, source_workers),
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
    /// When shared Arrow backing capacity suppresses all prefetch, exact sizing
    /// of admitted native copies can recover one lookahead slot. Already-positive
    /// windows stay unchanged; original source owners remain outside that scope.
    #[cfg(all(feature = "vortex-write", feature = "universal-format-io"))]
    pub(crate) fn admit_conversion_memory(
        &mut self,
        budget_bytes: u64,
        batch_bytes: u64,
        owned_copy_bytes: Option<u64>,
    ) {
        if self.shared_runtime {
            let admitted = batch_bytes.checked_mul(4).map_or(0, |slot_bytes| {
                usize::try_from(budget_bytes / slot_bytes.max(1)).unwrap_or(usize::MAX)
            });
            let owned_lookahead = usize::from(
                owned_copy_bytes
                    .and_then(|bytes| bytes.checked_mul(4))
                    .is_some_and(|bytes| bytes.max(1) <= budget_bytes),
            );
            self.prefetch_slots = self.prefetch_slots.min(admitted.max(owned_lookahead));
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
#[cfg_attr(not(feature = "universal-format-io"), allow(dead_code))]
fn lane_error(reason: &str) -> ShardLoomError {
    ShardLoomError::InvalidOperation(format!(
        "ingest CPU lane admission: {reason}; no fallback execution was attempted"
    ))
}

/// Govern per-column writer compression and stats concurrency against the shared
/// `LiveMemoryPool` headroom so high memory pressure throttles down safely.
#[cfg_attr(not(feature = "vortex-write"), allow(dead_code))]
pub(crate) fn governed_writer_concurrency(
    requested: usize,
    memory: Option<&LiveMemoryPool>,
) -> usize {
    let requested = requested.max(1);
    let Some(pool) = memory else {
        return requested;
    };
    let snapshot = pool.snapshot();
    if snapshot.limit_bytes == 0 {
        return 1;
    }
    let headroom = snapshot.limit_bytes.saturating_sub(snapshot.reserved_bytes);
    if headroom < snapshot.limit_bytes / 4 {
        1
    } else if headroom < snapshot.limit_bytes / 2 {
        (requested / 2).max(1)
    } else {
        requested
    }
}

/// Govern concurrent child `write_stream` chunks in [`BoundedIngestLayout`] under
/// the same [`LiveMemoryPool`] headroom controller (capped at 4 in-flight chunks).
#[cfg_attr(
    not(all(feature = "vortex-write", feature = "universal-format-io")),
    allow(dead_code)
)]
pub(crate) fn governed_layout_in_flight(
    requested: usize,
    memory: Option<&LiveMemoryPool>,
) -> usize {
    governed_writer_concurrency(requested, memory).min(4)
}

#[cfg(test)]
#[path = "ingest_cpu_lanes_tests.rs"]
mod tests;
