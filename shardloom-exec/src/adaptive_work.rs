//! Run-local work sizing against the existing shared reservation owner.
//!
//! Selection is allocation-free advice. The caller must apply it at a legal
//! continuation boundary, reserve actual storage before allocation, and call
//! `complete` only after that work finishes. Neither a shape nor its estimated
//! bytes grants memory or establishes a provider-allocator/process RSS bound.

use crate::live_memory::LiveMemorySnapshot;
use shardloom_core::{Result, ShardLoomError};

/// Additional simultaneous storage for one lane, excluding already held owners.
/// Include replacement/input/output overlap in the supplied unit cost. Schema,
/// encoded representation and observed value sizes belong to the owning adapter.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WorkCost {
    pub fixed_bytes: u64,
    pub bytes_per_unit: u64,
}

/// Geometry that the owner can actually execute without changing semantics.
/// Limits are internal/provider or caller ceilings, never new resource grants.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WorkLimits {
    pub min_units: u64,
    pub max_units: u64,
    pub max_in_flight: usize,
}

/// One selection; an immutable preview does not advance feedback or evidence.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WorkShape {
    units: u64,
    in_flight: usize,
    estimated_bytes_per_lane: u64,
    available_units: u64,
    headroom_bytes: u64,
    ceiling_bytes: u64,
    previous_units: Option<u64>,
    max_units: u64,
}

impl WorkShape {
    #[must_use]
    pub const fn units(self) -> u64 {
        self.units
    }

    #[must_use]
    pub const fn in_flight(self) -> usize {
        self.in_flight
    }

    #[must_use]
    pub const fn estimated_bytes_per_lane(self) -> u64 {
        self.estimated_bytes_per_lane
    }

    #[must_use]
    pub const fn headroom_bytes(self) -> u64 {
        self.headroom_bytes
    }
}

/// A minimum-workspace refusal is data, so an optional native optimization can
/// decline before allocating without parsing an error or retrying execution.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WorkDenial {
    pub required_additional_bytes: u64,
    pub headroom_bytes: u64,
    pub reserved_bytes: u64,
    pub ceiling_bytes: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WorkDecision {
    Selected(WorkShape),
    Declined(WorkDenial),
}

impl WorkDecision {
    /// Require a selected geometry after an owner has handled any permitted
    /// flush or optional decline. Conversion does not reserve or complete work.
    ///
    /// # Errors
    /// Reports the shared memory-preflight diagnostic for a declined minimum.
    pub fn require_selected(self, owner: &str) -> Result<WorkShape> {
        require_selected(owner, self)
    }
}

/// Bounded evidence of completed work. Zero windows means no applied decision.
/// Estimates are deliberately separate from `LiveMemoryPool` reservation peaks.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct AdaptiveWorkReport {
    pub completed_windows: u64,
    pub declined_windows: u64,
    pub maximum_declined_required_bytes: u64,
    pub headroom_limited_windows: u64,
    pub shrunk_windows: u64,
    pub grown_windows: u64,
    pub minimum_units: u64,
    pub maximum_units: u64,
    pub maximum_in_flight: usize,
    pub minimum_headroom_bytes: u64,
    pub ceiling_bytes: u64,
    pub peak_estimated_bytes_per_lane: u64,
}

impl AdaptiveWorkReport {
    /// Aggregate only work actually executed by the owner. This retains no
    /// unbounded trace and distinguishes pressure from a short final extent.
    pub fn record(&mut self, shape: WorkShape) {
        let shrunk = shape.units < shape.max_units
            && shape
                .previous_units
                .is_some_and(|units| shape.units < units);
        let grown = shape
            .previous_units
            .is_some_and(|units| shape.units > units);
        self.minimum_units = if self.completed_windows == 0 {
            shape.units
        } else {
            self.minimum_units.min(shape.units)
        };
        self.minimum_headroom_bytes = if self.completed_windows == 0 && self.declined_windows == 0 {
            shape.headroom_bytes
        } else {
            self.minimum_headroom_bytes.min(shape.headroom_bytes)
        };
        self.completed_windows = self.completed_windows.saturating_add(1);
        self.headroom_limited_windows = self
            .headroom_limited_windows
            .saturating_add(u64::from(shape.available_units < shape.max_units));
        self.shrunk_windows = self.shrunk_windows.saturating_add(u64::from(shrunk));
        self.grown_windows = self.grown_windows.saturating_add(u64::from(grown));
        self.maximum_units = self.maximum_units.max(shape.units);
        self.maximum_in_flight = self.maximum_in_flight.max(shape.in_flight);
        self.ceiling_bytes = shape.ceiling_bytes;
        self.peak_estimated_bytes_per_lane = self
            .peak_estimated_bytes_per_lane
            .max(shape.estimated_bytes_per_lane);
    }

    /// Record an executed decision to skip optional work. This does not count
    /// as a completed window, change feedback or establish a reservation.
    pub fn record_decline(&mut self, denial: WorkDenial) {
        self.minimum_headroom_bytes = if self.completed_windows == 0 && self.declined_windows == 0 {
            denial.headroom_bytes
        } else {
            self.minimum_headroom_bytes.min(denial.headroom_bytes)
        };
        self.declined_windows = self.declined_windows.saturating_add(1);
        self.maximum_declined_required_bytes = self
            .maximum_declined_required_bytes
            .max(denial.required_additional_bytes);
        self.ceiling_bytes = denial.ceiling_bytes;
    }
}

/// One owner's feedback state. Keep it with the operation, never globally or
/// across unrelated operations. No input is replayed or retried by this policy.
#[derive(Debug, Default)]
pub struct AdaptiveWorkSizer {
    previous_units: Option<u64>,
    target_units: Option<u64>,
    growth_windows: u8,
    observed_bytes_per_unit: u64,
    report: AdaptiveWorkReport,
}

impl AdaptiveWorkSizer {
    /// Select positive work from current accounted headroom. Additional workers
    /// fit only after one useful unit fits; unrelated held owners stay charged.
    ///
    /// # Errors
    /// Rejects invalid geometry, overflow, and an indivisible minimum that does
    /// not fit. An actual reservation can still reject after a concurrent change.
    pub fn select(
        &self,
        owner: &str,
        memory: LiveMemorySnapshot,
        cost: WorkCost,
        limits: WorkLimits,
    ) -> Result<WorkShape> {
        require_selected(owner, self.decide(owner, memory, cost, limits)?)
    }

    /// Select among a finite, strictly increasing set of legal geometries.
    /// Each candidate supplies its own additional workspace cost. This handles
    /// physical framing and costs such as run metadata that grow when blocks
    /// shrink, without teaching the shared policy about a storage format.
    ///
    /// The iterator must be allocation/effect free and bounded by the owner's
    /// admitted geometry. Feedback still limits the preferred target. If smaller
    /// candidates cannot fit their metadata, the smallest feasible candidate is
    /// the physical minimum, just as `min_units` constrains linear selection.
    ///
    /// # Errors
    /// Rejects invalid/unordered candidates, overflow and a set in which no
    /// candidate fits current accounted headroom. This does not reserve memory.
    pub fn select_candidates(
        &self,
        owner: &str,
        memory: LiveMemorySnapshot,
        limits: WorkLimits,
        candidates: impl IntoIterator<Item = Result<(u64, WorkCost)>>,
    ) -> Result<WorkShape> {
        require_selected(
            owner,
            self.decide_candidates(owner, memory, limits, candidates)?,
        )
    }

    /// Typed preview of [`Self::select_candidates`], with no progress credit.
    ///
    /// # Errors
    /// Rejects invalid/unordered candidates and checked capacity overflow.
    pub fn decide_candidates(
        &self,
        owner: &str,
        memory: LiveMemorySnapshot,
        limits: WorkLimits,
        candidates: impl IntoIterator<Item = Result<(u64, WorkCost)>>,
    ) -> Result<WorkDecision> {
        if limits.min_units == 0 || limits.max_units < limits.min_units || limits.max_in_flight == 0
        {
            return Err(failed(owner, "invalid work geometry"));
        }
        let headroom = memory.limit_bytes.saturating_sub(memory.reserved_bytes);
        let target = self
            .target_units
            .unwrap_or(limits.max_units)
            .clamp(limits.min_units, limits.max_units);
        let mut previous = 0;
        let mut minimum_required = u64::MAX;
        let mut first = None;
        let mut selected = None;
        let mut available_units = 0;
        for candidate in candidates {
            let (units, cost) = candidate?;
            if units < limits.min_units
                || units > limits.max_units
                || units <= previous
                || cost.bytes_per_unit == 0
            {
                return Err(failed(owner, "invalid or unordered work candidate"));
            }
            previous = units;
            let bytes = units
                .checked_mul(cost.bytes_per_unit.max(self.observed_bytes_per_unit))
                .and_then(|bytes| bytes.checked_add(cost.fixed_bytes))
                .ok_or_else(|| failed(owner, "candidate workspace size overflow"))?;
            minimum_required = minimum_required.min(bytes);
            if bytes <= headroom {
                available_units = units;
                first.get_or_insert((units, bytes));
                if units <= target {
                    selected = Some((units, bytes));
                }
            }
        }
        if previous == 0 {
            return Err(failed(owner, "work candidates are empty"));
        }
        let Some((units, estimated_bytes_per_lane)) = selected.or(first) else {
            return Ok(WorkDecision::Declined(WorkDenial {
                required_additional_bytes: minimum_required,
                headroom_bytes: headroom,
                reserved_bytes: memory.reserved_bytes,
                ceiling_bytes: memory.limit_bytes,
            }));
        };
        Ok(WorkDecision::Selected(WorkShape {
            units,
            in_flight: usize::try_from(headroom / estimated_bytes_per_lane)
                .unwrap_or(usize::MAX)
                .min(limits.max_in_flight),
            estimated_bytes_per_lane,
            available_units,
            headroom_bytes: headroom,
            ceiling_bytes: memory.limit_bytes,
            previous_units: self.previous_units,
            max_units: limits.max_units,
        }))
    }

    /// Preview the same selection with a typed headroom refusal. The owner may
    /// skip optional work only when its existing execution contract allows it.
    /// Inspecting this decision does not advance feedback or execution evidence.
    ///
    /// # Errors
    /// Rejects invalid geometry and arithmetic overflow.
    pub fn decide(
        &self,
        owner: &str,
        memory: LiveMemorySnapshot,
        cost: WorkCost,
        limits: WorkLimits,
    ) -> Result<WorkDecision> {
        if limits.min_units == 0
            || limits.max_units < limits.min_units
            || limits.max_in_flight == 0
            || cost.bytes_per_unit == 0
        {
            return Err(failed(owner, "invalid work geometry"));
        }
        let unit_bytes = cost.bytes_per_unit.max(self.observed_bytes_per_unit);
        let minimum = limits
            .min_units
            .checked_mul(unit_bytes)
            .and_then(|bytes| bytes.checked_add(cost.fixed_bytes))
            .ok_or_else(|| failed(owner, "minimum workspace size overflow"))?;
        let headroom = memory.limit_bytes.saturating_sub(memory.reserved_bytes);
        if minimum > headroom {
            return Ok(WorkDecision::Declined(WorkDenial {
                required_additional_bytes: minimum,
                headroom_bytes: headroom,
                reserved_bytes: memory.reserved_bytes,
                ceiling_bytes: memory.limit_bytes,
            }));
        }
        let available_units = ((headroom - cost.fixed_bytes) / unit_bytes).min(limits.max_units);
        let units = self
            .target_units
            .unwrap_or(limits.max_units)
            .clamp(limits.min_units, limits.max_units)
            .min(available_units);
        // Division above proves this multiplication and addition fit headroom.
        let estimated_bytes_per_lane = cost.fixed_bytes + units * unit_bytes;
        let in_flight = usize::try_from(headroom / estimated_bytes_per_lane)
            .unwrap_or(usize::MAX)
            .min(limits.max_in_flight);
        Ok(WorkDecision::Selected(WorkShape {
            units,
            in_flight,
            estimated_bytes_per_lane,
            available_units,
            headroom_bytes: headroom,
            ceiling_bytes: memory.limit_bytes,
            previous_units: self.previous_units,
            max_units: limits.max_units,
        }))
    }

    /// Record one actually completed window and its observed additional unit
    /// footprint, when available. Include the same overlap as `WorkCost`; do not
    /// supply process RSS, bytes read, or unrelated owners as an allocation size.
    /// A high-water estimate prevents alternating wide/narrow batches from
    /// repeatedly selecting an already observed oversized unit.
    pub fn complete(&mut self, shape: WorkShape, observed_bytes_per_unit: Option<u64>) {
        self.observed_bytes_per_unit = self
            .observed_bytes_per_unit
            .max(observed_bytes_per_unit.unwrap_or(0));
        // A short final extent must not poison the next file/fragment's target.
        // The next selection still rechecks actual shared headroom.
        let extent_limited = shape.units == shape.max_units
            && shape
                .previous_units
                .is_some_and(|units| units > shape.max_units);
        let shrunk = !extent_limited
            && shape
                .previous_units
                .is_some_and(|units| shape.units < units);
        if !shrunk && shape.available_units > shape.units {
            self.growth_windows = self.growth_windows.saturating_add(1);
        } else {
            self.growth_windows = 0;
        }
        let completed_units = if extent_limited {
            shape.previous_units.unwrap_or(shape.units)
        } else {
            shape.units
        };
        self.target_units = Some(if self.growth_windows >= 2 {
            self.growth_windows = 0;
            grow_target(shape.units, shape.available_units.min(shape.max_units))
        } else {
            completed_units
        });
        self.previous_units = Some(completed_units);
        self.report.record(shape);
    }

    #[must_use]
    pub const fn report(&self) -> AdaptiveWorkReport {
        self.report
    }
}

fn require_selected(owner: &str, decision: WorkDecision) -> Result<WorkShape> {
    match decision {
        WorkDecision::Selected(shape) => Ok(shape),
        WorkDecision::Declined(denial) => Err(failed(
            owner,
            &format!(
                "memory reservation denied at adaptive preflight: minimum workspace does not fit: required_additional_bytes={}, accounted_headroom_bytes={}, reserved_bytes={}, ceiling_bytes={}; release retained owners or use an admitted spill/smaller source workspace",
                denial.required_additional_bytes,
                denial.headroom_bytes,
                denial.reserved_bytes,
                denial.ceiling_bytes,
            ),
        )),
    }
}

// Shared with advisory sizing and scoped EndoPulse. These helpers perform no
// execution and preserve each caller's existing minimum/maximum interpretation.
pub(crate) const fn shrink_target(current: u64, minimum: u64) -> u64 {
    let half = current / 2;
    if half < minimum { minimum } else { half }
}

pub(crate) const fn grow_target(current: u64, maximum: u64) -> u64 {
    let doubled = current.saturating_mul(2);
    if doubled > maximum { maximum } else { doubled }
}

fn failed(owner: &str, reason: &str) -> ShardLoomError {
    ShardLoomError::InvalidOperation(format!(
        "native adaptive work owner={owner}: {reason}; no fallback execution was attempted"
    ))
}

#[cfg(test)]
#[path = "adaptive_work_tests.rs"]
mod tests;
