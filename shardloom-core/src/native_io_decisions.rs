//! Bounded, typed observations of native capability decisions.
//!
//! These are completed-operation summaries, not planning promises, traces or
//! inferred savings. Unobserved capabilities and work quantities remain absent.

/// A capability whose actual owner supplies admission and completion evidence.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NativeIoCapability {
    MetadataCount,
    RegionExclusion,
    ExistingOrder,
    CorrelatedMembership,
    NativeReplay,
    RepresentationRetention,
}

impl NativeIoCapability {
    pub const ALL: [Self; 6] = [
        Self::MetadataCount,
        Self::RegionExclusion,
        Self::ExistingOrder,
        Self::CorrelatedMembership,
        Self::NativeReplay,
        Self::RepresentationRetention,
    ];

    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::MetadataCount => "metadata_count",
            Self::RegionExclusion => "region_exclusion",
            Self::ExistingOrder => "existing_order",
            Self::CorrelatedMembership => "correlated_membership",
            Self::NativeReplay => "native_replay",
            Self::RepresentationRetention => "representation_retention",
        }
    }
}

/// Why the actual owner admitted or declined a capability. A reason describes
/// the checked contract; it does not assert a new optimization or safety proof.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NativeIoDecisionReason {
    ExactHeldMetadata,
    RequiredValueEvaluation,
    SharedSourceUsage,
    SourceCapabilityUnavailable,
    ExclusionProved,
    ExclusionNotProved,
    ValidatedExistingOrder,
    UnprovenComparisonDomain,
    ObservedDisorder,
    PureEqualityCorrelation,
    RequiredParameterExecution,
    DeclaredReplay,
    AdmittedNativeSpill,
    ResidentRetention,
    OwnedRepresentation,
    RequiredRepresentationCopy,
    InsufficientHeadroom,
    ValidatedRowSelection,
    EncodedLengthScanRequired,
}

impl NativeIoDecisionReason {
    pub const ALL: [Self; 19] = [
        Self::ExactHeldMetadata,
        Self::RequiredValueEvaluation,
        Self::SharedSourceUsage,
        Self::SourceCapabilityUnavailable,
        Self::ExclusionProved,
        Self::ExclusionNotProved,
        Self::ValidatedExistingOrder,
        Self::UnprovenComparisonDomain,
        Self::ObservedDisorder,
        Self::PureEqualityCorrelation,
        Self::RequiredParameterExecution,
        Self::DeclaredReplay,
        Self::AdmittedNativeSpill,
        Self::ResidentRetention,
        Self::OwnedRepresentation,
        Self::RequiredRepresentationCopy,
        Self::InsufficientHeadroom,
        Self::ValidatedRowSelection,
        Self::EncodedLengthScanRequired,
    ];

    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::ExactHeldMetadata => "exact_held_metadata",
            Self::RequiredValueEvaluation => "required_value_evaluation",
            Self::SharedSourceUsage => "shared_source_usage",
            Self::SourceCapabilityUnavailable => "source_capability_unavailable",
            Self::ExclusionProved => "exclusion_proved",
            Self::ExclusionNotProved => "exclusion_not_proved",
            Self::ValidatedExistingOrder => "validated_existing_order",
            Self::UnprovenComparisonDomain => "unproven_comparison_domain",
            Self::ObservedDisorder => "observed_disorder",
            Self::PureEqualityCorrelation => "pure_equality_correlation",
            Self::RequiredParameterExecution => "required_parameter_execution",
            Self::DeclaredReplay => "declared_replay",
            Self::AdmittedNativeSpill => "admitted_native_spill",
            Self::ResidentRetention => "resident_retention",
            Self::OwnedRepresentation => "owned_representation",
            Self::RequiredRepresentationCopy => "required_representation_copy",
            Self::InsufficientHeadroom => "insufficient_headroom",
            Self::ValidatedRowSelection => "validated_row_selection",
            Self::EncodedLengthScanRequired => "encoded_length_scan_required",
        }
    }
}

/// Final disposition at one instrumented owner boundary. `Executed` also counts
/// as considered and admitted. `Admitted` alone does not establish execution.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NativeIoDecisionOutcome {
    Admitted,
    Declined,
    Executed,
}

/// Optional measurements from the owner, without a fabricated zero for unknown
/// work. Units describe that capability's boundary, not the whole query.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct NativeIoObservedWork {
    pub input_rows: Option<u64>,
    pub output_rows: Option<u64>,
    pub regions: Option<u64>,
    pub excluded_regions: Option<u64>,
    pub copied_buffer_bytes: Option<u64>,
    pub transferred_buffer_bytes: Option<u64>,
    pub spill_runs: Option<u64>,
}

impl NativeIoObservedWork {
    fn merge(&mut self, other: Self) -> crate::Result<()> {
        fn add(value: &mut Option<u64>, incoming: Option<u64>) -> crate::Result<()> {
            if let Some(incoming) = incoming {
                *value = Some(
                    value
                        .unwrap_or(0)
                        .checked_add(incoming)
                        .ok_or_else(overflow)?,
                );
            }
            Ok(())
        }
        add(&mut self.input_rows, other.input_rows)?;
        add(&mut self.output_rows, other.output_rows)?;
        add(&mut self.regions, other.regions)?;
        add(&mut self.excluded_regions, other.excluded_regions)?;
        add(&mut self.copied_buffer_bytes, other.copied_buffer_bytes)?;
        add(
            &mut self.transferred_buffer_bytes,
            other.transferred_buffer_bytes,
        )?;
        add(&mut self.spill_runs, other.spill_runs)
    }
}

/// Fixed-size aggregation for one capability. Reasons are bounded sets; their
/// presence establishes an observed reason, not a frequency for that reason.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct NativeIoDecisionSummary {
    pub considered: u64,
    pub admitted: u64,
    pub declined: u64,
    pub executed: u64,
    admission_reasons: u32,
    decline_reasons: u32,
    pub observed: NativeIoObservedWork,
}

impl NativeIoDecisionSummary {
    #[must_use]
    pub const fn has_admission_reason(self, reason: NativeIoDecisionReason) -> bool {
        self.admission_reasons & (1 << reason as u32) != 0
    }

    #[must_use]
    pub const fn has_decline_reason(self, reason: NativeIoDecisionReason) -> bool {
        self.decline_reasons & (1 << reason as u32) != 0
    }
}

/// Six fixed summaries. Storage cannot grow with input or operator count.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct NativeIoCapabilityDecisions([NativeIoDecisionSummary; 6]);

impl NativeIoCapabilityDecisions {
    /// Construct one boundary outcome. A fresh summary cannot overflow.
    #[must_use]
    pub fn single(
        capability: NativeIoCapability,
        outcome: NativeIoDecisionOutcome,
        reason: NativeIoDecisionReason,
        observed: NativeIoObservedWork,
    ) -> Self {
        let declined = outcome == NativeIoDecisionOutcome::Declined;
        let mut decisions = Self::default();
        decisions.0[capability as usize] = NativeIoDecisionSummary {
            considered: 1,
            admitted: u64::from(!declined),
            declined: u64::from(declined),
            executed: u64::from(outcome == NativeIoDecisionOutcome::Executed),
            admission_reasons: if declined { 0 } else { 1 << reason as u32 },
            decline_reasons: if declined { 1 << reason as u32 } else { 0 },
            observed,
        };
        decisions
    }

    /// Record one completed boundary outcome, atomically with its observations.
    /// # Errors
    /// Rejects counter overflow without changing any existing evidence.
    pub fn record(
        &mut self,
        capability: NativeIoCapability,
        outcome: NativeIoDecisionOutcome,
        reason: NativeIoDecisionReason,
        observed: NativeIoObservedWork,
    ) -> crate::Result<()> {
        self.record_many(capability, 1, outcome, reason, observed)
    }

    /// Record a positive number of identical boundary outcomes without a trace
    /// or a loop. `observed` is their aggregate work, not work per outcome.
    /// # Errors
    /// Rejects zero outcomes or overflow without changing existing evidence.
    pub fn record_many(
        &mut self,
        capability: NativeIoCapability,
        count: u64,
        outcome: NativeIoDecisionOutcome,
        reason: NativeIoDecisionReason,
        observed: NativeIoObservedWork,
    ) -> crate::Result<()> {
        if count == 0 {
            return Err(crate::ShardLoomError::InvalidOperation(
                "native I/O decision evidence requires a positive outcome count".into(),
            ));
        }
        let mut row = self.0[capability as usize];
        row.considered = row.considered.checked_add(count).ok_or_else(overflow)?;
        if outcome == NativeIoDecisionOutcome::Declined {
            row.declined = row.declined.checked_add(count).ok_or_else(overflow)?;
            row.decline_reasons |= 1 << reason as u32;
        } else {
            row.admitted = row.admitted.checked_add(count).ok_or_else(overflow)?;
            row.admission_reasons |= 1 << reason as u32;
            if outcome == NativeIoDecisionOutcome::Executed {
                row.executed = row.executed.checked_add(count).ok_or_else(overflow)?;
            }
        }
        row.observed.merge(observed)?;
        self.0[capability as usize] = row;
        Ok(())
    }

    /// Combine disjoint completed owners without parsing report text.
    /// # Errors
    /// Rejects overflow atomically across every capability and work quantity.
    pub fn merge(&mut self, other: &Self) -> crate::Result<()> {
        let mut combined = *self;
        for (row, incoming) in combined.0.iter_mut().zip(other.0) {
            row.considered = row
                .considered
                .checked_add(incoming.considered)
                .ok_or_else(overflow)?;
            row.admitted = row
                .admitted
                .checked_add(incoming.admitted)
                .ok_or_else(overflow)?;
            row.declined = row
                .declined
                .checked_add(incoming.declined)
                .ok_or_else(overflow)?;
            row.executed = row
                .executed
                .checked_add(incoming.executed)
                .ok_or_else(overflow)?;
            row.admission_reasons |= incoming.admission_reasons;
            row.decline_reasons |= incoming.decline_reasons;
            row.observed.merge(incoming.observed)?;
        }
        *self = combined;
        Ok(())
    }

    /// Returns no row when no instrumented boundary considered this capability.
    #[must_use]
    pub fn get(&self, capability: NativeIoCapability) -> Option<&NativeIoDecisionSummary> {
        let row = &self.0[capability as usize];
        (row.considered != 0).then_some(row)
    }
}

fn overflow() -> crate::ShardLoomError {
    crate::ShardLoomError::InvalidOperation(
        "native I/O decision evidence counter overflow; no fallback execution was attempted".into(),
    )
}

#[cfg(test)]
#[path = "native_io_decisions_tests.rs"]
mod tests;
