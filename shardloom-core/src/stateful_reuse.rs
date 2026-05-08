//! CG-17 stateful reuse and incremental execution planning contracts.
//!
//! This module defines typed cache/reuse boundaries and invalidation proof
//! requirements only. It does not read caches, write caches, replay cached
//! results, or execute incremental recomputation.

use crate::{Diagnostic, DiagnosticSeverity};

/// Report-level status for stateful reuse planning.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StatefulReuseStatus {
    ReportOnlyPlanned,
    EvidenceIncomplete,
    ReuseCertified,
    Blocked,
}

impl StatefulReuseStatus {
    /// Stable machine-readable status label.
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::ReportOnlyPlanned => "report_only_planned",
            Self::EvidenceIncomplete => "evidence_incomplete",
            Self::ReuseCertified => "reuse_certified",
            Self::Blocked => "blocked",
        }
    }

    /// Returns whether this status should fail a report command.
    #[must_use]
    pub const fn is_error(&self) -> bool {
        matches!(self, Self::Blocked)
    }
}

/// Typed cache families CG-17 must keep separate.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReuseCacheKind {
    SegmentResult,
    PredicateResult,
    EncodedDictionary,
    EncodedFilter,
    LayoutDecision,
    ExecutionCertificate,
    IncrementalManifestDiff,
}

impl ReuseCacheKind {
    /// Stable machine-readable cache kind label.
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::SegmentResult => "segment_result",
            Self::PredicateResult => "predicate_result",
            Self::EncodedDictionary => "encoded_dictionary",
            Self::EncodedFilter => "encoded_filter",
            Self::LayoutDecision => "layout_decision",
            Self::ExecutionCertificate => "execution_certificate",
            Self::IncrementalManifestDiff => "incremental_manifest_diff",
        }
    }
}

/// Reuse eligibility status for a typed cache boundary.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReuseBoundaryStatus {
    Planned,
    RequiresInvalidationProof,
    RequiresCorrectnessProof,
    Reusable,
    Invalidated,
    Unsupported,
}

impl ReuseBoundaryStatus {
    /// Stable machine-readable status label.
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::Planned => "planned",
            Self::RequiresInvalidationProof => "requires_invalidation_proof",
            Self::RequiresCorrectnessProof => "requires_correctness_proof",
            Self::Reusable => "reusable",
            Self::Invalidated => "invalidated",
            Self::Unsupported => "unsupported",
        }
    }

    /// Returns whether this boundary status is an error.
    #[must_use]
    pub const fn is_error(&self) -> bool {
        matches!(self, Self::Unsupported)
    }
}

/// Typed stateful reuse boundary for one cache family.
#[derive(Debug, Clone, PartialEq, Eq)]
#[allow(clippy::struct_excessive_bools)]
pub struct StatefulReuseBoundary {
    pub boundary_id: String,
    pub cache_kind: ReuseCacheKind,
    pub status: ReuseBoundaryStatus,
    pub deterministic_key_required: bool,
    pub dataset_snapshot_scoped: bool,
    pub plan_hash_scoped: bool,
    pub semantic_profile_scoped: bool,
    pub encoding_layout_scoped: bool,
    pub adapter_fidelity_scoped: bool,
    pub correctness_proof_required: bool,
    pub invalidation_proof_required: bool,
    pub execution_certificate_required: bool,
    pub cross_dataset_reuse_allowed: bool,
    pub fallback_attempted: bool,
}

impl StatefulReuseBoundary {
    /// Creates a planned typed reuse boundary.
    #[must_use]
    pub fn planned(boundary_id: impl Into<String>, cache_kind: ReuseCacheKind) -> Self {
        Self {
            boundary_id: boundary_id.into(),
            cache_kind,
            status: ReuseBoundaryStatus::Planned,
            deterministic_key_required: true,
            dataset_snapshot_scoped: true,
            plan_hash_scoped: true,
            semantic_profile_scoped: true,
            encoding_layout_scoped: true,
            adapter_fidelity_scoped: true,
            correctness_proof_required: true,
            invalidation_proof_required: true,
            execution_certificate_required: true,
            cross_dataset_reuse_allowed: false,
            fallback_attempted: false,
        }
    }

    /// Returns whether this boundary has an error state.
    #[must_use]
    pub const fn has_errors(&self) -> bool {
        self.status.is_error() || self.cross_dataset_reuse_allowed || self.fallback_attempted
    }
}

/// Invalidation signals that must be conservatively handled before reuse.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InvalidationSignalKind {
    SnapshotChanged,
    SegmentAdded,
    SegmentRemoved,
    SegmentReplaced,
    SchemaChanged,
    PartitionChanged,
    PredicateChanged,
    SemanticProfileChanged,
    FunctionVersionChanged,
    AdapterFidelityChanged,
    UnknownChange,
}

impl InvalidationSignalKind {
    /// Stable machine-readable signal label.
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::SnapshotChanged => "snapshot_changed",
            Self::SegmentAdded => "segment_added",
            Self::SegmentRemoved => "segment_removed",
            Self::SegmentReplaced => "segment_replaced",
            Self::SchemaChanged => "schema_changed",
            Self::PartitionChanged => "partition_changed",
            Self::PredicateChanged => "predicate_changed",
            Self::SemanticProfileChanged => "semantic_profile_changed",
            Self::FunctionVersionChanged => "function_version_changed",
            Self::AdapterFidelityChanged => "adapter_fidelity_changed",
            Self::UnknownChange => "unknown_change",
        }
    }
}

/// Required proof for one invalidation signal.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InvalidationProofRequirement {
    pub signal_kind: InvalidationSignalKind,
    pub proof_required: bool,
    pub conservative_action: String,
    pub fallback_attempted: bool,
}

impl InvalidationProofRequirement {
    /// Creates a proof requirement with a conservative action.
    #[must_use]
    pub fn required(
        signal_kind: InvalidationSignalKind,
        conservative_action: impl Into<String>,
    ) -> Self {
        Self {
            signal_kind,
            proof_required: true,
            conservative_action: conservative_action.into(),
            fallback_attempted: false,
        }
    }
}

/// Report-only CG-17 stateful reuse plan.
#[derive(Debug, Clone, PartialEq, Eq)]
#[allow(clippy::struct_excessive_bools)]
pub struct StatefulReuseReport {
    pub schema_version: &'static str,
    pub report_id: String,
    pub status: StatefulReuseStatus,
    pub boundaries: Vec<StatefulReuseBoundary>,
    pub invalidation_requirements: Vec<InvalidationProofRequirement>,
    pub typed_cache_boundaries_required: bool,
    pub deterministic_keys_required: bool,
    pub invalidation_proofs_required: bool,
    pub correctness_proofs_required: bool,
    pub execution_certificates_required: bool,
    pub manifest_diff_required: bool,
    pub cache_read: bool,
    pub cache_write: bool,
    pub cache_replay: bool,
    pub incremental_execution: bool,
    pub runtime_execution: bool,
    pub data_read: bool,
    pub data_decoded: bool,
    pub data_materialized: bool,
    pub row_read: bool,
    pub arrow_converted: bool,
    pub object_store_io: bool,
    pub write_io: bool,
    pub spill_io_performed: bool,
    pub external_engine_execution: bool,
    pub fallback_execution_allowed: bool,
    pub fallback_attempted: bool,
    pub production_claim_allowed: bool,
    pub diagnostics: Vec<Diagnostic>,
}

impl StatefulReuseReport {
    /// Creates the CG-17 report-only foundation.
    #[must_use]
    pub fn cg17_foundation() -> Self {
        Self {
            schema_version: "shardloom.stateful_reuse.v1",
            report_id: "cg17.stateful-reuse".to_string(),
            status: StatefulReuseStatus::ReportOnlyPlanned,
            boundaries: cg17_reuse_boundaries(),
            invalidation_requirements: cg17_invalidation_requirements(),
            typed_cache_boundaries_required: true,
            deterministic_keys_required: true,
            invalidation_proofs_required: true,
            correctness_proofs_required: true,
            execution_certificates_required: true,
            manifest_diff_required: true,
            cache_read: false,
            cache_write: false,
            cache_replay: false,
            incremental_execution: false,
            runtime_execution: false,
            data_read: false,
            data_decoded: false,
            data_materialized: false,
            row_read: false,
            arrow_converted: false,
            object_store_io: false,
            write_io: false,
            spill_io_performed: false,
            external_engine_execution: false,
            fallback_execution_allowed: false,
            fallback_attempted: false,
            production_claim_allowed: false,
            diagnostics: Vec::new(),
        }
    }

    /// Number of typed cache boundaries.
    #[must_use]
    pub fn boundary_count(&self) -> usize {
        self.boundaries.len()
    }

    /// Number of invalidation proof requirements.
    #[must_use]
    pub fn invalidation_requirement_count(&self) -> usize {
        self.invalidation_requirements.len()
    }

    /// Number of boundaries requiring correctness proof.
    #[must_use]
    pub fn correctness_proof_required_count(&self) -> usize {
        self.boundaries
            .iter()
            .filter(|boundary| boundary.correctness_proof_required)
            .count()
    }

    /// Number of boundaries requiring invalidation proof.
    #[must_use]
    pub fn invalidation_proof_required_count(&self) -> usize {
        self.boundaries
            .iter()
            .filter(|boundary| boundary.invalidation_proof_required)
            .count()
    }

    /// Number of boundaries requiring execution certificate linkage.
    #[must_use]
    pub fn execution_certificate_required_count(&self) -> usize {
        self.boundaries
            .iter()
            .filter(|boundary| boundary.execution_certificate_required)
            .count()
    }

    /// Stable comma-separated cache-kind order for CLI reporting.
    #[must_use]
    pub fn cache_kind_order(&self) -> String {
        self.boundaries
            .iter()
            .map(|boundary| boundary.cache_kind.as_str())
            .collect::<Vec<_>>()
            .join(",")
    }

    /// Stable comma-separated invalidation-signal order for CLI reporting.
    #[must_use]
    pub fn invalidation_signal_order(&self) -> String {
        self.invalidation_requirements
            .iter()
            .map(|requirement| requirement.signal_kind.as_str())
            .collect::<Vec<_>>()
            .join(",")
    }

    /// Returns whether the report avoids all execution and IO side effects.
    #[must_use]
    pub const fn is_side_effect_free(&self) -> bool {
        !self.cache_read
            && !self.cache_write
            && !self.cache_replay
            && !self.incremental_execution
            && !self.runtime_execution
            && !self.data_read
            && !self.data_decoded
            && !self.data_materialized
            && !self.row_read
            && !self.arrow_converted
            && !self.object_store_io
            && !self.write_io
            && !self.spill_io_performed
            && !self.external_engine_execution
            && !self.fallback_execution_allowed
            && !self.fallback_attempted
    }

    /// Returns whether the report contains errors.
    #[must_use]
    pub fn has_errors(&self) -> bool {
        self.status.is_error()
            || !self.is_side_effect_free()
            || self.production_claim_allowed
            || self
                .boundaries
                .iter()
                .any(StatefulReuseBoundary::has_errors)
            || self
                .invalidation_requirements
                .iter()
                .any(|requirement| requirement.fallback_attempted)
            || self.diagnostics.iter().any(|diagnostic| {
                matches!(
                    diagnostic.severity,
                    DiagnosticSeverity::Error | DiagnosticSeverity::Fatal
                )
            })
    }

    /// Human-readable report summary.
    #[must_use]
    pub fn to_human_text(&self) -> String {
        format!(
            "stateful reuse plan\nschema_version: {}\nreport: {}\nstatus: {}\nboundaries: {}\ninvalidation requirements: {}\ncache read/write/replay: disabled\nincremental execution: disabled\nfallback execution: disabled",
            self.schema_version,
            self.report_id,
            self.status.as_str(),
            self.boundary_count(),
            self.invalidation_requirement_count(),
        )
    }
}

fn cg17_reuse_boundaries() -> Vec<StatefulReuseBoundary> {
    vec![
        StatefulReuseBoundary::planned("reuse.segment_result", ReuseCacheKind::SegmentResult),
        StatefulReuseBoundary::planned("reuse.predicate_result", ReuseCacheKind::PredicateResult),
        StatefulReuseBoundary::planned(
            "reuse.encoded_dictionary",
            ReuseCacheKind::EncodedDictionary,
        ),
        StatefulReuseBoundary::planned("reuse.encoded_filter", ReuseCacheKind::EncodedFilter),
        StatefulReuseBoundary::planned("reuse.layout_decision", ReuseCacheKind::LayoutDecision),
        StatefulReuseBoundary::planned(
            "reuse.execution_certificate",
            ReuseCacheKind::ExecutionCertificate,
        ),
        StatefulReuseBoundary::planned(
            "reuse.incremental_manifest_diff",
            ReuseCacheKind::IncrementalManifestDiff,
        ),
    ]
}

fn cg17_invalidation_requirements() -> Vec<InvalidationProofRequirement> {
    vec![
        InvalidationProofRequirement::required(
            InvalidationSignalKind::SnapshotChanged,
            "compare snapshot and manifest hashes before reuse",
        ),
        InvalidationProofRequirement::required(
            InvalidationSignalKind::SegmentAdded,
            "reuse unchanged segments and compute added segments only with proof",
        ),
        InvalidationProofRequirement::required(
            InvalidationSignalKind::SegmentRemoved,
            "remove stale segment results before reuse",
        ),
        InvalidationProofRequirement::required(
            InvalidationSignalKind::SegmentReplaced,
            "invalidate replaced segment results",
        ),
        InvalidationProofRequirement::required(
            InvalidationSignalKind::SchemaChanged,
            "require schema compatibility proof or recompute",
        ),
        InvalidationProofRequirement::required(
            InvalidationSignalKind::PartitionChanged,
            "require partition compatibility proof or recompute",
        ),
        InvalidationProofRequirement::required(
            InvalidationSignalKind::PredicateChanged,
            "require predicate hash match before predicate-result reuse",
        ),
        InvalidationProofRequirement::required(
            InvalidationSignalKind::SemanticProfileChanged,
            "require semantic profile match before reuse",
        ),
        InvalidationProofRequirement::required(
            InvalidationSignalKind::FunctionVersionChanged,
            "require function version compatibility before reuse",
        ),
        InvalidationProofRequirement::required(
            InvalidationSignalKind::AdapterFidelityChanged,
            "require adapter fidelity compatibility before reuse",
        ),
        InvalidationProofRequirement::required(
            InvalidationSignalKind::UnknownChange,
            "reject reuse and require recompute",
        ),
    ]
}

/// Produces the CG-17 stateful reuse report.
#[must_use]
pub fn plan_stateful_reuse() -> StatefulReuseReport {
    StatefulReuseReport::cg17_foundation()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stateful_reuse_foundation_is_report_only() {
        let report = StatefulReuseReport::cg17_foundation();

        assert_eq!(report.status, StatefulReuseStatus::ReportOnlyPlanned);
        assert_eq!(report.boundary_count(), 7);
        assert_eq!(report.invalidation_requirement_count(), 11);
        assert_eq!(report.correctness_proof_required_count(), 7);
        assert_eq!(report.invalidation_proof_required_count(), 7);
        assert_eq!(report.execution_certificate_required_count(), 7);
        assert_eq!(
            report.cache_kind_order(),
            "segment_result,predicate_result,encoded_dictionary,encoded_filter,layout_decision,execution_certificate,incremental_manifest_diff"
        );
        assert_eq!(
            report.invalidation_signal_order(),
            "snapshot_changed,segment_added,segment_removed,segment_replaced,schema_changed,partition_changed,predicate_changed,semantic_profile_changed,function_version_changed,adapter_fidelity_changed,unknown_change"
        );
        assert!(report.typed_cache_boundaries_required);
        assert!(report.deterministic_keys_required);
        assert!(report.invalidation_proofs_required);
        assert!(report.correctness_proofs_required);
        assert!(report.execution_certificates_required);
        assert!(report.manifest_diff_required);
        assert!(report.is_side_effect_free());
        assert!(!report.has_errors());
        assert!(!report.cache_read);
        assert!(!report.cache_write);
        assert!(!report.cache_replay);
        assert!(!report.incremental_execution);
        assert!(!report.runtime_execution);
        assert!(!report.external_engine_execution);
        assert!(!report.fallback_execution_allowed);
        assert!(!report.fallback_attempted);
        assert!(!report.production_claim_allowed);
    }

    #[test]
    fn stateful_reuse_report_flags_side_effects_and_production_claims_as_errors() {
        let mut report = StatefulReuseReport::cg17_foundation();
        report.cache_read = true;
        assert!(report.has_errors());

        let mut report = StatefulReuseReport::cg17_foundation();
        report.incremental_execution = true;
        assert!(report.has_errors());

        let mut report = StatefulReuseReport::cg17_foundation();
        report.production_claim_allowed = true;
        assert!(report.has_errors());
    }

    #[test]
    fn stateful_reuse_boundary_rejects_cross_dataset_reuse() {
        let mut boundary =
            StatefulReuseBoundary::planned("reuse.test", ReuseCacheKind::SegmentResult);

        boundary.cross_dataset_reuse_allowed = true;

        assert!(boundary.has_errors());
    }
}
