//! CG-17 stateful reuse and incremental execution planning contracts.
//!
//! This module defines typed cache/reuse boundaries and invalidation proof
//! requirements only. It does not read caches, write caches, replay cached
//! results, or execute incremental recomputation.

use std::fmt::Write as _;

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

/// CG-17 promotion surfaces that must be kept explicit before reuse can execute.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StatefulReusePromotionSurface {
    BoundaryReportFoundation,
    CdcIncrementalPlanningFoundation,
    StableReuseKeyDerivation,
    ReuseKeyDigestAndScope,
    ManifestDiffInputEvidence,
    InvalidationDecisionMatrix,
    CacheSafetyPolicy,
    StateCertificateSchema,
    ExecutionCertificateLinkage,
    NativeIoCertificateLinkage,
    ReuseBenchmarkConstitution,
    IncrementalRecomputeExecution,
    ProductionReuseClaimCloseout,
}

impl StatefulReusePromotionSurface {
    /// Stable machine-readable surface label.
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::BoundaryReportFoundation => "boundary_report_foundation",
            Self::CdcIncrementalPlanningFoundation => "cdc_incremental_planning_foundation",
            Self::StableReuseKeyDerivation => "stable_reuse_key_derivation",
            Self::ReuseKeyDigestAndScope => "reuse_key_digest_and_scope",
            Self::ManifestDiffInputEvidence => "manifest_diff_input_evidence",
            Self::InvalidationDecisionMatrix => "invalidation_decision_matrix",
            Self::CacheSafetyPolicy => "cache_safety_policy",
            Self::StateCertificateSchema => "state_certificate_schema",
            Self::ExecutionCertificateLinkage => "execution_certificate_linkage",
            Self::NativeIoCertificateLinkage => "native_io_certificate_linkage",
            Self::ReuseBenchmarkConstitution => "reuse_benchmark_constitution",
            Self::IncrementalRecomputeExecution => "incremental_recompute_execution",
            Self::ProductionReuseClaimCloseout => "production_reuse_claim_closeout",
        }
    }
}

/// Evidence status for one CG-17 promotion surface.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StatefulReusePromotionStatus {
    ExistingContractEvidence,
    BlockedUntilCertified,
}

impl StatefulReusePromotionStatus {
    /// Stable machine-readable status label.
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::ExistingContractEvidence => "existing_contract_evidence",
            Self::BlockedUntilCertified => "blocked_until_certified",
        }
    }

    /// Returns whether this surface is already represented by existing report contracts.
    #[must_use]
    pub const fn is_existing_evidence(&self) -> bool {
        matches!(self, Self::ExistingContractEvidence)
    }
}

/// One CG-17 promotion gate entry.
#[derive(Debug, Clone, PartialEq, Eq)]
#[allow(clippy::struct_excessive_bools)]
pub struct StatefulReusePromotionGateEntry {
    pub surface: StatefulReusePromotionSurface,
    pub status: StatefulReusePromotionStatus,
    pub existing_report_ref: Option<&'static str>,
    pub requires_stable_key: bool,
    pub requires_key_digest: bool,
    pub requires_manifest_diff_input: bool,
    pub requires_invalidation_evidence: bool,
    pub requires_cache_safety_policy: bool,
    pub requires_state_certificate: bool,
    pub requires_correctness_evidence: bool,
    pub requires_execution_certificate: bool,
    pub requires_native_io_certificate: bool,
    pub requires_reuse_benchmark: bool,
    pub cache_read_allowed: bool,
    pub cache_write_allowed: bool,
    pub cache_replay_allowed: bool,
    pub incremental_execution_allowed: bool,
    pub runtime_allowed: bool,
    pub external_engine_invoked: bool,
    pub fallback_execution_allowed: bool,
}

impl StatefulReusePromotionGateEntry {
    /// Existing report-only evidence already available in the repo.
    #[must_use]
    pub const fn existing(
        surface: StatefulReusePromotionSurface,
        existing_report_ref: &'static str,
    ) -> Self {
        Self {
            surface,
            status: StatefulReusePromotionStatus::ExistingContractEvidence,
            existing_report_ref: Some(existing_report_ref),
            requires_stable_key: false,
            requires_key_digest: false,
            requires_manifest_diff_input: false,
            requires_invalidation_evidence: false,
            requires_cache_safety_policy: false,
            requires_state_certificate: false,
            requires_correctness_evidence: false,
            requires_execution_certificate: false,
            requires_native_io_certificate: false,
            requires_reuse_benchmark: false,
            cache_read_allowed: false,
            cache_write_allowed: false,
            cache_replay_allowed: false,
            incremental_execution_allowed: false,
            runtime_allowed: false,
            external_engine_invoked: false,
            fallback_execution_allowed: false,
        }
    }

    /// Blocked promotion surface that needs evidence before execution or claims.
    #[must_use]
    pub const fn blocked(surface: StatefulReusePromotionSurface) -> Self {
        Self {
            surface,
            status: StatefulReusePromotionStatus::BlockedUntilCertified,
            existing_report_ref: None,
            requires_stable_key: true,
            requires_key_digest: true,
            requires_manifest_diff_input: true,
            requires_invalidation_evidence: true,
            requires_cache_safety_policy: true,
            requires_state_certificate: true,
            requires_correctness_evidence: true,
            requires_execution_certificate: true,
            requires_native_io_certificate: true,
            requires_reuse_benchmark: true,
            cache_read_allowed: false,
            cache_write_allowed: false,
            cache_replay_allowed: false,
            incremental_execution_allowed: false,
            runtime_allowed: false,
            external_engine_invoked: false,
            fallback_execution_allowed: false,
        }
    }

    /// Returns whether this entry has no runtime, IO, cache, or fallback effects.
    #[must_use]
    pub const fn side_effect_free(&self) -> bool {
        !self.cache_read_allowed
            && !self.cache_write_allowed
            && !self.cache_replay_allowed
            && !self.incremental_execution_allowed
            && !self.runtime_allowed
            && !self.external_engine_invoked
            && !self.fallback_execution_allowed
    }
}

/// Report-only gate for promoting CG-17 stateful reuse and incremental execution.
#[derive(Debug, Clone, PartialEq, Eq)]
#[allow(clippy::struct_excessive_bools)]
pub struct StatefulReusePromotionGateReport {
    pub schema_version: &'static str,
    pub report_id: &'static str,
    pub entries: Vec<StatefulReusePromotionGateEntry>,
    pub existing_report_refs: Vec<&'static str>,
    pub existing_stateful_reuse_boundary_report_present: bool,
    pub existing_cdc_incremental_planning_report_present: bool,
    pub stable_reuse_keys_required: bool,
    pub key_digest_and_scope_required: bool,
    pub manifest_diff_inputs_required: bool,
    pub invalidation_evidence_required: bool,
    pub cache_safety_policy_required: bool,
    pub state_certificates_required: bool,
    pub correctness_evidence_required: bool,
    pub execution_certificate_required: bool,
    pub native_io_certificate_required: bool,
    pub reuse_benchmark_required: bool,
    pub cache_read_allowed: bool,
    pub cache_write_allowed: bool,
    pub cache_replay_allowed: bool,
    pub incremental_execution_allowed: bool,
    pub runtime_execution_allowed: bool,
    pub manifest_diff_read_allowed: bool,
    pub state_certificate_claim_allowed: bool,
    pub reuse_performance_claim_allowed: bool,
    pub incremental_performance_claim_allowed: bool,
    pub production_claim_allowed: bool,
    pub external_engine_invoked: bool,
    pub fallback_execution_allowed: bool,
    pub fallback_attempted: bool,
    pub diagnostics: Vec<Diagnostic>,
}

impl StatefulReusePromotionGateReport {
    /// Creates the default report-only CG-17 promotion gate.
    #[must_use]
    pub fn planning_default() -> Self {
        Self {
            schema_version: "shardloom.stateful_reuse_promotion_gate.v1",
            report_id: "cg17.stateful_reuse_promotion_gate",
            entries: stateful_reuse_promotion_entries(),
            existing_report_refs: stateful_reuse_promotion_existing_report_refs(),
            existing_stateful_reuse_boundary_report_present: true,
            existing_cdc_incremental_planning_report_present: true,
            stable_reuse_keys_required: true,
            key_digest_and_scope_required: true,
            manifest_diff_inputs_required: true,
            invalidation_evidence_required: true,
            cache_safety_policy_required: true,
            state_certificates_required: true,
            correctness_evidence_required: true,
            execution_certificate_required: true,
            native_io_certificate_required: true,
            reuse_benchmark_required: true,
            cache_read_allowed: false,
            cache_write_allowed: false,
            cache_replay_allowed: false,
            incremental_execution_allowed: false,
            runtime_execution_allowed: false,
            manifest_diff_read_allowed: false,
            state_certificate_claim_allowed: false,
            reuse_performance_claim_allowed: false,
            incremental_performance_claim_allowed: false,
            production_claim_allowed: false,
            external_engine_invoked: false,
            fallback_execution_allowed: false,
            fallback_attempted: false,
            diagnostics: Vec::new(),
        }
    }

    /// Number of promotion surfaces.
    #[must_use]
    pub fn surface_count(&self) -> usize {
        self.entries.len()
    }

    /// Number of existing evidence surfaces.
    #[must_use]
    pub fn existing_evidence_surface_count(&self) -> usize {
        self.entries
            .iter()
            .filter(|entry| entry.status.is_existing_evidence())
            .count()
    }

    /// Number of blocked surfaces.
    #[must_use]
    pub fn blocked_surface_count(&self) -> usize {
        self.entries
            .iter()
            .filter(|entry| {
                matches!(
                    entry.status,
                    StatefulReusePromotionStatus::BlockedUntilCertified
                )
            })
            .count()
    }

    /// Stable surface order for CLI reporting.
    #[must_use]
    pub fn surface_order(&self) -> Vec<&'static str> {
        self.entries
            .iter()
            .map(|entry| entry.surface.as_str())
            .collect()
    }

    /// Returns whether all cache/reuse runtime promotions are blocked.
    #[must_use]
    pub fn runtime_promotions_blocked(&self) -> bool {
        !self.cache_read_allowed
            && !self.cache_write_allowed
            && !self.cache_replay_allowed
            && !self.incremental_execution_allowed
            && !self.runtime_execution_allowed
            && !self.manifest_diff_read_allowed
            && !self.external_engine_invoked
            && self.entries.iter().all(|entry| {
                !entry.cache_read_allowed
                    && !entry.cache_write_allowed
                    && !entry.cache_replay_allowed
                    && !entry.incremental_execution_allowed
                    && !entry.runtime_allowed
                    && !entry.external_engine_invoked
            })
    }

    /// Returns whether every cache/reuse claim remains blocked.
    #[must_use]
    pub const fn claim_blocked(&self) -> bool {
        !self.state_certificate_claim_allowed
            && !self.reuse_performance_claim_allowed
            && !self.incremental_performance_claim_allowed
            && !self.production_claim_allowed
    }

    /// Returns whether this gate has no execution, IO, cache, external-engine, or fallback effects.
    #[must_use]
    pub fn side_effect_free(&self) -> bool {
        self.runtime_promotions_blocked()
            && !self.fallback_execution_allowed
            && !self.fallback_attempted
            && self
                .entries
                .iter()
                .all(StatefulReusePromotionGateEntry::side_effect_free)
    }

    /// Returns whether the gate has impossible or unsafe state.
    #[must_use]
    pub fn has_errors(&self) -> bool {
        !self.side_effect_free()
            || !self.claim_blocked()
            || self.diagnostics.iter().any(|diagnostic| {
                matches!(
                    diagnostic.severity,
                    DiagnosticSeverity::Error | DiagnosticSeverity::Fatal
                )
            })
    }

    /// Human-readable gate summary.
    #[must_use]
    pub fn to_human_text(&self) -> String {
        let mut out = String::new();
        let _ = writeln!(out, "schema_version: {}", self.schema_version);
        let _ = writeln!(out, "report_id: {}", self.report_id);
        let _ = writeln!(
            out,
            "existing report refs: {}",
            self.existing_report_refs.join(",")
        );
        let _ = writeln!(
            out,
            "runtime promotions blocked: {}",
            self.runtime_promotions_blocked()
        );
        let _ = writeln!(out, "claim blocked: {}", self.claim_blocked());
        let _ = writeln!(out, "side effect free: {}", self.side_effect_free());
        let _ = writeln!(out, "fallback attempted: {}", self.fallback_attempted);
        let _ = writeln!(
            out,
            "fallback execution allowed: {}",
            self.fallback_execution_allowed
        );
        let _ = writeln!(out, "surfaces:");
        for entry in &self.entries {
            let _ = writeln!(
                out,
                "  - {} [{}] existing_ref={} requires_stable_key={} requires_manifest_diff_input={} requires_state_certificate={} requires_reuse_benchmark={} cache_read_allowed={} incremental_execution_allowed={} fallback_execution_allowed={}",
                entry.surface.as_str(),
                entry.status.as_str(),
                entry.existing_report_ref.unwrap_or("none"),
                entry.requires_stable_key,
                entry.requires_manifest_diff_input,
                entry.requires_state_certificate,
                entry.requires_reuse_benchmark,
                entry.cache_read_allowed,
                entry.incremental_execution_allowed,
                entry.fallback_execution_allowed
            );
        }
        out
    }
}

fn stateful_reuse_promotion_entries() -> Vec<StatefulReusePromotionGateEntry> {
    vec![
        StatefulReusePromotionGateEntry::existing(
            StatefulReusePromotionSurface::BoundaryReportFoundation,
            "stateful-reuse-plan",
        ),
        StatefulReusePromotionGateEntry::existing(
            StatefulReusePromotionSurface::CdcIncrementalPlanningFoundation,
            "incremental-plan cdc",
        ),
        StatefulReusePromotionGateEntry::blocked(
            StatefulReusePromotionSurface::StableReuseKeyDerivation,
        ),
        StatefulReusePromotionGateEntry::blocked(
            StatefulReusePromotionSurface::ReuseKeyDigestAndScope,
        ),
        StatefulReusePromotionGateEntry::blocked(
            StatefulReusePromotionSurface::ManifestDiffInputEvidence,
        ),
        StatefulReusePromotionGateEntry::blocked(
            StatefulReusePromotionSurface::InvalidationDecisionMatrix,
        ),
        StatefulReusePromotionGateEntry::blocked(StatefulReusePromotionSurface::CacheSafetyPolicy),
        StatefulReusePromotionGateEntry::blocked(
            StatefulReusePromotionSurface::StateCertificateSchema,
        ),
        StatefulReusePromotionGateEntry::blocked(
            StatefulReusePromotionSurface::ExecutionCertificateLinkage,
        ),
        StatefulReusePromotionGateEntry::blocked(
            StatefulReusePromotionSurface::NativeIoCertificateLinkage,
        ),
        StatefulReusePromotionGateEntry::blocked(
            StatefulReusePromotionSurface::ReuseBenchmarkConstitution,
        ),
        StatefulReusePromotionGateEntry::blocked(
            StatefulReusePromotionSurface::IncrementalRecomputeExecution,
        ),
        StatefulReusePromotionGateEntry::blocked(
            StatefulReusePromotionSurface::ProductionReuseClaimCloseout,
        ),
    ]
}

fn stateful_reuse_promotion_existing_report_refs() -> Vec<&'static str> {
    vec![
        "stateful-reuse-plan",
        "incremental-plan cdc",
        "execution-certificate-plan",
        "native-io-envelope-plan",
        "benchmark-claim-evidence-plan",
        "operational_contracts.evidence_artifact_envelope",
    ]
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

/// Produces the CG-17 stateful reuse promotion gate.
#[must_use]
pub fn plan_stateful_reuse_promotion_gate() -> StatefulReusePromotionGateReport {
    StatefulReusePromotionGateReport::planning_default()
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

    #[test]
    fn stateful_reuse_promotion_gate_blocks_reuse_runtime_and_claims() {
        let report = plan_stateful_reuse_promotion_gate();

        assert_eq!(
            report.schema_version,
            "shardloom.stateful_reuse_promotion_gate.v1"
        );
        assert_eq!(report.report_id, "cg17.stateful_reuse_promotion_gate");
        assert_eq!(report.surface_count(), 13);
        assert_eq!(report.existing_evidence_surface_count(), 2);
        assert_eq!(report.blocked_surface_count(), 11);
        assert_eq!(
            report.surface_order().join(","),
            "boundary_report_foundation,cdc_incremental_planning_foundation,stable_reuse_key_derivation,reuse_key_digest_and_scope,manifest_diff_input_evidence,invalidation_decision_matrix,cache_safety_policy,state_certificate_schema,execution_certificate_linkage,native_io_certificate_linkage,reuse_benchmark_constitution,incremental_recompute_execution,production_reuse_claim_closeout"
        );
        assert!(report.existing_stateful_reuse_boundary_report_present);
        assert!(report.existing_cdc_incremental_planning_report_present);
        assert!(report.stable_reuse_keys_required);
        assert!(report.key_digest_and_scope_required);
        assert!(report.manifest_diff_inputs_required);
        assert!(report.invalidation_evidence_required);
        assert!(report.cache_safety_policy_required);
        assert!(report.state_certificates_required);
        assert!(report.correctness_evidence_required);
        assert!(report.execution_certificate_required);
        assert!(report.native_io_certificate_required);
        assert!(report.reuse_benchmark_required);
        assert!(report.runtime_promotions_blocked());
        assert!(report.claim_blocked());
        assert!(report.side_effect_free());
        assert!(!report.has_errors());
        assert!(!report.cache_read_allowed);
        assert!(!report.cache_write_allowed);
        assert!(!report.cache_replay_allowed);
        assert!(!report.incremental_execution_allowed);
        assert!(!report.runtime_execution_allowed);
        assert!(!report.manifest_diff_read_allowed);
        assert!(!report.external_engine_invoked);
        assert!(!report.fallback_execution_allowed);
        assert!(!report.fallback_attempted);
    }

    #[test]
    fn stateful_reuse_promotion_gate_flags_runtime_or_claims_as_errors() {
        let mut report = plan_stateful_reuse_promotion_gate();
        report.cache_read_allowed = true;
        assert!(report.has_errors());

        let mut report = plan_stateful_reuse_promotion_gate();
        report.incremental_execution_allowed = true;
        assert!(report.has_errors());

        let mut report = plan_stateful_reuse_promotion_gate();
        report.reuse_performance_claim_allowed = true;
        assert!(report.has_errors());
    }
}
