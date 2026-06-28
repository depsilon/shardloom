//! CG-19 universal native I/O envelope contracts.
//!
//! This module defines report-only native I/O envelope evidence. It does not
//! probe adapters, read data, decode arrays, materialize rows, convert to Arrow,
//! execute object-store I/O, write outputs, spill data, or run fallback engines.

use crate::{Diagnostic, DiagnosticSeverity};

/// Report-level status for the CG-19 universal native I/O envelope.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NativeIoEnvelopeStatus {
    ReportOnlyPlanned,
    EvidenceIncomplete,
    Certified,
    Blocked,
}

impl NativeIoEnvelopeStatus {
    /// Stable machine-readable status label.
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::ReportOnlyPlanned => "report_only_planned",
            Self::EvidenceIncomplete => "evidence_incomplete",
            Self::Certified => "certified",
            Self::Blocked => "blocked",
        }
    }

    /// Returns whether this status should fail a report command.
    #[must_use]
    pub const fn is_error(&self) -> bool {
        matches!(self, Self::Blocked)
    }
}

/// Contract families required by RFC 0031.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NativeIoContractKind {
    NativeWorkEnvelope,
    NativeWorkStream,
    NativeResultStream,
    SourceCapabilityReport,
    SourcePushdownReport,
    SinkRequirementReport,
    AdapterFidelityReport,
    MaterializationBoundaryReport,
    NativeIoCertificate,
}

impl NativeIoContractKind {
    /// Stable machine-readable contract label.
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::NativeWorkEnvelope => "native_work_envelope",
            Self::NativeWorkStream => "native_work_stream",
            Self::NativeResultStream => "native_result_stream",
            Self::SourceCapabilityReport => "source_capability_report",
            Self::SourcePushdownReport => "source_pushdown_report",
            Self::SinkRequirementReport => "sink_requirement_report",
            Self::AdapterFidelityReport => "adapter_fidelity_report",
            Self::MaterializationBoundaryReport => "materialization_boundary_report",
            Self::NativeIoCertificate => "native_io_certificate",
        }
    }
}

/// Report-only field contract for one RFC 0031 surface.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NativeIoContractSurface {
    pub surface_id: String,
    pub kind: NativeIoContractKind,
    pub field_order: Vec<&'static str>,
    pub fallback_attempted: bool,
}

impl NativeIoContractSurface {
    /// Creates a contract surface with a stable field order.
    #[must_use]
    pub fn planned(
        surface_id: impl Into<String>,
        kind: NativeIoContractKind,
        field_order: Vec<&'static str>,
    ) -> Self {
        Self {
            surface_id: surface_id.into(),
            kind,
            field_order,
            fallback_attempted: false,
        }
    }

    /// Returns whether the contract surface is invalid.
    #[must_use]
    pub const fn has_errors(&self) -> bool {
        self.fallback_attempted
    }
}

/// Representation states tracked by the native I/O envelope.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RepresentationState {
    MetadataOnly,
    Pruned,
    VortexEncoded,
    ForeignEncoded,
    SelectionVectorEncoded,
    PartiallyDecoded,
    DecodedColumnar,
    MaterializedRows,
    ExternalEffect,
    Unsupported,
}

impl RepresentationState {
    /// Stable machine-readable representation label.
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::MetadataOnly => "metadata_only",
            Self::Pruned => "pruned",
            Self::VortexEncoded => "vortex_encoded",
            Self::ForeignEncoded => "foreign_encoded",
            Self::SelectionVectorEncoded => "selection_vector_encoded",
            Self::PartiallyDecoded => "partially_decoded",
            Self::DecodedColumnar => "decoded_columnar",
            Self::MaterializedRows => "materialized_rows",
            Self::ExternalEffect => "external_effect",
            Self::Unsupported => "unsupported",
        }
    }
}

/// Field-level semantics for a representation state.
#[derive(Debug, Clone, PartialEq, Eq)]
#[allow(clippy::struct_excessive_bools)]
pub struct RepresentationStateContract {
    pub state: RepresentationState,
    pub meaning: &'static str,
    pub allowed_transitions: Vec<RepresentationState>,
    pub forbidden_assumptions: Vec<&'static str>,
    pub implies_decode: bool,
    pub implies_row_materialization: bool,
    pub can_remain_encoded: bool,
    pub unsupported_terminal: bool,
}

impl RepresentationStateContract {
    /// Number of explicitly allowed transitions.
    #[must_use]
    pub fn allowed_transition_count(&self) -> usize {
        self.allowed_transitions.len()
    }
}

/// Example transition that future source/sink paths must report.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NativeIoTransitionExample {
    pub from_state: Option<RepresentationState>,
    pub to_state: RepresentationState,
    pub requires_materialization_boundary: bool,
    pub capability_failure_transition: bool,
    pub allowed: bool,
}

impl NativeIoTransitionExample {
    /// Creates a representation transition example.
    #[must_use]
    pub const fn new(
        from_state: RepresentationState,
        to_state: RepresentationState,
        requires_materialization_boundary: bool,
    ) -> Self {
        Self {
            from_state: Some(from_state),
            to_state,
            requires_materialization_boundary,
            capability_failure_transition: false,
            allowed: true,
        }
    }

    /// Creates the capability-failure transition to unsupported.
    #[must_use]
    pub const fn unsupported_on_capability_failure() -> Self {
        Self {
            from_state: None,
            to_state: RepresentationState::Unsupported,
            requires_materialization_boundary: false,
            capability_failure_transition: true,
            allowed: true,
        }
    }

    /// Stable label for the source state.
    #[must_use]
    pub const fn from_label(&self) -> &'static str {
        match self.from_state {
            Some(state) => state.as_str(),
            None => "any",
        }
    }

    /// Stable transition label for CLI reporting.
    #[must_use]
    pub fn transition_label(&self) -> String {
        format!("{}->{}", self.from_label(), self.to_state.as_str())
    }
}

/// Per-source/sink-path certificate requirement.
#[derive(Debug, Clone, PartialEq, Eq)]
#[allow(clippy::struct_excessive_bools)]
pub struct NativeIoCertificatePathRequirement {
    pub path_id: String,
    pub source_capability_report_required: bool,
    pub source_pushdown_report_required: bool,
    pub representation_transitions_required: bool,
    pub sink_requirement_report_required: bool,
    pub adapter_fidelity_report_required: bool,
    pub materialization_boundaries_required: bool,
    pub side_effects_required: bool,
    pub diagnostics_required: bool,
    pub fallback_attempted: bool,
}

impl NativeIoCertificatePathRequirement {
    /// Creates a required per-path certificate entry.
    #[must_use]
    pub fn required(path_id: impl Into<String>) -> Self {
        Self {
            path_id: path_id.into(),
            source_capability_report_required: true,
            source_pushdown_report_required: true,
            representation_transitions_required: true,
            sink_requirement_report_required: true,
            adapter_fidelity_report_required: true,
            materialization_boundaries_required: true,
            side_effects_required: true,
            diagnostics_required: true,
            fallback_attempted: false,
        }
    }

    /// Returns whether this path requirement has an error state.
    #[must_use]
    pub const fn has_errors(&self) -> bool {
        self.fallback_attempted
            || !self.source_capability_report_required
            || !self.source_pushdown_report_required
            || !self.representation_transitions_required
            || !self.sink_requirement_report_required
            || !self.adapter_fidelity_report_required
            || !self.materialization_boundaries_required
            || !self.side_effects_required
            || !self.diagnostics_required
    }
}

/// Source or sink side covered by the Native I/O source/sink matrix.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NativeIoCoverageDirection {
    Source,
    Sink,
}

impl NativeIoCoverageDirection {
    /// Stable machine-readable direction label.
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::Source => "source",
            Self::Sink => "sink",
        }
    }
}

/// Report-only support/evidence row for one source or sink family.
#[derive(Debug, Clone, PartialEq, Eq)]
#[allow(clippy::struct_excessive_bools)]
pub struct NativeIoSourceSinkCoverageRow {
    pub id: &'static str,
    pub direction: NativeIoCoverageDirection,
    pub family: &'static str,
    pub surface: &'static str,
    pub support_status: &'static str,
    pub support_basis: &'static str,
    pub execution_modes: &'static str,
    pub native_io_certificate_refs: &'static str,
    pub certificate_status: &'static str,
    pub unsupported_diagnostic_code: &'static str,
    pub blocker_id: &'static str,
    pub required_future_evidence: &'static str,
    pub claim_gate_status: &'static str,
    pub claim_boundary: &'static str,
    pub source_refs: &'static str,
    pub runtime_execution: bool,
    pub data_read: bool,
    pub write_io: bool,
    pub object_store_io: bool,
    pub catalog_probe: bool,
    pub network_probe: bool,
    pub external_effects_executed: bool,
    pub fallback_attempted: bool,
    pub external_engine_invoked: bool,
}

impl NativeIoSourceSinkCoverageRow {
    /// Returns whether this row needs a deterministic unsupported diagnostic.
    #[must_use]
    pub fn requires_unsupported_diagnostic(&self) -> bool {
        matches!(
            self.support_status,
            "unsupported" | "planned" | "report_only"
        )
    }

    /// Returns whether the row has a non-placeholder unsupported diagnostic.
    #[must_use]
    pub fn has_unsupported_diagnostic(&self) -> bool {
        self.unsupported_diagnostic_code != "none"
    }

    /// Returns whether this coverage row violates report-only/no-fallback policy.
    #[must_use]
    pub fn has_errors(&self) -> bool {
        self.runtime_execution
            || self.data_read
            || self.write_io
            || self.object_store_io
            || self.catalog_probe
            || self.network_probe
            || self.external_effects_executed
            || self.fallback_attempted
            || self.external_engine_invoked
            || (self.requires_unsupported_diagnostic() && !self.has_unsupported_diagnostic())
    }
}

/// Runtime source capability evidence for one native I/O path.
#[derive(Debug, Clone, PartialEq, Eq)]
#[allow(clippy::struct_excessive_bools)]
pub struct NativeIoSourceCapabilityReport {
    pub source_kind: String,
    pub adapter_id: String,
    pub schema_discovery_status: String,
    pub statistics_availability: String,
    pub pushdown_capabilities: String,
    pub encoded_representation_preserved: bool,
    pub range_read_capability: bool,
    pub streaming_capability: bool,
    pub object_store_capability: bool,
    pub fallback_attempted: bool,
}

impl NativeIoSourceCapabilityReport {
    /// Returns whether the source capability report violates no-fallback policy.
    #[must_use]
    pub const fn has_errors(&self) -> bool {
        self.fallback_attempted
    }
}

/// Runtime pushdown evidence for one source path.
#[derive(Debug, Clone, PartialEq, Eq)]
#[allow(clippy::struct_excessive_bools)]
pub struct NativeIoSourcePushdownReport {
    pub accepted_operations: Vec<String>,
    pub rejected_operations: Vec<String>,
    pub guarantee: String,
    pub proof_basis: String,
    pub residual_expression: Option<String>,
    pub conservative_false_positive_policy: bool,
    pub unsafe_rejected_reason: Option<String>,
    pub fallback_attempted: bool,
}

impl NativeIoSourcePushdownReport {
    /// Returns whether the pushdown report violates no-fallback policy.
    #[must_use]
    pub const fn has_errors(&self) -> bool {
        self.fallback_attempted
    }

    /// Stable comma-separated accepted operation order.
    #[must_use]
    pub fn accepted_operation_order(&self) -> String {
        self.accepted_operations.join(",")
    }

    /// Stable comma-separated rejected operation order.
    #[must_use]
    pub fn rejected_operation_order(&self) -> String {
        self.rejected_operations.join(",")
    }
}

/// Runtime sink requirement evidence for one native I/O path.
#[derive(Debug, Clone, PartialEq, Eq)]
#[allow(clippy::struct_excessive_bools)]
pub struct NativeIoSinkRequirementReport {
    pub target_format: String,
    pub accepts_encoded: bool,
    pub requires_decoded_columnar: bool,
    pub requires_rows: bool,
    pub preserves_metadata: bool,
    pub requires_ordering: bool,
    pub requires_partitioning: bool,
    pub requires_commit: bool,
    pub supports_streaming: bool,
    pub max_chunk_size: Option<u64>,
    pub backpressure_policy: String,
}

impl NativeIoSinkRequirementReport {
    /// Returns whether the sink requirement forces row materialization.
    #[must_use]
    pub const fn requires_row_materialization(&self) -> bool {
        self.requires_rows
    }
}

/// Runtime fidelity evidence for one source-to-sink adapter path.
#[derive(Debug, Clone, PartialEq, Eq)]
#[allow(clippy::struct_excessive_bools)]
pub struct NativeIoAdapterFidelityReport {
    pub adapter_id: String,
    pub source_kind: String,
    pub sink_kind: String,
    pub metadata_preserved: bool,
    pub statistics_preserved: bool,
    pub encoded_representation_preserved: bool,
    pub materialization_required: bool,
    pub fidelity_loss: String,
    pub metadata_loss: String,
    pub fallback_attempted: bool,
}

impl NativeIoAdapterFidelityReport {
    /// Returns whether the adapter fidelity report violates no-fallback policy.
    #[must_use]
    pub const fn has_errors(&self) -> bool {
        self.fallback_attempted
    }
}

/// Runtime representation transition evidence for one native I/O path.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NativeIoRepresentationTransition {
    pub from_state: RepresentationState,
    pub to_state: RepresentationState,
    pub materialization_boundary_reported: bool,
}

impl NativeIoRepresentationTransition {
    /// Creates a runtime representation transition.
    #[must_use]
    pub const fn new(
        from_state: RepresentationState,
        to_state: RepresentationState,
        materialization_boundary_reported: bool,
    ) -> Self {
        Self {
            from_state,
            to_state,
            materialization_boundary_reported,
        }
    }

    /// Stable transition label for CLI and benchmark reporting.
    #[must_use]
    pub fn transition_label(&self) -> String {
        format!("{}->{}", self.from_state.as_str(), self.to_state.as_str())
    }

    /// Returns whether this transition requires materialization boundary evidence.
    #[must_use]
    pub const fn requires_materialization_boundary(&self) -> bool {
        matches!(
            self.to_state,
            RepresentationState::PartiallyDecoded
                | RepresentationState::DecodedColumnar
                | RepresentationState::MaterializedRows
        )
    }

    /// Returns whether the transition is missing required boundary evidence.
    #[must_use]
    pub const fn has_errors(&self) -> bool {
        self.requires_materialization_boundary() && !self.materialization_boundary_reported
    }
}

/// Runtime materialization boundary evidence for one native I/O path.
#[derive(Debug, Clone, PartialEq, Eq)]
#[allow(clippy::struct_excessive_bools)]
pub struct NativeIoMaterializationBoundaryReport {
    pub boundary_id: String,
    pub from_state: RepresentationState,
    pub to_state: RepresentationState,
    pub required_by: String,
    pub reason: String,
    pub bytes_decoded: u64,
    pub rows_materialized: u64,
    pub fidelity_loss: String,
    pub fallback_attempted: bool,
}

impl NativeIoMaterializationBoundaryReport {
    /// Returns whether the boundary report violates no-fallback policy.
    #[must_use]
    pub const fn has_errors(&self) -> bool {
        self.fallback_attempted
    }
}

/// Runtime side-effect evidence for one native I/O path.
#[derive(Debug, Clone, PartialEq, Eq)]
#[allow(clippy::struct_excessive_bools)]
pub struct NativeIoSideEffectReport {
    pub data_read: bool,
    pub data_decoded: bool,
    pub data_materialized: bool,
    pub row_read: bool,
    pub arrow_converted: bool,
    pub object_store_io: bool,
    pub write_io: bool,
    pub spill_io_performed: bool,
    pub external_effects_executed: bool,
    pub fallback_attempted: bool,
    pub fallback_execution_allowed: bool,
}

impl NativeIoSideEffectReport {
    /// Returns whether side effects violate `ShardLoom`'s no-fallback policy.
    #[must_use]
    pub const fn has_errors(&self) -> bool {
        self.fallback_attempted || self.fallback_execution_allowed
    }
}

/// Runtime certificate emitted for a specific source/sink native I/O path.
#[derive(Debug, Clone, PartialEq, Eq)]
#[allow(clippy::struct_excessive_bools)]
pub struct NativeIoCertificate {
    pub schema_version: &'static str,
    pub certificate_id: String,
    pub path_id: String,
    pub source_capability_report: NativeIoSourceCapabilityReport,
    pub source_pushdown_report: NativeIoSourcePushdownReport,
    pub representation_transitions: Vec<NativeIoRepresentationTransition>,
    pub sink_requirement_report: NativeIoSinkRequirementReport,
    pub adapter_fidelity_report: NativeIoAdapterFidelityReport,
    pub materialization_boundaries: Vec<NativeIoMaterializationBoundaryReport>,
    pub side_effects: NativeIoSideEffectReport,
    pub diagnostics: Vec<Diagnostic>,
    pub fallback_attempted: bool,
}

impl NativeIoCertificate {
    /// Creates a runtime native I/O certificate.
    ///
    /// # Errors
    /// Returns an error when the certificate or path id is empty.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        certificate_id: impl Into<String>,
        path_id: impl Into<String>,
        source_capability_report: NativeIoSourceCapabilityReport,
        source_pushdown_report: NativeIoSourcePushdownReport,
        representation_transitions: Vec<NativeIoRepresentationTransition>,
        sink_requirement_report: NativeIoSinkRequirementReport,
        adapter_fidelity_report: NativeIoAdapterFidelityReport,
        materialization_boundaries: Vec<NativeIoMaterializationBoundaryReport>,
        side_effects: NativeIoSideEffectReport,
        diagnostics: Vec<Diagnostic>,
    ) -> crate::Result<Self> {
        let certificate_id = certificate_id.into();
        if certificate_id.trim().is_empty() {
            return Err(crate::ShardLoomError::InvalidOperation(
                "native I/O certificate id cannot be empty".to_string(),
            ));
        }
        let path_id = path_id.into();
        if path_id.trim().is_empty() {
            return Err(crate::ShardLoomError::InvalidOperation(
                "native I/O certificate path id cannot be empty".to_string(),
            ));
        }
        Ok(Self {
            schema_version: "shardloom.native_io_certificate.v1",
            certificate_id,
            path_id,
            source_capability_report,
            source_pushdown_report,
            representation_transitions,
            sink_requirement_report,
            adapter_fidelity_report,
            materialization_boundaries,
            side_effects,
            diagnostics,
            fallback_attempted: false,
        })
    }

    /// Stable certificate status label.
    #[must_use]
    pub fn status(&self) -> &'static str {
        if self.has_errors() {
            "blocked"
        } else {
            "certified"
        }
    }

    /// Returns whether the certificate is certified.
    #[must_use]
    pub fn is_certified(&self) -> bool {
        !self.has_errors()
    }

    /// Stable comma-separated transition order.
    #[must_use]
    pub fn representation_transition_order(&self) -> String {
        self.representation_transitions
            .iter()
            .map(NativeIoRepresentationTransition::transition_label)
            .collect::<Vec<_>>()
            .join(",")
    }

    /// Stable comma-separated materialization boundary order.
    #[must_use]
    pub fn materialization_boundary_order(&self) -> String {
        self.materialization_boundaries
            .iter()
            .map(|boundary| boundary.boundary_id.as_str())
            .collect::<Vec<_>>()
            .join(",")
    }

    /// Returns whether required materialization-boundary transitions have evidence.
    #[must_use]
    pub fn materializing_transitions_have_boundaries(&self) -> bool {
        let required_transition_count = self
            .representation_transitions
            .iter()
            .filter(|transition| transition.requires_materialization_boundary())
            .count();
        required_transition_count == 0
            || (!self.materialization_boundaries.is_empty()
                && self
                    .representation_transitions
                    .iter()
                    .filter(|transition| transition.requires_materialization_boundary())
                    .all(|transition| transition.materialization_boundary_reported))
    }

    /// Returns whether the runtime certificate is invalid.
    #[must_use]
    pub fn has_errors(&self) -> bool {
        self.fallback_attempted
            || self.source_capability_report.has_errors()
            || self.source_pushdown_report.has_errors()
            || self
                .representation_transitions
                .iter()
                .any(NativeIoRepresentationTransition::has_errors)
            || self.adapter_fidelity_report.has_errors()
            || self
                .materialization_boundaries
                .iter()
                .any(NativeIoMaterializationBoundaryReport::has_errors)
            || !self.materializing_transitions_have_boundaries()
            || self.side_effects.has_errors()
            || self.diagnostics.iter().any(|diagnostic| {
                matches!(
                    diagnostic.severity,
                    DiagnosticSeverity::Error | DiagnosticSeverity::Fatal
                )
            })
    }
}

/// Report-only CG-19 universal native I/O envelope plan.
#[derive(Debug, Clone, PartialEq, Eq)]
#[allow(clippy::struct_excessive_bools)]
pub struct NativeIoEnvelopeReport {
    pub schema_version: &'static str,
    pub report_id: String,
    pub status: NativeIoEnvelopeStatus,
    pub contract_surfaces: Vec<NativeIoContractSurface>,
    pub representation_states: Vec<RepresentationStateContract>,
    pub transition_examples: Vec<NativeIoTransitionExample>,
    pub certificate_path_requirements: Vec<NativeIoCertificatePathRequirement>,
    pub source_sink_coverage_rows: Vec<NativeIoSourceSinkCoverageRow>,
    pub per_path_certificate_required: bool,
    pub aggregate_certificate_not_sufficient: bool,
    pub preserve_encoded_or_foreign_encoded_when_possible: bool,
    pub decoded_arrow_normalization_allowed: bool,
    pub materialization_boundary_required_for_decoded_columnar: bool,
    pub materialization_boundary_required_for_rows: bool,
    pub source_pushdown_proof_required: bool,
    pub sink_requirement_propagation_required: bool,
    pub adapter_fidelity_report_required: bool,
    pub runtime_execution: bool,
    pub adapter_probe: bool,
    pub data_read: bool,
    pub data_decoded: bool,
    pub data_materialized: bool,
    pub row_read: bool,
    pub arrow_converted: bool,
    pub object_store_io: bool,
    pub write_io: bool,
    pub spill_io_performed: bool,
    pub fallback_execution_allowed: bool,
    pub fallback_attempted: bool,
    pub production_claim_allowed: bool,
    pub diagnostics: Vec<Diagnostic>,
}

impl NativeIoEnvelopeReport {
    /// Creates the CG-19 report-only foundation.
    #[must_use]
    pub fn cg19_foundation() -> Self {
        Self {
            schema_version: "shardloom.native_io_envelope.v1",
            report_id: "cg19.native-io-envelope".to_string(),
            status: NativeIoEnvelopeStatus::ReportOnlyPlanned,
            contract_surfaces: cg19_contract_surfaces(),
            representation_states: cg19_representation_states(),
            transition_examples: cg19_transition_examples(),
            certificate_path_requirements: cg19_certificate_path_requirements(),
            source_sink_coverage_rows: cg19_source_sink_coverage_rows(),
            per_path_certificate_required: true,
            aggregate_certificate_not_sufficient: true,
            preserve_encoded_or_foreign_encoded_when_possible: true,
            decoded_arrow_normalization_allowed: false,
            materialization_boundary_required_for_decoded_columnar: true,
            materialization_boundary_required_for_rows: true,
            source_pushdown_proof_required: true,
            sink_requirement_propagation_required: true,
            adapter_fidelity_report_required: true,
            runtime_execution: false,
            adapter_probe: false,
            data_read: false,
            data_decoded: false,
            data_materialized: false,
            row_read: false,
            arrow_converted: false,
            object_store_io: false,
            write_io: false,
            spill_io_performed: false,
            fallback_execution_allowed: false,
            fallback_attempted: false,
            production_claim_allowed: false,
            diagnostics: Vec::new(),
        }
    }

    /// Number of RFC 0031 contract surfaces.
    #[must_use]
    pub fn contract_count(&self) -> usize {
        self.contract_surfaces.len()
    }

    /// Number of representation state contracts.
    #[must_use]
    pub fn representation_state_count(&self) -> usize {
        self.representation_states.len()
    }

    /// Number of transition examples.
    #[must_use]
    pub fn transition_example_count(&self) -> usize {
        self.transition_examples.len()
    }

    /// Number of required per-path certificate entries.
    #[must_use]
    pub fn certificate_path_requirement_count(&self) -> usize {
        self.certificate_path_requirements.len()
    }

    /// Number of source/sink coverage rows.
    #[must_use]
    pub fn source_sink_coverage_row_count(&self) -> usize {
        self.source_sink_coverage_rows.len()
    }

    /// Number of source coverage rows.
    #[must_use]
    pub fn source_sink_coverage_source_count(&self) -> usize {
        self.source_sink_coverage_direction_count(NativeIoCoverageDirection::Source)
    }

    /// Number of sink coverage rows.
    #[must_use]
    pub fn source_sink_coverage_sink_count(&self) -> usize {
        self.source_sink_coverage_direction_count(NativeIoCoverageDirection::Sink)
    }

    fn source_sink_coverage_direction_count(&self, direction: NativeIoCoverageDirection) -> usize {
        self.source_sink_coverage_rows
            .iter()
            .filter(|row| row.direction == direction)
            .count()
    }

    /// Number of source/sink rows that still need diagnostics and future evidence.
    #[must_use]
    pub fn source_sink_coverage_unadmitted_row_count(&self) -> usize {
        self.source_sink_coverage_rows
            .iter()
            .filter(|row| row.requires_unsupported_diagnostic())
            .count()
    }

    /// Number of unadmitted rows that already carry deterministic diagnostics.
    #[must_use]
    pub fn source_sink_coverage_unadmitted_rows_with_diagnostics_count(&self) -> usize {
        self.source_sink_coverage_rows
            .iter()
            .filter(|row| row.requires_unsupported_diagnostic() && row.has_unsupported_diagnostic())
            .count()
    }

    /// Number of unadmitted rows missing deterministic diagnostics.
    #[must_use]
    pub fn source_sink_coverage_unadmitted_rows_missing_diagnostics_count(&self) -> usize {
        self.source_sink_coverage_unadmitted_row_count()
            .saturating_sub(self.source_sink_coverage_unadmitted_rows_with_diagnostics_count())
    }

    /// Stable comma-separated contract-kind order for CLI reporting.
    #[must_use]
    pub fn contract_kind_order(&self) -> String {
        self.contract_surfaces
            .iter()
            .map(|surface| surface.kind.as_str())
            .collect::<Vec<_>>()
            .join(",")
    }

    /// Stable comma-separated representation state order for CLI reporting.
    #[must_use]
    pub fn representation_state_order(&self) -> String {
        self.representation_states
            .iter()
            .map(|contract| contract.state.as_str())
            .collect::<Vec<_>>()
            .join(",")
    }

    /// Stable comma-separated transition example order for CLI reporting.
    #[must_use]
    pub fn transition_example_order(&self) -> String {
        self.transition_examples
            .iter()
            .map(NativeIoTransitionExample::transition_label)
            .collect::<Vec<_>>()
            .join(",")
    }

    /// Stable comma-separated certificate path order for CLI reporting.
    #[must_use]
    pub fn certificate_path_order(&self) -> String {
        self.certificate_path_requirements
            .iter()
            .map(|requirement| requirement.path_id.as_str())
            .collect::<Vec<_>>()
            .join(",")
    }

    /// Stable comma-separated source/sink coverage row order.
    #[must_use]
    pub fn source_sink_coverage_row_order(&self) -> String {
        self.source_sink_coverage_rows
            .iter()
            .map(|row| row.id)
            .collect::<Vec<_>>()
            .join(",")
    }

    /// Returns whether every coverage row preserves no-fallback status.
    #[must_use]
    pub fn source_sink_coverage_all_rows_fallback_attempted_false(&self) -> bool {
        self.source_sink_coverage_rows
            .iter()
            .all(|row| !row.fallback_attempted)
    }

    /// Returns whether every coverage row avoids external engines.
    #[must_use]
    pub fn source_sink_coverage_all_rows_external_engine_invoked_false(&self) -> bool {
        self.source_sink_coverage_rows
            .iter()
            .all(|row| !row.external_engine_invoked)
    }

    /// Returns whether all unadmitted rows carry deterministic diagnostics.
    #[must_use]
    pub fn source_sink_coverage_all_unadmitted_rows_have_diagnostics(&self) -> bool {
        self.source_sink_coverage_unadmitted_rows_missing_diagnostics_count() == 0
    }

    /// Returns whether the report avoids all execution and I/O side effects.
    #[must_use]
    pub const fn is_side_effect_free(&self) -> bool {
        !self.runtime_execution
            && !self.adapter_probe
            && !self.data_read
            && !self.data_decoded
            && !self.data_materialized
            && !self.row_read
            && !self.arrow_converted
            && !self.object_store_io
            && !self.write_io
            && !self.spill_io_performed
            && !self.fallback_execution_allowed
            && !self.fallback_attempted
    }

    /// Returns whether the report contains errors.
    #[must_use]
    pub fn has_errors(&self) -> bool {
        self.status.is_error()
            || !self.is_side_effect_free()
            || self.production_claim_allowed
            || !self.per_path_certificate_required
            || !self.aggregate_certificate_not_sufficient
            || !self.preserve_encoded_or_foreign_encoded_when_possible
            || self.decoded_arrow_normalization_allowed
            || !self.materialization_boundary_required_for_decoded_columnar
            || !self.materialization_boundary_required_for_rows
            || !self.source_pushdown_proof_required
            || !self.sink_requirement_propagation_required
            || !self.adapter_fidelity_report_required
            || self
                .contract_surfaces
                .iter()
                .any(NativeIoContractSurface::has_errors)
            || self
                .certificate_path_requirements
                .iter()
                .any(NativeIoCertificatePathRequirement::has_errors)
            || self
                .source_sink_coverage_rows
                .iter()
                .any(NativeIoSourceSinkCoverageRow::has_errors)
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
            "native I/O envelope plan\nschema_version: {}\nreport: {}\nstatus: {}\ncontract surfaces: {}\nrepresentation states: {}\ntransition examples: {}\ncertificate paths: {}\nsource/sink coverage rows: {}\nper-path certificates: required\naggregate certificate only: insufficient\ndecoded Arrow normalization: disabled\nfallback execution: disabled",
            self.schema_version,
            self.report_id,
            self.status.as_str(),
            self.contract_count(),
            self.representation_state_count(),
            self.transition_example_count(),
            self.certificate_path_requirement_count(),
            self.source_sink_coverage_row_count(),
        )
    }
}

#[allow(clippy::too_many_lines)]
fn cg19_contract_surfaces() -> Vec<NativeIoContractSurface> {
    vec![
        NativeIoContractSurface::planned(
            "native_io.native_work_envelope",
            NativeIoContractKind::NativeWorkEnvelope,
            vec![
                "envelope_id",
                "source_ref",
                "schema_ref",
                "representation_state",
                "statistics_ref",
                "selection_vector_ref",
                "pushdown_proof_ref",
                "materialization_boundary_ref",
                "ordering",
                "partitioning",
                "semantic_profile",
                "diagnostics",
                "fallback_attempted",
            ],
        ),
        NativeIoContractSurface::planned(
            "native_io.native_work_stream",
            NativeIoContractKind::NativeWorkStream,
            vec![
                "stream_id",
                "source_capability_report",
                "envelopes",
                "backpressure_policy",
                "streaming_mode",
                "task_granularity_policy",
                "diagnostics",
            ],
        ),
        NativeIoContractSurface::planned(
            "native_io.native_result_stream",
            NativeIoContractKind::NativeResultStream,
            vec![
                "stream_id",
                "sink_requirement_report",
                "result_envelopes",
                "materialization_boundary_report",
                "native_io_certificates",
                "native_io_certificate_summary",
                "diagnostics",
            ],
        ),
        NativeIoContractSurface::planned(
            "native_io.source_capability_report",
            NativeIoContractKind::SourceCapabilityReport,
            vec![
                "source_kind",
                "adapter_id",
                "schema_discovery_status",
                "statistics_availability",
                "pushdown_capabilities",
                "encoded_representation_preserved",
                "range_read_capability",
                "streaming_capability",
                "object_store_capability",
                "fallback_attempted",
            ],
        ),
        NativeIoContractSurface::planned(
            "native_io.source_pushdown_report",
            NativeIoContractKind::SourcePushdownReport,
            vec![
                "accepted_operations",
                "rejected_operations",
                "guarantee",
                "proof_basis",
                "residual_expression",
                "conservative_false_positive_policy",
                "unsafe_rejected_reason",
                "fallback_attempted",
            ],
        ),
        NativeIoContractSurface::planned(
            "native_io.sink_requirement_report",
            NativeIoContractKind::SinkRequirementReport,
            vec![
                "target_format",
                "accepts_encoded",
                "requires_decoded_columnar",
                "requires_rows",
                "preserves_metadata",
                "requires_ordering",
                "requires_partitioning",
                "requires_commit",
                "supports_streaming",
                "max_chunk_size",
                "backpressure_policy",
            ],
        ),
        NativeIoContractSurface::planned(
            "native_io.adapter_fidelity_report",
            NativeIoContractKind::AdapterFidelityReport,
            vec![
                "adapter_id",
                "source_kind",
                "sink_kind",
                "metadata_preserved",
                "statistics_preserved",
                "encoded_representation_preserved",
                "materialization_required",
                "fidelity_loss",
                "metadata_loss",
                "fallback_attempted",
            ],
        ),
        NativeIoContractSurface::planned(
            "native_io.materialization_boundary_report",
            NativeIoContractKind::MaterializationBoundaryReport,
            vec![
                "boundary_id",
                "from_state",
                "to_state",
                "required_by",
                "reason",
                "bytes_decoded",
                "rows_materialized",
                "fidelity_loss",
                "fallback_attempted",
            ],
        ),
        NativeIoContractSurface::planned(
            "native_io.native_io_certificate",
            NativeIoContractKind::NativeIoCertificate,
            vec![
                "certificate_id",
                "source_capability_report",
                "source_pushdown_report",
                "representation_transitions",
                "sink_requirement_report",
                "adapter_fidelity_report",
                "materialization_boundaries",
                "side_effects",
                "diagnostics",
                "fallback_attempted",
            ],
        ),
    ]
}

#[allow(clippy::too_many_lines)]
fn cg19_representation_states() -> Vec<RepresentationStateContract> {
    vec![
        RepresentationStateContract {
            state: RepresentationState::MetadataOnly,
            meaning: "planner can answer from metadata or route metadata pseudo-work without reading value buffers",
            allowed_transitions: vec![
                RepresentationState::Pruned,
                RepresentationState::VortexEncoded,
                RepresentationState::ForeignEncoded,
                RepresentationState::Unsupported,
            ],
            forbidden_assumptions: vec![
                "value buffers are present",
                "row data has been materialized",
            ],
            implies_decode: false,
            implies_row_materialization: false,
            can_remain_encoded: true,
            unsupported_terminal: false,
        },
        RepresentationStateContract {
            state: RepresentationState::Pruned,
            meaning: "source work was eliminated by metadata, statistics, or pushdown proof",
            allowed_transitions: vec![RepresentationState::Unsupported],
            forbidden_assumptions: vec![
                "a skipped chunk produced rows",
                "pruning implies exact predicate execution",
            ],
            implies_decode: false,
            implies_row_materialization: false,
            can_remain_encoded: true,
            unsupported_terminal: false,
        },
        RepresentationStateContract {
            state: RepresentationState::VortexEncoded,
            meaning: "work remains in a ShardLoom-native Vortex encoded representation",
            allowed_transitions: vec![
                RepresentationState::SelectionVectorEncoded,
                RepresentationState::PartiallyDecoded,
                RepresentationState::DecodedColumnar,
                RepresentationState::Unsupported,
            ],
            forbidden_assumptions: vec![
                "Arrow conversion is free",
                "encoded execution has already occurred",
            ],
            implies_decode: false,
            implies_row_materialization: false,
            can_remain_encoded: true,
            unsupported_terminal: false,
        },
        RepresentationStateContract {
            state: RepresentationState::ForeignEncoded,
            meaning: "work preserves useful physical encoding from a compatibility source",
            allowed_transitions: vec![
                RepresentationState::SelectionVectorEncoded,
                RepresentationState::PartiallyDecoded,
                RepresentationState::DecodedColumnar,
                RepresentationState::Unsupported,
            ],
            forbidden_assumptions: vec![
                "foreign encoding is equivalent to Vortex encoding",
                "decode to Arrow is the default bridge",
            ],
            implies_decode: false,
            implies_row_materialization: false,
            can_remain_encoded: true,
            unsupported_terminal: false,
        },
        RepresentationStateContract {
            state: RepresentationState::SelectionVectorEncoded,
            meaning: "encoded data is filtered or narrowed by a selection vector without row materialization",
            allowed_transitions: vec![
                RepresentationState::PartiallyDecoded,
                RepresentationState::DecodedColumnar,
                RepresentationState::Unsupported,
            ],
            forbidden_assumptions: vec![
                "selected rows have been materialized",
                "selection vector implies decoded values",
            ],
            implies_decode: false,
            implies_row_materialization: false,
            can_remain_encoded: true,
            unsupported_terminal: false,
        },
        RepresentationStateContract {
            state: RepresentationState::PartiallyDecoded,
            meaning: "only the required fields, chunks, or dictionary values have been decoded",
            allowed_transitions: vec![
                RepresentationState::DecodedColumnar,
                RepresentationState::MaterializedRows,
                RepresentationState::Unsupported,
            ],
            forbidden_assumptions: vec!["all columns are decoded", "rows have been materialized"],
            implies_decode: true,
            implies_row_materialization: false,
            can_remain_encoded: false,
            unsupported_terminal: false,
        },
        RepresentationStateContract {
            state: RepresentationState::DecodedColumnar,
            meaning: "values are decoded into columnar buffers at an explicit materialization boundary",
            allowed_transitions: vec![
                RepresentationState::VortexEncoded,
                RepresentationState::MaterializedRows,
                RepresentationState::ExternalEffect,
                RepresentationState::Unsupported,
            ],
            forbidden_assumptions: vec![
                "decoded columnar is the universal internal representation",
                "metadata and fidelity loss are irrelevant",
            ],
            implies_decode: true,
            implies_row_materialization: false,
            can_remain_encoded: false,
            unsupported_terminal: false,
        },
        RepresentationStateContract {
            state: RepresentationState::MaterializedRows,
            meaning: "values have crossed a row materialization boundary",
            allowed_transitions: vec![
                RepresentationState::ExternalEffect,
                RepresentationState::Unsupported,
            ],
            forbidden_assumptions: vec![
                "row materialization preserves encoded fidelity",
                "row output can count as encoded-native execution",
            ],
            implies_decode: true,
            implies_row_materialization: true,
            can_remain_encoded: false,
            unsupported_terminal: false,
        },
        RepresentationStateContract {
            state: RepresentationState::ExternalEffect,
            meaning: "work crosses an explicitly enabled effectful source, sink, UDF, API, or service boundary",
            allowed_transitions: vec![RepresentationState::Unsupported],
            forbidden_assumptions: vec![
                "external effects are safe by default",
                "external execution is fallback execution",
            ],
            implies_decode: false,
            implies_row_materialization: false,
            can_remain_encoded: false,
            unsupported_terminal: false,
        },
        RepresentationStateContract {
            state: RepresentationState::Unsupported,
            meaning: "capability proof failed and the path must stop with deterministic diagnostics",
            allowed_transitions: Vec::new(),
            forbidden_assumptions: vec![
                "unsupported paths may be delegated",
                "unsupported paths may silently decode",
            ],
            implies_decode: false,
            implies_row_materialization: false,
            can_remain_encoded: false,
            unsupported_terminal: true,
        },
    ]
}

fn cg19_transition_examples() -> Vec<NativeIoTransitionExample> {
    vec![
        NativeIoTransitionExample::new(
            RepresentationState::MetadataOnly,
            RepresentationState::Pruned,
            false,
        ),
        NativeIoTransitionExample::new(
            RepresentationState::VortexEncoded,
            RepresentationState::SelectionVectorEncoded,
            false,
        ),
        NativeIoTransitionExample::new(
            RepresentationState::ForeignEncoded,
            RepresentationState::PartiallyDecoded,
            true,
        ),
        NativeIoTransitionExample::new(
            RepresentationState::PartiallyDecoded,
            RepresentationState::DecodedColumnar,
            true,
        ),
        NativeIoTransitionExample::new(
            RepresentationState::DecodedColumnar,
            RepresentationState::MaterializedRows,
            true,
        ),
        NativeIoTransitionExample::unsupported_on_capability_failure(),
    ]
}

fn cg19_certificate_path_requirements() -> Vec<NativeIoCertificatePathRequirement> {
    vec![
        NativeIoCertificatePathRequirement::required("native_vortex_source_to_native_vortex_sink"),
        NativeIoCertificatePathRequirement::required("compatibility_source_to_native_vortex_sink"),
        NativeIoCertificatePathRequirement::required("multi_source_to_compatibility_sink"),
    ]
}

#[allow(clippy::too_many_lines)]
fn cg19_source_sink_coverage_rows() -> Vec<NativeIoSourceSinkCoverageRow> {
    vec![
        NativeIoSourceSinkCoverageRow {
            id: "local_vortex_file_scan",
            direction: NativeIoCoverageDirection::Source,
            family: "vortex_native_source",
            surface: "local_vortex_file_scan",
            support_status: "fixture_certified",
            support_basis: "local fixture count lane admitted through compute-capability-matrix",
            execution_modes: "native_vortex",
            native_io_certificate_refs: "certificates/cg19/local-vortex-count/native-io.json",
            certificate_status: "certified_for_fixture_path",
            unsupported_diagnostic_code: "none",
            blocker_id: "none",
            required_future_evidence: "claim_grade_benchmark_rows,broad_source_sink_operator_coverage",
            claim_gate_status: "fixture_smoke_only",
            claim_boundary: "local_fixture_scan_count_only_not_universal_source_support",
            source_refs: "docs/architecture/vortex-public-api-inventory.md,docs/architecture/global-architecture-review.md",
            runtime_execution: false,
            data_read: false,
            write_io: false,
            object_store_io: false,
            catalog_probe: false,
            network_probe: false,
            external_effects_executed: false,
            fallback_attempted: false,
            external_engine_invoked: false,
        },
        NativeIoSourceSinkCoverageRow {
            id: "compatibility_local_file_import_source",
            direction: NativeIoCoverageDirection::Source,
            family: "compatibility_source_adapter",
            surface: "csv_jsonl_parquet_arrow_avro_orc_local_import",
            support_status: "executable_uncertified",
            support_basis: "traditional analytics rows stage compatibility files into local Vortex artifacts",
            execution_modes: "compatibility_import_certified",
            native_io_certificate_refs: "native_io_certificate_required_after_native_vortex_stage",
            certificate_status: "required_per_path",
            unsupported_diagnostic_code: "none",
            blocker_id: "p74.compute.compatibility_import.certification_incomplete",
            required_future_evidence: "adapter_fidelity_report,native_io_certificate,benchmark_row",
            claim_gate_status: "not_claim_grade",
            claim_boundary: "compatibility_import_smoke_not_general_adapter_claim",
            source_refs: "docs/architecture/universal-input-contract.md,benchmarks/traditional_analytics/README.md",
            runtime_execution: false,
            data_read: false,
            write_io: false,
            object_store_io: false,
            catalog_probe: false,
            network_probe: false,
            external_effects_executed: false,
            fallback_attempted: false,
            external_engine_invoked: false,
        },
        NativeIoSourceSinkCoverageRow {
            id: "object_store_range_read_source",
            direction: NativeIoCoverageDirection::Source,
            family: "object_store_source",
            surface: "object_store_range_read",
            support_status: "unsupported",
            support_basis: "range-read/object-store runtime remains blocked",
            execution_modes: "auto,native_vortex,prepared_vortex",
            native_io_certificate_refs: "none",
            certificate_status: "missing_required",
            unsupported_diagnostic_code: "SL_UNSUPPORTED_NATIVE_OBJECT_STORE_SOURCE",
            blocker_id: "gar0002.native.source.object_store_range",
            required_future_evidence: "object_store_request_planner,range_read_certificate,native_io_certificate",
            claim_gate_status: "not_claim_grade",
            claim_boundary: "no_object_store_source_claim",
            source_refs: "docs/architecture/object-store-request-planner.md,docs/architecture/vortex-upstream-alignment-hardening.md",
            runtime_execution: false,
            data_read: false,
            write_io: false,
            object_store_io: false,
            catalog_probe: false,
            network_probe: false,
            external_effects_executed: false,
            fallback_attempted: false,
            external_engine_invoked: false,
        },
        NativeIoSourceSinkCoverageRow {
            id: "table_catalog_snapshot_source",
            direction: NativeIoCoverageDirection::Source,
            family: "table_catalog_source",
            surface: "table_catalog_snapshot_read",
            support_status: "unsupported",
            support_basis: "table/catalog metadata reads remain blocked",
            execution_modes: "auto,native_vortex,prepared_vortex",
            native_io_certificate_refs: "none",
            certificate_status: "missing_required",
            unsupported_diagnostic_code: "SL_UNSUPPORTED_NATIVE_TABLE_CATALOG_SOURCE",
            blocker_id: "gar0002.native.source.table_catalog",
            required_future_evidence: "table_catalog_metadata_read,namespace_policy,native_io_certificate",
            claim_gate_status: "not_claim_grade",
            claim_boundary: "no_table_catalog_source_claim",
            source_refs: "docs/architecture/table-intelligence-layer.md,docs/architecture/universal-input-contract.md",
            runtime_execution: false,
            data_read: false,
            write_io: false,
            object_store_io: false,
            catalog_probe: false,
            network_probe: false,
            external_effects_executed: false,
            fallback_attempted: false,
            external_engine_invoked: false,
        },
        NativeIoSourceSinkCoverageRow {
            id: "streaming_event_source",
            direction: NativeIoCoverageDirection::Source,
            family: "streaming_source",
            surface: "streaming_event_source",
            support_status: "unsupported",
            support_basis: "live/hybrid streaming source runtime remains blocked",
            execution_modes: "live,hybrid",
            native_io_certificate_refs: "none",
            certificate_status: "missing_required",
            unsupported_diagnostic_code: "SL_UNSUPPORTED_NATIVE_STREAM_SOURCE",
            blocker_id: "gar0002.native.source.streaming_events",
            required_future_evidence: "boundedness_policy,checkpoint_contract,execution_certificate",
            claim_gate_status: "not_claim_grade",
            claim_boundary: "no_streaming_source_claim",
            source_refs: "docs/architecture/dynamic-work-shaping.md,docs/architecture/operational-evidence-policy-hardening.md",
            runtime_execution: false,
            data_read: false,
            write_io: false,
            object_store_io: false,
            catalog_probe: false,
            network_probe: false,
            external_effects_executed: false,
            fallback_attempted: false,
            external_engine_invoked: false,
        },
        NativeIoSourceSinkCoverageRow {
            id: "unstructured_media_source",
            direction: NativeIoCoverageDirection::Source,
            family: "unstructured_media_source",
            surface: "unstructured_document_media_source",
            support_status: "unsupported",
            support_basis: "media/document/vector ingestion is report-only",
            execution_modes: "auto",
            native_io_certificate_refs: "none",
            certificate_status: "missing_required",
            unsupported_diagnostic_code: "SL_UNSUPPORTED_NATIVE_UNSTRUCTURED_MEDIA_SOURCE",
            blocker_id: "gar0002.native.source.unstructured_media",
            required_future_evidence: "media_decoder_policy,materialization_boundary,semantic_fixture",
            claim_gate_status: "not_claim_grade",
            claim_boundary: "no_unstructured_media_source_claim",
            source_refs: "docs/architecture/global-architecture-review.md,docs/architecture/operational-evidence-policy-hardening.md",
            runtime_execution: false,
            data_read: false,
            write_io: false,
            object_store_io: false,
            catalog_probe: false,
            network_probe: false,
            external_effects_executed: false,
            fallback_attempted: false,
            external_engine_invoked: false,
        },
        NativeIoSourceSinkCoverageRow {
            id: "external_adapter_source",
            direction: NativeIoCoverageDirection::Source,
            family: "external_adapter_source",
            surface: "external_database_saas_api_source",
            support_status: "unsupported",
            support_basis: "external source adapters are governed handles only until effect and credential evidence exists",
            execution_modes: "auto,live,hybrid",
            native_io_certificate_refs: "none",
            certificate_status: "missing_required",
            unsupported_diagnostic_code: "SL_UNSUPPORTED_NATIVE_EXTERNAL_ADAPTER_SOURCE",
            blocker_id: "gar0031.native.source.external_adapter",
            required_future_evidence: "credential_policy,effect_budget,adapter_fidelity_report,native_io_certificate",
            claim_gate_status: "not_claim_grade",
            claim_boundary: "no_external_adapter_source_claim",
            source_refs: "docs/rfcs/0031-universal-native-io-envelope.md,docs/rfcs/0033-user-data-workflow-etl-surface.md",
            runtime_execution: false,
            data_read: false,
            write_io: false,
            object_store_io: false,
            catalog_probe: false,
            network_probe: false,
            external_effects_executed: false,
            fallback_attempted: false,
            external_engine_invoked: false,
        },
        NativeIoSourceSinkCoverageRow {
            id: "typed_scalar_result_sink",
            direction: NativeIoCoverageDirection::Sink,
            family: "result_sink",
            surface: "typed_scalar_result",
            support_status: "fixture_certified",
            support_basis: "local fixture count lane emits typed scalar result evidence",
            execution_modes: "native_vortex",
            native_io_certificate_refs: "certificates/cg19/local-vortex-count/native-io.json",
            certificate_status: "certified_for_fixture_path",
            unsupported_diagnostic_code: "none",
            blocker_id: "none",
            required_future_evidence: "broad_result_sink_replay,claim_grade_benchmark_rows",
            claim_gate_status: "fixture_smoke_only",
            claim_boundary: "local_scalar_count_result_only_not_general_sink_support",
            source_refs: "docs/architecture/compute-engine-flow-reference.md,docs/architecture/global-architecture-review.md",
            runtime_execution: false,
            data_read: false,
            write_io: false,
            object_store_io: false,
            catalog_probe: false,
            network_probe: false,
            external_effects_executed: false,
            fallback_attempted: false,
            external_engine_invoked: false,
        },
        NativeIoSourceSinkCoverageRow {
            id: "local_vortex_artifact_sink",
            direction: NativeIoCoverageDirection::Sink,
            family: "vortex_native_sink",
            surface: "local_vortex_artifact_write",
            support_status: "runtime_supported",
            support_basis: "feature-gated local write_vortex uses the admitted native Vortex output contract",
            execution_modes: "compatibility_import_certified,prepared_vortex,native_vortex",
            native_io_certificate_refs: "docs/architecture/v1-local-output-sink-scope.md#write_vortex",
            certificate_status: "local_output_sink_contract_admitted",
            unsupported_diagnostic_code: "none",
            blocker_id: "none",
            required_future_evidence: "object_store_commit_recovery_before_remote_sink_claim",
            claim_gate_status: "local_workflow_runtime_supported",
            claim_boundary: "local_vortex_artifact_write_only_not_object_store_or_table_commit",
            source_refs: "docs/architecture/v1-local-output-sink-scope.md,docs/architecture/vortex-public-api-inventory.md",
            runtime_execution: false,
            data_read: false,
            write_io: false,
            object_store_io: false,
            catalog_probe: false,
            network_probe: false,
            external_effects_executed: false,
            fallback_attempted: false,
            external_engine_invoked: false,
        },
        NativeIoSourceSinkCoverageRow {
            id: "compatibility_export_sink",
            direction: NativeIoCoverageDirection::Sink,
            family: "compatibility_export_sink",
            surface: "compatibility_export_writer",
            support_status: "local_compatibility_export_admitted",
            support_basis: "Vortex-derived local compatibility exports carry fidelity, materialization, and write-certificate evidence",
            execution_modes: "compatibility_import_certified,auto",
            native_io_certificate_refs: "docs/release/compatibility-output-translation-report-coverage.json",
            certificate_status: "local_translation_certificate_admitted",
            unsupported_diagnostic_code: "none",
            blocker_id: "none",
            required_future_evidence: "object_store_or_table_compatibility_export_evidence_before_remote_sink_claim",
            claim_gate_status: "local_workflow_runtime_supported",
            claim_boundary: "local_compatibility_translation_only_not_external_sink_execution",
            source_refs: "docs/architecture/v1-local-output-sink-scope.md,docs/release/compatibility-output-translation-report-coverage.json",
            runtime_execution: false,
            data_read: false,
            write_io: false,
            object_store_io: false,
            catalog_probe: false,
            network_probe: false,
            external_effects_executed: false,
            fallback_attempted: false,
            external_engine_invoked: false,
        },
        NativeIoSourceSinkCoverageRow {
            id: "object_store_write_sink",
            direction: NativeIoCoverageDirection::Sink,
            family: "object_store_sink",
            surface: "object_store_native_write",
            support_status: "unsupported",
            support_basis: "object-store write and commit runtime remains blocked",
            execution_modes: "auto,native_vortex,prepared_vortex",
            native_io_certificate_refs: "none",
            certificate_status: "missing_required",
            unsupported_diagnostic_code: "SL_UNSUPPORTED_NATIVE_OBJECT_STORE_SINK",
            blocker_id: "gar0002.native.sink.object_store_write",
            required_future_evidence: "object_store_commit_protocol,retry_checkpoint_evidence,native_io_certificate",
            claim_gate_status: "not_claim_grade",
            claim_boundary: "no_object_store_sink_claim",
            source_refs: "docs/architecture/object-store-request-planner.md,docs/architecture/operational-evidence-policy-hardening.md",
            runtime_execution: false,
            data_read: false,
            write_io: false,
            object_store_io: false,
            catalog_probe: false,
            network_probe: false,
            external_effects_executed: false,
            fallback_attempted: false,
            external_engine_invoked: false,
        },
        NativeIoSourceSinkCoverageRow {
            id: "table_catalog_commit_sink",
            direction: NativeIoCoverageDirection::Sink,
            family: "table_catalog_sink",
            surface: "table_catalog_commit",
            support_status: "unsupported",
            support_basis: "table/catalog commits remain blocked",
            execution_modes: "auto,native_vortex,prepared_vortex",
            native_io_certificate_refs: "none",
            certificate_status: "missing_required",
            unsupported_diagnostic_code: "SL_UNSUPPORTED_NATIVE_TABLE_COMMIT",
            blocker_id: "gar0002.native.sink.table_catalog_commit",
            required_future_evidence: "commit_protocol,manifest_finalization,delete_tombstone_semantics",
            claim_gate_status: "not_claim_grade",
            claim_boundary: "no_table_catalog_commit_claim",
            source_refs: "docs/architecture/table-intelligence-layer.md,docs/architecture/object-store-request-planner.md",
            runtime_execution: false,
            data_read: false,
            write_io: false,
            object_store_io: false,
            catalog_probe: false,
            network_probe: false,
            external_effects_executed: false,
            fallback_attempted: false,
            external_engine_invoked: false,
        },
        NativeIoSourceSinkCoverageRow {
            id: "streaming_event_sink",
            direction: NativeIoCoverageDirection::Sink,
            family: "streaming_sink",
            surface: "streaming_event_sink",
            support_status: "unsupported",
            support_basis: "live/hybrid streaming sinks remain blocked",
            execution_modes: "live,hybrid",
            native_io_certificate_refs: "none",
            certificate_status: "missing_required",
            unsupported_diagnostic_code: "SL_UNSUPPORTED_NATIVE_STREAM_SINK",
            blocker_id: "gar0002.native.sink.streaming_events",
            required_future_evidence: "delivery_semantics,checkpoint_recovery,effect_budget_policy",
            claim_gate_status: "not_claim_grade",
            claim_boundary: "no_streaming_sink_claim",
            source_refs: "docs/architecture/effect-budget-plan.md,docs/architecture/dynamic-work-shaping.md",
            runtime_execution: false,
            data_read: false,
            write_io: false,
            object_store_io: false,
            catalog_probe: false,
            network_probe: false,
            external_effects_executed: false,
            fallback_attempted: false,
            external_engine_invoked: false,
        },
        NativeIoSourceSinkCoverageRow {
            id: "external_adapter_sink",
            direction: NativeIoCoverageDirection::Sink,
            family: "external_adapter_sink",
            surface: "external_database_saas_api_sink",
            support_status: "unsupported",
            support_basis: "external writes require explicit effect, credential, and idempotency evidence",
            execution_modes: "auto,live,hybrid",
            native_io_certificate_refs: "none",
            certificate_status: "missing_required",
            unsupported_diagnostic_code: "SL_UNSUPPORTED_NATIVE_EXTERNAL_ADAPTER_SINK",
            blocker_id: "gar0031.native.sink.external_adapter",
            required_future_evidence: "credential_policy,effect_budget,idempotency_contract,native_io_certificate",
            claim_gate_status: "not_claim_grade",
            claim_boundary: "no_external_adapter_sink_claim",
            source_refs: "docs/rfcs/0031-universal-native-io-envelope.md,docs/rfcs/0033-user-data-workflow-etl-surface.md",
            runtime_execution: false,
            data_read: false,
            write_io: false,
            object_store_io: false,
            catalog_probe: false,
            network_probe: false,
            external_effects_executed: false,
            fallback_attempted: false,
            external_engine_invoked: false,
        },
    ]
}

/// Produces the CG-19 universal native I/O envelope report.
#[must_use]
pub fn plan_native_io_envelope() -> NativeIoEnvelopeReport {
    NativeIoEnvelopeReport::cg19_foundation()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn native_io_envelope_foundation_is_report_only() {
        let report = NativeIoEnvelopeReport::cg19_foundation();

        assert_eq!(report.status, NativeIoEnvelopeStatus::ReportOnlyPlanned);
        assert_eq!(report.contract_count(), 9);
        assert_eq!(report.representation_state_count(), 10);
        assert_eq!(report.transition_example_count(), 6);
        assert_eq!(report.certificate_path_requirement_count(), 3);
        assert_eq!(report.source_sink_coverage_row_count(), 14);
        assert_eq!(report.source_sink_coverage_source_count(), 7);
        assert_eq!(report.source_sink_coverage_sink_count(), 7);
        assert_eq!(
            report.contract_kind_order(),
            "native_work_envelope,native_work_stream,native_result_stream,source_capability_report,source_pushdown_report,sink_requirement_report,adapter_fidelity_report,materialization_boundary_report,native_io_certificate"
        );
        assert_eq!(
            report.representation_state_order(),
            "metadata_only,pruned,vortex_encoded,foreign_encoded,selection_vector_encoded,partially_decoded,decoded_columnar,materialized_rows,external_effect,unsupported"
        );
        assert_eq!(
            report.transition_example_order(),
            "metadata_only->pruned,vortex_encoded->selection_vector_encoded,foreign_encoded->partially_decoded,partially_decoded->decoded_columnar,decoded_columnar->materialized_rows,any->unsupported"
        );
        assert_eq!(
            report.certificate_path_order(),
            "native_vortex_source_to_native_vortex_sink,compatibility_source_to_native_vortex_sink,multi_source_to_compatibility_sink"
        );
        assert_eq!(
            report.source_sink_coverage_row_order(),
            "local_vortex_file_scan,compatibility_local_file_import_source,object_store_range_read_source,table_catalog_snapshot_source,streaming_event_source,unstructured_media_source,external_adapter_source,typed_scalar_result_sink,local_vortex_artifact_sink,compatibility_export_sink,object_store_write_sink,table_catalog_commit_sink,streaming_event_sink,external_adapter_sink"
        );
        assert_eq!(report.source_sink_coverage_unadmitted_row_count(), 9);
        assert_eq!(
            report.source_sink_coverage_unadmitted_rows_with_diagnostics_count(),
            9
        );
        assert_eq!(
            report.source_sink_coverage_unadmitted_rows_missing_diagnostics_count(),
            0
        );
        assert!(report.source_sink_coverage_all_rows_fallback_attempted_false());
        assert!(report.source_sink_coverage_all_rows_external_engine_invoked_false());
        assert!(report.source_sink_coverage_all_unadmitted_rows_have_diagnostics());
        assert!(report.per_path_certificate_required);
        assert!(report.aggregate_certificate_not_sufficient);
        assert!(report.preserve_encoded_or_foreign_encoded_when_possible);
        assert!(!report.decoded_arrow_normalization_allowed);
        assert!(report.materialization_boundary_required_for_decoded_columnar);
        assert!(report.materialization_boundary_required_for_rows);
        assert!(report.source_pushdown_proof_required);
        assert!(report.sink_requirement_propagation_required);
        assert!(report.adapter_fidelity_report_required);
        assert!(report.is_side_effect_free());
        assert!(!report.has_errors());
        assert!(!report.runtime_execution);
        assert!(!report.adapter_probe);
        assert!(!report.data_read);
        assert!(!report.data_decoded);
        assert!(!report.data_materialized);
        assert!(!report.row_read);
        assert!(!report.arrow_converted);
        assert!(!report.object_store_io);
        assert!(!report.write_io);
        assert!(!report.spill_io_performed);
        assert!(!report.fallback_execution_allowed);
        assert!(!report.fallback_attempted);
        assert!(!report.production_claim_allowed);
    }

    #[test]
    fn representation_state_contracts_preserve_encoded_boundaries() {
        let report = NativeIoEnvelopeReport::cg19_foundation();

        let vortex = report
            .representation_states
            .iter()
            .find(|contract| contract.state == RepresentationState::VortexEncoded)
            .expect("vortex encoded state");
        assert!(vortex.can_remain_encoded);
        assert!(!vortex.implies_decode);
        assert!(!vortex.implies_row_materialization);

        let foreign = report
            .representation_states
            .iter()
            .find(|contract| contract.state == RepresentationState::ForeignEncoded)
            .expect("foreign encoded state");
        assert!(foreign.can_remain_encoded);
        assert!(!foreign.implies_decode);
        assert!(
            foreign
                .forbidden_assumptions
                .contains(&"decode to Arrow is the default bridge")
        );

        let rows = report
            .representation_states
            .iter()
            .find(|contract| contract.state == RepresentationState::MaterializedRows)
            .expect("materialized rows state");
        assert!(rows.implies_decode);
        assert!(rows.implies_row_materialization);
        assert!(!rows.can_remain_encoded);
    }

    #[test]
    fn materializing_transition_examples_require_boundaries() {
        let report = NativeIoEnvelopeReport::cg19_foundation();

        for transition in report.transition_examples.iter().filter(|transition| {
            matches!(
                transition.to_state,
                RepresentationState::PartiallyDecoded
                    | RepresentationState::DecodedColumnar
                    | RepresentationState::MaterializedRows
            )
        }) {
            assert!(transition.requires_materialization_boundary);
        }

        let unsupported = report
            .transition_examples
            .iter()
            .find(|transition| transition.capability_failure_transition)
            .expect("capability failure transition");
        assert_eq!(unsupported.from_label(), "any");
        assert_eq!(unsupported.to_state, RepresentationState::Unsupported);
    }

    #[test]
    fn native_io_envelope_flags_side_effects_and_arrow_normalization_as_errors() {
        let mut report = NativeIoEnvelopeReport::cg19_foundation();
        report.data_decoded = true;
        assert!(report.has_errors());

        let mut report = NativeIoEnvelopeReport::cg19_foundation();
        report.decoded_arrow_normalization_allowed = true;
        assert!(report.has_errors());

        let mut report = NativeIoEnvelopeReport::cg19_foundation();
        report.production_claim_allowed = true;
        assert!(report.has_errors());
    }

    #[test]
    fn certificate_path_requirements_reject_fallback_attempts() {
        let mut requirement = NativeIoCertificatePathRequirement::required(
            "native_vortex_source_to_native_vortex_sink",
        );

        requirement.fallback_attempted = true;

        assert!(requirement.has_errors());
    }

    #[test]
    fn runtime_native_io_certificate_requires_materialization_boundary_evidence() {
        let mut certificate = sample_runtime_certificate();

        assert!(certificate.is_certified());
        assert_eq!(certificate.status(), "certified");
        assert_eq!(
            certificate.representation_transition_order(),
            "foreign_encoded->decoded_columnar,decoded_columnar->vortex_encoded"
        );
        assert_eq!(
            certificate.materialization_boundary_order(),
            "cg19.csv_to_vortex_source_parse"
        );
        assert!(certificate.materializing_transitions_have_boundaries());

        certificate.representation_transitions[0].materialization_boundary_reported = false;

        assert!(certificate.has_errors());
        assert_eq!(certificate.status(), "blocked");
        assert!(!certificate.materializing_transitions_have_boundaries());
    }

    #[test]
    fn runtime_native_io_certificate_rejects_missing_boundary_records() {
        let mut certificate = sample_runtime_certificate();
        certificate.materialization_boundaries.clear();

        assert!(certificate.has_errors());
        assert_eq!(certificate.status(), "blocked");
        assert!(!certificate.materializing_transitions_have_boundaries());
    }

    #[test]
    fn runtime_native_io_certificate_rejects_fallback_attempts() {
        let mut certificate = sample_runtime_certificate();
        certificate.side_effects.fallback_attempted = true;

        assert!(certificate.has_errors());
    }

    fn sample_runtime_certificate() -> NativeIoCertificate {
        NativeIoCertificate::new(
            "cg19.test.certificate",
            "compatibility_source_to_native_vortex_sink",
            NativeIoSourceCapabilityReport {
                source_kind: "csv".to_string(),
                adapter_id: "test.csv".to_string(),
                schema_discovery_status: "declared_schema_validated".to_string(),
                statistics_availability: "none".to_string(),
                pushdown_capabilities: "none".to_string(),
                encoded_representation_preserved: false,
                range_read_capability: false,
                streaming_capability: false,
                object_store_capability: false,
                fallback_attempted: false,
            },
            NativeIoSourcePushdownReport {
                accepted_operations: Vec::new(),
                rejected_operations: vec!["group by aggregation".to_string()],
                guarantee: "unsupported".to_string(),
                proof_basis: "test-only CSV parser".to_string(),
                residual_expression: Some("group by aggregation".to_string()),
                conservative_false_positive_policy: false,
                unsafe_rejected_reason: None,
                fallback_attempted: false,
            },
            vec![
                NativeIoRepresentationTransition::new(
                    RepresentationState::ForeignEncoded,
                    RepresentationState::DecodedColumnar,
                    true,
                ),
                NativeIoRepresentationTransition::new(
                    RepresentationState::DecodedColumnar,
                    RepresentationState::VortexEncoded,
                    false,
                ),
            ],
            NativeIoSinkRequirementReport {
                target_format: "vortex".to_string(),
                accepts_encoded: true,
                requires_decoded_columnar: false,
                requires_rows: false,
                preserves_metadata: true,
                requires_ordering: false,
                requires_partitioning: false,
                requires_commit: false,
                supports_streaming: false,
                max_chunk_size: Some(3),
                backpressure_policy: "not_applicable_local_smoke".to_string(),
            },
            NativeIoAdapterFidelityReport {
                adapter_id: "test.csv".to_string(),
                source_kind: "csv".to_string(),
                sink_kind: "vortex".to_string(),
                metadata_preserved: false,
                statistics_preserved: false,
                encoded_representation_preserved: false,
                materialization_required: true,
                fidelity_loss: "none_for_declared_schema".to_string(),
                metadata_loss: "csv_source_has_no_vortex_metadata".to_string(),
                fallback_attempted: false,
            },
            vec![NativeIoMaterializationBoundaryReport {
                boundary_id: "cg19.csv_to_vortex_source_parse".to_string(),
                from_state: RepresentationState::ForeignEncoded,
                to_state: RepresentationState::DecodedColumnar,
                required_by: "csv_to_vortex_import".to_string(),
                reason: "CSV parse boundary".to_string(),
                bytes_decoded: 128,
                rows_materialized: 3,
                fidelity_loss: "none_for_declared_schema".to_string(),
                fallback_attempted: false,
            }],
            NativeIoSideEffectReport {
                data_read: true,
                data_decoded: true,
                data_materialized: true,
                row_read: true,
                arrow_converted: false,
                object_store_io: false,
                write_io: true,
                spill_io_performed: false,
                external_effects_executed: false,
                fallback_attempted: false,
                fallback_execution_allowed: false,
            },
            Vec::new(),
        )
        .expect("sample runtime certificate")
    }
}
