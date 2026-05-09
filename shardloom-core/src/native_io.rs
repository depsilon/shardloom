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
        self.representation_transitions
            .iter()
            .filter(|transition| transition.requires_materialization_boundary())
            .all(|transition| transition.materialization_boundary_reported)
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
            "native I/O envelope plan\nschema_version: {}\nreport: {}\nstatus: {}\ncontract surfaces: {}\nrepresentation states: {}\ntransition examples: {}\ncertificate paths: {}\nper-path certificates: required\naggregate certificate only: insufficient\ndecoded Arrow normalization: disabled\nfallback execution: disabled",
            self.schema_version,
            self.report_id,
            self.status.as_str(),
            self.contract_count(),
            self.representation_state_count(),
            self.transition_example_count(),
            self.certificate_path_requirement_count(),
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
