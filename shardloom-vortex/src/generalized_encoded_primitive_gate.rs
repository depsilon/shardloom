use std::fmt::Write as _;

use shardloom_core::{
    Diagnostic, DiagnosticCategory, DiagnosticCode, DiagnosticSeverity, FallbackStatus,
};

const SCHEMA_VERSION: &str = "shardloom.vortex_generalized_encoded_primitive_gate.v1";
const REPORT_ID: &str = "vortex.cg2.generalized-encoded-primitive-gate";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VortexGeneralizedEncodedPrimitiveGateStatus {
    GeneralizedExecutionBlocked,
    ReadyForGeneralizedExecution,
    Unsupported,
}

impl VortexGeneralizedEncodedPrimitiveGateStatus {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::GeneralizedExecutionBlocked => "generalized_execution_blocked",
            Self::ReadyForGeneralizedExecution => "ready_for_generalized_execution",
            Self::Unsupported => "unsupported",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VortexGeneralizedEncodedPrimitiveKind {
    DirectCount,
    FilteredCount,
    Projection,
}

impl VortexGeneralizedEncodedPrimitiveKind {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::DirectCount => "direct_count",
            Self::FilteredCount => "filtered_count",
            Self::Projection => "projection",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VortexGeneralizedEncodedPrimitiveStatus {
    LocalCountAllOnly,
    LocalDirectCountEvidence,
    LocalFilterScanPushdownEvidence,
    PreparedEncodedFilterEvidence,
    SourceBackedPreparedEncodedFilterEvidence,
    LocalProjectionScanPushdownEvidence,
    PreparedEncodedProjectionEvidence,
    SourceBackedPreparedEncodedProjectionEvidence,
    MetadataProofOnly,
    ReadinessOnly,
    GeneralizedBlocked,
}

impl VortexGeneralizedEncodedPrimitiveStatus {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::LocalCountAllOnly => "local_count_all_only",
            Self::LocalDirectCountEvidence => "local_direct_count_evidence",
            Self::LocalFilterScanPushdownEvidence => "local_filter_scan_pushdown_evidence",
            Self::PreparedEncodedFilterEvidence => "prepared_encoded_filter_evidence",
            Self::SourceBackedPreparedEncodedFilterEvidence => {
                "source_backed_prepared_encoded_filter_evidence"
            }
            Self::LocalProjectionScanPushdownEvidence => "local_projection_scan_pushdown_evidence",
            Self::PreparedEncodedProjectionEvidence => "prepared_encoded_projection_evidence",
            Self::SourceBackedPreparedEncodedProjectionEvidence => {
                "source_backed_prepared_encoded_projection_evidence"
            }
            Self::MetadataProofOnly => "metadata_proof_only",
            Self::ReadinessOnly => "readiness_only",
            Self::GeneralizedBlocked => "generalized_blocked",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
#[allow(clippy::struct_excessive_bools)]
pub struct VortexGeneralizedEncodedPrimitiveGateEntry {
    pub primitive: VortexGeneralizedEncodedPrimitiveKind,
    pub status: VortexGeneralizedEncodedPrimitiveStatus,
    pub current_scope: String,
    pub current_evidence: Vec<String>,
    pub implementation_blockers: Vec<String>,
    pub required_next_evidence: Vec<String>,
    pub local_vortex_count_all_execution_supported: bool,
    pub local_filter_scan_pushdown_supported: bool,
    pub prepared_encoded_filter_execution_supported: bool,
    pub source_backed_prepared_encoded_filter_execution_supported: bool,
    pub local_projection_scan_pushdown_supported: bool,
    pub prepared_encoded_projection_execution_supported: bool,
    pub source_backed_prepared_encoded_projection_execution_supported: bool,
    pub metadata_proof_supported: bool,
    pub readiness_contract_supported: bool,
    pub generalized_execution_allowed: bool,
    pub requires_public_scan_or_read_start_path: bool,
    pub requires_encoded_predicate_path: bool,
    pub requires_encoded_projection_path: bool,
    pub requires_selection_vector_pipeline: bool,
    pub requires_native_io_certificate: bool,
    pub requires_execution_certificate: bool,
    pub requires_correctness_evidence: bool,
    pub requires_benchmark_evidence: bool,
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
}

impl VortexGeneralizedEncodedPrimitiveGateEntry {
    #[must_use]
    fn direct_count() -> Self {
        Self {
            primitive: VortexGeneralizedEncodedPrimitiveKind::DirectCount,
            status: VortexGeneralizedEncodedPrimitiveStatus::LocalDirectCountEvidence,
            current_scope:
                "feature-gated local file/file:// .vortex direct CountAll execution evidence"
                    .to_string(),
            current_evidence: vec![
                "vortex-encoded-count-approval-plan".to_string(),
                "vortex-count --execute-local-encoded-count".to_string(),
                "local CountAll target policy for certified fixture and non-fixture local Vortex targets"
                    .to_string(),
                "cg19.local_encoded_count.native_io".to_string(),
                "local_encoded_count.execution_certificate".to_string(),
                "local_encoded_count.physical_kernel_evidence".to_string(),
            ],
            implementation_blockers: vec![
                "non-local sources and object-store reads are not approved".to_string(),
                "cross-target correctness fixture families are not complete".to_string(),
                "claim-grade comparative benchmark evidence is not complete".to_string(),
            ],
            required_next_evidence: vec![
                "object-store and non-local Vortex source authorization".to_string(),
                "CG-5 correctness fixtures across empty, null-heavy, chunked, and generated local Vortex counts".to_string(),
                "CG-19 native I/O certificates for every widened source/sink path".to_string(),
                "CG-6 benchmark evidence before production or superiority claims".to_string(),
            ],
            local_vortex_count_all_execution_supported: true,
            local_filter_scan_pushdown_supported: false,
            prepared_encoded_filter_execution_supported: false,
            source_backed_prepared_encoded_filter_execution_supported: false,
            local_projection_scan_pushdown_supported: false,
            prepared_encoded_projection_execution_supported: false,
            source_backed_prepared_encoded_projection_execution_supported: false,
            metadata_proof_supported: true,
            readiness_contract_supported: true,
            generalized_execution_allowed: false,
            requires_public_scan_or_read_start_path: true,
            requires_encoded_predicate_path: false,
            requires_encoded_projection_path: false,
            requires_selection_vector_pipeline: false,
            requires_native_io_certificate: true,
            requires_execution_certificate: true,
            requires_correctness_evidence: true,
            requires_benchmark_evidence: true,
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
        }
    }

    #[must_use]
    fn filtered_count() -> Self {
        Self {
            primitive: VortexGeneralizedEncodedPrimitiveKind::FilteredCount,
            status: VortexGeneralizedEncodedPrimitiveStatus::SourceBackedPreparedEncodedFilterEvidence,
            current_scope:
                "local .vortex CountWhere/FilterPredicate scan-pushdown evidence plus prepared encoded-value filter execution and source-bound prepared batch evidence; reader-generated batches/adapters still blocked"
                    .to_string(),
            current_evidence: vec![
                "vortex-filtered-count-readiness-plan".to_string(),
                "filtered_count_metadata_proof_report".to_string(),
                "selection_vector_filter_kernel_discovery_report".to_string(),
                "execute_vortex_generalized_filter_from_local_scan_pushdown".to_string(),
                "execute_vortex_generalized_filter_from_encoded_value_batches".to_string(),
                "execute_vortex_source_backed_filter_from_encoded_value_batches".to_string(),
                "vortex-count-where --execute-local-primitive".to_string(),
                "vortex-filter --execute-local-primitive".to_string(),
                "cg19.local_primitive.count_where/filter_predicate.native_io".to_string(),
                "cg19.prepared_encoded_filter.native_io".to_string(),
                "source-backed prepared encoded filter URI/split envelope".to_string(),
            ],
            implementation_blockers: vec![
                "non-local sources and object-store reads are not approved".to_string(),
                "reader and adapter paths do not yet produce prepared encoded-value batches"
                    .to_string(),
                "claim-grade predicate null/type correctness and benchmark evidence is not complete"
                    .to_string(),
            ],
            required_next_evidence: vec![
                "reader/read-start wiring that produces prepared encoded-value batches without decode"
                    .to_string(),
                "selection-vector preservation through downstream operators beyond prepared filter evidence".to_string(),
                "decoded-reference comparison fixtures for test-only validation".to_string(),
                "CG-19 native I/O certificates for every widened source/sink filter path"
                    .to_string(),
            ],
            local_vortex_count_all_execution_supported: false,
            local_filter_scan_pushdown_supported: true,
            prepared_encoded_filter_execution_supported: true,
            source_backed_prepared_encoded_filter_execution_supported: true,
            local_projection_scan_pushdown_supported: false,
            prepared_encoded_projection_execution_supported: false,
            source_backed_prepared_encoded_projection_execution_supported: false,
            metadata_proof_supported: true,
            readiness_contract_supported: true,
            generalized_execution_allowed: false,
            requires_public_scan_or_read_start_path: true,
            requires_encoded_predicate_path: true,
            requires_encoded_projection_path: false,
            requires_selection_vector_pipeline: true,
            requires_native_io_certificate: true,
            requires_execution_certificate: true,
            requires_correctness_evidence: true,
            requires_benchmark_evidence: true,
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
        }
    }

    #[must_use]
    fn projection() -> Self {
        Self {
            primitive: VortexGeneralizedEncodedPrimitiveKind::Projection,
            status: VortexGeneralizedEncodedPrimitiveStatus::SourceBackedPreparedEncodedProjectionEvidence,
            current_scope:
                "local .vortex ProjectColumns/FilterAndProject scan-pushdown evidence plus prepared encoded projection/filter-project execution and source-bound prepared projection evidence; reader-generated batches/adapters still blocked"
                    .to_string(),
            current_evidence: vec![
                "vortex-projection-readiness-plan".to_string(),
                "encoded_projection_kernel_admission".to_string(),
                "vortex-encoded-path-selection-plan".to_string(),
                "execute_vortex_generalized_projection_from_local_scan_pushdown".to_string(),
                "evaluate_vortex_prepared_encoded_projection".to_string(),
                "execute_vortex_generalized_projection_from_encoded_projection_batches".to_string(),
                "execute_vortex_source_backed_projection_from_encoded_projection_batches"
                    .to_string(),
                "vortex-project --execute-local-primitive".to_string(),
                "vortex-filter-project --execute-local-primitive".to_string(),
                "cg19.local_primitive.project_columns/filter_and_project.native_io".to_string(),
                "cg19.prepared_encoded_projection.native_io".to_string(),
                "source-backed prepared encoded projection URI/split envelope".to_string(),
            ],
            implementation_blockers: vec![
                "reader and adapter paths do not yet produce prepared encoded projection batches"
                    .to_string(),
                "claim-grade projection null/nested correctness and benchmark evidence is not complete"
                    .to_string(),
            ],
            required_next_evidence: vec![
                "reader/read-start wiring that produces prepared encoded projection batches without decode"
                    .to_string(),
                "selection-vector preservation through downstream operators beyond prepared filter-project evidence".to_string(),
                "projection fixtures for empty, null-heavy, wide, and nested columns".to_string(),
                "Native I/O certificates for every widened source/sink projection path"
                    .to_string(),
                "benchmark evidence before production or superiority claims".to_string(),
            ],
            local_vortex_count_all_execution_supported: false,
            local_filter_scan_pushdown_supported: false,
            prepared_encoded_filter_execution_supported: false,
            source_backed_prepared_encoded_filter_execution_supported: false,
            local_projection_scan_pushdown_supported: true,
            prepared_encoded_projection_execution_supported: true,
            source_backed_prepared_encoded_projection_execution_supported: true,
            metadata_proof_supported: false,
            readiness_contract_supported: true,
            generalized_execution_allowed: false,
            requires_public_scan_or_read_start_path: true,
            requires_encoded_predicate_path: false,
            requires_encoded_projection_path: true,
            requires_selection_vector_pipeline: false,
            requires_native_io_certificate: true,
            requires_execution_certificate: true,
            requires_correctness_evidence: true,
            requires_benchmark_evidence: true,
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
        }
    }

    #[must_use]
    pub const fn is_side_effect_free(&self) -> bool {
        !self.data_read
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
}

#[derive(Debug, Clone, PartialEq, Eq)]
#[allow(clippy::struct_excessive_bools)]
pub struct VortexGeneralizedEncodedPrimitiveGateReport {
    pub schema_version: &'static str,
    pub report_id: String,
    pub status: VortexGeneralizedEncodedPrimitiveGateStatus,
    pub entries: Vec<VortexGeneralizedEncodedPrimitiveGateEntry>,
    pub local_count_all_only: bool,
    pub generalized_count_ready: bool,
    pub filtered_count_execution_ready: bool,
    pub projection_execution_ready: bool,
    pub requires_public_scan_or_read_start_path: bool,
    pub requires_encoded_predicate_path: bool,
    pub requires_encoded_projection_path: bool,
    pub requires_selection_vector_pipeline: bool,
    pub requires_native_io_certificate: bool,
    pub requires_execution_certificate: bool,
    pub requires_correctness_evidence: bool,
    pub requires_benchmark_evidence: bool,
    pub data_read: bool,
    pub data_decoded: bool,
    pub data_materialized: bool,
    pub row_read: bool,
    pub arrow_converted: bool,
    pub object_store_io: bool,
    pub write_io: bool,
    pub spill_io_performed: bool,
    pub runtime_execution_allowed: bool,
    pub external_engine_execution: bool,
    pub fallback_execution_allowed: bool,
    pub fallback_attempted: bool,
    pub production_claim_allowed: bool,
    pub diagnostics: Vec<Diagnostic>,
}

impl VortexGeneralizedEncodedPrimitiveGateReport {
    #[must_use]
    pub fn report_only() -> Self {
        let entries = vec![
            VortexGeneralizedEncodedPrimitiveGateEntry::direct_count(),
            VortexGeneralizedEncodedPrimitiveGateEntry::filtered_count(),
            VortexGeneralizedEncodedPrimitiveGateEntry::projection(),
        ];
        Self {
            schema_version: SCHEMA_VERSION,
            report_id: REPORT_ID.to_string(),
            status: VortexGeneralizedEncodedPrimitiveGateStatus::GeneralizedExecutionBlocked,
            entries,
            local_count_all_only: false,
            generalized_count_ready: false,
            filtered_count_execution_ready: false,
            projection_execution_ready: false,
            requires_public_scan_or_read_start_path: true,
            requires_encoded_predicate_path: true,
            requires_encoded_projection_path: true,
            requires_selection_vector_pipeline: true,
            requires_native_io_certificate: true,
            requires_execution_certificate: true,
            requires_correctness_evidence: true,
            requires_benchmark_evidence: true,
            data_read: false,
            data_decoded: false,
            data_materialized: false,
            row_read: false,
            arrow_converted: false,
            object_store_io: false,
            write_io: false,
            spill_io_performed: false,
            runtime_execution_allowed: false,
            external_engine_execution: false,
            fallback_execution_allowed: false,
            fallback_attempted: false,
            production_claim_allowed: false,
            diagnostics: vec![generalized_execution_blocked_diagnostic()],
        }
    }

    #[must_use]
    pub fn entry_count(&self) -> usize {
        self.entries.len()
    }

    #[must_use]
    pub fn primitive_order(&self) -> Vec<&'static str> {
        self.entries
            .iter()
            .map(|entry| entry.primitive.as_str())
            .collect()
    }

    #[must_use]
    pub fn primitive_statuses(&self) -> Vec<&'static str> {
        self.entries
            .iter()
            .map(|entry| entry.status.as_str())
            .collect()
    }

    #[must_use]
    pub fn implementation_blocker_count(&self) -> usize {
        self.entries
            .iter()
            .map(|entry| entry.implementation_blockers.len())
            .sum()
    }

    #[must_use]
    pub fn required_next_evidence_count(&self) -> usize {
        self.entries
            .iter()
            .map(|entry| entry.required_next_evidence.len())
            .sum()
    }

    #[must_use]
    pub fn entries_with_local_count_support(&self) -> usize {
        self.entries
            .iter()
            .filter(|entry| entry.local_vortex_count_all_execution_supported)
            .count()
    }

    #[must_use]
    pub fn entries_with_local_filter_scan_pushdown_support(&self) -> usize {
        self.entries
            .iter()
            .filter(|entry| entry.local_filter_scan_pushdown_supported)
            .count()
    }

    #[must_use]
    pub fn entries_with_prepared_encoded_filter_execution_support(&self) -> usize {
        self.entries
            .iter()
            .filter(|entry| entry.prepared_encoded_filter_execution_supported)
            .count()
    }

    #[must_use]
    pub fn entries_with_source_backed_prepared_encoded_filter_execution_support(&self) -> usize {
        self.entries
            .iter()
            .filter(|entry| entry.source_backed_prepared_encoded_filter_execution_supported)
            .count()
    }

    #[must_use]
    pub fn entries_with_local_projection_scan_pushdown_support(&self) -> usize {
        self.entries
            .iter()
            .filter(|entry| entry.local_projection_scan_pushdown_supported)
            .count()
    }

    #[must_use]
    pub fn entries_with_prepared_encoded_projection_execution_support(&self) -> usize {
        self.entries
            .iter()
            .filter(|entry| entry.prepared_encoded_projection_execution_supported)
            .count()
    }

    #[must_use]
    pub fn entries_with_source_backed_prepared_encoded_projection_execution_support(
        &self,
    ) -> usize {
        self.entries
            .iter()
            .filter(|entry| entry.source_backed_prepared_encoded_projection_execution_supported)
            .count()
    }

    #[must_use]
    pub fn entries_with_metadata_proof(&self) -> usize {
        self.entries
            .iter()
            .filter(|entry| entry.metadata_proof_supported)
            .count()
    }

    #[must_use]
    pub fn entries_with_readiness_contract(&self) -> usize {
        self.entries
            .iter()
            .filter(|entry| entry.readiness_contract_supported)
            .count()
    }

    #[must_use]
    pub const fn is_side_effect_free(&self) -> bool {
        !self.data_read
            && !self.data_decoded
            && !self.data_materialized
            && !self.row_read
            && !self.arrow_converted
            && !self.object_store_io
            && !self.write_io
            && !self.spill_io_performed
            && !self.runtime_execution_allowed
            && !self.external_engine_execution
            && !self.fallback_execution_allowed
            && !self.fallback_attempted
    }

    #[must_use]
    pub fn has_errors(&self) -> bool {
        !self.is_side_effect_free()
            || self.production_claim_allowed
            || self.diagnostics.iter().any(|d| {
                matches!(
                    d.severity,
                    DiagnosticSeverity::Error | DiagnosticSeverity::Fatal
                )
            })
    }

    #[must_use]
    pub fn to_human_text(&self) -> String {
        let mut out = String::new();
        let _ = writeln!(&mut out, "Vortex generalized encoded primitive gate");
        let _ = writeln!(&mut out, "schema_version: {}", self.schema_version);
        let _ = writeln!(&mut out, "report: {}", self.report_id);
        let _ = writeln!(&mut out, "status: {}", self.status.as_str());
        let _ = writeln!(&mut out, "primitive entries: {}", self.entry_count());
        let _ = writeln!(
            &mut out,
            "primitive order: {}",
            self.primitive_order().join(",")
        );
        let _ = writeln!(
            &mut out,
            "primitive statuses: {}",
            self.primitive_statuses().join(",")
        );
        let _ = writeln!(
            &mut out,
            "local CountAll only: {}",
            self.local_count_all_only
        );
        let _ = writeln!(
            &mut out,
            "implementation blockers: {}",
            self.implementation_blocker_count()
        );
        let _ = writeln!(
            &mut out,
            "required next evidence: {}",
            self.required_next_evidence_count()
        );
        let _ = writeln!(&mut out, "runtime execution: disabled");
        let _ = writeln!(&mut out, "fallback execution: disabled");
        out
    }
}

#[must_use]
pub fn plan_vortex_generalized_encoded_primitive_gate()
-> VortexGeneralizedEncodedPrimitiveGateReport {
    VortexGeneralizedEncodedPrimitiveGateReport::report_only()
}

fn generalized_execution_blocked_diagnostic() -> Diagnostic {
    Diagnostic::new(
        DiagnosticCode::NotImplemented,
        DiagnosticSeverity::Warning,
        DiagnosticCategory::Planning,
        "Generalized encoded primitive execution remains blocked.",
        Some("vortex.generalized_encoded_primitive_execution".to_string()),
        Some(
            "Local file/file:// `.vortex` CountAll, CountWhere, FilterPredicate, ProjectColumns, and FilterAndProject scan-pushdown paths have runtime and Native I/O evidence, prepared encoded-value filter/projection execution is available, and source-bound prepared batch envelopes exist, but reader-generated batches, non-local sources, adapters, correctness, and benchmark evidence still block generalized execution."
                .to_string(),
        ),
        Some(
            "Keep using the explicit local primitive paths or prepared/source-bound encoded filter/projection surfaces for proven execution and land reader-backed encoded batch production, correctness, benchmark, and source-widening evidence before broader runtime behavior."
                .to_string(),
        ),
        FallbackStatus::disabled_by_policy(),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn report_only_gate_names_current_primitive_states() {
        let report = plan_vortex_generalized_encoded_primitive_gate();

        assert_eq!(
            report.status,
            VortexGeneralizedEncodedPrimitiveGateStatus::GeneralizedExecutionBlocked
        );
        assert_eq!(report.entry_count(), 3);
        assert_eq!(
            report.primitive_order(),
            vec!["direct_count", "filtered_count", "projection"]
        );
        assert_eq!(
            report.primitive_statuses(),
            vec![
                "local_direct_count_evidence",
                "source_backed_prepared_encoded_filter_evidence",
                "source_backed_prepared_encoded_projection_evidence"
            ]
        );
        assert!(!report.local_count_all_only);
        assert_eq!(report.entries_with_local_count_support(), 1);
        assert_eq!(report.entries_with_local_filter_scan_pushdown_support(), 1);
        assert_eq!(
            report.entries_with_prepared_encoded_filter_execution_support(),
            1
        );
        assert_eq!(
            report.entries_with_source_backed_prepared_encoded_filter_execution_support(),
            1
        );
        assert_eq!(
            report.entries_with_local_projection_scan_pushdown_support(),
            1
        );
        assert_eq!(
            report.entries_with_prepared_encoded_projection_execution_support(),
            1
        );
        assert_eq!(
            report.entries_with_source_backed_prepared_encoded_projection_execution_support(),
            1
        );
        assert_eq!(report.entries_with_metadata_proof(), 2);
        assert_eq!(report.entries_with_readiness_contract(), 3);
    }

    #[test]
    fn gate_blocks_generalized_execution_without_erroring_the_report() {
        let report = plan_vortex_generalized_encoded_primitive_gate();

        assert!(!report.generalized_count_ready);
        assert!(!report.filtered_count_execution_ready);
        assert!(!report.projection_execution_ready);
        assert!(report.requires_public_scan_or_read_start_path);
        assert!(report.requires_encoded_predicate_path);
        assert!(report.requires_encoded_projection_path);
        assert!(report.requires_selection_vector_pipeline);
        assert!(report.requires_native_io_certificate);
        assert!(report.requires_execution_certificate);
        assert!(report.requires_correctness_evidence);
        assert!(report.requires_benchmark_evidence);
        assert!(!report.has_errors());
    }

    #[test]
    fn gate_is_report_only_and_no_fallback() {
        let report = plan_vortex_generalized_encoded_primitive_gate();

        assert!(report.is_side_effect_free());
        assert!(!report.data_read);
        assert!(!report.data_decoded);
        assert!(!report.data_materialized);
        assert!(!report.row_read);
        assert!(!report.arrow_converted);
        assert!(!report.object_store_io);
        assert!(!report.write_io);
        assert!(!report.spill_io_performed);
        assert!(!report.runtime_execution_allowed);
        assert!(!report.external_engine_execution);
        assert!(!report.fallback_execution_allowed);
        assert!(!report.fallback_attempted);
        assert!(!report.production_claim_allowed);
        assert!(report.diagnostics.iter().all(|d| !d.fallback.attempted));
    }
}
