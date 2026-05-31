//! Vortex Scan API compatibility report.
//!
//! This surface maps `ShardLoom` Native I/O concepts to upstream Vortex
//! Source/Sink/Split concepts without invoking Vortex integrations or treating
//! external query-engine residual evaluation as `ShardLoom` execution.

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VortexScanCompatibilityStatus {
    ReportOnly,
    Admitted,
    PartiallyAdmitted,
    Deferred,
    Rejected,
}

impl VortexScanCompatibilityStatus {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::ReportOnly => "report_only",
            Self::Admitted => "admitted",
            Self::PartiallyAdmitted => "partially_admitted",
            Self::Deferred => "deferred",
            Self::Rejected => "rejected",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VortexScanResidualExecutor {
    None,
    ShardLoomNative,
    UnsupportedBlocked,
    ExternalBaselineOnly,
    ProhibitedExternalFallback,
}

impl VortexScanResidualExecutor {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::None => "none",
            Self::ShardLoomNative => "shardloom_native",
            Self::UnsupportedBlocked => "unsupported_blocked",
            Self::ExternalBaselineOnly => "external_baseline_only",
            Self::ProhibitedExternalFallback => "prohibited_external_fallback",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VortexSourceSplitAdmissionStatus {
    FixtureSmokeOnly,
    GeneralizedRuntimeBlocked,
}

impl VortexSourceSplitAdmissionStatus {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::FixtureSmokeOnly => "fixture_smoke_only",
            Self::GeneralizedRuntimeBlocked => "blocked_until_source_split_certificate",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VortexSegmentExtractionAdmissionStatus {
    BlockedUntilSegmentExtractionCertificate,
}

impl VortexSegmentExtractionAdmissionStatus {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::BlockedUntilSegmentExtractionCertificate => {
                "blocked_until_segment_extraction_certificate"
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
#[allow(clippy::struct_excessive_bools)]
pub struct VortexSegmentExtractionAdmissionRow {
    pub layout_family: &'static str,
    pub encoding_family: &'static str,
    pub selected_layout_status: VortexSegmentExtractionAdmissionStatus,
    pub support_status: &'static str,
    pub upstream_concepts_checked: &'static str,
    pub shardloom_surface: &'static str,
    pub admission_decision: &'static str,
    pub materialization_boundary_status: &'static str,
    pub decode_boundary_status: &'static str,
    pub correctness_refs: &'static str,
    pub benchmark_refs: &'static str,
    pub execution_certificate_refs: &'static str,
    pub native_io_certificate_refs: &'static str,
    pub materialization_decode_refs: &'static str,
    pub policy_refs: &'static str,
    pub unsupported_diagnostic_code: &'static str,
    pub blocker_id: &'static str,
    pub required_future_evidence: &'static str,
    pub claim_gate_status: &'static str,
    pub claim_boundary: &'static str,
    pub runtime_execution: bool,
    pub data_read: bool,
    pub data_decoded: bool,
    pub data_materialized: bool,
    pub object_store_io: bool,
    pub table_catalog_io: bool,
    pub write_io: bool,
    pub external_engine_invoked: bool,
    pub fallback_attempted: bool,
}

impl VortexSegmentExtractionAdmissionRow {
    #[must_use]
    pub const fn sparse_patch_fill_blocked() -> Self {
        Self {
            layout_family: "sparse_patch_fill",
            encoding_family: "vortex_sparse",
            selected_layout_status:
                VortexSegmentExtractionAdmissionStatus::BlockedUntilSegmentExtractionCertificate,
            support_status: "unsupported",
            upstream_concepts_checked: "Vortex sparse layout patch/fill semantics, child array traversal, validity handling, and canonicalization risk",
            shardloom_surface: "vortex_segment_extraction_admission",
            admission_decision: "blocked_until_vortex_or_shardloom_evidence",
            materialization_boundary_status: "not_entered",
            decode_boundary_status: "not_entered",
            correctness_refs: "required_before_admission",
            benchmark_refs: "traditional_analytics.coverage_table",
            execution_certificate_refs: "not_emitted_blocked_until_sparse_segment_extraction",
            native_io_certificate_refs: "not_emitted_blocked_until_sparse_segment_extraction",
            materialization_decode_refs: "not_emitted_no_data_read_decode_or_materialization",
            policy_refs: "fallback_attempted=false,external_engine_invoked=false",
            unsupported_diagnostic_code: "SL_UNSUPPORTED_VORTEX_SPARSE_SEGMENT_EXTRACTION",
            blocker_id: "gar0003a.sparse_patch_fill_segment_extraction",
            required_future_evidence: "sparse_layout_semantics,validity_handling,correctness_fixture,execution_certificate,native_io_certificate,materialization_decode_certificate,no_fallback_evidence",
            claim_gate_status: "not_claim_grade",
            claim_boundary: "sparse_patch_fill_segment_extraction_blocked_no_runtime_claim",
            runtime_execution: false,
            data_read: false,
            data_decoded: false,
            data_materialized: false,
            object_store_io: false,
            table_catalog_io: false,
            write_io: false,
            external_engine_invoked: false,
            fallback_attempted: false,
        }
    }

    #[must_use]
    pub const fn is_blocked(&self) -> bool {
        matches!(
            self.selected_layout_status,
            VortexSegmentExtractionAdmissionStatus::BlockedUntilSegmentExtractionCertificate
        )
    }

    #[must_use]
    pub const fn fallback_free(&self) -> bool {
        !self.runtime_execution
            && !self.data_read
            && !self.data_decoded
            && !self.data_materialized
            && !self.object_store_io
            && !self.table_catalog_io
            && !self.write_io
            && !self.external_engine_invoked
            && !self.fallback_attempted
    }

    #[must_use]
    pub fn has_deterministic_blocker(&self) -> bool {
        !self.unsupported_diagnostic_code.is_empty()
            && !self.blocker_id.is_empty()
            && self.claim_gate_status == "not_claim_grade"
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
#[allow(clippy::struct_excessive_bools)]
pub struct VortexSegmentExtractionAdmissionReport {
    pub schema_version: &'static str,
    pub report_id: &'static str,
    pub selected_layout_family: &'static str,
    pub selected_layout_status: VortexSegmentExtractionAdmissionStatus,
    pub row_order: Vec<&'static str>,
    pub rows: Vec<VortexSegmentExtractionAdmissionRow>,
    pub required_evidence: &'static str,
    pub claim_gate_status: &'static str,
    pub claim_boundary: &'static str,
    pub runtime_execution: bool,
    pub data_read: bool,
    pub data_decoded: bool,
    pub data_materialized: bool,
    pub object_store_io: bool,
    pub table_catalog_io: bool,
    pub write_io: bool,
    pub external_engine_invoked: bool,
    pub fallback_attempted: bool,
}

impl VortexSegmentExtractionAdmissionReport {
    #[must_use]
    pub fn current() -> Self {
        let sparse_row = VortexSegmentExtractionAdmissionRow::sparse_patch_fill_blocked();
        Self {
            schema_version: "shardloom.vortex_segment_extraction_admission.v1",
            report_id: "gar0003a.vortex_segment_extraction.sparse_patch_fill",
            selected_layout_family: sparse_row.layout_family,
            selected_layout_status: sparse_row.selected_layout_status,
            row_order: vec![sparse_row.layout_family],
            rows: vec![sparse_row],
            required_evidence: "correctness_fixture,execution_certificate,native_io_certificate,materialization_decode_certificate,no_fallback_evidence",
            claim_gate_status: "not_claim_grade",
            claim_boundary: "one sparse_patch_fill layout family is deterministically blocked; no segment extraction runtime or production layout coverage claim is allowed",
            runtime_execution: false,
            data_read: false,
            data_decoded: false,
            data_materialized: false,
            object_store_io: false,
            table_catalog_io: false,
            write_io: false,
            external_engine_invoked: false,
            fallback_attempted: false,
        }
    }

    #[must_use]
    pub fn supported_layout_count(&self) -> usize {
        self.rows
            .iter()
            .filter(|row| !row.is_blocked() && row.support_status == "supported")
            .count()
    }

    #[must_use]
    pub fn blocked_layout_count(&self) -> usize {
        self.rows.iter().filter(|row| row.is_blocked()).count()
    }

    #[must_use]
    pub fn unsupported_diagnostic_codes(&self) -> Vec<&'static str> {
        self.rows
            .iter()
            .filter(|row| row.is_blocked())
            .map(|row| row.unsupported_diagnostic_code)
            .collect()
    }

    #[must_use]
    pub fn blocker_ids(&self) -> Vec<&'static str> {
        self.rows.iter().map(|row| row.blocker_id).collect()
    }

    #[must_use]
    pub fn required_future_evidence(&self) -> Vec<&'static str> {
        self.rows
            .iter()
            .map(|row| row.required_future_evidence)
            .collect()
    }

    #[must_use]
    pub fn selected_layout_classified(&self) -> bool {
        self.rows.iter().any(|row| {
            row.layout_family == self.selected_layout_family
                && row.selected_layout_status == self.selected_layout_status
                && row.has_deterministic_blocker()
        })
    }

    #[must_use]
    pub fn all_rows_fallback_free(&self) -> bool {
        !self.runtime_execution
            && !self.data_read
            && !self.data_decoded
            && !self.data_materialized
            && !self.object_store_io
            && !self.table_catalog_io
            && !self.write_io
            && !self.external_engine_invoked
            && !self.fallback_attempted
            && self
                .rows
                .iter()
                .all(VortexSegmentExtractionAdmissionRow::fallback_free)
    }

    #[must_use]
    pub fn to_human_text(&self) -> String {
        format!(
            "vortex segment extraction admission\nschema_version: {}\nreport: {}\nselected layout: {}\nstatus: {}\nclaim gate: {}\nfallback execution: disabled",
            self.schema_version,
            self.report_id,
            self.selected_layout_family,
            self.selected_layout_status.as_str(),
            self.claim_gate_status,
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
#[allow(clippy::struct_excessive_bools)]
pub struct VortexScanPushdownDecision {
    pub operation: &'static str,
    pub status: VortexScanCompatibilityStatus,
    pub accepted: bool,
    pub residual_required: bool,
    pub residual_executor: VortexScanResidualExecutor,
    pub native_io_certificate_required: bool,
    pub external_engine_invoked: bool,
    pub fallback_attempted: bool,
}

impl VortexScanPushdownDecision {
    fn admitted(operation: &'static str) -> Self {
        Self {
            operation,
            status: VortexScanCompatibilityStatus::Admitted,
            accepted: true,
            residual_required: false,
            residual_executor: VortexScanResidualExecutor::None,
            native_io_certificate_required: true,
            external_engine_invoked: false,
            fallback_attempted: false,
        }
    }

    fn deferred(operation: &'static str) -> Self {
        Self {
            operation,
            status: VortexScanCompatibilityStatus::Deferred,
            accepted: false,
            residual_required: false,
            residual_executor: VortexScanResidualExecutor::UnsupportedBlocked,
            native_io_certificate_required: true,
            external_engine_invoked: false,
            fallback_attempted: false,
        }
    }

    fn rejected_residual(operation: &'static str) -> Self {
        Self {
            operation,
            status: VortexScanCompatibilityStatus::Rejected,
            accepted: false,
            residual_required: true,
            residual_executor: VortexScanResidualExecutor::UnsupportedBlocked,
            native_io_certificate_required: true,
            external_engine_invoked: false,
            fallback_attempted: false,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[allow(clippy::struct_excessive_bools)]
pub struct VortexSourceSplitRuntimeAdmissionProof {
    pub schema_version: &'static str,
    pub proof_id: &'static str,
    pub path_id: &'static str,
    pub selected_path_status: VortexSourceSplitAdmissionStatus,
    pub generalized_runtime_admission_status: VortexSourceSplitAdmissionStatus,
    pub provider_kind: &'static str,
    pub provider_crate: &'static str,
    pub provider_version: &'static str,
    pub feature_gate: &'static str,
    pub provider_api_surface: &'static str,
    pub shardloom_admission_policy: &'static str,
    pub source_surface: &'static str,
    pub split_surface: &'static str,
    pub split_ref_status: &'static str,
    pub split_estimate_status: &'static str,
    pub split_serialization_status: &'static str,
    pub field_mask_status: &'static str,
    pub predicate_ordering_status: &'static str,
    pub projection_pushdown_status: &'static str,
    pub filter_pushdown_status: &'static str,
    pub limit_pushdown_status: &'static str,
    pub residual_executor: VortexScanResidualExecutor,
    pub generalized_residual_executor: VortexScanResidualExecutor,
    pub correctness_refs: &'static str,
    pub benchmark_refs: &'static str,
    pub execution_certificate_refs: &'static str,
    pub native_io_certificate_refs: &'static str,
    pub predicate_ordering_refs: &'static str,
    pub policy_refs: &'static str,
    pub unsupported_diagnostic_code: &'static str,
    pub blocker_id: &'static str,
    pub required_future_evidence: &'static str,
    pub claim_gate_status: &'static str,
    pub claim_boundary: &'static str,
    pub runtime_execution: bool,
    pub object_store_io: bool,
    pub table_catalog_io: bool,
    pub write_io: bool,
    pub external_engine_invoked: bool,
    pub fallback_attempted: bool,
}

impl VortexSourceSplitRuntimeAdmissionProof {
    #[must_use]
    pub const fn local_fixture_scan() -> Self {
        Self {
            schema_version: "shardloom.vortex_source_split_runtime_admission.v1",
            proof_id: "gar0042a.vortex_source_split.local_fixture_scan",
            path_id: "local_vortex_file_scan_into_array_iter",
            selected_path_status: VortexSourceSplitAdmissionStatus::FixtureSmokeOnly,
            generalized_runtime_admission_status:
                VortexSourceSplitAdmissionStatus::GeneralizedRuntimeBlocked,
            provider_kind: "vortex_scan",
            provider_crate: "vortex",
            provider_version: "0.73",
            feature_gate: "vortex-local-primitives",
            provider_api_surface: "VortexFile::scan,ScanBuilder::with_filter,ScanBuilder::with_projection,ScanBuilder::into_array_iter",
            shardloom_admission_policy: "shardloom.vortex.local_scan_primitive.v1",
            source_surface: "local_vortex_file_scan",
            split_surface: "reader_chunk_split_ref",
            split_ref_status: "validated_local_reader_split_ref",
            split_estimate_status: "report_only_missing_claim_grade_estimates",
            split_serialization_status: "deferred",
            field_mask_status: "report_only_missing_runtime_mask",
            predicate_ordering_status: "report_only_missing_dynamic_ordering",
            projection_pushdown_status: "admitted_local_primitive",
            filter_pushdown_status: "admitted_local_primitive",
            limit_pushdown_status: "deferred",
            residual_executor: VortexScanResidualExecutor::None,
            generalized_residual_executor: VortexScanResidualExecutor::UnsupportedBlocked,
            correctness_refs: "local_primitive_scan_fixture_correctness",
            benchmark_refs: "vortex-count-benchmark.local_fixture_smoke,traditional_analytics.coverage_table",
            execution_certificate_refs: "certificates/cg16/local-vortex-count/execution.json",
            native_io_certificate_refs: "certificates/cg19/local-vortex-count/native-io.json",
            predicate_ordering_refs: "predicate_ordering_evidence_required",
            policy_refs: "fallback_attempted=false,external_engine_invoked=false",
            unsupported_diagnostic_code: "SL_UNSUPPORTED_GENERALIZED_VORTEX_SOURCE_SPLIT_RUNTIME",
            blocker_id: "gar0042a.generalized_source_split_runtime",
            required_future_evidence: "source_split_certificate,field_mask_evidence,predicate_ordering_evidence,split_serialization_evidence,native_io_certificate",
            claim_gate_status: "fixture_smoke_only",
            claim_boundary: "local_fixture_scan_only_not_generalized_source_split_runtime",
            runtime_execution: false,
            object_store_io: false,
            table_catalog_io: false,
            write_io: false,
            external_engine_invoked: false,
            fallback_attempted: false,
        }
    }

    #[must_use]
    pub const fn selected_fixture_path_classified(self) -> bool {
        matches!(
            self.selected_path_status,
            VortexSourceSplitAdmissionStatus::FixtureSmokeOnly
        )
    }

    #[must_use]
    pub const fn generalized_runtime_blocked(self) -> bool {
        matches!(
            self.generalized_runtime_admission_status,
            VortexSourceSplitAdmissionStatus::GeneralizedRuntimeBlocked
        ) && matches!(
            self.generalized_residual_executor,
            VortexScanResidualExecutor::UnsupportedBlocked
        )
    }

    #[must_use]
    pub const fn fallback_free(self) -> bool {
        !self.runtime_execution
            && !self.object_store_io
            && !self.table_catalog_io
            && !self.write_io
            && !self.external_engine_invoked
            && !self.fallback_attempted
    }

    #[must_use]
    pub fn to_human_text(self) -> String {
        format!(
            "vortex source/split runtime admission proof\nschema_version: {}\nproof: {}\npath: {}\nselected path: {}\ngeneralized runtime: {}\nclaim gate: {}\nfallback execution: disabled",
            self.schema_version,
            self.proof_id,
            self.path_id,
            self.selected_path_status.as_str(),
            self.generalized_runtime_admission_status.as_str(),
            self.claim_gate_status,
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
#[allow(clippy::struct_excessive_bools)]
pub struct VortexScanCompatibilityReport {
    pub schema_version: &'static str,
    pub report_id: &'static str,
    pub source_concept: &'static str,
    pub sink_concept: &'static str,
    pub split_concept: &'static str,
    pub native_work_stream_alignment: &'static str,
    pub native_result_stream_alignment: &'static str,
    pub decisions: Vec<VortexScanPushdownDecision>,
    pub field_masks_tracked: bool,
    pub filter_only_columns_discardable: bool,
    pub split_estimates_tracked: bool,
    pub split_serialization_status: VortexScanCompatibilityStatus,
    pub sink_requirements_status: VortexScanCompatibilityStatus,
    pub source_split_runtime_admission_proof: VortexSourceSplitRuntimeAdmissionProof,
    pub segment_extraction_admission_report: VortexSegmentExtractionAdmissionReport,
    pub split_level_native_io_certificates_required: bool,
    pub external_integrations_allowed_as_runtime: bool,
    pub fallback_attempted: bool,
}

impl VortexScanCompatibilityReport {
    #[must_use]
    pub fn current() -> Self {
        Self {
            schema_version: "shardloom.vortex_scan_compatibility.v1",
            report_id: "cg19.vortex_scan_compatibility",
            source_concept: "Vortex Source accepts scan request",
            sink_concept: "Vortex Sink describes write/output requirements",
            split_concept: "Vortex Split is independently executable work evidence",
            native_work_stream_alignment: "ShardLoom NativeWorkStream carries source, split, projection, filter, limit, field-mask, estimate, residual, and certificate evidence",
            native_result_stream_alignment: "ShardLoom NativeResultStream carries representation, materialization, sink, artifact, and certificate evidence",
            decisions: vec![
                VortexScanPushdownDecision::admitted("projection"),
                VortexScanPushdownDecision::admitted("filter"),
                VortexScanPushdownDecision::deferred("limit"),
                VortexScanPushdownDecision::deferred("field_mask"),
                VortexScanPushdownDecision::deferred("split_estimate"),
                VortexScanPushdownDecision::deferred("split_serialization"),
                VortexScanPushdownDecision::rejected_residual("external_residual_evaluation"),
            ],
            field_masks_tracked: true,
            filter_only_columns_discardable: true,
            split_estimates_tracked: true,
            split_serialization_status: VortexScanCompatibilityStatus::Deferred,
            sink_requirements_status: VortexScanCompatibilityStatus::ReportOnly,
            source_split_runtime_admission_proof:
                VortexSourceSplitRuntimeAdmissionProof::local_fixture_scan(),
            segment_extraction_admission_report: VortexSegmentExtractionAdmissionReport::current(),
            split_level_native_io_certificates_required: true,
            external_integrations_allowed_as_runtime: false,
            fallback_attempted: false,
        }
    }

    #[must_use]
    pub fn decision(&self, operation: &str) -> Option<&VortexScanPushdownDecision> {
        self.decisions
            .iter()
            .find(|decision| decision.operation == operation)
    }

    #[must_use]
    pub fn operation_order(&self) -> Vec<&'static str> {
        self.decisions
            .iter()
            .map(|decision| decision.operation)
            .collect()
    }

    #[must_use]
    pub fn external_residuals_blocked(&self) -> bool {
        self.decision("external_residual_evaluation")
            .is_some_and(|decision| {
                decision.residual_required
                    && decision.residual_executor == VortexScanResidualExecutor::UnsupportedBlocked
                    && !decision.external_engine_invoked
                    && !decision.fallback_attempted
            })
    }

    #[must_use]
    pub fn all_decisions_fallback_free(&self) -> bool {
        !self.fallback_attempted
            && !self.external_integrations_allowed_as_runtime
            && self
                .decisions
                .iter()
                .all(|decision| !decision.external_engine_invoked && !decision.fallback_attempted)
            && self.source_split_runtime_admission_proof.fallback_free()
            && self
                .segment_extraction_admission_report
                .all_rows_fallback_free()
    }

    #[must_use]
    pub fn to_human_text(&self) -> String {
        format!(
            "vortex scan compatibility\nschema_version: {}\nreport: {}\noperations: {}\nsplit native I/O certificates: required\nexternal integrations as runtime: disabled\nfallback execution: disabled",
            self.schema_version,
            self.report_id,
            self.operation_order().join(","),
        )
    }
}

#[must_use]
pub fn plan_vortex_scan_compatibility() -> VortexScanCompatibilityReport {
    VortexScanCompatibilityReport::current()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn scan_compatibility_tracks_scan_source_sink_split_alignment() {
        let report = plan_vortex_scan_compatibility();

        assert_eq!(
            report.schema_version,
            "shardloom.vortex_scan_compatibility.v1"
        );
        assert_eq!(report.source_concept, "Vortex Source accepts scan request");
        assert!(
            report
                .native_work_stream_alignment
                .contains("NativeWorkStream")
        );
        assert!(
            report
                .native_result_stream_alignment
                .contains("NativeResultStream")
        );
        assert!(report.field_masks_tracked);
        assert!(report.filter_only_columns_discardable);
        assert!(report.split_estimates_tracked);
        assert!(report.split_level_native_io_certificates_required);
    }

    #[test]
    fn scan_compatibility_records_pushdown_and_residual_policy() {
        let report = plan_vortex_scan_compatibility();

        assert_eq!(
            report.operation_order(),
            vec![
                "projection",
                "filter",
                "limit",
                "field_mask",
                "split_estimate",
                "split_serialization",
                "external_residual_evaluation"
            ]
        );
        assert_eq!(
            report
                .decision("projection")
                .map(|decision| decision.status),
            Some(VortexScanCompatibilityStatus::Admitted)
        );
        assert_eq!(
            report.decision("limit").map(|decision| decision.status),
            Some(VortexScanCompatibilityStatus::Deferred)
        );
        assert!(report.external_residuals_blocked());
    }

    #[test]
    fn scan_compatibility_blocks_external_runtime_fallback() {
        let report = plan_vortex_scan_compatibility();

        assert!(!report.external_integrations_allowed_as_runtime);
        assert!(report.all_decisions_fallback_free());
        assert!(
            report
                .to_human_text()
                .contains("external integrations as runtime: disabled")
        );
    }

    #[test]
    fn source_split_runtime_admission_proof_classifies_fixture_and_blocks_generalized_runtime() {
        let report = plan_vortex_scan_compatibility();
        let proof = report.source_split_runtime_admission_proof;

        assert_eq!(
            proof.schema_version,
            "shardloom.vortex_source_split_runtime_admission.v1"
        );
        assert_eq!(proof.path_id, "local_vortex_file_scan_into_array_iter");
        assert_eq!(proof.provider_kind, "vortex_scan");
        assert_eq!(proof.feature_gate, "vortex-local-primitives");
        assert_eq!(proof.residual_executor, VortexScanResidualExecutor::None);
        assert!(proof.selected_fixture_path_classified());
        assert!(proof.generalized_runtime_blocked());
        assert!(proof.fallback_free());
        assert!(
            proof
                .to_human_text()
                .contains("generalized runtime: blocked_until_source_split_certificate")
        );
    }

    #[test]
    fn segment_extraction_admission_blocks_sparse_layout_without_fallback() {
        let report = plan_vortex_scan_compatibility();
        let admission = report.segment_extraction_admission_report;

        assert_eq!(
            admission.schema_version,
            "shardloom.vortex_segment_extraction_admission.v1"
        );
        assert_eq!(admission.selected_layout_family, "sparse_patch_fill");
        assert_eq!(
            admission.selected_layout_status.as_str(),
            "blocked_until_segment_extraction_certificate"
        );
        assert_eq!(admission.supported_layout_count(), 0);
        assert_eq!(admission.blocked_layout_count(), 1);
        assert_eq!(
            admission.unsupported_diagnostic_codes(),
            vec!["SL_UNSUPPORTED_VORTEX_SPARSE_SEGMENT_EXTRACTION"]
        );
        assert_eq!(
            admission.blocker_ids(),
            vec!["gar0003a.sparse_patch_fill_segment_extraction"]
        );
        assert!(admission.selected_layout_classified());
        assert!(admission.all_rows_fallback_free());
        assert!(
            admission
                .to_human_text()
                .contains("fallback execution: disabled")
        );
    }
}
