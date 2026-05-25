//! Vortex upstream compatibility matrix.
//!
//! This module records `ShardLoom`'s current compatibility posture against the
//! upstream Vortex crate/API/file-format surface. It is report-only: it does not
//! probe Vortex, execute scans, widen runtime support, or treat upstream Vortex
//! query-engine integrations as `ShardLoom` execution.

use shardloom_core::Diagnostic;

const MATRIX_SCHEMA_VERSION: &str = "shardloom.vortex_compatibility_matrix.v1";
const MATRIX_REPORT_ID: &str = "cg19.cg20.vortex_compatibility_matrix";
const VORTEX_CRATE_VERSION: &str = "0.72";
const VORTEX_FILE_FORMAT_ASSUMPTION: &str = "stable_from_0.36.0_onward_api_evolving";
const RUST_TOOLCHAIN_COMPATIBILITY: &str = "rust_1.91.1";
const LOCAL_PRIMITIVES_FEATURE_GATE: &str = "vortex-local-primitives";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VortexCompatibilityStatus {
    Certified,
    EvidenceIncomplete,
    ReportOnly,
    BaselineOnly,
    Deferred,
    Blocked,
}

impl VortexCompatibilityStatus {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Certified => "certified",
            Self::EvidenceIncomplete => "evidence_incomplete",
            Self::ReportOnly => "report_only",
            Self::BaselineOnly => "baseline_only",
            Self::Deferred => "deferred",
            Self::Blocked => "blocked",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
#[allow(clippy::struct_excessive_bools)]
pub struct VortexCompatibilityMatrixRow {
    pub capability: &'static str,
    pub status: VortexCompatibilityStatus,
    pub crate_feature: Option<&'static str>,
    pub detail: &'static str,
    pub evidence_refs: Vec<&'static str>,
    pub support_claim_allowed: bool,
    pub external_engine_invoked: bool,
    pub fallback_attempted: bool,
}

impl VortexCompatibilityMatrixRow {
    fn certified(
        capability: &'static str,
        crate_feature: &'static str,
        detail: &'static str,
        evidence_refs: Vec<&'static str>,
    ) -> Self {
        Self {
            capability,
            status: VortexCompatibilityStatus::Certified,
            crate_feature: Some(crate_feature),
            detail,
            evidence_refs,
            support_claim_allowed: true,
            external_engine_invoked: false,
            fallback_attempted: false,
        }
    }

    fn evidence_incomplete(
        capability: &'static str,
        crate_feature: &'static str,
        detail: &'static str,
        evidence_refs: Vec<&'static str>,
    ) -> Self {
        Self {
            capability,
            status: VortexCompatibilityStatus::EvidenceIncomplete,
            crate_feature: Some(crate_feature),
            detail,
            evidence_refs,
            support_claim_allowed: false,
            external_engine_invoked: false,
            fallback_attempted: false,
        }
    }

    fn report_only(capability: &'static str, detail: &'static str) -> Self {
        Self {
            capability,
            status: VortexCompatibilityStatus::ReportOnly,
            crate_feature: None,
            detail,
            evidence_refs: Vec::new(),
            support_claim_allowed: false,
            external_engine_invoked: false,
            fallback_attempted: false,
        }
    }

    fn baseline_only(capability: &'static str, detail: &'static str) -> Self {
        Self {
            capability,
            status: VortexCompatibilityStatus::BaselineOnly,
            crate_feature: None,
            detail,
            evidence_refs: Vec::new(),
            support_claim_allowed: false,
            external_engine_invoked: false,
            fallback_attempted: false,
        }
    }

    fn deferred(capability: &'static str, detail: &'static str) -> Self {
        Self {
            capability,
            status: VortexCompatibilityStatus::Deferred,
            crate_feature: None,
            detail,
            evidence_refs: Vec::new(),
            support_claim_allowed: false,
            external_engine_invoked: false,
            fallback_attempted: false,
        }
    }

    fn blocked(capability: &'static str, detail: &'static str) -> Self {
        Self {
            capability,
            status: VortexCompatibilityStatus::Blocked,
            crate_feature: None,
            detail,
            evidence_refs: Vec::new(),
            support_claim_allowed: false,
            external_engine_invoked: false,
            fallback_attempted: false,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
#[allow(clippy::struct_excessive_bools)]
pub struct VortexCompatibilityMatrixReport {
    pub schema_version: &'static str,
    pub report_id: &'static str,
    pub vortex_crate_version: &'static str,
    pub vortex_file_format_assumption: &'static str,
    pub rust_toolchain_compatibility: &'static str,
    pub enabled_feature_gates: Vec<&'static str>,
    pub rows: Vec<VortexCompatibilityMatrixRow>,
    pub known_unsupported_vortex_apis: Vec<&'static str>,
    pub external_integrations_baseline_only: Vec<&'static str>,
    pub runtime_probe_performed: bool,
    pub external_engine_invoked: bool,
    pub fallback_attempted: bool,
    pub diagnostics: Vec<Diagnostic>,
}

impl VortexCompatibilityMatrixReport {
    #[must_use]
    pub fn current() -> Self {
        Self {
            schema_version: MATRIX_SCHEMA_VERSION,
            report_id: MATRIX_REPORT_ID,
            vortex_crate_version: VORTEX_CRATE_VERSION,
            vortex_file_format_assumption: VORTEX_FILE_FORMAT_ASSUMPTION,
            rust_toolchain_compatibility: RUST_TOOLCHAIN_COMPATIBILITY,
            enabled_feature_gates: vec![LOCAL_PRIMITIVES_FEATURE_GATE],
            rows: current_matrix_rows(),
            known_unsupported_vortex_apis: vec![
                "generalized_source_sink_api",
                "object_store_scan",
                "write_sink",
                "split_serialization",
                "gpu_device_residency",
                "vector_ann_topk_index",
                "geospatial_raster_extension_execution",
                "pyvortex_runtime_dependency",
            ],
            external_integrations_baseline_only: vec![
                "vortex_datafusion",
                "vortex_duckdb",
                "vortex_spark",
                "vortex_trino",
            ],
            runtime_probe_performed: false,
            external_engine_invoked: false,
            fallback_attempted: false,
            diagnostics: Vec::new(),
        }
    }

    #[must_use]
    pub fn row(&self, capability: &str) -> Option<&VortexCompatibilityMatrixRow> {
        self.rows.iter().find(|row| row.capability == capability)
    }

    #[must_use]
    pub fn status_for(&self, capability: &str) -> Option<VortexCompatibilityStatus> {
        self.row(capability).map(|row| row.status)
    }

    #[must_use]
    pub fn capability_order(&self) -> Vec<&'static str> {
        self.rows.iter().map(|row| row.capability).collect()
    }

    #[must_use]
    pub fn blocked_or_deferred_count(&self) -> usize {
        self.rows
            .iter()
            .filter(|row| {
                matches!(
                    row.status,
                    VortexCompatibilityStatus::Blocked | VortexCompatibilityStatus::Deferred
                )
            })
            .count()
    }

    #[must_use]
    pub fn all_rows_fallback_free(&self) -> bool {
        !self.external_engine_invoked
            && !self.fallback_attempted
            && self
                .rows
                .iter()
                .all(|row| !row.external_engine_invoked && !row.fallback_attempted)
    }

    #[must_use]
    pub fn support_claims_require_row_evidence(&self) -> bool {
        self.rows.iter().all(|row| {
            !row.support_claim_allowed
                || matches!(row.status, VortexCompatibilityStatus::Certified)
                    && !row.evidence_refs.is_empty()
        })
    }

    #[must_use]
    pub fn to_human_text(&self) -> String {
        format!(
            "vortex compatibility matrix\nschema_version: {}\nreport: {}\nvortex crate: {}\nfile format: {}\nrust toolchain: {}\nrows: {}\nblocked_or_deferred: {}\nruntime probe: disabled\nfallback execution: disabled",
            self.schema_version,
            self.report_id,
            self.vortex_crate_version,
            self.vortex_file_format_assumption,
            self.rust_toolchain_compatibility,
            self.rows.len(),
            self.blocked_or_deferred_count(),
        )
    }
}

fn current_matrix_rows() -> Vec<VortexCompatibilityMatrixRow> {
    vec![
        VortexCompatibilityMatrixRow::certified(
            "metadata_footer_local_open",
            LOCAL_PRIMITIVES_FEATURE_GATE,
            "feature-gated local metadata/footer fixture open path",
            vec![
                "vortex-public-api-inventory",
                "local primitive certificates",
            ],
        ),
        VortexCompatibilityMatrixRow::certified(
            "local_primitive_scan_count_filter_project",
            LOCAL_PRIMITIVES_FEATURE_GATE,
            "local .vortex CountAll, CountWhere, FilterPredicate, ProjectColumns, and FilterAndProject scan-pushdown paths",
            vec![
                "CG-2 local primitive execution",
                "CG-16 execution certificates",
                "CG-19 Native I/O certificates",
            ],
        ),
        VortexCompatibilityMatrixRow::evidence_incomplete(
            "reader_chunk_constant_dictionary_run_end_kernel_inputs",
            LOCAL_PRIMITIVES_FEATURE_GATE,
            "direct local reader-chunk lowering covers constants plus non-null host primitive dictionary and run-end arrays; nullable, sparse, nested, extension, and device paths stay blocked",
            vec![
                "reader-generated kernel input admission",
                "source-backed certificate-pair report",
                "CG-5.21 fixtures",
                "CG-6.24 benchmark slots",
            ],
        ),
        VortexCompatibilityMatrixRow::evidence_incomplete(
            "scan_api_alignment",
            LOCAL_PRIMITIVES_FEATURE_GATE,
            "local scan concepts are admitted through approved provider boundaries; generalized Source/Sink/Split integration remains staged",
            vec![
                "VortexNativeProviderBoundary",
                "VortexReaderBackedSplitEvidence",
            ],
        ),
        VortexCompatibilityMatrixRow::report_only(
            "dtype_layout_statistics_mapping",
            "primitive DType/statistics mapping exists for current local encoded paths; broad dtype/layout/statistics matrix remains planned",
        ),
        VortexCompatibilityMatrixRow::report_only(
            "arrow_interop_boundary",
            "Arrow is an explicit compatibility/export boundary, not the default execution representation",
        ),
        VortexCompatibilityMatrixRow::deferred(
            "source_sink_split_serialization",
            "generalized Vortex Source/Sink traits and split serialization are deferred until adapter/source evidence exists",
        ),
        VortexCompatibilityMatrixRow::deferred(
            "object_store_scan",
            "object-store Vortex scan and sub-segment/range-read evidence are deferred",
        ),
        VortexCompatibilityMatrixRow::deferred(
            "write_sink",
            "local/object-store/table write and streaming sink support remain deferred pending sink certificates",
        ),
        VortexCompatibilityMatrixRow::deferred(
            "gpu_device_residency",
            "GPU/device execution is report-only/deferred; CPU remains the only current default",
        ),
        VortexCompatibilityMatrixRow::deferred(
            "extension_dtypes_vector_geo_media",
            "vector, geospatial, raster, media-reference, and extension dtype support is tracked but not execution-certified",
        ),
        VortexCompatibilityMatrixRow::baseline_only(
            "vortex_query_engine_integrations",
            "DataFusion, DuckDB, Spark, Trino, and similar Vortex integrations are baselines/oracles/references only",
        ),
        VortexCompatibilityMatrixRow::blocked(
            "external_residual_fallback",
            "unsupported residual work must be blocked or handled by ShardLoom-native code, never delegated to external query engines",
        ),
    ]
}

#[must_use]
pub fn plan_vortex_compatibility_matrix() -> VortexCompatibilityMatrixReport {
    VortexCompatibilityMatrixReport::current()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn compatibility_matrix_records_versions_and_feature_gates() {
        let report = plan_vortex_compatibility_matrix();

        assert_eq!(report.schema_version, MATRIX_SCHEMA_VERSION);
        assert_eq!(report.vortex_crate_version, "0.72");
        assert_eq!(
            report.vortex_file_format_assumption,
            "stable_from_0.36.0_onward_api_evolving"
        );
        assert_eq!(report.rust_toolchain_compatibility, "rust_1.91.1");
        assert_eq!(
            report.enabled_feature_gates,
            vec!["vortex-local-primitives"]
        );
        assert!(!report.runtime_probe_performed);
        assert!(report.all_rows_fallback_free());
    }

    #[test]
    fn compatibility_matrix_distinguishes_supported_deferred_and_baseline_rows() {
        let report = plan_vortex_compatibility_matrix();

        assert_eq!(
            report.status_for("metadata_footer_local_open"),
            Some(VortexCompatibilityStatus::Certified)
        );
        assert_eq!(
            report.status_for("reader_chunk_constant_dictionary_run_end_kernel_inputs"),
            Some(VortexCompatibilityStatus::EvidenceIncomplete)
        );
        assert_eq!(
            report.status_for("object_store_scan"),
            Some(VortexCompatibilityStatus::Deferred)
        );
        assert_eq!(
            report.status_for("vortex_query_engine_integrations"),
            Some(VortexCompatibilityStatus::BaselineOnly)
        );
        assert_eq!(
            report.status_for("external_residual_fallback"),
            Some(VortexCompatibilityStatus::Blocked)
        );
        assert!(report.blocked_or_deferred_count() >= 5);
    }

    #[test]
    fn compatibility_matrix_keeps_claims_evidence_backed() {
        let report = plan_vortex_compatibility_matrix();
        let reader_chunk = report
            .row("reader_chunk_constant_dictionary_run_end_kernel_inputs")
            .expect("reader chunk row");
        assert!(!reader_chunk.support_claim_allowed);
        assert!(!reader_chunk.evidence_refs.is_empty());
        let integration = report
            .row("vortex_query_engine_integrations")
            .expect("integration row");
        assert!(!integration.support_claim_allowed);
        assert!(!integration.external_engine_invoked);
        assert!(report.support_claims_require_row_evidence());
        assert!(
            report
                .to_human_text()
                .contains("fallback execution: disabled")
        );
    }
}
