//! Vortex compute-provider alignment reports.
//!
//! These report surfaces distinguish upstream Vortex-native providers from
//! external query-engine integrations. They are policy/evidence contracts, not
//! new runtime execution or integration authorization.

use shardloom_core::ExecutionProviderKind;

use crate::VortexResidualBoundaryReport;

const VORTEX_VERSION: &str = "0.72";
const LOCAL_SCAN_FEATURE_GATE: &str = "vortex-local-primitives";
const LOCAL_SCAN_API_SURFACE: &str = "VortexFile::scan.into_array_iter";
const LOCAL_SCAN_ADMISSION_POLICY: &str = "shardloom.vortex.local_scan_primitive.v1";

#[derive(Debug, Clone, PartialEq, Eq)]
#[allow(clippy::struct_excessive_bools)]
pub struct VortexComputeProviderReport {
    pub schema_version: &'static str,
    pub report_id: &'static str,
    pub provider_kind: ExecutionProviderKind,
    pub vortex_version: &'static str,
    pub feature_gate: &'static str,
    pub shardloom_admission_policy: &'static str,
    pub provider_api_surface: &'static str,
    pub operation: &'static str,
    pub dtype_support: &'static str,
    pub encoding_support: &'static str,
    pub layout_support: &'static str,
    pub null_semantics: &'static str,
    pub selection_vector_behavior: &'static str,
    pub materialization_behavior: &'static str,
    pub decoded_reference_status: &'static str,
    pub residual_required: bool,
    pub residual_executor: &'static str,
    pub certificate_backed_before_support_claim: bool,
    pub support_claim_allowed: bool,
    pub external_engine_invoked: bool,
    pub fallback_attempted: bool,
    pub diagnostics: Vec<&'static str>,
}

impl VortexComputeProviderReport {
    #[must_use]
    pub fn local_scan_provider() -> Self {
        Self {
            schema_version: "shardloom.vortex_compute_provider_report.v1",
            report_id: "cg19.vortex_compute_provider.local_scan",
            provider_kind: ExecutionProviderKind::VortexScan,
            vortex_version: VORTEX_VERSION,
            feature_gate: LOCAL_SCAN_FEATURE_GATE,
            shardloom_admission_policy: LOCAL_SCAN_ADMISSION_POLICY,
            provider_api_surface: LOCAL_SCAN_API_SURFACE,
            operation: "local_primitive_scan_filter_project_reader_chunk_admission",
            dtype_support: "primitive_host_dtypes_only_for_current_certificate_scope",
            encoding_support: "constant_dictionary_run_end_only_when_no_decode_slots_exist",
            layout_support: "reader_chunk_layout_evidence_required",
            null_semantics: "nullable_dictionary_rle_sparse_nested_extension_blocked",
            selection_vector_behavior: "selection_vectors_preserved_or_reported",
            materialization_behavior: "no_row_read_no_arrow_no_hidden_materialization",
            decoded_reference_status: "correctness_reference_separate_from_provider_execution",
            residual_required: false,
            residual_executor: "none",
            certificate_backed_before_support_claim: true,
            support_claim_allowed: false,
            external_engine_invoked: false,
            fallback_attempted: false,
            diagnostics: Vec::new(),
        }
    }

    #[must_use]
    pub fn policy_admitted_and_fallback_free(&self) -> bool {
        self.provider_kind == ExecutionProviderKind::VortexScan
            && self.vortex_version == VORTEX_VERSION
            && self.feature_gate == LOCAL_SCAN_FEATURE_GATE
            && self.shardloom_admission_policy == LOCAL_SCAN_ADMISSION_POLICY
            && self.certificate_backed_before_support_claim
            && !self.external_engine_invoked
            && !self.fallback_attempted
    }

    #[must_use]
    pub const fn support_claim_blocked_without_evidence(&self) -> bool {
        !self.support_claim_allowed && self.certificate_backed_before_support_claim
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VortexIntegrationRole {
    UpstreamVortexNativeApiAllowed,
    VortexDataFusionBaselineOnly,
    VortexDuckDbBaselineOnly,
    VortexSparkBaselineOnly,
    VortexTrinoBaselineOnly,
    UnsupportedAsRuntime,
    ProhibitedFallback,
}

impl VortexIntegrationRole {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::UpstreamVortexNativeApiAllowed => "upstream_vortex_native_api_allowed",
            Self::VortexDataFusionBaselineOnly => "vortex_datafusion_baseline_only",
            Self::VortexDuckDbBaselineOnly => "vortex_duckdb_baseline_only",
            Self::VortexSparkBaselineOnly => "vortex_spark_baseline_only",
            Self::VortexTrinoBaselineOnly => "vortex_trino_baseline_only",
            Self::UnsupportedAsRuntime => "unsupported_as_runtime",
            Self::ProhibitedFallback => "prohibited_fallback",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
#[allow(clippy::struct_excessive_bools)]
pub struct VortexIntegrationBoundaryRow {
    pub integration_name: &'static str,
    pub role: VortexIntegrationRole,
    pub allowed_in_core: bool,
    pub allowed_in_benchmark: bool,
    pub allowed_in_oracle: bool,
    pub may_execute_shardloom_plan: bool,
    pub may_execute_residual: bool,
    pub fallback_attempted: bool,
}

impl VortexIntegrationBoundaryRow {
    fn native_api(integration_name: &'static str) -> Self {
        Self {
            integration_name,
            role: VortexIntegrationRole::UpstreamVortexNativeApiAllowed,
            allowed_in_core: true,
            allowed_in_benchmark: true,
            allowed_in_oracle: false,
            may_execute_shardloom_plan: true,
            may_execute_residual: false,
            fallback_attempted: false,
        }
    }

    fn baseline_only(integration_name: &'static str, role: VortexIntegrationRole) -> Self {
        Self {
            integration_name,
            role,
            allowed_in_core: false,
            allowed_in_benchmark: true,
            allowed_in_oracle: true,
            may_execute_shardloom_plan: false,
            may_execute_residual: false,
            fallback_attempted: false,
        }
    }

    fn prohibited(integration_name: &'static str, role: VortexIntegrationRole) -> Self {
        Self {
            integration_name,
            role,
            allowed_in_core: false,
            allowed_in_benchmark: false,
            allowed_in_oracle: false,
            may_execute_shardloom_plan: false,
            may_execute_residual: false,
            fallback_attempted: false,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
#[allow(clippy::struct_excessive_bools)]
pub struct VortexIntegrationBoundaryReport {
    pub schema_version: &'static str,
    pub report_id: &'static str,
    pub rows: Vec<VortexIntegrationBoundaryRow>,
    pub external_engine_invoked: bool,
    pub fallback_attempted: bool,
}

impl VortexIntegrationBoundaryReport {
    #[must_use]
    pub fn current() -> Self {
        Self {
            schema_version: "shardloom.vortex_integration_boundary_report.v1",
            report_id: "cg20.vortex_integration_boundary",
            rows: vec![
                VortexIntegrationBoundaryRow::native_api("vortex_scan_source_sink_api"),
                VortexIntegrationBoundaryRow::baseline_only(
                    "vortex_datafusion",
                    VortexIntegrationRole::VortexDataFusionBaselineOnly,
                ),
                VortexIntegrationBoundaryRow::baseline_only(
                    "vortex_duckdb",
                    VortexIntegrationRole::VortexDuckDbBaselineOnly,
                ),
                VortexIntegrationBoundaryRow::baseline_only(
                    "vortex_spark",
                    VortexIntegrationRole::VortexSparkBaselineOnly,
                ),
                VortexIntegrationBoundaryRow::baseline_only(
                    "vortex_trino",
                    VortexIntegrationRole::VortexTrinoBaselineOnly,
                ),
                VortexIntegrationBoundaryRow::prohibited(
                    "external_runtime_residual_evaluation",
                    VortexIntegrationRole::ProhibitedFallback,
                ),
                VortexIntegrationBoundaryRow::prohibited(
                    "unclassified_vortex_query_engine_integration",
                    VortexIntegrationRole::UnsupportedAsRuntime,
                ),
            ],
            external_engine_invoked: false,
            fallback_attempted: false,
        }
    }

    #[must_use]
    pub fn role_for(&self, integration_name: &str) -> Option<VortexIntegrationRole> {
        self.rows
            .iter()
            .find(|row| row.integration_name == integration_name)
            .map(|row| row.role)
    }

    #[must_use]
    pub fn all_external_integrations_are_baseline_or_blocked(&self) -> bool {
        !self.external_engine_invoked
            && !self.fallback_attempted
            && self.rows.iter().all(|row| {
                !row.fallback_attempted
                    && if row.allowed_in_core {
                        row.role == VortexIntegrationRole::UpstreamVortexNativeApiAllowed
                            && !row.may_execute_residual
                    } else {
                        !row.may_execute_shardloom_plan && !row.may_execute_residual
                    }
            })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
#[allow(clippy::struct_excessive_bools)]
pub struct VortexComputeProviderAlignmentReport {
    pub schema_version: &'static str,
    pub report_id: &'static str,
    pub compute_provider_report: VortexComputeProviderReport,
    pub residual_executor_values: Vec<&'static str>,
    pub integration_boundary_report: VortexIntegrationBoundaryReport,
    pub standalone_means_no_external_query_engine_fallback: bool,
    pub upstream_vortex_native_providers_allowed_with_certificates: bool,
    pub provider_feature_gate_required: bool,
    pub provider_version_record_required: bool,
    pub provider_policy_admission_required: bool,
    pub provider_certificate_required: bool,
    pub external_engine_invoked: bool,
    pub fallback_attempted: bool,
}

impl VortexComputeProviderAlignmentReport {
    #[must_use]
    pub fn current() -> Self {
        Self {
            schema_version: "shardloom.vortex_compute_provider_alignment.v1",
            report_id: "priority_2_6.vortex_compute_provider_alignment",
            compute_provider_report: VortexComputeProviderReport::local_scan_provider(),
            residual_executor_values: VortexResidualBoundaryReport::residual_executor_values()
                .to_vec(),
            integration_boundary_report: VortexIntegrationBoundaryReport::current(),
            standalone_means_no_external_query_engine_fallback: true,
            upstream_vortex_native_providers_allowed_with_certificates: true,
            provider_feature_gate_required: true,
            provider_version_record_required: true,
            provider_policy_admission_required: true,
            provider_certificate_required: true,
            external_engine_invoked: false,
            fallback_attempted: false,
        }
    }

    #[must_use]
    pub fn policy_complete_and_fallback_free(&self) -> bool {
        self.standalone_means_no_external_query_engine_fallback
            && self.upstream_vortex_native_providers_allowed_with_certificates
            && self.provider_feature_gate_required
            && self.provider_version_record_required
            && self.provider_policy_admission_required
            && self.provider_certificate_required
            && self
                .compute_provider_report
                .policy_admitted_and_fallback_free()
            && self
                .compute_provider_report
                .support_claim_blocked_without_evidence()
            && self
                .integration_boundary_report
                .all_external_integrations_are_baseline_or_blocked()
            && !self.external_engine_invoked
            && !self.fallback_attempted
    }

    #[must_use]
    pub fn to_human_text(&self) -> String {
        format!(
            "vortex compute-provider alignment\nschema_version: {}\nreport: {}\nprovider: {}\nintegrations: {}\nresidual executors: {}\nfallback execution: disabled",
            self.schema_version,
            self.report_id,
            self.compute_provider_report.provider_kind.as_str(),
            self.integration_boundary_report.rows.len(),
            self.residual_executor_values.len(),
        )
    }
}

#[must_use]
pub fn plan_vortex_compute_provider_alignment_report() -> VortexComputeProviderAlignmentReport {
    VortexComputeProviderAlignmentReport::current()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn compute_provider_report_records_vortex_native_boundary_requirements() {
        let report = VortexComputeProviderReport::local_scan_provider();

        assert_eq!(report.provider_kind, ExecutionProviderKind::VortexScan);
        assert_eq!(report.vortex_version, "0.72");
        assert_eq!(report.feature_gate, "vortex-local-primitives");
        assert_eq!(
            report.shardloom_admission_policy,
            "shardloom.vortex.local_scan_primitive.v1"
        );
        assert_eq!(report.provider_api_surface, LOCAL_SCAN_API_SURFACE);
        assert_eq!(report.residual_executor, "none");
        assert!(!report.support_claim_allowed);
        assert!(report.policy_admitted_and_fallback_free());
        assert!(report.support_claim_blocked_without_evidence());
    }

    #[test]
    fn integration_boundary_report_keeps_query_engine_integrations_out_of_core_execution() {
        let report = VortexIntegrationBoundaryReport::current();

        assert_eq!(
            report.role_for("vortex_datafusion"),
            Some(VortexIntegrationRole::VortexDataFusionBaselineOnly)
        );
        assert_eq!(
            report.role_for("vortex_scan_source_sink_api"),
            Some(VortexIntegrationRole::UpstreamVortexNativeApiAllowed)
        );
        assert!(report.all_external_integrations_are_baseline_or_blocked());
        assert!(
            report
                .rows
                .iter()
                .filter(|row| !row.allowed_in_core)
                .all(|row| !row.may_execute_shardloom_plan && !row.may_execute_residual)
        );
    }

    #[test]
    fn alignment_report_exposes_all_residual_executor_values() {
        let report = plan_vortex_compute_provider_alignment_report();

        assert_eq!(
            report.residual_executor_values,
            vec![
                "none",
                "shardloom_native",
                "unsupported_blocked",
                "external_baseline_only",
                "prohibited_external_fallback"
            ]
        );
        assert!(report.policy_complete_and_fallback_free());
        assert!(
            report
                .to_human_text()
                .contains("fallback execution: disabled")
        );
    }
}
