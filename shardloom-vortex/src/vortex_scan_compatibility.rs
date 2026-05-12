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
}
