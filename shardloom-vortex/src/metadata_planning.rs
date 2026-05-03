//! `Vortex` metadata-to-planning bridge for `ShardLoom`.
//!
//! This module converts `VortexMetadataSummaryReport` into scan/explain/estimate
//! planning skeletons. It does not execute scans, decode data, materialize
//! values, perform object-store IO, write files, or enable fallback execution.

use std::fmt::Write as _;

use shardloom_core::{
    DatasetRef, Diagnostic, DiagnosticCode, DiagnosticSeverity, ExecutionState, Result,
};
use shardloom_plan::{
    EstimateConfidence, EstimateReport, EstimateValue, ExecutionBoundary, ExplainPlanNode,
    ExplainReport, PlanNodeId, PlanNodeKind, ScanPlanSkeleton, ScanRequest,
};

/// Planning status for bridging `Vortex` metadata summaries into `ShardLoom` planning.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VortexMetadataPlanningStatus {
    Planned,
    ProbeDeferred,
    MetadataUnavailable,
    Unsupported,
}
impl VortexMetadataPlanningStatus {
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::Planned => "planned",
            Self::ProbeDeferred => "probe_deferred",
            Self::MetadataUnavailable => "metadata_unavailable",
            Self::Unsupported => "unsupported",
        }
    }
    #[must_use]
    pub const fn is_error(&self) -> bool {
        matches!(self, Self::Unsupported)
    }
}

/// Planning mode for metadata summary bridge integration.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VortexMetadataPlanningMode {
    MetadataOnly,
    PlanOnly,
    Unsupported,
}
impl VortexMetadataPlanningMode {
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::MetadataOnly => "metadata_only",
            Self::PlanOnly => "plan_only",
            Self::Unsupported => "unsupported",
        }
    }
    #[must_use]
    pub const fn requires_data_execution(&self) -> bool {
        false
    }
}

/// Planning report produced from a `Vortex` metadata summary.
#[derive(Debug, Clone, PartialEq)]
#[allow(clippy::struct_excessive_bools)]
pub struct VortexMetadataPlanningReport {
    pub status: VortexMetadataPlanningStatus,
    pub mode: VortexMetadataPlanningMode,
    pub metadata_summary: crate::VortexMetadataSummaryReport,
    pub scan_plan: Option<ScanPlanSkeleton>,
    pub explain_report: ExplainReport,
    pub estimate_report: EstimateReport,
    pub data_executed: bool,
    pub data_materialized: bool,
    pub object_store_io: bool,
    pub write_io: bool,
    pub fallback_execution_allowed: bool,
    pub diagnostics: Vec<Diagnostic>,
}
impl VortexMetadataPlanningReport {
    /// Builds a plan-only planning report from metadata summary.
    ///
    /// # Errors
    /// Returns errors only if underlying `ShardLoom` identifier validation fails.
    pub fn from_metadata_summary(summary: crate::VortexMetadataSummaryReport) -> Result<Self> {
        let status = match summary.status {
            crate::VortexMetadataSummaryStatus::Summarized => VortexMetadataPlanningStatus::Planned,
            crate::VortexMetadataSummaryStatus::ProbeDeferred => {
                VortexMetadataPlanningStatus::ProbeDeferred
            }
            crate::VortexMetadataSummaryStatus::MetadataUnavailable => {
                VortexMetadataPlanningStatus::MetadataUnavailable
            }
            crate::VortexMetadataSummaryStatus::Unsupported => {
                VortexMetadataPlanningStatus::Unsupported
            }
        };
        let mode = if matches!(status, VortexMetadataPlanningStatus::Unsupported) {
            VortexMetadataPlanningMode::Unsupported
        } else if matches!(
            status,
            VortexMetadataPlanningStatus::ProbeDeferred
                | VortexMetadataPlanningStatus::MetadataUnavailable
        ) {
            VortexMetadataPlanningMode::PlanOnly
        } else {
            VortexMetadataPlanningMode::MetadataOnly
        };

        let mut explain_report = ExplainReport::new("Vortex metadata-only planning");
        if let Some(uri) = &summary.summary.uri {
            explain_report.add_input_dataset(uri.as_str().to_string());
        }
        if let Ok(node_id) = PlanNodeId::new("vortex-metadata-summary") {
            explain_report.add_node(
                ExplainPlanNode::new(
                    node_id,
                    PlanNodeKind::Scan,
                    "Vortex metadata summary",
                    ExecutionState::MetadataOnly,
                )
                .with_boundary(ExecutionBoundary::NativeVortexInput)
                .with_boundary(ExecutionBoundary::MetadataOnly),
            );
        }
        let mut estimate_report = EstimateReport::unknown("Vortex metadata-only estimate");
        if let Some(rows) = summary.summary.row_count {
            estimate_report.estimated_rows_scanned = EstimateValue::known(rows);
            estimate_report.confidence = EstimateConfidence::High;
        }
        let segs = summary.summary.segment_count() as u64;
        if segs > 0 {
            estimate_report.estimated_segments_considered = EstimateValue::known(segs);
            if !matches!(estimate_report.confidence, EstimateConfidence::High) {
                estimate_report.confidence = EstimateConfidence::Medium;
            }
        }
        if matches!(estimate_report.confidence, EstimateConfidence::Unknown) {
            estimate_report.add_uncertainty("metadata row/segment counts are unavailable");
        }

        let mut scan_plan = None;
        if let Some(uri) = &summary.summary.uri
            && let Ok(dataset_ref) = DatasetRef::from_uri(uri.clone())
        {
            scan_plan = Some(ScanPlanSkeleton::plan_only(ScanRequest::new(dataset_ref)));
        }

        Ok(Self {
            status,
            mode,
            metadata_summary: summary.clone(),
            scan_plan,
            explain_report,
            estimate_report,
            data_executed: false,
            data_materialized: false,
            object_store_io: summary.summary.object_store_io,
            write_io: summary.summary.write_io,
            fallback_execution_allowed: false,
            diagnostics: summary.diagnostics,
        })
    }

    /// Creates a deferred metadata planning report.
    ///
    /// # Errors
    /// Returns errors only if underlying `ShardLoom` identifier validation fails.
    pub fn probe_deferred(summary: crate::VortexMetadataSummaryReport) -> Result<Self> {
        let mut out = Self::from_metadata_summary(summary)?;
        out.status = VortexMetadataPlanningStatus::ProbeDeferred;
        out.mode = VortexMetadataPlanningMode::PlanOnly;
        Ok(out)
    }

    /// Creates an unsupported metadata planning report with deterministic diagnostics.
    ///
    /// # Errors
    /// Returns errors only if underlying `ShardLoom` identifier validation fails.
    pub fn unsupported(
        summary: crate::VortexMetadataSummaryReport,
        feature: impl Into<String>,
        reason: impl Into<String>,
    ) -> Result<Self> {
        let mut out = Self::from_metadata_summary(summary)?;
        out.status = VortexMetadataPlanningStatus::Unsupported;
        out.mode = VortexMetadataPlanningMode::Unsupported;
        out.add_diagnostic(Diagnostic::unsupported(
            DiagnosticCode::NoFallbackExecution,
            feature,
            reason,
            None,
        ));
        Ok(out)
    }
    pub fn add_diagnostic(&mut self, diagnostic: Diagnostic) {
        self.diagnostics.push(diagnostic);
    }
    #[must_use]
    pub fn has_errors(&self) -> bool {
        self.status.is_error()
            || self
                .diagnostics
                .iter()
                .chain(self.explain_report.diagnostics.iter())
                .chain(self.estimate_report.diagnostics.iter())
                .any(|d| {
                    matches!(
                        d.severity,
                        DiagnosticSeverity::Error | DiagnosticSeverity::Fatal
                    )
                })
    }
    #[must_use]
    pub const fn is_plan_only(&self) -> bool {
        !self.data_executed
            && !self.data_materialized
            && !self.write_io
            && !self.fallback_execution_allowed
    }
    #[must_use]
    pub fn to_human_text(&self) -> String {
        let mut out = String::new();
        let _ = write!(
            out,
            "Vortex metadata planning
planning status: {}
planning mode: {}
metadata summary status: {}
scan plan present: {}
explain summary: {}
estimate summary: {}
data executed: {}
data materialized: {}
object-store IO: {}
write IO: {}
fallback execution disabled: {}",
            self.status.as_str(),
            self.mode.as_str(),
            self.metadata_summary.status.as_str(),
            self.scan_plan.is_some(),
            self.explain_report.operation_summary,
            self.estimate_report.operation_summary,
            self.data_executed,
            self.data_materialized,
            self.object_store_io,
            self.write_io,
            !self.fallback_execution_allowed
        );
        if self.diagnostics.is_empty() {
            out.push_str(
                "
diagnostics: none",
            );
        } else {
            out.push_str(
                "
diagnostics:",
            );
            for d in &self.diagnostics {
                let _ = write!(
                    out,
                    "
- {}",
                    d.to_human_text()
                );
            }
        }
        out
    }
}

/// Converts a metadata summary into plan/explain/estimate skeletons.
///
/// # Errors
/// Returns errors only if underlying `ShardLoom` identifier validation fails.
pub fn plan_from_vortex_metadata_summary(
    summary: crate::VortexMetadataSummaryReport,
) -> Result<VortexMetadataPlanningReport> {
    VortexMetadataPlanningReport::from_metadata_summary(summary)
}

/// Returns whether the metadata planning report is side-effect free.
#[must_use]
pub fn metadata_planning_is_side_effect_free(report: &VortexMetadataPlanningReport) -> bool {
    report.is_plan_only()
        && !report.object_store_io
        && !report.write_io
        && !report.fallback_execution_allowed
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn unsupported_is_error() {
        assert!(VortexMetadataPlanningStatus::Unsupported.is_error());
    }
    #[test]
    fn probe_deferred_not_error() {
        assert!(!VortexMetadataPlanningStatus::ProbeDeferred.is_error());
    }
    #[test]
    fn metadata_only_mode_no_exec() {
        assert!(!VortexMetadataPlanningMode::MetadataOnly.requires_data_execution());
    }
    #[test]
    fn plan_only_mode_no_exec() {
        assert!(!VortexMetadataPlanningMode::PlanOnly.requires_data_execution());
    }
    #[test]
    fn deferred_summary_report_flags() {
        let s = crate::VortexMetadataSummaryReport::from_probe_report(
            &crate::VortexMetadataProbeReport::deferred_api_unclear(),
        );
        let r = VortexMetadataPlanningReport::from_metadata_summary(s).expect("report");
        assert!(matches!(
            r.status,
            VortexMetadataPlanningStatus::ProbeDeferred | VortexMetadataPlanningStatus::Planned
        ));
        assert!(!r.data_executed);
        assert!(!r.data_materialized);
        assert!(!r.fallback_execution_allowed);
        assert!(r.is_plan_only());
        assert!(r.to_human_text().contains("fallback execution disabled"));
        assert!(r.to_human_text().contains("data executed: false"));
        assert!(r.to_human_text().contains("data materialized: false"));
    }
    #[test]
    fn human_text_with_diagnostics() {
        let mut s = crate::VortexMetadataSummaryReport::from_probe_report(
            &crate::VortexMetadataProbeReport::deferred_api_unclear(),
        );
        s.add_diagnostic(Diagnostic::no_fallback_execution("no fallback"));
        let r = VortexMetadataPlanningReport::from_metadata_summary(s).expect("report");
        assert!(r.to_human_text().contains("diagnostics:"));
    }
    #[test]
    fn helper_report_is_side_effect_free() {
        let s = crate::VortexMetadataSummaryReport::from_probe_report(
            &crate::VortexMetadataProbeReport::deferred_api_unclear(),
        );
        let r = plan_from_vortex_metadata_summary(s).expect("report");
        assert!(metadata_planning_is_side_effect_free(&r));
    }
    #[test]
    fn scan_plan_some_for_valid_uri() {
        let mut p = crate::VortexMetadataProbeReport::deferred_api_unclear();
        p.target_uri = Some(shardloom_core::DatasetUri::new("file://tmp/a.vortex").expect("uri"));
        let s = crate::VortexMetadataSummaryReport::from_probe_report(&p);
        let r = VortexMetadataPlanningReport::from_metadata_summary(s).expect("report");
        assert!(r.scan_plan.is_some());
        assert!(!r.scan_plan.expect("scan").request.requires_execution());
    }
}
