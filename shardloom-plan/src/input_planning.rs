//! Universal input planning bridge for `ShardLoom`.
//!
//! This module converts `InputAdapterReport` values into plan-only scan,
//! explain, and estimate artifacts. It performs no input reading, no file IO,
//! no object-store IO, and no external effects.

use std::fmt::Write as _;

use shardloom_core::{
    Diagnostic, DiagnosticCode, DiagnosticSeverity, InputAdapterReport, InputCapabilityStatus,
    InputMetadataAvailability, Result, UniversalInputSource, input_source_to_dataset_ref,
};

use crate::{
    EstimateReport, ExecutionBoundary, ExplainPlanNode, ExplainReport, PlanNodeId, PlanNodeKind,
    ScanPlanSkeleton, ScanRequest,
};

/// Planning status for universal input bridge reports.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InputPlanningStatus {
    Planned,
    MetadataDeferred,
    RequiresCredentials,
    RequiresExplicitEnablement,
    Unsupported,
}
impl InputPlanningStatus {
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::Planned => "planned",
            Self::MetadataDeferred => "metadata_deferred",
            Self::RequiresCredentials => "requires_credentials",
            Self::RequiresExplicitEnablement => "requires_explicit_enablement",
            Self::Unsupported => "unsupported",
        }
    }

    #[must_use]
    pub const fn is_error(&self) -> bool {
        matches!(self, Self::Unsupported)
    }
}

/// Planning mode for universal input bridge reports.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InputPlanningMode {
    PlanOnly,
    MetadataOnly,
    EffectPlanningOnly,
    Unsupported,
}
impl InputPlanningMode {
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::PlanOnly => "plan_only",
            Self::MetadataOnly => "metadata_only",
            Self::EffectPlanningOnly => "effect_planning_only",
            Self::Unsupported => "unsupported",
        }
    }
    #[must_use]
    pub const fn requires_data_execution(&self) -> bool {
        false
    }
    #[must_use]
    pub const fn executes_external_effects(&self) -> bool {
        false
    }
}

/// Plan-only bridge report from `InputAdapterReport` into planning surfaces.
#[derive(Debug, Clone, PartialEq)]
#[allow(clippy::struct_excessive_bools)]
pub struct UniversalInputPlanningReport {
    pub status: InputPlanningStatus,
    pub mode: InputPlanningMode,
    pub input_report: InputAdapterReport,
    pub dataset_ref: Option<shardloom_core::DatasetRef>,
    pub scan_request: Option<ScanRequest>,
    pub scan_plan: Option<ScanPlanSkeleton>,
    pub explain_report: ExplainReport,
    pub estimate_report: EstimateReport,
    pub data_read: bool,
    pub data_materialized: bool,
    pub object_store_io: bool,
    pub external_effects_executed: bool,
    pub write_io: bool,
    pub fallback_execution_allowed: bool,
    pub diagnostics: Vec<Diagnostic>,
}

impl UniversalInputPlanningReport {
    /// Builds a universal input planning report without performing IO or effects.
    ///
    /// # Errors
    /// Returns an error when dataset reference derivation fails.
    pub fn from_input_report(input_report: InputAdapterReport) -> Result<Self> {
        let dataset_ref = input_source_to_dataset_ref(&input_report.source)?;
        let scan_request = dataset_ref.clone().map(ScanRequest::new);
        let scan_plan = scan_request.clone().map(ScanPlanSkeleton::plan_only);

        let status = match input_report.capability_status {
            InputCapabilityStatus::RequiresCredentials => InputPlanningStatus::RequiresCredentials,
            InputCapabilityStatus::RequiresExplicitEnablement => {
                InputPlanningStatus::RequiresExplicitEnablement
            }
            InputCapabilityStatus::Unsupported | InputCapabilityStatus::Disabled => {
                InputPlanningStatus::Unsupported
            }
            _ if matches!(
                input_report.metadata_availability,
                InputMetadataAvailability::Deferred
            ) =>
            {
                InputPlanningStatus::MetadataDeferred
            }
            _ => InputPlanningStatus::Planned,
        };

        let mode = if matches!(status, InputPlanningStatus::RequiresExplicitEnablement) {
            InputPlanningMode::EffectPlanningOnly
        } else if matches!(status, InputPlanningStatus::Unsupported) {
            InputPlanningMode::Unsupported
        } else if input_report.source.is_native_vortex()
            && matches!(
                input_report.metadata_availability,
                InputMetadataAvailability::Available
            )
        {
            InputPlanningMode::MetadataOnly
        } else {
            InputPlanningMode::PlanOnly
        };

        let mut explain_report = ExplainReport::new("Universal input plan");
        explain_report.native_vortex_input = input_report.source.is_native_vortex();
        if let Some(uri) = &input_report.source.uri {
            explain_report.add_input_dataset(uri.as_str());
        }
        for d in &input_report.diagnostics {
            explain_report.add_diagnostic(d.clone());
        }

        if let Ok(node_id) = PlanNodeId::new("universal-input") {
            let kind = if matches!(status, InputPlanningStatus::Unsupported) {
                PlanNodeKind::Unsupported
            } else {
                PlanNodeKind::Scan
            };
            let state = if matches!(status, InputPlanningStatus::Unsupported) {
                shardloom_core::ExecutionState::Unsupported
            } else {
                shardloom_core::ExecutionState::MetadataOnly
            };
            let boundary = if input_report.source.is_native_vortex() {
                ExecutionBoundary::NativeVortexInput
            } else if matches!(status, InputPlanningStatus::Unsupported)
                || matches!(mode, InputPlanningMode::EffectPlanningOnly)
            {
                ExecutionBoundary::Unsupported
            } else {
                ExecutionBoundary::MetadataOnly
            };
            explain_report.add_node(
                ExplainPlanNode::new(node_id, kind, "universal input planning", state)
                    .with_boundary(boundary),
            );
        }

        let mut estimate_report = EstimateReport::unknown("Universal input estimate");
        estimate_report.add_uncertainty("Input metadata was not read.");
        estimate_report.add_uncertainty("No data was scanned.");
        estimate_report.add_uncertainty("No external effects were executed.");
        if input_report
            .source
            .source_kind
            .is_compatibility_structured()
        {
            estimate_report.add_uncertainty("Compatibility input statistics are deferred.");
        }
        if input_report.source.source_kind.is_effectful() {
            estimate_report.add_uncertainty("Effectful input requires explicit enablement.");
        }

        Ok(Self {
            status,
            mode,
            input_report,
            dataset_ref,
            scan_request,
            scan_plan,
            explain_report,
            estimate_report,
            data_read: false,
            data_materialized: false,
            object_store_io: false,
            external_effects_executed: false,
            write_io: false,
            fallback_execution_allowed: false,
            diagnostics: Vec::new(),
        })
    }

    /// Creates an unsupported report with no-fallback diagnostics.
    ///
    /// # Errors
    /// Returns an error when dataset reference derivation fails.
    pub fn unsupported(
        input_report: InputAdapterReport,
        feature: impl Into<String>,
        reason: impl Into<String>,
    ) -> Result<Self> {
        let mut report = Self::from_input_report(input_report)?;
        report.status = InputPlanningStatus::Unsupported;
        report.mode = InputPlanningMode::Unsupported;
        report.add_diagnostic(Diagnostic::unsupported(
            DiagnosticCode::NoFallbackExecution,
            feature,
            "Universal input planning bridge marked input as unsupported.",
            Some(reason.into()),
        ));
        Ok(report)
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
                .chain(self.input_report.diagnostics.iter())
                .any(|d| {
                    matches!(
                        d.severity,
                        DiagnosticSeverity::Error | DiagnosticSeverity::Fatal
                    )
                })
    }

    #[must_use]
    pub const fn is_side_effect_free(&self) -> bool {
        !self.data_read
            && !self.data_materialized
            && !self.object_store_io
            && !self.external_effects_executed
            && !self.write_io
            && !self.fallback_execution_allowed
    }

    #[must_use]
    pub fn to_human_text(&self) -> String {
        let mut out = String::new();
        let _ = writeln!(out, "planning status: {}", self.status.as_str());
        let _ = writeln!(out, "planning mode: {}", self.mode.as_str());
        let _ = writeln!(out, "input source: {}", self.input_report.source.summary());
        let _ = writeln!(out, "dataset ref present: {}", self.dataset_ref.is_some());
        let _ = writeln!(out, "scan request present: {}", self.scan_request.is_some());
        let _ = writeln!(out, "scan plan present: {}", self.scan_plan.is_some());
        let _ = writeln!(
            out,
            "explain summary: {}",
            self.explain_report.operation_summary
        );
        let _ = writeln!(
            out,
            "estimate summary: {}",
            self.estimate_report.operation_summary
        );
        out.push_str("data read: false\n");
        out.push_str("data materialized: false\n");
        out.push_str("object-store IO: false\n");
        out.push_str("external effects executed: false\n");
        out.push_str("write IO: false\n");
        out.push_str("fallback execution: disabled\n");
        if self.diagnostics.is_empty() && self.input_report.diagnostics.is_empty() {
            out.push_str("diagnostics: none\n");
        } else {
            out.push_str("diagnostics:\n");
            for diagnostic in self
                .diagnostics
                .iter()
                .chain(self.input_report.diagnostics.iter())
            {
                let _ = writeln!(out, "- {}", diagnostic.to_human_text());
            }
        }
        out
    }
}

/// Plans a `UniversalInputSource` into a plan-only bridge report.
///
/// # Errors
/// Returns an error when source normalization fails.
pub fn plan_universal_input_source(
    source: UniversalInputSource,
) -> Result<UniversalInputPlanningReport> {
    UniversalInputPlanningReport::from_input_report(InputAdapterReport::for_source(source))
}

/// Returns true when a planning report guarantees side-effect-free behavior.
#[must_use]
pub fn universal_input_planning_is_side_effect_free(report: &UniversalInputPlanningReport) -> bool {
    report.is_side_effect_free()
}

#[cfg(test)]
mod tests {
    use super::*;
    use shardloom_core::{DatasetUri, InputSourceId, InputSourceKind, UniversalInputSource};

    #[test]
    fn status_unsupported_is_error() {
        assert!(InputPlanningStatus::Unsupported.is_error());
    }
    #[test]
    fn status_metadata_deferred_not_error() {
        assert!(!InputPlanningStatus::MetadataDeferred.is_error());
    }
    #[test]
    fn mode_plan_only_no_data_execution() {
        assert!(!InputPlanningMode::PlanOnly.requires_data_execution());
    }
    #[test]
    fn mode_effect_planning_only_no_external_effects() {
        assert!(!InputPlanningMode::EffectPlanningOnly.executes_external_effects());
    }

    #[test]
    fn vortex_report_side_effect_free_with_dataset_and_plan() {
        let source = UniversalInputSource::from_dataset_uri(
            DatasetUri::new("file://tmp/a.vortex").expect("uri"),
        )
        .expect("source");
        let report = plan_universal_input_source(source).expect("report");
        assert!(report.is_side_effect_free());
        assert!(report.dataset_ref.is_some());
        assert!(report.scan_request.is_some() || report.scan_plan.is_some());
        assert!(
            !report.data_read
                && !report.data_materialized
                && !report.object_store_io
                && !report.external_effects_executed
        );
        assert!(!report.fallback_execution_allowed);
    }

    #[test]
    fn parquet_report_side_effect_free_and_not_native_vortex() {
        let source = UniversalInputSource::from_dataset_uri(
            DatasetUri::new("file://tmp/a.parquet").expect("uri"),
        )
        .expect("source");
        let report = plan_universal_input_source(source).expect("report");
        assert!(report.is_side_effect_free());
        assert!(!report.explain_report.native_vortex_input);
    }

    #[test]
    fn unsupported_has_errors_and_no_fallback_attempted() {
        let source = UniversalInputSource::new(
            InputSourceId::new("u").expect("id"),
            InputSourceKind::Unknown,
        );
        let report = UniversalInputPlanningReport::unsupported(
            InputAdapterReport::for_source(source),
            "test",
            "unsupported",
        )
        .expect("report");
        assert!(report.has_errors());
        assert!(report.diagnostics.iter().any(|d| !d.fallback.attempted));
    }

    #[test]
    fn human_text_contains_required_statements() {
        let source = UniversalInputSource::from_dataset_uri(
            DatasetUri::new("file://tmp/a.vortex").expect("uri"),
        )
        .expect("source");
        let report = plan_universal_input_source(source).expect("report");
        let text = report.to_human_text();
        assert!(text.contains("fallback execution: disabled"));
        assert!(text.contains("data read: false"));
        assert!(text.contains("external effects executed: false"));
    }

    #[test]
    fn side_effect_free_helper_delegates() {
        let source = UniversalInputSource::from_dataset_uri(
            DatasetUri::new("file://tmp/a.vortex").expect("uri"),
        )
        .expect("source");
        let report = plan_universal_input_source(source).expect("report");
        assert!(universal_input_planning_is_side_effect_free(&report));
    }
}
