use std::fmt::Write as _;

use shardloom_core::{
    Diagnostic, DiagnosticCode, DiagnosticSeverity, InputAdapterReport, Result,
    UniversalInputSource,
};

use crate::{
    VortexMetadataOpenReport, VortexMetadataOpenRequest, VortexMetadataOpenStatus,
    VortexMetadataPlanningReport, VortexMetadataPruningReport, VortexMetadataSummaryReport,
    open_vortex_metadata_only, plan_from_vortex_metadata_summary, plan_vortex_metadata_pruning,
};

/// Status for `Vortex` native universal input bridge planning in `ShardLoom`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VortexUniversalInputStatus {
    Planned,
    NativeVortexInput,
    MetadataOpenFeatureDisabled,
    MetadataDeferred,
    MetadataAvailable,
    CompatibilityInputUnsupported,
    EffectfulInputUnsupported,
    InvalidInput,
    Unsupported,
}
impl VortexUniversalInputStatus {
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::Planned => "planned",
            Self::NativeVortexInput => "native_vortex_input",
            Self::MetadataOpenFeatureDisabled => "metadata_open_feature_disabled",
            Self::MetadataDeferred => "metadata_deferred",
            Self::MetadataAvailable => "metadata_available",
            Self::CompatibilityInputUnsupported => "compatibility_input_unsupported",
            Self::EffectfulInputUnsupported => "effectful_input_unsupported",
            Self::InvalidInput => "invalid_input",
            Self::Unsupported => "unsupported",
        }
    }
    #[must_use]
    pub const fn is_error(&self) -> bool {
        matches!(
            self,
            Self::CompatibilityInputUnsupported
                | Self::EffectfulInputUnsupported
                | Self::InvalidInput
                | Self::Unsupported
        )
    }
}

/// Mode for `VortexUniversalInputPlan`; this bridge is always plan-only in this phase.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VortexUniversalInputMode {
    ReportOnly,
    MetadataOnly,
    PlanOnly,
    Unsupported,
}
impl VortexUniversalInputMode {
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::ReportOnly => "report_only",
            Self::MetadataOnly => "metadata_only",
            Self::PlanOnly => "plan_only",
            Self::Unsupported => "unsupported",
        }
    }
    #[must_use]
    pub const fn requires_data_execution(&self) -> bool {
        false
    }
    #[must_use]
    pub const fn performs_external_effects(&self) -> bool {
        false
    }
}

/// Bridge report from `UniversalInputSource` into `Vortex` metadata planning artifacts.
#[derive(Debug, Clone, PartialEq)]
#[allow(clippy::struct_excessive_bools)]
pub struct VortexUniversalInputPlan {
    pub status: VortexUniversalInputStatus,
    pub mode: VortexUniversalInputMode,
    pub source: UniversalInputSource,
    pub input_report: InputAdapterReport,
    pub metadata_open_report: Option<VortexMetadataOpenReport>,
    pub metadata_summary_report: Option<VortexMetadataSummaryReport>,
    pub metadata_planning_report: Option<VortexMetadataPlanningReport>,
    pub metadata_pruning_report: Option<VortexMetadataPruningReport>,
    pub data_read: bool,
    pub data_materialized: bool,
    pub object_store_io: bool,
    pub write_io: bool,
    pub external_effects_executed: bool,
    pub fallback_execution_allowed: bool,
    pub diagnostics: Vec<Diagnostic>,
}
impl VortexUniversalInputPlan {
    /// Builds a plan-only bridge from a normalized `UniversalInputSource`.
    ///
    /// # Errors
    /// Returns errors from metadata summary/planning/pruning helper contracts.
    pub fn from_source(source: UniversalInputSource) -> Result<Self> {
        let input_report = InputAdapterReport::for_source(source.clone());
        if source.source_kind.is_compatibility_structured() {
            return Ok(Self::unsupported(
                source,
                "vortex-universal-input-bridge",
                "Compatibility input is explicit and unsupported in this native Vortex bridge.",
            ));
        }
        if source.source_kind.is_effectful() {
            let mut plan = Self::unsupported(
                source,
                "vortex-universal-input-bridge",
                "Effectful inputs require explicit enablement and are not executed by this bridge.",
            );
            plan.status = VortexUniversalInputStatus::EffectfulInputUnsupported;
            return Ok(plan);
        }
        let mut out = Self {
            status: VortexUniversalInputStatus::Planned,
            mode: VortexUniversalInputMode::PlanOnly,
            source: source.clone(),
            input_report,
            metadata_open_report: None,
            metadata_summary_report: None,
            metadata_planning_report: None,
            metadata_pruning_report: None,
            data_read: false,
            data_materialized: false,
            object_store_io: false,
            write_io: false,
            external_effects_executed: false,
            fallback_execution_allowed: false,
            diagnostics: vec![],
        };
        if !source.is_native_vortex() {
            return Ok(Self::unsupported(
                source,
                "vortex-universal-input-bridge",
                "Source is not a native Vortex input.",
            ));
        }
        out.status = VortexUniversalInputStatus::NativeVortexInput;
        let Some(uri) = source.uri.clone() else {
            out.status = VortexUniversalInputStatus::MetadataDeferred;
            out.mode = VortexUniversalInputMode::ReportOnly;
            out.add_diagnostic(Diagnostic::configuration_error(
                "vortex-universal-input-bridge",
                "Native Vortex input did not include a URI.",
                "Provide a DatasetUri ending with .vortex.",
            ));
            return Ok(out);
        };
        let open_report = open_vortex_metadata_only(VortexMetadataOpenRequest::metadata_only(uri))?;
        out.metadata_summary_report
            .clone_from(&open_report.metadata_summary);
        match open_report.open_status {
            VortexMetadataOpenStatus::FeatureDisabled => {
                out.status = VortexUniversalInputStatus::MetadataOpenFeatureDisabled;
                out.mode = VortexUniversalInputMode::ReportOnly;
            }
            VortexMetadataOpenStatus::ApiDeferred => {
                out.status = VortexUniversalInputStatus::MetadataDeferred;
                out.mode = VortexUniversalInputMode::ReportOnly;
            }
            VortexMetadataOpenStatus::OpenedMetadataOnly => {
                out.status = if open_report.metadata_summary.is_some() {
                    VortexUniversalInputStatus::MetadataAvailable
                } else {
                    VortexUniversalInputStatus::MetadataDeferred
                };
                out.mode = VortexUniversalInputMode::MetadataOnly;
            }
            VortexMetadataOpenStatus::InvalidTarget => {
                out.status = VortexUniversalInputStatus::InvalidInput;
                out.mode = VortexUniversalInputMode::Unsupported;
            }
            VortexMetadataOpenStatus::FileMissing | VortexMetadataOpenStatus::Unsupported => {
                out.status = VortexUniversalInputStatus::Unsupported;
                out.mode = VortexUniversalInputMode::Unsupported;
            }
            VortexMetadataOpenStatus::Planned => {}
        }
        for d in &open_report.diagnostics {
            out.add_diagnostic(d.clone());
        }
        if let Some(summary) = out.metadata_summary_report.clone() {
            let planning = plan_from_vortex_metadata_summary(summary)?;
            out.metadata_pruning_report =
                Some(plan_vortex_metadata_pruning(planning.clone(), None)?);
            out.metadata_planning_report = Some(planning);
        }
        out.metadata_open_report = Some(open_report);
        Ok(out)
    }

    #[must_use]
    pub fn unsupported(
        source: UniversalInputSource,
        feature: impl Into<String>,
        reason: impl Into<String>,
    ) -> Self {
        let input_report = InputAdapterReport::for_source(source.clone());
        let diagnostics = vec![Diagnostic::unsupported(
            DiagnosticCode::NoFallbackExecution,
            feature,
            reason,
            Some("Fallback attempted: false".to_string()),
        )];
        Self {
            status: VortexUniversalInputStatus::CompatibilityInputUnsupported,
            mode: VortexUniversalInputMode::Unsupported,
            source,
            input_report,
            metadata_open_report: None,
            metadata_summary_report: None,
            metadata_planning_report: None,
            metadata_pruning_report: None,
            data_read: false,
            data_materialized: false,
            object_store_io: false,
            write_io: false,
            external_effects_executed: false,
            fallback_execution_allowed: false,
            diagnostics,
        }
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
            || self
                .metadata_open_report
                .as_ref()
                .is_some_and(VortexMetadataOpenReport::has_errors)
            || self
                .metadata_summary_report
                .as_ref()
                .is_some_and(VortexMetadataSummaryReport::has_errors)
            || self
                .metadata_planning_report
                .as_ref()
                .is_some_and(VortexMetadataPlanningReport::has_errors)
            || self
                .metadata_pruning_report
                .as_ref()
                .is_some_and(VortexMetadataPruningReport::has_errors)
    }
    #[must_use]
    pub const fn is_side_effect_free(&self) -> bool {
        !self.data_read
            && !self.data_materialized
            && !self.object_store_io
            && !self.write_io
            && !self.external_effects_executed
            && !self.fallback_execution_allowed
    }
    #[must_use]
    pub fn to_human_text(&self) -> String {
        let mut out = String::new();
        let _ = writeln!(out, "status: {}", self.status.as_str());
        let _ = writeln!(out, "mode: {}", self.mode.as_str());
        let _ = writeln!(out, "input source: {}", self.source.summary());
        let _ = writeln!(
            out,
            "input adapter report: capability={} metadata={}",
            self.input_report.capability_status.as_str(),
            self.input_report.metadata_availability.as_str()
        );
        let _ = writeln!(
            out,
            "metadata open report: {}",
            self.metadata_open_report
                .as_ref()
                .map_or("none".to_string(), |r| r.open_status.as_str().to_string())
        );
        let _ = writeln!(
            out,
            "metadata summary report present: {}",
            self.metadata_summary_report.is_some()
        );
        let _ = writeln!(
            out,
            "metadata planning report present: {}",
            self.metadata_planning_report.is_some()
        );
        let _ = writeln!(
            out,
            "metadata pruning report present: {}",
            self.metadata_pruning_report.is_some()
        );
        out.push_str("data read: false\n");
        out.push_str("data materialized: false\n");
        out.push_str("object-store IO: false\n");
        out.push_str("write IO: false\n");
        out.push_str("external effects executed: false\n");
        out.push_str("fallback execution: disabled\n");
        if self.diagnostics.is_empty() {
            out.push_str("diagnostics: none\n");
        } else {
            out.push_str("diagnostics:\n");
            for d in &self.diagnostics {
                let _ = writeln!(out, "- {}", d.to_human_text());
            }
        }
        out
    }
}

/// Plans a native `Vortex` `UniversalInputSource` bridge without scan execution.
///
/// # Errors
/// Returns errors from `VortexUniversalInputPlan::from_source`.
pub fn plan_native_vortex_universal_input(
    source: UniversalInputSource,
) -> Result<VortexUniversalInputPlan> {
    VortexUniversalInputPlan::from_source(source)
}

#[must_use]
pub fn vortex_universal_input_plan_is_side_effect_free(plan: &VortexUniversalInputPlan) -> bool {
    plan.is_side_effect_free()
}

#[cfg(test)]
mod tests {
    use super::*;
    use shardloom_core::{DatasetUri, InputSourceId, InputSourceKind};
    #[test]
    fn status_unsupported_is_error() {
        assert!(VortexUniversalInputStatus::Unsupported.is_error());
    }
    #[test]
    fn status_metadata_deferred_not_error() {
        assert!(!VortexUniversalInputStatus::MetadataDeferred.is_error());
    }
    #[test]
    fn mode_metadata_no_exec() {
        assert!(!VortexUniversalInputMode::MetadataOnly.requires_data_execution());
    }
    #[test]
    fn mode_plan_only_no_effects() {
        assert!(!VortexUniversalInputMode::PlanOnly.performs_external_effects());
    }
    #[test]
    fn vortex_plan_side_effect_free_and_flags() {
        let s = UniversalInputSource::from_dataset_uri(
            DatasetUri::new("file://tmp/a.vortex").expect("uri"),
        )
        .expect("source");
        let p = plan_native_vortex_universal_input(s).expect("plan");
        assert!(p.is_side_effect_free());
        assert!(
            !p.fallback_execution_allowed
                && !p.data_materialized
                && !p.write_io
                && !p.object_store_io
        );
    }
    #[test]
    fn parquet_unsupported() {
        let s = UniversalInputSource::from_dataset_uri(
            DatasetUri::new("file://tmp/a.parquet").expect("uri"),
        )
        .expect("source");
        let p = plan_native_vortex_universal_input(s).expect("plan");
        assert!(p.has_errors());
        assert!(!p.fallback_execution_allowed);
    }
    #[test]
    fn effectful_unsupported_no_effects() {
        let s =
            UniversalInputSource::new(InputSourceId::new("x").expect("id"), InputSourceKind::Api);
        let p = plan_native_vortex_universal_input(s).expect("plan");
        assert!(matches!(
            p.status,
            VortexUniversalInputStatus::EffectfulInputUnsupported
                | VortexUniversalInputStatus::Unsupported
        ));
        assert!(!p.external_effects_executed);
    }
    #[test]
    fn unsupported_has_errors() {
        let s = UniversalInputSource::new(
            InputSourceId::new("u").expect("id"),
            InputSourceKind::Unknown,
        );
        let p = VortexUniversalInputPlan::unsupported(s, "test", "unsupported");
        assert!(p.has_errors());
        assert!(p.diagnostics.iter().any(|d| !d.fallback.attempted));
    }
    #[test]
    fn human_text_includes_flags_and_diagnostics() {
        let s = UniversalInputSource::new(
            InputSourceId::new("u").expect("id"),
            InputSourceKind::Unknown,
        );
        let p = VortexUniversalInputPlan::unsupported(s, "test", "unsupported");
        let t = p.to_human_text();
        assert!(t.contains("fallback execution: disabled"));
        assert!(t.contains("data materialized: false"));
        assert!(t.contains("external effects executed: false"));
        assert!(t.contains("diagnostics:"));
    }
    #[test]
    fn helper_side_effect_free() {
        let s = UniversalInputSource::from_dataset_uri(
            DatasetUri::new("file://tmp/a.vortex").expect("uri"),
        )
        .expect("source");
        let p = plan_native_vortex_universal_input(s).expect("plan");
        assert!(vortex_universal_input_plan_is_side_effect_free(&p));
    }
}
