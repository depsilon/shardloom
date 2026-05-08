//! CG-18 universal import, deployment, and external-baseline harness contracts.
//!
//! This module defines report-only harness evidence. It does not import plans,
//! publish packages, deploy services, invoke Foundry, run external baselines, or
//! execute `ShardLoom` runtime work.

use crate::{BaselineEngine, Diagnostic, DiagnosticSeverity};

/// Report-level status for the CG-18 universal harness surface.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UniversalHarnessStatus {
    ReportOnlyPlanned,
    EvidenceIncomplete,
    HarnessCertified,
    Blocked,
}

impl UniversalHarnessStatus {
    /// Stable machine-readable status label.
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::ReportOnlyPlanned => "report_only_planned",
            Self::EvidenceIncomplete => "evidence_incomplete",
            Self::HarnessCertified => "harness_certified",
            Self::Blocked => "blocked",
        }
    }

    /// Returns whether this status should fail a report command.
    #[must_use]
    pub const fn is_error(&self) -> bool {
        matches!(self, Self::Blocked)
    }
}

/// Harness surface families CG-18 must keep explicit.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UniversalHarnessSurfaceKind {
    CliJsonRunner,
    PackageImport,
    DeploymentProfile,
    FoundryExample,
    ExternalBaselineRunner,
    ComparisonReportDataset,
    PortabilityCheck,
}

impl UniversalHarnessSurfaceKind {
    /// Stable machine-readable surface label.
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::CliJsonRunner => "cli_json_runner",
            Self::PackageImport => "package_import",
            Self::DeploymentProfile => "deployment_profile",
            Self::FoundryExample => "foundry_example",
            Self::ExternalBaselineRunner => "external_baseline_runner",
            Self::ComparisonReportDataset => "comparison_report_dataset",
            Self::PortabilityCheck => "portability_check",
        }
    }
}

/// Validation status for a harness surface.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UniversalHarnessSurfaceStatus {
    Planned,
    RequiresProtocolEvidence,
    RequiresPortabilityEvidence,
    RequiresBaselineEvidence,
    Certified,
    Unsupported,
}

impl UniversalHarnessSurfaceStatus {
    /// Stable machine-readable status label.
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::Planned => "planned",
            Self::RequiresProtocolEvidence => "requires_protocol_evidence",
            Self::RequiresPortabilityEvidence => "requires_portability_evidence",
            Self::RequiresBaselineEvidence => "requires_baseline_evidence",
            Self::Certified => "certified",
            Self::Unsupported => "unsupported",
        }
    }

    /// Returns whether this surface status is an error.
    #[must_use]
    pub const fn is_error(&self) -> bool {
        matches!(self, Self::Unsupported)
    }
}

/// One report-only universal harness surface.
#[derive(Debug, Clone, PartialEq, Eq)]
#[allow(clippy::struct_excessive_bools)]
pub struct UniversalHarnessSurface {
    pub surface_id: String,
    pub kind: UniversalHarnessSurfaceKind,
    pub status: UniversalHarnessSurfaceStatus,
    pub protocol_evidence_required: bool,
    pub portability_evidence_required: bool,
    pub package_import_evidence_required: bool,
    pub deployment_evidence_required: bool,
    pub baseline_evidence_required: bool,
    pub foundry_required: bool,
    pub foundry_optional_example: bool,
    pub external_only: bool,
    pub runtime_execution: bool,
    pub fallback_attempted: bool,
}

impl UniversalHarnessSurface {
    /// Creates a planned report-only harness surface.
    #[must_use]
    pub fn planned(surface_id: impl Into<String>, kind: UniversalHarnessSurfaceKind) -> Self {
        Self {
            surface_id: surface_id.into(),
            kind,
            status: UniversalHarnessSurfaceStatus::Planned,
            protocol_evidence_required: true,
            portability_evidence_required: matches!(
                kind,
                UniversalHarnessSurfaceKind::PortabilityCheck
                    | UniversalHarnessSurfaceKind::ComparisonReportDataset
            ),
            package_import_evidence_required: matches!(
                kind,
                UniversalHarnessSurfaceKind::PackageImport
                    | UniversalHarnessSurfaceKind::DeploymentProfile
            ),
            deployment_evidence_required: matches!(
                kind,
                UniversalHarnessSurfaceKind::DeploymentProfile
                    | UniversalHarnessSurfaceKind::FoundryExample
            ),
            baseline_evidence_required: matches!(
                kind,
                UniversalHarnessSurfaceKind::ExternalBaselineRunner
                    | UniversalHarnessSurfaceKind::ComparisonReportDataset
            ),
            foundry_required: false,
            foundry_optional_example: matches!(kind, UniversalHarnessSurfaceKind::FoundryExample),
            external_only: matches!(
                kind,
                UniversalHarnessSurfaceKind::ExternalBaselineRunner
                    | UniversalHarnessSurfaceKind::FoundryExample
            ),
            runtime_execution: false,
            fallback_attempted: false,
        }
    }

    /// Returns whether this surface has an error state.
    #[must_use]
    pub const fn has_errors(&self) -> bool {
        self.status.is_error()
            || self.foundry_required
            || self.runtime_execution
            || self.fallback_attempted
    }
}

/// Required external-baseline harness entry.
#[derive(Debug, Clone, PartialEq, Eq)]
#[allow(clippy::struct_excessive_bools)]
pub struct ExternalBaselineHarnessRequirement {
    pub baseline_engine: BaselineEngine,
    pub engine_version_required: bool,
    pub workload_id_required: bool,
    pub fixture_id_required: bool,
    pub command_or_transform_required: bool,
    pub correctness_result_required: bool,
    pub benchmark_metrics_required: bool,
    pub comparison_report_required: bool,
    pub external_only: bool,
    pub runner_execution_performed: bool,
    pub fallback_execution_allowed: bool,
    pub fallback_attempted: bool,
}

impl ExternalBaselineHarnessRequirement {
    /// Creates a report-only external-baseline requirement.
    #[must_use]
    pub const fn external_only(baseline_engine: BaselineEngine) -> Self {
        Self {
            baseline_engine,
            engine_version_required: true,
            workload_id_required: true,
            fixture_id_required: true,
            command_or_transform_required: true,
            correctness_result_required: true,
            benchmark_metrics_required: true,
            comparison_report_required: true,
            external_only: true,
            runner_execution_performed: false,
            fallback_execution_allowed: false,
            fallback_attempted: false,
        }
    }

    /// Returns whether this baseline requirement has an error state.
    #[must_use]
    pub const fn has_errors(&self) -> bool {
        !self.external_only
            || self.runner_execution_performed
            || self.fallback_execution_allowed
            || self.fallback_attempted
    }
}

/// Report-only CG-18 universal harness plan.
#[derive(Debug, Clone, PartialEq, Eq)]
#[allow(clippy::struct_excessive_bools)]
pub struct UniversalHarnessReport {
    pub schema_version: &'static str,
    pub report_id: String,
    pub status: UniversalHarnessStatus,
    pub runner_contract_fields: Vec<&'static str>,
    pub surfaces: Vec<UniversalHarnessSurface>,
    pub external_baselines: Vec<ExternalBaselineHarnessRequirement>,
    pub output_envelope_required: bool,
    pub stable_command_schema_required: bool,
    pub exit_code_required: bool,
    pub diagnostics_required: bool,
    pub side_effect_manifest_required: bool,
    pub output_artifacts_required: bool,
    pub metrics_required: bool,
    pub comparison_dataset_required: bool,
    pub correctness_evidence_required: bool,
    pub benchmark_evidence_required: bool,
    pub foundry_required: bool,
    pub foundry_optional_example: bool,
    pub package_import_performed: bool,
    pub deployment_performed: bool,
    pub external_baseline_execution: bool,
    pub runtime_execution: bool,
    pub filesystem_probe: bool,
    pub network_probe: bool,
    pub catalog_probe: bool,
    pub adapter_probe: bool,
    pub read_io: bool,
    pub write_io: bool,
    pub external_publish: bool,
    pub fallback_execution_allowed: bool,
    pub fallback_attempted: bool,
    pub production_claim_allowed: bool,
    pub diagnostics: Vec<Diagnostic>,
}

impl UniversalHarnessReport {
    /// Creates the CG-18 report-only foundation.
    #[must_use]
    pub fn cg18_foundation() -> Self {
        Self {
            schema_version: "shardloom.universal_harness.v1",
            report_id: "cg18.universal-harness".to_string(),
            status: UniversalHarnessStatus::ReportOnlyPlanned,
            runner_contract_fields: cg18_runner_contract_fields(),
            surfaces: cg18_harness_surfaces(),
            external_baselines: cg18_external_baselines(),
            output_envelope_required: true,
            stable_command_schema_required: true,
            exit_code_required: true,
            diagnostics_required: true,
            side_effect_manifest_required: true,
            output_artifacts_required: true,
            metrics_required: true,
            comparison_dataset_required: true,
            correctness_evidence_required: true,
            benchmark_evidence_required: true,
            foundry_required: false,
            foundry_optional_example: true,
            package_import_performed: false,
            deployment_performed: false,
            external_baseline_execution: false,
            runtime_execution: false,
            filesystem_probe: false,
            network_probe: false,
            catalog_probe: false,
            adapter_probe: false,
            read_io: false,
            write_io: false,
            external_publish: false,
            fallback_execution_allowed: false,
            fallback_attempted: false,
            production_claim_allowed: false,
            diagnostics: Vec::new(),
        }
    }

    /// Number of planned harness surfaces.
    #[must_use]
    pub fn surface_count(&self) -> usize {
        self.surfaces.len()
    }

    /// Number of external-baseline requirements.
    #[must_use]
    pub fn external_baseline_count(&self) -> usize {
        self.external_baselines.len()
    }

    /// Stable comma-separated surface-kind order for CLI reporting.
    #[must_use]
    pub fn surface_kind_order(&self) -> String {
        self.surfaces
            .iter()
            .map(|surface| surface.kind.as_str())
            .collect::<Vec<_>>()
            .join(",")
    }

    /// Stable comma-separated runner contract field order for CLI reporting.
    #[must_use]
    pub fn runner_contract_field_order(&self) -> String {
        self.runner_contract_fields.join(",")
    }

    /// Stable comma-separated baseline-engine order for CLI reporting.
    #[must_use]
    pub fn baseline_engine_order(&self) -> String {
        self.external_baselines
            .iter()
            .map(|requirement| requirement.baseline_engine.as_str())
            .collect::<Vec<_>>()
            .join(",")
    }

    /// Returns whether the report avoids all execution and IO side effects.
    #[must_use]
    pub const fn is_side_effect_free(&self) -> bool {
        !self.package_import_performed
            && !self.deployment_performed
            && !self.external_baseline_execution
            && !self.runtime_execution
            && !self.filesystem_probe
            && !self.network_probe
            && !self.catalog_probe
            && !self.adapter_probe
            && !self.read_io
            && !self.write_io
            && !self.external_publish
            && !self.fallback_execution_allowed
            && !self.fallback_attempted
    }

    /// Returns whether the report contains errors.
    #[must_use]
    pub fn has_errors(&self) -> bool {
        self.status.is_error()
            || self.foundry_required
            || self
                .surfaces
                .iter()
                .any(UniversalHarnessSurface::has_errors)
            || self
                .external_baselines
                .iter()
                .any(ExternalBaselineHarnessRequirement::has_errors)
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
            "universal harness plan\nschema_version: {}\nreport: {}\nstatus: {}\nsurfaces: {}\nexternal baselines: {}\nfoundry required: false\npackage import/deployment: disabled\nexternal baseline execution: disabled\nfallback execution: disabled",
            self.schema_version,
            self.report_id,
            self.status.as_str(),
            self.surface_count(),
            self.external_baseline_count(),
        )
    }
}

fn cg18_runner_contract_fields() -> Vec<&'static str> {
    vec![
        "command",
        "schema_version",
        "exit_code",
        "status",
        "diagnostics",
        "fallback_execution_allowed",
        "side_effects",
        "output_artifacts",
        "metrics",
    ]
}

fn cg18_harness_surfaces() -> Vec<UniversalHarnessSurface> {
    vec![
        UniversalHarnessSurface::planned(
            "harness.cli_json_runner",
            UniversalHarnessSurfaceKind::CliJsonRunner,
        ),
        UniversalHarnessSurface::planned(
            "harness.package_import",
            UniversalHarnessSurfaceKind::PackageImport,
        ),
        UniversalHarnessSurface::planned(
            "harness.deployment_profile",
            UniversalHarnessSurfaceKind::DeploymentProfile,
        ),
        UniversalHarnessSurface::planned(
            "harness.foundry_example",
            UniversalHarnessSurfaceKind::FoundryExample,
        ),
        UniversalHarnessSurface::planned(
            "harness.external_baseline_runner",
            UniversalHarnessSurfaceKind::ExternalBaselineRunner,
        ),
        UniversalHarnessSurface::planned(
            "harness.comparison_report_dataset",
            UniversalHarnessSurfaceKind::ComparisonReportDataset,
        ),
        UniversalHarnessSurface::planned(
            "harness.portability_check",
            UniversalHarnessSurfaceKind::PortabilityCheck,
        ),
    ]
}

fn cg18_external_baselines() -> Vec<ExternalBaselineHarnessRequirement> {
    vec![
        ExternalBaselineHarnessRequirement::external_only(BaselineEngine::Spark),
        ExternalBaselineHarnessRequirement::external_only(BaselineEngine::DataFusion),
        ExternalBaselineHarnessRequirement::external_only(BaselineEngine::Polars),
    ]
}

/// Produces the CG-18 universal harness report.
#[must_use]
pub fn plan_universal_harness() -> UniversalHarnessReport {
    UniversalHarnessReport::cg18_foundation()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn universal_harness_foundation_is_report_only() {
        let report = UniversalHarnessReport::cg18_foundation();

        assert_eq!(report.status, UniversalHarnessStatus::ReportOnlyPlanned);
        assert_eq!(report.surface_count(), 7);
        assert_eq!(report.external_baseline_count(), 3);
        assert_eq!(
            report.runner_contract_field_order(),
            "command,schema_version,exit_code,status,diagnostics,fallback_execution_allowed,side_effects,output_artifacts,metrics"
        );
        assert_eq!(
            report.surface_kind_order(),
            "cli_json_runner,package_import,deployment_profile,foundry_example,external_baseline_runner,comparison_report_dataset,portability_check"
        );
        assert_eq!(report.baseline_engine_order(), "spark,datafusion,polars");
        assert!(report.output_envelope_required);
        assert!(report.stable_command_schema_required);
        assert!(report.comparison_dataset_required);
        assert!(report.correctness_evidence_required);
        assert!(report.benchmark_evidence_required);
        assert!(!report.foundry_required);
        assert!(report.foundry_optional_example);
        assert!(report.is_side_effect_free());
        assert!(!report.has_errors());
        assert!(!report.package_import_performed);
        assert!(!report.deployment_performed);
        assert!(!report.external_baseline_execution);
        assert!(!report.runtime_execution);
        assert!(!report.external_publish);
        assert!(!report.fallback_execution_allowed);
        assert!(!report.fallback_attempted);
        assert!(!report.production_claim_allowed);
    }
}
