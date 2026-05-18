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

/// Execution-admission status for the universal harness.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UniversalHarnessExecutionGateStatus {
    BlockedMissingEvidence,
    ExecutionAdmitted,
    BlockedPolicy,
}

impl UniversalHarnessExecutionGateStatus {
    /// Stable machine-readable status label.
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::BlockedMissingEvidence => "blocked_missing_evidence",
            Self::ExecutionAdmitted => "execution_admitted",
            Self::BlockedPolicy => "blocked_policy",
        }
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
    pub optional_environment_only: bool,
    pub shardloom_runtime_dependency_allowed: bool,
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
            optional_environment_only: true,
            shardloom_runtime_dependency_allowed: false,
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
            || !self.optional_environment_only
            || self.shardloom_runtime_dependency_allowed
            || self.runner_execution_performed
            || self.fallback_execution_allowed
            || self.fallback_attempted
    }
}

/// Import/deployment harness environments required by CG-18.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UniversalHarnessEnvironmentKind {
    Local,
    ContinuousIntegration,
    Container,
    FoundryOptional,
    BenchmarkExtrasOptional,
}

impl UniversalHarnessEnvironmentKind {
    /// Stable machine-readable environment label.
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::Local => "local",
            Self::ContinuousIntegration => "ci",
            Self::Container => "container",
            Self::FoundryOptional => "foundry_optional",
            Self::BenchmarkExtrasOptional => "benchmark_extras_optional",
        }
    }
}

/// Maturity status for a universal import/deployment harness environment.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UniversalHarnessEnvironmentStatus {
    Required,
    EvidenceIncomplete,
    Certified,
    Blocked,
}

impl UniversalHarnessEnvironmentStatus {
    /// Stable machine-readable status label.
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::Required => "required",
            Self::EvidenceIncomplete => "evidence_incomplete",
            Self::Certified => "certified",
            Self::Blocked => "blocked",
        }
    }

    /// Returns whether this environment status is an error.
    #[must_use]
    pub const fn is_error(&self) -> bool {
        matches!(self, Self::Blocked)
    }
}

/// One required local/CI/container/optional-platform harness contract.
#[derive(Debug, Clone, PartialEq, Eq)]
#[allow(clippy::struct_excessive_bools)]
pub struct UniversalHarnessEnvironmentRequirement {
    pub environment_id: &'static str,
    pub kind: UniversalHarnessEnvironmentKind,
    pub status: UniversalHarnessEnvironmentStatus,
    pub command_contract: &'static str,
    pub environment_file_required: bool,
    pub clean_import_required: bool,
    pub cli_binary_resolution_required: bool,
    pub output_envelope_fixture_required: bool,
    pub artifact_root_required: bool,
    pub foundry_optional: bool,
    pub optional_benchmark_environment: bool,
    pub external_engines_runtime_dependencies_allowed: bool,
    pub package_publication_required: bool,
    pub harness_execution_performed: bool,
    pub filesystem_probe: bool,
    pub network_probe: bool,
    pub external_engine_invoked: bool,
    pub fallback_attempted: bool,
}

impl UniversalHarnessEnvironmentRequirement {
    /// Creates a report-only harness environment requirement.
    #[must_use]
    pub const fn required(
        environment_id: &'static str,
        kind: UniversalHarnessEnvironmentKind,
        command_contract: &'static str,
    ) -> Self {
        Self {
            environment_id,
            kind,
            status: UniversalHarnessEnvironmentStatus::Required,
            command_contract,
            environment_file_required: true,
            clean_import_required: true,
            cli_binary_resolution_required: true,
            output_envelope_fixture_required: true,
            artifact_root_required: true,
            foundry_optional: matches!(kind, UniversalHarnessEnvironmentKind::FoundryOptional),
            optional_benchmark_environment: matches!(
                kind,
                UniversalHarnessEnvironmentKind::BenchmarkExtrasOptional
            ),
            external_engines_runtime_dependencies_allowed: false,
            package_publication_required: false,
            harness_execution_performed: false,
            filesystem_probe: false,
            network_probe: false,
            external_engine_invoked: false,
            fallback_attempted: false,
        }
    }

    /// Returns whether this harness environment has an error state.
    #[must_use]
    pub const fn has_errors(&self) -> bool {
        self.status.is_error()
            || self.external_engines_runtime_dependencies_allowed
            || self.package_publication_required
            || self.harness_execution_performed
            || self.filesystem_probe
            || self.network_probe
            || self.external_engine_invoked
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
    pub execution_gate_status: UniversalHarnessExecutionGateStatus,
    pub runner_contract_fields: Vec<&'static str>,
    pub execution_gate_required_evidence_refs: Vec<&'static str>,
    pub execution_gate_attached_evidence_refs: Vec<&'static str>,
    pub execution_gate_missing_evidence_refs: Vec<&'static str>,
    pub surfaces: Vec<UniversalHarnessSurface>,
    pub harness_environments: Vec<UniversalHarnessEnvironmentRequirement>,
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
    pub capability_evidence_required: bool,
    pub execution_certificate_required: bool,
    pub native_io_certificate_required: bool,
    pub policy_no_fallback_evidence_required: bool,
    pub execution_allowed: bool,
    pub execution_attempted: bool,
    pub foundry_required: bool,
    pub foundry_optional_example: bool,
    pub local_harness_required: bool,
    pub ci_harness_required: bool,
    pub container_harness_required: bool,
    pub foundry_optional_harness_required: bool,
    pub optional_benchmark_environment_required: bool,
    pub external_engines_as_runtime_dependencies_allowed: bool,
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
            status: UniversalHarnessStatus::EvidenceIncomplete,
            execution_gate_status: UniversalHarnessExecutionGateStatus::BlockedMissingEvidence,
            runner_contract_fields: cg18_runner_contract_fields(),
            execution_gate_required_evidence_refs: cg18_execution_gate_required_evidence_refs(),
            execution_gate_attached_evidence_refs: Vec::new(),
            execution_gate_missing_evidence_refs: cg18_execution_gate_required_evidence_refs(),
            surfaces: cg18_harness_surfaces(),
            harness_environments: cg18_harness_environments(),
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
            capability_evidence_required: true,
            execution_certificate_required: true,
            native_io_certificate_required: true,
            policy_no_fallback_evidence_required: true,
            execution_allowed: false,
            execution_attempted: false,
            foundry_required: false,
            foundry_optional_example: true,
            local_harness_required: true,
            ci_harness_required: true,
            container_harness_required: true,
            foundry_optional_harness_required: true,
            optional_benchmark_environment_required: true,
            external_engines_as_runtime_dependencies_allowed: false,
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

    /// Number of import/deployment harness environments.
    #[must_use]
    pub fn harness_environment_count(&self) -> usize {
        self.harness_environments.len()
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

    /// Stable comma-separated harness-environment order for CLI reporting.
    #[must_use]
    pub fn harness_environment_kind_order(&self) -> String {
        self.harness_environments
            .iter()
            .map(|environment| environment.kind.as_str())
            .collect::<Vec<_>>()
            .join(",")
    }

    /// Stable comma-separated runner contract field order for CLI reporting.
    #[must_use]
    pub fn runner_contract_field_order(&self) -> String {
        self.runner_contract_fields.join(",")
    }

    /// Stable comma-separated evidence refs required before harness execution can be admitted.
    #[must_use]
    pub fn execution_gate_required_evidence_ref_order(&self) -> String {
        self.execution_gate_required_evidence_refs.join(",")
    }

    /// Stable comma-separated evidence refs currently attached to the gate.
    #[must_use]
    pub fn execution_gate_attached_evidence_ref_order(&self) -> String {
        self.execution_gate_attached_evidence_refs.join(",")
    }

    /// Stable comma-separated evidence refs still blocking execution.
    #[must_use]
    pub fn execution_gate_missing_evidence_ref_order(&self) -> String {
        self.execution_gate_missing_evidence_refs.join(",")
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

    /// Returns whether every required import/deployment environment is represented.
    #[must_use]
    pub fn has_required_harness_environments(&self) -> bool {
        self.has_harness_environment(UniversalHarnessEnvironmentKind::Local)
            && self.has_harness_environment(UniversalHarnessEnvironmentKind::ContinuousIntegration)
            && self.has_harness_environment(UniversalHarnessEnvironmentKind::Container)
            && self.has_harness_environment(UniversalHarnessEnvironmentKind::FoundryOptional)
            && self
                .has_harness_environment(UniversalHarnessEnvironmentKind::BenchmarkExtrasOptional)
    }

    /// Returns whether optional baselines remain comparison-only and outside `ShardLoom` runtime deps.
    #[must_use]
    pub fn baselines_are_comparison_only_and_runtime_dependency_free(&self) -> bool {
        !self.external_engines_as_runtime_dependencies_allowed
            && self.external_baselines.iter().all(|baseline| {
                baseline.external_only
                    && baseline.optional_environment_only
                    && !baseline.shardloom_runtime_dependency_allowed
                    && !baseline.runner_execution_performed
                    && !baseline.fallback_execution_allowed
                    && !baseline.fallback_attempted
            })
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
            && !self.execution_attempted
    }

    /// Returns whether the report contains errors.
    #[must_use]
    pub fn has_errors(&self) -> bool {
        self.status.is_error()
            || self.foundry_required
            || self.external_engines_as_runtime_dependencies_allowed
            || self.execution_attempted
            || self
                .surfaces
                .iter()
                .any(UniversalHarnessSurface::has_errors)
            || self
                .harness_environments
                .iter()
                .any(UniversalHarnessEnvironmentRequirement::has_errors)
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

    fn has_harness_environment(&self, kind: UniversalHarnessEnvironmentKind) -> bool {
        self.harness_environments
            .iter()
            .any(|environment| environment.kind == kind)
    }
}

fn cg18_execution_gate_required_evidence_refs() -> Vec<&'static str> {
    vec![
        "capability_refs",
        "execution_certificate_refs",
        "native_io_certificate_refs",
        "policy_no_fallback_refs",
        "output_envelope_refs",
        "output_artifact_refs",
        "correctness_evidence_refs",
        "benchmark_evidence_refs",
    ]
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

fn cg18_harness_environments() -> Vec<UniversalHarnessEnvironmentRequirement> {
    vec![
        UniversalHarnessEnvironmentRequirement::required(
            "harness.environment.local",
            UniversalHarnessEnvironmentKind::Local,
            "python -c \"import shardloom\" && shardloom status --format json",
        ),
        UniversalHarnessEnvironmentRequirement::required(
            "harness.environment.ci",
            UniversalHarnessEnvironmentKind::ContinuousIntegration,
            "cargo test --workspace && python -m pytest python/tests",
        ),
        UniversalHarnessEnvironmentRequirement::required(
            "harness.environment.container",
            UniversalHarnessEnvironmentKind::Container,
            "container smoke: shardloom --version && shardloom status --format json",
        ),
        UniversalHarnessEnvironmentRequirement::required(
            "harness.environment.foundry_optional",
            UniversalHarnessEnvironmentKind::FoundryOptional,
            "optional Foundry transform smoke with Conda package and certificate output",
        ),
        UniversalHarnessEnvironmentRequirement::required(
            "harness.environment.benchmark_extras_optional",
            UniversalHarnessEnvironmentKind::BenchmarkExtrasOptional,
            "optional local baseline benchmark smoke in isolated extras environment",
        ),
    ]
}

fn cg18_external_baselines() -> Vec<ExternalBaselineHarnessRequirement> {
    vec![
        ExternalBaselineHarnessRequirement::external_only(BaselineEngine::Spark),
        ExternalBaselineHarnessRequirement::external_only(BaselineEngine::DataFusion),
        ExternalBaselineHarnessRequirement::external_only(BaselineEngine::Polars),
        ExternalBaselineHarnessRequirement::external_only(BaselineEngine::DuckDb),
        ExternalBaselineHarnessRequirement::external_only(BaselineEngine::Dask),
        ExternalBaselineHarnessRequirement::external_only(BaselineEngine::Pandas),
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

        assert_eq!(report.status, UniversalHarnessStatus::EvidenceIncomplete);
        assert_eq!(
            report.execution_gate_status,
            UniversalHarnessExecutionGateStatus::BlockedMissingEvidence
        );
        assert_eq!(report.surface_count(), 7);
        assert_eq!(report.harness_environment_count(), 5);
        assert_eq!(report.external_baseline_count(), 6);
        assert_eq!(
            report.runner_contract_field_order(),
            "command,schema_version,exit_code,status,diagnostics,fallback_execution_allowed,side_effects,output_artifacts,metrics"
        );
        assert_eq!(
            report.execution_gate_required_evidence_ref_order(),
            "capability_refs,execution_certificate_refs,native_io_certificate_refs,policy_no_fallback_refs,output_envelope_refs,output_artifact_refs,correctness_evidence_refs,benchmark_evidence_refs"
        );
        assert_eq!(report.execution_gate_attached_evidence_ref_order(), "");
        assert_eq!(
            report.execution_gate_missing_evidence_ref_order(),
            "capability_refs,execution_certificate_refs,native_io_certificate_refs,policy_no_fallback_refs,output_envelope_refs,output_artifact_refs,correctness_evidence_refs,benchmark_evidence_refs"
        );
        assert_eq!(
            report.surface_kind_order(),
            "cli_json_runner,package_import,deployment_profile,foundry_example,external_baseline_runner,comparison_report_dataset,portability_check"
        );
        assert_eq!(
            report.harness_environment_kind_order(),
            "local,ci,container,foundry_optional,benchmark_extras_optional"
        );
        assert_eq!(
            report.baseline_engine_order(),
            "spark,datafusion,polars,duckdb,dask,pandas"
        );
        assert!(report.has_required_harness_environments());
        assert!(report.baselines_are_comparison_only_and_runtime_dependency_free());
        assert!(report.output_envelope_required);
        assert!(report.stable_command_schema_required);
        assert!(report.comparison_dataset_required);
        assert!(report.correctness_evidence_required);
        assert!(report.benchmark_evidence_required);
        assert!(report.capability_evidence_required);
        assert!(report.execution_certificate_required);
        assert!(report.native_io_certificate_required);
        assert!(report.policy_no_fallback_evidence_required);
        assert!(!report.execution_allowed);
        assert!(!report.execution_attempted);
        assert!(!report.foundry_required);
        assert!(report.foundry_optional_example);
        assert!(report.local_harness_required);
        assert!(report.ci_harness_required);
        assert!(report.container_harness_required);
        assert!(report.foundry_optional_harness_required);
        assert!(report.optional_benchmark_environment_required);
        assert!(!report.external_engines_as_runtime_dependencies_allowed);
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
