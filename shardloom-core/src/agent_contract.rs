//! Agent contract pack.
//!
//! This module defines the stable, report-only surface an autonomous agent can
//! inspect before deciding whether it can plan, run, or stop. It does not
//! execute commands, probe the environment, or authorize effects.

#![allow(
    clippy::missing_errors_doc,
    clippy::missing_panics_doc,
    clippy::struct_excessive_bools
)]

use crate::{Diagnostic, DiagnosticSeverity};
use std::fmt::Write as _;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AgentContractSurfaceKind {
    OutputEnvelope,
    Diagnostics,
    Capabilities,
    FeatureFootprint,
    EffectBudget,
    Doctor,
    ExplainEstimate,
    PlanPortability,
    NativeIoEnvelope,
    ExecutionCertificate,
    BenchmarkEvidence,
    WorldClassSufficiency,
    SecurityGovernance,
}

impl AgentContractSurfaceKind {
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::OutputEnvelope => "output_envelope",
            Self::Diagnostics => "diagnostics",
            Self::Capabilities => "capabilities",
            Self::FeatureFootprint => "feature_footprint",
            Self::EffectBudget => "effect_budget",
            Self::Doctor => "doctor",
            Self::ExplainEstimate => "explain_estimate",
            Self::PlanPortability => "plan_portability",
            Self::NativeIoEnvelope => "native_io_envelope",
            Self::ExecutionCertificate => "execution_certificate",
            Self::BenchmarkEvidence => "benchmark_evidence",
            Self::WorldClassSufficiency => "world_class_sufficiency",
            Self::SecurityGovernance => "security_governance",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AgentContractSurfaceStatus {
    Available,
    Planned,
    Deferred,
}

impl AgentContractSurfaceStatus {
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::Available => "available",
            Self::Planned => "planned",
            Self::Deferred => "deferred",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AgentContractSurface {
    pub kind: AgentContractSurfaceKind,
    pub status: AgentContractSurfaceStatus,
    pub command: Option<&'static str>,
    pub schema_version: &'static str,
    pub stable_json_required: bool,
    pub side_effect_free_by_default: bool,
    pub fallback_execution_allowed: bool,
}

impl AgentContractSurface {
    #[must_use]
    pub const fn available(
        kind: AgentContractSurfaceKind,
        command: Option<&'static str>,
        schema_version: &'static str,
    ) -> Self {
        Self {
            kind,
            status: AgentContractSurfaceStatus::Available,
            command,
            schema_version,
            stable_json_required: true,
            side_effect_free_by_default: true,
            fallback_execution_allowed: false,
        }
    }

    #[must_use]
    pub const fn planned(
        kind: AgentContractSurfaceKind,
        command: Option<&'static str>,
        schema_version: &'static str,
    ) -> Self {
        Self {
            kind,
            status: AgentContractSurfaceStatus::Planned,
            command,
            schema_version,
            stable_json_required: true,
            side_effect_free_by_default: true,
            fallback_execution_allowed: false,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AgentContractPack {
    pub schema_version: &'static str,
    pub pack_id: &'static str,
    pub surfaces: Vec<AgentContractSurface>,
    pub recommended_sequence: Vec<&'static str>,
    pub deterministic_json_required: bool,
    pub text_is_authoritative: bool,
    pub no_probe_default: bool,
    pub external_effects_default_denied: bool,
    pub destructive_effects_default_denied: bool,
    pub fallback_attempted: bool,
    pub fallback_execution_allowed: bool,
    pub diagnostics: Vec<Diagnostic>,
}

impl AgentContractPack {
    #[must_use]
    pub fn default_pack() -> Self {
        Self {
            schema_version: "shardloom.agent_contract_pack.v1",
            pack_id: "agent.contract_pack.default",
            surfaces: vec![
                AgentContractSurface::available(
                    AgentContractSurfaceKind::OutputEnvelope,
                    None,
                    "shardloom.output.v2",
                ),
                AgentContractSurface::available(
                    AgentContractSurfaceKind::Diagnostics,
                    None,
                    "shardloom.diagnostics.v1",
                ),
                AgentContractSurface::available(
                    AgentContractSurfaceKind::Capabilities,
                    Some("capabilities certification"),
                    "shardloom.capability_certification.v1",
                ),
                AgentContractSurface::available(
                    AgentContractSurfaceKind::FeatureFootprint,
                    Some("feature-footprint"),
                    "shardloom.feature_footprint.v1",
                ),
                AgentContractSurface::available(
                    AgentContractSurfaceKind::EffectBudget,
                    Some("effect-budget-plan"),
                    "shardloom.effect_budget.v1",
                ),
                AgentContractSurface::available(
                    AgentContractSurfaceKind::Doctor,
                    Some("doctor"),
                    "shardloom.feature_footprint.v1",
                ),
                AgentContractSurface::available(
                    AgentContractSurfaceKind::ExplainEstimate,
                    Some("explain,estimate"),
                    "shardloom.output.v2",
                ),
                AgentContractSurface::available(
                    AgentContractSurfaceKind::PlanPortability,
                    Some("plan-ir,plan-import,plan-export"),
                    "shardloom.plan_portability.v1",
                ),
                AgentContractSurface::available(
                    AgentContractSurfaceKind::NativeIoEnvelope,
                    Some("native-io-envelope-plan"),
                    "shardloom.native_io_envelope.v1",
                ),
                AgentContractSurface::available(
                    AgentContractSurfaceKind::ExecutionCertificate,
                    Some("execution-certificate-plan"),
                    "shardloom.execution_certificate_evidence_surface.v1",
                ),
                AgentContractSurface::available(
                    AgentContractSurfaceKind::BenchmarkEvidence,
                    Some("benchmark-plan,benchmark-claim-evidence-plan"),
                    "shardloom.benchmark_claim_evidence.v1",
                ),
                AgentContractSurface::available(
                    AgentContractSurfaceKind::WorldClassSufficiency,
                    Some("world-class-sufficiency-plan"),
                    "shardloom.world_class_sufficiency.v1",
                ),
                AgentContractSurface::available(
                    AgentContractSurfaceKind::SecurityGovernance,
                    Some("security-plan,agent-safety-plan,redaction-plan"),
                    "shardloom.security_plan.v1",
                ),
            ],
            recommended_sequence: vec![
                "feature-footprint --format json",
                "effect-budget-plan --format json",
                "doctor --format json",
                "capabilities certification --format json",
                "world-class-sufficiency-plan --format json",
                "benchmark-plan --format json",
                "benchmark-claim-evidence-plan --format json",
            ],
            deterministic_json_required: true,
            text_is_authoritative: false,
            no_probe_default: true,
            external_effects_default_denied: true,
            destructive_effects_default_denied: true,
            fallback_attempted: false,
            fallback_execution_allowed: false,
            diagnostics: Vec::new(),
        }
    }

    #[must_use]
    pub fn surface_order(&self) -> Vec<&'static str> {
        self.surfaces
            .iter()
            .map(|surface| surface.kind.as_str())
            .collect()
    }

    #[must_use]
    pub fn available_surface_count(&self) -> usize {
        self.surfaces
            .iter()
            .filter(|surface| surface.status == AgentContractSurfaceStatus::Available)
            .count()
    }

    #[must_use]
    pub fn side_effect_free_surface_count(&self) -> usize {
        self.surfaces
            .iter()
            .filter(|surface| surface.side_effect_free_by_default)
            .count()
    }

    #[must_use]
    pub fn fallback_allowed_surface_count(&self) -> usize {
        self.surfaces
            .iter()
            .filter(|surface| surface.fallback_execution_allowed)
            .count()
    }

    #[must_use]
    pub fn side_effect_free(&self) -> bool {
        self.no_probe_default
            && self.external_effects_default_denied
            && self.destructive_effects_default_denied
            && !self.fallback_attempted
            && !self.fallback_execution_allowed
            && self.fallback_allowed_surface_count() == 0
            && self.side_effect_free_surface_count() == self.surfaces.len()
    }

    #[must_use]
    pub fn has_errors(&self) -> bool {
        !self.side_effect_free()
            || self.diagnostics.iter().any(|diagnostic| {
                matches!(
                    diagnostic.severity,
                    DiagnosticSeverity::Error | DiagnosticSeverity::Fatal
                )
            })
    }

    #[must_use]
    pub fn to_human_text(&self) -> String {
        let mut out = String::new();
        let _ = writeln!(out, "schema_version: {}", self.schema_version);
        let _ = writeln!(out, "pack_id: {}", self.pack_id);
        let _ = writeln!(
            out,
            "deterministic json required: {}",
            self.deterministic_json_required
        );
        let _ = writeln!(out, "text authoritative: {}", self.text_is_authoritative);
        let _ = writeln!(out, "no probe default: {}", self.no_probe_default);
        let _ = writeln!(
            out,
            "fallback execution allowed: {}",
            self.fallback_execution_allowed
        );
        let _ = writeln!(out, "surfaces:");
        for surface in &self.surfaces {
            let _ = writeln!(
                out,
                "  - {} [{}] command={} schema={} side_effect_free={} fallback_allowed={}",
                surface.kind.as_str(),
                surface.status.as_str(),
                surface.command.unwrap_or("none"),
                surface.schema_version,
                surface.side_effect_free_by_default,
                surface.fallback_execution_allowed
            );
        }
        let _ = writeln!(out, "recommended sequence:");
        for command in &self.recommended_sequence {
            let _ = writeln!(out, "  - {command}");
        }
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_pack_is_no_probe_and_no_fallback() {
        let pack = AgentContractPack::default_pack();
        assert_eq!(pack.schema_version, "shardloom.agent_contract_pack.v1");
        assert_eq!(pack.fallback_allowed_surface_count(), 0);
        assert!(pack.side_effect_free());
        assert!(!pack.has_errors());
        assert!(!pack.text_is_authoritative);
    }

    #[test]
    fn default_pack_names_required_agent_surfaces() {
        let pack = AgentContractPack::default_pack();
        let surfaces = pack.surface_order();
        assert!(surfaces.contains(&"output_envelope"));
        assert!(surfaces.contains(&"feature_footprint"));
        assert!(surfaces.contains(&"effect_budget"));
        assert!(surfaces.contains(&"benchmark_evidence"));
        assert_eq!(pack.available_surface_count(), pack.surfaces.len());
    }

    #[test]
    fn default_pack_routes_agents_to_certification_and_plan_only_benchmark_surfaces() {
        let pack = AgentContractPack::default_pack();
        let capabilities = pack
            .surfaces
            .iter()
            .find(|surface| surface.kind == AgentContractSurfaceKind::Capabilities)
            .expect("capabilities surface");
        assert_eq!(capabilities.command, Some("capabilities certification"));

        let benchmark = pack
            .surfaces
            .iter()
            .find(|surface| surface.kind == AgentContractSurfaceKind::BenchmarkEvidence)
            .expect("benchmark surface");
        assert_eq!(
            benchmark.command,
            Some("benchmark-plan,benchmark-claim-evidence-plan")
        );
        let command = benchmark.command.expect("benchmark command list");
        assert!(!command.contains("vortex-count-benchmark"));
        assert!(!command.contains("traditional-analytics-run"));
    }

    #[test]
    fn unsafe_surface_or_fallback_marks_pack_error() {
        let mut pack = AgentContractPack::default_pack();
        pack.surfaces[0].fallback_execution_allowed = true;
        assert!(!pack.side_effect_free());
        assert!(pack.has_errors());

        let mut fallback = AgentContractPack::default_pack();
        fallback.fallback_attempted = true;
        assert!(fallback.has_errors());
    }
}
