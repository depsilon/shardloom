//! Priority 3.6 RFC coverage follow-through before broader runtime expansion.
//!
//! This module records a report-only contract tying RFC 0010, 0011, 0020,
//! 0022, and 0023 to the next user/runtime expansion work. It does not parse
//! SQL, execute extensions, probe catalogs, import external plans for
//! execution, expand dependencies, invoke external engines, or perform
//! fallback execution.

use crate::{Diagnostic, DiagnosticSeverity};

/// Report-level and entry-level status for RFC coverage follow-through.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RfcCoverageFollowThroughStatus {
    CoverageDeclared,
    EvidenceRequired,
    Certified,
    Blocked,
}

impl RfcCoverageFollowThroughStatus {
    /// Stable machine-readable status label.
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::CoverageDeclared => "coverage_declared",
            Self::EvidenceRequired => "evidence_required",
            Self::Certified => "certified",
            Self::Blocked => "blocked",
        }
    }

    /// Returns whether this status should fail a report command.
    #[must_use]
    pub const fn is_error(&self) -> bool {
        matches!(self, Self::Blocked)
    }
}

/// Priority 3.6 RFC area that must be carried forward.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RfcCoverageFollowThroughArea {
    DeveloperAgentUsability,
    ModularExtensibility,
    SchemaCatalogTableCompatibility,
    NativePlanIrInterop,
    ExtensionPluginSandboxing,
}

impl RfcCoverageFollowThroughArea {
    /// Stable machine-readable area label.
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::DeveloperAgentUsability => "developer_agent_usability",
            Self::ModularExtensibility => "modular_extensibility",
            Self::SchemaCatalogTableCompatibility => "schema_catalog_table_compatibility",
            Self::NativePlanIrInterop => "native_plan_ir_interop",
            Self::ExtensionPluginSandboxing => "extension_plugin_sandboxing",
        }
    }
}

/// One RFC follow-through row.
#[derive(Debug, Clone, PartialEq, Eq)]
#[allow(clippy::struct_excessive_bools)]
pub struct RfcCoverageFollowThroughEntry {
    pub rfc_id: &'static str,
    pub area: RfcCoverageFollowThroughArea,
    pub status: RfcCoverageFollowThroughStatus,
    pub existing_surface_refs: Vec<&'static str>,
    pub required_before_runtime_expansion: Vec<&'static str>,
    pub runtime_expansion_allowed: bool,
    pub dependency_expansion_allowed: bool,
    pub external_effect_allowed: bool,
    pub fallback_attempted: bool,
}

impl RfcCoverageFollowThroughEntry {
    /// Creates a report-only RFC follow-through row.
    #[must_use]
    pub fn evidence_required(
        rfc_id: &'static str,
        area: RfcCoverageFollowThroughArea,
        existing_surface_refs: Vec<&'static str>,
        required_before_runtime_expansion: Vec<&'static str>,
    ) -> Self {
        Self {
            rfc_id,
            area,
            status: RfcCoverageFollowThroughStatus::EvidenceRequired,
            existing_surface_refs,
            required_before_runtime_expansion,
            runtime_expansion_allowed: false,
            dependency_expansion_allowed: false,
            external_effect_allowed: false,
            fallback_attempted: false,
        }
    }

    /// Returns whether this RFC row has an error state.
    #[must_use]
    pub const fn has_errors(&self) -> bool {
        self.status.is_error()
            || self.runtime_expansion_allowed
            || self.dependency_expansion_allowed
            || self.external_effect_allowed
            || self.fallback_attempted
    }
}

/// Priority 3.6 report-only RFC coverage contract.
#[derive(Debug, Clone, PartialEq, Eq)]
#[allow(clippy::struct_excessive_bools)]
pub struct RfcCoverageFollowThroughReport {
    pub schema_version: &'static str,
    pub report_id: &'static str,
    pub status: RfcCoverageFollowThroughStatus,
    pub entries: Vec<RfcCoverageFollowThroughEntry>,
    pub deterministic_machine_readable_required: bool,
    pub human_readable_required: bool,
    pub side_effect_explicit_required: bool,
    pub import_discovery_dry_run_safety_required: bool,
    pub typed_effect_materialization_metadata_required: bool,
    pub effectful_extensions_blocked: bool,
    pub metadata_discovery_separate_from_read_write_commit: bool,
    pub table_write_commit_claims_blocked: bool,
    pub imported_plan_execution_blocked: bool,
    pub substrait_bridge_fallback_blocked: bool,
    pub extension_manifest_inspection_only: bool,
    pub extension_code_execution_blocked: bool,
    pub runtime_expansion_performed: bool,
    pub parser_expansion_performed: bool,
    pub adapter_expansion_performed: bool,
    pub dependency_expansion_performed: bool,
    pub external_effect_performed: bool,
    pub external_engine_invoked: bool,
    pub fallback_attempted: bool,
    pub diagnostics: Vec<Diagnostic>,
}

impl RfcCoverageFollowThroughReport {
    /// Creates the Priority 3.6 report-only follow-through contract.
    #[must_use]
    pub fn priority_3_6_contract() -> Self {
        Self {
            schema_version: "shardloom.rfc_coverage_followthrough.v1",
            report_id: "priority_3_6.rfc_coverage_followthrough",
            status: RfcCoverageFollowThroughStatus::EvidenceRequired,
            entries: priority_3_6_entries(),
            deterministic_machine_readable_required: true,
            human_readable_required: true,
            side_effect_explicit_required: true,
            import_discovery_dry_run_safety_required: true,
            typed_effect_materialization_metadata_required: true,
            effectful_extensions_blocked: true,
            metadata_discovery_separate_from_read_write_commit: true,
            table_write_commit_claims_blocked: true,
            imported_plan_execution_blocked: true,
            substrait_bridge_fallback_blocked: true,
            extension_manifest_inspection_only: true,
            extension_code_execution_blocked: true,
            runtime_expansion_performed: false,
            parser_expansion_performed: false,
            adapter_expansion_performed: false,
            dependency_expansion_performed: false,
            external_effect_performed: false,
            external_engine_invoked: false,
            fallback_attempted: false,
            diagnostics: Vec::new(),
        }
    }

    /// Number of RFC rows tracked by this report.
    #[must_use]
    pub fn entry_count(&self) -> usize {
        self.entries.len()
    }

    /// Stable comma-separated RFC order for CLI reporting.
    #[must_use]
    pub fn rfc_order(&self) -> String {
        self.entries
            .iter()
            .map(|entry| entry.rfc_id)
            .collect::<Vec<_>>()
            .join(",")
    }

    /// Stable comma-separated area order for CLI reporting.
    #[must_use]
    pub fn area_order(&self) -> String {
        self.entries
            .iter()
            .map(|entry| entry.area.as_str())
            .collect::<Vec<_>>()
            .join(",")
    }

    /// Returns the stable status for an RFC row, or `missing`.
    #[must_use]
    pub fn status_for_rfc(&self, rfc_id: &str) -> &'static str {
        self.entries
            .iter()
            .find(|entry| entry.rfc_id == rfc_id)
            .map_or("missing", |entry| entry.status.as_str())
    }

    /// Returns whether all RFC rows keep runtime expansion blocked.
    #[must_use]
    pub fn all_entries_runtime_expansion_blocked(&self) -> bool {
        self.entries
            .iter()
            .all(|entry| !entry.runtime_expansion_allowed)
    }

    /// Returns whether all RFC rows keep dependency expansion blocked.
    #[must_use]
    pub fn all_entries_dependency_expansion_blocked(&self) -> bool {
        self.entries
            .iter()
            .all(|entry| !entry.dependency_expansion_allowed)
    }

    /// Returns whether all RFC rows keep external effects blocked.
    #[must_use]
    pub fn all_entries_external_effects_blocked(&self) -> bool {
        self.entries
            .iter()
            .all(|entry| !entry.external_effect_allowed)
    }

    /// Returns whether the report avoids all side effects.
    #[must_use]
    pub const fn is_side_effect_free(&self) -> bool {
        !self.runtime_expansion_performed
            && !self.parser_expansion_performed
            && !self.adapter_expansion_performed
            && !self.dependency_expansion_performed
            && !self.external_effect_performed
            && !self.external_engine_invoked
            && !self.fallback_attempted
    }

    /// Returns whether the report has any error state.
    #[must_use]
    pub fn has_errors(&self) -> bool {
        self.status.is_error()
            || !self.effectful_extensions_blocked
            || !self.metadata_discovery_separate_from_read_write_commit
            || !self.table_write_commit_claims_blocked
            || !self.imported_plan_execution_blocked
            || !self.substrait_bridge_fallback_blocked
            || !self.extension_manifest_inspection_only
            || !self.extension_code_execution_blocked
            || !self.all_entries_runtime_expansion_blocked()
            || !self.all_entries_dependency_expansion_blocked()
            || !self.all_entries_external_effects_blocked()
            || !self.is_side_effect_free()
            || self
                .entries
                .iter()
                .any(RfcCoverageFollowThroughEntry::has_errors)
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
            "RFC coverage follow-through\nschema_version: {}\nreport: {}\nstatus: {}\nrfc rows: {}\nruntime expansion: disabled\nexternal effects: disabled\nfallback execution: disabled",
            self.schema_version,
            self.report_id,
            self.status.as_str(),
            self.entry_count(),
        )
    }
}

fn priority_3_6_entries() -> Vec<RfcCoverageFollowThroughEntry> {
    vec![
        RfcCoverageFollowThroughEntry::evidence_required(
            "rfc_0010",
            RfcCoverageFollowThroughArea::DeveloperAgentUsability,
            vec![
                "CliApiJsonProtocolReport",
                "PythonWrapperFoundationReport",
                "AgentContractPack",
            ],
            vec![
                "deterministic machine-readable and human-readable command surfaces",
                "side-effect-explicit import, discovery, dry-run, diagnostic, benchmark, and certificate outputs",
                "agent-facing workflows remain discovery/dry-run safe before execution/write permissions",
            ],
        ),
        RfcCoverageFollowThroughEntry::evidence_required(
            "rfc_0011",
            RfcCoverageFollowThroughArea::ModularExtensibility,
            vec![
                "ExtensionInspectionReport",
                "UnstructuredWorkflowCertificate",
                "EmbeddingBoundaryReport",
            ],
            vec![
                "typed/effect/materialization metadata for SQL, UDF, media, model, embedding, vector, and external-effect surfaces",
                "sandboxing, governance, correctness, and certificate evidence before effectful extension execution",
                "deterministic unsupported diagnostics for unsafe Python, external, model, or vector execution",
            ],
        ),
        RfcCoverageFollowThroughEntry::evidence_required(
            "rfc_0020",
            RfcCoverageFollowThroughArea::SchemaCatalogTableCompatibility,
            vec![
                "TableCompatibilityReport",
                "SchemaEvolutionCompatibilityReport",
                "CatalogMetadataIntegrationGateReport",
            ],
            vec![
                "real snapshot, schema, partition, delete, and catalog evidence before metadata promotion",
                "metadata discovery remains separate from read, write, commit, update, delete, and merge certification",
                "table semantics and recovery evidence before update/delete/merge claims",
            ],
        ),
        RfcCoverageFollowThroughEntry::evidence_required(
            "rfc_0022",
            RfcCoverageFollowThroughArea::NativePlanIrInterop,
            vec![
                "NativePlanDocument",
                "ImportedPlanCapabilityGateReport",
                "PlanPortabilityReport",
            ],
            vec![
                "native plan import/export and capability-gate evidence before imported plan execution",
                "optional dependency-free Substrait-like import/export posture before approved parser dependencies",
                "no interop format may bridge unsupported work into an external fallback engine",
            ],
        ),
        RfcCoverageFollowThroughEntry::evidence_required(
            "rfc_0023",
            RfcCoverageFollowThroughArea::ExtensionPluginSandboxing,
            vec![
                "ExtensionManifest",
                "ExtensionInspectionReport",
                "SandboxPolicy",
            ],
            vec![
                "manifest, lifecycle, permission, provenance, signing, sandbox, and resource-limit evidence",
                "agent-inspection evidence before plugin or UDF execution",
                "manifest inspection must not execute extension code",
            ],
        ),
    ]
}

/// Produces the Priority 3.6 RFC coverage follow-through report.
#[must_use]
pub fn plan_rfc_coverage_followthrough() -> RfcCoverageFollowThroughReport {
    RfcCoverageFollowThroughReport::priority_3_6_contract()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rfc_coverage_followthrough_blocks_runtime_expansion() {
        let report = plan_rfc_coverage_followthrough();

        assert_eq!(
            report.schema_version,
            "shardloom.rfc_coverage_followthrough.v1"
        );
        assert_eq!(
            report.status,
            RfcCoverageFollowThroughStatus::EvidenceRequired
        );
        assert_eq!(report.entry_count(), 5);
        assert_eq!(
            report.rfc_order(),
            "rfc_0010,rfc_0011,rfc_0020,rfc_0022,rfc_0023"
        );
        assert_eq!(
            report.area_order(),
            "developer_agent_usability,modular_extensibility,schema_catalog_table_compatibility,native_plan_ir_interop,extension_plugin_sandboxing"
        );
        assert_eq!(report.status_for_rfc("rfc_0011"), "evidence_required");
        assert!(report.deterministic_machine_readable_required);
        assert!(report.human_readable_required);
        assert!(report.side_effect_explicit_required);
        assert!(report.import_discovery_dry_run_safety_required);
        assert!(report.typed_effect_materialization_metadata_required);
        assert!(report.effectful_extensions_blocked);
        assert!(report.metadata_discovery_separate_from_read_write_commit);
        assert!(report.table_write_commit_claims_blocked);
        assert!(report.imported_plan_execution_blocked);
        assert!(report.substrait_bridge_fallback_blocked);
        assert!(report.extension_manifest_inspection_only);
        assert!(report.extension_code_execution_blocked);
        assert!(report.all_entries_runtime_expansion_blocked());
        assert!(report.all_entries_dependency_expansion_blocked());
        assert!(report.all_entries_external_effects_blocked());
        assert!(report.is_side_effect_free());
        assert!(!report.has_errors());
        assert!(!report.runtime_expansion_performed);
        assert!(!report.parser_expansion_performed);
        assert!(!report.adapter_expansion_performed);
        assert!(!report.dependency_expansion_performed);
        assert!(!report.external_effect_performed);
        assert!(!report.external_engine_invoked);
        assert!(!report.fallback_attempted);
    }
}
