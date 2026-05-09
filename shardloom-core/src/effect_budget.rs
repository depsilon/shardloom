//! Effect-budget planning contracts.
//!
//! This module is report-only. It does not resolve credentials, probe external
//! systems, execute UDFs/plugins, call models/APIs, read files, or perform IO.

#![allow(
    clippy::missing_errors_doc,
    clippy::missing_panics_doc,
    clippy::struct_excessive_bools
)]

use crate::{Diagnostic, DiagnosticSeverity};
use std::fmt::Write as _;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EffectBudgetScope {
    LocalFileRead,
    LocalFileWrite,
    ObjectStoreRead,
    ObjectStoreWrite,
    CatalogRead,
    CatalogWrite,
    ApiRead,
    ApiWrite,
    LlmCall,
    EmbeddingGeneration,
    VectorSearch,
    PythonUdf,
    WasmUdf,
    ExternalServiceUdf,
    PluginExecution,
    MediaExtraction,
    NetworkEgress,
}

impl EffectBudgetScope {
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::LocalFileRead => "local_file_read",
            Self::LocalFileWrite => "local_file_write",
            Self::ObjectStoreRead => "object_store_read",
            Self::ObjectStoreWrite => "object_store_write",
            Self::CatalogRead => "catalog_read",
            Self::CatalogWrite => "catalog_write",
            Self::ApiRead => "api_read",
            Self::ApiWrite => "api_write",
            Self::LlmCall => "llm_call",
            Self::EmbeddingGeneration => "embedding_generation",
            Self::VectorSearch => "vector_search",
            Self::PythonUdf => "python_udf",
            Self::WasmUdf => "wasm_udf",
            Self::ExternalServiceUdf => "external_service_udf",
            Self::PluginExecution => "plugin_execution",
            Self::MediaExtraction => "media_extraction",
            Self::NetworkEgress => "network_egress",
        }
    }

    #[must_use]
    pub const fn requires_credentials(&self) -> bool {
        matches!(
            self,
            Self::ObjectStoreRead
                | Self::ObjectStoreWrite
                | Self::CatalogRead
                | Self::CatalogWrite
                | Self::ApiRead
                | Self::ApiWrite
                | Self::LlmCall
                | Self::EmbeddingGeneration
                | Self::VectorSearch
                | Self::ExternalServiceUdf
                | Self::NetworkEgress
        )
    }

    #[must_use]
    pub const fn requires_redaction(&self) -> bool {
        matches!(
            self,
            Self::ApiRead
                | Self::ApiWrite
                | Self::LlmCall
                | Self::EmbeddingGeneration
                | Self::VectorSearch
                | Self::PythonUdf
                | Self::ExternalServiceUdf
                | Self::PluginExecution
                | Self::MediaExtraction
                | Self::NetworkEgress
        )
    }

    #[must_use]
    pub const fn can_egress_data(&self) -> bool {
        matches!(
            self,
            Self::ObjectStoreWrite
                | Self::CatalogWrite
                | Self::ApiWrite
                | Self::LlmCall
                | Self::EmbeddingGeneration
                | Self::VectorSearch
                | Self::ExternalServiceUdf
                | Self::PluginExecution
                | Self::NetworkEgress
        )
    }

    #[must_use]
    pub const fn is_destructive_or_mutating(&self) -> bool {
        matches!(
            self,
            Self::LocalFileWrite
                | Self::ObjectStoreWrite
                | Self::CatalogWrite
                | Self::ApiWrite
                | Self::PluginExecution
        )
    }

    #[must_use]
    pub const fn is_external(&self) -> bool {
        !matches!(
            self,
            Self::LocalFileRead | Self::LocalFileWrite | Self::WasmUdf
        )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EffectBudgetStatus {
    DeniedByDefault,
    Planned,
    RequiresApproval,
    ApprovedForPlan,
    Exceeded,
    Unsupported,
}

impl EffectBudgetStatus {
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::DeniedByDefault => "denied_by_default",
            Self::Planned => "planned",
            Self::RequiresApproval => "requires_approval",
            Self::ApprovedForPlan => "approved_for_plan",
            Self::Exceeded => "exceeded",
            Self::Unsupported => "unsupported",
        }
    }

    #[must_use]
    pub const fn is_approved(&self) -> bool {
        matches!(self, Self::ApprovedForPlan)
    }

    #[must_use]
    pub const fn is_error(&self) -> bool {
        matches!(self, Self::Exceeded | Self::Unsupported)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EffectBudgetEntry {
    pub scope: EffectBudgetScope,
    pub status: EffectBudgetStatus,
    pub requested_call_count: u32,
    pub approved_call_count: u32,
    pub requested_egress_bytes: Option<u64>,
    pub approved_egress_bytes: Option<u64>,
    pub estimated_cost_micros: Option<u64>,
    pub approved_cost_micros: Option<u64>,
    pub credentials_required: bool,
    pub redaction_required: bool,
    pub audit_required: bool,
    pub approval_required: bool,
    pub materialization_boundary_required: bool,
    pub diagnostics: Vec<Diagnostic>,
}

impl EffectBudgetEntry {
    #[must_use]
    pub fn denied_by_default(scope: EffectBudgetScope) -> Self {
        Self {
            scope,
            status: EffectBudgetStatus::DeniedByDefault,
            requested_call_count: 0,
            approved_call_count: 0,
            requested_egress_bytes: None,
            approved_egress_bytes: None,
            estimated_cost_micros: None,
            approved_cost_micros: None,
            credentials_required: scope.requires_credentials(),
            redaction_required: scope.requires_redaction(),
            audit_required: scope.is_external() || scope.is_destructive_or_mutating(),
            approval_required: scope.is_external() || scope.is_destructive_or_mutating(),
            materialization_boundary_required: scope.can_egress_data()
                || matches!(
                    scope,
                    EffectBudgetScope::PythonUdf
                        | EffectBudgetScope::ExternalServiceUdf
                        | EffectBudgetScope::MediaExtraction
                ),
            diagnostics: Vec::new(),
        }
    }

    #[must_use]
    pub fn has_errors(&self) -> bool {
        self.status.is_error()
            || self.diagnostics.iter().any(|diagnostic| {
                matches!(
                    diagnostic.severity,
                    DiagnosticSeverity::Error | DiagnosticSeverity::Fatal
                )
            })
    }

    #[must_use]
    pub const fn effect_allowed(&self) -> bool {
        self.status.is_approved() && self.approved_call_count > 0
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EffectBudgetReport {
    pub schema_version: &'static str,
    pub report_id: &'static str,
    pub budget_mode: &'static str,
    pub entries: Vec<EffectBudgetEntry>,
    pub external_effects_allowed: bool,
    pub destructive_effects_allowed: bool,
    pub network_egress_allowed: bool,
    pub credentials_resolved: bool,
    pub secrets_loaded: bool,
    pub redaction_policy_required: bool,
    pub audit_required: bool,
    pub runtime_execution_performed: bool,
    pub filesystem_probe: bool,
    pub network_probe: bool,
    pub catalog_probe: bool,
    pub fallback_attempted: bool,
    pub fallback_execution_allowed: bool,
    pub diagnostics: Vec<Diagnostic>,
}

impl EffectBudgetReport {
    #[must_use]
    pub fn planning_default() -> Self {
        Self {
            schema_version: "shardloom.effect_budget.v1",
            report_id: "cross_cutting.effect_budget",
            budget_mode: "deny_external_effects_by_default",
            entries: Self::default_scopes()
                .iter()
                .copied()
                .map(EffectBudgetEntry::denied_by_default)
                .collect(),
            external_effects_allowed: false,
            destructive_effects_allowed: false,
            network_egress_allowed: false,
            credentials_resolved: false,
            secrets_loaded: false,
            redaction_policy_required: true,
            audit_required: true,
            runtime_execution_performed: false,
            filesystem_probe: false,
            network_probe: false,
            catalog_probe: false,
            fallback_attempted: false,
            fallback_execution_allowed: false,
            diagnostics: Vec::new(),
        }
    }

    #[must_use]
    pub const fn default_scopes() -> &'static [EffectBudgetScope] {
        &[
            EffectBudgetScope::LocalFileRead,
            EffectBudgetScope::LocalFileWrite,
            EffectBudgetScope::ObjectStoreRead,
            EffectBudgetScope::ObjectStoreWrite,
            EffectBudgetScope::CatalogRead,
            EffectBudgetScope::CatalogWrite,
            EffectBudgetScope::ApiRead,
            EffectBudgetScope::ApiWrite,
            EffectBudgetScope::LlmCall,
            EffectBudgetScope::EmbeddingGeneration,
            EffectBudgetScope::VectorSearch,
            EffectBudgetScope::PythonUdf,
            EffectBudgetScope::WasmUdf,
            EffectBudgetScope::ExternalServiceUdf,
            EffectBudgetScope::PluginExecution,
            EffectBudgetScope::MediaExtraction,
            EffectBudgetScope::NetworkEgress,
        ]
    }

    #[must_use]
    pub fn denied_scope_count(&self) -> usize {
        self.entries
            .iter()
            .filter(|entry| entry.status == EffectBudgetStatus::DeniedByDefault)
            .count()
    }

    #[must_use]
    pub fn approved_scope_count(&self) -> usize {
        self.entries
            .iter()
            .filter(|entry| entry.effect_allowed())
            .count()
    }

    #[must_use]
    pub fn approval_required_scope_count(&self) -> usize {
        self.entries
            .iter()
            .filter(|entry| entry.approval_required)
            .count()
    }

    #[must_use]
    pub fn credential_required_scope_count(&self) -> usize {
        self.entries
            .iter()
            .filter(|entry| entry.credentials_required)
            .count()
    }

    #[must_use]
    pub fn materialization_boundary_required_scope_count(&self) -> usize {
        self.entries
            .iter()
            .filter(|entry| entry.materialization_boundary_required)
            .count()
    }

    #[must_use]
    pub fn scope_order(&self) -> Vec<&'static str> {
        self.entries
            .iter()
            .map(|entry| entry.scope.as_str())
            .collect()
    }

    #[must_use]
    pub fn side_effect_free(&self) -> bool {
        !self.runtime_execution_performed
            && !self.filesystem_probe
            && !self.network_probe
            && !self.catalog_probe
            && !self.external_effects_allowed
            && !self.destructive_effects_allowed
            && !self.network_egress_allowed
            && !self.fallback_attempted
            && !self.fallback_execution_allowed
            && self.approved_scope_count() == 0
    }

    #[must_use]
    pub fn has_errors(&self) -> bool {
        !self.side_effect_free()
            || self.entries.iter().any(EffectBudgetEntry::has_errors)
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
        let _ = writeln!(out, "report_id: {}", self.report_id);
        let _ = writeln!(out, "budget_mode: {}", self.budget_mode);
        let _ = writeln!(
            out,
            "external effects allowed: {}",
            self.external_effects_allowed
        );
        let _ = writeln!(
            out,
            "destructive effects allowed: {}",
            self.destructive_effects_allowed
        );
        let _ = writeln!(
            out,
            "network egress allowed: {}",
            self.network_egress_allowed
        );
        let _ = writeln!(out, "credentials resolved: {}", self.credentials_resolved);
        let _ = writeln!(out, "secrets loaded: {}", self.secrets_loaded);
        let _ = writeln!(
            out,
            "runtime execution: {}",
            self.runtime_execution_performed
        );
        let _ = writeln!(
            out,
            "fallback execution allowed: {}",
            self.fallback_execution_allowed
        );
        let _ = writeln!(out, "effect scopes:");
        for entry in &self.entries {
            let _ = writeln!(
                out,
                "  - {} [{}] approval_required={} credentials_required={} materialization_boundary_required={}",
                entry.scope.as_str(),
                entry.status.as_str(),
                entry.approval_required,
                entry.credentials_required,
                entry.materialization_boundary_required
            );
        }
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn planning_default_denies_effects_without_runtime_work() {
        let report = EffectBudgetReport::planning_default();
        assert_eq!(report.schema_version, "shardloom.effect_budget.v1");
        assert_eq!(
            report.entries.len(),
            EffectBudgetReport::default_scopes().len()
        );
        assert_eq!(report.approved_scope_count(), 0);
        assert_eq!(report.denied_scope_count(), report.entries.len());
        assert!(report.side_effect_free());
        assert!(!report.has_errors());
        assert!(!report.fallback_execution_allowed);
        assert!(!report.fallback_attempted);
    }

    #[test]
    fn default_report_tracks_credential_and_materialization_boundaries() {
        let report = EffectBudgetReport::planning_default();
        assert!(report.credential_required_scope_count() > 0);
        assert!(report.materialization_boundary_required_scope_count() > 0);
        assert!(
            report
                .scope_order()
                .contains(&EffectBudgetScope::LlmCall.as_str())
        );
        assert!(
            report
                .scope_order()
                .contains(&EffectBudgetScope::NetworkEgress.as_str())
        );
    }

    #[test]
    fn probes_or_fallbacks_make_report_unsafe() {
        let mut report = EffectBudgetReport::planning_default();
        report.network_probe = true;
        assert!(!report.side_effect_free());
        assert!(report.has_errors());

        let mut fallback = EffectBudgetReport::planning_default();
        fallback.fallback_attempted = true;
        assert!(fallback.has_errors());
    }
}
