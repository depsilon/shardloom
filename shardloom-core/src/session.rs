//! Explicit `ShardLoom` session and registry posture.
//!
//! Vortex's explicit session/registry model is a useful design reference for
//! `ShardLoom`: provider, operator, function, adapter, policy, and evidence
//! registries should be carried through explicit session context rather than
//! hidden globals. This module is report-only and does not mutate registries.

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ShardLoomSessionRegistryKind {
    Operator,
    Function,
    Aggregate,
    Sketch,
    Window,
    Join,
    SourceSinkAdapter,
    ExecutionProvider,
    SemanticProfile,
    EvidenceArtifact,
    PolicyEffect,
}

impl ShardLoomSessionRegistryKind {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Operator => "operator",
            Self::Function => "function",
            Self::Aggregate => "aggregate",
            Self::Sketch => "sketch",
            Self::Window => "window",
            Self::Join => "join",
            Self::SourceSinkAdapter => "source_sink_adapter",
            Self::ExecutionProvider => "execution_provider",
            Self::SemanticProfile => "semantic_profile",
            Self::EvidenceArtifact => "evidence_artifact",
            Self::PolicyEffect => "policy_effect",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ShardLoomSessionRegistryStatus {
    ExistingReportSurface,
    PlannedExplicitRegistry,
    BlockedUntilAdmissionPolicy,
}

impl ShardLoomSessionRegistryStatus {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::ExistingReportSurface => "existing_report_surface",
            Self::PlannedExplicitRegistry => "planned_explicit_registry",
            Self::BlockedUntilAdmissionPolicy => "blocked_until_admission_policy",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
#[allow(clippy::struct_excessive_bools)]
pub struct ShardLoomSessionRegistryEntry {
    pub kind: ShardLoomSessionRegistryKind,
    pub registry_ref: &'static str,
    pub status: ShardLoomSessionRegistryStatus,
    pub explicit_session_required: bool,
    pub hidden_global_state_allowed: bool,
    pub runtime_mutation_allowed: bool,
    pub fallback_attempted: bool,
}

impl ShardLoomSessionRegistryEntry {
    #[must_use]
    pub const fn new(
        kind: ShardLoomSessionRegistryKind,
        registry_ref: &'static str,
        status: ShardLoomSessionRegistryStatus,
    ) -> Self {
        Self {
            kind,
            registry_ref,
            status,
            explicit_session_required: true,
            hidden_global_state_allowed: false,
            runtime_mutation_allowed: false,
            fallback_attempted: false,
        }
    }

    #[must_use]
    pub const fn safe_by_default(&self) -> bool {
        self.explicit_session_required
            && !self.hidden_global_state_allowed
            && !self.runtime_mutation_allowed
            && !self.fallback_attempted
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
#[allow(clippy::struct_excessive_bools)]
pub struct ShardLoomSessionModelReport {
    pub schema_version: &'static str,
    pub report_id: &'static str,
    pub design_reference: &'static str,
    pub explicit_session_context_required: bool,
    pub hidden_global_registries_allowed: bool,
    pub runtime_registry_mutation_allowed: bool,
    pub registry_entries: Vec<ShardLoomSessionRegistryEntry>,
    pub admission_policy_required: bool,
    pub evidence_registry_required: bool,
    pub provider_registry_required: bool,
    pub runtime_execution_allowed: bool,
    pub external_engine_invoked: bool,
    pub fallback_attempted: bool,
}

impl ShardLoomSessionModelReport {
    #[must_use]
    pub fn report_only() -> Self {
        use ShardLoomSessionRegistryKind as Kind;
        use ShardLoomSessionRegistryStatus as Status;

        Self {
            schema_version: "shardloom.session_model_report.v1",
            report_id: "priority_2_6.vortex_inspired_session_model",
            design_reference: "vortex_session_and_registries",
            explicit_session_context_required: true,
            hidden_global_registries_allowed: false,
            runtime_registry_mutation_allowed: false,
            registry_entries: vec![
                ShardLoomSessionRegistryEntry::new(
                    Kind::Operator,
                    "PhysicalKernelRegistryPlan",
                    Status::ExistingReportSurface,
                ),
                ShardLoomSessionRegistryEntry::new(
                    Kind::Function,
                    "KernelRegistrySnapshot",
                    Status::ExistingReportSurface,
                ),
                ShardLoomSessionRegistryEntry::new(
                    Kind::Aggregate,
                    "future_aggregate_registry",
                    Status::PlannedExplicitRegistry,
                ),
                ShardLoomSessionRegistryEntry::new(
                    Kind::Sketch,
                    "ApproxSketchFunctionGateReport",
                    Status::ExistingReportSurface,
                ),
                ShardLoomSessionRegistryEntry::new(
                    Kind::Window,
                    "future_window_registry",
                    Status::PlannedExplicitRegistry,
                ),
                ShardLoomSessionRegistryEntry::new(
                    Kind::Join,
                    "future_join_registry",
                    Status::PlannedExplicitRegistry,
                ),
                ShardLoomSessionRegistryEntry::new(
                    Kind::SourceSinkAdapter,
                    "InputAdapterRegistrySnapshot",
                    Status::ExistingReportSurface,
                ),
                ShardLoomSessionRegistryEntry::new(
                    Kind::ExecutionProvider,
                    "ExecutionProviderKind",
                    Status::ExistingReportSurface,
                ),
                ShardLoomSessionRegistryEntry::new(
                    Kind::SemanticProfile,
                    "ShardLoomNativeSemanticProfile",
                    Status::ExistingReportSurface,
                ),
                ShardLoomSessionRegistryEntry::new(
                    Kind::EvidenceArtifact,
                    "EvidenceArtifactEnvelope",
                    Status::ExistingReportSurface,
                ),
                ShardLoomSessionRegistryEntry::new(
                    Kind::PolicyEffect,
                    "ShardLoomExecutionPolicy",
                    Status::ExistingReportSurface,
                ),
            ],
            admission_policy_required: true,
            evidence_registry_required: true,
            provider_registry_required: true,
            runtime_execution_allowed: false,
            external_engine_invoked: false,
            fallback_attempted: false,
        }
    }

    #[must_use]
    pub fn all_registries_safe_by_default(&self) -> bool {
        self.registry_entries
            .iter()
            .all(ShardLoomSessionRegistryEntry::safe_by_default)
    }

    #[must_use]
    pub fn registry_kind_order(&self) -> Vec<&'static str> {
        self.registry_entries
            .iter()
            .map(|entry| entry.kind.as_str())
            .collect()
    }

    #[must_use]
    pub const fn preserves_no_runtime_expansion(&self) -> bool {
        self.explicit_session_context_required
            && !self.hidden_global_registries_allowed
            && !self.runtime_registry_mutation_allowed
            && !self.runtime_execution_allowed
            && !self.external_engine_invoked
            && !self.fallback_attempted
    }
}

#[must_use]
pub fn plan_shardloom_session_model() -> ShardLoomSessionModelReport {
    ShardLoomSessionModelReport::report_only()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn session_model_requires_explicit_context_and_no_globals() {
        let report = plan_shardloom_session_model();

        assert!(report.explicit_session_context_required);
        assert!(!report.hidden_global_registries_allowed);
        assert!(!report.runtime_registry_mutation_allowed);
        assert!(report.all_registries_safe_by_default());
        assert!(report.preserves_no_runtime_expansion());
    }

    #[test]
    fn session_model_tracks_required_registry_families() {
        let report = plan_shardloom_session_model();
        let kinds = report.registry_kind_order();

        for expected in [
            "operator",
            "function",
            "aggregate",
            "sketch",
            "source_sink_adapter",
            "execution_provider",
            "semantic_profile",
            "evidence_artifact",
            "policy_effect",
        ] {
            assert!(kinds.contains(&expected));
        }
    }
}
